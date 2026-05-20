#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re

from hubspot_course_sheet_guardrails import (
    COURSE_SHEET_HEADER,
    COURSE_SHEET_INDEX,
    COURSE_SHEET_LAST_COLUMN,
    DEFAULT_SERVICE_ACCOUNT_JSON,
    DEFAULT_SPREADSHEET_ID,
    DEFAULT_VALIDATION_REPORT_MAX_AGE_MINUTES,
    REVIEW_HUBSPOT_ONLY_SHEET,
    REVIEW_MASTER_UNMATCHED_SHEET,
    TARGET_COURSES,
    derive_validation_report_path,
    header_matches_expected,
    load_json,
    now_jst,
    normalize_month_value,
    normalize_sheet_matrix,
    parse_iso_datetime,
    sheets_call,
    snapshot_sha256,
    staging_tab_title,
    write_sheet_values,
)

PARTIAL_GA4_ALLOWED_CODES = {
    "provisional_send_date",
    "duplicate_session_manual_ad_content",
    "multiple_candidate_email",
    "missing_ga4_map_row",
    "hubspot_detail_fetch_error",
    "field_mismatch",
}
VOLATILE_PARTIAL_FIELDS = {
    "配信数",
    "開封数（bot除外）",
    "開封数（bot含む）",
    "クリック数",
    "配信停止数",
    "開封率（bot除外）",
    "開封率（bot含む）",
    "クリック率",
    "クリックスルー率",
    "配信停止率",
}
HYPERLINK_EMAIL_ID_RE = re.compile(r"/details/(\d+)/performance")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote validated staging tabs into live course tabs.")
    parser.add_argument("--month", default="2026-03", help="YYYY-MM")
    parser.add_argument("--spreadsheet-id", default=DEFAULT_SPREADSHEET_ID)
    parser.add_argument(
        "--service-account-json",
        default=os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", DEFAULT_SERVICE_ACCOUNT_JSON),
    )
    parser.add_argument(
        "--validation-report",
        default="",
        help="Validation report JSON created by validate_hubspot_course_staging.py",
    )
    parser.add_argument(
        "--max-validation-report-age-minutes",
        type=int,
        default=DEFAULT_VALIDATION_REPORT_MAX_AGE_MINUTES,
        help="Refuse promotion when the validation report is older than this threshold.",
    )
    parser.add_argument(
        "--sync-layout",
        action="store_true",
        default=os.environ.get("HUBSPOT_COURSE_SYNC_LAYOUT_EACH_RUN", "").strip().lower() in {"1", "true", "yes", "on"},
        help="Also resync live tab formatting from CIA/USCPA. Disabled by default to reduce Sheets API quota pressure.",
    )
    return parser.parse_args()


def parse_hyperlink_email_id(formula_value: str) -> str:
    match = HYPERLINK_EMAIL_ID_RE.search(formula_value or "")
    return match.group(1) if match else ""


def run_sheets_call(label: str, func, attempts: int = 4, retry_sleep_seconds: int = 70):
    return sheets_call(label, func, attempts=attempts, retry_sleep_seconds=retry_sleep_seconds)


def quote_sheet_title(title: str) -> str:
    return "'" + title.replace("'", "''") + "'"


def load_worksheets_by_title(spreadsheet) -> dict:
    worksheets = run_sheets_call("worksheets", lambda: spreadsheet.worksheets())
    return {worksheet.title: worksheet for worksheet in worksheets}


def require_cached_worksheet(worksheets_by_title: dict, title: str):
    worksheet = worksheets_by_title.get(title)
    if not worksheet:
        raise SystemExit(f"Worksheet not found: {title}")
    return worksheet


def ensure_cached_worksheet(spreadsheet, worksheets_by_title: dict, title: str, rows: int, cols: int):
    worksheet = worksheets_by_title.get(title)
    if worksheet:
        return worksheet
    worksheet = run_sheets_call(
        f"add_worksheet:{title}",
        lambda: spreadsheet.add_worksheet(title=title, rows=rows, cols=cols),
    )
    worksheets_by_title[title] = worksheet
    return worksheet


def batch_get_matrices(spreadsheet, worksheets_by_title: dict, titles: list[str]) -> dict[str, list[list[str]]]:
    ranges = []
    existing_titles = []
    for title in titles:
        worksheet = worksheets_by_title.get(title)
        if not worksheet:
            continue
        existing_titles.append(title)
        ranges.append(f"{quote_sheet_title(title)}!A1:{COURSE_SHEET_LAST_COLUMN}{max(worksheet.row_count, 1)}")
    if not ranges:
        return {}
    response = run_sheets_call(
        "values_batch_get",
        lambda: spreadsheet.values_batch_get(ranges, params={"valueRenderOption": "FORMULA"}),
    )
    value_ranges = response.get("valueRanges") or []
    out: dict[str, list[list[str]]] = {}
    for index, title in enumerate(existing_titles):
        values = []
        if index < len(value_ranges):
            values = value_ranges[index].get("values") or []
        out[title] = normalize_sheet_matrix(values)
    return out


def preformat_live_value_columns(worksheet) -> None:
    run_sheets_call(
        f"preformat_value_columns:{worksheet.title}",
        lambda: worksheet.batch_format(
            [
                {"range": "A:A", "format": {"numberFormat": {"type": "TEXT"}}},
                {"range": "C:C", "format": {"numberFormat": {"type": "TEXT"}}},
                {"range": f"O:{COURSE_SHEET_LAST_COLUMN}", "format": {"numberFormat": {"type": "TEXT"}}},
            ]
        ),
    )


def collect_blocked_email_ids(report: dict) -> tuple[str, set[str]]:
    issues = report.get("issues", []) or []
    issue_codes = {str(issue.get("code", "")).strip() for issue in issues}
    issue_codes.discard("")

    if report.get("status") == "pass" and int(report.get("blocking_issue_count", 0)) == 0:
        return "full", set()

    unexpected_codes = issue_codes - PARTIAL_GA4_ALLOWED_CODES
    if unexpected_codes:
        raise SystemExit(
            "Validation report contains non-recoverable issues. Promotion aborted: "
            + ", ".join(sorted(unexpected_codes))
        )
    provisional_email_ids = {
        str(issue.get("email_id", "")).strip()
        for issue in issues
        if str(issue.get("code", "")).strip() == "provisional_send_date"
    }
    unexpected_field_mismatches = sorted(
        {
            str(issue.get("field", "")).strip()
            for issue in issues
            if str(issue.get("code", "")).strip() == "field_mismatch"
            and str(issue.get("field", "")).strip() not in VOLATILE_PARTIAL_FIELDS
            and str(issue.get("email_id", "")).strip() not in provisional_email_ids
        }
    )
    if unexpected_field_mismatches:
        raise SystemExit(
            "Validation report contains non-recoverable field mismatches. Promotion aborted: "
            + ", ".join(unexpected_field_mismatches)
        )

    blocked_email_ids: set[str] = set()
    for issue in issues:
        code = str(issue.get("code", "")).strip()
        if code in {"provisional_send_date", "multiple_candidate_email", "missing_ga4_map_row", "hubspot_detail_fetch_error"}:
            email_id = str(issue.get("email_id", "")).strip()
            if email_id:
                blocked_email_ids.add(email_id)
        elif code == "duplicate_session_manual_ad_content":
            for row in issue.get("emails", []) or []:
                email_id = str((row or {}).get("email_id", "")).strip()
                if email_id:
                    blocked_email_ids.add(email_id)

    return "partial", blocked_email_ids


def load_and_validate_report(path: str, month: str, max_age_minutes: int) -> tuple[dict, str, set[str]]:
    if not os.path.exists(path):
        raise SystemExit(f"Validation report not found: {path}")
    report = load_json(path)
    if str(report.get("month", "")) != month:
        raise SystemExit(f"Validation report month mismatch: expected {month}, got {report.get('month')!r}")
    snapshot_hash = str(report.get("staging_formula_snapshot_sha256", "")).strip()
    if not snapshot_hash:
        raise SystemExit("Validation report missing staging_formula_snapshot_sha256.")
    generated_at_raw = str(report.get("generated_at", "")).strip()
    if not generated_at_raw:
        raise SystemExit("Validation report missing generated_at.")
    generated_at = parse_iso_datetime(generated_at_raw)
    age_minutes = (now_jst() - generated_at.astimezone(now_jst().tzinfo)).total_seconds() / 60.0
    if age_minutes > max_age_minutes:
        raise SystemExit(
            f"Validation report is stale ({age_minutes:.1f} minutes old > {max_age_minutes} minutes). Promotion aborted."
        )
    mode, blocked_email_ids = collect_blocked_email_ids(report)
    return report, mode, blocked_email_ids


def sync_live_layout_from_cia(spreadsheet, worksheets_by_title: dict) -> None:
    source_title = "CIA"
    subject_source_title = "USCPA"
    source_ws = require_cached_worksheet(worksheets_by_title, source_title)
    source_col_count = len(COURSE_SHEET_HEADER)

    metadata = run_sheets_call(
        "fetch_layout_metadata",
        lambda: spreadsheet.fetch_sheet_metadata(
            params={
                "ranges": [f"{source_title}!A:{COURSE_SHEET_LAST_COLUMN}", f"{subject_source_title}!B:B"],
                "includeGridData": "true",
                "fields": "sheets(properties(sheetId,title,gridProperties.frozenRowCount),data(columnMetadata(pixelSize)))",
            }
        ),
    )
    sheets = metadata.get("sheets") or []
    source_sheet = next((sheet for sheet in sheets if (sheet.get("properties") or {}).get("title") == source_title), {})
    subject_sheet = next((sheet for sheet in sheets if (sheet.get("properties") or {}).get("title") == subject_source_title), {})
    source_props = source_sheet.get("properties") or {}
    source_data = ((source_sheet.get("data") or [{}])[0]) if source_sheet else {}
    source_column_meta = source_data.get("columnMetadata") or []
    source_widths = []
    for idx in range(source_col_count):
        pixel_size = 100
        if idx < len(source_column_meta):
            pixel_size = int((source_column_meta[idx] or {}).get("pixelSize") or 100)
        source_widths.append(pixel_size)
    subject_data = ((subject_sheet.get("data") or [{}])[0]) if subject_sheet else {}
    subject_column_meta = subject_data.get("columnMetadata") or []
    if subject_column_meta:
        source_widths[1] = int((subject_column_meta[0] or {}).get("pixelSize") or source_widths[1])
    frozen_rows = int((source_props.get("gridProperties") or {}).get("frozenRowCount") or 1)

    requests = []
    for title in TARGET_COURSES:
        target_ws = require_cached_worksheet(worksheets_by_title, title)
        if title != source_title:
            requests.append(
                {
                    "copyPaste": {
                        "source": {
                            "sheetId": source_ws.id,
                            "startRowIndex": 0,
                            "endRowIndex": source_ws.row_count,
                            "startColumnIndex": 0,
                            "endColumnIndex": source_col_count,
                        },
                        "destination": {
                            "sheetId": target_ws.id,
                            "startRowIndex": 0,
                            "endRowIndex": target_ws.row_count,
                            "startColumnIndex": 0,
                            "endColumnIndex": source_col_count,
                        },
                        "pasteType": "PASTE_FORMAT",
                        "pasteOrientation": "NORMAL",
                    }
                }
            )
        requests.append(
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": target_ws.id,
                        "gridProperties": {"frozenRowCount": frozen_rows},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            }
        )
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": target_ws.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": source_col_count,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "wrapStrategy": "WRAP",
                            "verticalAlignment": "MIDDLE",
                        }
                    },
                    "fields": "userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment",
                }
            }
        )
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": target_ws.id,
                        "dimension": "ROWS",
                        "startIndex": 1,
                        "endIndex": target_ws.row_count,
                    },
                    "properties": {"pixelSize": 21},
                    "fields": "pixelSize",
                }
            }
        )
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": target_ws.id,
                        "dimension": "ROWS",
                        "startIndex": 0,
                        "endIndex": 1,
                    },
                    "properties": {"pixelSize": 42},
                    "fields": "pixelSize",
                }
            }
        )
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": target_ws.id,
                        "startRowIndex": 1,
                        "endRowIndex": target_ws.row_count,
                        "startColumnIndex": 10,
                        "endColumnIndex": 11,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "PERCENT",
                                "pattern": "0.00%",
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": target_ws.id,
                        "startRowIndex": 1,
                        "endRowIndex": target_ws.row_count,
                        "startColumnIndex": COURSE_SHEET_INDEX["送付リスト"],
                        "endColumnIndex": COURSE_SHEET_INDEX["送付リスト"] + 1,
                    },
                    "cell": {"userEnteredFormat": {"wrapStrategy": "CLIP"}},
                    "fields": "userEnteredFormat.wrapStrategy",
                }
            }
        )
        for idx, pixel_size in enumerate(source_widths):
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": target_ws.id,
                            "dimension": "COLUMNS",
                            "startIndex": idx,
                            "endIndex": idx + 1,
                        },
                        "properties": {"pixelSize": pixel_size},
                        "fields": "pixelSize",
                    }
                }
            )

    if requests:
        run_sheets_call("sync_live_layout", lambda: spreadsheet.batch_update({"requests": requests}))


def delete_worksheet_if_exists(spreadsheet, worksheets_by_title: dict, title: str) -> None:
    worksheet = worksheets_by_title.get(title)
    if not worksheet:
        return
    run_sheets_call(f"delete_worksheet:{title}", lambda: spreadsheet.del_worksheet(worksheet))
    worksheets_by_title.pop(title, None)


def main() -> None:
    args = parse_args()
    if not os.path.exists(args.service_account_json):
        raise SystemExit(f"Service account json not found: {args.service_account_json}")

    report_path = args.validation_report or derive_validation_report_path(args.month)
    report, promotion_mode, blocked_email_ids = load_and_validate_report(
        report_path,
        args.month,
        args.max_validation_report_age_minutes,
    )
    report_spreadsheet_id = str(report.get("spreadsheet_id", "")).strip()
    if report_spreadsheet_id and report_spreadsheet_id != args.spreadsheet_id:
        raise SystemExit(
            f"Validation report spreadsheet mismatch: expected {args.spreadsheet_id}, got {report_spreadsheet_id}."
        )

    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(args.service_account_json, scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = run_sheets_call("open_by_key", lambda: gc.open_by_key(args.spreadsheet_id))
    worksheets_by_title = load_worksheets_by_title(spreadsheet)

    staging_titles = [staging_tab_title(course) for course in TARGET_COURSES]
    staging_formula_snapshot = batch_get_matrices(spreadsheet, worksheets_by_title, staging_titles)
    for course in TARGET_COURSES:
        title = staging_tab_title(course)
        values = staging_formula_snapshot.get(title, [])
        if not values:
            raise SystemExit(f"Staging tab {title} is empty. Promotion aborted.")
        if not header_matches_expected(values[0]):
            raise SystemExit(f"Staging tab {title} header mismatch. Promotion aborted.")

    current_snapshot_hash = snapshot_sha256(staging_formula_snapshot)
    expected_snapshot_hash = str(report.get("staging_formula_snapshot_sha256", "")).strip()
    if current_snapshot_hash != expected_snapshot_hash:
        raise SystemExit("Staging snapshot hash no longer matches the validated report. Promotion aborted.")

    header_index = {name: idx for idx, name in enumerate(COURSE_SHEET_HEADER)}
    cv_count_idx = header_index["CV数"]
    cv_breakdown_idx = header_index["CV内訳"]
    month_idx = header_index["対象月"]
    send_date_idx = header_index["送付日"]
    partial_blank_rows = 0
    live_worksheets_by_course = {}
    for course in TARGET_COURSES:
        source_values = staging_formula_snapshot[staging_tab_title(course)]
        live_worksheets_by_course[course] = ensure_cached_worksheet(
            spreadsheet,
            worksheets_by_title,
            course,
            max(200, len(source_values) + 30),
            len(COURSE_SHEET_HEADER) + 3,
        )
    live_formula_snapshot = batch_get_matrices(spreadsheet, worksheets_by_title, list(TARGET_COURSES))
    visibility_requests = []

    for course in TARGET_COURSES:
        values = [list(row) for row in staging_formula_snapshot[staging_tab_title(course)]]
        for row in values[1:]:
            if len(row) > month_idx:
                row[month_idx] = args.month
        if promotion_mode == "partial":
            for row in values[1:]:
                email_id = parse_hyperlink_email_id(row[header_index["メール件名（HubSpotリンク）"]])
                if email_id and email_id in blocked_email_ids:
                    if row[cv_count_idx] != "" or row[cv_breakdown_idx] != "":
                        partial_blank_rows += 1
                    row[cv_count_idx] = ""
                    row[cv_breakdown_idx] = ""
        live_ws = live_worksheets_by_course[course]
        existing_values = live_formula_snapshot.get(course, [])
        preserved_rows = []
        if existing_values:
            if not header_matches_expected(existing_values[0]):
                raise SystemExit(f"Live tab {course} header mismatch. Promotion aborted.")
            for row in existing_values[1:]:
                if not any(cell != "" for cell in row):
                    continue
                if normalize_month_value(row[month_idx]) != args.month:
                    preserved_rows.append(list(row))
        merged_rows = preserved_rows + [list(row) for row in values[1:]]
        merged_rows.sort(key=lambda row: (row[send_date_idx], row[header_index["メール内部名"]]))
        required_rows = max(200, len(merged_rows) + 30)
        required_cols = len(COURSE_SHEET_HEADER) + 3
        if live_ws.row_count < required_rows or live_ws.col_count < required_cols:
            run_sheets_call(
                f"resize:{course}",
                lambda live_ws=live_ws: live_ws.resize(
                    rows=max(live_ws.row_count, required_rows),
                    cols=max(live_ws.col_count, required_cols),
                ),
            )
        output_values = [values[0]] + merged_rows
        preformat_live_value_columns(live_ws)
        run_sheets_call(
            f"write_sheet_values:{course}",
            lambda live_ws=live_ws, output_values=output_values: write_sheet_values(
                live_ws,
                output_values,
                apply_formatting=False,
            ),
        )
        visibility_requests.append(
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": live_ws.id, "hidden": False},
                    "fields": "hidden",
                }
            }
        )
        staging_ws = require_cached_worksheet(worksheets_by_title, staging_tab_title(course))
        visibility_requests.append(
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": staging_ws.id, "hidden": True},
                    "fields": "hidden",
                }
            }
        )

    if visibility_requests:
        run_sheets_call("set_visibility", lambda: spreadsheet.batch_update({"requests": visibility_requests}))

    for title in [
        REVIEW_HUBSPOT_ONLY_SHEET,
        REVIEW_MASTER_UNMATCHED_SHEET,
        staging_tab_title(REVIEW_HUBSPOT_ONLY_SHEET),
        staging_tab_title(REVIEW_MASTER_UNMATCHED_SHEET),
    ]:
        delete_worksheet_if_exists(spreadsheet, worksheets_by_title, title)

    if args.sync_layout:
        sync_live_layout_from_cia(spreadsheet, worksheets_by_title)

    print(f"validation_report={os.path.abspath(report_path)}")
    print(f"promotion_mode={promotion_mode}")
    print(f"partial_ga4_blank_rows={partial_blank_rows}")
    print(f"layout_sync={'enabled' if args.sync_layout else 'skipped'}")
    print(f"live_sheet_updated=https://docs.google.com/spreadsheets/d/{args.spreadsheet_id}/edit")


if __name__ == "__main__":
    main()
