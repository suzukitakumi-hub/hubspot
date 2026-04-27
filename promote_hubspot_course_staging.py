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
    ensure_worksheet,
    header_matches_expected,
    load_json,
    now_jst,
    normalize_header_row,
    parse_iso_datetime,
    read_worksheet_matrix,
    set_worksheet_hidden,
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
    "開封数（bot含む）",
    "開封率（bot含む）",
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
    return parser.parse_args()


def parse_hyperlink_email_id(formula_value: str) -> str:
    match = HYPERLINK_EMAIL_ID_RE.search(formula_value or "")
    return match.group(1) if match else ""


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


def sync_live_layout_from_cia(spreadsheet) -> None:
    source_title = "CIA"
    subject_source_title = "USCPA"
    source_ws = spreadsheet.worksheet(source_title)
    source_col_count = len(COURSE_SHEET_HEADER)

    metadata = spreadsheet.fetch_sheet_metadata(
        params={
            "ranges": [f"{source_title}!A:{COURSE_SHEET_LAST_COLUMN}", f"{subject_source_title}!B:B"],
            "includeGridData": "true",
            "fields": "sheets(properties(sheetId,title,gridProperties.frozenRowCount),data(columnMetadata(pixelSize)))",
        }
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
        target_ws = spreadsheet.worksheet(title)
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
        spreadsheet.batch_update({"requests": requests})


def delete_worksheet_if_exists(spreadsheet, title: str) -> None:
    import gspread

    try:
        worksheet = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return
    spreadsheet.del_worksheet(worksheet)


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
    spreadsheet = gc.open_by_key(args.spreadsheet_id)

    staging_formula_snapshot = {}
    for course in TARGET_COURSES:
        worksheet = spreadsheet.worksheet(staging_tab_title(course))
        values = read_worksheet_matrix(worksheet, value_render_option="FORMULA")
        if not values:
            raise SystemExit(f"Staging tab {staging_tab_title(course)} is empty. Promotion aborted.")
        if not header_matches_expected(values[0]):
            raise SystemExit(f"Staging tab {staging_tab_title(course)} header mismatch. Promotion aborted.")
        staging_formula_snapshot[staging_tab_title(course)] = values

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

    for course in TARGET_COURSES:
        values = [list(row) for row in staging_formula_snapshot[staging_tab_title(course)]]
        if promotion_mode == "partial":
            for row in values[1:]:
                email_id = parse_hyperlink_email_id(row[header_index["メール件名（HubSpotリンク）"]])
                if email_id and email_id in blocked_email_ids:
                    if row[cv_count_idx] != "" or row[cv_breakdown_idx] != "":
                        partial_blank_rows += 1
                    row[cv_count_idx] = ""
                    row[cv_breakdown_idx] = ""
        live_ws = ensure_worksheet(spreadsheet, course, max(200, len(values) + 30), len(COURSE_SHEET_HEADER) + 3)
        existing_values = read_worksheet_matrix(live_ws, value_render_option="FORMULA")
        preserved_rows = []
        if existing_values:
            if not header_matches_expected(existing_values[0]):
                raise SystemExit(f"Live tab {course} header mismatch. Promotion aborted.")
            for row in existing_values[1:]:
                if not any(cell != "" for cell in row):
                    continue
                if row[month_idx] != args.month:
                    preserved_rows.append(list(row))
        merged_rows = preserved_rows + [list(row) for row in values[1:]]
        merged_rows.sort(key=lambda row: (row[send_date_idx], row[header_index["メール内部名"]]))
        required_rows = max(200, len(merged_rows) + 30)
        required_cols = len(COURSE_SHEET_HEADER) + 3
        if live_ws.row_count < required_rows or live_ws.col_count < required_cols:
            live_ws.resize(rows=max(live_ws.row_count, required_rows), cols=max(live_ws.col_count, required_cols))
        write_sheet_values(live_ws, [values[0]] + merged_rows, apply_formatting=False)
        set_worksheet_hidden(spreadsheet, live_ws, False)
        set_worksheet_hidden(spreadsheet, spreadsheet.worksheet(staging_tab_title(course)), True)

    for title in [
        REVIEW_HUBSPOT_ONLY_SHEET,
        REVIEW_MASTER_UNMATCHED_SHEET,
        staging_tab_title(REVIEW_HUBSPOT_ONLY_SHEET),
        staging_tab_title(REVIEW_MASTER_UNMATCHED_SHEET),
    ]:
        delete_worksheet_if_exists(spreadsheet, title)

    sync_live_layout_from_cia(spreadsheet)

    print(f"validation_report={os.path.abspath(report_path)}")
    print(f"promotion_mode={promotion_mode}")
    print(f"partial_ga4_blank_rows={partial_blank_rows}")
    print(f"live_sheet_updated=https://docs.google.com/spreadsheets/d/{args.spreadsheet_id}/edit")


if __name__ == "__main__":
    main()
