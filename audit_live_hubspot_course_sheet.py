#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter, defaultdict
from typing import Dict, List, Tuple

from hubspot_course_sheet_guardrails import (
    COURSE_SHEET_HEADER,
    COURSE_SHEET_LAST_COLUMN,
    DEFAULT_GA4_MAP_MAX_AGE_MINUTES,
    DEFAULT_PROVISIONAL_DAYS,
    DEFAULT_SERVICE_ACCOUNT_JSON,
    DEFAULT_SPREADSHEET_ID,
    TARGET_COURSES,
    derive_ga4_map_manifest_path,
    header_matches_expected,
    now_jst_iso,
    normalize_header_row,
    strip_literal_prefix,
    write_json,
)
from update_test_hubspot_course_tabs import validate_ga4_map_bundle
from validate_hubspot_course_staging import (
    build_source_contexts,
    load_ga4_map,
    parse_hyperlink_email_id,
)

VOLATILE_FIELD_NAMES = {
    "開封数（bot含む）",
    "開封率（bot含む）",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit the live HubSpot course sheet against HubSpot and GA4 source data.")
    parser.add_argument("--month", default="2026-03", help="YYYY-MM")
    parser.add_argument("--spreadsheet-id", default=DEFAULT_SPREADSHEET_ID)
    parser.add_argument("--hubspot-token", default=os.environ.get("HUBSPOT_PAT", ""))
    parser.add_argument(
        "--service-account-json",
        default=os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", DEFAULT_SERVICE_ACCOUNT_JSON),
    )
    parser.add_argument(
        "--ga4-map-csv",
        default="ga4_hubspot_cv_map_2026-03_email_map.csv",
        help="CSV created by map_ga4_cv_to_hubspot_emails.py",
    )
    parser.add_argument(
        "--ga4-map-manifest",
        default="",
        help="Manifest JSON created alongside --ga4-map-csv",
    )
    parser.add_argument(
        "--max-ga4-map-age-minutes",
        type=int,
        default=DEFAULT_GA4_MAP_MAX_AGE_MINUTES,
    )
    parser.add_argument(
        "--provisional-days",
        type=int,
        default=DEFAULT_PROVISIONAL_DAYS,
    )
    parser.add_argument("--email-type", default="BATCH_EMAIL")
    parser.add_argument(
        "--output",
        default="hubspot_course_sheet_live_audit_2026-03.json",
        help="Write audit JSON to this path.",
    )
    return parser.parse_args()


def parse_sheet_int(value: str) -> int:
    text = strip_literal_prefix(value).replace(",", "").strip()
    if not text:
        return 0
    return int(float(text))


def load_manifest(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def blocked_ga4_email_ids(manifest: dict, source_contexts: Dict[str, dict], provisional_days: int) -> Tuple[set[str], Dict[str, List[dict]]]:
    issues_by_code: Dict[str, List[dict]] = defaultdict(list)
    blocked: set[str] = set()

    provisional_cutoff = None
    # build_source_contexts already applies provisional_days, so use send_dt from source_contexts
    if provisional_days >= 0:
        import datetime as dt
        from hubspot_course_sheet_guardrails import now_jst

        provisional_cutoff = now_jst() - dt.timedelta(days=provisional_days)

    for email_id, ctx in source_contexts.items():
        if provisional_cutoff and ctx["send_dt"] > provisional_cutoff:
            issue = {
                "email_id": email_id,
                "course": ctx["course"],
                "email_name": ctx["email_name"],
                "send_date": ctx["send_date_text"],
            }
            issues_by_code["provisional_send_date"].append(issue)
            blocked.add(email_id)

    for row in manifest.get("duplicate_keys", []) or []:
        relevant_emails = []
        for email in row.get("emails", []) or []:
            email_id = str((email or {}).get("email_id", "")).strip()
            if email_id and email_id in source_contexts:
                relevant_emails.append(email)
                blocked.add(email_id)
        if relevant_emails:
            issues_by_code["duplicate_session_manual_ad_content"].append(
                {
                    "session_manual_ad_content": row.get("sessionManualAdContent"),
                    "email_count": len(relevant_emails),
                    "emails": relevant_emails,
                }
            )

    for row in manifest.get("multiple_candidate_emails", []) or []:
        email_id = str(row.get("email_id", "")).strip()
        if email_id and email_id in source_contexts:
            issues_by_code["multiple_candidate_email"].append(row)
            blocked.add(email_id)

    for email_id, ctx in source_contexts.items():
        if ctx["email_id"] and not ctx["matched_session_manual_ad_content"]:
            issues_by_code["missing_ga4_map_row"].append(
                {
                    "email_id": email_id,
                    "course": ctx["course"],
                    "email_name": ctx["email_name"],
                }
            )
            blocked.add(email_id)
        if ctx["hubspot_detail_fetch_error"]:
            issues_by_code["hubspot_detail_fetch_error"].append(
                {
                    "email_id": email_id,
                    "course": ctx["course"],
                    "email_name": ctx["email_name"],
                    "error": ctx["hubspot_detail_fetch_error"],
                }
            )
            blocked.add(email_id)

    return blocked, issues_by_code


def main() -> None:
    args = parse_args()
    token = (args.hubspot_token or "").strip()
    if not token:
        raise SystemExit("HubSpot token is missing. Set HUBSPOT_PAT or pass --hubspot-token.")
    if not os.path.exists(args.service_account_json):
        raise SystemExit(f"Service account json not found: {args.service_account_json}")

    manifest_path = args.ga4_map_manifest or derive_ga4_map_manifest_path(args.ga4_map_csv)
    validate_ga4_map_bundle(
        csv_path=args.ga4_map_csv,
        manifest_path=manifest_path,
        month=args.month,
        max_age_minutes=args.max_ga4_map_age_minutes,
    )
    manifest = load_manifest(manifest_path)
    ga4_map = load_ga4_map(args.ga4_map_csv)

    from google.oauth2.service_account import Credentials
    import gspread
    from update_test_hubspot_course_tabs import HubSpotClient

    client = HubSpotClient(token)
    source_contexts, source_ids_by_course, provisional_rows, hubspot_only_rows, management_unmatched_rows = build_source_contexts(
        client=client,
        month=args.month,
        email_type=args.email_type,
        ga4_map=ga4_map,
        provisional_days=args.provisional_days,
        service_account_json=args.service_account_json,
    )
    blocked_email_ids, blocked_issues = blocked_ga4_email_ids(manifest, source_contexts, args.provisional_days)
    provisional_email_ids = {
        str(row.get("email_id", "")).strip()
        for row in blocked_issues.get("provisional_send_date", [])
    }

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(args.service_account_json, scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(args.spreadsheet_id)

    issues: List[dict] = []
    issue_counter: Counter[str] = Counter()
    field_counter: Counter[str] = Counter()
    per_course_rows: Dict[str, int] = {}
    per_course_issue_counts: Dict[str, Counter[str]] = {course: Counter() for course in TARGET_COURSES}
    volatile_field_mismatches: List[dict] = []
    provisional_field_mismatches: List[dict] = []
    checked_rows = 0

    for course in TARGET_COURSES:
        worksheet = spreadsheet.worksheet(course)
        values = worksheet.get_all_values()
        if not values:
            issue = {"code": "empty_live_tab", "course": course, "message": "Live tab is empty."}
            issues.append(issue)
            issue_counter[issue["code"]] += 1
            per_course_issue_counts[course][issue["code"]] += 1
            continue
        header = values[0]
        if not header_matches_expected(header):
            issue = {
                "code": "header_mismatch",
                "course": course,
                "message": "Live tab header does not match the expected schema.",
                "actual": normalize_header_row(header),
                "expected": COURSE_SHEET_HEADER,
            }
            issues.append(issue)
            issue_counter[issue["code"]] += 1
            per_course_issue_counts[course][issue["code"]] += 1
            continue

        header_index = {name: idx for idx, name in enumerate(normalize_header_row(header))}
        formula_rows = worksheet.get(
            f"A1:{COURSE_SHEET_LAST_COLUMN}{max(worksheet.row_count, 1)}",
            value_render_option="FORMULA",
        )
        row_count = max(len(values), len(formula_rows))
        per_course_rows[course] = 0
        live_email_ids = []

        for row_idx in range(1, row_count):
            display_row = values[row_idx] if row_idx < len(values) else []
            formula_row = formula_rows[row_idx] if row_idx < len(formula_rows) else []
            if not any(cell != "" for cell in display_row) and not any(cell != "" for cell in formula_row):
                continue
            def display(name: str) -> str:
                idx = header_index[name]
                return display_row[idx] if idx < len(display_row) else ""

            def formula(name: str) -> str:
                idx = header_index[name]
                return formula_row[idx] if idx < len(formula_row) else ""

            if strip_literal_prefix(display("対象月")) != args.month:
                continue

            per_course_rows[course] += 1
            email_id = parse_hyperlink_email_id(formula("メール件名（HubSpotリンク）"))
            if not email_id or email_id not in source_contexts:
                issue = {
                    "code": "row_not_in_source",
                    "course": course,
                    "sheet_row": row_idx + 1,
                    "email_id": email_id,
                    "email_name": display("メール内部名"),
                }
                issues.append(issue)
                issue_counter[issue["code"]] += 1
                per_course_issue_counts[course][issue["code"]] += 1
                continue

            live_email_ids.append(email_id)
            checked_rows += 1
            ctx = source_contexts[email_id]

            expected_fields = {
                "送付日": ctx["send_date_text"],
                "配信数": ctx["delivered"],
                "開封数（bot除外）": ctx["opened"],
                "開封数（bot含む）": ctx["opened_including_bots"],
                "クリック数": ctx["clicked"],
                "配信停止数": ctx["unsubscribed"],
                "開封率（bot除外）": ctx["open_rate_text"],
                "開封率（bot含む）": ctx["open_rate_including_bots_text"],
                "クリック率": ctx["click_rate_text"],
                "クリックスルー率": ctx["click_through_rate_text"],
                "配信停止率": ctx["unsub_rate_text"],
            }
            actual_fields = {
                "送付日": strip_literal_prefix(display("送付日")),
                "配信数": parse_sheet_int(display("配信数")),
                "開封数（bot除外）": parse_sheet_int(display("開封数（bot除外）")),
                "開封数（bot含む）": parse_sheet_int(display("開封数（bot含む）")),
                "クリック数": parse_sheet_int(display("クリック数")),
                "配信停止数": parse_sheet_int(display("配信停止数")),
                "開封率（bot除外）": strip_literal_prefix(display("開封率（bot除外）")),
                "開封率（bot含む）": strip_literal_prefix(display("開封率（bot含む）")),
                "クリック率": strip_literal_prefix(display("クリック率")),
                "クリックスルー率": strip_literal_prefix(display("クリックスルー率")),
                "配信停止率": strip_literal_prefix(display("配信停止率")),
            }

            for field_name, actual_value in actual_fields.items():
                expected_value = expected_fields[field_name]
                if actual_value != expected_value:
                    issue = {
                        "code": "field_mismatch",
                        "field": field_name,
                        "course": course,
                        "sheet_row": row_idx + 1,
                        "email_id": email_id,
                        "email_name": ctx["email_name"],
                        "actual": actual_value,
                        "expected": expected_value,
                    }
                    if email_id in provisional_email_ids:
                        provisional_field_mismatches.append(issue)
                        continue
                    if field_name in VOLATILE_FIELD_NAMES:
                        volatile_field_mismatches.append(issue)
                        continue
                    issues.append(issue)
                    issue_counter[issue["code"]] += 1
                    field_counter[field_name] += 1
                    per_course_issue_counts[course][issue["code"]] += 1

            actual_cv = parse_sheet_int(display("CV数")) if display("CV数") else 0
            actual_breakdown = strip_literal_prefix(display("CV内訳"))
            actual_cv_blank = display("CV数") == "" and display("CV内訳") == ""

            if email_id in blocked_email_ids:
                if not actual_cv_blank:
                    issue = {
                        "code": "unsafe_ga4_not_blank",
                        "course": course,
                        "sheet_row": row_idx + 1,
                        "email_id": email_id,
                        "email_name": ctx["email_name"],
                        "actual_cv": actual_cv,
                        "actual_breakdown": actual_breakdown,
                    }
                    issues.append(issue)
                    issue_counter[issue["code"]] += 1
                    per_course_issue_counts[course][issue["code"]] += 1
            else:
                if display("CV数") == "":
                    issue = {
                        "code": "safe_ga4_blank",
                        "course": course,
                        "sheet_row": row_idx + 1,
                        "email_id": email_id,
                        "email_name": ctx["email_name"],
                        "expected_cv": ctx["ga4_key_events"],
                        "expected_breakdown": ctx["ga4_breakdown"],
                    }
                    issues.append(issue)
                    issue_counter[issue["code"]] += 1
                    per_course_issue_counts[course][issue["code"]] += 1
                else:
                    if actual_cv != ctx["ga4_key_events"]:
                        issue = {
                            "code": "ga4_cv_mismatch",
                            "course": course,
                            "sheet_row": row_idx + 1,
                            "email_id": email_id,
                            "email_name": ctx["email_name"],
                            "actual": actual_cv,
                            "expected": ctx["ga4_key_events"],
                        }
                        issues.append(issue)
                        issue_counter[issue["code"]] += 1
                        per_course_issue_counts[course][issue["code"]] += 1
                    if actual_breakdown != ctx["ga4_breakdown"]:
                        issue = {
                            "code": "ga4_breakdown_mismatch",
                            "course": course,
                            "sheet_row": row_idx + 1,
                            "email_id": email_id,
                            "email_name": ctx["email_name"],
                            "actual": actual_breakdown,
                            "expected": ctx["ga4_breakdown"],
                        }
                        issues.append(issue)
                        issue_counter[issue["code"]] += 1
                        per_course_issue_counts[course][issue["code"]] += 1

        missing_live_ids = sorted(set(source_ids_by_course[course]) - set(live_email_ids))
        for email_id in missing_live_ids:
            issue = {
                "code": "missing_live_row",
                "course": course,
                "email_id": email_id,
                "email_name": source_contexts[email_id]["email_name"],
                "send_date": source_contexts[email_id]["send_date_text"],
            }
            issues.append(issue)
            issue_counter[issue["code"]] += 1
            per_course_issue_counts[course][issue["code"]] += 1

    payload = {
        "generated_at": now_jst_iso(),
        "month": args.month,
        "spreadsheet_id": args.spreadsheet_id,
        "checked_rows": checked_rows,
        "per_course_rows": per_course_rows,
        "blocked_ga4_email_count": len(blocked_email_ids),
        "blocked_ga4_issue_summary": {code: len(rows) for code, rows in blocked_issues.items()},
        "hubspot_only_review_count": len(hubspot_only_rows),
        "management_unmatched_review_count": len(management_unmatched_rows),
        "volatile_field_mismatch_count": len(volatile_field_mismatches),
        "volatile_field_mismatches": volatile_field_mismatches,
        "provisional_field_mismatch_count": len(provisional_field_mismatches),
        "provisional_field_mismatches": provisional_field_mismatches,
        "issue_summary": dict(issue_counter),
        "field_mismatch_summary": dict(field_counter),
        "per_course_issue_summary": {course: dict(counter) for course, counter in per_course_issue_counts.items()},
        "issues": issues,
    }
    write_json(args.output, payload)
    print(f"checked_rows={checked_rows} issue_count={len(issues)}")
    print(f"output={os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
