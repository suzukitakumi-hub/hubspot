#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
from collections import Counter
from typing import Dict, List, Tuple

from hubspot_course_sheet_guardrails import (
    COURSE_SHEET_COLS,
    COURSE_SHEET_HEADER,
    COURSE_SHEET_INDEX,
    DEFAULT_GA4_MAP_MAX_AGE_MINUTES,
    DEFAULT_PROVISIONAL_DAYS,
    DEFAULT_SERVICE_ACCOUNT_JSON,
    DEFAULT_SPREADSHEET_ID,
    JST,
    TARGET_COURSES,
    derive_ga4_map_manifest_path,
    derive_validation_report_path,
    header_matches_expected,
    now_jst,
    now_jst_iso,
    normalize_header_row,
    read_worksheet_matrix,
    snapshot_sha256,
    staging_tab_title,
    strip_literal_prefix,
    write_json,
)
from update_test_hubspot_course_tabs import (
    HubSpotClient,
    classify_segment,
    detect_course,
    detect_course_for_unregistered,
    extract_campaign_ids,
    hubspot_email_url,
    load_management_index,
    resolve_management_row,
    month_bounds_utc,
    parse_published_at_jst,
    parse_published_at_jst_dt,
    pick_send_datetime_raw,
    safe_num,
    to_iso_utc,
    validate_ga4_map_bundle,
)


EMAIL_FETCH_MARGIN_DAYS = 31
HYPERLINK_EMAIL_ID_RE = re.compile(r"/details/(\d+)/performance")
GENERIC_DUPLICATE_EVENTS = {"generate_lead", "form_submit", "form_start"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate hidden staging tabs for the HubSpot course KPI sheet.")
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
        help="Manifest JSON created alongside --ga4-map-csv.",
    )
    parser.add_argument(
        "--max-ga4-map-age-minutes",
        type=int,
        default=DEFAULT_GA4_MAP_MAX_AGE_MINUTES,
        help="Refuse validation when the GA4 map bundle is older than this threshold.",
    )
    parser.add_argument(
        "--provisional-days",
        type=int,
        default=DEFAULT_PROVISIONAL_DAYS,
        help="Block promotion for emails sent within the last N days.",
    )
    parser.add_argument("--email-type", default="BATCH_EMAIL")
    parser.add_argument(
        "--output",
        default="",
        help="Write validation JSON to this path. Defaults to hubspot_course_sheet_validation_<month>.json",
    )
    return parser.parse_args()


def load_ga4_map(path: str) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            email_id = str(row.get("email_id", "")).strip()
            if email_id:
                out[email_id] = row
    return out


def parse_sheet_int(value: str) -> int:
    text = strip_literal_prefix(value).replace(",", "").strip()
    if not text:
        return 0
    return int(float(text))


def rate_pct_2dp_text_for_validation(num: float, den: float) -> str:
    if den <= 0:
        return "0.00%"
    return f"{(num / den) * 100.0:.2f}%"


def dedupe_preserving_order(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        if value not in seen:
            out.append(value)
            seen.add(value)
    return out


def event_name_to_japanese_for_validation(event_name: str) -> str:
    ev = (event_name or "").strip()
    ev_lower = ev.lower()
    if ev == "generate_lead":
        return "資料請求"
    if ev == "purchase":
        return "購入"
    if ev == "form_submit":
        return "フォーム送信"
    if ev == "form_start":
        return "フォーム開始"
    if re.match(r"^cv_sales_event_([a-z0-9]+)$", ev_lower):
        return "営業特別イベント"
    if re.match(r"^cv_marketing_event_([a-z0-9]+)$", ev_lower):
        return "マーケティング特別イベント"
    if re.match(r"^cv_seminar_reservation_([a-z0-9]+)$", ev_lower):
        return "セミナー予約"
    if re.match(r"^cv_counseling_reservation_([a-z0-9]+)$", ev_lower):
        return "個別相談予約"
    if re.match(r"^cv_online_trial_([a-z0-9]+)$", ev_lower):
        return "オンライン体験"
    if re.match(r"^cv_document_request_([a-z0-9]+)$", ev_lower):
        return "資料請求"
    if re.match(r"^cv_contact_([a-z0-9]+)$", ev_lower):
        return "お問い合わせ"
    if re.match(r"^cv_credit_assessment_([a-z0-9]+)$", ev_lower):
        return "単位評価"
    if "営業特別イベント" in ev:
        return "営業特別イベント"
    if "マーケティング特別イベント" in ev:
        return "マーケティング特別イベント"
    if "セミナー予約" in ev:
        return "セミナー予約"
    if "個別相談" in ev:
        return "個別相談予約"
    if "見積" in ev:
        return "見積り依頼"
    if "資料請求" in ev:
        return "資料請求"
    if "オンライン" in ev and "体験" in ev:
        return "オンライン体験"
    if ev.startswith("CV_"):
        return ev.replace("CV_", "", 1)
    if ev.startswith("cv_"):
        return ev.replace("cv_", "", 1)
    return ev


def parse_cv_breakdown_for_validation(raw: str, cv_total: int | None = None) -> str:
    if not raw:
        return ""
    pairs: List[Tuple[str, int]] = []
    for part in [x.strip() for x in raw.split("|") if x.strip()]:
        if ":" not in part:
            continue
        name, count = part.rsplit(":", 1)
        try:
            parsed_count = int(float(count.strip()))
        except Exception:
            continue
        pairs.append((name.strip(), parsed_count))

    if not pairs:
        return ""

    has_specific = any(name.lower().startswith("cv_") for name, _ in pairs)
    if has_specific:
        pairs = [(name, n) for name, n in pairs if name not in GENERIC_DUPLICATE_EVENTS]

    bucket: Dict[str, int] = {}
    for name, count in pairs:
        label = event_name_to_japanese_for_validation(name)
        bucket[label] = bucket.get(label, 0) + count

    if not bucket:
        if cv_total and cv_total > 0:
            return f"その他キーイベント：{cv_total}"
        return ""

    sum_after = sum(bucket.values())
    if cv_total is not None and cv_total > sum_after:
        bucket["その他キーイベント"] = bucket.get("その他キーイベント", 0) + (cv_total - sum_after)

    lines = [f"{name}：{count}" for name, count in sorted(bucket.items(), key=lambda item: (-item[1], item[0]))]
    return "\n".join(lines)


def build_month_window(month: str) -> Tuple[dt.datetime, dt.datetime, dt.datetime, dt.datetime]:
    start_utc, end_utc = month_bounds_utc(month)
    month_start_jst = dt.datetime.strptime(month + "-01", "%Y-%m-%d").replace(tzinfo=JST)
    if month_start_jst.month == 12:
        month_end_jst = month_start_jst.replace(year=month_start_jst.year + 1, month=1)
    else:
        month_end_jst = month_start_jst.replace(month=month_start_jst.month + 1)
    return start_utc, end_utc, month_start_jst, month_end_jst


def fetch_list_map(client: HubSpotClient, emails: List[dict]) -> Dict[str, dict]:
    legacy_lists = client.fetch_all_legacy_lists()
    list_map = dict(legacy_lists)
    all_ref_ids = set()
    for email in emails:
        to = email.get("to") or {}
        all_ref_ids.update(str(x) for x in ((to.get("contactLists") or {}).get("include") or []) if x is not None)
        all_ref_ids.update(str(x) for x in ((to.get("contactIlsLists") or {}).get("include") or []) if x is not None)

    unknown_ids = sorted(x for x in all_ref_ids if x and x not in list_map)
    for list_id in unknown_ids:
        row = client.fetch_crm_list_by_id(list_id)
        if row:
            list_map[list_id] = row
    return list_map


def build_source_contexts(
    client: HubSpotClient,
    month: str,
    email_type: str,
    ga4_map: Dict[str, dict],
    provisional_days: int,
    service_account_json: str,
) -> Tuple[Dict[str, dict], Dict[str, List[str]], List[dict], List[dict], List[dict]]:
    start_utc, end_utc, month_start_jst, month_end_jst = build_month_window(month)
    fetch_start_iso = to_iso_utc(start_utc - dt.timedelta(days=EMAIL_FETCH_MARGIN_DAYS))
    fetch_end_iso = to_iso_utc(end_utc + dt.timedelta(days=EMAIL_FETCH_MARGIN_DAYS))
    emails = client.fetch_marketing_emails(fetch_start_iso, fetch_end_iso, include_stats=True, email_type=email_type)
    list_map = fetch_list_map(client, emails)
    management_index = load_management_index(service_account_json, month)
    provisional_cutoff = now_jst() - dt.timedelta(days=provisional_days)

    contexts: Dict[str, dict] = {}
    ids_by_course: Dict[str, List[str]] = {course: [] for course in TARGET_COURSES}
    provisional_rows: List[dict] = []
    hubspot_only_rows: List[dict] = []
    matched_management_uids: set[str] = set()

    for email in emails:
        send_raw = pick_send_datetime_raw(email)
        send_dt = parse_published_at_jst_dt(send_raw)
        if not send_dt:
            continue
        if not (month_start_jst <= send_dt < month_end_jst):
            continue

        email_id = str(email.get("id", "")).strip()
        if not email_id:
            continue
        raw_email_name = email.get("name", "") or ""
        subject = email.get("subject", "") or ""

        to = email.get("to") or {}
        include_contact_lists = (to.get("contactLists") or {}).get("include") or []
        include_ils_lists = (to.get("contactIlsLists") or {}).get("include") or []
        include_ids = sorted({str(x) for x in include_contact_lists + include_ils_lists if x is not None and str(x) != ""})
        list_names = dedupe_preserving_order(
            [list_map[list_id]["name"] for list_id in include_ids if list_id in list_map and list_map[list_id].get("name")]
        )

        management_row, management_match_reason = resolve_management_row(
            management_index=management_index,
            email_id=email_id,
            raw_email_name=raw_email_name,
            subject=subject,
            send_dt=send_dt,
        )
        if not management_row:
            email_name = raw_email_name
            course = detect_course_for_unregistered(raw_email_name, list_names, subject)
            if course not in TARGET_COURSES:
                hubspot_only_rows.append(
                    {
                        "email_id": email_id,
                        "send_date": parse_published_at_jst(send_raw),
                        "subject": subject,
                        "raw_email_name": raw_email_name,
                        "estimated_course": detect_course(raw_email_name, list_names, subject) or "要確認",
                        "segment": classify_segment(raw_email_name, list_names),
                        "list_names_text": " | ".join(list_names),
                        "internal_ids_text": ",".join(extract_campaign_ids(email)),
                        "reason": management_match_reason,
                    }
                )
                continue
        else:
            email_name = management_row.get("link_key") or raw_email_name
            course = detect_course(email_name, list_names, subject)
            if course not in TARGET_COURSES:
                course = detect_course_for_unregistered(raw_email_name, list_names, subject)
            if course not in TARGET_COURSES:
                hubspot_only_rows.append(
                    {
                        "email_id": email_id,
                        "send_date": parse_published_at_jst(send_raw),
                        "subject": subject,
                        "raw_email_name": raw_email_name,
                        "estimated_course": "要確認",
                        "segment": classify_segment(email_name, list_names),
                        "list_names_text": " | ".join(list_names),
                        "internal_ids_text": ",".join(extract_campaign_ids(email)),
                        "reason": "講座判定不能",
                    }
                )
                continue
            matched_management_uids.add(management_row["management_uid"])

        if course not in TARGET_COURSES:
            continue

        stats = email.get("stats") or {}
        counters = stats.get("counters") or {}
        delivered = int(safe_num(counters.get("delivered")))
        opened = int(safe_num(counters.get("open")))
        clicked = int(safe_num(counters.get("click")))
        unsubscribed = int(safe_num(counters.get("unsubscribed")))

        internal_ids = extract_campaign_ids(email)
        internal_ids_text = ",".join(internal_ids)
        opened_including_bots = client.fetch_unique_opened_recipients_including_bots(internal_ids)

        ga4_row = ga4_map.get(email_id, {})
        ga4_key_events = int(float(ga4_row.get("ga4_keyEvents") or 0))
        ga4_breakdown = parse_cv_breakdown_for_validation(ga4_row.get("ga4_cv_event_breakdown") or "", ga4_key_events)

        context = {
            "email_id": email_id,
            "course": course,
            "segment": classify_segment(email_name, list_names),
            "send_dt": send_dt,
            "send_date_text": parse_published_at_jst(send_raw),
            "subject": subject,
            "email_name": email_name,
            "internal_ids_text": internal_ids_text,
            "list_names_text": " | ".join(list_names),
            "delivered": delivered,
            "opened": opened,
            "opened_including_bots": opened_including_bots,
            "clicked": clicked,
            "unsubscribed": unsubscribed,
            "open_rate_text": rate_pct_2dp_text_for_validation(opened, delivered),
            "open_rate_including_bots_text": rate_pct_2dp_text_for_validation(opened_including_bots, delivered),
            "click_rate_text": rate_pct_2dp_text_for_validation(clicked, delivered),
            "click_through_rate_text": rate_pct_2dp_text_for_validation(clicked, opened),
            "unsub_rate_text": rate_pct_2dp_text_for_validation(unsubscribed, delivered),
            "ga4_key_events": ga4_key_events,
            "ga4_breakdown": ga4_breakdown,
            "hubspot_url": hubspot_email_url(email_id),
            "matched_session_manual_ad_content": ga4_row.get("matched_sessionManualAdContent", "") or "",
            "matched_key_candidates_count": int(float(ga4_row.get("matched_key_candidates_count") or 0)),
            "hubspot_detail_fetch_error": ga4_row.get("hubspot_detail_fetch_error", "") or "",
        }
        contexts[email_id] = context
        ids_by_course[course].append(email_id)

        if send_dt > provisional_cutoff:
            provisional_rows.append(
                {
                    "email_id": email_id,
                    "course": course,
                    "send_date": context["send_date_text"],
                    "email_name": email_name,
                    "days_since_send": round((now_jst() - send_dt).total_seconds() / 86400.0, 2),
                }
            )

    for course in TARGET_COURSES:
        ids_by_course[course].sort(key=lambda email_id: contexts[email_id]["send_date_text"])
    provisional_rows.sort(key=lambda row: (row["send_date"], row["course"], row["email_id"]))
    hubspot_only_rows.sort(key=lambda row: (row["send_date"], row["estimated_course"], row["email_id"]))

    management_unmatched_rows: List[dict] = []
    for row in management_index["all_rows"]:
        if row["management_uid"] in matched_management_uids:
            continue
        email_name = row.get("link_key") or ""
        management_unmatched_rows.append(
            {
                "management_uid": row["management_uid"],
                "send_date": row.get("send_date", ""),
                "send_time": row.get("send_time", ""),
                "email_name": email_name,
                "subject": row.get("subject", ""),
                "estimated_course": detect_course(email_name, [], row.get("subject", "")) or "要確認",
                "source_tab": row.get("source_tab", ""),
                "status": row.get("status", ""),
            }
        )
    management_unmatched_rows.sort(key=lambda row: (row["send_date"], row["send_time"], row["email_name"]))
    return contexts, ids_by_course, provisional_rows, hubspot_only_rows, management_unmatched_rows


def add_issue(issues: List[dict], code: str, message: str, **extra) -> None:
    issue = {"code": code, "message": message}
    issue.update(extra)
    issues.append(issue)


def parse_hyperlink_email_id(formula_value: str) -> str:
    match = HYPERLINK_EMAIL_ID_RE.search(formula_value or "")
    return match.group(1) if match else ""


def normalize_row(row: List[str]) -> List[str]:
    current = list(row[:COURSE_SHEET_COLS])
    if len(current) < COURSE_SHEET_COLS:
        current.extend([""] * (COURSE_SHEET_COLS - len(current)))
    return current


def read_staging_snapshot(spreadsheet) -> Tuple[Dict[str, List[List[str]]], Dict[str, List[List[str]]], Dict[str, dict], Dict[str, List[str]], List[dict]]:
    from gspread import WorksheetNotFound

    display_snapshot: Dict[str, List[List[str]]] = {}
    formula_snapshot: Dict[str, List[List[str]]] = {}
    rows_by_email_id: Dict[str, dict] = {}
    ids_by_course: Dict[str, List[str]] = {course: [] for course in TARGET_COURSES}
    issues: List[dict] = []

    for course in TARGET_COURSES:
        title = staging_tab_title(course)
        try:
            worksheet = spreadsheet.worksheet(title)
        except WorksheetNotFound:
            display_snapshot[title] = []
            formula_snapshot[title] = []
            add_issue(issues, "missing_staging_tab", f"Staging tab {title} was not found.", course=course, tab=title)
            continue

        display_values = read_worksheet_matrix(worksheet)
        formula_values = read_worksheet_matrix(worksheet, value_render_option="FORMULA")
        display_snapshot[title] = display_values
        formula_snapshot[title] = formula_values

        if not display_values:
            add_issue(issues, "empty_staging_tab", f"Staging tab {title} is empty.", course=course, tab=title)
            continue
        if not header_matches_expected(display_values[0]):
            add_issue(
                issues,
                "header_mismatch",
                f"Staging tab {title} header does not match the expected schema.",
                course=course,
                tab=title,
                expected=COURSE_SHEET_HEADER,
                actual=normalize_header_row(display_values[0]),
            )
            continue

        previous_send_date = ""
        row_count = max(len(display_values), len(formula_values))
        for index in range(1, row_count):
            display_row = normalize_row(display_values[index] if index < len(display_values) else [])
            formula_row = normalize_row(formula_values[index] if index < len(formula_values) else [])
            if not any(display_row) and not any(formula_row):
                continue

            sheet_row = index + 1
            email_id = parse_hyperlink_email_id(formula_row[COURSE_SHEET_INDEX["メール件名（HubSpotリンク）"]])
            if not email_id:
                add_issue(
                    issues,
                    "missing_email_id_formula",
                    "Could not parse HubSpot email id from the hyperlink formula.",
                    course=course,
                    tab=title,
                    sheet_row=sheet_row,
                    formula=formula_row[COURSE_SHEET_INDEX["メール件名（HubSpotリンク）"]],
                )
                continue
            if email_id in rows_by_email_id:
                add_issue(
                    issues,
                    "duplicate_staging_email",
                    "The same HubSpot email id appears more than once in staging.",
                    course=course,
                    tab=title,
                    sheet_row=sheet_row,
                    email_id=email_id,
                )
                continue

            send_date = strip_literal_prefix(display_row[COURSE_SHEET_INDEX["送付日"]])
            if previous_send_date and send_date < previous_send_date:
                add_issue(
                    issues,
                    "staging_not_sorted",
                    "Rows are not sorted by send date ascending.",
                    course=course,
                    tab=title,
                    sheet_row=sheet_row,
                    previous_send_date=previous_send_date,
                    current_send_date=send_date,
                )
            previous_send_date = send_date

            rows_by_email_id[email_id] = {
                "course": course,
                "tab": title,
                "sheet_row": sheet_row,
                "display_row": display_row,
                "formula_row": formula_row,
            }
            ids_by_course[course].append(email_id)

    return display_snapshot, formula_snapshot, rows_by_email_id, ids_by_course, issues


def compare_staging_to_source(
    month: str,
    staging_rows: Dict[str, dict],
    source_contexts: Dict[str, dict],
) -> List[dict]:
    issues: List[dict] = []
    source_ids = set(source_contexts.keys())
    staging_ids = set(staging_rows.keys())

    for email_id in sorted(source_ids - staging_ids):
        context = source_contexts[email_id]
        add_issue(
            issues,
            "missing_staging_row",
            "A source email is missing from staging.",
            course=context["course"],
            email_id=email_id,
            send_date=context["send_date_text"],
            email_name=context["email_name"],
        )

    for email_id in sorted(staging_ids - source_ids):
        row = staging_rows[email_id]
        add_issue(
            issues,
            "unexpected_staging_row",
            "A staging row does not exist in the source email set.",
            course=row["course"],
            email_id=email_id,
            sheet_row=row["sheet_row"],
        )

    def compare_field(email_id: str, field: str, actual, expected, row_info: dict) -> None:
        if actual == expected:
            return
        add_issue(
            issues,
            "field_mismatch",
            f"{field} does not match the source value.",
            course=row_info["course"],
            email_id=email_id,
            sheet_row=row_info["sheet_row"],
            field=field,
            actual=actual,
            expected=expected,
        )

    for email_id in sorted(source_ids & staging_ids):
        context = source_contexts[email_id]
        row_info = staging_rows[email_id]
        display_row = row_info["display_row"]
        formula_row = row_info["formula_row"]

        compare_field(email_id, "対象月", strip_literal_prefix(display_row[COURSE_SHEET_INDEX["対象月"]]), month, row_info)
        compare_field(email_id, "講座", strip_literal_prefix(display_row[COURSE_SHEET_INDEX["講座"]]), context["course"], row_info)
        compare_field(email_id, "送付日", strip_literal_prefix(display_row[COURSE_SHEET_INDEX["送付日"]]), context["send_date_text"], row_info)
        compare_field(email_id, "メール件名", display_row[COURSE_SHEET_INDEX["メール件名（HubSpotリンク）"]], context["subject"], row_info)
        compare_field(email_id, "メール内部名", display_row[COURSE_SHEET_INDEX["メール内部名"]], context["email_name"], row_info)
        compare_field(
            email_id,
            "INTERNAL HUBSPOT IDS",
            strip_literal_prefix(display_row[COURSE_SHEET_INDEX["INTERNAL HUBSPOT IDS"]]),
            context["internal_ids_text"],
            row_info,
        )
        compare_field(
            email_id,
            "送付リスト",
            strip_literal_prefix(display_row[COURSE_SHEET_INDEX["送付リスト"]]),
            context["list_names_text"],
            row_info,
        )
        compare_field(email_id, "配信数", parse_sheet_int(display_row[COURSE_SHEET_INDEX["配信数"]]), context["delivered"], row_info)
        compare_field(
            email_id,
            "開封数（bot除外）",
            parse_sheet_int(display_row[COURSE_SHEET_INDEX["開封数（bot除外）"]]),
            context["opened"],
            row_info,
        )
        compare_field(
            email_id,
            "開封数（bot含む）",
            parse_sheet_int(display_row[COURSE_SHEET_INDEX["開封数（bot含む）"]]),
            context["opened_including_bots"],
            row_info,
        )
        compare_field(email_id, "クリック数", parse_sheet_int(display_row[COURSE_SHEET_INDEX["クリック数"]]), context["clicked"], row_info)
        compare_field(
            email_id,
            "配信停止数",
            parse_sheet_int(display_row[COURSE_SHEET_INDEX["配信停止数"]]),
            context["unsubscribed"],
            row_info,
        )
        compare_field(
            email_id,
            "開封率（bot除外）",
            strip_literal_prefix(display_row[COURSE_SHEET_INDEX["開封率（bot除外）"]]),
            context["open_rate_text"],
            row_info,
        )
        compare_field(
            email_id,
            "開封率（bot含む）",
            strip_literal_prefix(display_row[COURSE_SHEET_INDEX["開封率（bot含む）"]]),
            context["open_rate_including_bots_text"],
            row_info,
        )
        compare_field(
            email_id,
            "クリック率",
            strip_literal_prefix(display_row[COURSE_SHEET_INDEX["クリック率"]]),
            context["click_rate_text"],
            row_info,
        )
        compare_field(
            email_id,
            "クリックスルー率",
            strip_literal_prefix(display_row[COURSE_SHEET_INDEX["クリックスルー率"]]),
            context["click_through_rate_text"],
            row_info,
        )
        compare_field(
            email_id,
            "配信停止率",
            strip_literal_prefix(display_row[COURSE_SHEET_INDEX["配信停止率"]]),
            context["unsub_rate_text"],
            row_info,
        )
        compare_field(email_id, "CV数", parse_sheet_int(display_row[COURSE_SHEET_INDEX["CV数"]]), context["ga4_key_events"], row_info)
        compare_field(
            email_id,
            "CV内訳",
            strip_literal_prefix(display_row[COURSE_SHEET_INDEX["CV内訳"]]),
            context["ga4_breakdown"],
            row_info,
        )

        hyperlink_formula = formula_row[COURSE_SHEET_INDEX["メール件名（HubSpotリンク）"]]
        if not hyperlink_formula.startswith("=HYPERLINK(") or context["hubspot_url"] not in hyperlink_formula:
            add_issue(
                issues,
                "hyperlink_formula_mismatch",
                "The HubSpot hyperlink formula does not point to the expected email detail URL.",
                course=row_info["course"],
                email_id=email_id,
                sheet_row=row_info["sheet_row"],
                actual=hyperlink_formula,
                expected_url=context["hubspot_url"],
            )

    return issues


def main() -> None:
    args = parse_args()
    token = (args.hubspot_token or "").strip()
    if not token:
        raise SystemExit("HubSpot token is missing. Set HUBSPOT_PAT or pass --hubspot-token.")
    if not os.path.exists(args.service_account_json):
        raise SystemExit(f"Service account json not found: {args.service_account_json}")

    manifest_path = args.ga4_map_manifest or derive_ga4_map_manifest_path(args.ga4_map_csv)
    manifest = validate_ga4_map_bundle(
        csv_path=args.ga4_map_csv,
        manifest_path=manifest_path,
        month=args.month,
        max_age_minutes=args.max_ga4_map_age_minutes,
    )
    ga4_map = load_ga4_map(args.ga4_map_csv)

    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(args.service_account_json, scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(args.spreadsheet_id)

    display_snapshot, formula_snapshot, staging_rows, staging_ids_by_course, issues = read_staging_snapshot(spreadsheet)
    display_snapshot_hash = snapshot_sha256(display_snapshot)
    formula_snapshot_hash = snapshot_sha256(formula_snapshot)

    client = HubSpotClient(token)
    source_contexts, source_ids_by_course, provisional_rows, hubspot_only_rows, management_unmatched_rows = build_source_contexts(
        client=client,
        month=args.month,
        email_type=args.email_type,
        ga4_map=ga4_map,
        provisional_days=args.provisional_days,
        service_account_json=args.service_account_json,
    )

    relevant_source_ids = set(source_contexts.keys())
    ga4_missing_rows = sorted(
        email_id
        for email_id in relevant_source_ids
        if not source_contexts[email_id].get("matched_session_manual_ad_content")
    )
    for email_id in ga4_missing_rows:
        context = source_contexts[email_id]
        add_issue(
            issues,
            "missing_ga4_map_row",
            "The GA4 map has no matched sessionManualAdContent for this source email.",
            course=context["course"],
            email_id=email_id,
            email_name=context["email_name"],
        )

    compare_issues = compare_staging_to_source(args.month, staging_rows, source_contexts)
    issues.extend(compare_issues)

    for course in TARGET_COURSES:
        if len(staging_ids_by_course[course]) != len(source_ids_by_course[course]):
            add_issue(
                issues,
                "course_row_count_mismatch",
                "Staging row count differs from the source row count for this course tab.",
                course=course,
                staging_rows=len(staging_ids_by_course[course]),
                source_rows=len(source_ids_by_course[course]),
            )

    if provisional_rows:
        for row in provisional_rows:
            add_issue(
                issues,
                "provisional_send_date",
                "This email was sent within the provisional window and cannot be auto-promoted.",
                **row,
            )

    manifest_duplicate_keys = [
        row for row in manifest.get("duplicate_keys", []) if any(email.get("email_id") in relevant_source_ids for email in row.get("emails", []))
    ]
    for row in manifest_duplicate_keys:
        add_issue(
            issues,
            "duplicate_session_manual_ad_content",
            "The same GA4 sessionManualAdContent maps to multiple target emails.",
            session_manual_ad_content=row.get("sessionManualAdContent"),
            email_count=row.get("email_count"),
            emails=row.get("emails", []),
        )

    manifest_multiple_candidate_emails = [
        row for row in manifest.get("multiple_candidate_emails", []) if row.get("email_id") in relevant_source_ids
    ]
    for row in manifest_multiple_candidate_emails:
        add_issue(
            issues,
            "multiple_candidate_email",
            "A target email matched multiple GA4 key candidates and is ambiguous.",
            email_id=row.get("email_id"),
            course=row.get("course"),
            email_name=row.get("email_name"),
            matched_key_candidates_count=row.get("matched_key_candidates_count"),
            matched_session_manual_ad_content=row.get("matched_sessionManualAdContent"),
        )

    detail_fetch_errors = []
    for email_id in sorted(relevant_source_ids):
        row = ga4_map.get(email_id, {})
        error_text = (row.get("hubspot_detail_fetch_error") or "").strip()
        if error_text:
            detail_fetch_errors.append(
                {
                    "email_id": email_id,
                    "course": source_contexts[email_id]["course"],
                    "email_name": source_contexts[email_id]["email_name"],
                    "error": error_text,
                }
            )
    for row in detail_fetch_errors:
        add_issue(
            issues,
            "hubspot_detail_fetch_error",
            "HubSpot email detail fetch failed when building the GA4 map.",
            **row,
        )

    issue_counter = Counter(issue["code"] for issue in issues)
    output_path = args.output or derive_validation_report_path(args.month)
    payload = {
        "status": "pass" if not issues else "fail",
        "generated_at": now_jst_iso(),
        "month": args.month,
        "spreadsheet_id": args.spreadsheet_id,
        "ga4_map_csv_path": os.path.abspath(args.ga4_map_csv),
        "ga4_map_manifest_path": os.path.abspath(manifest_path),
        "ga4_map_csv_sha256": manifest.get("detail_csv_sha256", ""),
        "staging_display_snapshot_sha256": display_snapshot_hash,
        "staging_formula_snapshot_sha256": formula_snapshot_hash,
        "provisional_days": args.provisional_days,
        "provisional_cutoff": (now_jst() - dt.timedelta(days=args.provisional_days)).isoformat(),
        "per_course_source_rows": {course: len(source_ids_by_course[course]) for course in TARGET_COURSES},
        "per_course_staging_rows": {course: len(staging_ids_by_course[course]) for course in TARGET_COURSES},
        "source_email_count": len(source_contexts),
        "staging_email_count": len(staging_rows),
        "hubspot_only_review_count": len(hubspot_only_rows),
        "management_unmatched_review_count": len(management_unmatched_rows),
        "relevant_duplicate_key_count": len(manifest_duplicate_keys),
        "relevant_multiple_candidate_email_count": len(manifest_multiple_candidate_emails),
        "relevant_detail_fetch_error_count": len(detail_fetch_errors),
        "provisional_email_count": len(provisional_rows),
        "blocking_issue_count": len(issues),
        "blocking_issue_summary": dict(issue_counter),
        "review_issue_summary": {
            "hubspot_only_rows": len(hubspot_only_rows),
            "management_unmatched_rows": len(management_unmatched_rows),
        },
        "hubspot_only_rows": hubspot_only_rows,
        "management_unmatched_rows": management_unmatched_rows,
        "issues": issues,
    }
    write_json(output_path, payload)

    print(f"status={payload['status']} blocking_issue_count={len(issues)}")
    print(f"output={os.path.abspath(output_path)}")
    print(f"staging_formula_snapshot_sha256={formula_snapshot_hash}")

    if issues:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
