#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import os
from typing import Dict, List, Tuple

from hubspot_course_sheet_guardrails import (
    JST,
    TARGET_COURSES,
    now_jst,
    now_jst_iso,
    parse_iso_datetime,
    write_json,
)
from update_test_hubspot_course_tabs import (
    EMAIL_FETCH_MARGIN_DAYS,
    HubSpotClient,
    as_literal_text,
    classify_segment,
    detect_course,
    detect_course_for_unregistered,
    escape_formula_text,
    extract_campaign_ids,
    hubspot_email_url,
    load_ga4_map,
    load_management_index,
    month_bounds_utc,
    parse_cv_breakdown_to_japanese,
    parse_published_at_jst,
    parse_published_at_jst_dt,
    pick_send_datetime_raw,
    rate_pct_2dp_text,
    resolve_management_row,
    safe_num,
    to_iso_utc,
)


def dedupe_preserving_order(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        if value not in seen:
            out.append(value)
            seen.add(value)
    return out


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


def _serialize_contexts(source_contexts: Dict[str, dict]) -> Dict[str, dict]:
    serialized: Dict[str, dict] = {}
    for email_id, context in source_contexts.items():
        current = dict(context)
        send_dt = current.get("send_dt")
        if isinstance(send_dt, dt.datetime):
            current["send_dt"] = send_dt.isoformat()
        serialized[email_id] = current
    return serialized


def _deserialize_contexts(source_contexts: Dict[str, dict]) -> Dict[str, dict]:
    deserialized: Dict[str, dict] = {}
    for email_id, context in source_contexts.items():
        current = dict(context)
        send_dt = current.get("send_dt")
        if isinstance(send_dt, str) and send_dt:
            current["send_dt"] = parse_iso_datetime(send_dt).astimezone(JST)
        deserialized[email_id] = current
    return deserialized


def build_source_contexts_snapshot(
    *,
    client: HubSpotClient,
    month: str,
    email_type: str,
    ga4_map: Dict[str, dict],
    provisional_days: int,
    service_account_json: str,
) -> dict:
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
        opened_including_bots = client.fetch_unique_opened_recipients_including_bots(internal_ids)

        ga4_row = ga4_map.get(email_id, {})
        ga4_key_events = int(float(ga4_row.get("ga4_keyEvents") or 0))
        ga4_breakdown = parse_cv_breakdown_to_japanese(ga4_row.get("ga4_cv_event_breakdown") or "", ga4_key_events)

        context = {
            "email_id": email_id,
            "course": course,
            "segment": classify_segment(email_name, list_names),
            "send_dt": send_dt,
            "send_date_text": parse_published_at_jst(send_raw),
            "subject": subject,
            "email_name": email_name,
            "internal_ids_text": ",".join(internal_ids),
            "list_names_text": " | ".join(list_names),
            "delivered": delivered,
            "opened": opened,
            "opened_including_bots": opened_including_bots,
            "clicked": clicked,
            "unsubscribed": unsubscribed,
            "open_rate_text": rate_pct_2dp_text(opened, delivered),
            "open_rate_including_bots_text": rate_pct_2dp_text(opened_including_bots, delivered),
            "click_rate_text": rate_pct_2dp_text(clicked, delivered),
            "click_through_rate_text": rate_pct_2dp_text(clicked, opened),
            "unsub_rate_text": rate_pct_2dp_text(unsubscribed, delivered),
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

    return {
        "schema_version": 1,
        "generated_at": now_jst_iso(),
        "month": month,
        "email_type": email_type,
        "source_contexts": _serialize_contexts(contexts),
        "source_ids_by_course": ids_by_course,
        "provisional_rows": provisional_rows,
        "hubspot_only_rows": hubspot_only_rows,
        "management_unmatched_rows": management_unmatched_rows,
        "source_email_count": len(contexts),
        "per_course_source_rows": {course: len(ids_by_course[course]) for course in TARGET_COURSES},
    }


def save_source_snapshot(path: str, snapshot: dict) -> None:
    write_json(path, snapshot)


def load_source_snapshot(path: str, *, month: str | None = None, email_type: str | None = None) -> dict:
    import json

    with open(path, "r", encoding="utf-8") as handle:
        snapshot = json.load(handle)
    if month and snapshot.get("month") != month:
        raise SystemExit(f"Source snapshot month mismatch: expected {month}, got {snapshot.get('month')!r}")
    if email_type and snapshot.get("email_type") != email_type:
        raise SystemExit(f"Source snapshot email_type mismatch: expected {email_type}, got {snapshot.get('email_type')!r}")
    snapshot["source_contexts"] = _deserialize_contexts(snapshot.get("source_contexts", {}))
    return snapshot


def unpack_source_snapshot(snapshot: dict) -> Tuple[Dict[str, dict], Dict[str, List[str]], List[dict], List[dict], List[dict]]:
    return (
        snapshot.get("source_contexts", {}),
        snapshot.get("source_ids_by_course", {course: [] for course in TARGET_COURSES}),
        snapshot.get("provisional_rows", []),
        snapshot.get("hubspot_only_rows", []),
        snapshot.get("management_unmatched_rows", []),
    )


def build_course_rows_from_snapshot(snapshot: dict, month: str) -> Dict[str, List[List]]:
    source_contexts, source_ids_by_course, _, _, _ = unpack_source_snapshot(snapshot)
    course_rows: Dict[str, List[List]] = {course: [] for course in TARGET_COURSES}
    for course in TARGET_COURSES:
        for email_id in source_ids_by_course.get(course, []):
            context = source_contexts[email_id]
            subject_formula = f'=HYPERLINK("{context["hubspot_url"]}","{escape_formula_text(context["subject"])}")'
            course_rows[course].append(
                [
                    as_literal_text(context["send_date_text"]),
                    subject_formula,
                    context["email_name"],
                    int(context["delivered"]),
                    int(context["opened"]),
                    int(context["opened_including_bots"]),
                    int(context["clicked"]),
                    int(context["unsubscribed"]),
                    as_literal_text(context["open_rate_text"]),
                    as_literal_text(context["open_rate_including_bots_text"]),
                    as_literal_text(context["click_rate_text"]),
                    as_literal_text(context["click_through_rate_text"]),
                    as_literal_text(context["unsub_rate_text"]),
                    int(context["ga4_key_events"]),
                    context["ga4_breakdown"],
                    context["list_names_text"],
                    context["internal_ids_text"],
                    month,
                    course,
                ]
            )
    return course_rows


def load_ga4_map_for_snapshot(path: str) -> Dict[str, dict]:
    if not os.path.exists(path):
        raise SystemExit(f"GA4 map csv not found: {path}")
    return load_ga4_map(path)
