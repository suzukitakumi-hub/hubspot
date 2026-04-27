#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
import time
from collections import defaultdict
from typing import Dict, List, Tuple

import requests
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    Metric,
    RunReportRequest,
)
from google.oauth2 import service_account

from hubspot_course_sheet_guardrails import (
    derive_ga4_map_manifest_path,
    now_jst_iso,
    sha256_file,
    write_json,
)


BASE_URL = "https://api.hubapi.com"
JST = dt.timezone(dt.timedelta(hours=9))
EMAIL_FETCH_MARGIN_DAYS = 31
COURSE_KEYS = ["CIA", "CISA", "CFE", "IFRS", "USCPA", "MBA"]
EXTRA_CONVERSION_EVENTS = {"generate_lead", "purchase", "form_submit", "form_start"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Map GA4 email CV(keyEvents) to HubSpot marketing emails by ad content key."
    )
    parser.add_argument("--month", default="2026-03", help="Target month in YYYY-MM")
    parser.add_argument("--hubspot-token", default=os.environ.get("HUBSPOT_PAT", ""))
    parser.add_argument("--ga4-property-id", default=os.environ.get("GA4_PROPERTY_ID", "249786227"))
    parser.add_argument(
        "--service-account-json",
        default=os.environ.get(
            "GOOGLE_SERVICE_ACCOUNT_JSON",
            "C:/Users/suzuki.takumi/Desktop/AI/Hubspot/micro-environs-470717-j2-58800aec23bb.json",
        ),
    )
    parser.add_argument("--type", default="BATCH_EMAIL", help="HubSpot email type filter")
    parser.add_argument("--output-prefix", default="ga4_hubspot_cv_map")
    return parser.parse_args()


def month_bounds(month_yyyy_mm: str) -> Tuple[dt.datetime, dt.datetime]:
    start = dt.datetime.strptime(month_yyyy_mm + "-01", "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def to_iso_utc(value: dt.datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def to_jst_str(value: str) -> str:
    if not value:
        return ""
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return value


def pick_send_datetime_raw(email_obj: dict) -> str:
    # HubSpot UIの送信日時に合わせるため、publishDateを優先する。
    return email_obj.get("publishDate") or email_obj.get("publishedAt") or ""


def parse_send_datetime_jst(value: str) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(JST)
    except Exception:
        return None


def detect_course(email_name: str, subject: str) -> str:
    text = f"{email_name} {subject}".upper()
    for key in COURSE_KEYS:
        if key in text:
            return key
    return "OTHER"


class HubSpotClient:
    def __init__(self, token: str):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

    def fetch_monthly_emails(self, published_after: str, published_before: str, email_type: str) -> List[dict]:
        url = f"{BASE_URL}/marketing/v3/emails"
        params = {
            "limit": 100,
            "isPublished": "true",
            "includeStats": "true",
            "publishedAfter": published_after,
            "publishedBefore": published_before,
            "sort": "-publishedAt",
            "type": email_type,
        }
        out: List[dict] = []
        while True:
            resp = self.session.get(url, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            out.extend(data.get("results", []))
            after = ((data.get("paging") or {}).get("next") or {}).get("after")
            if not after:
                break
            params["after"] = after
        return out

    def fetch_email_detail(self, email_id: str) -> dict:
        url = f"{BASE_URL}/marketing/v3/emails/{email_id}"
        last_error = None
        for _ in range(3):
            resp = self.session.get(url, timeout=120)
            if resp.status_code < 500:
                resp.raise_for_status()
                return resp.json()
            last_error = f"{resp.status_code} {resp.text[:200]}"
            time.sleep(1.2)
        raise RuntimeError(f"Failed to fetch email detail {email_id}: {last_error}")


def ga4_run_paged_report(
    client: BetaAnalyticsDataClient,
    property_id: str,
    dimensions: List[str],
    metrics: List[str],
    start_date: str,
    end_date: str,
    filter_expression: FilterExpression,
    page_size: int = 10000,
) -> List[dict]:
    rows_out: List[dict] = []
    offset = 0
    while True:
        req = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=name) for name in dimensions],
            metrics=[Metric(name=name) for name in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=filter_expression,
            limit=page_size,
            offset=offset,
        )
        resp = client.run_report(req)
        if not resp.rows:
            break
        for row in resp.rows:
            rows_out.append(
                {
                    "dimensions": [x.value for x in row.dimension_values],
                    "metrics": [x.value for x in row.metric_values],
                }
            )
        if len(resp.rows) < page_size:
            break
        offset += page_size
    return rows_out


def write_csv(path: str, header: List[str], rows: List[List]) -> None:
    import csv

    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    token = (args.hubspot_token or "").strip()
    if not token:
        raise SystemExit("HubSpot token not found. Set HUBSPOT_PAT or pass --hubspot-token.")
    if not os.path.exists(args.service_account_json):
        raise SystemExit(f"Service account json not found: {args.service_account_json}")

    month = args.month
    start_dt, end_dt = month_bounds(month)
    start_iso = to_iso_utc(start_dt - dt.timedelta(days=EMAIL_FETCH_MARGIN_DAYS))
    end_iso = to_iso_utc(end_dt + dt.timedelta(days=EMAIL_FETCH_MARGIN_DAYS))
    start_date, end_date = start_dt.strftime("%Y-%m-%d"), (end_dt - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    month_start_jst = dt.datetime.strptime(month + "-01", "%Y-%m-%d").replace(tzinfo=JST)
    if month_start_jst.month == 12:
        month_end_jst = month_start_jst.replace(year=month_start_jst.year + 1, month=1)
    else:
        month_end_jst = month_start_jst.replace(month=month_start_jst.month + 1)

    hs = HubSpotClient(token)
    emails = hs.fetch_monthly_emails(start_iso, end_iso, args.type)

    details: Dict[str, str] = {}
    detail_fetch_errors: Dict[str, str] = {}
    email_rows: List[dict] = []
    for email in emails:
        email_id = str(email.get("id", ""))
        if not email_id:
            continue
        send_raw = pick_send_datetime_raw(email)
        send_dt = parse_send_datetime_jst(send_raw)
        if not send_dt or not (month_start_jst <= send_dt < month_end_jst):
            continue
        try:
            detail = hs.fetch_email_detail(email_id)
            detail_text = json.dumps(detail, ensure_ascii=False)
            details[email_id] = detail_text
        except Exception as exc:
            details[email_id] = ""
            detail_fetch_errors[email_id] = str(exc)

        counters = ((email.get("stats") or {}).get("counters") or {})
        email_rows.append(
            {
                "email_id": email_id,
                "email_name": email.get("name", "") or "",
                "subject": email.get("subject", "") or "",
                "published_at_jst": send_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "course": detect_course(email.get("name", "") or "", email.get("subject", "") or ""),
                "delivered": int(counters.get("delivered") or 0),
                "open": int(counters.get("open") or 0),
                "click": int(counters.get("click") or 0),
                "unsubscribed": int(counters.get("unsubscribed") or 0),
            }
        )

    creds = service_account.Credentials.from_service_account_file(args.service_account_json)
    ga = BetaAnalyticsDataClient(credentials=creds)
    medium_email_filter = FilterExpression(
        filter=Filter(
            field_name="sessionMedium",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.EXACT,
                value="email",
            ),
        )
    )

    key_event_rows = ga4_run_paged_report(
        client=ga,
        property_id=args.ga4_property_id,
        dimensions=["sessionManualAdContent"],
        metrics=["keyEvents"],
        start_date=start_date,
        end_date=end_date,
        filter_expression=medium_email_filter,
    )

    adcontent_keyevents: Dict[str, float] = {}
    for row in key_event_rows:
        ad_content = row["dimensions"][0]
        value = float(row["metrics"][0] or 0)
        adcontent_keyevents[ad_content] = adcontent_keyevents.get(ad_content, 0.0) + value

    event_rows = ga4_run_paged_report(
        client=ga,
        property_id=args.ga4_property_id,
        dimensions=["sessionManualAdContent", "eventName"],
        metrics=["keyEvents"],
        start_date=start_date,
        end_date=end_date,
        filter_expression=medium_email_filter,
    )

    adcontent_events: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in event_rows:
        ad_content, event_name = row["dimensions"]
        count = float(row["metrics"][0] or 0)
        if count <= 0:
            continue
        ev_lower = event_name.lower()
        if ev_lower.startswith("cv_") or event_name in EXTRA_CONVERSION_EVENTS:
            adcontent_events[ad_content][event_name] += count

    candidate_keys = [
        key
        for key in adcontent_keyevents.keys()
        if key and key != "(not set)" and re.fullmatch(r"[A-Za-z0-9_-]{2,80}", key)
    ]

    output_rows: List[List] = []
    mapped_keys = set()

    for row in sorted(email_rows, key=lambda x: (x["published_at_jst"], x["email_name"])):
        email_id = row["email_id"]
        text = details.get(email_id, "")
        hits = [key for key in candidate_keys if key in text]
        chosen = ""
        if len(hits) == 1:
            chosen = hits[0]
        elif len(hits) > 1:
            chosen = sorted(hits, key=lambda x: adcontent_keyevents.get(x, 0.0), reverse=True)[0]
        if chosen:
            mapped_keys.add(chosen)

        events = adcontent_events.get(chosen, {})
        event_breakdown = " | ".join(
            [f"{name}:{int(count)}" for name, count in sorted(events.items(), key=lambda x: (-x[1], x[0]))]
        )

        output_rows.append(
            [
                month,
                row["course"],
                row["published_at_jst"],
                row["email_id"],
                row["email_name"],
                row["subject"],
                chosen,
                len(hits),
                int(adcontent_keyevents.get(chosen, 0.0)),
                event_breakdown,
                row["delivered"],
                row["open"],
                row["click"],
                row["unsubscribed"],
                detail_fetch_errors.get(email_id, ""),
            ]
        )

    unmapped_key_rows = []
    for key, key_events in sorted(adcontent_keyevents.items(), key=lambda x: (-x[1], x[0])):
        if key in {"", "(not set)"}:
            continue
        if key not in mapped_keys:
            events = adcontent_events.get(key, {})
            breakdown = " | ".join(
                [f"{name}:{int(count)}" for name, count in sorted(events.items(), key=lambda x: (-x[1], x[0]))]
            )
            unmapped_key_rows.append([month, key, int(key_events), breakdown])

    detail_path = f"{args.output_prefix}_{month}_email_map.csv"
    unmapped_path = f"{args.output_prefix}_{month}_unmapped_keys.csv"
    write_csv(
        detail_path,
        [
            "month",
            "course",
            "published_at_jst",
            "email_id",
            "email_name",
            "subject",
            "matched_sessionManualAdContent",
            "matched_key_candidates_count",
            "ga4_keyEvents",
            "ga4_cv_event_breakdown",
            "hubspot_delivered",
            "hubspot_open",
            "hubspot_click",
            "hubspot_unsubscribed",
            "hubspot_detail_fetch_error",
        ],
        output_rows,
    )
    write_csv(
        unmapped_path,
        ["month", "sessionManualAdContent", "ga4_keyEvents", "ga4_cv_event_breakdown"],
        unmapped_key_rows,
    )

    mapped_email_count = sum(1 for row in output_rows if row[6])
    mapped_with_key_events = sum(1 for row in output_rows if row[6] and row[8] > 0)
    key_to_emails: Dict[str, List[dict]] = defaultdict(list)
    multiple_candidate_emails: List[dict] = []
    for row in output_rows:
        chosen_key = row[6]
        if chosen_key:
            key_to_emails[chosen_key].append(
                {
                    "email_id": row[3],
                    "email_name": row[4],
                    "course": row[1],
                    "ga4_keyEvents": row[8],
                }
            )
        if int(row[7] or 0) > 1:
            multiple_candidate_emails.append(
                {
                    "email_id": row[3],
                    "email_name": row[4],
                    "course": row[1],
                    "matched_key_candidates_count": row[7],
                    "matched_sessionManualAdContent": row[6],
                }
            )

    duplicate_keys = []
    for key, emails_for_key in sorted(key_to_emails.items()):
        if len(emails_for_key) > 1:
            duplicate_keys.append(
                {
                    "sessionManualAdContent": key,
                    "email_count": len(emails_for_key),
                    "emails": emails_for_key,
                }
            )

    manifest_path = derive_ga4_map_manifest_path(detail_path)
    manifest = {
        "month": month,
        "generated_at": now_jst_iso(),
        "detail_csv_path": os.path.abspath(detail_path),
        "detail_csv_sha256": sha256_file(detail_path),
        "unmapped_csv_path": os.path.abspath(unmapped_path),
        "unmapped_csv_sha256": sha256_file(unmapped_path),
        "hubspot_email_count": len(output_rows),
        "mapped_email_count": mapped_email_count,
        "mapped_emails_with_ga4_keyevents": mapped_with_key_events,
        "ga4_total_keys": len(candidate_keys),
        "ga4_unmapped_keys": len(unmapped_key_rows),
        "detail_fetch_error_count": len(detail_fetch_errors),
        "duplicate_key_count": len(duplicate_keys),
        "duplicate_keys": duplicate_keys,
        "multiple_candidate_email_count": len(multiple_candidate_emails),
        "multiple_candidate_emails": multiple_candidate_emails,
    }
    write_json(manifest_path, manifest)

    print(f"month={month}")
    print(f"hubspot_emails={len(output_rows)}")
    print(f"mapped_emails={mapped_email_count}")
    print(f"mapped_emails_with_ga4_keyevents={mapped_with_key_events}")
    print(f"ga4_total_keys={len(candidate_keys)}")
    print(f"ga4_unmapped_keys={len(unmapped_key_rows)}")
    print(f"detail_fetch_errors={len(detail_fetch_errors)}")
    print(f"output_detail=./{detail_path}")
    print(f"output_unmapped=./{unmapped_path}")
    print(f"output_manifest=./{manifest_path}")


if __name__ == "__main__":
    main()
