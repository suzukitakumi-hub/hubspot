#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import time

from hubspot_course_sheet_guardrails import (
    DEFAULT_GA4_MAP_MAX_AGE_MINUTES,
    DEFAULT_SERVICE_ACCOUNT_JSON,
    DEFAULT_SPREADSHEET_ID,
    derive_ga4_map_manifest_path,
    sha256_file,
)
from hubspot_course_source_snapshot import (
    build_source_contexts_snapshot,
    load_ga4_map_for_snapshot,
    save_source_snapshot,
)
from update_test_hubspot_course_tabs import HubSpotClient, validate_ga4_map_bundle


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a reusable source snapshot for course sheet updates.")
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
    )
    parser.add_argument("--provisional-days", type=int, default=0)
    parser.add_argument("--email-type", default="BATCH_EMAIL")
    parser.add_argument("--output", default="", help="Output source snapshot JSON path.")
    return parser.parse_args()


def main() -> None:
    started = time.perf_counter()
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
    ga4_map = load_ga4_map_for_snapshot(args.ga4_map_csv)
    client = HubSpotClient(token)
    snapshot = build_source_contexts_snapshot(
        client=client,
        month=args.month,
        email_type=args.email_type,
        ga4_map=ga4_map,
        provisional_days=args.provisional_days,
        service_account_json=args.service_account_json,
    )
    snapshot["spreadsheet_id"] = args.spreadsheet_id
    snapshot["ga4_map_csv_path"] = os.path.abspath(args.ga4_map_csv)
    snapshot["ga4_map_manifest_path"] = os.path.abspath(manifest_path)
    snapshot["ga4_map_csv_sha256"] = sha256_file(args.ga4_map_csv)

    output = args.output or f"hubspot_course_source_snapshot_{args.month}.json"
    save_source_snapshot(output, snapshot)
    elapsed = time.perf_counter() - started

    print(f"source_snapshot={os.path.abspath(output)}")
    print(f"source_email_count={snapshot['source_email_count']}")
    print(f"per_course_source_rows={snapshot['per_course_source_rows']}")
    print(f"duration_seconds={elapsed:.3f}")


if __name__ == "__main__":
    main()
