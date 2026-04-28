#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

from hubspot_course_sheet_guardrails import (
    DEFAULT_GA4_MAP_MAX_AGE_MINUTES,
    DEFAULT_PROVISIONAL_DAYS,
    DEFAULT_SERVICE_ACCOUNT_JSON,
    DEFAULT_SPREADSHEET_ID,
    DEFAULT_VALIDATION_REPORT_MAX_AGE_MINUTES,
    derive_ga4_map_manifest_path,
    derive_validation_report_path,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safely write, validate, and promote the HubSpot course KPI sheet.")
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
    parser.add_argument("--ga4-property-id", default=os.environ.get("GA4_PROPERTY_ID", "249786227"))
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
        "--validation-report",
        default="",
        help="Path for the validation report JSON.",
    )
    parser.add_argument(
        "--max-validation-report-age-minutes",
        type=int,
        default=DEFAULT_VALIDATION_REPORT_MAX_AGE_MINUTES,
    )
    parser.add_argument("--skip-promote", action="store_true", default=False)
    parser.add_argument("--skip-ga4-map-refresh", action="store_true", default=False)
    return parser.parse_args()

def redact_command(command: list[str]) -> str:
    redacted: list[str] = []
    skip_next = False
    for index, part in enumerate(command):
        if skip_next:
            skip_next = False
            continue
        if part == "--hubspot-token" and index + 1 < len(command):
            redacted.extend([part, "<redacted>"])
            skip_next = True
            continue
        redacted.append(part)
    return " ".join(redacted)


def run_step(
    name: str,
    command: list[str],
    allowed_exit_codes: tuple[int, ...] = (0,),
    attempts: int = 1,
    retry_sleep_seconds: int = 30,
) -> int:
    last_returncode = 0
    for attempt in range(1, attempts + 1):
        print(f"step={name}")
        print(f"attempt={attempt}/{attempts}")
        print("command=" + redact_command(command))
        completed = subprocess.run(command, check=False)
        last_returncode = completed.returncode
        if completed.returncode in allowed_exit_codes:
            return completed.returncode
        if attempt < attempts:
            sleep_seconds = retry_sleep_seconds * attempt
            print(f"retrying_step={name} sleep_seconds={sleep_seconds} returncode={completed.returncode}")
            time.sleep(sleep_seconds)
    raise subprocess.CalledProcessError(last_returncode, command)


def main() -> None:
    args = parse_args()
    token = (args.hubspot_token or "").strip()
    if not token:
        raise SystemExit("HubSpot token is missing. Set HUBSPOT_PAT or pass --hubspot-token.")
    if not os.path.exists(args.service_account_json):
        raise SystemExit(f"Service account json not found: {args.service_account_json}")

    ga4_manifest_path = args.ga4_map_manifest or derive_ga4_map_manifest_path(args.ga4_map_csv)
    validation_report_path = args.validation_report or derive_validation_report_path(args.month)
    ga4_map_cmd = [
        sys.executable,
        "map_ga4_cv_to_hubspot_emails.py",
        "--month",
        args.month,
        "--hubspot-token",
        token,
        "--service-account-json",
        args.service_account_json,
        "--ga4-property-id",
        args.ga4_property_id,
    ]

    writer_cmd = [
        sys.executable,
        "update_test_hubspot_course_tabs.py",
        "--month",
        args.month,
        "--spreadsheet-id",
        args.spreadsheet_id,
        "--service-account-json",
        args.service_account_json,
        "--ga4-map-csv",
        args.ga4_map_csv,
        "--ga4-map-manifest",
        ga4_manifest_path,
        "--max-ga4-map-age-minutes",
        str(args.max_ga4_map_age_minutes),
        "--email-type",
        args.email_type,
        "--hubspot-token",
        token,
    ]
    validator_cmd = [
        sys.executable,
        "validate_hubspot_course_staging.py",
        "--month",
        args.month,
        "--spreadsheet-id",
        args.spreadsheet_id,
        "--service-account-json",
        args.service_account_json,
        "--ga4-map-csv",
        args.ga4_map_csv,
        "--ga4-map-manifest",
        ga4_manifest_path,
        "--max-ga4-map-age-minutes",
        str(args.max_ga4_map_age_minutes),
        "--provisional-days",
        str(args.provisional_days),
        "--email-type",
        args.email_type,
        "--output",
        validation_report_path,
        "--hubspot-token",
        token,
    ]
    promote_cmd = [
        sys.executable,
        "promote_hubspot_course_staging.py",
        "--month",
        args.month,
        "--spreadsheet-id",
        args.spreadsheet_id,
        "--service-account-json",
        args.service_account_json,
        "--validation-report",
        validation_report_path,
        "--max-validation-report-age-minutes",
        str(args.max_validation_report_age_minutes),
    ]

    if not args.skip_ga4_map_refresh:
        run_step("build_ga4_map", ga4_map_cmd, attempts=2)

    run_step("write_staging", writer_cmd, attempts=3)
    validate_returncode = run_step("validate_staging", validator_cmd, allowed_exit_codes=(0, 1), attempts=3)
    if not os.path.exists(validation_report_path):
        raise SystemExit(
            f"Validation report was not created after validate_staging. "
            f"Promotion aborted. validation_returncode={validate_returncode} path={validation_report_path}"
        )

    if args.skip_promote:
        print("promotion=skipped")
        return

    run_step("promote_live", promote_cmd, attempts=2)


if __name__ == "__main__":
    main()
