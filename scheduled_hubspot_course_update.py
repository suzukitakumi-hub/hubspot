#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from hubspot_course_sheet_guardrails import DEFAULT_SPREADSHEET_ID, JST, retry_sleep_seconds_for_attempt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scheduled production updater for HubSpot course KPI sheets.")
    parser.add_argument(
        "--months",
        nargs="*",
        default=[],
        help="YYYY-MM values to update. Defaults to the current JST month.",
    )
    parser.add_argument(
        "--include-previous-month-until-day",
        type=int,
        default=7,
        help="Also update the previous month while the current JST day is <= this value.",
    )
    parser.add_argument("--spreadsheet-id", default=os.environ.get("HUBSPOT_COURSE_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID))
    parser.add_argument("--provisional-days", type=int, default=0)
    parser.add_argument("--email-type", default=os.environ.get("HUBSPOT_EMAIL_TYPE", "BATCH_EMAIL"))
    parser.add_argument("--log-dir", default=os.environ.get("HUBSPOT_COURSE_UPDATE_LOG_DIR", "logs/course_sheet_updates"))
    parser.add_argument("--skip-promote", action="store_true", default=False)
    parser.add_argument(
        "--post-promote-cooldown-seconds",
        type=int,
        default=int(os.environ.get("HUBSPOT_COURSE_POST_PROMOTE_COOLDOWN_SECONDS", "70")),
        help="Cooldown before live audit after promotion to avoid Sheets API per-minute quotas.",
    )
    parser.add_argument(
        "--audit-issue-retry-attempts",
        type=int,
        default=int(os.environ.get("HUBSPOT_COURSE_AUDIT_ISSUE_RETRY_ATTEMPTS", "3")),
        help="Retry live audit when it completes but still reports issues, allowing Sheets propagation to settle.",
    )
    parser.add_argument(
        "--audit-issue-retry-sleep-seconds",
        type=int,
        default=int(os.environ.get("HUBSPOT_COURSE_AUDIT_ISSUE_RETRY_SLEEP_SECONDS", "90")),
        help="Base cooldown between live-audit issue retries.",
    )
    return parser.parse_args()


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def month_add(month: str, delta: int) -> str:
    year, mon = [int(part) for part in month.split("-")]
    mon += delta
    while mon <= 0:
        year -= 1
        mon += 12
    while mon > 12:
        year += 1
        mon -= 12
    return f"{year:04d}-{mon:02d}"


def default_months(include_previous_until_day: int) -> list[str]:
    today = dt.datetime.now(JST).date()
    current = today.strftime("%Y-%m")
    months = [current]
    if include_previous_until_day > 0 and today.day <= include_previous_until_day:
        months.insert(0, month_add(current, -1))
    return months


def run_logged(command: list[str], log_file: Path, attempts: int = 1, retry_sleep_seconds: int = 90) -> None:
    last_returncode = 0
    for attempt in range(1, attempts + 1):
        started = time.perf_counter()
        with log_file.open("a", encoding="utf-8") as handle:
            handle.write("\n$ " + " ".join(command) + "\n")
            handle.write(f"command_attempt={attempt}/{attempts}\n")
            handle.write(f"command_started_at={dt.datetime.now(JST).isoformat()}\n")
            handle.flush()
            completed = subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT, check=False)
            elapsed = time.perf_counter() - started
            handle.write(f"command_finished_at={dt.datetime.now(JST).isoformat()}\n")
            handle.write(f"command_returncode={completed.returncode} duration_seconds={elapsed:.3f}\n")
        last_returncode = completed.returncode
        if completed.returncode == 0:
            return
        if attempt < attempts:
            sleep_seconds = retry_sleep_seconds_for_attempt(
                attempt,
                retry_sleep_seconds,
                max_sleep_seconds=300,
                jitter_seconds=3,
            )
            with log_file.open("a", encoding="utf-8") as handle:
                handle.write(
                    f"retrying_command attempt={attempt}/{attempts} "
                    f"sleep_seconds={sleep_seconds:.1f} returncode={completed.returncode}\n"
                )
            time.sleep(sleep_seconds)
    raise subprocess.CalledProcessError(last_returncode, command)


def cooldown(label: str, seconds: int, log_file: Path) -> None:
    if seconds <= 0:
        return
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(f"scheduled_cooldown label={label} sleep_seconds={seconds}\n")
    time.sleep(seconds)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def run_audit_until_clean(
    audit_cmd: list[str],
    audit_report: Path,
    log_file: Path,
    attempts: int,
    retry_sleep_seconds: int,
) -> dict:
    attempts = max(1, attempts)
    for attempt in range(1, attempts + 1):
        run_logged(audit_cmd, log_file, attempts=3, retry_sleep_seconds=90)
        audit = read_json(audit_report)
        issue_count = len(audit.get("issues", []) or [])
        if issue_count == 0 or attempt >= attempts:
            return audit

        sleep_seconds = retry_sleep_seconds_for_attempt(
            attempt,
            retry_sleep_seconds,
            max_sleep_seconds=300,
            jitter_seconds=3,
        )
        with log_file.open("a", encoding="utf-8") as handle:
            handle.write(
                f"retrying_live_audit_for_reported_issues "
                f"attempt={attempt}/{attempts} sleep_seconds={sleep_seconds:.1f} issue_count={issue_count}\n"
            )
        time.sleep(sleep_seconds)
    return read_json(audit_report)


def main() -> None:
    repo_root = Path(__file__).resolve().parent
    os.chdir(repo_root)
    load_dotenv(repo_root / ".env")
    load_dotenv(repo_root / ".env.production")

    args = parse_args()
    months = args.months or default_months(args.include_previous_month_until_day)
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    run_id = dt.datetime.now(JST).strftime("%Y%m%d_%H%M%S")
    run_log = log_dir / f"hubspot_course_update_{run_id}.log"
    summary: dict[str, object] = {
        "run_id": run_id,
        "started_at_jst": dt.datetime.now(JST).isoformat(),
        "months": months,
        "spreadsheet_id": args.spreadsheet_id,
        "results": [],
    }

    if not os.environ.get("HUBSPOT_PAT"):
        raise SystemExit("HUBSPOT_PAT is missing. Set it in .env.production or the scheduler environment.")
    if not os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON"):
        raise SystemExit("GOOGLE_SERVICE_ACCOUNT_JSON is missing. Set it in .env.production or the scheduler environment.")

    try:
        for month in months:
            ga4_map_csv = f"ga4_hubspot_cv_map_{month}_email_map.csv"
            source_snapshot = f"hubspot_course_source_snapshot_{month}.json"
            validation_report = f"hubspot_course_sheet_validation_{month}.json"
            audit_report = f"hubspot_course_sheet_live_audit_{month}.json"
            pipeline_cmd = [
                sys.executable,
                "run_hubspot_course_sheet_pipeline.py",
                "--month",
                month,
                "--spreadsheet-id",
                args.spreadsheet_id,
                "--ga4-map-csv",
                ga4_map_csv,
                "--validation-report",
                validation_report,
                "--source-snapshot-json",
                source_snapshot,
                "--provisional-days",
                str(args.provisional_days),
                "--email-type",
                args.email_type,
            ]
            if args.skip_promote:
                pipeline_cmd.append("--skip-promote")
            run_logged(pipeline_cmd, run_log, attempts=2, retry_sleep_seconds=120)

            if args.skip_promote:
                summary["results"].append(
                    {
                        "month": month,
                        "promotion": "skipped",
                        "audit": "skipped",
                        "validation_report": validation_report,
                    }
                )
                continue

            audit_cmd = [
                sys.executable,
                "audit_live_hubspot_course_sheet.py",
                "--month",
                month,
                "--spreadsheet-id",
                args.spreadsheet_id,
                "--ga4-map-csv",
                ga4_map_csv,
                "--provisional-days",
                str(args.provisional_days),
                "--email-type",
                args.email_type,
                "--output",
                audit_report,
                "--source-snapshot-json",
                source_snapshot,
            ]
            cooldown("before_live_audit", args.post_promote_cooldown_seconds, run_log)
            audit = run_audit_until_clean(
                audit_cmd,
                Path(audit_report),
                run_log,
                attempts=args.audit_issue_retry_attempts,
                retry_sleep_seconds=args.audit_issue_retry_sleep_seconds,
            )
            issue_count = len(audit.get("issues", []) or [])
            summary["results"].append(
                {
                    "month": month,
                    "checked_rows": audit.get("checked_rows"),
                    "issue_count": issue_count,
                    "issue_summary": audit.get("issue_summary", {}),
                    "per_course_rows": audit.get("per_course_rows", {}),
                }
            )
            if issue_count:
                raise SystemExit(f"Audit failed for {month}: issue_count={issue_count}")
    finally:
        summary["finished_at_jst"] = dt.datetime.now(JST).isoformat()
        summary["log_file"] = str(run_log)
        summary_path = log_dir / f"hubspot_course_update_{run_id}.json"
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
