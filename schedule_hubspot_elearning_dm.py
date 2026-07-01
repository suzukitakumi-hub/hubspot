#!/usr/bin/env python3
"""Clone the latest e-learning DM email and send it through HubSpot."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://api.hubapi.com"
PORTAL_ID = "39827439"
JST = ZoneInfo("Asia/Tokyo")
UTC = timezone.utc
EMAIL_NAME_PREFIX = "eラーニング動画体験会誘致DM"
DEFAULT_SEND_TIME_JST = "18:00"

SESSION = requests.Session()
SESSION.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=5,
            connect=5,
            read=5,
            status=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=None,
            raise_on_status=False,
        )
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Copy the latest published HubSpot e-learning DM and publish the "
            "copy at the JST send time."
        )
    )
    parser.add_argument("--apply", action="store_true", help="Actually clone/update and publish the HubSpot email.")
    parser.add_argument(
        "--source-email-id",
        default=os.environ.get("HUBSPOT_ELEARNING_SOURCE_EMAIL_ID", "").strip(),
        help="Optional source email ID. Defaults to the latest published matching email.",
    )
    parser.add_argument(
        "--send-date-jst",
        default="",
        help="YYYY-MM-DD. Defaults to the current date in Japan.",
    )
    parser.add_argument(
        "--send-time-jst",
        default=DEFAULT_SEND_TIME_JST,
        help=f"HH:MM in Japan time. Defaults to {DEFAULT_SEND_TIME_JST}.",
    )
    parser.add_argument(
        "--target-name",
        default="",
        help="Optional target internal name. Defaults to eラーニング動画体験会誘致DMYYYYMMDD.",
    )
    parser.add_argument(
        "--minimum-lead-minutes",
        type=int,
        default=0,
        help="Deprecated. Kept for workflow compatibility.",
    )
    parser.add_argument(
        "--allow-early-minutes",
        type=int,
        default=5,
        help="Allow publishing this many minutes before the nominal JST send time.",
    )
    parser.add_argument(
        "--allow-late-minutes",
        type=int,
        default=180,
        help="Allow publishing this many minutes after the nominal JST send time.",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory for the JSON run summary.",
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


def hubspot_token() -> str:
    value = os.environ.get("HUBSPOT_PAT", "").strip()
    if not value:
        raise SystemExit("HUBSPOT_PAT is missing")
    return value


def headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {hubspot_token()}", "Content-Type": "application/json"}


def request_json(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    response = SESSION.request(method, BASE_URL + path, headers=headers(), timeout=120, **kwargs)
    response.raise_for_status()
    if not response.content:
        return {}
    return response.json()


def fetch_email(email_id: str) -> dict[str, Any]:
    return request_json("GET", f"/marketing/v3/emails/{email_id}")


def patch_email(email_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    request_json("PATCH", f"/marketing/v3/emails/{email_id}", json=payload)
    return fetch_email(email_id)


def publish_email(email_id: str) -> None:
    response = SESSION.post(
        BASE_URL + f"/marketing/v3/emails/{email_id}/publish",
        headers=headers(),
        json={},
        timeout=120,
    )
    response.raise_for_status()


def clone_email(source_id: str, clone_name: str) -> str:
    payload = {"id": source_id, "cloneName": clone_name, "language": "ja"}
    response = request_json("POST", "/marketing/v3/emails/clone", json=payload)
    return str(response["id"])


def list_marketing_emails() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    after: str | None = None
    while True:
        params: dict[str, Any] = {"limit": 100}
        if after:
            params["after"] = after
        payload = request_json("GET", "/marketing/v3/emails", params=params)
        results.extend(payload.get("results", []) or [])
        after = payload.get("paging", {}).get("next", {}).get("after")
        if not after:
            return results


def parse_hubspot_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=UTC)
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def sort_key(email: dict[str, Any]) -> tuple[datetime, datetime, str]:
    return (
        parse_hubspot_datetime(email.get("publishDate") or email.get("publishedAt")),
        parse_hubspot_datetime(email.get("updatedAt")),
        str(email.get("id", "")),
    )


def find_source_email(all_emails: list[dict[str, Any]], source_email_id: str) -> dict[str, Any]:
    if source_email_id:
        source = fetch_email(source_email_id)
        if not str(source.get("id")):
            raise RuntimeError(f"Source email not found: {source_email_id}")
        if not str(source.get("name", "")).startswith(EMAIL_NAME_PREFIX):
            raise RuntimeError(
                f"Source email name does not start with {EMAIL_NAME_PREFIX}: {source.get('name')}"
            )
        return source

    candidates = [
        email
        for email in all_emails
        if str(email.get("name", "")).startswith(EMAIL_NAME_PREFIX)
        and email.get("state") == "PUBLISHED"
        and email.get("isPublished") is True
        and not email.get("archived")
    ]
    if not candidates:
        raise RuntimeError(f"No published HubSpot email found with prefix: {EMAIL_NAME_PREFIX}")
    source_id = str(sorted(candidates, key=sort_key, reverse=True)[0]["id"])
    return fetch_email(source_id)


def find_existing_targets(all_emails: list[dict[str, Any]], target_name: str) -> list[dict[str, Any]]:
    return [
        email
        for email in all_emails
        if email.get("name") == target_name and not email.get("archived")
    ]


def stable_hash(value: Any) -> str:
    serialized = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def scheduled_datetime(send_date_jst: str, send_time_jst: str) -> datetime:
    if send_date_jst:
        target_date = date.fromisoformat(send_date_jst)
    else:
        target_date = datetime.now(JST).date()

    hour, minute = [int(part) for part in send_time_jst.split(":", 1)]
    return datetime.combine(target_date, time(hour, minute), tzinfo=JST)


def hubspot_utc_string(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def default_target_name(scheduled_jst: datetime) -> str:
    return EMAIL_NAME_PREFIX + scheduled_jst.strftime("%Y%m%d")


def email_summary(email: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(email.get("id", "")),
        "name": email.get("name"),
        "subject": email.get("subject"),
        "state": email.get("state"),
        "isPublished": email.get("isPublished"),
        "publishDate": email.get("publishDate"),
        "publishedAt": email.get("publishedAt"),
        "updatedAt": email.get("updatedAt"),
        "sendOnPublish": email.get("sendOnPublish"),
        "editUrl": f"https://app.hubspot.com/email/{PORTAL_ID}/edit/{email.get('id')}/content",
    }


def clone_verification(source: dict[str, Any], target: dict[str, Any], expected_name: str) -> dict[str, bool]:
    return {
        "nameOk": target.get("name") == expected_name,
        "subjectSameAsSource": target.get("subject") == source.get("subject"),
        "statePublishedOrProcessing": target.get("state") in {"PRE_PROCESSING", "PROCESSING", "PUBLISHED"},
        "isPublishedTrue": target.get("isPublished") is True,
        "sendOnPublishTrue": target.get("sendOnPublish") is True,
        "contentSameAsSource": stable_hash(target.get("content")) == stable_hash(source.get("content")),
        "recipientsSameAsSource": stable_hash(target.get("to")) == stable_hash(source.get("to")),
        "fromSameAsSource": stable_hash(target.get("from")) == stable_hash(source.get("from")),
        "subscriptionSameAsSource": stable_hash(target.get("subscriptionDetails"))
        == stable_hash(source.get("subscriptionDetails")),
    }


def ensure_send_window(scheduled_jst: datetime, allow_early_minutes: int, allow_late_minutes: int) -> None:
    now_jst = datetime.now(JST)
    early_seconds = (scheduled_jst - now_jst).total_seconds()
    late_seconds = (now_jst - scheduled_jst).total_seconds()
    if early_seconds > allow_early_minutes * 60 or late_seconds > allow_late_minutes * 60:
        raise RuntimeError(
            "Current time is outside the allowed send window: "
            f"now={now_jst.isoformat()} scheduled={scheduled_jst.isoformat()} "
            f"allowEarlyMinutes={allow_early_minutes} allowLateMinutes={allow_late_minutes}"
        )


def publish_existing_or_new_draft(email_id: str, source: dict[str, Any], target_name: str) -> dict[str, Any]:
    now_utc = hubspot_utc_string(datetime.now(UTC))
    target = patch_email(
        email_id,
        {
            "name": target_name,
            "publishDate": now_utc,
            "sendOnPublish": True,
        },
    )
    publish_email(str(target["id"]))
    return fetch_email(str(target["id"]))


def main() -> None:
    root = Path(__file__).resolve().parent
    load_dotenv(root / ".env.production")
    args = parse_args()

    scheduled_jst = scheduled_datetime(args.send_date_jst, args.send_time_jst)
    scheduled_utc = hubspot_utc_string(scheduled_jst)
    target_name = args.target_name.strip() or default_target_name(scheduled_jst)

    all_emails = list_marketing_emails()
    source = find_source_email(all_emails, args.source_email_id.strip())
    existing_targets = find_existing_targets(all_emails, target_name)

    if len(existing_targets) > 1:
        raise RuntimeError(f"Multiple existing target emails found for {target_name}: {existing_targets}")

    output: dict[str, Any] = {
        "apply": args.apply,
        "scheduledJst": scheduled_jst.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "scheduledUtc": scheduled_utc,
        "targetName": target_name,
        "source": email_summary(source),
        "existingTargets": [email_summary(email) for email in existing_targets],
        "action": "dry_run",
        "after": None,
        "checks": {},
        "allChecksOk": False,
    }

    if not args.apply:
        output["allChecksOk"] = True
        write_output(args.output_dir, output)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return

    ensure_send_window(scheduled_jst, args.allow_early_minutes, args.allow_late_minutes)

    if existing_targets:
        target = fetch_email(str(existing_targets[0]["id"]))
        if target.get("isPublished") is True or target.get("state") == "PUBLISHED":
            output["action"] = "already_published"
            output["after"] = email_summary(target)
            output["checks"] = {
                "targetExists": True,
                "alreadyPublished": True,
                "contentSameAsSource": stable_hash(target.get("content")) == stable_hash(source.get("content")),
                "recipientsSameAsSource": stable_hash(target.get("to")) == stable_hash(source.get("to")),
            }
            output["allChecksOk"] = all(output["checks"].values())
        elif target.get("state") == "DRAFT":
            output["action"] = "reuse_existing_draft_and_publish"
            target = publish_existing_or_new_draft(str(target["id"]), source, target_name)
            output["after"] = email_summary(target)
            output["checks"] = clone_verification(source, target, target_name)
            output["allChecksOk"] = all(output["checks"].values())
        else:
            raise RuntimeError(f"Existing target is not editable: {email_summary(target)}")
    else:
        output["action"] = "clone_and_publish"
        new_id = clone_email(str(source["id"]), target_name)
        target = publish_existing_or_new_draft(new_id, source, target_name)
        output["after"] = email_summary(target)
        output["checks"] = clone_verification(source, target, target_name)
        output["allChecksOk"] = all(output["checks"].values())

    write_output(args.output_dir, output)
    print(json.dumps(output, ensure_ascii=False, indent=2))
    if not output["allChecksOk"]:
        sys.exit(2)


def write_output(output_dir: str, output: dict[str, Any]) -> None:
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(JST).strftime("%Y%m%d_%H%M%S")
    target_slug = output["targetName"].replace("/", "_").replace("\\", "_")
    out_path = path / f"hubspot_elearning_dm_{target_slug}_{timestamp}.json"
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
