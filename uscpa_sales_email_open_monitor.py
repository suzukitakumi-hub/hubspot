import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


BASE_URL = "https://api.hubapi.com"
PORTAL_ID = "39827439"
PAST_LEAD_LIST_ID = "6567"
OUTPUT_DIR = Path("outputs")
OUTPUT_DIR.mkdir(exist_ok=True)

SHEET_HEADERS = [
    "通知日時",
    "担当者",
    "顧客名",
    "メール",
    "ヨミ",
    "前回接触",
    "行動",
    "開封メール件名",
    "送信元",
    "宛先",
    "開封数",
    "HubSpot",
    "CRMメールURL",
    "CRMメールID",
]

ALLOWED_SENDERS = {
    "info@abitus.co.jp",
    "takahashi.shota@abitus.co.jp",
    "sakai.moena@abitus.co.jp",
    "iwasaki@abitus.co.jp",
    "kanoko.nakamura@abitus.co.jp",
    "hirayama@abitus.co.jp",
    "yuki.ren@abitus.co.jp",
    "sugiyama.runo@abitus.co.jp",
    "morimune@abitus.co.jp",
}

ALLOWED_CPA_OWNER_IDS = {
    "1878305994",  # 高橋 菖太
    "87109514",  # 酒井 萌花
    "1345885568",  # 岩﨑 香菜子
    "741220351",  # 中村 佳乃子
    "80584487",  # 平山 弥怜
    "83615897",  # 結城 怜
    "83615896",  # 杉山 琉望
    "1182211497",  # 森宗 峻一
}

CPA_OWNER_EMAILS = {
    "1878305994": "takahashi.shota@abitus.co.jp",
    "87109514": "sakai.moena@abitus.co.jp",
    "1345885568": "iwasaki@abitus.co.jp",
    "741220351": "kanoko.nakamura@abitus.co.jp",
    "80584487": "hirayama@abitus.co.jp",
    "83615897": "yuki.ren@abitus.co.jp",
    "83615896": "sugiyama.runo@abitus.co.jp",
    "1182211497": "morimune@abitus.co.jp",
}

CPA_OWNER_NAMES = {
    "1878305994": "高橋 菖太",
    "87109514": "酒井 萌花",
    "1345885568": "岩﨑 香菜子",
    "741220351": "中村 佳乃子",
    "80584487": "平山 弥怜",
    "83615897": "結城 怜",
    "83615896": "杉山 琉望",
    "1182211497": "森宗 峻一",
}

CONTACT_PROPS = [
    "email",
    "firstname",
    "lastname",
    "sales_staff_cpa",
    "yomi",
    "notes_last_contacted",
    "uscpa_reactivation_last_email_id",
    "uscpa_reactivation_last_notified_at",
]

EMAIL_PROPS = [
    "hs_email_from_email",
    "hs_email_sender_email",
    "hs_email_to_email",
    "hs_email_subject",
    "hs_email_open_count",
    "hs_email_click_count",
    "hs_email_direction",
    "hs_email_status",
    "hs_email_logged_from",
    "hs_timestamp",
    "hs_lastmodifieddate",
]


class HubSpot:
    def __init__(self) -> None:
        token = os.environ.get("HUBSPOT_PAT")
        if not token:
            raise RuntimeError("HUBSPOT_PAT is not set")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

    def request(self, method: str, path: str, **kwargs):
        url = f"{BASE_URL}{path}"
        for attempt in range(5):
            response = self.session.request(method, url, timeout=60, **kwargs)
            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep(2**attempt)
                continue
            if response.status_code >= 400:
                raise RuntimeError(
                    f"{method} {path} failed: {response.status_code} {response.text[:2000]}"
                )
            if response.text:
                return response.json()
            return None
        raise RuntimeError(f"{method} {path} failed after retries")


def parse_args():
    parser = argparse.ArgumentParser(
        description="USCPA過去リード向けCRM営業メール開封Slack通知のHubSpotプロパティ更新処理"
    )
    parser.add_argument("--lookback-hours", type=int, default=48)
    parser.add_argument("--limit-per-sender", type=int, default=100)
    parser.add_argument("--suppression-days", type=int, default=10)
    parser.add_argument("--apply", action="store_true", help="条件一致コンタクトを実際に更新する")
    parser.add_argument(
        "--max-updates",
        type=int,
        default=0,
        help="apply時の最大更新件数。0は無制限。",
    )
    parser.add_argument(
        "--slack-map-json",
        default=os.environ.get("USCPA_SLACK_USER_MAP_JSON", "{}"),
        help='HubSpot owner ID -> Slack mentionのJSON。例: {"875246223":"<@U...>"}',
    )
    parser.add_argument(
        "--delivery",
        choices=["hubspot", "slack", "both"],
        default=os.environ.get("USCPA_NOTIFICATION_DELIVERY", "hubspot"),
        help="通知方法。hubspotはHubSpot標準Slack WF、slackはSlack API/Webhook直送、bothは両方。",
    )
    parser.add_argument(
        "--slack-channel-id",
        default=os.environ.get("USCPA_SLACK_CHANNEL_ID", "C0B3XE1DG4W"),
    )
    parser.add_argument("--slack-bot-token", default=os.environ.get("SLACK_BOT_TOKEN"))
    parser.add_argument("--slack-webhook-url", default=os.environ.get("SLACK_WEBHOOK_URL"))
    parser.add_argument(
        "--sheet-output",
        action="store_true",
        default=os.environ.get("USCPA_SHEET_OUTPUT", "").lower() in {"1", "true", "yes"},
        help="通知済みレコードをGoogleスプレッドシートにも追記する。",
    )
    parser.add_argument(
        "--sheet-spreadsheet-id",
        default=os.environ.get("USCPA_SHEET_SPREADSHEET_ID"),
    )
    parser.add_argument(
        "--google-service-account-json",
        default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        or "micro-environs-470717-j2-58800aec23bb.json",
    )
    return parser.parse_args()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_ms(dt: datetime) -> str:
    return str(int(dt.timestamp() * 1000))


def parse_hs_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.fromtimestamp(int(value) / 1000, timezone.utc)
        except Exception:
            return None


def load_list_member_ids(client: HubSpot) -> set[str]:
    member_ids: set[str] = set()
    after = None
    while True:
        params = {"limit": 250}
        if after:
            params["after"] = after
        data = client.request(
            "GET", f"/crm/v3/lists/{PAST_LEAD_LIST_ID}/memberships", params=params
        )
        for row in data.get("results", []):
            member_ids.add(str(row.get("recordId")))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            return member_ids


def search_open_emails(client: HubSpot, sender: str, since_ms: str, limit: int) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    after = None
    while True:
        payload: dict[str, Any] = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "hs_email_open_count",
                            "operator": "GT",
                            "value": "0",
                        },
                        {
                            "propertyName": "hs_email_from_email",
                            "operator": "EQ",
                            "value": sender,
                        },
                        {
                            "propertyName": "hs_lastmodifieddate",
                            "operator": "GTE",
                            "value": since_ms,
                        },
                    ]
                }
            ],
            "properties": EMAIL_PROPS,
            "limit": min(100, max(1, limit - len(results))),
            "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "DESCENDING"}],
        }
        if after:
            payload["after"] = after
        data = client.request("POST", "/crm/v3/objects/emails/search", json=payload)
        results.extend(data.get("results", []))
        if len(results) >= limit:
            return results[:limit]
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            return results


def batch_email_contact_associations(client: HubSpot, email_ids: list[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for start in range(0, len(email_ids), 100):
        chunk = email_ids[start : start + 100]
        if not chunk:
            continue
        data = client.request(
            "POST",
            "/crm/v4/associations/emails/contacts/batch/read",
            json={"inputs": [{"id": eid} for eid in chunk]},
        )
        for row in data.get("results", []):
            from_id = str(row.get("from", {}).get("id"))
            out[from_id] = [str(item.get("toObjectId")) for item in row.get("to", [])]
    return out


def batch_read_contacts(client: HubSpot, contact_ids: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for start in range(0, len(contact_ids), 100):
        chunk = contact_ids[start : start + 100]
        if not chunk:
            continue
        data = client.request(
            "POST",
            "/crm/v3/objects/contacts/batch/read",
            json={
                "properties": CONTACT_PROPS,
                "inputs": [{"id": cid} for cid in chunk],
            },
        )
        for row in data.get("results", []):
            out[str(row.get("id"))] = row
    return out


def split_recipients(value: str | None) -> list[str]:
    if not value:
        return []
    raw = value.replace(";", ",").replace("\n", ",")
    return [part.strip().lower() for part in raw.split(",") if part.strip()]


def is_internal_recipient_only(to_email: str | None) -> bool:
    recipients = split_recipients(to_email)
    if not recipients:
        return False
    if any(recipient == "39827439@bcc.hubspot.com" for recipient in recipients):
        return True
    return all(recipient.endswith("@abitus.co.jp") for recipient in recipients)


def should_skip_email(email_obj: dict[str, Any]) -> str | None:
    props = email_obj.get("properties", {})
    from_email = (props.get("hs_email_from_email") or "").lower()
    sender_email = (props.get("hs_email_sender_email") or "").lower()
    to_email = props.get("hs_email_to_email")
    if "noreply" in from_email or "noreply" in sender_email:
        return "noreply sender"
    if from_email not in ALLOWED_SENDERS:
        return "sender not allowed"
    if is_internal_recipient_only(to_email):
        return "internal recipient"
    return None


def should_notify_contact(
    contact: dict[str, Any],
    email_id: str,
    past_member_ids: set[str],
    now: datetime,
    suppression_days: int,
) -> str | None:
    contact_id = str(contact.get("id"))
    props = contact.get("properties", {})
    if contact_id not in past_member_ids:
        return "not in past lead list 6567"
    owner_id = str(props.get("sales_staff_cpa") or "")
    if owner_id not in ALLOWED_CPA_OWNER_IDS:
        return "sales_staff_cpa not target owner"
    if str(props.get("uscpa_reactivation_last_email_id") or "") == str(email_id):
        return "same email already notified"
    last_notified = parse_hs_datetime(props.get("uscpa_reactivation_last_notified_at"))
    if last_notified and now - last_notified < timedelta(days=suppression_days):
        return "within suppression window"
    return None


def update_contact_for_notification(
    client: HubSpot,
    contact: dict[str, Any],
    email_obj: dict[str, Any],
    slack_map: dict[str, str],
    now: datetime,
    trigger_hubspot_workflow: bool,
):
    contact_id = str(contact["id"])
    cprops = contact.get("properties", {})
    eprops = email_obj.get("properties", {})
    owner_id = str(cprops.get("sales_staff_cpa") or "")
    email_id = str(email_obj["id"])
    payload = {
        "properties": {
            "uscpa_reactivation_last_email_id": email_id,
            "uscpa_reactivation_last_notified_at": now.isoformat().replace("+00:00", "Z"),
            "uscpa_reactivation_last_email_subject": eprops.get("hs_email_subject") or "",
            "uscpa_reactivation_last_email_from": eprops.get("hs_email_from_email") or "",
            "uscpa_reactivation_last_action_type": "USCPA CRM営業メール開封",
            "uscpa_reactivation_slack_mention": slack_map.get(owner_id, ""),
        }
    }
    if trigger_hubspot_workflow:
        payload["properties"]["uscpa_reactivation_notification_seq"] = (
            f"{email_id}:{int(now.timestamp())}"
        )
    return client.request("PATCH", f"/crm/v3/objects/contacts/{contact_id}", json=payload)


def slack_api_request(token: str, method: str, payload: dict[str, Any]):
    response = requests.post(
        f"https://slack.com/api/{method}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack API {method} failed: {data}")
    return data


def lookup_slack_mentions_by_email(token: str, existing_map: dict[str, str]) -> dict[str, str]:
    slack_map = dict(existing_map)
    for owner_id, email in CPA_OWNER_EMAILS.items():
        if owner_id in slack_map and slack_map[owner_id]:
            continue
        response = requests.get(
            "https://slack.com/api/users.lookupByEmail",
            headers={"Authorization": f"Bearer {token}"},
            params={"email": email},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("ok") and data.get("user", {}).get("id"):
            slack_map[owner_id] = f"<@{data['user']['id']}>"
    return slack_map


def build_slack_text(contact: dict[str, Any], email_obj: dict[str, Any], mention: str) -> str:
    cprops = contact.get("properties", {})
    eprops = email_obj.get("properties", {})
    contact_id = str(contact.get("id"))
    email_id = str(email_obj.get("id"))
    owner_id = str(cprops.get("sales_staff_cpa") or "")
    contact_name = f"{cprops.get('lastname') or ''} {cprops.get('firstname') or ''}".strip()
    contact_url = f"https://app.hubspot.com/contacts/{PORTAL_ID}/contact/{contact_id}"
    email_url = f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-49/{email_id}"
    email_subject = eprops.get("hs_email_subject") or email_id
    linked_contact_name = f"<{contact_url}|{contact_name or contact_id}>"
    linked_email = f"<{email_url}|{email_subject}>"
    return "\n".join(
        [
            "過去リードが再行動しました",
            "",
            f"担当者: {mention}".strip(),
            "行動: USCPA CRM営業メール開封",
            f"顧客名: {linked_contact_name}",
            f"メール: {cprops.get('email') or ''}",
            f"ヨミ: {cprops.get('yomi') or ''}",
            f"前回接触: {cprops.get('notes_last_contacted') or ''}",
            f"開封メール: {linked_email}",
            f"送信元: {eprops.get('hs_email_from_email') or ''}",
        ]
    )


def post_slack_notification(
    contact: dict[str, Any],
    email_obj: dict[str, Any],
    slack_map: dict[str, str],
    channel_id: str,
    bot_token: str | None,
    webhook_url: str | None,
) -> str:
    owner_id = str(contact.get("properties", {}).get("sales_staff_cpa") or "")
    mention = slack_map.get(owner_id, "")
    if not mention:
        raise RuntimeError(f"Slack mention missing for owner_id={owner_id}")
    text = build_slack_text(contact, email_obj, mention)
    if bot_token:
        data = slack_api_request(
            bot_token,
            "chat.postMessage",
            {"channel": channel_id, "text": text, "mrkdwn": True, "unfurl_links": False},
        )
        return f"chat.postMessage:{data.get('ts')}"
    if webhook_url:
        response = requests.post(
            webhook_url,
            json={"text": text, "mrkdwn": True, "unfurl_links": False},
            timeout=30,
        )
        response.raise_for_status()
        if response.text.strip().lower() != "ok":
            raise RuntimeError(f"Slack webhook failed: {response.status_code} {response.text[:500]}")
        return "incoming_webhook:ok"
    raise RuntimeError("Slack delivery requested, but SLACK_BOT_TOKEN or SLACK_WEBHOOK_URL is not set")


def normalize_sheet_title(value: str) -> str:
    title = value.replace("/", "_").replace("\\", "_").replace("?", "_")
    title = title.replace("*", "_").replace("[", "_").replace("]", "_").replace(":", "_")
    return title[:99] or "未設定"


def get_gspread_client(service_account_json: str):
    import gspread

    path = Path(service_account_json)
    if not path.exists():
        raise RuntimeError(f"Google service account json not found: {service_account_json}")
    return gspread.service_account(filename=str(path))


def ensure_worksheet(spreadsheet, title: str):
    try:
        worksheet = spreadsheet.worksheet(title)
    except Exception:
        worksheet = spreadsheet.add_worksheet(title=title, rows=1000, cols=len(SHEET_HEADERS))
    header_range = f"A1:{chr(ord('A') + len(SHEET_HEADERS) - 1)}1"
    values = worksheet.get(header_range)
    if not values or values[0] != SHEET_HEADERS:
        worksheet.update(header_range, [SHEET_HEADERS])
        worksheet.freeze(rows=1)
    return worksheet


def append_sheet_row(
    spreadsheet_id: str | None,
    service_account_json: str,
    notify_time: datetime,
    contact: dict[str, Any],
    email_obj: dict[str, Any],
) -> str:
    if not spreadsheet_id:
        return "sheet skipped: spreadsheet id not set"
    client = get_gspread_client(service_account_json)
    spreadsheet = client.open_by_key(spreadsheet_id)
    cprops = contact.get("properties", {})
    eprops = email_obj.get("properties", {})
    contact_id = str(contact.get("id"))
    owner_id = str(cprops.get("sales_staff_cpa") or "")
    owner_name = CPA_OWNER_NAMES.get(owner_id, owner_id or "担当未設定")
    contact_name = f"{cprops.get('lastname') or ''} {cprops.get('firstname') or ''}".strip()
    contact_url = f"https://app.hubspot.com/contacts/{PORTAL_ID}/contact/{contact_id}"
    email_id = str(email_obj.get("id"))
    email_url = f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-49/{email_id}"
    worksheet = ensure_worksheet(spreadsheet, normalize_sheet_title(owner_name.replace(" ", "")))
    row = [
        notify_time.astimezone().strftime("%Y/%m/%d %H:%M:%S"),
        owner_name,
        f'=HYPERLINK("{contact_url}","{contact_name or contact_id}")',
        cprops.get("email") or "",
        cprops.get("yomi") or "",
        cprops.get("notes_last_contacted") or "",
        "USCPA CRM営業メール開封",
        eprops.get("hs_email_subject") or "",
        eprops.get("hs_email_from_email") or "",
        eprops.get("hs_email_to_email") or "",
        eprops.get("hs_email_open_count") or "",
        contact_url,
        f'=HYPERLINK("{email_url}","{email_id}")',
        email_id,
    ]
    worksheet.append_row(row, value_input_option="USER_ENTERED")
    return f"sheet appended:{worksheet.title}"


def validate_runtime_config(args, slack_map: dict[str, str]) -> None:
    if args.delivery in {"slack", "both"}:
        if not args.slack_bot_token and not args.slack_webhook_url:
            raise RuntimeError(
                "Slack delivery requires SLACK_BOT_TOKEN or SLACK_WEBHOOK_URL"
            )
        missing = [
            CPA_OWNER_NAMES.get(owner_id, owner_id)
            for owner_id in sorted(ALLOWED_CPA_OWNER_IDS)
            if not slack_map.get(owner_id)
        ]
        if missing:
            raise RuntimeError(
                "Slack mentions are missing for CPA owners: "
                + ", ".join(missing)
                + ". Check SLACK_BOT_TOKEN users:read.email scope or USCPA_SLACK_USER_MAP_JSON."
            )

    if args.sheet_output:
        if not args.sheet_spreadsheet_id:
            raise RuntimeError("USCPA_SHEET_SPREADSHEET_ID is not set")
        if not Path(args.google_service_account_json).exists():
            raise RuntimeError(
                f"Google service account json not found: {args.google_service_account_json}"
            )


def main():
    args = parse_args()
    client = HubSpot()
    now = utc_now()
    since = now - timedelta(hours=args.lookback_hours)
    slack_map = json.loads(args.slack_map_json or "{}")
    if args.delivery in {"slack", "both"} and args.slack_bot_token:
        slack_map = lookup_slack_mentions_by_email(args.slack_bot_token, slack_map)
    validate_runtime_config(args, slack_map)
    since_ms = to_ms(since)

    past_member_ids = load_list_member_ids(client)

    emails: list[dict[str, Any]] = []
    for sender in sorted(ALLOWED_SENDERS):
        emails.extend(search_open_emails(client, sender, since_ms, args.limit_per_sender))

    by_id = {str(item["id"]): item for item in emails}
    email_ids = list(by_id.keys())
    associations = batch_email_contact_associations(client, email_ids)
    all_contact_ids = sorted({cid for ids in associations.values() for cid in ids})
    contacts = batch_read_contacts(client, all_contact_ids)

    rows = []
    updates = 0
    for email_id, email_obj in by_id.items():
        email_skip = should_skip_email(email_obj)
        contact_ids = associations.get(email_id, [])
        if email_skip:
            rows.append(
                {
                    "email_id": email_id,
                    "status": "skipped",
                    "reason": email_skip,
                    "contact_ids": contact_ids,
                    "subject": email_obj.get("properties", {}).get("hs_email_subject"),
                }
            )
            continue
        for contact_id in contact_ids:
            contact = contacts.get(contact_id)
            if not contact:
                rows.append(
                    {
                        "email_id": email_id,
                        "contact_id": contact_id,
                        "status": "skipped",
                        "reason": "contact not readable",
                    }
                )
                continue
            reason = should_notify_contact(
                contact, email_id, past_member_ids, now, args.suppression_days
            )
            cprops = contact.get("properties", {})
            eprops = email_obj.get("properties", {})
            row = {
                "email_id": email_id,
                "contact_id": contact_id,
                "contact_url": f"https://app.hubspot.com/contacts/{PORTAL_ID}/contact/{contact_id}",
                "status": "eligible" if reason is None else "skipped",
                "reason": reason or "",
                "sender": eprops.get("hs_email_from_email"),
                "recipient": eprops.get("hs_email_to_email"),
                "subject": eprops.get("hs_email_subject"),
                "open_count": eprops.get("hs_email_open_count"),
                "email_lastmodified": eprops.get("hs_lastmodifieddate"),
                "contact_email": cprops.get("email"),
                "contact_name": f"{cprops.get('lastname') or ''} {cprops.get('firstname') or ''}".strip(),
                "sales_staff_cpa": cprops.get("sales_staff_cpa"),
                "yomi": cprops.get("yomi"),
                "notes_last_contacted": cprops.get("notes_last_contacted"),
            }
            if reason is None and args.apply:
                if args.max_updates and updates >= args.max_updates:
                    row["status"] = "skipped"
                    row["reason"] = "max updates reached"
                else:
                    slack_result = ""
                    if args.delivery in {"slack", "both"}:
                        slack_result = post_slack_notification(
                            contact,
                            email_obj,
                            slack_map,
                            args.slack_channel_id,
                            args.slack_bot_token,
                            args.slack_webhook_url,
                        )
                    sheet_result = ""
                    if args.sheet_output:
                        try:
                            sheet_result = append_sheet_row(
                                args.sheet_spreadsheet_id,
                                args.google_service_account_json,
                                now,
                                contact,
                                email_obj,
                            )
                        except Exception as exc:
                            sheet_result = f"sheet error: {exc}"
                    update_contact_for_notification(
                        client,
                        contact,
                        email_obj,
                        slack_map,
                        now,
                        trigger_hubspot_workflow=args.delivery in {"hubspot", "both"},
                    )
                    updates += 1
                    row["status"] = "updated"
                    row["delivery"] = args.delivery
                    row["slack_result"] = slack_result
                    row["sheet_result"] = sheet_result
            rows.append(row)

    tag = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    out = OUTPUT_DIR / f"uscpa_sales_email_open_monitor_{tag}.json"
    summary = {
        "apply": args.apply,
        "delivery": args.delivery,
        "lookback_hours": args.lookback_hours,
        "since": since.isoformat(),
        "candidate_emails": len(email_ids),
        "associated_contacts": len(all_contact_ids),
        "eligible": sum(1 for row in rows if row["status"] == "eligible"),
        "updated": updates,
        "rows": rows,
    }
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k != "rows"}, ensure_ascii=False, indent=2))
    print(f"details: {out}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
