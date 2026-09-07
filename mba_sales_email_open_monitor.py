import argparse
import hashlib
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://api.hubapi.com"
PORTAL_ID = "39827439"
ATTENDANCE_OBJECT_TYPE = "2-16678867"
EVENT_OBJECT_TYPE = "2-16619393"
OUTPUT_DIR = Path("outputs")
OUTPUT_DIR.mkdir(exist_ok=True)
SLACK_API_MAX_ATTEMPTS = 4
SLACK_API_TIMEOUT_SECONDS = 20
MBA_DEFAULT_REACTIVATION_DAYS = 60

CONTACT_CHECK_HEADERS = [
    "営業接触済み",
    "接触不要",
]

LEGACY_SHEET_HEADERS = [
    "通知日時",
    "担当者",
    "顧客名",
    "メール",
    "ヨミ",
    "前回接触",
    "行動",
    "詳細",
    "送信元/ページURL",
    "宛先",
    "開封数",
    "HubSpot",
    "関連URL",
    "通知ID",
]

SHEET_HEADERS = [
    "通知日時",
    "担当者",
    "顧客名",
    *CONTACT_CHECK_HEADERS,
    "メール",
    "ヨミ",
    "前回接触",
    "行動",
    "詳細",
    "送信元/ページURL",
    "宛先",
    "開封数",
    "HubSpot",
    "関連URL",
    "通知ID",
]

ALLOWED_SENDERS = {
    "info@abitus.co.jp",
    "mba@abitus.co.jp",
    "mba_umass@abitus.co.jp",
    "info+ishida@abitus.co.jp",
    "ishida@abitus.co.jp",
    "info+tsushima@abitus.co.jp",
    "tsushima@abitus.co.jp",
    "info+kuribayashi@abitus.co.jp",
    "kuribayashi@abitus.co.jp",
    "info+kanegae@abitus.co.jp",
    "kanegae@abitus.co.jp",
    "info+torihara@abitus.co.jp",
    "torihara@abitus.co.jp",
}

ALLOWED_MBA_OWNER_IDS = {
    "1141449920",  # 津島 恵介
    "1223691227",  # 栗林 真理絵
    "875246223",  # 石田 彩
    "495505977",  # 鐘ヶ江 遼平
    "1504551294",  # 鳥原 大輔
}

MBA_OWNER_EMAILS = {
    "1141449920": "tsushima@abitus.co.jp",
    "1223691227": "kuribayashi@abitus.co.jp",
    "875246223": "ishida@abitus.co.jp",
    "495505977": "kanegae@abitus.co.jp",
    "1504551294": "torihara@abitus.co.jp",
}

DEFAULT_MBA_SLACK_MENTIONS = {
    "1141449920": "<@U07TFT3QZTL>",  # 津島 恵介
    "1223691227": "<@U07T2KB9JSH>",  # 栗林 真理絵
    "875246223": "<@U07T2JL0SRK>",  # 石田 彩
    "495505977": "<@U07TFD8A81G>",  # 鐘ヶ江 遼平
    "1504551294": "<@U07TTUTRXS4>",  # 鳥原 大輔
}

MBA_OWNER_NAMES = {
    "1141449920": "津島 恵介",
    "1223691227": "栗林 真理絵",
    "875246223": "石田 彩",
    "495505977": "鐘ヶ江 遼平",
    "1504551294": "鳥原 大輔",
}

CONTACT_PROPS = [
    "email",
    "firstname",
    "lastname",
    "sales_staff_mba",
    "yomi",
    "notes_last_contacted",
    "core_mba_entry",
    "core_mba_project",
    "mba_reactivation_last_email_id",
    "mba_reactivation_last_notified_at",
    "hs_analytics_last_url",
    "hs_analytics_last_timestamp",
    "hs_analytics_last_visit_timestamp",
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

ATTENDANCE_PROPS = [
    "attendance_sw",
    "date",
    "event_id",
    "event_kind_inner",
    "product",
    "start_time",
    "submission_idempotent_id",
    "hs_createdate",
    "hs_lastmodifieddate",
]

EVENT_PROPS = [
    "date",
    "event_id",
    "event_kind",
    "event_kind_inner",
    "lp_url",
    "product",
    "start_time",
    "sub_title",
    "title",
    "webinar_url",
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
        description="MBA過去リード向けCRM営業メール開封/Web閲覧Slack通知処理"
    )
    parser.add_argument("--lookback-hours", type=int, default=48)
    parser.add_argument("--limit-per-sender", type=int, default=100)
    parser.add_argument("--limit-web-contacts", type=int, default=500)
    parser.add_argument("--reactivation-days", type=int, default=MBA_DEFAULT_REACTIVATION_DAYS)
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
        default=os.environ.get("MBA_SLACK_USER_MAP_JSON", "{}"),
        help='HubSpot owner ID -> Slack mentionのJSON。例: {"875246223":"<@U...>"}',
    )
    parser.add_argument(
        "--delivery",
        choices=["hubspot", "slack", "both"],
        default=os.environ.get("MBA_NOTIFICATION_DELIVERY", "slack"),
        help="通知方法。hubspotはHubSpot標準Slack WF、slackはSlack API/Webhook直送、bothは両方。",
    )
    parser.add_argument(
        "--slack-channel-id",
        default=os.environ.get("MBA_SLACK_CHANNEL_ID"),
    )
    parser.add_argument("--slack-bot-token", default=os.environ.get("SLACK_BOT_TOKEN"))
    parser.add_argument("--slack-webhook-url", default=os.environ.get("SLACK_WEBHOOK_URL"))
    parser.add_argument(
        "--sheet-output",
        action="store_true",
        default=os.environ.get("MBA_SHEET_OUTPUT", "").lower() in {"1", "true", "yes"},
        help="通知済みレコードをGoogleスプレッドシートにも追記する。",
    )
    parser.add_argument(
        "--sheet-setup-only",
        action="store_true",
        help="Google Sheetsの担当者別タブ、ヘッダー、接触管理チェック列だけを整備して終了する。",
    )
    parser.add_argument(
        "--disable-web",
        action="store_true",
        help="Webページ閲覧通知を無効化する。既定では有効。",
    )
    parser.add_argument(
        "--sheet-spreadsheet-id",
        default=os.environ.get("MBA_SHEET_SPREADSHEET_ID"),
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


def mba_reactivation_filter_groups(cutoff_ms: str) -> list[dict[str, Any]]:
    return [
        {
            "filters": [
                {
                    "propertyName": "notes_last_contacted",
                    "operator": "LT",
                    "value": cutoff_ms,
                },
                {
                    "propertyName": "core_mba_entry",
                    "operator": "NOT_HAS_PROPERTY",
                },
                {
                    "propertyName": "core_mba_project",
                    "operator": "HAS_PROPERTY",
                },
                {
                    "propertyName": "sales_staff_mba",
                    "operator": "IN",
                    "values": sorted(ALLOWED_MBA_OWNER_IDS),
                },
            ],
        },
    ]


def load_past_member_ids(client: HubSpot, reactivation_days: int) -> set[str]:
    member_ids: set[str] = set()
    after = None
    cutoff = utc_now() - timedelta(days=reactivation_days)
    filter_groups = mba_reactivation_filter_groups(to_ms(cutoff))
    while True:
        payload: dict[str, Any] = {
            "filterGroups": filter_groups,
            "properties": ["email"],
            "limit": 100,
        }
        if after:
            payload["after"] = after
        data = client.request("POST", "/crm/v3/objects/contacts/search", json=payload)
        for row in data.get("results", []):
            member_ids.add(str(row.get("id")))
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


def search_recent_web_contacts(client: HubSpot, since_ms: str, limit: int) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    after = None
    while True:
        payload: dict[str, Any] = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "hs_analytics_last_timestamp",
                            "operator": "GTE",
                            "value": since_ms,
                        }
                    ]
                }
            ],
            "properties": CONTACT_PROPS,
            "limit": min(100, max(1, limit - len(results))),
            "sorts": [
                {"propertyName": "hs_analytics_last_timestamp", "direction": "DESCENDING"}
            ],
        }
        if after:
            payload["after"] = after
        data = client.request("POST", "/crm/v3/objects/contacts/search", json=payload)
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


def is_mba_page(url: str | None) -> bool:
    if not url:
        return False
    return "mba" in url.lower()


def web_notification_key(contact: dict[str, Any]) -> str:
    props = contact.get("properties", {})
    timestamp = props.get("hs_analytics_last_timestamp") or props.get(
        "hs_analytics_last_visit_timestamp"
    ) or ""
    url = props.get("hs_analytics_last_url") or ""
    digest = hashlib.sha256(f"{timestamp}|{url}".encode("utf-8")).hexdigest()[:16]
    return f"web:{timestamp}:{digest}"


def shorten_url_label(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    if not parsed.netloc:
        return url
    return f"{parsed.netloc}{parsed.path}" or url


def is_thanks_page_url(url: str | None) -> bool:
    if not url:
        return False
    path = urlparse(url).path.lower().rstrip("/")
    return path.endswith("_thanks") or path.endswith("/thanks") or "thanks" in path


def extract_submission_guid(url: str | None) -> str:
    if not url:
        return ""
    query = parse_qs(urlparse(url).query)
    for key, values in query.items():
        if key.lower() == "submissionguid" and values:
            return values[0]
    return ""


def clean_title(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def fetch_page_title(url: str, cache: dict[str, str]) -> str:
    if not url:
        return ""
    if url in cache:
        return cache[url]
    title = ""
    try:
        response = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; MBA reactivation monitor; "
                    "+https://www.abitus.co.jp/)"
                )
            },
            timeout=8,
            allow_redirects=True,
        )
        content_type = response.headers.get("Content-Type", "")
        if response.status_code < 400 and "html" in content_type.lower():
            response.encoding = response.encoding or response.apparent_encoding
            match = re.search(r"<title[^>]*>(.*?)</title>", response.text, flags=re.I | re.S)
            if match:
                title = clean_title(match.group(1))
    except requests.RequestException:
        title = ""
    cache[url] = title
    return title


def format_ymd(value: str | None) -> str:
    if not value:
        return ""
    try:
        return datetime.fromisoformat(value).strftime("%Y/%m/%d")
    except ValueError:
        return value


def build_event_title(attendance: dict[str, Any] | None, event: dict[str, Any] | None) -> str:
    attendance_props = (attendance or {}).get("properties", {})
    event_props = (event or {}).get("properties", {})
    title = (
        clean_title(event_props.get("title"))
        or clean_title(event_props.get("event_kind_inner"))
        or clean_title(attendance_props.get("event_kind_inner"))
    )
    date = format_ymd(event_props.get("date") or attendance_props.get("date"))
    start_time = event_props.get("start_time") or attendance_props.get("start_time") or ""
    when = " ".join(part for part in [date, start_time] if part)
    if title and when:
        return f"{title}（{when}）"
    return title or when


def build_registration_url(raw_url: str, attendance: dict[str, Any] | None) -> str:
    if not raw_url:
        return ""
    event_id = ((attendance or {}).get("properties") or {}).get("event_id")
    parsed = urlparse(raw_url)
    path = parsed.path
    lower_path = path.lower()
    if "seminar_thanks" in lower_path:
        path = re.sub("seminar_thanks", "seminar_input", path, flags=re.I)
    elif "counseling_thanks" in lower_path:
        path = re.sub("counseling_thanks", "counseling", path, flags=re.I)
    elif "request_thanks" in lower_path:
        path = re.sub("request_thanks", "request", path, flags=re.I)
    else:
        path = re.sub("_thanks", "_input", path, flags=re.I)
    query: dict[str, str] = {}
    if event_id:
        query["event_id"] = str(event_id)
    if "/mba/" in path.lower():
        query["program"] = "MBA"
    return urlunparse((parsed.scheme, parsed.netloc, path, "", urlencode(query), ""))


class WebPageContextResolver:
    def __init__(self, client: HubSpot) -> None:
        self.client = client
        self.title_cache: dict[str, str] = {}
        self.association_cache: dict[tuple[str, str, str], list[str]] = {}
        self.object_cache: dict[tuple[str, str], dict[str, Any]] = {}

    def associated_ids(self, from_type: str, from_id: str, to_type: str) -> list[str]:
        cache_key = (from_type, from_id, to_type)
        if cache_key in self.association_cache:
            return self.association_cache[cache_key]
        ids: list[str] = []
        after = None
        while True:
            params: dict[str, Any] = {"limit": 100}
            if after:
                params["after"] = after
            data = self.client.request(
                "GET",
                f"/crm/v4/objects/{from_type}/{from_id}/associations/{to_type}",
                params=params,
            )
            ids.extend(str(row["toObjectId"]) for row in data.get("results", []))
            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                break
        self.association_cache[cache_key] = ids
        return ids

    def read_object(self, object_type: str, object_id: str, properties: list[str]) -> dict[str, Any]:
        cache_key = (object_type, object_id)
        if cache_key in self.object_cache:
            return self.object_cache[cache_key]
        data = self.client.request(
            "GET",
            f"/crm/v3/objects/{object_type}/{object_id}",
            params={"properties": ",".join(properties)},
        )
        self.object_cache[cache_key] = data
        return data

    def search_event_by_event_id(self, event_id: str | None) -> dict[str, Any] | None:
        if not event_id:
            return None
        data = self.client.request(
            "POST",
            f"/crm/v3/objects/{EVENT_OBJECT_TYPE}/search",
            json={
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "event_id",
                                "operator": "EQ",
                                "value": str(event_id),
                            }
                        ]
                    }
                ],
                "properties": EVENT_PROPS,
                "limit": 1,
            },
        )
        results = data.get("results", [])
        return results[0] if results else None

    def find_attendance(self, contact_id: str, submission_guid: str) -> dict[str, Any] | None:
        attendance_ids = self.associated_ids("contacts", contact_id, ATTENDANCE_OBJECT_TYPE)
        attendance_records = [
            self.read_object(ATTENDANCE_OBJECT_TYPE, attendance_id, ATTENDANCE_PROPS)
            for attendance_id in attendance_ids
        ]
        if submission_guid:
            for attendance in attendance_records:
                if (
                    attendance.get("properties", {}).get("submission_idempotent_id")
                    == submission_guid
                ):
                    return attendance
        return None

    def event_for_attendance(self, attendance: dict[str, Any] | None) -> dict[str, Any] | None:
        if not attendance:
            return None
        attendance_id = str(attendance["id"])
        event_ids = self.associated_ids(
            ATTENDANCE_OBJECT_TYPE,
            attendance_id,
            EVENT_OBJECT_TYPE,
        )
        if event_ids:
            return self.read_object(EVENT_OBJECT_TYPE, event_ids[0], EVENT_PROPS)
        return self.search_event_by_event_id(attendance.get("properties", {}).get("event_id"))

    def resolve(self, contact: dict[str, Any]) -> dict[str, str]:
        cprops = contact.get("properties", {})
        raw_url = cprops.get("hs_analytics_last_url") or ""
        context = {
            "raw_url": raw_url,
            "display_url": raw_url,
            "page_title": "",
            "source": "hs_analytics_last_url",
            "attendance_id": "",
            "event_record_id": "",
            "event_id": "",
        }
        if not raw_url:
            return context

        if is_thanks_page_url(raw_url):
            submission_guid = extract_submission_guid(raw_url)
            attendance = self.find_attendance(str(contact.get("id")), submission_guid)
            event = self.event_for_attendance(attendance)
            event_title = build_event_title(attendance, event)
            attendance_props = (attendance or {}).get("properties", {})
            event_props = (event or {}).get("properties", {})
            display_url = event_props.get("lp_url") or build_registration_url(raw_url, attendance)
            if display_url:
                context["display_url"] = display_url
            if event_title:
                context["page_title"] = event_title
            context["source"] = "event_attendance" if attendance else "thanks_url"
            context["attendance_id"] = str((attendance or {}).get("id") or "")
            context["event_record_id"] = str((event or {}).get("id") or "")
            context["event_id"] = str(event_props.get("event_id") or attendance_props.get("event_id") or "")

        if not context["page_title"]:
            context["page_title"] = fetch_page_title(context["display_url"], self.title_cache)

        return context


def should_skip_web_contact(contact: dict[str, Any]) -> str | None:
    props = contact.get("properties", {})
    url = props.get("hs_analytics_last_url")
    if not url:
        return "web page url missing"
    if not is_mba_page(url):
        return "not mba page"
    return None


def should_notify_contact(
    contact: dict[str, Any],
    notification_key: str,
    past_member_ids: set[str],
    now: datetime,
    suppression_days: int,
) -> str | None:
    contact_id = str(contact.get("id"))
    props = contact.get("properties", {})
    if contact_id not in past_member_ids:
        return "not in MBA past lead pool"
    owner_id = str(props.get("sales_staff_mba") or "")
    if owner_id not in ALLOWED_MBA_OWNER_IDS:
        return "sales_staff_mba not target owner"
    if str(props.get("mba_reactivation_last_email_id") or "") == str(notification_key):
        return "same action already notified"
    last_notified = parse_hs_datetime(props.get("mba_reactivation_last_notified_at"))
    if last_notified and now - last_notified < timedelta(days=suppression_days):
        return "within suppression window"
    return None


def update_contact_for_notification(
    client: HubSpot,
    contact: dict[str, Any],
    notification_key: str,
    action_type: str,
    detail: str,
    source_or_url: str,
    slack_map: dict[str, str],
    now: datetime,
    trigger_hubspot_workflow: bool,
):
    contact_id = str(contact["id"])
    cprops = contact.get("properties", {})
    owner_id = str(cprops.get("sales_staff_mba") or "")
    payload = {
        "properties": {
            "mba_reactivation_last_email_id": notification_key,
            "mba_reactivation_last_notified_at": now.isoformat().replace("+00:00", "Z"),
            "mba_reactivation_last_email_subject": detail,
            "mba_reactivation_last_email_from": source_or_url,
            "mba_reactivation_last_action_type": action_type,
            "mba_reactivation_slack_mention": slack_map.get(owner_id, ""),
        }
    }
    if trigger_hubspot_workflow:
        payload["properties"]["mba_reactivation_notification_seq"] = (
            f"{notification_key}:{int(now.timestamp())}"
        )
    return client.request("PATCH", f"/crm/v3/objects/contacts/{contact_id}", json=payload)


def slack_retry_delay(attempt: int, response: requests.Response | None = None) -> float:
    if response is not None and response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(1.0, float(retry_after))
            except ValueError:
                pass
    return float(2**attempt)


def slack_api_request(
    token: str,
    method: str,
    payload: dict[str, Any] | None = None,
    *,
    http_method: str = "POST",
    params: dict[str, Any] | None = None,
):
    url = f"https://slack.com/api/{method}"
    headers = {"Authorization": f"Bearer {token}"}
    if http_method.upper() == "POST":
        headers["Content-Type"] = "application/json"
    last_error: Exception | None = None
    for attempt in range(SLACK_API_MAX_ATTEMPTS):
        response: requests.Response | None = None
        try:
            if http_method.upper() == "GET":
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=SLACK_API_TIMEOUT_SECONDS,
                )
            else:
                response = requests.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=SLACK_API_TIMEOUT_SECONDS,
                )
            if response.status_code in (429, 500, 502, 503, 504):
                if attempt < SLACK_API_MAX_ATTEMPTS - 1:
                    time.sleep(slack_retry_delay(attempt, response))
                    continue
            response.raise_for_status()
            data = response.json()
            if not data.get("ok"):
                raise RuntimeError(f"Slack API {method} failed: {data}")
            return data
        except requests.RequestException as exc:
            last_error = exc
            if attempt < SLACK_API_MAX_ATTEMPTS - 1:
                time.sleep(slack_retry_delay(attempt, response))
                continue
            raise RuntimeError(f"Slack API {method} request failed after retries: {exc}") from exc
    if last_error:
        raise RuntimeError(f"Slack API {method} request failed after retries: {last_error}")
    raise RuntimeError(f"Slack API {method} request failed after retries")


def owner_slack_display(owner_id: str, slack_map: dict[str, str]) -> str:
    return slack_map.get(owner_id) or MBA_OWNER_NAMES.get(owner_id, owner_id)


def slack_api_post_webhook(webhook_url: str, payload: dict[str, Any]) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(SLACK_API_MAX_ATTEMPTS):
        response: requests.Response | None = None
        try:
            response = requests.post(
                webhook_url,
                json=payload,
                timeout=SLACK_API_TIMEOUT_SECONDS,
            )
            if response.status_code in (429, 500, 502, 503, 504):
                if attempt < SLACK_API_MAX_ATTEMPTS - 1:
                    time.sleep(slack_retry_delay(attempt, response))
                    continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt < SLACK_API_MAX_ATTEMPTS - 1:
                time.sleep(slack_retry_delay(attempt, response))
                continue
            raise RuntimeError(f"Slack webhook request failed after retries: {exc}") from exc
    if last_error:
        raise RuntimeError(f"Slack webhook request failed after retries: {last_error}")
    raise RuntimeError("Slack webhook request failed after retries")


def lookup_slack_mentions_by_email(token: str, existing_map: dict[str, str]) -> dict[str, str]:
    # Verified defaults are authoritative so an old external map cannot point
    # a HubSpot owner ID at another staff member's Slack account.
    slack_map = {**existing_map, **DEFAULT_MBA_SLACK_MENTIONS}
    for owner_id, email in MBA_OWNER_EMAILS.items():
        try:
            data = slack_api_request(
                token,
                "users.lookupByEmail",
                http_method="GET",
                params={"email": email},
            )
        except RuntimeError as exc:
            print(
                "WARNING: Slack mention lookup skipped for "
                f"{MBA_OWNER_NAMES.get(owner_id, owner_id)} ({email}): {exc}",
                file=sys.stderr,
            )
            continue
        user = data.get("user", {})
        profile_email = str(user.get("profile", {}).get("email") or "").lower()
        if profile_email and profile_email != email.lower():
            print(
                "WARNING: Slack mention lookup returned a different email for "
                f"{MBA_OWNER_NAMES.get(owner_id, owner_id)}: expected {email}, got {profile_email}",
                file=sys.stderr,
            )
            continue
        if data.get("ok") and user.get("id"):
            slack_map[owner_id] = f"<@{user['id']}>"
    return slack_map


def build_email_slack_text(contact: dict[str, Any], email_obj: dict[str, Any], mention: str) -> str:
    cprops = contact.get("properties", {})
    eprops = email_obj.get("properties", {})
    contact_id = str(contact.get("id"))
    email_id = str(email_obj.get("id"))
    owner_id = str(cprops.get("sales_staff_mba") or "")
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
            "行動: MBA CRM営業メール開封",
            f"顧客名: {linked_contact_name}",
            f"メール: {cprops.get('email') or ''}",
            f"前回接触: {cprops.get('notes_last_contacted') or ''}",
            f"開封メール: {linked_email}",
            f"送信元: {eprops.get('hs_email_from_email') or ''}",
        ]
    )


def build_web_slack_text(
    contact: dict[str, Any],
    mention: str,
    web_context: dict[str, str] | None = None,
) -> str:
    cprops = contact.get("properties", {})
    web_context = web_context or {}
    contact_id = str(contact.get("id"))
    contact_name = f"{cprops.get('lastname') or ''} {cprops.get('firstname') or ''}".strip()
    contact_url = f"https://app.hubspot.com/contacts/{PORTAL_ID}/contact/{contact_id}"
    page_url = web_context.get("display_url") or cprops.get("hs_analytics_last_url") or ""
    page_title = web_context.get("page_title") or ""
    linked_contact_name = f"<{contact_url}|{contact_name or contact_id}>"
    linked_page = f"<{page_url}|{shorten_url_label(page_url)}>" if page_url else ""
    lines = [
        "過去リードが再行動しました",
        "",
        f"担当者: {mention}".strip(),
        "行動: MBA Webページ閲覧",
        f"顧客名: {linked_contact_name}",
        f"メール: {cprops.get('email') or ''}",
        f"前回接触: {cprops.get('notes_last_contacted') or ''}",
    ]
    if page_title:
        lines.append(f"ページタイトル: {page_title}")
    lines.extend(
        [
            f"ページURL: {linked_page}",
            f"閲覧日時: {cprops.get('hs_analytics_last_timestamp') or ''}",
        ]
    )
    return "\n".join(lines)


def post_slack_notification(
    contact: dict[str, Any],
    email_obj: dict[str, Any],
    slack_map: dict[str, str],
    channel_id: str,
    bot_token: str | None,
    webhook_url: str | None,
) -> str:
    owner_id = str(contact.get("properties", {}).get("sales_staff_mba") or "")
    mention = owner_slack_display(owner_id, slack_map)
    text = build_email_slack_text(contact, email_obj, mention)
    if bot_token:
        data = slack_api_request(
            bot_token,
            "chat.postMessage",
            {"channel": channel_id, "text": text, "mrkdwn": True, "unfurl_links": False},
        )
        return f"chat.postMessage:{data.get('ts')}"
    if webhook_url:
        response = slack_api_post_webhook(
            webhook_url,
            {"text": text, "mrkdwn": True, "unfurl_links": False},
        )
        if response.text.strip().lower() != "ok":
            raise RuntimeError(f"Slack webhook failed: {response.status_code} {response.text[:500]}")
        return "incoming_webhook:ok"
    raise RuntimeError("Slack delivery requested, but SLACK_BOT_TOKEN or SLACK_WEBHOOK_URL is not set")


def post_web_slack_notification(
    contact: dict[str, Any],
    web_context: dict[str, str],
    slack_map: dict[str, str],
    channel_id: str,
    bot_token: str | None,
    webhook_url: str | None,
) -> str:
    owner_id = str(contact.get("properties", {}).get("sales_staff_mba") or "")
    mention = owner_slack_display(owner_id, slack_map)
    text = build_web_slack_text(contact, mention, web_context)
    if bot_token:
        data = slack_api_request(
            bot_token,
            "chat.postMessage",
            {"channel": channel_id, "text": text, "mrkdwn": True, "unfurl_links": False},
        )
        return f"chat.postMessage:{data.get('ts')}"
    if webhook_url:
        response = slack_api_post_webhook(
            webhook_url,
            {"text": text, "mrkdwn": True, "unfurl_links": False},
        )
        if response.text.strip().lower() != "ok":
            raise RuntimeError(f"Slack webhook failed: {response.status_code} {response.text[:500]}")
        return "incoming_webhook:ok"
    raise RuntimeError("Slack delivery requested, but SLACK_BOT_TOKEN or SLACK_WEBHOOK_URL is not set")


def normalize_sheet_title(value: str) -> str:
    title = value.replace("/", "_").replace("\\", "_").replace("?", "_")
    title = title.replace("*", "_").replace("[", "_").replace("]", "_").replace(":", "_")
    return title[:99] or "未設定"


def column_letter(index_1based: int) -> str:
    value = index_1based
    out = ""
    while value > 0:
        value, remainder = divmod(value - 1, 26)
        out = chr(65 + remainder) + out
    return out


def header_range() -> str:
    return f"A1:{column_letter(len(SHEET_HEADERS))}1"


def get_gspread_client(service_account_json: str):
    import gspread

    path = Path(service_account_json)
    if not path.exists():
        raise RuntimeError(f"Google service account json not found: {service_account_json}")
    client = gspread.service_account(filename=str(path))
    client.http_client.session.mount(
        "https://sheets.googleapis.com/",
        HTTPAdapter(max_retries=Retry(
            total=5, backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "PUT", "DELETE"}),
            raise_on_status=False,
        )),
    )
    return client


def normalized_header(values: list[list[Any]], width: int) -> list[str]:
    row = [str(value) for value in (values[0] if values else [])]
    if len(row) < width:
        row.extend([""] * (width - len(row)))
    return row[:width]


def is_effective_sheet_data_row(values: list[Any]) -> bool:
    """Treat checkbox-only rows as empty for notification appends."""
    for index, value in enumerate(values):
        if index in {3, 4}:
            continue
        if value not in ("", None):
            return True
    return False


def notification_sheet_row_state(worksheet, notification_id: str) -> tuple[int | None, int]:
    values = worksheet.get(f"A1:{column_letter(len(SHEET_HEADERS))}{worksheet.row_count}")
    last_data_row = 1
    for row_index, values_row in enumerate(values[1:], start=2):
        padded = [*values_row, *([""] * (len(SHEET_HEADERS) - len(values_row)))]
        if notification_id and notification_id in {str(padded[14]), str(padded[15])}:
            return row_index, row_index
        if is_effective_sheet_data_row(values_row):
            last_data_row = row_index
    return None, last_data_row + 1


def checkbox_cell_range(worksheet, start_row: int, end_row: int) -> dict[str, int]:
    return {
        "sheetId": worksheet.id,
        "startRowIndex": start_row - 1,
        "endRowIndex": end_row,
        "startColumnIndex": 3,
        "endColumnIndex": 5,
    }


def contact_checkbox_rule() -> dict[str, Any]:
    return {
        "condition": {"type": "BOOLEAN"},
        "strict": True,
        "showCustomUi": True,
    }


def contact_checkbox_column_width_request(worksheet) -> dict[str, Any]:
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": worksheet.id,
                "dimension": "COLUMNS",
                "startIndex": 3,
                "endIndex": 5,
            },
            "properties": {"pixelSize": 110},
            "fields": "pixelSize",
        }
    }


def set_contact_checkbox_validation(worksheet, start_row: int, end_row: int) -> None:
    if end_row < start_row:
        return
    worksheet.spreadsheet.batch_update(
        {
            "requests": [
                {
                    "setDataValidation": {
                        "range": checkbox_cell_range(worksheet, start_row, end_row),
                        "rule": contact_checkbox_rule(),
                    }
                },
                contact_checkbox_column_width_request(worksheet),
            ]
        }
    )


def sync_contact_checkbox_rows(worksheet) -> int:
    _, next_row = notification_sheet_row_state(worksheet, "")
    last_data_row = next_row - 1
    requests: list[dict[str, Any]] = [contact_checkbox_column_width_request(worksheet)]
    if last_data_row >= 2:
        requests.append(
            {
                "setDataValidation": {
                    "range": checkbox_cell_range(worksheet, 2, last_data_row),
                    "rule": contact_checkbox_rule(),
                }
            }
        )
    if last_data_row < worksheet.row_count:
        blank_range = checkbox_cell_range(worksheet, last_data_row + 1, worksheet.row_count)
        requests.extend(
            [
                {
                    "repeatCell": {
                        "range": blank_range,
                        "cell": {},
                        "fields": "userEnteredValue",
                    }
                },
                {
                    "setDataValidation": {
                        "range": blank_range,
                    }
                },
            ]
        )
    worksheet.spreadsheet.batch_update({"requests": requests})
    return max(last_data_row - 1, 0)


def ensure_worksheet(spreadsheet, title: str):
    from gspread import WorksheetNotFound

    try:
        worksheet = spreadsheet.worksheet(title)
    except WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=title, rows=1000, cols=len(SHEET_HEADERS))

    if worksheet.row_count < 1000 or worksheet.col_count < len(SHEET_HEADERS):
        worksheet.resize(rows=max(worksheet.row_count, 1000), cols=max(worksheet.col_count, len(SHEET_HEADERS)))

    header_values = worksheet.get(f"A1:{column_letter(len(SHEET_HEADERS))}1")
    current = normalized_header(header_values, len(SHEET_HEADERS))
    legacy = normalized_header(header_values, len(LEGACY_SHEET_HEADERS))
    if legacy == LEGACY_SHEET_HEADERS and current != SHEET_HEADERS:
        worksheet.spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "insertDimension": {
                            "range": {
                                "sheetId": worksheet.id,
                                "dimension": "COLUMNS",
                                "startIndex": 3,
                                "endIndex": 5,
                            },
                            "inheritFromBefore": False,
                        }
                    }
                ]
            }
        )
        worksheet.resize(rows=max(worksheet.row_count, 1000), cols=max(worksheet.col_count, len(SHEET_HEADERS)))
        current = []

    if current != SHEET_HEADERS:
        worksheet.update(header_range(), [SHEET_HEADERS])
    worksheet.freeze(rows=1)
    return worksheet


def setup_sheet_tabs(spreadsheet_id: str | None, service_account_json: str) -> dict[str, Any]:
    if not spreadsheet_id:
        raise RuntimeError("MBA_SHEET_SPREADSHEET_ID is not set")
    client = get_gspread_client(service_account_json)
    spreadsheet = client.open_by_key(spreadsheet_id)
    tabs = []
    for owner_name in MBA_OWNER_NAMES.values():
        title = normalize_sheet_title(owner_name.replace(" ", ""))
        worksheet = ensure_worksheet(spreadsheet, title)
        sync_contact_checkbox_rows(worksheet)
        tabs.append(title)
    return {
        "spreadsheet_id": spreadsheet_id,
        "tabs": tabs,
        "headers": SHEET_HEADERS,
        "checkbox_columns": CONTACT_CHECK_HEADERS,
    }


def append_sheet_row(
    spreadsheet_id: str | None,
    service_account_json: str,
    notify_time: datetime,
    contact: dict[str, Any],
    action_type: str,
    detail: str,
    source_or_url: str,
    recipient: str,
    open_count: str,
    related_url: str,
    notification_id: str,
) -> str:
    if not spreadsheet_id:
        return "sheet skipped: spreadsheet id not set"
    client = get_gspread_client(service_account_json)
    spreadsheet = client.open_by_key(spreadsheet_id)
    cprops = contact.get("properties", {})
    contact_id = str(contact.get("id"))
    owner_id = str(cprops.get("sales_staff_mba") or "")
    owner_name = MBA_OWNER_NAMES.get(owner_id, owner_id or "担当未設定")
    contact_name = f"{cprops.get('lastname') or ''} {cprops.get('firstname') or ''}".strip()
    contact_url = f"https://app.hubspot.com/contacts/{PORTAL_ID}/contact/{contact_id}"
    worksheet = ensure_worksheet(spreadsheet, normalize_sheet_title(owner_name.replace(" ", "")))
    row = [
        notify_time.astimezone().strftime("%Y/%m/%d %H:%M:%S"),
        owner_name,
        f'=HYPERLINK("{contact_url}","{contact_name or contact_id}")',
        False,
        False,
        cprops.get("email") or "",
        cprops.get("yomi") or "",
        cprops.get("notes_last_contacted") or "",
        action_type,
        detail,
        source_or_url,
        recipient,
        open_count,
        contact_url,
        f'=HYPERLINK("{related_url}","{notification_id}")' if related_url else "",
        notification_id,
    ]
    existing_row, target_row = notification_sheet_row_state(worksheet, notification_id)
    if existing_row is not None:
        return f"sheet already exists:{worksheet.title}:row {existing_row}"
    if target_row > worksheet.row_count:
        worksheet.resize(rows=target_row, cols=max(worksheet.col_count, len(SHEET_HEADERS)))
    worksheet.update(
        f"A{target_row}:{column_letter(len(SHEET_HEADERS))}{target_row}",
        [row],
        value_input_option="USER_ENTERED",
    )
    set_contact_checkbox_validation(worksheet, target_row, target_row)
    return f"sheet appended:{worksheet.title}:row {target_row}"


def validate_runtime_config(args, slack_map: dict[str, str]) -> None:
    if not args.sheet_setup_only and args.delivery in {"slack", "both"}:
        if not args.slack_bot_token and not args.slack_webhook_url:
            raise RuntimeError(
                "Slack delivery requires SLACK_BOT_TOKEN or SLACK_WEBHOOK_URL"
            )
        missing = [
            MBA_OWNER_NAMES.get(owner_id, owner_id)
            for owner_id in sorted(ALLOWED_MBA_OWNER_IDS)
            if not slack_map.get(owner_id)
        ]
        if missing:
            print(
                "WARNING: Slack mentions are missing; owner names will be used instead: "
                + ", ".join(missing),
                file=sys.stderr,
            )

    if args.sheet_output or args.sheet_setup_only:
        if not args.sheet_spreadsheet_id:
            raise RuntimeError("MBA_SHEET_SPREADSHEET_ID is not set")
        if not Path(args.google_service_account_json).exists():
            raise RuntimeError(
                f"Google service account json not found: {args.google_service_account_json}"
            )


def main():
    args = parse_args()
    now = utc_now()
    since = now - timedelta(hours=args.lookback_hours)
    slack_map = json.loads(args.slack_map_json or "{}")
    if args.sheet_setup_only:
        validate_runtime_config(args, slack_map)
        result = setup_sheet_tabs(args.sheet_spreadsheet_id, args.google_service_account_json)
        tag = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        out = OUTPUT_DIR / f"mba_sales_email_open_monitor_{tag}.json"
        summary = {
            "apply": False,
            "sheet_setup_only": True,
            "updated": 0,
            **result,
        }
        out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print(f"details: {out}")
        return

    client = HubSpot()
    if args.delivery in {"slack", "both"} and args.slack_bot_token:
        slack_map = lookup_slack_mentions_by_email(args.slack_bot_token, slack_map)
    validate_runtime_config(args, slack_map)
    since_ms = to_ms(since)

    past_member_ids = load_past_member_ids(client, args.reactivation_days)

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
                "sales_staff_mba": cprops.get("sales_staff_mba"),
                "yomi": cprops.get("yomi"),
                "notes_last_contacted": cprops.get("notes_last_contacted"),
            }
            if reason is None and args.apply:
                if args.max_updates and updates >= args.max_updates:
                    row["status"] = "skipped"
                    row["reason"] = "max updates reached"
                else:
                    sheet_result = ""
                    if args.sheet_output:
                        email_url = f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-49/{email_id}"
                        sheet_result = append_sheet_row(
                            args.sheet_spreadsheet_id,
                            args.google_service_account_json,
                            now,
                            contact,
                            "MBA CRM営業メール開封",
                            eprops.get("hs_email_subject") or "",
                            eprops.get("hs_email_from_email") or "",
                            eprops.get("hs_email_to_email") or "",
                            str(eprops.get("hs_email_open_count") or ""),
                            email_url,
                            email_id,
                        )
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
                    update_contact_for_notification(
                        client,
                        contact,
                        email_id,
                        "MBA CRM営業メール開封",
                        eprops.get("hs_email_subject") or "",
                        eprops.get("hs_email_from_email") or "",
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

    web_contacts: list[dict[str, Any]] = []
    if not args.disable_web:
        web_contacts = search_recent_web_contacts(client, since_ms, args.limit_web_contacts)

    web_context_resolver = WebPageContextResolver(client)
    for contact in web_contacts:
        contact_id = str(contact.get("id"))
        cprops = contact.get("properties", {})
        web_skip = should_skip_web_contact(contact)
        notification_key = web_notification_key(contact)
        reason = web_skip or should_notify_contact(
            contact, notification_key, past_member_ids, now, args.suppression_days
        )
        page_url = cprops.get("hs_analytics_last_url") or ""
        web_context = (
            web_context_resolver.resolve(contact)
            if reason is None
            else {
                "raw_url": page_url,
                "display_url": page_url,
                "page_title": "",
                "source": "skipped",
                "attendance_id": "",
                "event_record_id": "",
                "event_id": "",
            }
        )
        display_url = web_context.get("display_url") or page_url
        page_title = web_context.get("page_title") or ""
        row = {
            "action": "web_view",
            "notification_key": notification_key,
            "contact_id": contact_id,
            "contact_url": f"https://app.hubspot.com/contacts/{PORTAL_ID}/contact/{contact_id}",
            "status": "eligible" if reason is None else "skipped",
            "reason": reason or "",
            "page_url": page_url,
            "display_page_url": display_url,
            "page_title": page_title,
            "page_context_source": web_context.get("source") or "",
            "attendance_id": web_context.get("attendance_id") or "",
            "event_record_id": web_context.get("event_record_id") or "",
            "event_id": web_context.get("event_id") or "",
            "page_timestamp": cprops.get("hs_analytics_last_timestamp"),
            "contact_email": cprops.get("email"),
            "contact_name": f"{cprops.get('lastname') or ''} {cprops.get('firstname') or ''}".strip(),
            "sales_staff_mba": cprops.get("sales_staff_mba"),
            "yomi": cprops.get("yomi"),
            "notes_last_contacted": cprops.get("notes_last_contacted"),
        }
        if reason is None and args.apply:
            if args.max_updates and updates >= args.max_updates:
                row["status"] = "skipped"
                row["reason"] = "max updates reached"
            else:
                sheet_result = ""
                if args.sheet_output:
                    sheet_result = append_sheet_row(
                        args.sheet_spreadsheet_id,
                        args.google_service_account_json,
                        now,
                        contact,
                        "MBA Webページ閲覧",
                        page_title or cprops.get("hs_analytics_last_timestamp") or "",
                        display_url,
                        "",
                        "",
                        page_url,
                        notification_key,
                    )
                slack_result = ""
                if args.delivery in {"slack", "both"}:
                    slack_result = post_web_slack_notification(
                        contact,
                        web_context,
                        slack_map,
                        args.slack_channel_id,
                        args.slack_bot_token,
                        args.slack_webhook_url,
                    )
                update_contact_for_notification(
                    client,
                    contact,
                    notification_key,
                    "MBA Webページ閲覧",
                    page_title or cprops.get("hs_analytics_last_timestamp") or "",
                    display_url,
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
    out = OUTPUT_DIR / f"mba_sales_email_open_monitor_{tag}.json"
    summary = {
        "apply": args.apply,
        "delivery": args.delivery,
        "lookback_hours": args.lookback_hours,
        "reactivation_days": args.reactivation_days,
        "past_member_ids": len(past_member_ids),
        "since": since.isoformat(),
        "candidate_emails": len(email_ids),
        "candidate_web_contacts": len(web_contacts),
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
