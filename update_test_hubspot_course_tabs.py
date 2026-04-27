#!/usr/bin/env python3
import argparse
import datetime as dt
import os
import re
import time
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from requests import exceptions as request_exceptions

from hubspot_course_sheet_guardrails import (
    COURSE_SHEET_HEADER,
    DEFAULT_GA4_MAP_MAX_AGE_MINUTES,
    DEFAULT_SERVICE_ACCOUNT_JSON,
    DEFAULT_SPREADSHEET_ID,
    derive_ga4_map_manifest_path,
    ensure_worksheet,
    load_json,
    now_jst,
    parse_iso_datetime,
    set_worksheet_hidden,
    sha256_file,
    staging_tab_title,
    write_sheet_values,
)


BASE_URL = "https://api.hubapi.com"
JST = dt.timezone(dt.timedelta(hours=9))
EMAIL_FETCH_MARGIN_DAYS = 31

TARGET_COURSES = ["CIA", "CISA", "CFE", "IFRS", "USCPA", "MBA"]
PORTAL_ID = "39827439"
MANAGEMENT_SPREADSHEET_ID = "1oF5ospicN7doWTwa5tm0QY5isSBaPiQ3lNA169mHD8Y"

LEAD_KEYWORDS = ["案件", "リード", "見込み", "見込", "prospect", "lead"]
UNIVERSITY_KEYWORDS = ["大学生", "学部生", "学生", "student", "students", "undergrad", "undergraduate"]
ENROLLED_KEYWORDS = ["受講生", "在校生", "修了生", "卒業生", "受講"]
IGNORE_SEGMENT_KEYWORDS = ["スタッフ", "staff", "テスト", "test", "社内", "除外"]
COURSE_LIST_ALIASES = {
    "CIA": ["CIA"],
    "CISA": ["CISA"],
    "CFE": ["CFE"],
    "IFRS": ["IFRS"],
    "USCPA": ["USCPA", "CPA受講生", "CPA案件", "CPA案件ALL", "USCPA案件", "米国CPA", "U.S.CPA"],
    "MBA": ["MBA"],
}

HUBSPOT_EMAIL_ID_RE = re.compile(r"/details/(\d+)/performance")
PERSONALIZATION_TOKEN_RE = re.compile(r"\{\{\s*personalization_token\([^}]+\)\s*\}\}", re.IGNORECASE)
CONTACT_TOKEN_RE = re.compile(r"\{\{\s*contact\.[^}]+\}\}", re.IGNORECASE)
SUBJECT_COURSE_FALLBACK_BLOCK_PREFIXES = ("CAREER_",)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write monthly per-course tabs to staging with verified GA4 CV.")
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
        help="Manifest JSON created alongside --ga4-map-csv. Required for sheet writes.",
    )
    parser.add_argument(
        "--max-ga4-map-age-minutes",
        type=int,
        default=DEFAULT_GA4_MAP_MAX_AGE_MINUTES,
        help="Refuse sheet writes when the GA4 map bundle is older than this threshold.",
    )
    parser.add_argument("--email-type", default="BATCH_EMAIL")
    parser.add_argument("--skip-sheet", action="store_true", default=False)
    return parser.parse_args()


def month_bounds_utc(month_yyyy_mm: str) -> Tuple[dt.datetime, dt.datetime]:
    start = dt.datetime.strptime(month_yyyy_mm + "-01", "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def to_iso_utc(value: dt.datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_published_at_jst(value: Optional[str]) -> str:
    if not value:
        return ""
    raw = str(value)
    if "T" in raw:
        try:
            x = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return x.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return raw
    for fmt in ["%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
        try:
            x = dt.datetime.strptime(raw, fmt).replace(tzinfo=dt.timezone.utc)
            return x.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return raw


def parse_published_at_jst_dt(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    raw = str(value)
    if "T" in raw:
        try:
            x = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return x.astimezone(JST)
        except Exception:
            return None
    for fmt in ["%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
        try:
            x = dt.datetime.strptime(raw, fmt).replace(tzinfo=dt.timezone.utc)
            return x.astimezone(JST)
        except Exception:
            pass
    return None


def pick_send_datetime_raw(email_obj: dict) -> Optional[str]:
    # HubSpot UIの送信時刻に合わせるため、publishDateを優先。
    return (email_obj.get("publishDate") or email_obj.get("publishedAt") or None)


def safe_num(v) -> float:
    if v is None or v == "":
        return 0.0
    try:
        return float(v)
    except Exception:
        return 0.0


def rate_pct(num: float, den: float) -> float:
    if den <= 0:
        return 0.0
    return round((num / den) * 100.0, 3)

def rate_pct_2dp_text(num: float, den: float) -> str:
    if den <= 0:
        return "0.00%"
    return f"{(num / den) * 100.0:.2f}%"


def as_literal_text(value: str) -> str:
    v = value or ""
    if not v:
        return v
    if v.startswith("'"):
        return v
    return "'" + v


def detect_course_candidates(text: str, alias_map: Dict[str, List[str]]) -> List[str]:
    normalized = (text or "").upper()
    hits: List[str] = []
    for course, aliases in alias_map.items():
        if any(alias.upper() in normalized for alias in aliases):
            hits.append(course)
    return hits


def detect_course(email_name: str, list_names: List[str], subject: str = "") -> str:
    list_hits = detect_course_candidates(" | ".join(list_names), COURSE_LIST_ALIASES)
    if len(list_hits) == 1:
        return list_hits[0]
    if len(list_hits) > 1:
        email_hits = detect_course_candidates(email_name, {course: [course] for course in TARGET_COURSES})
        if len(email_hits) == 1:
            return email_hits[0]
        if (email_name or "").upper().startswith(SUBJECT_COURSE_FALLBACK_BLOCK_PREFIXES):
            return ""
        subject_hits = detect_course_candidates(subject, {course: [course] for course in TARGET_COURSES})
        if len(subject_hits) == 1:
            return subject_hits[0]
        return list_hits[0]

    name = (email_name or "").upper()
    for c in TARGET_COURSES:
        if c in name:
            return c
    if name.startswith(SUBJECT_COURSE_FALLBACK_BLOCK_PREFIXES):
        return ""
    subject_upper = (subject or "").upper()
    for c in TARGET_COURSES:
        if c in subject_upper:
            return c
    return ""


def detect_course_for_unregistered(email_name: str, list_names: List[str], subject: str = "") -> str:
    list_hits = detect_course_candidates(" | ".join(list_names), COURSE_LIST_ALIASES)
    if len(list_hits) == 1:
        return list_hits[0]

    email_hits = detect_course_candidates(email_name, {course: [course] for course in TARGET_COURSES})
    if len(email_hits) == 1:
        return email_hits[0]

    combined = " | ".join([email_name or "", " | ".join(list_names)]).upper()
    if "FAR" in combined:
        return "USCPA"
    return ""


def classify_segment(email_name: str, list_names: List[str]) -> str:
    cleaned = [x for x in list_names if x and not any(k.lower() in x.lower() for k in IGNORE_SEGMENT_KEYWORDS)]
    text = " | ".join(cleaned)
    text_lower = text.lower()
    name_lower = (email_name or "").lower()

    lead_hit = any(k.lower() in text_lower for k in LEAD_KEYWORDS)
    university_hit = any(k.lower() in text_lower for k in UNIVERSITY_KEYWORDS)
    enrolled_hit = any(k.lower() in text_lower for k in ENROLLED_KEYWORDS)

    if not (lead_hit or university_hit or enrolled_hit):
        lead_hit = any(k in name_lower for k in ["_lead", "lead_"])
        university_hit = any(k in name_lower for k in ["_student", "student_", "undergrad", "undergraduate"])
        enrolled_hit = any(k in name_lower for k in ["受講生", "在校生", "alumni", "enrolled"])

    parts: List[str] = []
    if lead_hit:
        parts.append("リード")
    if university_hit:
        parts.append("大学生")
    if enrolled_hit:
        parts.append("受講生")
    if parts:
        return "+".join(parts)
    return "未分類"


def normalize_management_subject(text: str) -> str:
    value = (text or "").strip().lower()
    value = value.replace("（", "(").replace("）", ")").replace("　", " ")
    value = PERSONALIZATION_TOKEN_RE.sub("", value)
    value = CONTACT_TOKEN_RE.sub("", value)
    for token in ["##__名前__##様", "##__名前__##", "##名前##様", "##名前##", "様をご招待", "様を特別招待", "様へ", "様"]:
        value = value.replace(token.lower(), "")
    value = re.sub(r"\s+", "", value)
    return value


def parse_management_email_id(value: str) -> str:
    match = HUBSPOT_EMAIL_ID_RE.search(value or "")
    return match.group(1) if match else ""


def normalize_management_date(value: str, target_year: int) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    for fmt in ["%Y/%m/%d", "%Y-%m-%d", "%m/%d", "%m-%d"]:
        try:
            parsed = dt.datetime.strptime(raw, fmt)
            year = parsed.year if "%Y" in fmt else target_year
            return dt.date(year, parsed.month, parsed.day).isoformat()
        except ValueError:
            continue
    return ""


def normalize_management_time(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    for fmt in ["%H:%M:%S", "%H:%M"]:
        try:
            return dt.datetime.strptime(raw, fmt).strftime("%H:%M")
        except ValueError:
            continue
    return raw[:5]


def extract_year_from_management_key(value: str) -> str:
    match = re.search(r"(20\d{2})", (value or "").strip())
    return match.group(1) if match else ""


def should_skip_management_row(link_key: str, subject: str, status: str) -> bool:
    key = (link_key or "").strip()
    text = " | ".join([key, (subject or "").strip(), (status or "").strip()]).lower()
    if key.upper().startswith("FALSE_"):
        return True
    if "一旦停止" in text:
        return True
    return any(token in text for token in ["配信中止", "配信キャンセル", "cancelled", "canceled"])


def sanitize_management_link_key(value: str) -> str:
    key = (value or "").strip()
    if not key:
        return ""
    if re.fullmatch(r"\d+", key):
        return ""
    return key


def _append_unique_row(bucket: Dict[str, List[dict]], key: str, row: dict) -> None:
    if key:
        bucket.setdefault(key, []).append(row)


def load_management_index(service_account_json: str, month: str) -> dict:
    import gspread
    from google.oauth2.service_account import Credentials

    target_year, target_month = [int(part) for part in month.split("-")]
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(service_account_json, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(MANAGEMENT_SPREADSHEET_ID)

    index = {
        "progress_by_link_key": {},
        "progress_by_subject_dt": {},
        "progress_by_subject_only": {},
        "new_by_email_id": {},
        "legacy_by_email_id": {},
        "all_rows": [],
    }

    def make_row(
        source_tab: str,
        row_number: int,
        link_key: str,
        subject: str,
        send_date: str,
        send_time: str,
        status: str = "",
        email_id: str = "",
    ) -> dict:
        return {
            "management_uid": f"{source_tab}:{row_number}",
            "source_tab": source_tab,
            "row_number": row_number,
            "link_key": (link_key or "").strip(),
            "subject": (subject or "").strip(),
            "send_date": send_date,
            "send_time": send_time,
            "status": (status or "").strip(),
            "email_id": (email_id or "").strip(),
        }

    progress_ws = sh.worksheet("進捗シート")
    progress_values = progress_ws.get_all_values()
    if progress_values:
        header = progress_values[0]
        idx_key = header.index("リンクキー")
        idx_date = header.index("配信日")
        idx_time = header.index("配信時間")
        idx_subject = header.index("件名")
        idx_status = header.index("ステータス") if "ステータス" in header else None
        for row_number, row in enumerate(progress_values[1:], start=2):
            if len(row) <= idx_subject:
                continue
            send_date = normalize_management_date(row[idx_date], target_year)
            if not send_date or not send_date.startswith(f"{month}-"):
                continue
            send_time = normalize_management_time(row[idx_time])
            link_key = sanitize_management_link_key(row[idx_key])
            link_key_year = extract_year_from_management_key(link_key)
            if link_key_year and link_key_year != str(target_year):
                continue
            subject = row[idx_subject]
            status = row[idx_status] if idx_status is not None and len(row) > idx_status else ""
            if should_skip_management_row(link_key, subject, status):
                continue
            normalized_subject = normalize_management_subject(subject)
            payload = make_row(
                "進捗シート",
                row_number,
                link_key,
                subject,
                send_date,
                send_time,
                status,
            )
            _append_unique_row(index["progress_by_link_key"], payload["link_key"], payload)
            _append_unique_row(index["progress_by_subject_dt"], f"{send_date}|{send_time}|{normalized_subject}", payload)
            _append_unique_row(index["progress_by_subject_only"], normalized_subject, payload)
            index["all_rows"].append(payload)

    for tab_name, link_key_column, email_id_bucket in [
        ("新管理表", "リンクキー", "new_by_email_id"),
        ("CPA_DM_一覧", "hs_Eメール名", "legacy_by_email_id"),
    ]:
        worksheet = sh.worksheet(tab_name)
        values = worksheet.get_all_values()
        if not values:
            continue
        header = values[0]
        idx_link_key = header.index(link_key_column)
        idx_date = header.index("配信日")
        idx_time = header.index("配信時間")
        idx_subject = header.index("件名" if "件名" in header else "DMの件名")
        idx_perf = header.index("パフォーマンスリンク")
        idx_status = header.index("ステータス") if "ステータス" in header else None
        for row_number, row in enumerate(values[1:], start=2):
            if len(row) <= idx_perf:
                continue
            send_date = normalize_management_date(row[idx_date], target_year)
            if not send_date or not send_date.startswith(f"{month}-"):
                continue
            email_id = parse_management_email_id(row[idx_perf])
            status = row[idx_status] if idx_status is not None and len(row) > idx_status else ""
            if should_skip_management_row(row[idx_link_key], row[idx_subject], status):
                continue
            payload = make_row(
                tab_name,
                row_number,
                sanitize_management_link_key(row[idx_link_key]),
                row[idx_subject],
                send_date,
                normalize_management_time(row[idx_time]),
                status,
                email_id,
            )
            index["all_rows"].append(payload)
            _append_unique_row(index[email_id_bucket], email_id, payload)

    return index


def _unique_match(candidates: List[dict]) -> Optional[dict]:
    return candidates[0] if len(candidates) == 1 else None


def resolve_management_row(
    management_index: dict,
    email_id: str,
    raw_email_name: str,
    subject: str,
    send_dt: dt.datetime,
) -> Tuple[Optional[dict], str]:
    ambiguity_reasons: List[str] = []

    lookup_steps = [
        ("progress_by_link_key", raw_email_name, "進捗シートのリンクキー一致"),
        ("new_by_email_id", email_id, "新管理表のパフォーマンスリンク一致"),
        ("legacy_by_email_id", email_id, "旧管理表のパフォーマンスリンク一致"),
    ]
    subject_key = normalize_management_subject(subject)
    lookup_steps.append(("progress_by_subject_dt", f"{send_dt.date().isoformat()}|{send_dt.strftime('%H:%M')}|{subject_key}", "進捗シートの件名+送信日時一致"))
    lookup_steps.append(("progress_by_subject_only", subject_key, "進捗シートの件名一致"))

    for bucket_name, lookup_key, label in lookup_steps:
        candidates = management_index[bucket_name].get(lookup_key, []) if lookup_key else []
        if len(candidates) == 1:
            return candidates[0], label
        if len(candidates) > 1:
            ambiguity_reasons.append(f"{label}が{len(candidates)}件")

    if ambiguity_reasons:
        return None, " / ".join(ambiguity_reasons)
    return None, "管理シート未登録"


def match_management_row(
    management_index: dict,
    email_id: str,
    raw_email_name: str,
    subject: str,
    send_dt: dt.datetime,
) -> Optional[dict]:
    row, _ = resolve_management_row(
        management_index=management_index,
        email_id=email_id,
        raw_email_name=raw_email_name,
        subject=subject,
        send_dt=send_dt,
    )
    return row


def hubspot_email_url(email_id: str) -> str:
    return f"https://app.hubspot.com/email/{PORTAL_ID}/details/{email_id}/performance"


def escape_formula_text(text: str) -> str:
    return (text or "").replace('"', '""')


def event_name_to_japanese(event_name: str) -> str:
    ev = (event_name or "").strip()
    if not ev:
        return ev
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

    # GA4 側で運用されている和名イベント
    if "営業特別イベント" in ev:
        return "営業特別イベント"
    if "マーケティング特別イベント" in ev:
        return "マーケティング特別イベント"
    if "セミナー予約" in ev:
        return "セミナー予約"
    if "個別相談" in ev:
        return "個別相談予約"
    # 「見積り依頼」と「資料請求」は別CVとして扱う。
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


def parse_cv_breakdown_to_japanese(raw: str, cv_total: Optional[int] = None) -> str:
    if not raw:
        return ""
    pairs: List[Tuple[str, int]] = []
    parts = [x.strip() for x in raw.split("|") if x.strip()]
    for part in parts:
        if ":" not in part:
            continue
        name, count = part.rsplit(":", 1)
        try:
            n = int(float(count.strip()))
        except Exception:
            continue
        pairs.append((name.strip(), n))

    if not pairs:
        return ""

    # If specific cv_* events exist, generic lead/form events are often duplicated signals.
    has_specific = any(name.startswith("cv_") for name, _ in pairs)
    if has_specific:
        pairs = [
            (name, n)
            for name, n in pairs
            if name not in {"generate_lead", "form_submit", "form_start"}
        ]

    acc: Dict[str, int] = defaultdict(int)
    for name, n in pairs:
        jp = event_name_to_japanese(name)
        acc[jp] += n
    if not acc:
        if cv_total and cv_total > 0:
            return f"その他キーイベント：{cv_total}"
        return ""

    sum_after = sum(acc.values())
    if cv_total is not None and cv_total > sum_after:
        acc["その他キーイベント"] += cv_total - sum_after

    lines = [f"{k}：{v}" for k, v in sorted(acc.items(), key=lambda x: (-x[1], x[0]))]
    return "\n".join(lines)


def load_ga4_map(path: str) -> Dict[str, dict]:
    import csv

    out: Dict[str, dict] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email_id = str(row.get("email_id", "")).strip()
            if not email_id:
                continue
            out[email_id] = row
    return out


def validate_ga4_map_bundle(csv_path: str, manifest_path: str, month: str, max_age_minutes: int) -> dict:
    if not os.path.exists(csv_path):
        raise SystemExit(f"GA4 map csv not found: {csv_path}")
    if not manifest_path:
        manifest_path = derive_ga4_map_manifest_path(csv_path)
    if not os.path.exists(manifest_path):
        raise SystemExit(f"GA4 map manifest not found: {manifest_path}")

    manifest = load_json(manifest_path)
    if str(manifest.get("month", "")) != month:
        raise SystemExit(
            f"GA4 map manifest month mismatch: expected {month}, got {manifest.get('month')!r}"
        )

    expected_sha = str(manifest.get("detail_csv_sha256", "")).strip()
    actual_sha = sha256_file(csv_path)
    if not expected_sha or expected_sha != actual_sha:
        raise SystemExit("GA4 map csv hash mismatch. Regenerate the GA4 map before staging writes.")

    generated_at_raw = str(manifest.get("generated_at", "")).strip()
    if not generated_at_raw:
        raise SystemExit("GA4 map manifest missing generated_at.")
    generated_at = parse_iso_datetime(generated_at_raw).astimezone(JST)
    age_minutes = (now_jst() - generated_at).total_seconds() / 60.0
    if age_minutes > max_age_minutes:
        raise SystemExit(
            f"GA4 map bundle is stale ({age_minutes:.1f} minutes old > {max_age_minutes} minutes)."
        )

    return manifest


class HubSpotClient:
    def __init__(self, token: str):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )
        self._bot_included_open_cache: Dict[str, set[str]] = {}

    def get_with_retry(self, url: str, *, params: Optional[dict] = None, timeout: int = 120, retryable_statuses: Optional[set[int]] = None):
        retryable_statuses = retryable_statuses or {429, 500, 502, 503, 504}
        last_error: Optional[Exception] = None
        for attempt in range(5):
            try:
                resp = self.session.get(url, params=params, timeout=timeout)
            except request_exceptions.RequestException as exc:
                last_error = exc
                if attempt == 4:
                    raise
                time.sleep(2 * (attempt + 1))
                continue
            if resp.status_code in retryable_statuses:
                if attempt == 4:
                    resp.raise_for_status()
                time.sleep(2 * (attempt + 1))
                continue
            return resp
        if last_error:
            raise last_error
        raise RuntimeError(f"Request failed without response: {url}")

    def fetch_marketing_emails(
        self,
        published_after_iso: str,
        published_before_iso: str,
        include_stats: bool = True,
        email_type: str = "BATCH_EMAIL",
    ) -> List[dict]:
        url = f"{BASE_URL}/marketing/v3/emails"
        params = {
            "limit": 100,
            "isPublished": "true",
            "includeStats": str(include_stats).lower(),
            "publishedAfter": published_after_iso,
            "publishedBefore": published_before_iso,
            "sort": "-publishedAt",
            "type": email_type,
        }
        results: List[dict] = []
        while True:
            resp = self.get_with_retry(url, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("results", []))
            after = ((data.get("paging") or {}).get("next") or {}).get("after")
            if not after:
                break
            params["after"] = after
        return results

    def fetch_all_legacy_lists(self) -> Dict[str, dict]:
        url = f"{BASE_URL}/contacts/v1/lists"
        offset = 0
        result: Dict[str, dict] = {}
        while True:
            resp = self.get_with_retry(url, params={"count": 250, "offset": offset}, timeout=90)
            resp.raise_for_status()
            data = resp.json()
            for row in data.get("lists", []):
                lid = str(row.get("listId"))
                result[lid] = {
                    "id": lid,
                    "name": row.get("name", ""),
                    "dynamic": row.get("dynamic"),
                    "size": ((row.get("metaData") or {}).get("size")),
                    "source": "legacy",
                }
            if not data.get("has-more"):
                break
            offset = data.get("offset")
            if not offset:
                break
        return result

    def fetch_crm_list_by_id(self, list_id: str) -> Optional[dict]:
        url = f"{BASE_URL}/crm/v3/lists/{list_id}"
        resp = self.get_with_retry(url, timeout=60, retryable_statuses={500, 502, 503, 504})
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        row = data.get("list") or {}
        if not row:
            return None
        return {
            "id": str(row.get("listId", list_id)),
            "name": row.get("name", ""),
            "dynamic": row.get("processingType") == "DYNAMIC",
            "size": row.get("size"),
            "source": "crm_v3",
        }

    def fetch_open_recipients_including_bots(self, campaign_id: str) -> set[str]:
        campaign_key = str(campaign_id or "").strip()
        if not campaign_key:
            return set()
        if campaign_key in self._bot_included_open_cache:
            return self._bot_included_open_cache[campaign_key]

        recipients = set()
        offset = None
        while True:
            params = {
                "campaignId": campaign_key,
                "eventType": "OPEN",
                "limit": 1000,
            }
            if offset:
                params["offset"] = offset
            resp = self.get_with_retry(f"{BASE_URL}/email/public/v1/events", params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            for event in data.get("events", []) or []:
                recipient = str(event.get("recipient", "")).strip().lower()
                if recipient:
                    recipients.add(recipient)
            if not data.get("hasMore"):
                break
            offset = data.get("offset")
            if not offset:
                break

        self._bot_included_open_cache[campaign_key] = recipients
        return recipients

    def fetch_unique_opened_recipients_including_bots(self, campaign_ids: List[str]) -> int:
        recipients: set[str] = set()
        for campaign_id in campaign_ids:
            recipients.update(self.fetch_open_recipients_including_bots(campaign_id))
        return len(recipients)


def extract_campaign_ids(email_obj: dict) -> List[str]:
    internal_ids = email_obj.get("allEmailCampaignIds") or []
    if not internal_ids and email_obj.get("primaryEmailCampaignId"):
        internal_ids = [str(email_obj.get("primaryEmailCampaignId"))]
    return [str(x) for x in internal_ids if x is not None and str(x) != ""]


def write_staging_tabs(
    service_account_json: str,
    spreadsheet_id: str,
    course_tabs: Dict[str, List[List]],
) -> None:
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(service_account_json, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)

    for tab_name, rows in course_tabs.items():
        title = staging_tab_title(tab_name)
        ws = ensure_worksheet(sh, title, max(200, len(rows) + 30), len(COURSE_SHEET_HEADER) + 3)
        write_sheet_values(ws, [COURSE_SHEET_HEADER] + rows)
        set_worksheet_hidden(sh, ws, True)


def main() -> None:
    args = parse_args()
    token = (args.hubspot_token or "").strip()
    if not token:
        raise SystemExit("HubSpot token is missing. Set HUBSPOT_PAT or pass --hubspot-token.")
    if not os.path.exists(args.service_account_json):
        raise SystemExit(f"Service account json not found: {args.service_account_json}")

    if not args.skip_sheet:
        validate_ga4_map_bundle(
            csv_path=args.ga4_map_csv,
            manifest_path=(args.ga4_map_manifest or derive_ga4_map_manifest_path(args.ga4_map_csv)),
            month=args.month,
            max_age_minutes=args.max_ga4_map_age_minutes,
        )
    elif not os.path.exists(args.ga4_map_csv):
        raise SystemExit(f"GA4 map csv not found: {args.ga4_map_csv}")

    ga4_map = load_ga4_map(args.ga4_map_csv)
    management_index = load_management_index(args.service_account_json, args.month)

    start, end = month_bounds_utc(args.month)
    start_iso = to_iso_utc(start - dt.timedelta(days=EMAIL_FETCH_MARGIN_DAYS))
    end_iso = to_iso_utc(end + dt.timedelta(days=EMAIL_FETCH_MARGIN_DAYS))
    month_start_jst = dt.datetime.strptime(args.month + "-01", "%Y-%m-%d").replace(tzinfo=JST)
    if month_start_jst.month == 12:
        month_end_jst = month_start_jst.replace(year=month_start_jst.year + 1, month=1)
    else:
        month_end_jst = month_start_jst.replace(month=month_start_jst.month + 1)

    client = HubSpotClient(token)
    emails = client.fetch_marketing_emails(start_iso, end_iso, include_stats=True, email_type=args.email_type)

    legacy_lists = client.fetch_all_legacy_lists()
    list_map = dict(legacy_lists)
    all_ref_ids = set()
    for e in emails:
        to = e.get("to") or {}
        all_ref_ids.update(str(x) for x in ((to.get("contactLists") or {}).get("include") or []) if x is not None)
        all_ref_ids.update(str(x) for x in ((to.get("contactIlsLists") or {}).get("include") or []) if x is not None)
    unknown_ids = sorted(x for x in all_ref_ids if x and x not in list_map)
    for lid in unknown_ids:
        row = client.fetch_crm_list_by_id(lid)
        if row:
            list_map[lid] = row

    course_rows: Dict[str, List[List]] = {c: [] for c in TARGET_COURSES}
    matched_management_uids: set[str] = set()

    for e in emails:
        send_raw = pick_send_datetime_raw(e)
        published_jst_dt = parse_published_at_jst_dt(send_raw)
        if not published_jst_dt:
            continue
        # Strictly keep emails sent in target month in JST.
        if not (month_start_jst <= published_jst_dt < month_end_jst):
            continue

        email_id = str(e.get("id", ""))
        raw_email_name = e.get("name", "") or ""
        subject = e.get("subject", "") or ""

        to = e.get("to") or {}
        include_contact_lists = (to.get("contactLists") or {}).get("include") or []
        include_ils_lists = (to.get("contactIlsLists") or {}).get("include") or []
        include_ids = sorted({str(x) for x in include_contact_lists + include_ils_lists if x is not None and str(x) != ""})

        list_names = []
        for lid in include_ids:
            if lid in list_map and list_map[lid].get("name"):
                list_names.append(list_map[lid]["name"])
        # remove duplicates while preserving order
        list_names = list(dict.fromkeys(list_names))

        management_row, management_match_reason = resolve_management_row(
            management_index=management_index,
            email_id=email_id,
            raw_email_name=raw_email_name,
            subject=subject,
            send_dt=published_jst_dt,
        )
        internal_ids = extract_campaign_ids(e)
        internal_ids_text = ",".join(internal_ids)
        hs_link = hubspot_email_url(email_id)
        subject_formula = f'=HYPERLINK("{hs_link}","{escape_formula_text(subject)}")'
        send_date_text = as_literal_text(parse_published_at_jst(send_raw))

        if not management_row:
            email_name = raw_email_name
            course = detect_course_for_unregistered(raw_email_name, list_names, subject)
            if course not in TARGET_COURSES:
                continue
        else:
            email_name = management_row.get("link_key") or raw_email_name
            course = detect_course(email_name, list_names, subject)
            if course not in TARGET_COURSES:
                course = detect_course_for_unregistered(raw_email_name, list_names, subject)
            if course not in TARGET_COURSES:
                continue
            matched_management_uids.add(management_row["management_uid"])

        if course not in TARGET_COURSES:
            continue

        segment = classify_segment(email_name, list_names)
        stats = e.get("stats") or {}
        counters = stats.get("counters") or {}
        delivered = safe_num(counters.get("delivered"))
        opened = safe_num(counters.get("open"))
        clicked = safe_num(counters.get("click"))
        unsubscribed = safe_num(counters.get("unsubscribed"))

        g = ga4_map.get(email_id, {})
        ga4_key_events = int(float(g.get("ga4_keyEvents") or 0))
        ga4_breakdown_raw = g.get("ga4_cv_event_breakdown") or ""
        ga4_breakdown_jp = parse_cv_breakdown_to_japanese(ga4_breakdown_raw, ga4_key_events)

        opened_including_bots = client.fetch_unique_opened_recipients_including_bots(internal_ids)

        open_rate_text = as_literal_text(rate_pct_2dp_text(opened, delivered))
        open_rate_including_bots_text = as_literal_text(rate_pct_2dp_text(opened_including_bots, delivered))
        click_rate_text = as_literal_text(rate_pct_2dp_text(clicked, delivered))
        unsub_rate_text = as_literal_text(rate_pct_2dp_text(unsubscribed, delivered))
        click_through_rate_text = as_literal_text(rate_pct_2dp_text(clicked, opened))

        course_rows[course].append(
            [
                send_date_text,
                subject_formula,
                email_name,
                int(delivered),
                int(opened),
                int(opened_including_bots),
                int(clicked),
                int(unsubscribed),
                open_rate_text,
                open_rate_including_bots_text,
                click_rate_text,
                click_through_rate_text,
                unsub_rate_text,
                ga4_key_events,
                ga4_breakdown_jp,
                " | ".join(list_names),
                internal_ids_text,
                args.month,
                course,
            ]
        )

    for c in TARGET_COURSES:
        # Sort primarily by send datetime so each course tab reads chronologically.
        course_rows[c].sort(key=lambda x: x[0])

    for c in TARGET_COURSES:
        print(f"{c}_rows={len(course_rows[c])}")

    if args.skip_sheet:
        import csv

        for c in TARGET_COURSES:
            out = f"test_hubspot_{c}_{args.month}.csv"
            with open(out, "w", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f)
                w.writerow(COURSE_SHEET_HEADER)
                w.writerows(course_rows[c])
            print(f"output_csv=./{out}")
        return

    write_staging_tabs(
        service_account_json=args.service_account_json,
        spreadsheet_id=args.spreadsheet_id,
        course_tabs=course_rows,
    )
    print(f"staging_sheet_updated=https://docs.google.com/spreadsheets/d/{args.spreadsheet_id}/edit")


if __name__ == "__main__":
    main()
