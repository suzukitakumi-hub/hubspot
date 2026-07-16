#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import html
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import parse_qs, urlsplit


JST = dt.timezone(dt.timedelta(hours=9))
NUMERIC_CAMPAIGN_ID_RE = re.compile(r"^\d+$")
CAMPAIGN_ID_SPLIT_RE = re.compile(r"[,|\s]+")
EMAIL_CONTENT_ID_RE = re.compile(r"/details/(\d+)/performance")
FINAL_STATUS = "確定"
PROVISIONAL_STATUS = "集計中（送信後30日未満）"


@dataclass(frozen=True)
class FormRule:
    guid: str
    name: str
    category: str


@dataclass(frozen=True)
class FormRegistry:
    included: Mapping[str, FormRule]
    excluded: Mapping[str, Mapping[str, str]]
    unavailable_excluded: Mapping[str, Mapping[str, str]]
    category_order: Sequence[str]
    meetings_routes: Mapping[str, str]
    known_non_meetings_counseling_routes: frozenset[str]
    meetings_detection_minimum: int


@dataclass(frozen=True)
class EmailRow:
    content_id: str
    course: str
    sheet_row: int
    send_at: dt.datetime
    email_name: str
    campaign_ids: tuple[str, ...]
    old_count: str
    old_breakdown: str


@dataclass(frozen=True)
class SubmissionEvidence:
    form_guid: str
    form_name: str
    conversion_id: str
    submitted_at: dt.datetime
    page_hsmi_ids: tuple[str, ...]
    page_utm_content_ids: tuple[str, ...]
    hidden_utm_content_ids: tuple[str, ...]
    page_utm_sources: tuple[str, ...]
    page_utm_media: tuple[str, ...]


@dataclass(frozen=True)
class Attribution:
    status: str
    campaign_id: str = ""
    page_ids: tuple[str, ...] = ()
    hidden_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class EmailCvResult:
    email: EmailRow
    count: int
    breakdown: str
    status: str
    category_counts: Mapping[str, int]


@dataclass
class CvComputation:
    results: list[EmailCvResult]
    accepted_submission_count: int
    accepted_before_window_filter: int
    accepted_by_category: Counter[str]
    outside_window_count: int
    pre_send_count: int
    tolerated_pre_send_count: int
    duplicate_count: int
    hidden_only_count: int
    hidden_only_by_form: Counter[str]
    no_page_attribution_count: int
    conflict_count: int
    unverified_page_utm_count: int
    unknown_page_attributed_count: int
    unknown_page_attributed_forms: Counter[str]
    excluded_page_attributed_count: int
    excluded_page_attributed_forms: Counter[str]
    missing_conversion_id_count: int
    sheet_campaign_id_count: int

    @property
    def blocking_issue_count(self) -> int:
        return (
            self.pre_send_count
            + self.duplicate_count
            + self.conflict_count
            + self.unverified_page_utm_count
            + self.unknown_page_attributed_count
            + self.missing_conversion_id_count
        )


def load_registry(path: str | Path) -> FormRegistry:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if int(raw.get("schema_version") or 0) != 1:
        raise ValueError("Unsupported form registry schema_version")

    included: dict[str, FormRule] = {}
    for guid, item in (raw.get("included_forms") or {}).items():
        guid_text = str(guid).strip().lower()
        name = str((item or {}).get("name") or "").strip()
        category = str((item or {}).get("category") or "").strip()
        if not guid_text or not name or not category:
            raise ValueError(f"Invalid included form rule: {guid!r}")
        included[guid_text] = FormRule(guid=guid_text, name=name, category=category)

    excluded = {
        str(guid).strip().lower(): {
            "name": str((item or {}).get("name") or "").strip(),
            "reason": str((item or {}).get("reason") or "").strip(),
        }
        for guid, item in (raw.get("excluded_forms") or {}).items()
        if str(guid).strip()
    }
    overlap = sorted(set(included) & set(excluded))
    if overlap:
        raise ValueError(f"Forms cannot be both included and excluded: {overlap}")

    category_order = [str(value).strip() for value in (raw.get("category_order") or []) if str(value).strip()]
    missing_categories = sorted({rule.category for rule in included.values()} - set(category_order))
    if missing_categories:
        raise ValueError(f"category_order is missing: {missing_categories}")
    unavailable_excluded = {
        str(guid).strip().lower(): {
            "name": str((item or {}).get("name") or "").strip(),
            "reason": str((item or {}).get("reason") or "").strip(),
        }
        for guid, item in (raw.get("unavailable_excluded_forms") or {}).items()
        if str(guid).strip()
    }
    meetings_routes = {
        normalize_destination_url(route): normalize_destination_url(expected_embed)
        for route, expected_embed in (raw.get("meetings_routes") or {}).items()
        if normalize_destination_url(route) and normalize_destination_url(expected_embed)
    }
    known_non_meetings_counseling_routes = frozenset(
        normalize_destination_url(value)
        for value in (raw.get("known_non_meetings_counseling_routes") or [])
        if normalize_destination_url(value)
    )
    meetings_detection_minimum = int(raw.get("meetings_detection_minimum") or 0)
    if meetings_detection_minimum < 0:
        raise ValueError("meetings_detection_minimum must be non-negative")
    return FormRegistry(
        included=included,
        excluded=excluded,
        unavailable_excluded=unavailable_excluded,
        category_order=category_order,
        meetings_routes=meetings_routes,
        known_non_meetings_counseling_routes=known_non_meetings_counseling_routes,
        meetings_detection_minimum=meetings_detection_minimum,
    )


def normalize_destination_url(value: Any) -> str:
    decoded = html.unescape("" if value is None else str(value)).strip()
    if not decoded:
        return ""
    try:
        parsed = urlsplit(decoded)
    except ValueError:
        return ""
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return ""
    path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"


def active_marketing_email_urls(raw_email: Mapping[str, Any]) -> frozenset[str]:
    content = raw_email.get("content") or {}
    active_widget_ids: set[str] = set()

    def collect_widget_references(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                if str(key) == "widgets" and isinstance(child, list):
                    active_widget_ids.update(str(item) for item in child if str(item).strip())
                collect_widget_references(child)
        elif isinstance(value, list):
            for child in value:
                collect_widget_references(child)

    collect_widget_references(content.get("flexAreas") or {})
    widgets = content.get("widgets") or {}
    urls: set[str] = set()
    for widget_id in active_widget_ids:
        widget = widgets.get(widget_id) or {}
        body = widget.get("body") or {}
        for field in ("destination", "link"):
            normalized = normalize_destination_url(body.get(field))
            if normalized:
                urls.add(normalized)
    return frozenset(urls)


def strip_sheet_literal(value: Any) -> str:
    text = "" if value is None else str(value)
    return text[1:] if text.startswith("'") else text


def normalize_header(value: Any) -> str:
    text = strip_sheet_literal(value).replace("\n", "").strip()
    aliases = {
        "フォーム送信数（送信後30日）": "CV数",
        "フォーム別内訳": "CV内訳",
    }
    return aliases.get(text, text)


def parse_send_at(value: Any) -> dt.datetime:
    text = strip_sheet_literal(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return dt.datetime.strptime(text, fmt).replace(tzinfo=JST)
        except ValueError:
            pass
    parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=JST)
    return parsed.astimezone(JST)


def parse_campaign_ids(value: Any) -> tuple[str, ...]:
    text = strip_sheet_literal(value).strip()
    ids = {
        part
        for part in CAMPAIGN_ID_SPLIT_RE.split(text)
        if part and NUMERIC_CAMPAIGN_ID_RE.fullmatch(part)
    }
    return tuple(sorted(ids, key=int))


def parse_email_content_id(value: Any) -> str:
    text = strip_sheet_literal(value).strip()
    match = EMAIL_CONTENT_ID_RE.search(text)
    if not match:
        raise ValueError("メール件名（HubSpotリンク）からcontent IDを取得できません")
    return match.group(1)


def parse_email_rows(course: str, matrix: Sequence[Sequence[Any]]) -> list[EmailRow]:
    if not matrix:
        raise ValueError(f"{course}: empty sheet")
    header = [normalize_header(value) for value in matrix[0]]
    required = [
        "送付日",
        "メール件名（HubSpotリンク）",
        "メール内部名",
        "CV数",
        "CV内訳",
        "INTERNAL HUBSPOT IDS",
    ]
    missing = [name for name in required if name not in header]
    if missing:
        raise ValueError(f"{course}: missing headers: {missing}")
    indexes = {name: header.index(name) for name in required}

    rows: list[EmailRow] = []
    for sheet_row, raw_row in enumerate(matrix[1:], start=2):
        row = list(raw_row)
        if not any(str(value).strip() for value in row):
            continue
        def get(name: str) -> Any:
            index = indexes[name]
            return row[index] if index < len(row) else ""

        campaign_ids = parse_campaign_ids(get("INTERNAL HUBSPOT IDS"))
        if not campaign_ids:
            raise ValueError(f"{course}!{sheet_row}: INTERNAL HUBSPOT IDS is empty or non-numeric")
        rows.append(
            EmailRow(
                content_id=parse_email_content_id(get("メール件名（HubSpotリンク）")),
                course=course,
                sheet_row=sheet_row,
                send_at=parse_send_at(get("送付日")),
                email_name=strip_sheet_literal(get("メール内部名")).strip(),
                campaign_ids=campaign_ids,
                old_count=strip_sheet_literal(get("CV数")).strip(),
                old_breakdown=strip_sheet_literal(get("CV内訳")).strip(),
            )
        )
    return rows


def _numeric_values(values: Iterable[Any]) -> tuple[str, ...]:
    out: set[str] = set()
    for raw in values:
        text = "" if raw is None else str(raw).strip()
        if NUMERIC_CAMPAIGN_ID_RE.fullmatch(text):
            out.add(text)
    return tuple(sorted(out, key=int))


def _page_query(page_url: str) -> Mapping[str, list[str]]:
    decoded = html.unescape(page_url or "")
    try:
        query_text = urlsplit(decoded).query
    except ValueError:
        query_text = decoded.split("?", 1)[1] if "?" in decoded else decoded
    # Some legacy records contain a damaged URL without "?" even though the
    # query parameters remain. Recover only the attribution-related tail.
    if not query_text:
        lowered = decoded.lower()
        positions = [
            position
            for key in ("_hsmi", "utm_content", "utm_source", "utm_medium")
            if (position := lowered.find(key + "=")) >= 0
        ]
        if positions:
            query_text = decoded[min(positions):]
    query = parse_qs(query_text, keep_blank_values=False)
    return {str(key).lower(): list(values) for key, values in query.items()}


def submission_from_api(form_guid: str, form_name: str, raw: Mapping[str, Any]) -> SubmissionEvidence:
    query = _page_query(str(raw.get("pageUrl") or ""))
    page_hsmi = _numeric_values(query.get("_hsmi") or [])
    page_utm = _numeric_values(query.get("utm_content") or [])
    hidden_utm = _numeric_values(
        item.get("value")
        for item in (raw.get("values") or [])
        if str((item or {}).get("name") or "").strip() == "utm_content"
    )
    submitted_at_ms = int(raw.get("submittedAt"))
    submitted_at = dt.datetime.fromtimestamp(submitted_at_ms / 1000.0, tz=dt.timezone.utc).astimezone(JST)
    return SubmissionEvidence(
        form_guid=str(form_guid).strip().lower(),
        form_name=str(form_name).strip(),
        conversion_id=str(raw.get("conversionId") or "").strip(),
        submitted_at=submitted_at,
        page_hsmi_ids=page_hsmi,
        page_utm_content_ids=page_utm,
        hidden_utm_content_ids=hidden_utm,
        page_utm_sources=tuple(sorted({str(value).strip().lower() for value in query.get("utm_source") or [] if str(value).strip()})),
        page_utm_media=tuple(sorted({str(value).strip().lower() for value in query.get("utm_medium") or [] if str(value).strip()})),
    )


def attribute_submission(submission: SubmissionEvidence) -> Attribution:
    hsmi_ids = tuple(sorted(set(submission.page_hsmi_ids), key=int))
    page_utm_ids = tuple(sorted(set(submission.page_utm_content_ids), key=int))
    page_ids = tuple(sorted(set(hsmi_ids) | set(page_utm_ids), key=int))
    hidden_ids = tuple(sorted(set(submission.hidden_utm_content_ids), key=int))
    if len(page_ids) > 1 or len(hidden_ids) > 1:
        return Attribution(status="conflict", page_ids=page_ids, hidden_ids=hidden_ids)
    if page_ids and hidden_ids and page_ids[0] != hidden_ids[0]:
        return Attribution(status="conflict", page_ids=page_ids, hidden_ids=hidden_ids)
    if len(hsmi_ids) == 1:
        return Attribution(status="page", campaign_id=page_ids[0], page_ids=page_ids, hidden_ids=hidden_ids)
    if len(page_utm_ids) == 1:
        email_source = "hs_email" in submission.page_utm_sources
        email_medium = "email" in submission.page_utm_media
        if email_source or email_medium:
            return Attribution(status="page", campaign_id=page_utm_ids[0], page_ids=page_ids, hidden_ids=hidden_ids)
        return Attribution(status="page_utm_unverified", campaign_id=page_utm_ids[0], page_ids=page_ids, hidden_ids=hidden_ids)
    if len(hidden_ids) == 1:
        return Attribution(status="hidden_only", campaign_id=hidden_ids[0], hidden_ids=hidden_ids)
    return Attribution(status="none")


def format_breakdown(
    category_counts: Mapping[str, int],
    category_order: Sequence[str],
    status: str,
    *,
    meetings_unattributable: bool = False,
) -> str:
    lines: list[str] = []
    for category in category_order:
        count = int(category_counts.get(category) or 0)
        if count:
            lines.append(f"{category}：{count}")
    for category in sorted(set(category_counts) - set(category_order)):
        count = int(category_counts.get(category) or 0)
        if count:
            lines.append(f"{category}：{count}")
    if meetings_unattributable:
        lines.append("カウンセリング予約：メール別確定不可（Meetings）")
    if status == PROVISIONAL_STATUS:
        lines.append(PROVISIONAL_STATUS)
    return "\n".join(lines)


def compute_email_cv(
    email_rows: Sequence[EmailRow],
    submissions: Iterable[SubmissionEvidence],
    registry: FormRegistry,
    *,
    now: dt.datetime,
    window_days: int = 30,
    start_tolerance_minutes: int = 5,
    meetings_email_content_ids: Iterable[str] = (),
) -> CvComputation:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if start_tolerance_minutes < 0:
        raise ValueError("start_tolerance_minutes must be non-negative")
    now_jst = now.astimezone(JST)
    meetings_content_ids = frozenset(str(value).strip() for value in meetings_email_content_ids)

    campaign_to_email: dict[str, EmailRow] = {}
    for email in email_rows:
        for campaign_id in email.campaign_ids:
            existing = campaign_to_email.get(campaign_id)
            if existing and existing != email:
                raise ValueError(
                    f"Campaign ID {campaign_id} is duplicated: "
                    f"{existing.course}!{existing.sheet_row}, {email.course}!{email.sheet_row}"
                )
            campaign_to_email[campaign_id] = email

    counts_by_email: dict[tuple[str, int], Counter[str]] = defaultdict(Counter)
    accepted_keys: set[tuple[str, str]] = set()
    accepted_submission_count = 0
    accepted_before_window_filter = 0
    accepted_by_category: Counter[str] = Counter()
    outside_window_count = 0
    pre_send_count = 0
    tolerated_pre_send_count = 0
    duplicate_count = 0
    hidden_only_count = 0
    hidden_only_by_form: Counter[str] = Counter()
    no_page_attribution_count = 0
    conflict_count = 0
    unverified_page_utm_count = 0
    unknown_page_attributed_count = 0
    unknown_page_attributed_forms: Counter[str] = Counter()
    excluded_page_attributed_count = 0
    excluded_page_attributed_forms: Counter[str] = Counter()
    missing_conversion_id_count = 0

    for submission in submissions:
        attribution = attribute_submission(submission)
        if attribution.status == "conflict":
            if set(attribution.page_ids) & set(campaign_to_email) or set(attribution.hidden_ids) & set(campaign_to_email):
                conflict_count += 1
            continue
        if attribution.status == "page_utm_unverified":
            if attribution.campaign_id in campaign_to_email:
                unverified_page_utm_count += 1
            continue
        if attribution.status == "hidden_only":
            if attribution.campaign_id in campaign_to_email:
                hidden_only_count += 1
                hidden_only_by_form[submission.form_guid] += 1
            continue
        if attribution.status == "none":
            no_page_attribution_count += 1
            continue

        email = campaign_to_email.get(attribution.campaign_id)
        if not email:
            continue
        rule = registry.included.get(submission.form_guid)
        if not rule:
            if submission.form_guid in registry.excluded:
                excluded_page_attributed_count += 1
                excluded_page_attributed_forms[submission.form_guid] += 1
            else:
                unknown_page_attributed_count += 1
                unknown_page_attributed_forms[submission.form_guid] += 1
            continue
        if not submission.conversion_id:
            missing_conversion_id_count += 1
            continue

        unique_key = (submission.form_guid, submission.conversion_id)
        if unique_key in accepted_keys:
            duplicate_count += 1
            continue
        accepted_keys.add(unique_key)

        earliest_allowed = email.send_at - dt.timedelta(minutes=start_tolerance_minutes)
        if submission.submitted_at < earliest_allowed:
            pre_send_count += 1
            continue
        if submission.submitted_at < email.send_at:
            tolerated_pre_send_count += 1
        accepted_before_window_filter += 1
        window_end = email.send_at + dt.timedelta(days=window_days)
        if submission.submitted_at > window_end or submission.submitted_at > now_jst:
            outside_window_count += 1
            continue

        counts_by_email[email.content_id][rule.category] += 1
        accepted_by_category[rule.category] += 1
        accepted_submission_count += 1

    results: list[EmailCvResult] = []
    for email in sorted(email_rows, key=lambda item: (item.course, item.sheet_row)):
        category_counts = counts_by_email.get(email.content_id, Counter())
        status = PROVISIONAL_STATUS if now_jst < email.send_at + dt.timedelta(days=window_days) else FINAL_STATUS
        total = sum(category_counts.values())
        meetings_unattributable = email.content_id in meetings_content_ids
        results.append(
            EmailCvResult(
                email=email,
                count=total,
                breakdown=format_breakdown(
                    category_counts,
                    registry.category_order,
                    status,
                    meetings_unattributable=meetings_unattributable,
                ),
                status=status,
                category_counts=dict(category_counts),
            )
        )

    return CvComputation(
        results=results,
        accepted_submission_count=accepted_submission_count,
        accepted_before_window_filter=accepted_before_window_filter,
        accepted_by_category=accepted_by_category,
        outside_window_count=outside_window_count,
        pre_send_count=pre_send_count,
        tolerated_pre_send_count=tolerated_pre_send_count,
        duplicate_count=duplicate_count,
        hidden_only_count=hidden_only_count,
        hidden_only_by_form=hidden_only_by_form,
        no_page_attribution_count=no_page_attribution_count,
        conflict_count=conflict_count,
        unverified_page_utm_count=unverified_page_utm_count,
        unknown_page_attributed_count=unknown_page_attributed_count,
        unknown_page_attributed_forms=unknown_page_attributed_forms,
        excluded_page_attributed_count=excluded_page_attributed_count,
        excluded_page_attributed_forms=excluded_page_attributed_forms,
        missing_conversion_id_count=missing_conversion_id_count,
        sheet_campaign_id_count=len(campaign_to_email),
    )
