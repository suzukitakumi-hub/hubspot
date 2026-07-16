#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import html
import json
import os
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import requests
from requests import exceptions as request_exceptions

from hubspot_course_sheet_guardrails import (
    DEFAULT_SERVICE_ACCOUNT_JSON,
    DEFAULT_SPREADSHEET_ID,
    FORM_CV_BREAKDOWN_HEADER,
    FORM_CV_COUNT_HEADER,
    TARGET_COURSES,
    ensure_worksheet,
    set_worksheet_hidden,
    sheets_call,
)
from hubspot_form_cv import (
    FINAL_STATUS,
    JST,
    PROVISIONAL_STATUS,
    CvComputation,
    EmailCvResult,
    EmailRow,
    FormRegistry,
    SubmissionEvidence,
    active_marketing_email_urls,
    compute_email_cv,
    load_registry,
    normalize_destination_url,
    parse_email_rows,
    submission_from_api,
)


BASE_URL = "https://api.hubapi.com"
DEFAULT_REGISTRY = Path(__file__).resolve().parent / "config" / "hubspot_form_cv_registry.json"
AUDIT_TAB = "__formcv__監査"
USAGE_TAB = "使い方・更新仕様"
COUNT_HEADER = FORM_CV_COUNT_HEADER
BREAKDOWN_HEADER = FORM_CV_BREAKDOWN_HEADER


@dataclass(frozen=True)
class FormMeta:
    guid: str
    name: str
    archived: bool
    form_type: str


@dataclass
class FormFetchResult:
    form: FormMeta
    submissions: list[SubmissionEvidence]
    scanned_records: int
    page_count: int
    reached_start: bool
    ordering_violation: bool
    error: str = ""
    unavailable_excluded: bool = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replace the course-sheet GA4 CV columns with directly attributed HubSpot form submissions."
    )
    parser.add_argument("--spreadsheet-id", default=os.environ.get("HUBSPOT_COURSE_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID))
    parser.add_argument(
        "--service-account-json",
        default=os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", DEFAULT_SERVICE_ACCOUNT_JSON),
    )
    parser.add_argument("--hubspot-token", default=os.environ.get("HUBSPOT_PAT", ""), help=argparse.SUPPRESS)
    parser.add_argument("--registry", default=str(DEFAULT_REGISTRY))
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--start-tolerance-minutes", type=int, default=5)
    parser.add_argument("--max-workers", type=int, default=5)
    parser.add_argument("--max-pages-per-form", type=int, default=2000)
    parser.add_argument("--min-scanned-records", type=int, default=0)
    parser.add_argument("--min-counted-submissions", type=int, default=0)
    parser.add_argument("--write-audit-tab", action="store_true", default=False)
    parser.add_argument("--apply", action="store_true", default=False)
    parser.add_argument("--update-usage", action="store_true", default=False)
    parser.add_argument(
        "--expect-campaign-count",
        action="append",
        default=[],
        metavar="CAMPAIGN_ID=COUNT",
        help="Optional fail-closed representative check. May be specified more than once.",
    )
    parser.add_argument("--output", default="")
    return parser.parse_args()


def parse_expected_checks(values: Iterable[str]) -> dict[str, int]:
    checks: dict[str, int] = {}
    for raw in values:
        if "=" not in raw:
            raise SystemExit(f"Invalid --expect-campaign-count: {raw!r}")
        campaign_id, count_text = raw.split("=", 1)
        campaign_id = campaign_id.strip()
        if not campaign_id.isdigit():
            raise SystemExit(f"Invalid campaign ID in --expect-campaign-count: {raw!r}")
        checks[campaign_id] = int(count_text)
    return checks


class HubSpotClient:
    def __init__(self, token: str):
        self.token = token
        self.local = threading.local()

    def session(self) -> requests.Session:
        session = getattr(self.local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({"Authorization": f"Bearer {self.token}"})
            self.local.session = session
        return session

    def get_json(self, url: str, *, params: Mapping[str, Any] | None = None, timeout: int = 90) -> dict:
        last_error: Exception | None = None
        for attempt in range(1, 9):
            try:
                response = self.session().get(url, params=params, timeout=timeout)
            except request_exceptions.RequestException as error:
                last_error = error
                if attempt >= 8:
                    raise RuntimeError(f"HubSpot request failed: {url}") from error
                time.sleep(min(60, 2 ** attempt))
                continue
            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt >= 8:
                    raise RuntimeError(f"HubSpot {response.status_code}: {url}")
                retry_after = response.headers.get("Retry-After")
                sleep_seconds = float(retry_after) if retry_after and retry_after.replace(".", "", 1).isdigit() else min(60, 2 ** attempt)
                time.sleep(sleep_seconds)
                continue
            if response.status_code >= 400:
                error = RuntimeError(f"HubSpot {response.status_code}: {url}")
                setattr(error, "status_code", response.status_code)
                raise error
            return response.json()
        if last_error:
            raise last_error
        raise RuntimeError(f"HubSpot request failed without response: {url}")

    def fetch_forms(self, archived: bool) -> list[FormMeta]:
        forms: list[FormMeta] = []
        after = ""
        while True:
            params: dict[str, Any] = {"limit": 100, "archived": str(archived).lower()}
            if after:
                params["after"] = after
            data = self.get_json(f"{BASE_URL}/marketing/v3/forms", params=params)
            for raw in data.get("results") or []:
                forms.append(
                    FormMeta(
                        guid=str(raw.get("id") or "").strip().lower(),
                        name=str(raw.get("name") or "").strip(),
                        archived=bool(raw.get("archived")),
                        form_type=str(raw.get("formType") or "").strip(),
                    )
                )
            after = str((((data.get("paging") or {}).get("next") or {}).get("after") or "")).strip()
            if not after:
                break
        return forms

    def fetch_marketing_emails(
        self,
        *,
        published_after: dt.datetime,
        published_before: dt.datetime,
    ) -> list[dict[str, Any]]:
        def iso_utc(value: dt.datetime) -> str:
            return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")

        params: dict[str, Any] = {
            "limit": 100,
            "isPublished": "true",
            "includeStats": "false",
            "publishedAfter": iso_utc(published_after),
            "publishedBefore": iso_utc(published_before),
            "sort": "-publishedAt",
            "type": "BATCH_EMAIL",
        }
        emails: list[dict[str, Any]] = []
        while True:
            data = self.get_json(f"{BASE_URL}/marketing/v3/emails", params=params)
            emails.extend(data.get("results") or [])
            after = str((((data.get("paging") or {}).get("next") or {}).get("after") or "")).strip()
            if not after:
                break
            params["after"] = after
        return emails

    def fetch_form_submissions(
        self,
        form: FormMeta,
        *,
        earliest_needed: dt.datetime,
        max_pages: int,
        registry: FormRegistry,
    ) -> FormFetchResult:
        after = ""
        submissions: list[SubmissionEvidence] = []
        scanned_records = 0
        page_count = 0
        ordering_violation = False
        previous_page_oldest: dt.datetime | None = None
        reached_start = False

        try:
            while True:
                page_count += 1
                if page_count > max_pages:
                    return FormFetchResult(
                        form=form,
                        submissions=submissions,
                        scanned_records=scanned_records,
                        page_count=page_count - 1,
                        reached_start=False,
                        ordering_violation=ordering_violation,
                        error=f"page_cap_reached:{max_pages}",
                    )
                params: dict[str, Any] = {"limit": 50}
                if after:
                    params["after"] = after
                data = self.get_json(
                    f"{BASE_URL}/form-integrations/v1/submissions/forms/{form.guid}",
                    params=params,
                )
                raw_results = list(data.get("results") or [])
                scanned_records += len(raw_results)
                page_evidence = [submission_from_api(form.guid, form.name, raw) for raw in raw_results]
                page_times = [item.submitted_at for item in page_evidence]
                if page_times:
                    if page_times != sorted(page_times, reverse=True):
                        ordering_violation = True
                    if previous_page_oldest and max(page_times) > previous_page_oldest:
                        ordering_violation = True
                    previous_page_oldest = min(page_times)
                submissions.extend(item for item in page_evidence if item.submitted_at >= earliest_needed)

                next_after = str((((data.get("paging") or {}).get("next") or {}).get("after") or "")).strip()
                if page_times and max(page_times) < earliest_needed and not ordering_violation:
                    reached_start = True
                    break
                if not next_after:
                    reached_start = True
                    break
                after = next_after
        except Exception as error:
            status_code = getattr(error, "status_code", None)
            unavailable_excluded = bool(
                status_code == 404
                and form.archived
                and form.guid in registry.unavailable_excluded
            )
            return FormFetchResult(
                form=form,
                submissions=submissions,
                scanned_records=scanned_records,
                page_count=page_count,
                reached_start=False,
                ordering_violation=ordering_violation,
                error=f"{type(error).__name__}:{error}",
                unavailable_excluded=unavailable_excluded,
            )

        return FormFetchResult(
            form=form,
            submissions=submissions,
            scanned_records=scanned_records,
            page_count=page_count,
            reached_start=reached_start,
            ordering_violation=ordering_violation,
        )


def open_spreadsheet(service_account_json: str, spreadsheet_id: str):
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(service_account_json, scopes=scopes)
    return gspread.authorize(credentials).open_by_key(spreadsheet_id)


def read_course_rows(
    spreadsheet,
    worksheets: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, list[list[Any]]], list[EmailRow]]:
    if worksheets is None:
        worksheets = {worksheet.title: worksheet for worksheet in sheets_call("worksheets", spreadsheet.worksheets)}
    ranges: list[str] = []
    for course in TARGET_COURSES:
        worksheet = worksheets.get(course)
        if not worksheet:
            raise RuntimeError(f"Missing course sheet: {course}")
        ranges.append(f"'{course}'!A1:S{max(worksheet.row_count, 1)}")
    response = sheets_call(
        "course_formula_batch_read",
        lambda: spreadsheet.values_batch_get(ranges, params={"valueRenderOption": "FORMULA"}),
    )
    value_ranges = response.get("valueRanges") or []
    if len(value_ranges) != len(TARGET_COURSES):
        raise RuntimeError(
            f"Course sheet batch read returned {len(value_ranges)} ranges; expected {len(TARGET_COURSES)}"
        )
    matrices: dict[str, list[list[Any]]] = {}
    email_rows: list[EmailRow] = []
    seen_content_ids: dict[str, tuple[str, int]] = {}
    for index, course in enumerate(TARGET_COURSES):
        matrix = list((value_ranges[index] or {}).get("values") or [])
        matrices[course] = matrix
        parsed = parse_email_rows(course, matrix)
        for email in parsed:
            previous = seen_content_ids.get(email.content_id)
            if previous:
                raise RuntimeError(
                    f"Marketing email content ID {email.content_id} is duplicated: "
                    f"{previous[0]}!{previous[1]}, {email.course}!{email.sheet_row}"
                )
            seen_content_ids[email.content_id] = (email.course, email.sheet_row)
        email_rows.extend(parsed)
    return worksheets, matrices, email_rows


def email_identity_snapshot(email_rows: Iterable[EmailRow]) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        (
            email.course,
            email.sheet_row,
            email.content_id,
            email.campaign_ids,
            email.send_at.isoformat(),
        )
        for email in email_rows
    )


def form_value_snapshot(email_rows: Iterable[EmailRow]) -> tuple[tuple[str, str, str], ...]:
    return tuple((email.content_id, email.old_count, email.old_breakdown) for email in email_rows)


def assert_prewrite_snapshot_unchanged(
    expected_rows: list[EmailRow],
    current_rows: list[EmailRow],
) -> None:
    if email_identity_snapshot(expected_rows) != email_identity_snapshot(current_rows):
        raise RuntimeError("Course-sheet email identity changed during HubSpot fetch; live update aborted")
    if form_value_snapshot(expected_rows) != form_value_snapshot(current_rows):
        raise RuntimeError("Course-sheet form values changed during HubSpot fetch; live update aborted")


def marketing_email_campaign_ids(raw_email: Mapping[str, Any]) -> tuple[str, ...]:
    raw_ids = raw_email.get("allEmailCampaignIds") or []
    if not raw_ids and raw_email.get("primaryEmailCampaignId") is not None:
        raw_ids = [raw_email.get("primaryEmailCampaignId")]
    if not isinstance(raw_ids, (list, tuple, set)):
        raw_ids = [raw_ids]
    ids = {
        str(value).strip()
        for value in raw_ids
        if str(value).strip().isdigit()
    }
    return tuple(sorted(ids, key=int))


def is_counseling_like_route(url: str) -> bool:
    lowered = url.lower()
    return any(marker in lowered for marker in ("counsel", "consult", "soudan", "sodan"))


def audit_marketing_email_routes(
    email_rows: list[EmailRow],
    raw_emails: Iterable[Mapping[str, Any]],
    registry: FormRegistry,
) -> tuple[frozenset[str], dict[str, Any], list[dict[str, Any]]]:
    by_content_id: dict[str, Mapping[str, Any]] = {}
    issues: list[dict[str, Any]] = []
    duplicate_ids: set[str] = set()
    for raw in raw_emails:
        content_id = str(raw.get("id") or "").strip()
        if not content_id:
            continue
        if content_id in by_content_id:
            duplicate_ids.add(content_id)
        by_content_id[content_id] = raw
    for content_id in sorted(duplicate_ids, key=int):
        issues.append({"code": "duplicate_marketing_email_detail", "content_id": content_id})

    meeting_content_ids: set[str] = set()
    missing_details: list[str] = []
    unknown_routes: Counter[str] = Counter()
    campaign_mismatches = 0
    meeting_routes = set(registry.meetings_routes)
    known_non_meetings = set(registry.known_non_meetings_counseling_routes)
    for email in email_rows:
        raw = by_content_id.get(email.content_id)
        if raw is None:
            missing_details.append(email.content_id)
            issues.append(
                {
                    "code": "marketing_email_detail_missing",
                    "content_id": email.content_id,
                    "course": email.course,
                }
            )
            continue
        actual_campaign_ids = marketing_email_campaign_ids(raw)
        if actual_campaign_ids != email.campaign_ids:
            campaign_mismatches += 1
            issues.append(
                {
                    "code": "marketing_email_campaign_ids_mismatch",
                    "content_id": email.content_id,
                    "course": email.course,
                    "sheet_campaign_ids": list(email.campaign_ids),
                    "api_campaign_ids": list(actual_campaign_ids),
                }
            )
        urls = active_marketing_email_urls(raw)
        matched_meeting_routes = sorted(set(urls) & meeting_routes)
        if len(matched_meeting_routes) > 1:
            issues.append(
                {
                    "code": "multiple_meetings_routes_in_email",
                    "content_id": email.content_id,
                    "routes": matched_meeting_routes,
                }
            )
        if matched_meeting_routes:
            meeting_content_ids.add(email.content_id)
        for url in urls:
            if is_counseling_like_route(url) and url not in meeting_routes and url not in known_non_meetings:
                unknown_routes[url] += 1

    for route, count in sorted(unknown_routes.items()):
        issues.append({"code": "unknown_counseling_route", "route": route, "email_count": count})
    if len(meeting_content_ids) < registry.meetings_detection_minimum:
        issues.append(
            {
                "code": "meetings_detection_below_minimum",
                "expected_minimum": registry.meetings_detection_minimum,
                "actual": len(meeting_content_ids),
            }
        )
    audit = {
        "fetched_email_details": len(by_content_id),
        "sheet_email_details_found": len(email_rows) - len(missing_details),
        "missing_email_details": len(missing_details),
        "campaign_id_mismatches": campaign_mismatches,
        "meetings_email_count": len(meeting_content_ids),
        "meetings_by_course": dict(Counter(email.course for email in email_rows if email.content_id in meeting_content_ids)),
        "unknown_counseling_routes": dict(unknown_routes),
    }
    return frozenset(meeting_content_ids), audit, issues


def validate_meetings_landing_pages(registry: FormRegistry) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    session = requests.Session()
    session.headers.update({"User-Agent": "Abitus-HubSpot-Form-CV-Audit/1.0"})
    for route, expected_embed in registry.meetings_routes.items():
        try:
            response = session.get(route, timeout=45)
            response.raise_for_status()
            body = html.unescape(response.text)
        except request_exceptions.RequestException as error:
            issues.append(
                {
                    "code": "meetings_landing_page_fetch_error",
                    "route": route,
                    "error": type(error).__name__,
                }
            )
            continue
        normalized_expected = normalize_destination_url(expected_embed)
        if expected_embed not in body and normalized_expected not in body:
            issues.append(
                {
                    "code": "meetings_embed_mismatch",
                    "route": route,
                    "expected_embed": normalized_expected,
                }
            )
    return issues


def fetch_all_submission_evidence(
    client: HubSpotClient,
    registry: FormRegistry,
    email_rows: list[EmailRow],
    *,
    max_workers: int,
    max_pages_per_form: int,
) -> tuple[list[FormMeta], list[FormFetchResult], list[SubmissionEvidence]]:
    active = client.fetch_forms(archived=False)
    archived = client.fetch_forms(archived=True)
    forms_by_guid = {form.guid: form for form in active + archived if form.guid}
    for guid, rule in registry.included.items():
        forms_by_guid.setdefault(guid, FormMeta(guid=guid, name=rule.name, archived=False, form_type="registry"))
    for guid, item in registry.excluded.items():
        forms_by_guid.setdefault(guid, FormMeta(guid=guid, name=item.get("name", ""), archived=False, form_type="registry"))
    for guid, item in registry.unavailable_excluded.items():
        forms_by_guid.setdefault(guid, FormMeta(guid=guid, name=item.get("name", ""), archived=True, form_type="registry"))

    earliest_needed = min(email.send_at for email in email_rows) - dt.timedelta(days=1)
    forms = sorted(forms_by_guid.values(), key=lambda form: (form.archived, form.guid))
    results: list[FormFetchResult] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        futures = {
            executor.submit(
                client.fetch_form_submissions,
                form,
                earliest_needed=earliest_needed,
                max_pages=max_pages_per_form,
                registry=registry,
            ): form
            for form in forms
        }
        for completed_count, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            results.append(future.result())
            if completed_count % 20 == 0 or completed_count == len(futures):
                print(f"forms_fetched={completed_count}/{len(futures)}", flush=True)
    results.sort(key=lambda item: item.form.guid)
    submissions = [submission for result in results for submission in result.submissions]
    return forms, results, submissions


def integer_or_zero(value: str) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def result_by_campaign(results: list[EmailCvResult]) -> dict[str, EmailCvResult]:
    out: dict[str, EmailCvResult] = {}
    for result in results:
        for campaign_id in result.email.campaign_ids:
            existing = out.get(campaign_id)
            if existing and existing.email.content_id != result.email.content_id:
                raise RuntimeError(f"Campaign ID duplicated in results: {campaign_id}")
            out[campaign_id] = result
    return out


def build_audit_rows(results: list[EmailCvResult]) -> list[list[Any]]:
    course_index = {course: index for index, course in enumerate(TARGET_COURSES)}
    rows: list[list[Any]] = []
    for result in sorted(results, key=lambda item: (course_index[item.email.course], item.email.sheet_row)):
        old_count = integer_or_zero(result.email.old_count)
        rows.append(
            [
                result.email.content_id,
                result.email.course,
                result.email.sheet_row,
                result.email.send_at.strftime("%Y-%m-%d %H:%M:%S"),
                result.email.email_name,
                ",".join(result.email.campaign_ids),
                old_count,
                result.email.old_breakdown,
                result.count,
                result.breakdown,
                result.status,
                result.count - old_count,
            ]
        )
    return rows


def write_audit_tab(
    spreadsheet,
    rows: list[list[Any]],
    *,
    generated_at_jst: str,
    data_cutoff: dt.datetime,
    blocking_issue_count: int,
) -> None:
    header = [
        "マーケティングメールID",
        "講座",
        "元シート行",
        "送付日",
        "メール内部名",
        "INTERNAL HUBSPOT IDS",
        "旧CV数（GA4）",
        "旧CV内訳（GA4）",
        COUNT_HEADER,
        BREAKDOWN_HEADER,
        "集計状態",
        "差分",
        "監査生成日時（JST）",
        "データ基準日時（JST）",
        "停止条件件数",
    ]
    enriched_rows: list[list[Any]] = []
    for index, row in enumerate(rows):
        metadata = ["", "", ""]
        if index == 0:
            metadata = [
                "'" + generated_at_jst,
                "'" + data_cutoff.astimezone(JST).isoformat(),
                blocking_issue_count,
            ]
        enriched_rows.append(list(row) + metadata)
    expected_values = [header] + enriched_rows
    worksheet = ensure_worksheet(spreadsheet, AUDIT_TAB, max(800, len(rows) + 20), len(header))
    set_worksheet_hidden(spreadsheet, worksheet, True)
    required_rows = max(800, len(expected_values) + 20)
    if worksheet.row_count < required_rows or worksheet.col_count < len(header):
        sheets_call(
            f"{AUDIT_TAB}.resize",
            lambda: worksheet.resize(
                rows=max(worksheet.row_count, required_rows),
                cols=max(worksheet.col_count, len(header)),
            ),
        )
    sheets_call(f"{AUDIT_TAB}.clear", worksheet.clear)
    sheets_call(
        f"{AUDIT_TAB}.update",
        lambda: worksheet.update(expected_values, value_input_option="USER_ENTERED"),
    )
    sheets_call(f"{AUDIT_TAB}.freeze", lambda: worksheet.freeze(rows=1))
    readback = sheets_call(
        f"{AUDIT_TAB}.verify",
        lambda: worksheet.get(
            f"A1:O{len(expected_values)}",
            value_render_option="UNFORMATTED_VALUE",
        ),
    )
    if not readback or list(readback[0]) != header:
        raise RuntimeError("Form CV audit-tab header readback mismatch")
    for index, expected in enumerate(enriched_rows, start=2):
        actual = readback[index - 1] if index - 1 < len(readback) else []
        for column in (0, 8, 9, 10, 11):
            actual_value = actual[column] if column < len(actual) else ""
            if str(actual_value) != str(expected[column]):
                raise RuntimeError(f"Form CV audit-tab readback mismatch at row {index}")
    if enriched_rows:
        metadata_row = readback[1] if len(readback) > 1 else []
        expected_metadata = [generated_at_jst, data_cutoff.astimezone(JST).isoformat(), blocking_issue_count]
        actual_metadata = [metadata_row[index] if index < len(metadata_row) else "" for index in (12, 13, 14)]
        if [str(value) for value in actual_metadata] != [str(value) for value in expected_metadata]:
            raise RuntimeError("Form CV audit-tab metadata readback mismatch")


def usage_updates(data_cutoff: dt.datetime) -> list[dict[str, Any]]:
    date_text = data_cutoff.astimezone(JST).strftime("%Y-%m-%d")
    return [
        # Prefix with an apostrophe so Sheets keeps the ISO date as display text.
        {"range": f"'{USAGE_TAB}'!B2", "values": [["'" + date_text]]},
        {
            "range": f"'{USAGE_TAB}'!B4",
            "values": [["各講座の担当者が、HubSpotメールの配信実績と、メール経由のHubSpot標準フォーム送信数を講座別に確認するためのレポートです。"]],
        },
        {
            "range": f"'{USAGE_TAB}'!B10",
            "values": [["メール実績は当月分を更新します。フォーム送信数は送信後30日を確定させるため、全講座の全掲載メールを毎回再集計します。"]],
        },
        {
            "range": f"'{USAGE_TAB}'!B15",
            "values": [["HubSpot標準フォームのpage URLに残るメールIDと、メールのINTERNAL HUBSPOT IDSを完全一致で照合します。"]],
        },
        {
            "range": f"'{USAGE_TAB}'!B16",
            "values": [["メール実績をstagingタブで検証して各講座へ反映した後、フォーム送信数を別監査し、N/O列だけ更新します。"]],
        },
        {
            "range": f"'{USAGE_TAB}'!A22:B22",
            "values": [["HubSpot標準フォーム", "フォーム送信数、フォーム別内訳（メールID一致、送信日時の5分前から30日後まで）"]],
        },
        {
            "range": f"'{USAGE_TAB}'!A32:B32",
            "values": [[COUNT_HEADER + " / " + BREAKDOWN_HEADER, "同じメールIDをpage URLに保持したHubSpot標準フォーム送信を、フォームGUIDとconversionIdの組み合わせで重複排除して表示します。"]],
        },
        {
            "range": f"'{USAGE_TAB}'!A35:B38",
            "values": [
                ["6. フォーム送信数の見方", ""],
                ["0件", "条件に合う標準フォーム送信が0件です。送信から30日未満の行は、フォーム別内訳に「集計中」と表示します。"],
                ["カウンセリング", "USCPA/MBAの現行予約導線はHubSpot Meetingsのため、この数値には含みません。メール別に確定できない行は「メール別確定不可（Meetings）」と表示します。"],
                ["イベント", "イベント系はフォーム予約数です。実際の出席者数ではありません。"],
            ],
        },
    ]


def build_live_payload(
    results: list[EmailCvResult],
    matrices: Mapping[str, list[list[Any]]],
    *,
    include_usage: bool,
    usage_matrix: list[list[Any]] | None,
    data_cutoff: dt.datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    by_course: dict[str, list[EmailCvResult]] = defaultdict(list)
    for result in results:
        by_course[result.email.course].append(result)

    write_data: list[dict[str, Any]] = []
    rollback_data: list[dict[str, Any]] = []
    for course in TARGET_COURSES:
        course_results = sorted(by_course[course], key=lambda item: item.email.sheet_row)
        expected_rows = list(range(2, len(course_results) + 2))
        actual_rows = [item.email.sheet_row for item in course_results]
        if actual_rows != expected_rows:
            raise RuntimeError(f"{course}: non-contiguous data rows: {actual_rows[:5]} ... {actual_rows[-5:]}")
        end_row = len(course_results) + 1
        write_values = [[COUNT_HEADER, BREAKDOWN_HEADER]] + [[item.count, item.breakdown] for item in course_results]
        write_data.append({"range": f"'{course}'!N1:O{end_row}", "values": write_values})

        matrix = matrices[course]
        rollback_values: list[list[Any]] = []
        for row_number in range(1, end_row + 1):
            raw_row = matrix[row_number - 1] if row_number - 1 < len(matrix) else []
            rollback_values.append([
                raw_row[13] if len(raw_row) > 13 else "",
                raw_row[14] if len(raw_row) > 14 else "",
            ])
        rollback_data.append({"range": f"'{course}'!N1:O{end_row}", "values": rollback_values})

    if include_usage:
        if usage_matrix is None:
            raise RuntimeError("Usage-sheet rollback data is missing")
        write_data.extend(usage_updates(data_cutoff))
        rollback_values: list[list[Any]] = []
        for row_number in range(1, 46):
            raw_row = usage_matrix[row_number - 1] if row_number - 1 < len(usage_matrix) else []
            rollback_values.append(
                [
                    raw_row[0] if len(raw_row) > 0 else "",
                    raw_row[1] if len(raw_row) > 1 else "",
                ]
            )
        rollback_data.append({"range": f"'{USAGE_TAB}'!A1:B45", "values": rollback_values})
    return (
        {"valueInputOption": "USER_ENTERED", "data": write_data},
        {"valueInputOption": "USER_ENTERED", "data": rollback_data},
    )


def build_keyed_rollback_payload(
    current_email_rows: list[EmailRow],
    results: list[EmailCvResult],
    original_matrices: Mapping[str, list[list[Any]]],
    *,
    include_usage: bool,
    usage_matrix: list[list[Any]] | None,
) -> dict[str, Any]:
    backup_by_content_id = {
        result.email.content_id: (result.email.old_count, result.email.old_breakdown)
        for result in results
    }
    data: list[dict[str, Any]] = []
    for course in TARGET_COURSES:
        original = original_matrices.get(course) or []
        header = original[0] if original else []
        data.append(
            {
                "range": f"'{course}'!N1:O1",
                "values": [[
                    header[13] if len(header) > 13 else "CV数",
                    header[14] if len(header) > 14 else "CV内訳",
                ]],
            }
        )
    for email in current_email_rows:
        backup = backup_by_content_id.get(email.content_id)
        if backup is None:
            continue
        data.append(
            {
                "range": f"'{email.course}'!N{email.sheet_row}:O{email.sheet_row}",
                "values": [[backup[0], backup[1]]],
            }
        )
    if include_usage and usage_matrix is not None:
        rollback_values: list[list[Any]] = []
        for row_number in range(1, 46):
            raw_row = usage_matrix[row_number - 1] if row_number - 1 < len(usage_matrix) else []
            rollback_values.append(
                [
                    raw_row[0] if len(raw_row) > 0 else "",
                    raw_row[1] if len(raw_row) > 1 else "",
                ]
            )
        data.append({"range": f"'{USAGE_TAB}'!A1:B45", "values": rollback_values})
    return {"valueInputOption": "USER_ENTERED", "data": data}


def verify_live_values(
    matrices: Mapping[str, list[list[Any]]],
    expected_email_rows: list[EmailRow],
    actual_email_rows: list[EmailRow],
    results: list[EmailCvResult],
) -> list[dict[str, Any]]:
    by_course: dict[str, list[EmailCvResult]] = defaultdict(list)
    for result in results:
        by_course[result.email.course].append(result)
    issues: list[dict[str, Any]] = []
    if email_identity_snapshot(expected_email_rows) != email_identity_snapshot(actual_email_rows):
        issues.append({"code": "post_write_email_identity_mismatch"})
        return issues
    for course in TARGET_COURSES:
        course_results = sorted(by_course[course], key=lambda item: item.email.sheet_row)
        values = matrices.get(course) or []
        header = values[0] if values else []
        if len(header) < 15 or [header[13], header[14]] != [COUNT_HEADER, BREAKDOWN_HEADER]:
            issues.append({"course": course, "code": "header_mismatch"})
            continue
        for result in course_results:
            index = result.email.sheet_row - 1
            row = values[index] if index < len(values) else []
            raw_count = row[13] if len(row) > 13 else None
            numeric_count = (
                not isinstance(raw_count, bool)
                and isinstance(raw_count, (int, float))
                and float(raw_count).is_integer()
            )
            actual_count = int(raw_count) if numeric_count else None
            actual_breakdown = str(row[14]) if len(row) > 14 else ""
            if not numeric_count or actual_count != result.count or actual_breakdown != result.breakdown:
                issues.append(
                    {
                        "course": course,
                        "content_id": result.email.content_id,
                        "code": "value_mismatch",
                        "expected_count": result.count,
                        "actual_count": actual_count,
                        "actual_count_type": type(raw_count).__name__,
                    }
                )
    return issues


def rollback_backup(results: list[EmailCvResult]) -> list[dict[str, Any]]:
    return [
        {
            "content_id": result.email.content_id,
            "course": result.email.course,
            "sheet_row": result.email.sheet_row,
            "campaign_ids": list(result.email.campaign_ids),
            "old_count": result.email.old_count,
            "old_breakdown": result.email.old_breakdown,
        }
        for result in results
    ]


def verify_usage_values(
    worksheet: Any,
    data_cutoff: dt.datetime,
) -> list[dict[str, Any]]:
    matrix = sheets_call(
        f"{USAGE_TAB}.verify_form_cv_notes",
        lambda: worksheet.get("A1:B38", value_render_option="FORMATTED_VALUE"),
    )
    expected_cells: dict[tuple[int, int], str] = {}
    for update in usage_updates(data_cutoff):
        cell_range = str(update["range"]).split("!", 1)[1]
        start, _, end = cell_range.partition(":")

        def coordinate(cell: str) -> tuple[int, int]:
            column_text = "".join(character for character in cell if character.isalpha()).upper()
            row_text = "".join(character for character in cell if character.isdigit())
            column = 0
            for character in column_text:
                column = column * 26 + (ord(character) - ord("A") + 1)
            return int(row_text), column

        start_row, start_column = coordinate(start)
        end_row, end_column = coordinate(end or start)
        values = update["values"]
        for row_offset, row_number in enumerate(range(start_row, end_row + 1)):
            for column_offset, column_number in enumerate(range(start_column, end_column + 1)):
                raw_expected = values[row_offset][column_offset]
                expected = str(raw_expected)
                if expected.startswith("'"):
                    expected = expected[1:]
                expected_cells[(row_number, column_number)] = expected

    issues: list[dict[str, Any]] = []
    for (row_number, column_number), expected in expected_cells.items():
        raw_row = matrix[row_number - 1] if row_number - 1 < len(matrix) else []
        actual = str(raw_row[column_number - 1]) if column_number - 1 < len(raw_row) else ""
        if actual != expected:
            issues.append(
                {
                    "course": USAGE_TAB,
                    "cell": f"{chr(ord('A') + column_number - 1)}{row_number}",
                    "code": "usage_value_mismatch",
                }
            )
    return issues


def registry_sha256(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def build_report(
    *,
    args: argparse.Namespace,
    registry: FormRegistry,
    forms: list[FormMeta],
    fetch_results: list[FormFetchResult],
    computation: CvComputation,
    email_rows: list[EmailRow],
    data_cutoff: dt.datetime,
    expected_checks: Mapping[str, int],
    expected_issues: list[dict[str, Any]],
    marketing_email_audit: Mapping[str, Any],
    apply_issues: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    form_name_by_guid = {form.guid: form.name for form in forms}
    blocking_fetch_errors = [
        result
        for result in fetch_results
        if result.error and not result.unavailable_excluded
    ]
    unavailable_warnings = [result for result in fetch_results if result.unavailable_excluded]
    ordering_issues = [result for result in fetch_results if result.ordering_violation and not result.reached_start]
    changed_rows = [result for result in computation.results if integer_or_zero(result.email.old_count) != result.count or result.email.old_breakdown != result.breakdown]
    per_course_rows = Counter(email.course for email in email_rows)
    per_course_new_total = Counter(result.email.course for result in computation.results for _ in range(result.count))
    provisional_rows = sum(result.status == PROVISIONAL_STATUS for result in computation.results)
    blocking_issue_count = (
        computation.blocking_issue_count
        + len(blocking_fetch_errors)
        + len(ordering_issues)
        + len(expected_issues)
        + len(apply_issues or [])
    )
    return {
        "schema_version": 1,
        "generated_at_jst": dt.datetime.now(JST).isoformat(),
        "data_cutoff_jst": data_cutoff.astimezone(JST).isoformat(),
        "mode": "apply" if args.apply else ("staging" if args.write_audit_tab else "dry_run"),
        "spreadsheet_id": args.spreadsheet_id,
        "registry_path": str(Path(args.registry).resolve()),
        "registry_sha256": registry_sha256(args.registry),
        "window_days": args.window_days,
        "start_tolerance_minutes": args.start_tolerance_minutes,
        "minimum_scanned_records": args.min_scanned_records,
        "minimum_counted_submissions": args.min_counted_submissions,
        "blocking_issue_count": blocking_issue_count,
        "sheet": {
            "email_rows": len(email_rows),
            "unique_content_ids": len({email.content_id for email in email_rows}),
            "campaign_ids": computation.sheet_campaign_id_count,
            "per_course_rows": dict(per_course_rows),
            "changed_rows": len(changed_rows),
            "provisional_rows": provisional_rows,
            "per_course_form_submissions": dict(per_course_new_total),
        },
        "registry": {
            "included_forms": len(registry.included),
            "excluded_forms": len(registry.excluded),
            "unavailable_excluded_forms": len(registry.unavailable_excluded),
            "meetings_routes": len(registry.meetings_routes),
            "known_non_meetings_counseling_routes": len(registry.known_non_meetings_counseling_routes),
            "meetings_detection_minimum": registry.meetings_detection_minimum,
        },
        "marketing_emails": dict(marketing_email_audit),
        "forms": {
            "listed": len(forms),
            "fetched": len(fetch_results),
            "scanned_records": sum(result.scanned_records for result in fetch_results),
            "pages": sum(result.page_count for result in fetch_results),
            "blocking_fetch_errors": [
                {"guid": result.form.guid, "name": result.form.name, "error": result.error}
                for result in blocking_fetch_errors
            ],
            "known_unavailable_warnings": [
                {"guid": result.form.guid, "name": result.form.name, "error": result.error}
                for result in unavailable_warnings
            ],
            "ordering_issues": [
                {"guid": result.form.guid, "name": result.form.name}
                for result in ordering_issues
            ],
        },
        "submissions": {
            "page_attributed_included_before_window": computation.accepted_before_window_filter,
            "counted_within_window": computation.accepted_submission_count,
            "by_category": dict(computation.accepted_by_category),
            "outside_30_day_window": computation.outside_window_count,
            "tolerated_pre_send": computation.tolerated_pre_send_count,
            "pre_send_before_tolerance": computation.pre_send_count,
            "duplicates": computation.duplicate_count,
            "hidden_only_not_counted": computation.hidden_only_count,
            "hidden_only_by_form": {
                guid: {"name": form_name_by_guid.get(guid, ""), "count": count}
                for guid, count in computation.hidden_only_by_form.items()
            },
            "no_page_attribution": computation.no_page_attribution_count,
            "conflicts": computation.conflict_count,
            "unverified_page_utm": computation.unverified_page_utm_count,
            "unknown_page_attributed": computation.unknown_page_attributed_count,
            "unknown_page_attributed_forms": {
                guid: {"name": form_name_by_guid.get(guid, ""), "count": count}
                for guid, count in computation.unknown_page_attributed_forms.items()
            },
            "excluded_page_attributed": computation.excluded_page_attributed_count,
            "excluded_page_attributed_forms": {
                guid: {"name": form_name_by_guid.get(guid, ""), "count": count}
                for guid, count in computation.excluded_page_attributed_forms.items()
            },
            "missing_conversion_id": computation.missing_conversion_id_count,
        },
        "representative_checks": {
            "requested": dict(expected_checks),
            "issues": expected_issues,
        },
        "apply_readback_issues": apply_issues or [],
        "largest_count_differences": [
            {
                "content_id": result.email.content_id,
                "course": result.email.course,
                "sheet_row": result.email.sheet_row,
                "campaign_ids": list(result.email.campaign_ids),
                "old_count": integer_or_zero(result.email.old_count),
                "new_count": result.count,
                "difference": result.count - integer_or_zero(result.email.old_count),
                "status": result.status,
            }
            for result in sorted(
                changed_rows,
                key=lambda item: abs(item.count - integer_or_zero(item.email.old_count)),
                reverse=True,
            )[:50]
        ],
    }


def default_output_path(data_cutoff: dt.datetime) -> Path:
    timestamp = data_cutoff.astimezone(JST).strftime("%Y%m%d_%H%M%S")
    return Path("logs") / "form_cv_updates" / f"hubspot_form_cv_{timestamp}.json"


def write_report(path: Path, report: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    if args.min_scanned_records < 0 or args.min_counted_submissions < 0:
        raise SystemExit("Minimum safety thresholds must be non-negative")
    if args.apply:
        args.write_audit_tab = True
    token = (args.hubspot_token or "").strip()
    if not token:
        raise SystemExit("HUBSPOT_PAT is missing")
    if not Path(args.service_account_json).exists():
        raise SystemExit(f"Service account JSON not found: {args.service_account_json}")
    registry = load_registry(args.registry)
    expected_checks = parse_expected_checks(args.expect_campaign_count)
    data_cutoff = dt.datetime.now(JST)
    output_path = Path(args.output) if args.output else default_output_path(data_cutoff)

    spreadsheet = open_spreadsheet(args.service_account_json, args.spreadsheet_id)
    worksheets, matrices, email_rows = read_course_rows(spreadsheet)
    usage_matrix: list[list[Any]] | None = None
    if args.update_usage:
        usage_worksheet = worksheets.get(USAGE_TAB)
        if not usage_worksheet:
            raise RuntimeError(f"Missing usage sheet: {USAGE_TAB}")
        usage_matrix = sheets_call(
            f"{USAGE_TAB}.read_rollback_matrix",
            lambda: usage_worksheet.get("A1:B45", value_render_option="FORMULA"),
        )
    print(f"sheet_email_rows={len(email_rows)}", flush=True)

    client = HubSpotClient(token)
    raw_marketing_emails = client.fetch_marketing_emails(
        published_after=min(email.send_at for email in email_rows) - dt.timedelta(days=1),
        published_before=data_cutoff + dt.timedelta(minutes=5),
    )
    meetings_email_content_ids, marketing_email_audit, marketing_email_issues = audit_marketing_email_routes(
        email_rows,
        raw_marketing_emails,
        registry,
    )
    meetings_landing_page_issues = validate_meetings_landing_pages(registry)
    forms, fetch_results, submissions = fetch_all_submission_evidence(
        client,
        registry,
        email_rows,
        max_workers=args.max_workers,
        max_pages_per_form=args.max_pages_per_form,
    )
    computation = compute_email_cv(
        email_rows,
        submissions,
        registry,
        now=data_cutoff,
        window_days=args.window_days,
        start_tolerance_minutes=args.start_tolerance_minutes,
        meetings_email_content_ids=meetings_email_content_ids,
    )
    by_campaign = result_by_campaign(computation.results)
    expected_issues: list[dict[str, Any]] = list(marketing_email_issues) + list(meetings_landing_page_issues)
    for campaign_id, expected_count in expected_checks.items():
        result = by_campaign.get(campaign_id)
        if not result:
            expected_issues.append({"campaign_id": campaign_id, "code": "campaign_not_found"})
        elif result.count != expected_count:
            expected_issues.append(
                {
                    "campaign_id": campaign_id,
                    "code": "count_mismatch",
                    "expected": expected_count,
                    "actual": result.count,
                    "content_id": result.email.content_id,
                }
            )
    scanned_records = sum(result.scanned_records for result in fetch_results)
    if scanned_records < args.min_scanned_records:
        expected_issues.append(
            {
                "code": "scanned_records_below_minimum",
                "expected_minimum": args.min_scanned_records,
                "actual": scanned_records,
            }
        )
    if computation.accepted_submission_count < args.min_counted_submissions:
        expected_issues.append(
            {
                "code": "counted_submissions_below_minimum",
                "expected_minimum": args.min_counted_submissions,
                "actual": computation.accepted_submission_count,
            }
        )

    preliminary_report = build_report(
        args=args,
        registry=registry,
        forms=forms,
        fetch_results=fetch_results,
        computation=computation,
        email_rows=email_rows,
        data_cutoff=data_cutoff,
        expected_checks=expected_checks,
        expected_issues=expected_issues,
        marketing_email_audit=marketing_email_audit,
    )
    if args.apply:
        preliminary_report["rollback_backup"] = rollback_backup(computation.results)
        if args.update_usage:
            preliminary_report["usage_rollback_backup"] = usage_matrix or []
    write_report(output_path, preliminary_report)
    print(f"report={output_path.resolve()}")
    print(f"counted_submissions={computation.accepted_submission_count}")
    print(f"blocking_issue_count={preliminary_report['blocking_issue_count']}")

    if args.write_audit_tab:
        write_audit_tab(
            spreadsheet,
            build_audit_rows(computation.results),
            generated_at_jst=str(preliminary_report["generated_at_jst"]),
            data_cutoff=data_cutoff,
            blocking_issue_count=int(preliminary_report["blocking_issue_count"]),
        )
        print(f"audit_tab={AUDIT_TAB}")

    if preliminary_report["blocking_issue_count"]:
        raise SystemExit("Form CV audit has blocking issues; live values were not changed")
    if not args.apply:
        return

    _, current_matrices, current_email_rows = read_course_rows(spreadsheet, worksheets)
    assert_prewrite_snapshot_unchanged(email_rows, current_email_rows)
    if args.update_usage:
        current_usage_matrix = sheets_call(
            f"{USAGE_TAB}.prewrite_snapshot",
            lambda: worksheets[USAGE_TAB].get("A1:B45", value_render_option="FORMULA"),
        )
        if current_usage_matrix != usage_matrix:
            raise RuntimeError("Usage sheet changed during HubSpot fetch; live update aborted")

    live_payload, rollback_payload = build_live_payload(
        computation.results,
        current_matrices,
        include_usage=args.update_usage,
        usage_matrix=usage_matrix,
        data_cutoff=data_cutoff,
    )
    try:
        sheets_call("form_cv_live_batch_update", lambda: spreadsheet.values_batch_update(live_payload))
        _, post_matrices, post_email_rows = read_course_rows(spreadsheet, worksheets)
        apply_issues = verify_live_values(
            post_matrices,
            email_rows,
            post_email_rows,
            computation.results,
        )
        if args.update_usage:
            apply_issues.extend(verify_usage_values(worksheets[USAGE_TAB], data_cutoff))
        if apply_issues:
            raise RuntimeError(f"Form CV readback mismatch: {len(apply_issues)}")
    except Exception:
        safe_rollback_payload = rollback_payload
        try:
            _, _, rollback_current_rows = read_course_rows(spreadsheet, worksheets)
            safe_rollback_payload = build_keyed_rollback_payload(
                rollback_current_rows,
                computation.results,
                current_matrices,
                include_usage=args.update_usage,
                usage_matrix=usage_matrix,
            )
        except Exception as rollback_snapshot_error:
            print(f"rollback_snapshot_fallback={type(rollback_snapshot_error).__name__}", flush=True)
        sheets_call("form_cv_rollback", lambda: spreadsheet.values_batch_update(safe_rollback_payload))
        raise

    final_report = build_report(
        args=args,
        registry=registry,
        forms=forms,
        fetch_results=fetch_results,
        computation=computation,
        email_rows=email_rows,
        data_cutoff=data_cutoff,
        expected_checks=expected_checks,
        expected_issues=expected_issues,
        marketing_email_audit=marketing_email_audit,
        apply_issues=[],
    )
    final_report["live_update"] = {
        "updated": True,
        "course_tabs": list(TARGET_COURSES),
        "updated_columns": ["N", "O"],
        "usage_updated": bool(args.update_usage),
        "readback_issue_count": 0,
    }
    final_report["rollback_backup"] = rollback_backup(computation.results)
    if args.update_usage:
        final_report["usage_rollback_backup"] = usage_matrix or []
    write_report(output_path, final_report)
    print("live_update=verified")


if __name__ == "__main__":
    main()
