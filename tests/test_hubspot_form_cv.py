import datetime as dt
import unittest

from hubspot_form_cv import (
    FINAL_STATUS,
    JST,
    PROVISIONAL_STATUS,
    EmailRow,
    FormRegistry,
    FormRule,
    SubmissionEvidence,
    attribute_submission,
    active_marketing_email_urls,
    compute_email_cv,
    parse_email_rows,
    submission_from_api,
)
from sync_hubspot_form_cv_sheet import (
    USAGE_TAB,
    assert_prewrite_snapshot_unchanged,
    audit_marketing_email_routes,
    build_live_payload,
    verify_live_values,
)


FORM_GUID = "11111111-1111-1111-1111-111111111111"
UNKNOWN_GUID = "99999999-9999-9999-9999-999999999999"


def registry(*, meeting_routes=None, known_non_meetings=(), meetings_minimum=0) -> FormRegistry:
    return FormRegistry(
        included={
            FORM_GUID: FormRule(
                guid=FORM_GUID,
                name="テスト体験フォーム",
                category="オンライン体験",
            )
        },
        excluded={},
        unavailable_excluded={},
        category_order=["オンライン体験"],
        meetings_routes=meeting_routes or {},
        known_non_meetings_counseling_routes=frozenset(known_non_meetings),
        meetings_detection_minimum=meetings_minimum,
    )


def email_row(*, content_id="123", campaign_id="456", send_at=None, old_count="0") -> EmailRow:
    return EmailRow(
        content_id=content_id,
        course="USCPA",
        sheet_row=2,
        send_at=send_at or dt.datetime(2026, 1, 1, 12, 0, tzinfo=JST),
        email_name="テストメール",
        campaign_ids=(campaign_id,),
        old_count=old_count,
        old_breakdown="",
    )


def evidence(
    *,
    conversion_id="conversion-1",
    submitted_at=None,
    form_guid=FORM_GUID,
    hsmi=("456",),
    page_utm=(),
    hidden=(),
    sources=(),
    media=(),
) -> SubmissionEvidence:
    return SubmissionEvidence(
        form_guid=form_guid,
        form_name="テストフォーム",
        conversion_id=conversion_id,
        submitted_at=submitted_at or dt.datetime(2026, 1, 2, 12, 0, tzinfo=JST),
        page_hsmi_ids=tuple(hsmi),
        page_utm_content_ids=tuple(page_utm),
        hidden_utm_content_ids=tuple(hidden),
        page_utm_sources=tuple(sources),
        page_utm_media=tuple(media),
    )


class AttributionTests(unittest.TestCase):
    def test_hsmi_is_read_from_html_escaped_page_url(self):
        raw = {
            "conversionId": "abc",
            "submittedAt": 1767222000000,
            "pageUrl": "https://example.test/form?utm_source=hs_email&amp;_hsmi=456",
            "values": [{"name": "utm_content", "value": "456"}],
        }
        submission = submission_from_api(FORM_GUID, "フォーム", raw)

        result = attribute_submission(submission)

        self.assertEqual("page", result.status)
        self.assertEqual("456", result.campaign_id)

    def test_hsmi_is_recovered_from_url_missing_question_mark(self):
        raw = {
            "conversionId": "abc",
            "submittedAt": 1767222000000,
            "pageUrl": "https://example.test/form/_hsmi=456&utm_source=hs_email",
            "values": [],
        }

        result = attribute_submission(submission_from_api(FORM_GUID, "フォーム", raw))

        self.assertEqual("page", result.status)
        self.assertEqual("456", result.campaign_id)

    def test_utm_content_requires_email_source_or_medium(self):
        verified = evidence(hsmi=(), page_utm=("456",), sources=("hs_email",))
        unverified = evidence(hsmi=(), page_utm=("456",), sources=("google",))

        self.assertEqual("page", attribute_submission(verified).status)
        self.assertEqual("page_utm_unverified", attribute_submission(unverified).status)

    def test_hidden_only_is_not_page_attribution(self):
        result = attribute_submission(evidence(hsmi=(), hidden=("456",)))

        self.assertEqual("hidden_only", result.status)

    def test_conflicting_page_and_hidden_ids_are_blocking_evidence(self):
        result = attribute_submission(evidence(hsmi=("456",), hidden=("789",)))

        self.assertEqual("conflict", result.status)


class ComputationTests(unittest.TestCase):
    def test_dedupes_and_applies_inclusive_30_day_window(self):
        email = email_row()
        send_at = email.send_at
        submissions = [
            evidence(conversion_id="valid", submitted_at=send_at + dt.timedelta(days=1)),
            evidence(conversion_id="valid", submitted_at=send_at + dt.timedelta(days=1)),
            evidence(conversion_id="boundary", submitted_at=send_at + dt.timedelta(days=30)),
            evidence(conversion_id="late", submitted_at=send_at + dt.timedelta(days=30, seconds=1)),
        ]

        result = compute_email_cv(
            [email],
            submissions,
            registry(),
            now=send_at + dt.timedelta(days=40),
        )

        self.assertEqual(2, result.results[0].count)
        self.assertEqual("オンライン体験：2", result.results[0].breakdown)
        self.assertEqual(FINAL_STATUS, result.results[0].status)
        self.assertEqual(1, result.duplicate_count)
        self.assertEqual(1, result.outside_window_count)

    def test_five_minute_start_tolerance_and_older_pre_send_block(self):
        email = email_row()
        result = compute_email_cv(
            [email],
            [
                evidence(conversion_id="tolerated", submitted_at=email.send_at - dt.timedelta(minutes=1)),
                evidence(conversion_id="too-early", submitted_at=email.send_at - dt.timedelta(minutes=6)),
            ],
            registry(),
            now=email.send_at + dt.timedelta(days=40),
        )

        self.assertEqual(1, result.results[0].count)
        self.assertEqual(1, result.tolerated_pre_send_count)
        self.assertEqual(1, result.pre_send_count)

    def test_recent_meetings_email_is_marked_provisional_and_unattributable(self):
        email = email_row(content_id="123")
        result = compute_email_cv(
            [email],
            [],
            registry(),
            now=email.send_at + dt.timedelta(days=10),
            meetings_email_content_ids={"123"},
        ).results[0]

        self.assertEqual(PROVISIONAL_STATUS, result.status)
        self.assertIn("カウンセリング予約：メール別確定不可（Meetings）", result.breakdown)
        self.assertIn(PROVISIONAL_STATUS, result.breakdown)

    def test_unknown_page_attributed_form_fails_closed(self):
        email = email_row()
        result = compute_email_cv(
            [email],
            [evidence(form_guid=UNKNOWN_GUID)],
            registry(),
            now=email.send_at + dt.timedelta(days=40),
        )

        self.assertEqual(0, result.results[0].count)
        self.assertEqual(1, result.unknown_page_attributed_count)
        self.assertEqual(1, result.blocking_issue_count)


class SheetPayloadTests(unittest.TestCase):
    @staticmethod
    def marketing_email(content_id, campaign_id, destination):
        return {
            "id": content_id,
            "allEmailCampaignIds": [campaign_id],
            "content": {
                "flexAreas": {
                    "main": {"sections": [{"columns": [{"widgets": ["button"]}]}]}
                },
                "widgets": {"button": {"body": {"destination": destination}}},
            },
        }

    def test_marketing_email_routes_use_exact_active_url_classification(self):
        emails = [
            email_row(content_id="101", campaign_id="201"),
            EmailRow(**{**email_row(content_id="102", campaign_id="202").__dict__, "sheet_row": 3}),
            EmailRow(**{**email_row(content_id="103", campaign_id="203").__dict__, "sheet_row": 4}),
        ]
        raw = [
            self.marketing_email("101", "201", "https://lp.pathmake.co.jp/uscpa/counseling/?x=1"),
            self.marketing_email("102", "202", "https://lp.pathmake.co.jp/uscpa/ceeak_entry/counseling"),
            self.marketing_email("103", "203", "https://lp.pathmake.co.jp/new/counseling"),
        ]
        reg = registry(
            meeting_routes={
                "https://lp.pathmake.co.jp/uscpa/counseling": "https://meetings.hubspot.com/uscpa/uscpa"
            },
            known_non_meetings={"https://lp.pathmake.co.jp/uscpa/ceeak_entry/counseling"},
        )

        meetings, audit, issues = audit_marketing_email_routes(emails, raw, reg)

        self.assertEqual(frozenset({"101"}), meetings)
        self.assertEqual(1, audit["meetings_email_count"])
        self.assertEqual(["unknown_counseling_route"], [issue["code"] for issue in issues])

    def test_only_flex_area_referenced_widget_links_are_active(self):
        raw_email = {
            "content": {
                "flexAreas": {
                    "main": {
                        "sections": [
                            {"columns": [{"widgets": ["active-button"]}]},
                        ]
                    }
                },
                "widgets": {
                    "active-button": {
                        "body": {
                            "destination": "https://lp.pathmake.co.jp/uscpa/counseling/?x=1#cta"
                        }
                    },
                    "stale-button": {
                        "body": {"destination": "https://lp.pathmake.co.jp/mba/counseling"}
                    },
                },
            }
        }

        urls = active_marketing_email_urls(raw_email)

        self.assertEqual(
            frozenset({"https://lp.pathmake.co.jp/uscpa/counseling"}),
            urls,
        )

    def test_parses_new_headers_and_stable_marketing_email_id(self):
        matrix = [
            [
                "送付日",
                "メール件名（HubSpotリンク）",
                "メール内部名",
                "フォーム送信数（送信後30日）",
                "フォーム別内訳",
                "INTERNAL HUBSPOT IDS",
            ],
            [
                "2026-01-01 12:00:00",
                '=HYPERLINK("https://app.hubspot.com/email/123/details/777/performance","件名")',
                "内部名",
                4,
                "オンライン体験：4",
                "456 457",
            ],
        ]

        rows = parse_email_rows("USCPA", matrix)

        self.assertEqual("777", rows[0].content_id)
        self.assertEqual(("456", "457"), rows[0].campaign_ids)

    def test_usage_sheet_is_part_of_rollback_payload(self):
        result = compute_email_cv(
            [email_row()],
            [],
            registry(),
            now=dt.datetime(2026, 2, 15, tzinfo=JST),
        ).results
        course_matrix = [[""] * 19, [""] * 19]
        matrices = {course: course_matrix for course in ("CIA", "CISA", "CFE", "IFRS", "USCPA", "MBA", "AAIA")}
        # build_live_payload requires one contiguous result per target course.
        results = []
        for course in matrices:
            row = email_row(content_id=str(1000 + len(results)), campaign_id=str(2000 + len(results)))
            results.extend(
                compute_email_cv(
                    [EmailRow(**{**row.__dict__, "course": course})],
                    [],
                    registry(),
                    now=dt.datetime(2026, 2, 15, tzinfo=JST),
                ).results
            )

        _, rollback = build_live_payload(
            results,
            matrices,
            include_usage=True,
            usage_matrix=[["項目", "値"]],
            data_cutoff=dt.datetime(2026, 2, 15, tzinfo=JST),
        )

        self.assertIn(
            {"range": f"'{USAGE_TAB}'!A1:B45", "values": [["項目", "値"]] + [["", ""]] * 44},
            rollback["data"],
        )

    def test_prewrite_snapshot_detects_row_identity_change(self):
        expected = [email_row(content_id="100", campaign_id="200")]
        changed = [EmailRow(**{**expected[0].__dict__, "sheet_row": 3})]

        with self.assertRaises(RuntimeError):
            assert_prewrite_snapshot_unchanged(expected, changed)

    def test_zero_readback_must_be_numeric_not_blank(self):
        results = []
        rows = []
        matrices = {}
        courses = ("CIA", "CISA", "CFE", "IFRS", "USCPA", "MBA", "AAIA")
        for index, course in enumerate(courses):
            email = EmailRow(
                **{
                    **email_row(content_id=str(1000 + index), campaign_id=str(2000 + index)).__dict__,
                    "course": course,
                }
            )
            rows.append(email)
            result = compute_email_cv(
                [email],
                [],
                registry(),
                now=dt.datetime(2026, 2, 15, tzinfo=JST),
            ).results[0]
            results.append(result)
            header = [""] * 19
            header[13:15] = ["フォーム送信数（送信後30日）", "フォーム別内訳"]
            data = [""] * 19
            data[13] = "" if course == "USCPA" else 0
            data[14] = result.breakdown
            matrices[course] = [header, data]

        issues = verify_live_values(matrices, rows, rows, results)

        self.assertEqual(1, len(issues))
        self.assertEqual("USCPA", issues[0]["course"])
        self.assertEqual("str", issues[0]["actual_count_type"])


if __name__ == "__main__":
    unittest.main()
