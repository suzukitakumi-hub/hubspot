import unittest

from uscpa_sales_email_open_monitor import (
    build_event_title,
    build_registration_url,
    extract_submission_guid,
    is_thanks_page_url,
)


class WebPageContextTests(unittest.TestCase):
    def test_extract_submission_guid_is_case_insensitive(self):
        url = "https://lp.pathmake.co.jp/uscpa/seminar_thanks?submissionGuid=abc-123"
        self.assertEqual(extract_submission_guid(url), "abc-123")

    def test_detects_thanks_page(self):
        self.assertTrue(is_thanks_page_url("https://lp.pathmake.co.jp/uscpa/seminar_thanks"))
        self.assertFalse(
            is_thanks_page_url("https://www.abitus.co.jp/information/uscpa/260530_uscpa_event.html/")
        )

    def test_builds_registration_url_from_seminar_thanks(self):
        attendance = {"properties": {"event_id": "72995"}}
        url = build_registration_url(
            "https://lp.pathmake.co.jp/uscpa/seminar_thanks?submissionGuid=abc-123",
            attendance,
        )
        self.assertEqual(
            url,
            "https://lp.pathmake.co.jp/uscpa/seminar_input?event_id=72995&program=USCPA",
        )

    def test_builds_concise_event_title(self):
        attendance = {
            "properties": {
                "date": "2026-05-29",
                "event_kind_inner": "説明会",
                "start_time": "19:00",
            }
        }
        event = {
            "properties": {
                "date": "2026-05-29",
                "start_time": "19:00",
                "title": "USCPAオンライン説明会",
                "sub_title": "アジェンダ：USCPAについて",
            }
        }
        self.assertEqual(
            build_event_title(attendance, event),
            "USCPAオンライン説明会（2026/05/29 19:00）",
        )


if __name__ == "__main__":
    unittest.main()
