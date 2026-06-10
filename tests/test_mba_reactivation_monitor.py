import unittest
from datetime import datetime, timezone

from mba_sales_email_open_monitor import (
    ALLOWED_MBA_OWNER_IDS,
    build_registration_url,
    is_mba_page,
    mba_reactivation_filter_groups,
    should_notify_contact,
)


class MbaReactivationMonitorTests(unittest.TestCase):
    def test_filter_groups_use_mba_properties_and_owner_condition(self):
        groups = mba_reactivation_filter_groups("1710000000000")
        properties = {
            item["propertyName"]
            for group in groups
            for item in group["filters"]
        }

        self.assertIn("notes_last_contacted", properties)
        self.assertIn("core_mba_entry", properties)
        self.assertIn("core_mba_project", properties)
        self.assertIn("sales_staff_mba", properties)
        self.assertNotIn("yomi", properties)
        self.assertEqual(len(groups), 1)

        owner_filters = [
            item
            for group in groups
            for item in group["filters"]
            if item["propertyName"] == "sales_staff_mba"
        ]
        self.assertTrue(owner_filters)
        self.assertEqual(set(owner_filters[0]["values"]), ALLOWED_MBA_OWNER_IDS)

    def test_mba_page_detection(self):
        self.assertTrue(is_mba_page("https://lp.pathmake.co.jp/mba/mevent_input?event_id=1"))
        self.assertTrue(is_mba_page("https://www.abitus.co.jp/information/mba/260613_mba_event.html"))
        self.assertFalse(is_mba_page("https://www.abitus.co.jp/information/uscpa/260530_uscpa_event.html"))

    def test_registration_url_adds_mba_program(self):
        attendance = {"properties": {"event_id": "73287"}}
        url = build_registration_url(
            "https://lp.pathmake.co.jp/mba/mevent_thanks?submissionGuid=abc",
            attendance,
        )

        self.assertEqual(
            url,
            "https://lp.pathmake.co.jp/mba/mevent_input?event_id=73287&program=MBA",
        )

    def test_requires_mba_owner(self):
        contact = {
            "id": "123",
            "properties": {
                "sales_staff_mba": "",
            },
        }

        self.assertEqual(
            should_notify_contact(contact, "web:1", {"123"}, datetime.now(timezone.utc), 10),
            "sales_staff_mba not target owner",
        )


if __name__ == "__main__":
    unittest.main()
