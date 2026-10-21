import unittest
from http.client import RemoteDisconnected
from datetime import datetime, timezone
from unittest.mock import Mock, call, patch

import requests

from mba_sales_email_open_monitor import (
    ALLOWED_MBA_OWNER_IDS,
    DEFAULT_MBA_SLACK_MENTIONS,
    HubSpot,
    build_registration_url,
    is_mba_page,
    lookup_slack_mentions_by_email,
    mba_reactivation_filter_groups,
    should_notify_contact,
    search_open_emails,
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

    def test_verified_default_mentions_override_stale_external_map(self):
        stale_map = {"1141449920": "<@U07T2KB9JSH>"}

        with patch("mba_sales_email_open_monitor.time.sleep"), patch(
            "mba_sales_email_open_monitor.requests.get",
            side_effect=requests.ConnectTimeout("timed out"),
        ), patch("mba_sales_email_open_monitor.sys.stderr"):
            result = lookup_slack_mentions_by_email("token", stale_map)

        self.assertEqual(result["1141449920"], "<@U07TFT3QZTL>")
        self.assertEqual(result, DEFAULT_MBA_SLACK_MENTIONS)


class HubSpotReadRecoveryTests(unittest.TestCase):
    def setUp(self):
        # A synthetic credential is consumed locally; all HTTP and waits are mocked.
        with patch.dict("os.environ", {"HUBSPOT_PAT": "local-test-only"}):
            self.client = HubSpot()
        self.send = self.client.session.request = Mock()
        sleeper = patch("mba_sales_email_open_monitor.time.sleep")
        self.sleep = sleeper.start()
        self.addCleanup(sleeper.stop)

    def response(self, status=200, payload=None):
        response = requests.Response()
        response.status_code = status
        response._content = b'{"results": []}'
        response.json = Mock(return_value={"results": []} if payload is None else payload)
        return response

    def test_reported_email_search_recovers_from_remote_disconnect(self):
        self.send.side_effect = [
            requests.ConnectionError("Connection aborted.", RemoteDisconnected("Remote end closed connection without response")),
            self.response(payload={"results": [{"id": "test-email"}]}),
        ]
        result = search_open_emails(self.client, "mba@abitus.co.jp", "123", 200)
        self.assertEqual(result, [{"id": "test-email"}])
        self.assertEqual(self.send.call_count, 2)
        self.assertEqual(self.send.call_args_list[0], self.send.call_args_list[1])
        self.assertEqual(self.send.call_args.args, ("POST", "https://api.hubapi.com/crm/v3/objects/emails/search"))
        self.sleep.assert_called_once_with(1)

    def test_safe_reads_recover_from_connection_and_timeout_errors(self):
        for method, path in [
            ("GET", "/crm/v3/objects/contacts/test-contact"),
            ("HEAD", "/account-info/v3/details"),
            ("POST", "/crm/v3/objects/contacts/search"),
            ("POST", "/crm/v3/objects/contacts/batch/read"),
            ("POST", "/crm/v4/associations/emails/contacts/batch/read"),
        ]:
            for error_type in [requests.ConnectionError, requests.ConnectTimeout, requests.ReadTimeout]:
                with self.subTest(method=method, path=path, error=error_type.__name__):
                    self.send.reset_mock(side_effect=True)
                    self.sleep.reset_mock()
                    self.send.side_effect = [error_type("transient"), self.response()]
                    self.assertEqual(self.client.request(method, path), {"results": []})
                    self.assertEqual(self.send.call_count, 2)
                    self.sleep.assert_called_once_with(1)

    def test_transient_http_errors_and_disconnect_share_the_same_attempt_limit(self):
        self.send.side_effect = [self.response(503), requests.ReadTimeout("transient"), self.response(429), self.response()]
        self.assertEqual(self.client.request("POST", "/crm/v3/objects/emails/search"), {"results": []})
        self.assertEqual(self.send.call_count, 4)
        self.assertEqual(self.sleep.call_args_list, [call(1), call(2), call(4)])

    def test_persistent_connection_failure_stops_after_five_attempts(self):
        self.send.side_effect = requests.ConnectionError("persistent")
        with self.assertRaisesRegex(RuntimeError, "failed after 5 attempts") as raised:
            self.client.request("POST", "/crm/v3/objects/emails/search")
        self.assertIsInstance(raised.exception.__cause__, requests.ConnectionError)
        self.assertEqual(self.send.call_count, 5)
        self.assertEqual(self.sleep.call_args_list, [call(1), call(2), call(4), call(8)])

    def test_transient_http_statuses_recover_and_persistent_errors_still_fail(self):
        for status in [429, 500, 502, 503, 504]:
            with self.subTest(status=status):
                self.send.reset_mock(side_effect=True)
                self.sleep.reset_mock()
                self.send.side_effect = [self.response(status), self.response()]
                self.assertEqual(self.client.request("GET", "/account-info/v3/details"), {"results": []})
                self.assertEqual(self.send.call_count, 2)
        self.send.reset_mock(side_effect=True)
        self.sleep.reset_mock()
        self.send.return_value = self.response(503)
        with self.assertRaisesRegex(RuntimeError, "failed: 503"):
            self.client.request("GET", "/account-info/v3/details")
        self.assertEqual(self.send.call_count, 5)
        self.assertEqual(self.sleep.call_args_list, [call(1), call(2), call(4), call(8)])

    def test_auth_and_validation_errors_are_not_retried(self):
        for status in [400, 401, 403, 404]:
            with self.subTest(status=status):
                self.send.reset_mock(side_effect=True)
                self.send.return_value = self.response(status)
                with self.assertRaisesRegex(RuntimeError, f"failed: {status}"):
                    self.client.request("POST", "/crm/v3/objects/emails/search")
                self.assertEqual(self.send.call_count, 1)
        self.sleep.assert_not_called()

    def test_writes_are_not_replayed_after_ambiguous_network_or_server_failure(self):
        for method, path in [("PATCH", "/crm/v3/objects/contacts/test-contact"), ("POST", "/crm/v3/objects/contacts"),
                             ("POST", "/crm/v3/objects/contacts/batch/create")]:
            for failure in [requests.ConnectionError("unknown outcome"), requests.ReadTimeout("unknown outcome"), self.response(503)]:
                with self.subTest(method=method, path=path, failure=type(failure).__name__):
                    self.send.reset_mock(side_effect=True)
                    self.send.side_effect = [failure]
                    with self.assertRaises((requests.RequestException, RuntimeError)):
                        self.client.request(method, path, json={"properties": {"synthetic": "value"}})
                    self.assertEqual(self.send.call_count, 1)
        self.sleep.assert_not_called()

    def test_certificate_failure_is_not_retried(self):
        self.send.side_effect = requests.exceptions.SSLError("certificate rejected")
        with self.assertRaises(requests.exceptions.SSLError):
            self.client.request("GET", "/account-info/v3/details")
        self.assertEqual(self.send.call_count, 1)
        self.sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
