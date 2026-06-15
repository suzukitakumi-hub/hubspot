import unittest
from unittest.mock import Mock, patch

import requests

from uscpa_sales_email_open_monitor import (
    ALLOWED_CPA_OWNER_IDS,
    CPA_OWNER_NAMES,
    DEFAULT_CPA_SLACK_MENTIONS,
    lookup_slack_mentions_by_email,
    owner_slack_display,
    slack_api_request,
)


class SlackResilienceTests(unittest.TestCase):
    def test_lookup_mentions_continues_when_slack_times_out(self):
        with patch("uscpa_sales_email_open_monitor.time.sleep"), patch(
            "uscpa_sales_email_open_monitor.requests.get",
            side_effect=requests.ConnectTimeout("timed out"),
        ), patch("uscpa_sales_email_open_monitor.sys.stderr"):
            result = lookup_slack_mentions_by_email("token", {})

        self.assertEqual(result, DEFAULT_CPA_SLACK_MENTIONS)

    def test_verified_default_mentions_override_stale_external_map(self):
        stale_map = {"80584487": "<@U09CS427JAY>"}

        with patch("uscpa_sales_email_open_monitor.time.sleep"), patch(
            "uscpa_sales_email_open_monitor.requests.get",
            side_effect=requests.ConnectTimeout("timed out"),
        ), patch("uscpa_sales_email_open_monitor.sys.stderr"):
            result = lookup_slack_mentions_by_email("token", stale_map)

        self.assertEqual(result["80584487"], "<@U08SP4MAUNA>")

    def test_owner_display_falls_back_to_owner_name(self):
        owner_id = next(iter(ALLOWED_CPA_OWNER_IDS))

        self.assertEqual(owner_slack_display(owner_id, {}), CPA_OWNER_NAMES[owner_id])

    def test_slack_api_request_retries_transient_timeout(self):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"ok": True, "ts": "123.456"}
        response.raise_for_status.return_value = None

        with patch("uscpa_sales_email_open_monitor.time.sleep"), patch(
            "uscpa_sales_email_open_monitor.requests.post",
            side_effect=[requests.ConnectTimeout("timed out"), response],
        ) as post:
            data = slack_api_request("token", "chat.postMessage", {"text": "hello"})

        self.assertEqual(data["ts"], "123.456")
        self.assertEqual(post.call_count, 2)


if __name__ == "__main__":
    unittest.main()
