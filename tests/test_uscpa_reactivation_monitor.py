import unittest
from unittest.mock import call

import requests
import uscpa_sales_email_open_monitor as uscpa
from tests import test_mba_reactivation_monitor as mba_tests


class UscpaHubSpotReadRecoveryTests(mba_tests.HubSpotReadRecoveryTests):
    monitor = uscpa
    max_attempts = 7

    def adhoc_response(self):
        response = self.response(400)
        response._content = b'{"context":{"invalidProcessingType":["ADHOC"]}}'
        return response

    def test_list_loading_recovers_from_adhoc_state_and_disconnect(self):
        self.send.side_effect = [
            self.adhoc_response(),
            requests.ConnectionError("transient"),
            self.response(payload={"results": [{"recordId": "test-contact"}]}),
        ]
        self.assertEqual(uscpa.load_list_member_ids(self.client), {"test-contact"})
        self.assertEqual(self.send.call_count, 3)
        self.assertEqual(self.sleep.call_args_list, [call(1), call(2)])

    def test_persistent_adhoc_state_stops_after_seven_attempts(self):
        self.send.return_value = self.adhoc_response()
        with self.assertRaisesRegex(RuntimeError, "failed: 400"):
            uscpa.load_list_member_ids(self.client)
        self.assertEqual(self.send.call_count, 7)
        self.assertEqual(self.sleep.call_args_list, [call(2**i) for i in range(6)])

    def test_adhoc_error_outside_list_memberships_is_not_retried(self):
        self.send.return_value = self.adhoc_response()
        with self.assertRaisesRegex(RuntimeError, "failed: 400"):
            self.client.request("POST", "/crm/v3/objects/contacts/search")
        self.assertEqual(self.send.call_count, 1)
        self.sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
