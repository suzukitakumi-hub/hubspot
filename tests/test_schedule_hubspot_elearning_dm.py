import copy
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import schedule_hubspot_elearning_dm as dm


class CloneVerificationTest(unittest.TestCase):
    def setUp(self):
        self.source = {
            "id": "source",
            "name": dm.EMAIL_NAME_PREFIX + "20260930",
            "subject": "動画体験会",
            "content": {"widgets": {"body": {"html": "<p>本文</p>"}}},
            "to": {"contactLists": {"include": [123], "exclude": [456]}},
            "from": {"fromName": "アビタス"},
            "subscriptionDetails": {"subscriptionId": 1},
        }
        self.target = dict(copy.deepcopy(self.source), id="target", state="DRAFT")

    def test_identical_copy_needs_no_retry(self):
        with patch.object(dm, "fetch_email") as fetch, patch.object(dm, "sleep") as wait:
            self.assertEqual(dm.verify_copy(self.source, self.target), self.target)
            fetch.assert_not_called()
            wait.assert_not_called()

    def test_stale_content_is_reread_without_mutating(self):
        stale = dict(self.target, content={})
        with patch.object(dm, "fetch_email", return_value=self.target) as fetch, \
             patch.object(dm, "sleep") as wait, patch.object(dm, "patch_email") as update, \
             patch.object(dm, "publish_email") as publish:
            self.assertEqual(dm.verify_copy(self.source, stale), self.target)
            fetch.assert_called_once_with("target")
            wait.assert_called_once_with(2)
            update.assert_not_called()
            publish.assert_not_called()

    def test_real_differences_are_not_ignored(self):
        for field in dm.COPY_FIELDS:
            with self.subTest(field=field):
                changed = dict(self.target, **{field: "different"})
                with patch.object(dm, "fetch_email", return_value=changed) as fetch, \
                     patch.object(dm, "sleep") as wait:
                    with self.assertRaisesRegex(RuntimeError, field):
                        dm.verify_copy(self.source, changed)
                    self.assertEqual(fetch.call_count, 5)
                    self.assertEqual(wait.call_count, 5)

    def test_persistent_difference_blocks_publish(self):
        changed = dict(self.target, content={"html": "changed"}, sendOnPublish=False,
                       publishDate="2099-10-02T09:00:00Z")
        with patch.object(dm, "patch_email", return_value=changed), \
             patch.object(dm, "fetch_email", return_value=changed), \
             patch.object(dm, "sleep"), patch.object(dm, "publish_email") as publish:
            with self.assertRaisesRegex(RuntimeError, "content"):
                dm.publish_existing_or_new_draft("target", self.source, "name",
                                                 dm.scheduled_datetime("2099-10-02", "18:00"))
            publish.assert_not_called()

    def test_dry_run_verifies_existing_copy_without_writes(self):
        with tempfile.TemporaryDirectory() as out:
            args = SimpleNamespace(apply=False, send_date_jst="2026-10-02", send_time_jst="18:00",
                                   target_name=dm.EMAIL_NAME_PREFIX + "20261002",
                                   source_email_id="source", output_dir=out)
            target = dict(self.target, name=args.target_name)
            with patch.object(dm, "parse_args", return_value=args), \
                 patch.object(dm, "load_dotenv"), \
                 patch.object(dm, "list_marketing_emails", return_value=[target]), \
                 patch.object(dm, "find_source_email", return_value=self.source), \
                 patch.object(dm, "fetch_email", return_value=target), \
                 patch.object(dm, "clone_email") as clone, \
                 patch.object(dm, "patch_email") as update, \
                 patch.object(dm, "publish_email") as publish, patch("builtins.print"):
                dm.main()
                clone.assert_not_called()
                update.assert_not_called()
                publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
