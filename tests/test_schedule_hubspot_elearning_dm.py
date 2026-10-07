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
        self.target = dict(copy.deepcopy(self.source), id="target", state="DRAFT", isPublished=False)

    def test_generic_draft_without_layout_uses_draft_endpoint(self):
        generic = dict(self.target, content={"flexAreas": {"main": {}}})
        with patch.object(dm, "request_json", side_effect=[generic, self.target]) as request:
            self.assertEqual(dm.fetch_email("target"), self.target)
        self.assertEqual(request.call_args_list[-1].args, ("GET", "/marketing/v3/emails/target/draft"))

    def test_published_source_does_not_use_unpublished_draft(self):
        published = dict(self.source, state="PUBLISHED", isPublished=True)
        with patch.object(dm, "request_json", return_value=published) as request:
            self.assertEqual(dm.fetch_email("source"), published)
            request.assert_called_once_with("GET", "/marketing/v3/emails/source")

    def test_draft_identity_or_state_change_fails_closed(self):
        for changed in (dict(self.target, id="other"), dict(self.target, state="PUBLISHED"),
                        dict(self.target, isPublished=True)):
            with self.subTest(changed=changed), patch.object(dm, "request_json", side_effect=[self.target, changed]):
                with self.assertRaisesRegex(RuntimeError, "state changed"):
                    dm.fetch_email("target")

    def test_api_publication_is_disabled(self):
        with patch.object(dm.SESSION, "post") as post:
            with self.assertRaisesRegex(RuntimeError, "human confirmation"):
                dm.publish_email("target")
            post.assert_not_called()

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
                dm.prepare_draft("target", self.source, "name",
                                                 dm.scheduled_datetime("2099-10-02", "18:00"))
            publish.assert_not_called()

    def test_apply_creates_only_verified_draft(self):
        args = SimpleNamespace(apply=True, send_date_jst="2026-10-07", send_time_jst="18:00",
                               target_name="test", source_email_id="source", output_dir="unused")
        ready = dict(self.target, name="test", sendOnPublish=False, publishDate="2026-10-07T09:00:00Z")
        with patch.object(dm, "parse_args", return_value=args), patch.object(dm, "load_dotenv"), \
             patch.object(dm, "request_json", return_value={"portalId": dm.PORTAL_ID}), \
             patch.object(dm, "list_marketing_emails", return_value=[]), \
             patch.object(dm, "find_source_email", return_value=self.source), \
             patch.object(dm, "clone_email", return_value="target") as clone, \
             patch.object(dm, "fetch_email", return_value=self.target), \
             patch.object(dm, "patch_email", return_value=ready) as update, \
             patch.object(dm, "publish_email") as publish, patch.object(dm, "write_output") as output, \
             patch("builtins.print"):
            dm.main()
            clone.assert_called_once_with("source", "test")
            self.assertFalse(update.call_args.args[1]["sendOnPublish"])
            publish.assert_not_called()
            self.assertTrue(output.call_args.args[1]["allChecksOk"])
            self.assertTrue(output.call_args.args[1]["deliveryRequiresHumanConfirmation"])

    def test_apply_rejects_wrong_portal_before_listing_or_writing(self):
        args = SimpleNamespace(apply=True, send_date_jst="2026-10-07", send_time_jst="18:00")
        with patch.object(dm, "parse_args", return_value=args), patch.object(dm, "load_dotenv"), \
             patch.object(dm, "request_json", return_value={"portalId": "wrong"}), \
             patch.object(dm, "list_marketing_emails") as listing, patch.object(dm, "clone_email") as clone:
            with self.assertRaisesRegex(RuntimeError, "portal"):
                dm.main()
            listing.assert_not_called()
            clone.assert_not_called()

    def test_repeated_prepare_does_not_modify_ready_draft(self):
        ready = dict(self.target, name="test", sendOnPublish=False, publishDate="2026-10-07T09:00:00Z")
        with patch.object(dm, "fetch_email", return_value=ready), patch.object(dm, "patch_email") as update:
            self.assertEqual(dm.prepare_draft("target", self.source, "test",
                                             dm.scheduled_datetime("2026-10-07", "18:00")), ready)
            update.assert_not_called()

    def test_existing_modified_draft_is_not_overwritten(self):
        changed = dict(self.target, subject="user edit")
        with patch.object(dm, "fetch_email", return_value=changed), patch.object(dm, "sleep"), \
             patch.object(dm, "patch_email") as update:
            with self.assertRaisesRegex(RuntimeError, "subject"):
                dm.prepare_draft("target", self.source, "test", dm.scheduled_datetime("2026-10-07", "18:00"))
            update.assert_not_called()

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
