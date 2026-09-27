from datetime import datetime, timezone
from pathlib import Path
import sys
from unittest import TestCase
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from meta_giveaway import GiveawayService, snapshot_digest
from meta_ads import AdsError
from instagram_feed import InstagramMetaClient
from meta_client import MetaError


class Store:
    def __init__(self):
        self.metadata = None
        self.comments = None
        self.draws = []

    def save_snapshot(self, metadata, comments):
        self.metadata, self.comments = metadata, comments

    def load_snapshot(self, identifier):
        if self.metadata and self.metadata["snapshot_id"] == identifier:
            return self.metadata, self.comments
        return None, None

    def save_draw(self, draw):
        self.draws.append(draw)


class Instagram:
    def __init__(self):
        self.calls = 0

    def get_media(self, identifier):
        return {"id": identifier, "username": "polarbar.se"}

    def list_comments(self, identifier, *, limit, after=None):
        self.calls += 1
        if not after:
            return {"data": [{"id": "1", "username": "Alice", "text": "Polarbär!", "timestamp": "2026-09-01T10:00:00+00:00"},
                             {"id": "2", "username": "ALICE", "text": "Polarbär again", "timestamp": "2026-09-02T10:00:00+00:00"}], "after": "next"}
        return {"data": [{"id": "3", "username": "Bob", "text": "Polarbär!", "timestamp": "2026-09-03T10:00:00+00:00"},
                         {"id": "4", "username": "Carol", "text": "No keyword", "timestamp": "2026-09-04T10:00:00+00:00"}], "after": None}


class GiveawayTests(TestCase):
    def setUp(self):
        self.store, self.client = Store(), Instagram()
        self.service = GiveawayService(self.store, self.client,
                                       now=lambda: datetime(2026, 9, 27, tzinfo=timezone.utc))

    def test_snapshot_and_reproducible_deduped_draw(self):
        snapshot = self.service.snapshot("123", "olle")
        self.assertEqual(snapshot["comment_count"], 4)
        self.assertEqual(self.client.calls, 2)
        self.assertEqual(snapshot["snapshot_hash"], snapshot_digest(self.store.comments))
        rules = {"required_keyword": "polarbär", "winner_count": 2}
        first = self.service.draw(snapshot["snapshot_id"], rules, "olle", confirm=True, seed="fixed-seed-123456")
        second = self.service.draw(snapshot["snapshot_id"], rules, "olle", confirm=True, seed="fixed-seed-123456")
        self.assertEqual(first["winners"], second["winners"])
        self.assertEqual(first["eligible_count"], 2)
        self.assertEqual({row["username"].casefold() for row in first["winners"]}, {"alice", "bob"})
        self.assertEqual(len(self.store.draws), 2)

    def test_confirmation_integrity_and_exclusions(self):
        snapshot = self.service.snapshot("123", "olle")
        with self.assertRaises(AdsError) as error:
            self.service.draw(snapshot["snapshot_id"], {}, "olle", confirm=False)
        self.assertEqual(error.exception.code, "confirmation_required")
        with self.assertRaises(AdsError) as error:
            self.service.draw(snapshot["snapshot_id"], {"excluded_usernames": ["ALICE"], "winner_count": 3}, "olle", confirm=True)
        self.assertEqual(error.exception.code, "not_enough_eligible_entries")
        self.store.comments[0]["text"] = "tampered"
        with self.assertRaises(AdsError) as error:
            self.service.draw(snapshot["snapshot_id"], {}, "olle", confirm=True)
        self.assertEqual(error.exception.code, "snapshot_integrity_failed")

    def test_instagram_comments_use_bearer_and_validate_owner_and_paging(self):
        transport = Mock()
        transport.get.side_effect = [
            Mock(status_code=200, json=lambda: {"id": "123", "username": "polarbar.se"}),
            Mock(status_code=200, json=lambda: {"data": [{"id": "1"}], "paging": {
                "next": "https://graph.facebook.com/v26.0/123/comments?after=next",
                "cursors": {"after": "next"}}}),
        ]
        client = InstagramMetaClient({"INSTAGRAM_ACCESS_TOKEN": "test-only",
            "INSTAGRAM_ACCOUNT_ID": "17841475991503244"}, transport)
        self.assertEqual(client.list_comments("123")["after"], "next")
        for call in transport.get.call_args_list:
            self.assertNotIn("access_token", call.args[0])
            self.assertEqual(call.kwargs["headers"]["Authorization"], "Bearer test-only")
        transport.get.side_effect = [Mock(status_code=200, json=lambda: {
            "id": "123", "username": "someone_else"})]
        with self.assertRaises(MetaError):
            client.list_comments("123")


if __name__ == "__main__":
    from unittest import main
    main()
