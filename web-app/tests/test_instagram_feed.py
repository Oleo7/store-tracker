import hashlib
import hmac
import json
from pathlib import Path
import sys
import threading
import time
from unittest import TestCase, main
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import requests
from flask import Flask
from instagram_feed import FeedService, MetaClient, MetaError, create_blueprint, mix_feed, normalize


def media(number, source="own"):
    return normalize({"id": str(number), "permalink": f"https://www.instagram.com/p/TEST{number}/",
                      "timestamp": f"2026-08-{number % 28 + 1:02d}T12:00:00+0000",
                      "media_type": "VIDEO", "username": "polarbar.se"}, source)


class InstagramTests(TestCase):
    def setUp(self):
        self.now = 100
        self.client = Mock()
        self.client.collection.side_effect = lambda account, edge, deadline: [media(1 if edge == "media" else 2)]
        self.service = FeedService(self.client, {"INSTAGRAM_ACCOUNT_ID": "123"}, lambda: self.now)

    def test_mix_newest_dedup_and_fallback(self):
        own = [media(n) for n in range(1, 9)]
        ugc = [media(n, "ugc") for n in range(10, 26)]
        result = mix_feed(own, ugc + [own[0]], 12)
        self.assertEqual([r["source"] for r in result], ["ugc", "ugc", "own"] * 4)
        self.assertEqual(len({r["id"] for r in result}), 12)
        for source in ("own", "ugc"):
            stamps = [r["timestamp"] for r in result if r["source"] == source]
            self.assertEqual(stamps, sorted(stamps, reverse=True))
        self.assertEqual(len(mix_feed(own, [], 12)), 8)
        self.assertEqual(len(mix_feed([], ugc, 12)), 12)
        self.assertEqual(len(mix_feed(own[:1], ugc, 12)), 12)
        duplicate = dict(ugc[0], id="999")
        self.assertEqual(len(mix_feed([], [ugc[0], duplicate])), 1)

    def test_normalization_rejects_non_embed_and_removes_extra_fields(self):
        row = media(1)
        self.assertIsNone(normalize(dict(row, permalink="https://evil.example/p/test/"), "own"))
        self.assertIsNone(normalize(dict(row, media_type="STORY"), "own"))
        self.assertIsNone(normalize(dict(row, timestamp="invalid"), "own"))
        self.assertNotIn("access_token", normalize(dict(row, access_token="secret"), "own"))

    def test_cache_hit_expiry_and_partial_stale_recovery(self):
        initial = self.service.feed()
        self.service.feed()
        self.assertEqual(self.client.collection.call_count, 2)
        self.now += 36001
        self.client.collection.side_effect = MetaError("timeout")
        result = self.service.feed()
        self.assertTrue(result["stale"])
        self.assertEqual(result["items"], initial["items"])
        self.service.feed()
        self.assertEqual(self.client.collection.call_count, 4)
        self.now += 61
        self.client.collection.side_effect = lambda account, edge, deadline: []
        self.assertEqual(self.service.feed()["items"], [])
        self.assertFalse(self.service.stale)

    def test_partial_source_success_is_kept(self):
        self.client.collection.side_effect = [[media(1)], MetaError("timeout")]
        result = self.service.feed()
        self.assertEqual(len(result["items"]), 1)
        self.assertTrue(result["stale"])

    def test_parallel_requests_only_start_one_refresh(self):
        entered, release = threading.Event(), threading.Event()
        def blocked(*args):
            entered.set()
            release.wait(2)
            return [media(1)]
        self.client.collection.side_effect = blocked
        thread = threading.Thread(target=self.service.feed)
        thread.start()
        self.assertTrue(entered.wait(1))
        self.assertEqual(self.service.feed()["items"], [])
        self.assertEqual(self.client.collection.call_count, 1)
        release.set()
        thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.client.collection.call_count, 2)

    def test_meta_paginates_tags_without_next_and_uses_bearer(self):
        transport = Mock()
        transport.get.side_effect = [Mock(status_code=200, json=lambda: {"data": [], "paging": {"cursors": {"after": "cursor"}}}),
                                     Mock(status_code=200, json=lambda: {"data": [media(2)]})]
        client = MetaClient({"INSTAGRAM_ACCESS_TOKEN": "secret"}, transport)
        self.assertEqual(len(client.collection("123", "tags", time.monotonic() + 20)), 1)
        args = transport.get.call_args.kwargs
        self.assertEqual(args["params"]["after"], "cursor")
        self.assertNotIn("access_token", args["params"])
        self.assertEqual(args["headers"]["Authorization"], "Bearer secret")
        self.assertFalse(args["allow_redirects"])

    def test_meta_timeout_and_invalid_json_are_safe(self):
        transport = Mock()
        transport.get.side_effect = requests.Timeout("secret URL")
        client = MetaClient({"INSTAGRAM_ACCESS_TOKEN": "secret"}, transport)
        with self.assertRaisesRegex(MetaError, "network_or_json_error"):
            client.get("123/media", {}, time.monotonic() + 20)
        with self.assertRaisesRegex(MetaError, "timeout"):
            client.get("123/media", {}, time.monotonic() - 1)

    def test_webhook_signatures_account_filter_and_cache_ttl(self):
        self.service.env.update(INSTAGRAM_APP_SECRET="test-secret", INSTAGRAM_WEBHOOK_VERIFY_TOKEN="verify")
        app = Flask(__name__)
        app.register_blueprint(create_blueprint(self.service))
        client = app.test_client()
        payload = {"object": "instagram", "entry": [{"id": "123", "changes": [{"field": "mentions", "value": {"media_id": "456"}}]}]}
        raw = json.dumps(payload).encode()
        signature = "sha256=" + hmac.new(b"test-secret", raw, hashlib.sha256).hexdigest()
        self.assertEqual(client.post('/api/webhooks/instagram', data=raw, content_type='application/json').status_code, 403)
        self.assertEqual(client.post('/api/webhooks/instagram', data=raw, content_type='application/json', headers={"X-Hub-Signature-256": signature}).status_code, 200)
        self.assertIn(("media", "456"), self.service.events)
        self.client.mention.return_value = media(9, "ugc")
        self.assertTrue(self.service.feed()["items"])
        self.client.mention.assert_called_once()
        self.assertEqual(client.get('/api/webhooks/instagram?hub.mode=subscribe&hub.verify_token=verify&hub.challenge=abc').data, b'abc')

    def test_endpoint_and_cors_do_not_change_crm_access(self):
        import app as app_module
        client = app_module.app.test_client()
        response = client.get('/api/public/instagram-feed', headers={"Origin": "https://polarbar.se"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get('Access-Control-Allow-Origin'), 'https://polarbar.se')
        self.assertNotIn('Access-Control-Allow-Credentials', response.headers)
        self.assertEqual(set(response.json), {"items", "stale"})
        for origin in ('https://evil.example', 'https://polarbar.se.evil.example'):
            response = client.get('/api/public/instagram-feed', headers={"Origin": origin})
            self.assertEqual(response.status_code, 403)
            self.assertNotIn('Access-Control-Allow-Origin', response.headers)
        self.assertEqual(client.get('/customers').status_code, 401)
        self.assertEqual(client.get('/email-proposal-settings').status_code, 401)
        self.assertEqual(client.post('/api/public/instagram-feed').status_code, 401)


if __name__ == '__main__':
    main()
