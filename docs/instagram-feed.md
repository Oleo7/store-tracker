# Polarbär Instagram feed

Implementation branch: `codex/instagram-feed`, based on master `fc5f3dd`.
Existing Render service: `store-tracker`, `srv-d7d0rppkh4rs739glhj0`.
Public URL: https://store-tracker.onrender.com/api/public/instagram-feed
App: **Polarbär Social Feed**, `1553369652626144`.
Instagram account: `polarbar.se`, `17841475991503244` (verified with Graph API).

## Installation in Squarespace

Pages → Home → Edit → Add Section → Blank, above the footer → Add Block → Code.
Choose HTML, turn **Display Source** off, and paste `instagram-squarespace.html`.
Use a full-width block. Preview on the published page outside editor mode (Squarespace
may suppress scripts in its editor). No Squarespace edits are performed by this project.

The component inherits the site's font and uses black headings, white space, and peach
arrow controls, based on visual inspection of the current site's Q&A and store locator.
It does not replace the Instagram iframe's content or visual identity.

## Data and cache

Facebook Login Graph API v26.0 is used because its `/tags` edge is needed.
`GET /{ig-user-id}/media` reads own images, albums and videos/Reels.
`GET /{ig-user-id}/tags` reads public tagged media, including Reels.
Each collection reads up to three pages of 25 objects; tags pagination uses `after`
cursors even when Meta omits `next`. Results are sorted newest first within each source.
Only ID, permalink, media type, timestamp, optional username and source are returned.
Media files and captions are neither downloaded nor stored.

`MIX` in `instagram_feed.py` defines the deterministic UGC, UGC, own pattern.
Empty pools fall back to the other source. IDs and canonical permalinks are deduplicated;
own membership wins for collaborative media appearing in both sources.

Metadata is cached in memory for 36,000 seconds by default. Expired data refreshes on a
visitor's request. A process lock allows one refresh; concurrent requests return the
available snapshot (an empty list on concurrent cold start). Each collection retains its
previous data on failure, so an own-media failure does not erase available tagged media.
Failures back off for 60 seconds. Upstream calls have connect/read timeouts and a shared
25-second refresh deadline (an in-flight request can run beyond that deadline by its
bounded timeout). No cron, database, Redis or scheduler is used.

The existing Render setup has one worker, four threads and one instance. Keep that topology:
process-local locks, feed metadata and mention IDs are not shared between workers.
Restarting the service clears the entire cache and the received mention ID list.

## @mentions are different from tags

Meta does not provide a list of historical caption/comment mentions. It sends `mentions`
webhooks containing media/comment IDs. The signed callback is
`https://store-tracker.onrender.com/api/webhooks/instagram`.

Configure Meta's **Instagram** webhook object, subscribe to **mentions**, and subscribe
the professional account to the app as required by Meta. GET verifies the callback;
POST requires a valid `X-Hub-Signature-256` HMAC using the app secret. Only events for the
configured Instagram account are accepted. The receiver stores at most 100 identifiers,
for at most 30 days, only in memory. No API calls run during webhook receipt.

On cache refresh, up to 12 recent IDs are resolved using
`mentioned_media.media_id(ID)` for caption mentions or
`mentioned_comment.comment_id(ID)` for comment mentions, followed by a media metadata
lookup. An item is included only if Meta supplies a valid public permalink. Unsupported,
deleted or private media are skipped independently. New mentions can take up to the TTL
to appear. Mentions received before webhook activation, during downtime or before a
restart cannot be recovered automatically with this no-persistence architecture.
Story mentions are not supported by these endpoints. Public accounts can disable embeds;
the affected card keeps an original-post link and does not break the carousel.

## Permissions and token renewal

Verified granted in Graph API Explorer: `instagram_basic`, `instagram_manage_comments`,
`pages_read_engagement`, `pages_show_list`, `ads_read`, `public_profile`.
The comments permission is required by tags/mentions read endpoints. `ads_read` is required
when the Page role is assigned via Business Manager. The implementation never publishes,
replies, changes comments or accesses ads. `/me/accounts` returned an empty list for this
token, but direct calls to the verified Instagram account returned own and tagged media.

A long-lived Facebook User token was issued with expiry **6 November 2026**. It must be
renewed before expiry through Meta's login/token tools and the Render secret replaced.
Data access has its own expiry and can also require reauthorization. Tokens can be revoked
earlier by Meta or account/security changes. This is not an indefinitely self-renewing
Instagram Login token. App roles permit development testing with the owner's account;
Advanced Access/App Review and business verification may be required for third-party
access. Meta explicitly states that an unpublished app receives only dashboard test
webhooks, even for app admins/developers/testers. Real mention delivery has not been
verified. The app remains in Development mode; see the remaining step below.

Server environment variables (never place values in source or frontend):

| Name | Purpose |
| --- | --- |
| INSTAGRAM_ACCESS_TOKEN | Long-lived Facebook User access token |
| INSTAGRAM_ACCOUNT_ID | Verified professional Instagram ID |
| INSTAGRAM_APP_SECRET | Webhook signature validation |
| INSTAGRAM_WEBHOOK_VERIFY_TOKEN | Random callback verification secret |
| INSTAGRAM_FEED_CACHE_TTL_SECONDS | Default 36000, supported range 60–604800 |
| INSTAGRAM_GRAPH_API_VERSION | Default v26.0 |
| INSTAGRAM_FEED_LIMIT | Default 18, supported range 1–24 |

## Security and browser behavior

Only the exact `instagram.feed` and signed `instagram.webhook` endpoints are added to
the public endpoint set. CRM session authentication is unchanged. The feed has separate,
credential-free CORS for https://polarbar.se and https://www.polarbar.se. Other browser
Origins are rejected. CORS is not authentication: non-browser clients can read this public
metadata too. The global CRM CORS configuration is retained, excluding these two routes.
Tokens are sent upstream in Authorization headers; upstream exception text/URLs are not logged.

The tiny deferred loader does not fetch the feed, CSS, embed.js, or any Instagram media
until its mount is within 400px of the viewport. Horizontal card observation creates
embeds only for the visible cards plus 100px of prefetch. There is no external carousel
library. Native scrolling, scroll snap, keyboard arrows, reduced-motion support and
48px navigation buttons are used. A failed feed hides the section; failed embed loading
retains links to Instagram. No-IntersectionObserver browsers hide the section.

## Verification

- Real Graph API own-media and tagged-media reads succeeded with v26.0; tagged results
  included third-party videos/Reels and images.
- All 664 existing and new Python web-app tests passed on 7 September 2026 (before final
  focused hardening). Tests cover mix, fallback, dedup, cache hit/expiry, stale recovery,
  concurrent refresh, timeout, safe fields, webhook signing, endpoint and CORS isolation.
- Mobile browser at 390×844: zero feed/Instagram requests before scrolling; one feed request,
  one embed script and two cards near the section; navigation loaded a third card. No page
  overflow. Real Instagram embeds rendered with original links and controls.
- Desktop at 1440×900: three full cards visible, horizontal navigation, no page overflow.
- Render deployed `f629717bcd42ac5dfa23158641c5c8b2ff838e70` successfully on 7 September
  2026. Live endpoint returned HTTP 200, 12 unique items, 8 UGC and 4 own in the exact
  UGC/UGC/own pattern, with `stale: false`. A repeat request used the populated cache.
- The original six-second read timeout was too short for tagged-media responses in
  Render. Pages were reduced to 25 objects and the bounded read timeout raised to 15
  seconds. All 9 focused Instagram tests passed after this change.
- Live CORS allowed both Polarbär origins without credentials; another origin received
  HTTP 403. Unauthenticated `/customers` still returned HTTP 401.
- Meta verified the callback URL; Instagram `mentions` is subscribed at v26.0. A dashboard
  test POST reached the deployed callback with HTTP 200 (7 September, 19:54:06 UTC),
  verifying the real signature path. This is a synthetic event, not a real customer mention.
- Browser fault checks: blocking Instagram scripts with CSP left original-post links
  and navigation available; a feed HTTP 503 hid the section with zero Instagram scripts.
  The surrounding page remained usable. Invalid metadata/permalinks are rejected by tests.

## Remaining Meta step and deployment state

Explicit approval is needed to switch **Polarbär Social Feed** from **Development** to
**Live** at https://developers.facebook.com/apps/1553369652626144/webhooks/?view=instagram.
Automatic approval review blocked that UI action because it publishes the app and enables
production webhook delivery. No publication took place. Once approved, resolve any actual
Meta publication requirements, verify the professional account subscription if required,
and observe a real public caption/comment mention before declaring mentions operational.
Do not create an unsolicited public test post on the user's behalf.

Own and tagged-media retrieval already work independently of this remaining step.
The token must be renewed before **6 November 2026**. There is no historical mention
backfill, Story mention support, or persistence across service restarts.

Render auto-deploy was automatically disabled by its specific-commit deployment flow.
The service still tracks master in its settings, but the live runtime is the explicitly
deployed Instagram commit. No merge to master was performed. Keep auto-deploy disabled
until this branch is merged through the repository's normal review process; deploying
master before that would remove the Instagram routes and assets.

Changed files: `web-app/app.py`, `web-app/instagram_feed.py`,
`web-app/static/instagram-feed.js`, `web-app/static/instagram-feed.css`,
`web-app/tests/test_instagram_feed.py`, `web-app/tests/instagram_browser_harness.py`,
`.env.example`, `render.yaml`, `docs/instagram-feed.md`, `docs/instagram-squarespace.html`.
Implementation commits: `d4c2147` and `f629717`; subsequent documentation/test-fixture
commits do not change the deployed runtime.

## Official references checked 7 September 2026

- [Tags](https://developers.facebook.com/documentation/instagram-platform/instagram-graph-api/reference/ig-user/tags)
- [Mentions](https://developers.facebook.com/documentation/instagram-platform/instagram-api-with-facebook-login/mentions)
- [Mentioned Media](https://developers.facebook.com/documentation/instagram-platform/instagram-graph-api/reference/ig-user/mentioned_media)
- [Embed Button](https://developers.facebook.com/documentation/instagram-platform/embed-button)

The current app is reused; no infrastructure or Squarespace publishing is created by the
snippet. Deploy this branch's reviewed commit on the existing Render service without
merging master. A later automatic deploy of master will omit the feature until it is merged.
