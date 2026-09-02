# Platform APIs — User Activity Tracking

**Page type:** grid page (nav-card grid, auto-fit columns min 280px, per-card brand-colored category labels and topic tags)
**HTML title tag:** Platform APIs — User Activity Tracking

**Subtitle:** Passive behavioral data — what users did, viewed, consumed, or moved through. The analytics and insights surface of each platform, distinct from the content-fetching surface.

## Callout (philosophy box)

**Why this matters:** Activity tracking APIs expose what the platform observed about user behavior — impressions, views, movement, biometrics — as derived metrics rather than content. The same platform often has both surfaces, gated and retained differently: content endpoints return what a user created, these endpoints return what the platform measured about them. The granularity varies wildly: some give you second-level heartbeats or five-minute glucose readings, others give you daily aggregates with a 48-hour delay. Rate limits, tier gating, and deprecation cycles mean the data you can get today may vanish tomorrow.

## Cards

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | SOCIAL | Twitter/X Analytics API | [platform-apis-activity/01-twitter-x-analytics.md](platform-apis-activity/01-twitter-x-analytics.md) | Impressions, profile visits, engagement metrics per tweet. Gated behind paid tiers since 2023. | impressions, engagement, paid-tier |
| 2 | SOCIAL | Meta Insights API | [platform-apis-activity/02-meta-insights.md](platform-apis-activity/02-meta-insights.md) | Page reach, post impressions, audience demographics, story views. Requires business account and app review. | reach, demographics, business-only |
| 3 | PROFESSIONAL | LinkedIn Analytics | [platform-apis-activity/03-linkedin-analytics.md](platform-apis-activity/03-linkedin-analytics.md) | Profile views, search appearances, post impressions, follower demographics. Heavy restrictions on personal data. | profile-views, search-rank, restricted |
| 4 | VIDEO | YouTube Analytics API | [platform-apis-activity/04-youtube-analytics.md](platform-apis-activity/04-youtube-analytics.md) | Watch time, audience retention curves, traffic sources, real-time concurrent viewers. Channel owner access only. | watch-time, retention, owner-only |
| 5 | DEVICE | Apple Screen Time | [platform-apis-activity/05-apple-screentime.md](platform-apis-activity/05-apple-screentime.md) | App usage duration, pickups, notification counts. No public API — available via MDM or DeviceActivity framework. | app-usage, no-public-api, MDM |
| 6 | WEARABLE | Oura Ring API | [platform-apis-activity/06-oura-ring.md](platform-apis-activity/06-oura-ring.md) | Sleep stages, HRV, readiness score, body temperature deviation. REST API with OAuth2, 5-min granularity. | biometrics, sleep, 5-min-intervals |
| 7 | WEARABLE | Fitbit / Google Fit API | [platform-apis-activity/07-fitbit-google-fit.md](platform-apis-activity/07-fitbit-google-fit.md) | Steps, heart rate (1-sec intraday), sleep stages, SpO2, blood glucose records (Health Connect). Migrating from Fitbit Web API to Google Health Connect. | intraday, heart-rate, migration |
| 8 | IOT | Ring Doorbell API | [platform-apis-activity/08-ring-doorbell.md](platform-apis-activity/08-ring-doorbell.md) | Motion events, dings, video history. No official public API — unofficial reverse-engineered endpoints exist. | motion-events, unofficial, video |
| 9 | FITNESS | Strava API | [platform-apis-activity/09-strava.md](platform-apis-activity/09-strava.md) | Activities, GPS streams, heart rate zones, segment leaderboards. Rate limited to 100 req/15min, 1000/day. | GPS-streams, segments, rate-limited |
| 10 | MEDIA | Spotify Recently Played | [platform-apis-activity/10-spotify-listening.md](platform-apis-activity/10-spotify-listening.md) | Last 50 tracks played, top tracks/artists over time windows. No play count — only relative ranking. | listening-history, top-items, no-counts |
| 11 | LOCATION | Google Maps Timeline | [platform-apis-activity/11-google-maps-timeline.md](platform-apis-activity/11-google-maps-timeline.md) | Location visits, routes, dwell time, place categories. Being moved on-device — cloud access shrinking. | location, dwell-time, deprecating |
| 12 | SOCIAL | Instagram Insights API | [platform-apis-activity/12-instagram-insights.md](platform-apis-activity/12-instagram-insights.md) | Reach, impressions, story views, profile visits. Creator/business accounts only. 30-day data windows. | reach, stories, 30-day-window |
| 13 | WEARABLE | CGM / Glucose Monitors | [platform-apis-activity/13-cgm-glucose.md](platform-apis-activity/13-cgm-glucose.md) | Interstitial glucose every ~5 minutes from arm-worn sensors. Over-the-counter since 2024 — no prescription. Delayed feeds, not real-time. | glucose, 5-min-intervals, OTC |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, one `.philosophy` callout, one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(280px, 1fr))`, 14px gap.
- **Links:** the table above links to `.md` versions; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead (subfolder `platform-apis-activity/`).
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num" style="color:BRAND_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors (per-card brand colors):** 1 SOCIAL `#1da1f2`; 2 SOCIAL `#1877f2`; 3 PROFESSIONAL `#0a66c2`; 4 VIDEO `#ff0000`; 5 DEVICE `#555`; 6 WEARABLE `#27ae60`; 7 WEARABLE `#27ae60`; 8 IOT `#1c95e0`; 9 FITNESS `#fc4c02`; 10 MEDIA `#1db954`; 11 LOCATION `#4285f4`; 12 SOCIAL `#e4405f`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 18px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. Card-num 0.72em bold; h3 `#1a3a4a` 1em; description `#555` 0.84em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em, text `#222`.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (card labels use brand-specific colors listed above). No canvases on this page; any added canvases use `window.devicePixelRatio` scaling.
