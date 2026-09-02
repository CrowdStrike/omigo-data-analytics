# Personal Data Archives & Takeout

**Page type:** grid page (nav-card grid, auto-fit columns min 280px, per-card brand-colored category labels and topic tags)
**HTML title tag:** Personal Data Archives & Takeout

**Subtitle:** What you get when you request your own data — your content, your behavioral profile, the ad interest graph the platform built about you, and how long it takes to arrive.

## Callout (philosophy box)

**Why this matters:** Every major platform now lets you download "your data" — but what arrives in the archive is a curated subset of what they actually store. The interesting question is the gap: what's included vs. what's inferred, how stale it is, and how the export compares to what the platform's own internal systems see. Archives blend your UGC with aggregated tracking data (ad profiles, inferred demographics, device fingerprints) in ways that reveal the platform's model of you.

## Cards

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | GOOGLE | Google Takeout | [data-exports/01-google-takeout.md](data-exports/01-google-takeout.md) | Search history, location timeline, ad profile, YouTube watch history, emails, photos, Drive files. 80+ products selectable. | comprehensive, multi-product, scheduled-export |
| 2 | TWITTER/X | Twitter/X Archive | [data-exports/02-twitter-x-archive.md](data-exports/02-twitter-x-archive.md) | Tweets, DMs, ad engagements, inferred interests, impression counts, IP login history, connected apps. | ad-interests, impressions, DMs |
| 3 | META | Facebook Download Your Information | [data-exports/03-facebook-dyi.md](data-exports/03-facebook-dyi.md) | Posts, messages, ad interest profile, off-Facebook activity log, search history, marketplace history. | off-facebook, ad-profile, search-history |
| 4 | META | Instagram Data Download | [data-exports/04-instagram-download.md](data-exports/04-instagram-download.md) | Posts, stories, messages, ad interests, search history, shopping activity, content interactions. | stories, ad-interests, interactions |
| 5 | PROFESSIONAL | LinkedIn Data Export | [data-exports/05-linkedin-export.md](data-exports/05-linkedin-export.md) | Connections, messages, profile views, job applications, inferred skills, ad targeting categories, endorsements. | connections, inferred-skills, ad-targeting |
| 6 | APPLE | Apple Privacy (privacy.apple.com) | [data-exports/06-apple-privacy.md](data-exports/06-apple-privacy.md) | App usage, iCloud data, Apple Pay transactions, Siri interactions, device analytics, AppleCare history. | iCloud, Siri, transactions |
| 7 | MEDIA | Spotify Extended Streaming History | [data-exports/07-spotify-history.md](data-exports/07-spotify-history.md) | Full listening history with timestamps, skip events, search queries, offline/online flag, shuffle state. | full-history, skip-events, timestamps |
| 8 | SOCIAL | Reddit Data Request | [data-exports/08-reddit-data-request.md](data-exports/08-reddit-data-request.md) | Posts, comments, votes, IP logs, ad interactions, moderation actions, saved content. | IP-logs, votes, mod-actions |
| 9 | MESSAGING | WhatsApp Chat Export | [data-exports/09-whatsapp-export.md](data-exports/09-whatsapp-export.md) | Messages, media, call logs per chat. In-app export only — no web portal. Group vs. individual chats. | per-chat, in-app-only, media |
| 10 | COMMERCE | Amazon Request My Data | [data-exports/10-amazon-data.md](data-exports/10-amazon-data.md) | Orders, browsing history, Alexa recordings, Kindle highlights, ad clicks, search history, Prime Video watch. | Alexa, orders, browsing |
| 11 | MEDIA | Netflix Viewing Activity | [data-exports/11-netflix-data.md](data-exports/11-netflix-data.md) | Viewing history, searches, ratings, device logins, billing history, profile preferences, taste clusters. | viewing, taste-profile, devices |
| 12 | TRANSPORT | Uber / Lyft Data Download | [data-exports/12-uber-lyft.md](data-exports/12-uber-lyft.md) | Trip history with routes, surge pricing at time of ride, payment details, ratings given and received, wait times. | routes, surge-data, ratings |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, one `.philosophy` callout, one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(280px, 1fr))`, 14px gap.
- **Links:** the table above links to `.md` versions; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead (subfolder `data-exports/`).
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num" style="color:BRAND_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors (per-card brand colors):** 1 GOOGLE `#4285f4`; 2 TWITTER/X `#1da1f2`; 3 META `#1877f2`; 4 META `#e4405f`; 5 PROFESSIONAL `#0a66c2`; 6 APPLE `#555`; 7 MEDIA `#1db954`; 8 SOCIAL `#ff4500`; 9 MESSAGING `#25d366`; 10 COMMERCE `#ff9900`; 11 MEDIA `#e50914`; 12 TRANSPORT `#000`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 18px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. Card-num 0.72em bold; h3 `#1a3a4a` 1em; description `#555` 0.84em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em, text `#222`.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (card labels use brand-specific colors listed above). No canvases on this page; any added canvases use `window.devicePixelRatio` scaling.
