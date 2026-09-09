# Recently Added & Miscellaneous

**Page type:** grid page (card navigation grid, 3 columns, cards with topic tag pills)
**HTML title tag:** Recently Added & Miscellaneous

**Subtitle:** Topics that don't fit neatly into a single category — recent additions, cross-cutting themes, and standalone explorations.

## Cards

Each card links to a detail page under `recently-added-misc/`. The card shows a colored uppercase category label, a numbered title, a one-to-two sentence description, and a row of topic tag pills.

| # | Category | Title | Link | Description |
|---|----------|-------|------|-------------|
| 1 | SIGNALS | Signal Reframing & Optimization Proxies | [22-recently-added-misc/01-signal-reframing-and-optimization-proxies.md](22-recently-added-misc/01-signal-reframing-and-optimization-proxies.md) | Practices where controversial signals are operationalized through indirect mechanisms — algorithmic selection, proxy metrics, and neutral framing. |
| 2 | HISTORY | Evolution of Computing, Data & Tech | [22-recently-added-misc/02-evolution-of-computing-data-and-tech.md](22-recently-added-misc/02-evolution-of-computing-data-and-tech.md) | Factual history of how the industry arrived here — significant phases in each strand, including the ones that ended and the ones that became ordinary. |
| 3 | TRACKING | Tracking Data Collection Methods | [22-recently-added-misc/03-tracking-data-collection-methods.md](22-recently-added-misc/03-tracking-data-collection-methods.md) | How data is collected across devices, platforms, and physical spaces — from cookies and pixels to eye tracking and license plate readers. |
| 4 | DATA COLLECTION | Platform APIs — User Activity Tracking | [22-recently-added-misc/04-platform-apis-user-activity-tracking.md](22-recently-added-misc/04-platform-apis-user-activity-tracking.md) | Passive behavioral data via official APIs — impressions, views, biometrics, movement. Granularity from real-time to daily aggregates. |
| 5 | DATA COLLECTION | Platform APIs to Fetch Data | [22-recently-added-misc/05-platform-apis-to-fetch-data.md](22-recently-added-misc/05-platform-apis-to-fetch-data.md) | Landscape of what data platforms expose — content, metadata, telemetry, audit logs. Messaging, storage, identity, health, mobile OS, AR/VR, social. |
| 6 | DATA COLLECTION | Personal Data Archives & Takeout | [22-recently-added-misc/06-personal-data-archives-and-takeout.md](22-recently-added-misc/06-personal-data-archives-and-takeout.md) | Download-your-own-data — what arrives vs what the platform knows. UGC dumps, ad interest graphs, inferred demographics, behavioral profiles. |
| 7 | HISTORY | How Things Happened Together | [22-recently-added-misc/07-how-things-happened-together.md](22-recently-added-misc/07-how-things-happened-together.md) | Related strands put on one timeline so the relationship shows without being drawn — hardware capability arriving before the software that used it, query languages accumulating rather than replacing, workloads preceding their tools, and the one window where every strand turned over at once. |

**Topic tags per card:**

| # | Topic tags |
|---|------------|
| 1 | headline testing, engagement feeds, personalized pricing, proxy variables |
| 2 | languages, data & ML, architectures, internet, open source |
| 3 | cookies, pixels, fingerprinting, wearables, location |
| 4 | APIs, activity, rate-limits |
| 5 | APIs, content, telemetry, audit-logs |
| 6 | takeout, GDPR, archives |
| 7 | cross-strand, timelines, causality, synchrony |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(3, 1fr)`, 16px gap, `margin-top: 15px`; responsive: 2 columns below 900px, 1 column below 600px.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, and `<div class="topics">` with `<span class="topic-tag">` pills.
- **Category label colors** (set by a small script mapping `.card-num` text to color): SIGNALS `#e74c3c`; HISTORY `#2980b9`; TRACKING `#27ae60`; DATA COLLECTION `#8e44ad`. Default `.card-num` color `#2980b9`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. `.card-num` 0.75em bold; h3 `#1a3a4a` 1em; description `#555` 0.85em.
- **Topic tag style:** `.topic-tag` — background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, color `#666`; `.topics` is flex with wrap, 4px gap, 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No canvases on this page; detail pages use `window.devicePixelRatio` canvas scaling.
