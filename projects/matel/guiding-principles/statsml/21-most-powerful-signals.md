# Most Powerful Signals in Data

**Page type:** grid page (3-column card grid with topic-tag pills per card; per-card border color matches label color)
**HTML title tag:** Most Powerful Signals in Data

**Subtitle:** The raw signals that tech companies use to power recommendations, ranking, pricing, and personalization. Each signal type gets its own page showing how it's captured, shaped, and exploited.

## Cards

Each card links to a detail page under `most-powerful-signals/`. The card shows a colored uppercase category label, a numbered title, a description, and a row of topic-tag pills. Each card's border color is set inline to the same hex as its label color (shown in the Category column).

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | BEHAVIORAL (#e74c3c) | Engagement Signals | [21-most-powerful-signals/01-engagement-signals.md](21-most-powerful-signals/01-engagement-signals.md) | Likes, shares, comments, saves, reactions — the explicit actions users take that reveal intent and preference. | likes, shares, comments |
| 2 | SELECTION (#2980b9) | Click-Through Rate (CTR) | [21-most-powerful-signals/02-click-through-rate-ctr.md](21-most-powerful-signals/02-click-through-rate-ctr.md) | The ratio of clicks to impressions — the single most optimized metric in digital advertising, search, and recommendation. | position bias, selection signal, implicit preference |
| 3 | ATTENTION (#27ae60) | Dwell Time & Session Duration | [21-most-powerful-signals/03-dwell-time-and-session-duration.md](21-most-powerful-signals/03-dwell-time-and-session-duration.md) | How long a user stays — the strongest implicit signal of content quality. Powers feed ranking at every major platform. | time-on-page, scroll depth, session length |
| 4 | CONTEXT (#e67e22) | Location & Geo Signals | [21-most-powerful-signals/04-location-and-geo-signals.md](21-most-powerful-signals/04-location-and-geo-signals.md) | GPS, IP, WiFi fingerprint — location reveals intent better than search queries. Powers local ads, surge pricing, foot traffic. | GPS, geofencing, proximity |
| 5 | TRANSACTION (#8e44ad) | Purchase & Conversion Signals | [21-most-powerful-signals/05-purchase-and-conversion-signals.md](21-most-powerful-signals/05-purchase-and-conversion-signals.md) | What people actually spend money on. The highest-intent signal — everything else is a proxy for this. | basket analysis, LTV, frequency |
| 6 | INTENT (#1a5276) | Search Queries & Navigation | [21-most-powerful-signals/06-search-queries-and-navigation.md](21-most-powerful-signals/06-search-queries-and-navigation.md) | What users type and refine when searching — intent classes, session feedback, click labels, and demand trends that feed ML systems. | query intent, sessions, click labels |
| 7 | NETWORK (#16a085) | Social Graph & Connections | [21-most-powerful-signals/07-social-graph-and-connections.md](21-most-powerful-signals/07-social-graph-and-connections.md) | Who you know predicts what you'll do next. Friend clusters, interaction frequency, influence propagation paths. | homophily, influence, clusters |
| 8 | DEVICE (#795548) | Device & Environment Context | [21-most-powerful-signals/08-device-and-environment-context.md](21-most-powerful-signals/08-device-and-environment-context.md) | Time of day, device type, OS, battery level, network speed — the ambient context that shapes what content works. | time-of-day, device type, connectivity |
| 9 | CONSUMPTION (#f39c12) | Content Consumption Patterns | [21-most-powerful-signals/09-content-consumption-patterns.md](21-most-powerful-signals/09-content-consumption-patterns.md) | Watch %, read depth, skip rate, replay — what people actually consume vs. what they say they want. | completion rate, skip signals, replay |
| 10 | ENGINEERING (#2c3e50) | Feature Engineering | [21-most-powerful-signals/10-feature-engineering.md](21-most-powerful-signals/10-feature-engineering.md) | How raw signals become model-ready features. Binning, ratios, lag windows, interaction terms — the craft that turns noisy data into predictive power. | binning, ratios, lag features |
| 11 | SUBCONSCIOUS (#d35400) | Reaction & Reflex Time | [21-most-powerful-signals/11-reaction-and-reflex-time.md](21-most-powerful-signals/11-reaction-and-reflex-time.md) | Millisecond-level hesitation and response latency — the signal that's nearly impossible to consciously mask. Used by short-video platforms to detect genuine interest, by implicit association tests to measure hidden bias, and by intelligence services to identify concealed associations via Stroop-like interference. | implicit association, scroll hesitation, Stroop interference |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one `.grid` of `.card` anchors. No philosophy callouts on this page.
- **Layout:** `.grid` is CSS grid, `repeat(3, 1fr)`, 16px gap, margin `20px 0 30px 0`; responsive: 2 columns below 800px, 1 column below 500px.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="card" href="..." style="border-color:HEX;">` (same hex as its label color, per row) containing `<div class="card-label" style="color:HEX">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, `<div class="topics">` with one `<span class="topic-tag">` per topic listed in the Topics column.
- **Card style:** background `#f8fafb`, border `1px solid #e0e0e0` (color overridden inline per card), radius 8px, padding 16px; hover: shadow `0 4px 12px rgba(0,0,0,0.1)`, border `#2980b9`. Label 0.72em bold uppercase letter-spacing 0.5px, h3 `#1a5276` 1.0em, description 0.85em `#555` margin 0.
- **Topic tags:** `.topics` flex row with wrap, 4px gap, margin-top 8px; `.topic-tag` background `#eef4f8`, border `1px solid #cdd`, radius 4px, padding 2px 6px, 0.7em, `#555`.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em, margin-bottom 30px. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus per-card label/border colors listed in the table. No canvases on this page; any canvases elsewhere use `window.devicePixelRatio` scaling.
