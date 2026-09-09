# Metrics — Patterns, Anti-Patterns & Design

**Page type:** grid page (card navigation grid, 3 columns, one philosophy callout, cards with colored borders)
**HTML title tag:** Metrics — Patterns, Anti-Patterns & Design

**Subtitle:** Good metrics drive good decisions. Bad metrics drive confident wrong decisions. Sub-documents covering: what makes a metric good or bad, business vs informational metrics, collection frequency, UI reporting pitfalls, anti-patterns, and design patterns.

## Callout (philosophy box)

**The meta-principle:** A metric is a LENS on reality — not reality itself. Every lens distorts. The distortion is acceptable if you KNOW what it distorts and have compensating lenses (counter-metrics). A metric without a counter-metric is a one-eyed view. A metric without a threshold is a number without meaning. A metric without an owner is noise with a dashboard.

## Cards

Cards link to detail pages under `metrics/`; card and file index numbers match. Each card shows a colored uppercase category label, a numbered title, and a description. Each card anchor also carries an inline `border-color` matching its label color.

| # | Category | Title | Link | Description |
|---|----------|-------|------|-------------|
| 1 | EXAMPLES | Good Metrics | [14-metrics-design/01-good-metrics.md](14-metrics-design/01-good-metrics.md) | Real-world examples by domain (SaaS, Security, Healthcare, Manufacturing) — what makes each metric work and why. Actionable + hard to game + has counter. |
| 2 | EXAMPLES | Bad Metrics | [14-metrics-design/02-bad-metrics.md](14-metrics-design/02-bad-metrics.md) | Real-world disasters — metrics that looked good but caused damage. Vanity numbers, gameable targets, lagging indicators mistaken for leading ones. |
| 3 | ANTI-PATTERNS | Metric Anti-Patterns | [14-metrics-design/03-metric-anti-patterns.md](14-metrics-design/03-metric-anti-patterns.md) | No counter-metric, activity vs outcome, redefining when bad, average hiding bimodal, success-only dashboard, GMV vanity, MAU inflation, NPS, CTR, trivially improvable, and more. |
| 4 | DESIGN | Collection Frequency | [14-metrics-design/04-collection-frequency.md](14-metrics-design/04-collection-frequency.md) | Cases where timing created blind spots — fraud batched daily, spikes in 15-min intervals, weekly security reviews. Frequency must match decision speed. |
| 5 | DESIGN | Granularity & Resolution | [14-metrics-design/05-granularity-and-resolution.md](14-metrics-design/05-granularity-and-resolution.md) | Too coarse hides incidents. Too fine is noise. The low-hanging fruit plateau: easy cases create the business case, hard cases are what's left. |
| 6 | DESIGN | UI Reporting Pitfalls | [14-metrics-design/06-ui-reporting-pitfalls.md](14-metrics-design/06-ui-reporting-pitfalls.md) | Dashboard design failures — green before test done, Y-axis tricks, stale data, success-only view, alert fatigue. Same data, two designs → opposite decisions. |
| 7 | STATISTICAL | Metric Testing (Non-Normal) | [14-metrics-design/07-metric-testing-non-normal.md](14-metrics-design/07-metric-testing-non-normal.md) | Most metrics aren't normal. t-test gives wrong answers. Alternatives: Mann-Whitney, bootstrap, permutation, quantile tests. |
| 8 | PATTERNS | Design Patterns & Checklist | [14-metrics-design/08-design-patterns-and-checklist.md](14-metrics-design/08-design-patterns-and-checklist.md) | 7-point metric design checklist. Metric lifecycle: Create → Validate → Operate → Retire. How to build metrics that resist gaming and inform real decisions. |
| 9 | ANTI-PATTERNS | Bragging Metrics | [14-metrics-design/09-bragging-metrics.md](14-metrics-design/09-bragging-metrics.md) | A metric chosen because it is the most flattering true statement available. The number is not a lie; the selection is the defect. |
| 10 | ANTI-PATTERNS | Sign-Inverting Metrics | [14-metrics-design/10-sign-inverting-metrics.md](14-metrics-design/10-sign-inverting-metrics.md) | Metrics that improve because something bad happened outside their view — crash rate down because the crash killed the reporter. |
| 11 | BIAS | Ranking Position Bias | [14-metrics-design/11-ranking-position-bias.md](14-metrics-design/11-ranking-position-bias.md) | How top-k positions on search results and product listings create self-reinforcing rank, starve new items, and make CTR a useless relevance signal. |
| 12 | BIAS | Recommendation Feedback Loops | [14-metrics-design/12-recommendation-feedback-loops.md](14-metrics-design/12-recommendation-feedback-loops.md) | Autoplay, view-history-based ranking, and business decisions create self-reinforcing content bubbles and confounded engagement signals. |
| 13 | BIAS | Mobile Swipe Biases | [14-metrics-design/13-mobile-swipe-biases.md](14-metrics-design/13-mobile-swipe-biases.md) | Single-result swipe interfaces eliminate visual position bias but introduce temporal decay, attention fatigue, and framing effects from no-comparison context. |
| 14 | STATISTICAL | Small-Sample Rate Extremes | [14-metrics-design/14-small-sample-rate-extremes.md](14-metrics-design/14-small-sample-rate-extremes.md) | With few impressions, rate metrics (CTR, conversion, sales per impression) become noise masquerading as signal — one lucky event creates an extreme rate the system treats as truth. |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, one `.philosophy` callout, then one `.grid` of `.card` anchors.
- **Layout:** `.grid` is CSS grid, `repeat(3, 1fr)`, 16px gap, margin `30px 0`; responsive: 2 columns below 800px, 1 column below 500px.
- **Links:** the table above links to the `.md` versions for markdown navigation; in the regenerated HTML each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="card" href="..." style="border-color:LABEL_COLOR;">` containing `<div class="card-label" style="color:LABEL_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number), `<p>description</p>`.
- **Per-card label/border colors:** card 1 `#27ae60`; card 2 `#e74c3c`; card 3 `#c0392b`; card 4 `#e67e22`; card 5 `#f39c12`; card 6 `#2980b9`; card 7 `#8e44ad`; card 8 `#1a5276`; card 9 `#e74c3c`; card 10 `#c0392b`; card 11 `#2980b9`; card 12 `#8e44ad`; card 13 `#e67e22`; card 14 `#f39c12`.
- **Card style:** background `#f8fafb`, base border `1px solid #e0e0e0` (overridden per card by inline border-color), radius 8px, padding 16px; hover: shadow `0 4px 12px rgba(0,0,0,0.1)`, border `#2980b9`. Label 0.72em bold uppercase letter-spacing 0.5px, h3 `#1a5276` 1.0em, description 0.85em `#555`.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No canvases on this page; detail pages use `window.devicePixelRatio` scaling.
