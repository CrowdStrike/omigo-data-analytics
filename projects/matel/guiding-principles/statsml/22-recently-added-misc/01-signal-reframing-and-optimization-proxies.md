# Signal Reframing & Optimization Proxies

**Page type:** other (sectioned card catalog: two section titles, each followed by a 3-column grid of non-linking content cards with category labels and topic tags)
**HTML title tag:** Signal Reframing & Optimization Proxies

**Subtitle:** Practices where a controversial or ethically sensitive signal is operationalized through indirect mechanisms — multi-variant testing, algorithmic selection, proxy metrics — so the outcome appears to emerge from neutral optimization rather than deliberate editorial choice. A second group covers signals captured beyond what the user knowingly shared.

## Section: Optimization as Framing

**Section blurb:** The controversial choice is delegated to a metric, an experiment, or a model — and the outcome is presented as neutral.

| # | Category | Title | Description | Topic tags |
|---|----------|-------|-------------|------------|
| 1 | MEDIA (`#c0392b`) | Multi-Variant Headline Testing | Publishing the same story with multiple headlines at different emotional intensities, letting a multi-armed bandit surface the winner. The algorithm selects the sensational framing — not the editor. | multi-armed bandit; emotional arousal; editorial deniability |
| 2 | FEEDS (`#e67e22`) | Engagement-Optimized Feeds | Optimizing for dwell time and interaction as a proxy for user satisfaction — where in practice the metric tends to favor high-arousal content over genuinely useful content. | dwell time proxy; arousal bias; satisfaction proxy |
| 3 | PRICING (`#8e44ad`) | Personalized Pricing via Context | Differential pricing presented as "dynamic pricing" or "contextual offers" — using device type, location, and browsing history as willingness-to-pay signals. | differential pricing; willingness-to-pay; contextual signals |
| 4 | UX (`#2c3e50`) | Conversion-Optimized UX Patterns | Interface choices like pre-checked boxes, multi-step unsubscribe, or confirm/dismiss asymmetry — framed as "reducing friction" or "improving conversion" when measured purely by completion rate. | conversion optimization; friction reduction; metric framing |
| 5 | CURATION (`#2980b9`) | Algorithmic Curation as Neutrality | Presenting algorithmically ranked content as "personalized for you" rather than acknowledging the editorial choices embedded in the objective function. | objective function; neutrality framing; hidden editorial |
| 6 | SEGMENTATION (`#e74c3c`) | Proxy Variables & Indirect Segmentation | Removing sensitive attributes but retaining zip code, device type, or browsing patterns that correlate strongly enough to reconstruct the original grouping for targeting purposes. | proxy variables; correlated features; indirect targeting |
| 7 | ATTENTION (`#e74c3c`) | Red Notification Badges via A/B Testing | Testing multiple notification badge colors and letting the experiment surface that red drives the highest open rate — leveraging a known urgency association while positioning it as a data-driven design decision. | color psychology; urgency association; A/B rationalization |
| 8 | SEGMENTATION (`#795548`) | User Agent as Price Segmentation | Using browser user agent as a feature to determine which inventory price points to surface — a proxy for mobile OS (Android vs iOS) which correlates with spending capacity, enabling segment-specific merchandising without explicit income signals. | user agent; OS as spending proxy; inventory tiering |
| 9 | URGENCY (`#d35400`) | Battery Level as Surge Pricing Signal | Users on low battery accept surge prices more readily, and apps can read battery state. No platform is documented to use it in pricing — but the observed correlation shows how an incidental device signal becomes a willingness-to-pay proxy. | battery state; urgency signal; price elasticity proxy |
| 10 | OPACITY (`#2c3e50`) | Model Opacity as Accountability Shield | Choosing complex models (deep nets, ensemble stacks) over interpretable ones — where opacity makes it difficult to attribute specific outcomes to specific input signals. The model's complexity reduces the ability to audit how individual features influence decisions. | interpretability trade-off; auditability gap; feature attribution |

## Section: Capture Beyond Consent

**Section blurb:** Signals the user did not knowingly share — collected as a by-product of ordinary interaction.

| # | Category | Title | Description | Topic tags |
|---|----------|-------|-------------|------------|
| 11 | INVOLUNTARY (`#d35400`) | Reaction & Reflex Time as Implicit Signal | Millisecond-level scroll hesitation, pause duration, and response latency reveal preferences the user did not consciously choose to share. The signal captures subconscious interest that bypasses the user's ability to curate what the system learns about them. | scroll hesitation; subconscious capture; consent boundary |
| 12 | SURVEILLANCE (`#1a5276`) | Precise Location as Behavioral Inference | Phone positioning resolves to a few meters outdoors, and indoor WiFi and Bluetooth place a device at a store section — enough to infer what someone is doing: which department, which competitor's lot, how long at a display. Context well beyond what "location permission" implies. | meter-level positioning; indoor positioning; implied vs actual consent |

## Regeneration instructions

- **Template:** nav-grid style card layout (see `docs/statsml/ui-templates/02-nav-grid`), but cards are plain `<div class="card">` content cards — NOT anchors; no links anywhere on the page. No canvases.
- **Structure:** h1, `.subtitle` paragraph, then two repeated blocks of: `<h2 class="section-title">`, `<p class="section-blurb">`, `<div class="grid">` of cards.
- **Card structure:** `<div class="card" style="border-color:CATEGORY_COLOR;">` containing `<div class="card-label" style="color:CATEGORY_COLOR;">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number, numbering continuous across both sections 1–12), `<p>description</p>`, and `<div class="topics">` of `<span class="topic-tag">` pills. Each card's inline `border-color` matches its label color (listed in the Category column above).
- **Layout:** `.grid` is CSS grid `repeat(3, 1fr)`, 16px gap; responsive: 2 columns below 800px, 1 column below 500px.
- **Page CSS:** global reset `* { margin:0; padding:0; box-sizing:border-box; }`; body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `.section-title` 1.15em `#1a5276` with `border-bottom: 2px solid #d6e4ee`; `.section-blurb` `#666` 0.88em; card background `#f8fafb`, border `1px solid #e0e0e0` (overridden per-card by inline color), radius 8px, padding 16px; card h3 `#1a5276` 1.0em; card p 0.85em `#555`; `.card-label` 0.72em bold uppercase letter-spacing 0.5px; `.topic-tag` background `#eef4f8`, border `1px solid #cdd`, radius 4px, padding 2px 6px, 0.7em `#555`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; card accents `#c0392b`, `#8e44ad`, `#2c3e50`, `#2980b9`, `#795548`, `#d35400`.
- No canvases on this page; if any were added, they would use `window.devicePixelRatio` scaling. In regenerated HTML, card links (if ever added) use `.html` extensions.
