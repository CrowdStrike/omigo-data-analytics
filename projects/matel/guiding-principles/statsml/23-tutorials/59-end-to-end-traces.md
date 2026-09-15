# End-to-End Traces

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** End-to-End Traces

**Subtitle:** One everyday action followed step by step through every subsystem it touches — from the trigger to the final result.

## Cards

Each card links to a topic page under `end-to-end-traces/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | SYSTEM TRACES | You Type a URL and Press Enter | [59-end-to-end-traces/01-you-type-a-url-and-press-enter.md](59-end-to-end-traces/01-you-type-a-url-and-press-enter.md) | Between the Enter key and the first pixel, a dozen subsystems each do one job in strict order — keyboard, OS, browser, DNS, TCP, TLS, HTTP, server, render. | browser, DNS to render, layers |
| 2 | SYSTEM TRACES | The Life of an Analytics Event | [59-end-to-end-traces/02-the-life-of-an-analytics-event.md](59-end-to-end-traces/02-the-life-of-an-analytics-event.md) | One "Add to cart" tap travels eight hops — SDK, beacon, collector, enricher, sessionizer, warehouse, model, dashboard — and it can die or double at every one of them. | event pipeline, eight hops, data loss |
| 3 | SYSTEM TRACES | The Life of an ML Prediction | [59-end-to-end-traces/03-the-life-of-an-ml-prediction.md](59-end-to-end-traces/03-the-life-of-an-ml-prediction.md) | One card swipe traced end to end — request, features, score, decision, log, and the label that comes back days later to retrain the model. | serving path, latency budget, feedback loop |
| 4 | SYSTEM TRACES | A Page Load, Round Trip | [59-end-to-end-traces/04-a-page-load-round-trip.md](59-end-to-end-traces/04-a-page-load-round-trip.md) | The same journey in four legs — the client prepares the request with DNS and cookies, HTTPS carries it, edge and application answer, the browser paints. | cookies, https, cdn, application side |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "SYSTEM TRACES" `#2980b9`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
