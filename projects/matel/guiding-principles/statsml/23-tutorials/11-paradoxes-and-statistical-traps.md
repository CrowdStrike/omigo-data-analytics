# Paradoxes & Statistical Traps

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Paradoxes & Statistical Traps

**Subtitle:** Situations where a perfectly reasonable reading of the data gives the wrong answer — because of how the rows were selected, sampled, or incentivized.

## Cards

Each card links to a topic page under `paradoxes-traps/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | SAMPLING PARADOXES | Berkson's Paradox | [11-paradoxes-and-statistical-traps/01-berksons-paradox.md](11-paradoxes-and-statistical-traps/01-berksons-paradox.md) | Keep only the rows that cleared a bar built from two traits, and a negative correlation appears between them that never existed in the full population. | selection effect, collider, fake trade-off |
| 2 | SAMPLING PARADOXES | The Inspection Paradox | [11-paradoxes-and-statistical-traps/02-the-inspection-paradox.md](11-paradoxes-and-statistical-traps/02-the-inspection-paradox.md) | Buses average one every 10 minutes, yet your wait is nearly 10 — random arrivals land in long gaps more often, so what you sample is bigger than the average. | size-biased sampling, waiting times, long gaps |
| 3 | INCENTIVE & JUDGMENT TRAPS | Goodhart's Law | [11-paradoxes-and-statistical-traps/03-goodharts-law.md](11-paradoxes-and-statistical-traps/03-goodharts-law.md) | When a measure becomes a target, people optimize the number instead of the thing it measured — and the number quietly stops meaning anything. | metric as target, gaming, broken proxy |
| 4 | INCENTIVE & JUDGMENT TRAPS | Anchoring, Decoys & Loss Aversion | [11-paradoxes-and-statistical-traps/04-anchoring-decoys-and-loss-aversion.md](11-paradoxes-and-statistical-traps/04-anchoring-decoys-and-loss-aversion.md) | The first number you see drags your estimate toward it — menus plant decoy prices and "don't lose it" wording to steer choices the same way. | anchoring, decoy pricing, loss framing |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "SAMPLING PARADOXES" `#2980b9`, "INCENTIVE & JUDGMENT TRAPS" `#8e44ad`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9` and `#8e44ad`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
