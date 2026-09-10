# N=1 Generalization

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** N=1 Generalization — Pseudoscience in Data Analysis

**Subtitle:** Individual anecdote replacing statistical evidence

## One Anecdote Extrapolated as Universal Truth

- **"My grandma smoked until 95":** Therefore smoking isn't that bad — one lucky survivor against 480,000 US smoking-related deaths per year. The anecdote doesn't disprove the population statistic; it is exactly the outlier the statistics predict will occasionally exist.
- **"We tried agile and it failed":** Therefore agile doesn't work — but this is one team, with specific dysfunction, at one company, and maybe the team failed rather than the methodology. The single bad experience becomes permanent "evidence" against the approach.
- **"Someone got COVID twice despite vaccination":** Therefore vaccines don't work — one breakthrough case against millions with reduced severity and death. The anecdote is memorable; the millions of non-events are invisible.
- **Product feature:** One customer says they hate feature X, which has a 95% satisfaction rate. The vocal unhappy customer's story overrides the quantitative data because stories are more compelling to human brains than numbers.

**Why it's pseudoscience:** The human brain treats vivid stories as more true than abstract numbers, so one emotional anecdote beats 10,000 data points in a meeting — but it shouldn't.

### Visualization (canvas `c1`, 720×300)

Two-box comparison diagram: anecdote vs statistics.

- **Title (bold 17px `#1a5276`, top center):** "N=1 Anecdote vs N=480,000 Statistics: Which Wins in a Meeting?"
- **Left box:** rectangle at x=60, y=50, 250×120; fill `rgba(231,76,60,0.3)`, stroke `#e74c3c` width 2. Centered text (x=185): bold red two lines "\"My grandma smoked" / "until 95!\"" (y=80, 100), then dark `#333` regular lines "Vivid. Emotional. Memorable." (y=130) and "N = 1" (y=150).
- **Right box:** rectangle at x=410, y=50, 250×120; fill `rgba(39,174,96,0.3)`, stroke `#27ae60` width 2. Centered text (x=535): bold green two lines "480,000 smoking deaths" / "per year (US)" (y=80, 100), then `#333` lines "Abstract. Boring. Forgettable." (y=130) and "N = 480,000" (y=150).
- **Bottom line (bold red `#e74c3c`, centered, y=h-8):** "The anecdote wins in every meeting. The statistics are correct."

### Visualization (canvas `c2`, 720×300)

Grouped horizontal bar chart comparing two metrics across two categories.

- **Title (bold 17px `#1a5276`, top center):** "Statistical Power of Anecdotes"
- **Categories:** "Anecdote", "Statistics" (bold `#1a5276` labels at left of each group).
- **Metrics (legend swatches at top, 14×14 squares with `#2c3e50` 16px labels):**
  - "Convincingness to Humans", color `#e67e22`, values: Anecdote 90%, Statistics 30%.
  - "Actual Evidential Value", color `#27ae60`, values: Anecdote 1%, Statistics 99%.
- **Bars:** horizontal, 22px tall, 8px gap, group width 250px scaled to value/100, starting x = w/2−groupW−30 plus 90px label offset; fill is the metric color at 53% alpha (hex suffix `88`), 1px stroke in the metric color; bold value label ("90%", "30%", "1%", "99%") in the metric color to the right of each bar.
- **Insight (bold 18px `#c0392b`, bottom center):** "What convinces us is inversely correlated with what is actually evidence."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` with left `<td>` (40%) holding `.obj-title`, a `<ul>` of bullets, and a closing `<p>`; right `<td>` (60%, centered) holding the two canvases stacked.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark red `#c0392b`, gray text `#666`/`#333`/`#555`.
- In regenerated HTML, any card links use `.html` extensions.
