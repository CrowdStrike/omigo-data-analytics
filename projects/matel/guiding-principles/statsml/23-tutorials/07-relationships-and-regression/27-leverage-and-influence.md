# Leverage & Influence

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Leverage & Influence

**Subtitle:** One unusual point can drag a whole regression line toward itself — leverage says how far out it sits, influence says how hard it actually pulled, and Cook's distance puts one number on the pull

## The Mansion at the End of the Street

**Tags:** `core idea` (blue), `one point` (orange), `regression line` (green)

- **The street** — 9 ordinary houses, 800 to 1,600 sq ft, sold for $180k up to $342k
- **The fit** — a line through those 9 sales says price rises about $201 per extra sq ft
- **The mansion** — one 4,000 sq ft fixer-upper sells for $500k; the trend predicted $824k
- **The drag** — refit with the mansion included and the slope collapses to $95 per sq ft
- **One point** — 1 sale out of 10 cut the street's price-per-sq-ft estimate in half

*Example (italic):* An agent quoting "$95 per sq ft" for a 1,000 sq ft listing would underprice it by roughly $100k — because of one distant mansion.

**Key point:** A single point far out on the x-axis can tilt the whole line toward itself. Always ask: would my line survive deleting one point?

### Visualization (canvas `c1`, 720×300)

Single-panel scatter of the 10 house sales with two fitted lines: the 9-house fit (blue solid) and the 10-house fit including the mansion (magenta dashed).

- **Title (bold 15px, `#1a5276`, top center):** "One Mansion, Two Very Different Lines".
- **Data (sq ft, price $k):** 9 houses `[800,180], [900,198], [1000,222], [1100,238], [1200,262], [1300,278], [1400,302], [1500,318], [1600,342]`; mansion `[4000, 500]`.
- **Axes:** origin x=60, baseline y=250, plot width 620, height 195; x scale 700–4,200 sq ft with ticks at 1,000 / 2,000 / 3,000 / 4,000 (12px `#444`); y scale $100k–$900k with ticks at 200 / 400 / 600 / 800 labeled "$200k" etc.; axis lines 2px `#1a5276`; light grid `#e5e9ef` on y ticks.
- **Points:** 9 houses as blue `#2a78d6` 5px dots; mansion as orange `#d95926` 7px dot with bold 12px orange label "4,000 sq ft, sold $500k" above-left of it.
- **Line without mansion:** blue `#2a78d6` solid 3px, `y = 18.4 + 0.2013x`, drawn x=700→4,200; bold 13px blue annotation near its upper end: "without the mansion: $201 / sq ft".
- **Line with mansion:** magenta `#d55181` dashed (dash 6/4) 3px, `y = 143.7 + 0.0948x`, drawn x=700→4,200; bold 13px magenta annotation below it mid-chart: "with it: $95 / sq ft".
- **Caption (12px `#444`, bottom left):** "same 9 houses in both fits — only the mansion differs (illustrative)".

## On the Trend vs Off the Trend

**Tags:** `worked example` (blue), `leverage` (orange), `influence` (red)

- **Leverage** — how unusual a point's x-value is; the mansion's h = 0.93 vs about 0.10–0.16 for the rest
- **The average** — leverage averages p/n = 2/10 = 0.2 here, so 0.93 means one point owns the fit
- **Not guilt** — leverage is only potential; a priced-on-trend mansion at $824k leaves the line untouched
- **Influence** — actual pull on the fit; it needs high leverage AND a big miss from the trend at once
- **The $500k sale** — same leverage, but $324k below trend, so it tilts the slope from $201 to $95

*Example (italic):* Two identical mansions: one sells at the trend's $824k and changes nothing; one sells at $500k and halves the slope.

**Key point:** Leverage is where the point sits (extreme x); influence is what it does to the fit. High leverage plus a large residual is the dangerous combination.

### Visualization (canvas `c2`, 720×300)

Dual-panel scatter: the same 9 houses plus a mansion priced on-trend (left) vs the mansion priced off-trend (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same Leverage, Different Influence".
- **Shared data:** the 9 houses from c1; both panels use x scale 700–4,200, y scale $100k–$900k.
- **Left panel (on-trend):** origin x=55, plot width 280, baseline y=240, height 180; heading bold 12px `#444` "mansion sells on trend: $824k"; mansion point `[4000, 824]` as green `#008300` 7px dot; a single blue `#2a78d6` solid 3px line `y = 18.4 + 0.2013x` (the 10-point fit is identical); green bold 12px annotation, two lines: "line doesn't move" / "high leverage, zero influence".
- **Right panel (off-trend):** origin x=400, plot width 280, same baseline/height; heading "mansion sells off trend: $500k"; mansion point `[4000, 500]` as orange `#d95926` 7px dot; blue solid 3px line `y = 18.4 + 0.2013x` and magenta `#d55181` dashed 3px line `y = 143.7 + 0.0948x`; magenta bold 12px annotation, two lines: "slope $201 → $95" / "high leverage + big miss = pull".
- **Points:** 9 houses as blue 4px dots in both panels; x ticks 1,000 / 4,000 only (11px `#444`) to avoid crowding.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Cook's Distance Puts a Number on It

**Tags:** `rule of thumb` (green), `where it's used` (blue)

- **The recipe** — Cook's distance blends the residual and the leverage into one score per point
- **The meaning** — D asks "how much would every fitted value change if I deleted this point?"
- **The street** — the 9 houses score D between 0.00 and 0.19; the $500k mansion scores D ≈ 52
- **Thresholds** — common flags are D > 4/n (here 0.4) for a look, D > 1 for serious concern
- **The workflow** — flag, investigate, and refit without the point to see what changes; never silently delete

*Example (italic):* One `influence.measures()` or `cooks_distance` call on the street's fit returns nine tiny numbers and one 52 — the mansion convicts itself.

**Key point:** Cook's distance is the standard delete-one-point audit. A point can hide a small residual (it dragged the line to itself) yet still show a huge D.

### Visualization (canvas `c3`, 720×300)

Bar chart of Cook's distance for all 10 sales, with the mansion's bar broken at the top because it is two orders of magnitude above the rest.

- **Title (bold 15px, `#1a5276`, top center):** "Cook's Distance per House: One Bar Off the Chart".
- **Data:** x labels (sq ft) `["800","900","1000","1100","1200","1300","1400","1500","1600","4000"]`; Cook's D `[0.19, 0.10, 0.03, 0.01, 0.00, 0.01, 0.04, 0.07, 0.15, 52.3]`.
- **Axes:** origin x=60, baseline y=245, plot width 620, height 185; y scale 0–0.6 with ticks 0 / 0.2 / 0.4 / 0.6 (12px `#444`); x labels 11px `#444` below each bar; bar width 44px, gap 18px.
- **House bars:** fill `rgba(42,120,214,0.45)`, 1px `#2a78d6` stroke; each bar's D value 11px `#6b7280` above it.
- **Mansion bar:** fill `rgba(217,89,38,0.55)`, drawn from baseline to y=45 (past the 0.6 tick), with two white diagonal break slashes (3px, at y≈70 and y≈78) across it; bold 13px orange `#d95926` label above: "D ≈ 52 — off the chart".
- **Threshold:** dashed green `#008300` (dash 5/4) horizontal line at D = 0.4 spanning the plot, bold 12px green label at its right end: "rule of thumb: 4/n = 0.4".
- **Caption (12px `#444`, bottom left):** "residual × leverage combined; every other sale is far below the line".

## The Shock in the Middle of the Street

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The mistake** — assuming any wild price is influential; a shocking y at an ordinary x mostly isn't
- **The test** — the 1,200 sq ft house resells for a crazy $400k, $138k above its previous sale
- **No tilt** — 1,200 sq ft is the street's average size (leverage 0.11), so the slope stays $201 / sq ft
- **Just a lift** — the whole line shifts up about $15k (intercept $18k → $34k); it cannot rotate
- **Edge vs middle** — the same-sized shock out at 4,000 sq ft is what tilts the line, as the mansion showed

*Example (italic):* A $400k sale mid-street nudges every prediction up $15k; a $324k miss at the street's edge rewrites the price per square foot.

**Common mistake:** Screening for influence by residual size alone. A big residual at the center of x barely matters; a modest residual at extreme x can steer the whole fit.

### Visualization (canvas `c4`, 720×300)

Dual-panel scatter contrasting a price shock at the middle of the size range (left, line lifts) with the shock at the edge (right, line tilts), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Shock in the Middle Lifts the Line; Shock at the Edge Tilts It".
- **Left panel (middle shock):** origin x=55, plot width 280, baseline y=240, height 180; x scale 700–1,700, y scale $100k–$450k; heading bold 12px `#444` "1,200 sq ft house sells for $400k"; data: the 9 houses from c1 but with `[1200, 400]` replacing `[1200, 262]`, shock point as orange `#d95926` 7px dot, others blue 4px dots; old line blue `#2a78d6` solid 3px `y = 18.4 + 0.2013x`; new line violet `#4a3aa7` dashed (dash 6/4) 3px `y = 33.7 + 0.2013x` (parallel); violet bold 12px annotation, two lines: "same slope: $201 / sq ft" / "line just lifts ~$15k".
- **Right panel (edge shock):** origin x=400, plot width 280, same baseline/height; x scale 700–4,200, y scale $100k–$900k; heading "4,000 sq ft mansion sells for $500k"; the 9 original houses as blue 4px dots, mansion `[4000, 500]` as orange 7px dot; blue solid line `y = 18.4 + 0.2013x` and magenta `#d55181` dashed line `y = 143.7 + 0.0948x`; magenta bold 12px annotation: "slope tilts: $201 → $95".
- **X ticks:** left 800 / 1,200 / 1,600; right 1,000 / 4,000 (11px `#444`).
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Caption (12px `#444`, bottom center):** "both shocks are one house; only the one at extreme size moves the slope (illustrative)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
