# Two-way ANOVA

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Two-way ANOVA

**Subtitle:** One experiment, two factors, three tests — does each factor matter on its own, and does the effect of one depend on the level of the other?

## Two Knobs on One Tomato Garden

**Tags:** `core idea` (blue), `two factors` (green), `factorial design` (orange)

- **The garden** — 24 tomato plants, two knobs turned at once: fertilizer (no/yes) and water (weekly/daily)
- **Four cells** — every combination gets 6 plants: no+weekly, no+daily, yes+weekly, yes+daily
- **Cell means** — average yield per plant: 2.0, 3.0, 2.5, and 5.5 kg across the four combinations
- **One experiment** — two-way ANOVA studies both factors together instead of one at a time
- **Three questions** — does fertilizer matter, does water matter, and do the two interact?

*Example (italic):* The fertilizer-plus-daily-water cell averages 5.5 kg per plant — far more than either knob alone predicts.

**Key point:** A two-way ANOVA is one experiment with two factors crossed, answering three questions at once: each factor's main effect plus their interaction.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart of the four cell means, grouped by watering schedule with fertilizer as the within-group pair.

- **Title (bold 15px, `#1a5276`, top center):** "Tomato Yield per Plant: Four Fertilizer × Water Cells (kg, illustrative)".
- **Data:** water weekly group `[2.0, 2.5]` (no fertilizer, fertilizer); water daily group `[3.0, 5.5]`.
- **Axes:** origin x=70, baseline y=240, chart height 180, y scale 0–6 kg; horizontal gridlines `#e5e9ef` at 2, 4, 6 with 12px `#6b7280` labels on the left.
- **Bars:** 70px wide, 10px gap within each pair; weekly pair centered at x=230, daily pair at x=500; no-fertilizer bars fill `rgba(42,120,214,0.5)`, fertilizer bars fill `rgba(0,131,0,0.45)`.
- **Labels:** bold 13px value labels "2.0", "2.5", "3.0", "5.5" above each bar (blue `#2a78d6` for no-fertilizer, green `#008300` for fertilizer); group labels "water weekly" / "water daily" bold 12px `#444` below baseline.
- **Legend (top right):** 12px — blue swatch "no fertilizer", green swatch "fertilizer".
- **Annotation:** orange `#d95926` bold 13px near the 5.5 bar: "fertilizer + daily water: the standout cell".

## Reading Main Effects from the Margins

**Tags:** `worked example` (blue), `main effects` (green)

- **Grand mean** — the four cell means average to (2.0 + 3.0 + 2.5 + 5.5) / 4 = 3.25 kg per plant
- **Fertilizer effect** — unfertilized plants average 2.5 kg, fertilized 4.0 kg: a +1.5 kg main effect
- **Water effect** — weekly plants average 2.25 kg, daily 4.25 kg: a +2.0 kg main effect
- **Margins** — each main effect comes from row or column means, averaging over the other factor
- **Three F tests** — fertilizer F=12.4, water F=22.1, interaction F=8.9, each p < 0.01 (illustrative)

*Example (italic):* Read the fertilizer margin by hand: (2.0 + 3.0) / 2 = 2.5 kg without it vs (2.5 + 5.5) / 2 = 4.0 kg with it.

**Key point:** A main effect is a factor's average effect across the levels of the other factor — computed from marginal means, and tested with its own F statistic.

### Visualization (canvas `c2`, 720×300)

Dual-panel bar chart of the two sets of marginal means, each with an effect-size arrow, split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Marginal Means: Two Main Effects Read from the Margins".
- **Left panel (fertilizer):** axis origin x=55, width 280, baseline y=240, chart height 175, y scale 0–6; bars 80px wide at "no fertilizer" 2.5 fill `rgba(42,120,214,0.5)` and "fertilizer" 4.0 fill `rgba(0,131,0,0.45)`; bold 13px value labels "2.5" and "4.0" above the bars; orange `#d95926` 3px vertical arrow from the 2.5 level to the 4.0 level between the bars with bold 13px label "+1.5 kg"; caption 12px `#444` "fertilizer main effect: +1.5 kg".
- **Right panel (water):** axis origin x=400, width 280, same baseline/height/scale; bars "weekly" 2.25 fill `rgba(25,158,112,0.45)` and "daily" 4.25 fill `rgba(74,58,167,0.4)`; bold 13px value labels "2.25" and "4.25"; orange arrow labeled "+2.0 kg"; caption "water main effect: +2.0 kg".
- **Labels:** category names 12px `#444` below the baseline in both panels.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## When One Knob Changes What the Other Does

**Tags:** `interaction` (orange), `interaction plot` (blue), `rule of thumb` (green)

- **Fertilizer under weekly** — yield goes 2.0 → 2.5 kg, a gain of only +0.5 kg
- **Fertilizer under daily** — yield goes 3.0 → 5.5 kg, a gain of +2.5 kg, five times bigger
- **Interaction** — the effect of one factor changes with the level of the other factor
- **Parallel lines** — no interaction: both watering lines would rise by the same amount
- **Fanning lines** — interaction: the lines spread apart or cross because slopes disagree

*Example (italic):* Fertilizer is nearly useless for weekly-watered plants but transforms the daily-watered ones.

**Key point:** The interaction plot is the picture to check first — parallel lines mean the factors simply add; non-parallel lines mean one factor changes what the other does.

### Visualization (canvas `c3`, 720×300)

Dual-panel interaction plot: a hypothetical no-interaction version (left) vs the actual garden data (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Interaction Plot: Parallel Lines vs Our Garden (kg per plant)".
- **X positions (both panels):** two ticks, "no fert" at 30% and "fertilizer" at 70% of panel width, 12px `#444` labels below the baseline.
- **Left panel (hypothetical, no interaction):** axis origin x=55, width 280, baseline y=245, chart height 185, y scale 0–6; weekly line blue `#2a78d6` 3px from 2.0 to 3.5 with 5px dots; daily line green `#008300` 3px from 3.0 to 4.5 with 5px dots; 12px line labels "weekly" (blue) and "daily" (green) at the right ends; caption 12px `#444` "hypothetical: both rise +1.5 → parallel, no interaction".
- **Right panel (actual):** axis origin x=400, width 280, same baseline/height/scale; weekly line blue from 2.0 to 2.5; daily line green from 3.0 to 5.5; bold 13px orange `#d95926` annotation "slopes +0.5 vs +2.5 → interaction"; caption "our garden: the lines fan apart".
- **Dot value labels:** bold 12px matching line color at each dot (2.0, 3.5, 3.0, 4.5 left; 2.0, 2.5, 3.0, 5.5 right).
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Average That Describes No Plant

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The trap** — reporting "fertilizer adds +1.5 kg" when no watering schedule actually sees +1.5
- **Reality** — weekly plants gain +0.5 kg, daily plants gain +2.5 kg; +1.5 is just their average
- **Two one-ways** — separate one-way ANOVAs on each factor can never detect the interaction
- **Check first** — test the interaction before quoting main effects; if large, report per level
- **When it's fine** — with parallel lines (no interaction), main effects tell the whole story

*Example (italic):* A garden blog quotes "+1.5 kg from fertilizer"; a weekly-watering reader gets +0.5 kg and feels cheated.

**Common mistake:** Quoting a main effect as "the" effect when a strong interaction is present — the average of +0.5 and +2.5 describes no actual plant's situation.

### Visualization (canvas `c4`, 720×300)

Three-bar chart comparing the fertilizer effect under each watering schedule against the misleading averaged main effect.

- **Title (bold 15px, `#1a5276`, top center):** "The Fertilizer Effect: One Average, Two Different Stories".
- **Data:** bars "weekly watering" +0.5, "reported average" +1.5, "daily watering" +2.5 (kg gained from fertilizer).
- **Axes:** origin x=70, baseline y=240, chart height 175, y scale 0–3 kg; horizontal gridlines `#e5e9ef` at 1, 2, 3 with 12px `#6b7280` labels.
- **Bars:** 90px wide, centered at x=190, x=360, x=530; weekly fill `rgba(42,120,214,0.5)`, average fill `rgba(201,133,0,0.4)`, daily fill `rgba(0,131,0,0.45)`; bold 13px value labels "+0.5", "+1.5", "+2.5" above the bars; category labels 12px `#444` below the baseline.
- **Reference line:** dashed magenta `#d55181` (dash 4/3) horizontal line across the plot at the 1.5 level.
- **Annotation:** magenta `#d55181` bold 13px, two lines above the middle bar: "no plant experiences +1.5 —" / "it is just the average of +0.5 and +2.5".
- **Caption (12px `#444`, bottom):** "effect = fertilized mean minus unfertilized mean, per watering schedule (illustrative)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
