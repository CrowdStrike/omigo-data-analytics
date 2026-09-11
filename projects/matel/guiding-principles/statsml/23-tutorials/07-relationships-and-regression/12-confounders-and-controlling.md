# Confounders & Controlling

**Page type:** detail page (tutorial: card-sections, each a two-column layout table — text left 50%, canvas right 50%; one section uses a 3-column 38/31/31 layout)
**HTML title tag:** Confounders & Controlling

**Subtitle:** A hidden third factor can create a link between two things — controlling means comparing like with like so the illusion disappears

## Coffee Looks Guilty

**Tags:** `core idea` (blue), `running example` (green)

- **The study** — 200 coffee drinkers and 200 non-drinkers, checked for heart disease
- **The headline** — 20% of coffee drinkers have it vs 10% of non-drinkers
- **Twice the rate** — coffee looks like it doubles heart disease risk
- **The catch** — the groups differ in more than coffee: smokers here love coffee
- **Confounder** — a third factor that drives both the habit and the disease

*Example:* In this data 120 of the 200 coffee drinkers smoke, but only 40 of the 200 non-drinkers.

**Key point:** Before blaming coffee, ask what else separates the two groups — here it is smoking.

### Visualization (canvas `c1`, 720×300)

Two-bar chart of raw heart-disease rates.

- **Title (bold 15px, ink `#1a5276`, top center):** "Heart disease rate: coffee drinkers vs non-drinkers"
- **Bars (width 140px):**
  - "coffee drinkers": 20%, orange `#d95926`, value label bold 14px "20%  (40 of 200)" above bar
  - "non-drinkers": 10%, blue `#2a78d6`, value label "10%  (20 of 200)"
- **Axes:** y 0% to 25%, gridlines (`#e5e9ef`) and muted labels every 5%; padding: left 70, top 50, bottom 60
- **Caption (bold 13px magenta `#d55181`, bottom center):** "2× the rate — but the groups also differ in who smokes"

## Split by Smoking and the Effect Vanishes

**Tags:** `worked example` (green), `do it by hand` (blue)

- **Within smokers** — coffee: 36/120 = 30% sick; no coffee: 12/40 = 30%. Identical
- **Within non-smokers** — coffee: 4/80 = 5%; no coffee: 8/160 = 5%. Identical
- **Recount the totals** — coffee: 36+4 = 40/200 = 20%; no coffee: 12+8 = 20/200 = 10%
- **Controlling** — means comparing within smokers and within non-smokers separately
- **Verdict** — smoking carries all the risk (30% vs 5%); coffee adds nothing

*Example:* The same 400 people, re-sorted into four cells, tell the opposite story from two cells.

**Key point:** Controlling for a confounder = splitting the data by it and comparing inside each slice.

This section uses the 3-column layout (text 38%, two canvases 31% each).

### Visualization (canvas `c2a`, 420×340)

Grouped bar chart of stratified disease rates.

- **Title (bold 15px, ink, top center):** "Within each smoking group"
- **Groups (bar width 55px, in-group gap 14px):**
  - "smokers": coffee 30% (sublabel "36/120"), no-coffee 30% (sublabel "12/40")
  - "non-smokers": coffee 5% (sublabel "4/80"), no-coffee 5% (sublabel "8/160")
- **Bar colors:** coffee = orange `#d95926`, no coffee = blue `#2a78d6`; bold 12px "%" value labels above bars, muted 11px fraction sublabels and bold 12px group labels below baseline
- **Legend (top left, 11px squares):** orange "coffee", blue "no coffee"
- **Axes:** y 0% to 35%, gridlines and muted labels every 5%; padding: left 55, top 50, bottom 80
- **Caption (bold 13px green `#008300`, bottom center):** "30 = 30 and 5 = 5: coffee adds nothing"

### Visualization (canvas `c2b`, 420×340)

Two stacked composition bars showing who is in each group.

- **Title (bold 15px, ink, top center):** "Who is in each group of 200"
- **Stacked bars (width 110px, full column height = 200 people):**
  - "coffee drinkers": 80 non-smokers (aqua `#199e70`, top) + 120 smokers (magenta `#d55181`, bottom); bold magenta sublabel "60% smoke" below
  - "non-drinkers": 160 non-smokers (aqua, top) + 40 smokers (magenta, bottom); bold magenta sublabel "20% smoke" below
- **In-bar labels (bold 13px white, centered):** "80 non-smokers" / "120 smokers" and "160 non-smokers" / "40 smokers"
- **Layout:** padding left 60, top 50, bottom 80; group labels 13px below bars
- **Caption (bold 13px magenta, bottom center):** "the groups differ in who joined them"

## Why Data Scientists Check for This Everywhere

**Tags:** `where it's used` (blue), `common mistake` (orange)

- **Observational data** — people choose their own group, so groups rarely start alike
- **The test** — a confounder causes the exposure AND the outcome, both arrows
- **Everyday cases** — ice cream and drowning (summer), app version and spend (heavy users)
- **In regression** — adding the confounder as a variable does the splitting automatically
- **Randomization** — A/B tests kill confounders by making groups alike via coin flip

*Example:* Users on the new app version spend more — but early adopters were heavy spenders already.

**Key point:** Whenever two groups self-select, list what else differs before reading the gap as an effect.

### Visualization (canvas `c3`, 720×300)

Confounder triangle diagram (causal DAG).

- **Title (bold 15px, ink, top center):** "The confounder sits above and causes both sides"
- **Nodes (150×38px boxes, `#f8f9fa` fill, 2px colored border, bold 13px colored label):**
  - Top center (x=w/2, y=85): "SMOKING", magenta `#d55181`
  - Bottom left (x=190, y=225): "coffee habit", blue `#2a78d6`
  - Bottom right (x=w−190, y=225): "heart disease", violet `#4a3aa7`
- **Arrows:** two solid magenta arrows (width 2.5, filled triangle heads) from SMOKING down to each bottom box, labeled in bold 12px magenta: "causes the habit" (left) and "causes the disease" (right)
- **Dashed link:** gray (`#6b7280`, dash 6/4, width 2) horizontal line between coffee habit and heart disease, with bold 15px red `#e74c3c` label above it centered: "? — the link is borrowed, not real"
- **Caption (12px text `#2c3e50`, bottom center):** "both arrows must point away from the confounder — that is the test for one"

## Not Every Third Variable Should Be Controlled

**Tags:** `common mistake` (orange), `rule of thumb` (blue)

- **The rule** — a real confounder causes both sides: smoking → coffee, smoking → disease
- **Downstream trap** — controlling for something the exposure causes erases real effects
- **Example** — judging a diet while controlling for weight loss hides how the diet works
- **No fishing** — don't adjust for every column; pick variables by the cause test
- **Residual doubt** — controlling only fixes confounders you measured; others remain

*Example:* Controlling for "doctor visits" when studying smoking hides harm — smoking causes the visits.

**Key point:** Control variables that cause both exposure and outcome; never ones the exposure itself causes.

### Visualization (canvas `c4`, 720×300)

Two-panel diagram (confounder vs mediator) split by a vertical dashed divider (`#bdc3c7`, dash 4/3, at x=w/2).

- **Title (bold 15px, ink, top center):** "Control a common cause — never a step on the path"
- **Left panel — confounder pattern (triangle of 108×32px nodes, `#f8f9fa` fill, 2px colored borders, bold 12px labels):**
  - "smoking" (magenta `#d55181`, top at x=w/4, y=80), "coffee" (blue `#2a78d6`, bottom left at x=105, y=190), "disease" (violet `#4a3aa7`, bottom right at x=w/2−100, y=190)
  - Two magenta arrows (width 2, triangle heads) from smoking down to coffee and to disease
  - Verdict (bold 13px green `#008300`): "✓ common cause: control it"
  - Sub-caption (muted 12px): "splitting by smoking removes the illusion"
- **Right panel — mediator pattern (three narrower nodes — 72/84/76px wide — in a horizontal chain at y=135, centered on x=3w/4 with ±140px offsets):**
  - "diet" (blue) → "weight loss" (yellow `#c98500`) → "health" (violet); blue arrow diet→weight loss, yellow arrow weight loss→health
  - Verdict (bold 13px red `#e74c3c`): "✗ step on the path: do NOT control"
  - Sub-caption (muted 12px): "fixing weight loss hides how the diet works"

## Regeneration instructions

- **Template/layout:** tutorials topic-page skeleton. `<h1>` (no index number) with 2px bottom border `#2980b9`, `.subtitle` paragraph, then four `.card-section` blocks each with an `<h2>` (1.3rem, `#1a5276`, 2px bottom border `#2980b9`) and a `table.layout` (one `<tr>`: `.text-col` 50% / `.viz-col` 50%). Section 2 uses `table.layout.layout3` with `.text-col3` 38% and two `.viz-col3` cells at 31% each holding canvases `c2a`/`c2b`.
- **Left column structure:** `.tags` row of pill spans first (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem, 600 weight, 2px 10px padding, radius 10px), then a `<ul>` of one-line bullets each opening with `<b>` (bold terms colored `#1a5276`), an italic `.example` paragraph (`#555`, 0.9rem), and a `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) beginning with `<strong>Key point:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, 1px solid `#e0e0e0` border, 4px radius; ul 0.92rem.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates, and fills a white background.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
