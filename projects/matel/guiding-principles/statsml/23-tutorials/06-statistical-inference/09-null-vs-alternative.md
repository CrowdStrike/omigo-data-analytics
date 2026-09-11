# Null vs Alternative

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table row, text left 50% / canvas right 50%)
**HTML title tag:** Null vs Alternative

**Subtitle:** Every test pits two stories against each other: "nothing really changed" versus "something changed" — and the boring story wins by default until the data overturns it.

## Two Stories About One New Checkout Page

Tags: `core idea` (blue), `burden of proof` (orange)

- **The scene** — an online shop redesigns its checkout page; the old page converts 10%
- **Story 1, the null** — "the redesign changed nothing; any gap you see is luck"
- **Story 2, the alternative** — "the redesign really changed how people buy"
- **Default winner** — the null holds office until the evidence throws it out
- **Burden of proof** — the alternative must earn belief; the null never has to

*Example (italic):* Same courtroom as the coin: the null is the defendant, presumed right; the data must convict it.

**Key point:** you never "prove" the alternative directly — you show the null's luck-only story can't explain the data.

### Visualization (canvas `c1`, 720×300)

Diagram: observed conversion bars in the center flanked by two story boxes (null left, alternative right).

- **Title (bold 15px, `#1a5276`, top center):** "One Observation, Two Competing Stories".
- **Center bars:** "old page" 10.0% in `#2a78d6` and "new page" 11.8% in `#199e70`, both at 75% alpha; bar width 62, gap 34, centered; baseline y=200, y scale max 14% over 120px; bold value labels "10.0%" / "11.8%" above, category labels below, gray `#999` baseline. Bold violet `#4a3aa7` caption under the bars: "who explains this gap?".
- **Left box (x=20, y=70, 200×92, fill `#f8f9fa`, stroke `#2a78d6` width 2):** bold blue "NULL (H0)"; 12px text: '"nothing really changed —' / 'the gap is luck"'; bold green `#008300` 12px: "holds office by default".
- **Right box (x = w−220, same size, stroke `#d95926`):** bold orange "ALTERNATIVE (H1)"; 12px text: '"the redesign changed' / 'buying behavior"'; bold orange 12px: "must earn belief with data".
- **Footer (muted `#6b7280`, 12px, bottom center):** 'the test never asks "which story is prettier?" — only "can the null still explain the data?"'.

## Putting the Null on Trial With 2,000 Shoppers

Tags: `worked example` (green), `small numbers` (blue)

- **The split** — 1,000 shoppers see the old page, 1,000 see the new one
- **The result** — old page: 100 buyers (10.0%); new page: 118 buyers (11.8%)
- **Null's wobble** — if nothing changed, the buyer gap wobbles around 0 by about ±14
- **Luck's reach** — gaps up to about 2 wobbles (±28 buyers) are ordinary luck
- **Verdict** — the gap of 18 sits inside ±28, so the null survives
- **What would convict** — 130 buyers (gap 30) lands outside ±28: null overturned

*Example (italic):* 18 extra buyers feels like a win, but the null's luck-only story covers gaps up to 28.

**Key point:** write both stories down before looking at data — then check whether the gap escapes the null's luck range.

### Visualization (canvas `c2`, 720×300)

Number line of the buyer gap with the null's luck band and two marked points (observed +18 inside, hypothetical +30 outside).

- **Title (bold 15px, `#1a5276`, top center):** "Buyer Gap (new minus old) If the Null Is True: 0 ± Luck".
- **Scale:** gap −45..+45 mapped to x between padding left 70 / right 40; axis line at y=170 with ticks/labels at −42, −28, −14, 0, +14, +28, +42; axis caption: "gap in buyers (per 1,000 shoppers each side)".
- **Luck band:** rectangle from −28 to +28, y 60–170, filled `rgba(42,120,214,0.14)` with 1px `#2a78d6` outline; dashed muted center line at 0. Bold blue label above: "ordinary luck: gaps up to ±28"; 12px below it: "(2 wobbles of ±14)".
- **Observed point:** radius-8 aqua `#199e70` dot at +18, y=125; bold label above: "+18 observed"; bold 12px below: "inside: null survives".
- **Hypothetical point:** radius-8 orange `#d95926` dot at +30, y=125; bold label right: "+30 would convict"; 12px below: "(130 buyers)".
- **Footer annotations:** bold violet `#4a3aa7` center: "118 vs 100 buyers looked like a win — but the null's luck-only story still covers it"; muted 12px: "wobble ±14 comes from the sample size; more shoppers shrink it".

## Why the Boring Story Gets the Benefit of the Doubt

Tags: `where it's used` (blue), `common mistake` (red)

- **Flip the burden** — trust every "looks better" result and you ship lucky duds
- **Luck is generous** — among variants that truly do nothing, about half show a positive gap
- **The chart's ten** — ten do-nothing variants: five "beat" the old page by pure luck
- **The null as filter** — it blocks changes whose only evidence is a gap luck covers
- **Real cost** — shipping duds wastes engineering time and erodes trust in experiments

*Example (italic):* A team that ships anything positive would have shipped 5 of these 10 do-nothing variants.

**Key point:** the null hypothesis is not pessimism — it is the filter that keeps luck from getting promoted to strategy.

### Visualization (canvas `c3`, 720×300)

Diverging bar chart: buyer gaps of ten do-nothing variants around a zero line.

- **Title (bold 15px, `#1a5276`, top center):** "10 Variants That Truly Do Nothing — Gaps From Luck Alone (illustrative)".
- **Data (v1–v10):** `[9, -5, 14, -11, 3, -16, 7, 12, -8, -2]`; value scale ±30 around a center zero line; bar width 55% of slot. Positive bars filled `rgba(25,158,112,0.65)` (aqua) rising above the line; negative bars `rgba(107,114,128,0.45)` (gray) hanging below. Signed value labels ("+9", "−5", ...) at bar ends; "v1".."v10" labels along the bottom. Padding: top 56, bottom 56, left 70, right 25.
- **Y labels (right-aligned, muted):** 0, +20, −20.
- **Annotations:** bold aqua top-left: '5 of 10 "beat" the old page — all luck'; bold violet `#4a3aa7` 12px below it: "ship-anything-positive would ship all 5 duds".
- **X caption (12px, center):** "buyer gap vs old page (per 1,000 shoppers, no real effect exists)".

## A Surviving Null Is Not a Proven Null

Tags: `common mistake` (red), `rule of thumb` (green)

- **Survived ≠ true** — "null not rejected" means "couldn't tell", not "pages are equal"
- **Hidden wins** — a real +1.8% lift can sit comfortably inside a small test's luck range
- **Size matters** — with 1,000 per page, luck covers ±2.8 points; with 10,000, only ±0.9
- **Same lift, new verdict** — the identical 1.8-point gap convicts the null at 10,000 shoppers
- **Say it right** — report "no detectable difference at this sample size", never "no difference"

*Example (italic):* The same redesign "did nothing" in a 2,000-shopper test and "clearly won" in a 20,000-shopper test.

**Key point:** absence of evidence is not evidence of absence — a weak test acquits almost everyone.

### Visualization (canvas `c4`, 720×300)

Two stacked number-line rows: the same +1.8-point lift against a wide luck band (small n) and a narrow luck band (large n).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Real +1.8-Point Lift, Two Sample Sizes".
- **Scale:** conversion gap −4..+4 percentage points mapped to x between padding left 70 / right 40; shared x labels at −4, −2, 0, +2, +4 (y=258) with caption "conversion gap, percentage points".
- **Rows** (each: luck band rectangle `rgba(42,120,214,0.14)` with 1px `#2a78d6` outline, 44px tall centered on the row line; light `#ccc` axis line; dashed muted zero tick; radius-8 orange `#d95926` dot at +1.8 labeled bold "+1.8"; bold `#1a5276` row label above-left; blue 12px band label "luck: ±<band> pts" below-left; bold verdict at right):
  - Row 1 (y=95), band ±2.8, label "1,000 shoppers per page", verdict in muted `#6b7280`: "lift hides inside luck: null survives".
  - Row 2 (y=195), band ±0.9, label "10,000 shoppers per page", verdict in green `#008300`: "lift escapes luck: null overturned".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, 2px `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holds `.tags` pill row, `<ul>` bullets (each starting with `<b>` term in `#1a5276`), one italic `.example` paragraph, one `.key-point` callout; right `<td class="viz-col">` (50%) holds one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c` (red on this page), padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` / `#1a5276`; green: bg `rgba(39,174,96,0.15)` / `#27ae60`; red: bg `rgba(231,76,60,0.12)` / `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Canvas:** 720×300 intrinsic attributes (setup helper reads width/height attributes), CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML any card links use `.html` extensions (this page has none).
