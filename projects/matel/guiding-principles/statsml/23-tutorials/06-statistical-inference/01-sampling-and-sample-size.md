# Sampling & Sample Size

**Page type:** detail page (tutorial page: h2 card-sections, each a two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** Sampling &amp; Sample Size

**Subtitle:** Why a small taste can describe the whole pot — and how big the taste needs to be

## One Spoonful Describes the Whole Pot

**Tags:** `core idea` (blue), `running example` (green)

- **The pot** — a cook stirs a huge pot of soup: thousands of spoonfuls' worth
- **The taste** — she tastes one spoonful and confidently salts the whole pot
- **Population** — everything you care about: the full pot, every spoonful in it
- **Sample** — the part you actually measure: the spoonfuls you taste
- **Stir first** — a well-mixed pot makes any spoonful look like the rest

*Example:* Nobody drinks the pot to judge the soup — one stirred spoonful stands in for all of it.

**Key point:** a sample works because it is a miniature of the population — the stirring, not the spoon size, is what makes it representative.

### Visualization (canvas `c1`, 720×300)

Dot-grid diagram: the pot as a population of dots with a few tasted spoonfuls highlighted.

- **Title (bold 16px, `#1a5276`, top center):** "The Pot (population) and the Taste (sample)"
- **Pot:** 14×8 grid of dots starting at (70,60), spacing 30px horizontal / 24px vertical, enclosed by a 2px `#1a5276` rectangle outline (24px/20px margin around the grid).
- **Dots:** unsampled dots radius 4.5 in translucent blue `rgba(26,82,118,0.35)`; six sampled dots radius 8 in orange `#d95926` at grid positions (col_row): 1_2, 4_6, 6_1, 8_4, 11_6, 12_2.
- **Caption (12px muted gray `#6b7280`, bottom left):** "every dot = one spoonful in the pot"
- **Annotations (bold 13px, left-aligned at x=530):** orange three-line "6 tasted spoonfuls" / "stand in for the" / "whole stirred pot" with a 2px orange pointer line to a sampled dot; green `#008300` three-line "stirred well =" / "any spoonful looks" / "like the rest".

## Tasting 4, 25, Then 100 Spoonfuls

**Tags:** `worked example` (green), `by hand` (blue)

- **Setup** — the pot truly averages 6.0 g/L of salt; single spoonfuls wobble by about 1.5
- **The rule** — the average of n spoonfuls typically misses by 1.5 ÷ √n
- **n = 4** — typical miss 1.5 ÷ 2 = 0.75 g/L: a rough guess
- **n = 25** — typical miss 1.5 ÷ 5 = 0.30 g/L: usable
- **n = 100** — typical miss 1.5 ÷ 10 = 0.15 g/L: quite sharp

*Example:* After 25 spoonfuls the running average sits within about 0.3 of the true 6.0 (illustrative).

**Key point:** the sample average homes in on the truth as √n grows — you can compute the typical miss with one division.

### Visualization (canvas `c2`, 720×300)

Line chart: running average of spoonfuls converging to 6.0 inside a shrinking ±1.5/√n funnel.

- **Title (bold 15px, `#1a5276`, top center):** "Running Average of the Taste vs Spoonfuls Tasted (illustrative)"
- **Data (40 running-average values):** `[7.2, 6.6, 5.9, 6.3, 6.5, 6.2, 5.95, 6.1, 6.25, 6.15, 6.0, 5.9, 5.95, 6.05, 6.12, 6.18, 6.1, 6.02, 5.97, 6.0, 6.05, 6.08, 6.03, 5.98, 6.0, 6.04, 6.06, 6.02, 5.99, 6.01, 6.03, 6.05, 6.02, 6.0, 5.98, 6.0, 6.02, 6.03, 6.01, 6.0]`
- **Axes:** padding top 52, bottom 48, left 60, right 30; y range 4.5–7.5 with tick labels 5.0, 6.0, 7.0; x ticks at spoonfuls 1, 10, 20, 30, 40; x-axis title "spoonfuls tasted" in muted gray. L-shaped axis in `#999`.
- **Funnel:** filled band `rgba(42,120,214,0.12)` between 6.0 ± 1.5/√k for k = 1..40, clamped to the plot area.
- **True-value line:** dashed green `#008300` (dash 6/4, width 2) at y=6.0, with bold 12px green label "true pot average 6.0" above it at the left.
- **Series:** running-average line blue `#2a78d6`, width 3.
- **Annotation (bold 13px blue, near n=25):** "typical miss at n = 25: about 0.30"

## Quadruple the Tasting, Halve the Error

**Tags:** `rule of thumb` (blue), `where it's used` (orange)

- **Diminishing returns** — going 4 → 16 spoonfuls halves the error; so does 100 → 400
- **The √n law** — precision is bought with squared effort, not linear effort
- **Surveys** — pollsters stop near 1,000 people because the next 1,000 buys little
- **A/B tests** — halving the uncertainty on a metric needs 4x the traffic
- **Budgeting** — decide the error you can live with first, then solve for n

*Example:* Cutting a poll's error from 3% to 1.5% means paying for four times as many phone calls.

**Key point:** without the √n law you either over-collect (wasted money) or under-collect (a mushy answer you can't act on).

### Visualization (canvas `c3`, 720×300)

Line chart with dots: typical error vs n, showing diminishing returns.

- **Title (bold 15px, `#1a5276`, top center):** "Typical Error of the Average = 1.5 ÷ √n"
- **Data:** n values `[4, 9, 16, 25, 36, 64, 100, 144]` (equally spaced on the x-axis) with errors `[0.75, 0.5, 0.375, 0.30, 0.25, 0.1875, 0.15, 0.125]`; y max 0.85.
- **Axes:** padding top 52, bottom 52, left 65, right 30; L-shaped axis in `#999`; x tick labels are the n values; x-axis title "spoonfuls tasted (n)"; rotated y-axis title "typical error (g/L)" — both muted gray 12px.
- **Series:** violet `#4a3aa7` line width 3 with 4px-radius violet dots at each point; 12px value labels "0.75" (near n=4), "0.375" (n=16), "0.15" (n=100).
- **Halving guide:** dashed orange `#d95926` (dash 5/4, width 2) step path from (n=4, 0.75) horizontally to n=16 then down to 0.375.
- **Annotation (bold 13px orange, left-aligned):** "4 → 16 spoonfuls: error only halves (0.75 → 0.375)"

## The Pot Size Barely Matters

**Tags:** `common mistake` (red), `counterintuitive` (blue)

- **The instinct** — "a bigger pot surely needs a bigger taste" — it doesn't
- **The math** — for a 3-point margin, a town of 10,000 needs ~965 people
- **Scale up** — a country of 100 million needs ~1,067 — barely more
- **Why** — error depends on how many you taste, not on what fraction of the pot
- **The exception** — only when the sample is a big slice of the pot (>10%) does size help

*Example:* A spoonful judges a cup, a bowl, or a swimming pool of soup equally well — if it's stirred.

**Common mistake:** dismissing a 1,000-person poll of a huge country as "too small a fraction" — the fraction is irrelevant; the count is what counts.

### Visualization (canvas `c4`, 720×300)

Bar chart: sample size needed for a ±3% answer vs population size — nearly flat.

- **Title (bold 15px, `#1a5276`, top center):** "People Needed for a ±3% Answer vs Population Size"
- **Data:** populations `['1,000', '10,000', '100,000', '1 million', '100 million']` needing `[517, 965, 1056, 1066, 1067]` people; y max 1300.
- **Axes:** padding top 56, bottom 56, left 70, right 30; horizontal baseline in `#999`; bars 76px wide, evenly spaced; x-axis title "population size (the pot)" in muted gray.
- **Bars:** first bar lighter blue `rgba(42,120,214,0.45)`, the rest `rgba(42,120,214,0.8)`; bold 12px count labels above bars in `#2c3e50`; population labels below.
- **Annotation (bold 13px green `#008300`, centered near top):** "10,000x bigger pot → only ~100 more spoonfuls needed"

## Regeneration instructions

- **Template:** tutorial detail page (tutorials style, see `tutorials/CLAUDE.md`). h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `.text-col` (50%) and `.viz-col` (50%) holding one 720×300 canvas.
- **Text cell structure:** `.tags` row of colored pills, `<ul>` of one-line bullets each opening with `<b>bold term</b>` (colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout. On this page the `.key-point` left border is `3px solid #1a5276` (blue, not red); the last section's callout is prefixed "Common mistake:" instead of "Key point:".
- **Tag pill styles:** 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; bullets 0.92rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius. HTML entities used in text: `&divide;` (÷), `&radic;` (√), `&rarr;` (→), `&times;` (×), `&gt;` (>).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Project palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic 720×300; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) via a shared `setup(id)` helper (`ctx.scale` back to logical coordinates). All data hardcoded literal arrays — no `Math.random()`; invented series labeled "illustrative".
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
