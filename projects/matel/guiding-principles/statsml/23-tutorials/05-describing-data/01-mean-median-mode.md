# Mean, Median, Mode

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Mean, Median, Mode

**Subtitle:** Three different answers to "what's typical?" — and one boss's paycheck can make them disagree wildly

## One Office, Three "Averages"

**Tags:** `core idea` (blue), `running example` (green)

- **Ten paychecks** — nine people earn $42k to $59k; the CEO takes home $1.05M
- **Mean** — add all ten salaries, divide by 10: **$150k**
- **Median** — line everyone up by salary, take the middle: **$50k**
- **Mode** — the salary that shows up most often: **$50k** (two people earn it)
- **One person** pushed the mean to triple what anyone but the CEO earns

*Example (italic):* A recruiter advertising "average salary $150k" is telling the truth — and misleading 9 of the 10 people who will ever work there.

**Key point:** "Average" is not one number. Mean, median, and mode answer different questions, and they split apart the moment the data is lopsided.

### Visualization (canvas `c1`, 720×300)

Bar chart of the ten salaries with dashed mean and median reference lines.

- **Title (bold 15px `#1a5276`, top center):** "Ten Salaries: Nine Neighbors and One Skyscraper".
- **Data ($k):** `[42, 45, 48, 49, 50, 50, 52, 55, 59, 1050]`; y scaled to max 1100.
- **Axes:** padding top 48, bottom 44, left 60, right 20; L-shaped `#999` axes; y labels "$0k", "$500k", "$1000k" (12px `#6b7280`, right-aligned) with `#e5e9ef` gridlines at 500 and 1000.
- **Bars:** 48px wide, evenly gapped; first nine in blue `#2a78d6`, the CEO bar in orange `#d95926`. X labels below bars (12px `#2c3e50`): "E1"…"E9", "CEO". Value label above the CEO bar (bold 12px orange): "$1.05M".
- **Reference lines (dashed 7/4, 2px, full plot width):** magenta `#d55181` at $150k labeled "mean $150k" (bold 12px magenta); green `#008300` at $50k labeled "median $50k" (bold 12px green).
- **Annotation (bold 13px orange, centered near top):** "one salary drags the mean to 3x what anyone else earns".

## The Whole Calculation by Hand

**Tags:** `worked example` (green), `core idea` (blue)

- **The salaries ($k)** — 42, 45, 48, 49, 50, 50, 52, 55, 59, and 1,050
- **Mean** — the sum is 1,500; 1,500 ÷ 10 = 150 → $150k
- **Median** — already sorted; the middle two (5th, 6th) are both 50 → $50k
- **Mode** — 50 appears twice, every other value once → $50k
- **Sanity check** — 9 of 10 people earn far below the "average" of $150k

*Example (italic):* Drop the CEO and the mean falls to 450 ÷ 9 = $50k — landing right on top of the median.

**Key point:** When mean and median sit far apart, something extreme is hiding in the data. The gap is a free outlier detector.

### Visualization (canvas `c2`, 720×300)

Split panel: sorted salary lineup with the middle pair bracketed (left); three-bar mean/median comparison (right). Dashed vertical divider at x=392 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px `#1a5276`, top center):** "Median: Count to the Middle.  Mean: One Guest Changes Everything.".
- **Left panel — sorted lineup:** header (bold 12px `#1a5276`, centered): "the 10 salaries ($k), sorted". Ten dots equally spaced on a horizontal `#e5e9ef` line at y=150 (x from 44 to 356): values labeled "42, 45, 48, 49, 50, 50, 52, 55, 59, 1,050" alternating above/below (12px `#2c3e50`); rank labels "#1"…"#10" below (11px `#6b7280`). Dots 7px blue `#2a78d6`, except positions 5 and 6 (the middle pair) 10px green `#008300` with bold labels, and position 10 (the CEO) orange `#d95926`. A green bracket over the middle two labeled (bold 12px green): "middle two: (50 + 50) / 2 = 50". Captions below (centered): 12px `#6b7280`: "5 values on each side of the middle pair"; bold 12px orange: "the CEO is just "one more value on the right"".
- **Right panel — three bars:** baseline `#999` at y=240, bars 70px wide, y scaled to max 170 over 150px height: "mean, all 10" = $150k magenta `#d55181`; "mean, no CEO" = $50k aqua `#199e70`; "median" = $50k green `#008300`. Value labels above bars (bold 12px `#2c3e50`): "$150k", "$50k", "$50k"; category labels below (12px). Header annotation (bold 13px magenta, centered): "remove one person: mean drops 150 to 50". Footer (12px `#6b7280`): "1,500 / 10 = 150     450 / 9 = 50".

## Which Number Should You Report?

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Skewed money data** — incomes, house prices, order values: report the median
- **Symmetric data** — heights, test scores: mean and median agree, use either
- **Mode** — best for categories: most common shirt size, most common error code
- **Totals** — use the mean when the sum matters: payroll budget = mean × headcount
- **Check both** — a big mean–median gap is the first hint of skew or outliers

*Example (italic):* Payroll needs the mean ($150k × 10 = the real $1.5M cost); a job seeker needs the median.

**Rule of thumb:** "What does a typical one look like?" → median. "What does the total work out to?" → mean. "Which one is most common?" → mode.

### Visualization (canvas `c3`, 720×300)

Right-skewed density curve with three dashed vertical markers splitting mode, median, and mean apart.

- **Title (bold 15px `#1a5276`, top center):** "Skewed Data Splits the Three Averages Apart".
- **Curve:** right-skewed shape f(t) = t^1.6 · e^(−2.6t) over t ∈ [0, 4], normalized to its peak; stroked blue `#2a78d6` 3px, filled underneath with `rgba(42,120,214,0.12)`. Padding top 50, bottom 52, left 55, right 30; L-shaped `#999` axes.
- **Markers (dashed 6/4, 2px vertical lines, each with a bold 13px label at staggered heights):** mode at t=0.615, aqua `#199e70`; median at t=0.85, green `#008300`; mean at t=1.0, magenta `#d55181`.
- **Annotation (bold 13px orange `#d95926`, left-aligned mid-plot):** "the long tail pulls the mean right — the median barely follows".
- **X-axis caption (12px `#6b7280`, bottom center):** "salary  (illustrative right-skewed shape — like the office: many small values, few huge ones)".

## The Confusion: "Average" Hides the Choice

**Tags:** `common mistake` (red), `skew` (orange)

- **"Average" usually means mean** — but reports rarely say which one they used
- **Outlier sensitivity** — the mean moves with every dollar; the median only cares about order
- **Watch it live** — grow the CEO's pay from $60k to $1.05M: mean triples, median never moves
- **Not a lie detector** — "mean salary $150k" is arithmetically true and still misleading

*Example (italic):* "Average net worth in this bar just jumped by millions" — a billionaire walked in; nobody else got richer.

**Common mistake:** Accepting "the average is X" without asking "mean or median?" — with skewed data, the choice of average IS the message.

### Visualization (canvas `c4`, 720×300)

Line chart: mean rises as only the CEO's salary grows; median stays flat.

- **Title (bold 15px `#1a5276`, top center):** "Raise Only the CEO: Mean Triples, Median Never Moves".
- **Data:** CEO salary steps `[60, 200, 400, 600, 800, 1050]` ($k, x labels "$60k", "$200k", "$400k", "$600k", "$800k", "$1.05M"); mean of all 10 = `[51, 65, 85, 105, 125, 150]` (computed as (450 + ceo) / 10); median flat at 50.
- **Axes:** padding top 52, bottom 52, left 60, right 165; y scaled to max 170 with labels "$0k", "$50k", "$100k", "$150k" (12px `#6b7280`) and `#e5e9ef` gridlines; L-shaped `#999` axes.
- **Median series:** flat green `#008300` line, 3px, at $50k.
- **Mean series:** magenta `#d55181` line, 3px, with 4px-radius magenta dots; endpoint value labels (bold 12px `#2c3e50`): "51" at the first point, "150" at the last.
- **Annotation (bold 13px green, left-aligned just below the median line):** "median stays $50k the whole time".
- **Legend (top right, 12px, colored swatches):** magenta "mean of all 10"; green "median of all 10".
- **X-axis caption (12px `#6b7280`, bottom center):** "CEO's salary (the other 9 salaries never change)".

## Regeneration instructions

- **Template/layout:** tutorials topic-page skeleton. `<h1>` (no index number) + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%), both 12px padding, top-aligned.
- **Left column structure:** `.tags` row of colored pill spans (`.tag` — 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem). Note the callout's `<strong>` prefix varies by section on this page: "Key point:" (sections 1–2), "Rule of thumb:" (section 3), "Common mistake:" (section 4).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions (this page has none).
