# Gamma & Chi-Squared

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Gamma & Chi-Squared

**Subtitle:** Gamma is the wait for the k-th random arrival — stacked exponential gaps — and chi-squared is its famous special case that test statistics call home

## Waiting for the Third Customer

**Tags:** `core idea` (blue), `waiting time` (green), `poisson link` (orange)

- **The truck** — customers walk up to a food truck at random, on average one every 2 minutes
- **One gap** — the wait for the very next customer follows an exponential curve (Gamma with k = 1)
- **Stack gaps** — the wait for the 3rd customer is three gaps added: 1.1 + 2.8 + 1.7 = 5.6 minutes
- **The name** — "time until the k-th event" follows a Gamma distribution with shape k and rate λ
- **Rate λ** — one every 2 minutes means λ = 0.5 per minute, so the average wait is k/λ minutes

*Example (italic):* The owner opened at 9:00; customers arrived at 9:01.1, 9:03.9, and 9:05.6 — the third customer took 5.6 minutes.

**Key point:** A Gamma variable is nothing exotic — it is k exponential waits summed. Today's 5.6 minutes is one draw from Gamma(k = 3, λ = 0.5).

### Visualization (canvas `c1`, 720×300)

Timeline of one morning at the truck: a horizontal minutes axis with arrival dots, small gap brackets below, and one big bracket above summing the first three gaps.

- **Title (bold 15px, `#1a5276`, top center):** "One Morning: the Wait for the 3rd Customer = Three Gaps Summed".
- **Data:** arrival times (minutes after opening) `[1.1, 3.9, 5.6, 9.2, 10.4]`; gaps between events `[1.1, 2.8, 1.7, 3.6, 1.2]`.
- **Axis:** horizontal line 2px `#999` at y=150 from x=70, width 580, scale 0–12 minutes (48.33 px/min); ticks at 0, 2, 4, 6, 8, 10, 12 with 12px `#444` labels "0 min" ... "12 min".
- **Arrival dots:** 7px circles, first three blue `#2a78d6`, last two mute `#6b7280`; 12px labels "1st", "2nd", "3rd", "4th", "5th" just below each dot.
- **Gap brackets (below axis, y=185):** thin aqua `#199e70` 2px brackets under each of the first three gaps with bold 12px aqua labels "1.1", "2.8", "1.7"; heading 12px `#444` at left "gaps (each ~ exponential, mean 2 min)".
- **Sum bracket (above axis, y=95):** blue `#2a78d6` 3px bracket from x of 0 to x of 5.6; bold 13px blue label above it "wait for 3rd = 1.1 + 2.8 + 1.7 = 5.6 min".
- **Caption (12px `#444`, bottom center):** "one every 2 min on average → λ = 0.5/min; the sum of 3 gaps is one draw from Gamma(k=3, λ=0.5)".

## The Whole Family of Waits

**Tags:** `worked example` (blue), `shape k` (green), `mean & mode` (orange)

- **Mean wait** — the 3rd customer takes k/λ = 3/0.5 = 6 minutes on average
- **Most likely** — the k = 3 curve peaks at (k−1)/λ = 4 minutes; the mode sits left of the mean
- **Spread** — sd = √k/λ = √3/0.5 ≈ 3.5 minutes, so today's 5.6 was a perfectly ordinary day
- **k = 1** — waiting for just the next customer is the exponential: highest at zero, no peak
- **Bigger k** — the 5th customer's curve peaks at 8 minutes and looks noticeably more symmetric

*Example (italic):* Waiting for the 1st, 2nd, 3rd, and 5th customer gives four curves from the same λ = 0.5 — the peak walks right and the skew fades as k grows.

**Key point:** Gamma's shape is controlled by k: k = 1 is a cliff at zero, and as k grows the sum of many waits drifts toward a bell — a preview of the central limit theorem.

### Visualization (canvas `c2`, 720×300)

Overlay line chart of four Gamma densities (λ = 0.5; k = 1, 2, 3, 5) over 0–20 minutes, with the k = 3 curve highlighted and its mode/mean marked.

- **Title (bold 15px, `#1a5276`, top center):** "Wait for the k-th Customer: Gamma Curves at λ = 0.5".
- **Axis:** origin x=55, width 610, baseline y=245, chart height 185; x scale 0–20 minutes with ticks every 4 (12px `#444` labels "0" ... "20 min"); y scale 0–0.55 density (unlabeled axis line only).
- **Data (density at x = 0, 1, 2, ..., 20):**
  - k=1: `[0.5000, 0.3033, 0.1839, 0.1116, 0.0677, 0.0410, 0.0249, 0.0151, 0.0092, 0.0056, 0.0034, 0.0020, 0.0012, 0.0008, 0.0005, 0.0003, 0.0002, 0.0001, 0.0001, 0.0000, 0.0000]`
  - k=2: `[0, 0.1516, 0.1839, 0.1673, 0.1353, 0.1026, 0.0747, 0.0529, 0.0366, 0.0250, 0.0168, 0.0112, 0.0074, 0.0049, 0.0032, 0.0021, 0.0013, 0.0009, 0.0006, 0.0004, 0.0002]`
  - k=3: `[0, 0.0379, 0.0920, 0.1255, 0.1353, 0.1283, 0.1120, 0.0925, 0.0733, 0.0562, 0.0421, 0.0309, 0.0223, 0.0159, 0.0112, 0.0078, 0.0054, 0.0037, 0.0025, 0.0017, 0.0011]`
  - k=5: `[0, 0.0008, 0.0077, 0.0235, 0.0451, 0.0668, 0.0840, 0.0944, 0.0976, 0.0948, 0.0878, 0.0780, 0.0670, 0.0559, 0.0456, 0.0365, 0.0286, 0.0221, 0.0168, 0.0127, 0.0095]`
- **Lines:** k=1 yellow `#c98500` 2px; k=2 aqua `#199e70` 2px; k=3 blue `#2a78d6` 3px (the hero curve); k=5 violet `#4a3aa7` 2px; bold 12px labels in matching colors near each peak: "k=1", "k=2", "k=3", "k=5".
- **Markers for k=3:** vertical dashed `#bdc3c7` (dash 4/3) lines at x=4 (mode) and x=6 (mean), 12px `#444` labels "peak 4 min" and "mean 6 min" above the baseline.
- **Annotation (bold 13px blue `#2a78d6`, upper right):** "3rd customer: most likely 4 min, average 6 min, sd ≈ 3.5".
- **Caption (12px `#444`, bottom center):** "same rate λ = 0.5 throughout; only the target event number k changes".

## Chi-Squared: Where Test Scores Live

**Tags:** `where it's used` (blue), `test statistic` (green), `goodness of fit` (orange)

- **The question** — the truck's 6 menu items got 60 orders: are counts 8, 12, 13, 9, 6, 12 even enough?
- **The recipe** — score each item with (observed − expected)²/expected and add: 38/10 = 3.8
- **Its home** — if popularity is truly even, that score follows a chi-squared curve with df = 6 − 1 = 5
- **Why this curve** — a sum of df squared standard-normal wobbles follows exactly this distribution
- **The cutoff** — a truly even menu produces a score above 11.07 only 5% of the time
- **Verdict** — 3.8 sits well below 11.07, so this wobble looks like ordinary randomness

*Example (italic):* Deviations from 10 are −2, 2, 3, −1, −4, 2; squaring gives 4 + 4 + 9 + 1 + 16 + 4 = 38, and dividing by the expected 10 gives the score 3.8.

**Key point:** Chi-squared is the distribution a test statistic follows when nothing is going on — you compute one score from your data and check where it lands on that curve.

### Visualization (canvas `c3`, 720×300)

Dual panel: observed order counts vs the even-menu expectation (left) and the chi-squared df = 5 curve with the computed score and the 5% cutoff (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Are 6 Menu Items Equally Popular? 60 Orders, One Score".
- **Left panel (bars):** observed counts `[8, 12, 13, 9, 6, 12]` for items labeled "A"–"F" (12px `#444` below bars); axis origin x=50, width 290, baseline y=240, chart height 170, y scale 0–15; bars fill `rgba(42,120,214,0.45)` with bold 12px blue count labels above each; horizontal dashed ink `#1a5276` (dash 4/3) line at count 10 labeled bold 12px "expected: 10 each"; caption 12px `#444` "score = Σ (obs − 10)² / 10 = 3.8".
- **Right panel (curve):** chi-squared df=5 density at x = 0, 1, ..., 20: `[0, 0.0807, 0.1384, 0.1542, 0.1440, 0.1221, 0.0973, 0.0744, 0.0551, 0.0399, 0.0283, 0.0198, 0.0137, 0.0094, 0.0064, 0.0043, 0.0029, 0.0019, 0.0012, 0.0008, 0.0005]`; axis origin x=395, width 290, baseline y=240, chart height 170, x scale 0–20 (ticks every 5), y scale 0–0.17; green `#008300` 3px line; area under the curve beyond x=11.07 filled `rgba(231,76,60,0.25)` with red `#e74c3c` dashed vertical line at 11.07 and bold 12px red label "5% cutoff: 11.07"; green 6px dot on the baseline at x=3.8 with bold 13px green label "your score: 3.8"; caption 12px `#444` "chi-squared, df = 5".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## One Family in Disguise

**Tags:** `common mistake` (red), `one family` (green)

- **Same family** — chi-squared with df degrees of freedom is exactly Gamma with shape df/2, scale 2
- **Proof by picture** — chi-squared(10) equals the truck's wait-for-the-5th-customer curve exactly
- **Mean & spread** — chi-squared mean = df and variance = 2·df, so a score near df is unremarkable
- **Shape shifts** — df = 1 spikes at zero, df = 2 is the exponential, df = 10 already looks bell-ish
- **Never negative** — both live on positive numbers only; a bell is the wrong picture for small df

*Example (italic):* "Gamma(3, 0.5)" from two libraries gave one analyst mean 6 and another mean 1.5 — one read 0.5 as a rate, the other as a scale.

**Common mistake:** Quoting Gamma parameters without saying rate or scale. With rate λ the mean is k/λ = 3/0.5 = 6; with scale θ it is kθ = 3 × 0.5 = 1.5 — a 4× disagreement from one ambiguous symbol.

### Visualization (canvas `c4`, 720×300)

Overlay of four chi-squared densities (df = 1, 2, 5, 10) over 0–20 showing the shape morph from a spike at zero to a near-bell, with the df = 10 curve flagged as the k = 5 Gamma from earlier.

- **Title (bold 15px, `#1a5276`, top center):** "Chi-Squared for df = 1, 2, 5, 10: One Gamma Family".
- **Axis:** origin x=55, width 610, baseline y=245, chart height 185; x scale 0–20 with ticks every 4 (12px `#444`); y scale 0–0.50, values above 0.50 clipped at the panel top.
- **Data:**
  - df=1 at x = `[0.2, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`: `[0.8072, 0.4394, 0.2420, 0.1038, 0.0514, 0.0270, 0.0146, 0.0081, 0.0046, 0.0026, 0.0015, 0.0009]` (first point clips above the panel; draw the line entering from the top edge)
  - df=2 at x = 0, 1, ..., 20: `[0.5000, 0.3033, 0.1839, 0.1116, 0.0677, 0.0410, 0.0249, 0.0151, 0.0092, 0.0056, 0.0034, 0.0020, 0.0012, 0.0008, 0.0005, 0.0003, 0.0002, 0.0001, 0.0001, 0.0000, 0.0000]`
  - df=5 at x = 0, 1, ..., 20: `[0, 0.0807, 0.1384, 0.1542, 0.1440, 0.1221, 0.0973, 0.0744, 0.0551, 0.0399, 0.0283, 0.0198, 0.0137, 0.0094, 0.0064, 0.0043, 0.0029, 0.0019, 0.0012, 0.0008, 0.0005]`
  - df=10 at x = 0, 1, ..., 20: `[0, 0.0008, 0.0077, 0.0235, 0.0451, 0.0668, 0.0840, 0.0944, 0.0976, 0.0948, 0.0878, 0.0780, 0.0670, 0.0559, 0.0456, 0.0365, 0.0286, 0.0221, 0.0168, 0.0127, 0.0095]`
- **Lines:** df=1 magenta `#d55181` 2px; df=2 orange `#d95926` 2px; df=5 green `#008300` 2px; df=10 blue `#2a78d6` 3px; bold 12px labels in matching colors near each curve: "df=1", "df=2", "df=5", "df=10".
- **Annotations:** bold 12px magenta near the top left "df=1 shoots up near 0"; bold 13px blue near the df=10 peak (x=8) "df=10 = the truck's k=5 wait curve (mean 10, peak 8)".
- **Caption (12px `#444`, bottom center):** "every curve has mean = df; chi-squared(df) is Gamma(shape df/2, scale 2)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
