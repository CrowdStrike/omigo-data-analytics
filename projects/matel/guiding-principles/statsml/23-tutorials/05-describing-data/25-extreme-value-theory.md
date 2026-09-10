# Extreme Value Theory

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Extreme Value Theory

**Subtitle:** Extreme value theory is the statistics of the worst case — instead of asking "what is a typical year?", it models the biggest flood, crash, or spike, and how often the record will be broken

## The Levee Only Cares About the Worst Day

**Tags:** `core idea` (blue), `annual maxima` (green), `tail risk` (orange)

- **The town** — a river town keeps 40 years of records: the highest water level seen each year
- **Block maxima** — EVT keeps one number per year, the annual maximum, and throws the rest away
- **Typical vs worst** — the average annual peak is 3.5 m, but the levee only cares about the worst
- **The record** — 2020 hit 6.2 m, overtopping the 5.5 m levee; the average never saw it coming
- **The definition** — extreme value theory models maxima and minima directly: how bad, how often

*Example (italic):* In 2020 the river reached 6.2 m — 0.7 m over the levee — while the 40-year average annual peak was just 3.5 m.

**Key point:** Averages describe the middle of the data; EVT models the tail directly, because floods, crashes, and outages are decided by the single worst value, not the typical one.

### Visualization (canvas `c1`, 720×300)

Bar chart of 40 annual maximum river levels with a dashed levee line and a dashed average line; the one bar that tops the levee is highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Annual Maximum River Level, 1986–2025 (illustrative)".
- **Data (40 values, meters, years 1986–2025):** `[3.1, 2.8, 3.6, 2.5, 4.0, 3.3, 2.9, 3.8, 3.2, 2.6, 4.4, 3.0, 3.5, 2.7, 3.9, 3.4, 5.1, 3.1, 2.8, 3.7, 3.3, 4.2, 2.9, 3.6, 3.0, 4.7, 3.2, 2.7, 3.8, 3.5, 2.6, 4.1, 3.4, 2.9, 6.2, 3.3, 3.7, 3.0, 4.5, 3.1]`.
- **Layout:** axis origin x=55, plot width 640, baseline y=245, chart height 190; y scale 0–7 m with gridlines (`#e5e9ef`) and 12px `#444` labels at 0, 2, 4, 6; x labels 12px `#444` every 5th year (1986, 1991, ... 2021).
- **Bars:** fill `rgba(42,120,214,0.45)` (blue), ~11px wide; the 2020 bar (value 6.2, index 34) filled solid magenta `#d55181`.
- **Levee line:** dashed orange `#d95926` (dash 5/4) horizontal at 5.5 m, bold 12px orange label "levee 5.5 m" at the left end.
- **Average line:** dashed green `#008300` (dash 5/4) horizontal at 3.5 m, bold 12px green label "average peak 3.5 m".
- **Annotation (bold 13px magenta, near the tall bar):** "2020: 6.2 m — over the levee".
- **Caption (12px `#444`, bottom right):** "one value per year: the annual maximum".

## Ranking Floods to Read the 100-Year Level

**Tags:** `worked example` (blue), `return period` (green)

- **Sort and rank** — sort the 40 annual peaks from biggest down; the 6.2 m record gets rank 1
- **Return period** — rank r maps to (40+1)/r years: 6.2 m ≈ once in 41 years, 5.1 m ≈ once in 20
- **Plot it** — level vs return period on a log axis lines the ranked points up nearly straight
- **Extrapolate** — the fitted (Gumbel) line reads 6.9 m at the 100-year mark — beyond all data
- **Levee check** — 5.5 m sits at ≈ 18 years on the line: the levee is an 18-year levee, not 100

*Example (italic):* The 100-year flood level of 6.9 m comes from extending the fitted line past 6.2 m — the largest level ever observed.

**Key point:** EVT estimates events rarer than anything in your data: rank the extremes, fit a line on a return-period scale, and read off levels the record has never reached.

### Visualization (canvas `c2`, 720×300)

Return-level plot: ranked annual maxima as dots on a log10 return-period axis, a Gumbel fit line extended past the data to the 100-year mark, and the levee height read against it.

- **Title (bold 15px, `#1a5276`, top center):** "Return-Level Plot: Level vs Return Period (log scale)".
- **Observed points (return period T in years, level in m):** `[[41, 6.2], [20.5, 5.1], [13.7, 4.7], [10.3, 4.5], [8.2, 4.4], [6.8, 4.2], [5.9, 4.1], [5.1, 4.0], [4.6, 3.9], [4.1, 3.8]]` — top 10 ranks, T = 41/rank; blue `#2a78d6` 5px dots.
- **Fit line (Gumbel, level = 3.2 + 0.8·y(T)):** draw through hardcoded anchor points `[[2, 3.5], [5, 4.4], [10, 5.0], [20, 5.6], [50, 6.3], [100, 6.9]]`; solid green `#008300` 3px up to T=41, dashed green (dash 6/4) from T=41 to T=100.
- **Axes:** origin x=65, plot width 600, baseline y=245, chart height 190; x is log10(T) from T=2 to T=120 with ticks and 12px `#444` labels at 2, 5, 10, 20, 50, 100; y from 3 to 7.5 m, 12px labels at 3, 4, 5, 6, 7.
- **100-year marker:** vertical dashed `#bdc3c7` line at T=100 up to the fit; solid green 6px dot at (100, 6.9); bold 13px green annotation "100-year flood ≈ 6.9 m".
- **Levee read-off:** horizontal dashed orange `#d95926` line at 5.5 m meeting the fit near T=18; bold 12px orange annotation "levee 5.5 m ≈ 18-year flood".
- **Caption (12px `#444`, bottom right):** "dots: ranked observed peaks, T = 41/rank; dashed = extrapolation".

## Why the Bell Curve Gets the Tail Wrong

**Tags:** `where it's used` (blue), `heavy tails` (orange), `failure mode` (red)

- **Beyond floods** — the same math prices insurance, sizes market-crash risk, and plans peak load
- **Normal trap** — a normal fit to the 40 peaks (mean 3.5 m, sd 0.75 m) puts 6.2 m at z = 3.6
- **The verdict** — that z-score calls 6.2 m a 1-in-6,300-year event; it happened inside 40 years
- **EVT verdict** — the Gumbel line from the ranked peaks says 6.2 m is ≈ a 1-in-43-year event
- **Own family** — maxima converge to the GEV family (Gumbel, Fréchet, Weibull), not the bell curve

*Example (italic):* The normal fit and the Gumbel fit disagree about the 2020 flood by a factor of almost 150 — and the flood already happened.

**Key point:** The central limit theorem covers sums and averages; maxima obey their own limit law (GEV). Fitting a normal to extremes can understate tail risk by a factor of 100 or more.

### Visualization (canvas `c3`, 720×300)

Histogram of the 40 annual maxima with a normal curve and a Gumbel curve overlaid; the two curves visibly disagree in the right tail where the 6.2 m record sits.

- **Title (bold 15px, `#1a5276`, top center):** "40 Annual Peaks: Normal Fit vs Gumbel Fit".
- **Histogram data:** bin edges 2.5 to 6.5 m in 0.5 m steps; counts `[10, 13, 9, 4, 2, 1, 0, 1]`; bars fill `rgba(42,120,214,0.45)`, bin-edge labels 11px `#444` ("2.5" ... "6.5").
- **Layout:** axis origin x=60, plot width 610, baseline y=245, chart height 185; y scale 0–14 counts.
- **Normal curve:** mean 3.5, sd 0.75, scaled to peak ≈ 13 counts; solid magenta `#d55181` 2.5px line — it dives to ~0 before 6 m.
- **Gumbel curve:** location 3.2, scale 0.8, same peak scaling; solid green `#008300` 2.5px line — visibly fatter right tail passing above the 6.0–6.5 bar.
- **Record marker:** vertical dashed `#bdc3c7` line at 6.2 m with the lone count-1 bar beneath it.
- **Annotations:** bold 12px magenta, two lines near the right tail: "normal: 6.2 m =" / "1-in-6,300 yrs"; bold 12px green below it: "Gumbel: ≈ 1-in-43 yrs".
- **Caption (12px `#444`, bottom right):** "same 40 peaks; only the tails disagree — and the tail is the question".

## "100-Year Flood" Is a Rate, Not a Schedule

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The label** — a "100-year flood" has a 1% chance every single year; the name is not a schedule
- **No memory** — a flood last year does not lower this year's odds; each year is a fresh 1% draw
- **Homeowner math** — over a 30-year mortgage the chance of at least one is 1 − 0.99³⁰ ≈ 26%
- **Coin flip** — by year 69 the odds of having seen one pass 50%; by year 100 they reach 63%
- **Wrong headline** — a second big flood soon after the first is unlucky, not proof models broke

*Example (italic):* A buyer skipped flood insurance because the last "100-year flood" was in 2020 — but 2026 carries the same 1% as every other year.

**Common mistake:** Reading "100-year" as a promise of a quiet century. It is an annual 1% rate: over any 30-year stretch the odds of at least one such flood are about 1 in 4.

### Visualization (canvas `c4`, 720×300)

Rising curve of the probability of seeing at least one 100-year flood as the horizon grows from 0 to 100 years, with marked milestones at 10, 30, 50, 69, and 100 years.

- **Title (bold 15px, `#1a5276`, top center):** "Chance of At Least One 100-Year Flood vs Years Watched".
- **Curve:** p(n) = 1 − 0.99ⁿ for n = 0..100 (deterministic formula, no randomness); solid blue `#2a78d6` 3px line.
- **Milestone points (n, p):** `[[10, 0.096], [30, 0.26], [50, 0.395], [69, 0.50], [100, 0.634]]`; 5px dots — blue at 10, 50, 100; orange `#d95926` at 30; magenta `#d55181` at 69.
- **Layout:** axis origin x=65, plot width 600, baseline y=245, chart height 190; x 0–100 years, ticks every 20 with 12px `#444` labels; y 0–100% with gridlines (`#e5e9ef`) at 25/50/75 and 12px labels.
- **50% guide:** horizontal dashed `#bdc3c7` line at 50%; bold 13px magenta annotation at the year-69 dot: "coin flip by year 69".
- **Mortgage annotation:** bold 13px orange at the year-30 dot: "30-year mortgage: 26%".
- **End label:** bold 12px blue at the year-100 dot: "63% by year 100".
- **Caption (12px `#444`, bottom right):** "1 − 0.99ⁿ: a 1% annual rate compounds into near-certainty".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
