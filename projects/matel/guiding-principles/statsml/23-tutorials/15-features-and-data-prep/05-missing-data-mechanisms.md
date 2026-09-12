# Missing-Data Mechanisms — MCAR, MAR, MNAR

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Missing-Data Mechanisms — MCAR, MAR, MNAR

**Subtitle:** Why a value is missing decides what is safe to do about it — the same blank cell can be a harmless nuisance or quietly poison your average

## One Survey, Three Kinds of Blank

**Tags:** `core idea` (blue), `MCAR / MAR / MNAR` (green), `why it's blank` (orange)

- **The clinic** — 10 patients answer a follow-up form: "how many hours did you exercise this week?"
- **MCAR** — the scanner jams and eats two random forms; the blanks relate to nothing at all
- **MAR** — three older patients skip the online portal; missingness depends on age, which we recorded
- **MNAR** — the three least active patients skip out of embarrassment; the blank depends on the answer
- **Same blank cell** — all three stories leave an empty cell that looks identical in the table

*Example (italic):* Patient #4's exercise cell is blank — a jammed scanner, a skipped portal, or embarrassment would all leave the exact same empty cell.

**Key point:** The mechanism is a story about WHY the cell is blank, not about the cell itself — and that story decides everything you may safely do next.

### Visualization (canvas `c1`, 720×300)

Three side-by-side dot panels showing the same 10 patients (older row / younger row, positioned by exercise hours), with different patients missing under each mechanism.

- **Title (bold 15px, `#1a5276`, top center):** "10 Patients, Weekly Exercise Hours: Who Goes Missing Under Each Mechanism".
- **Data:** older (65+) patients have hours `[1, 1, 2, 2, 3]`; younger patients have hours `[4, 5, 6, 8, 8]`.
- **Panels:** three panels with x origins 25, 255, 485, each 200px wide; hour h maps to x = origin + h × 21; older row at y=130, younger row at y=195; duplicate hours (the two 1s, the two 8s) offset vertically ±7px so both dots show.
- **Dots:** observed = filled blue `#2a78d6` circles radius 6; missing = hollow circles, 2px magenta `#d55181` stroke, with a small magenta × inside.
- **Missing per panel:** MCAR panel — one older 2 and the younger 5; MAR panel — older 1, 2, 3; MNAR panel — the 1, 1, 2 (the three lowest).
- **Row labels (leftmost panel only, 11px `#6b7280`):** "older (65+)" beside y=130, "younger" beside y=195.
- **Panel captions (bold 12px, centered under each panel at y=245):** "MCAR: scanner jam — random" in green `#008300`; "MAR: older skip the portal" in yellow `#c98500`; "MNAR: low exercisers skip" in magenta `#d55181`.
- **Bottom caption (11px `#6b7280`, centered at y=285):** "same 10 patients, same question — three reasons for a blank (illustrative)".

## What Each Blank Does to the Average

**Tags:** `worked example` (blue), `bias` (red)

- **The truth** — the 10 answers are 1, 1, 2, 2, 3 and 4, 5, 6, 8, 8 hours; the true mean is 40/10 = 4.0
- **MCAR** — the jam eats a 2 and a 5; the eight left average 33/8 = 4.1, almost unchanged
- **MAR** — older patients with 1, 2, 3 skip; the seven left average 34/7 ≈ 4.9, biased upward
- **MNAR** — the three lowest (1, 1, 2) skip; the seven left average 36/7 ≈ 5.1, the worst bias
- **Bias rule** — when the missing share a trait tied to the value, the survivors' mean drifts

*Example (italic):* Same 10 patients, same question — the three mechanisms report means of 4.1, 4.9, and 5.1 against a truth of 4.0.

**Key point:** MCAR only costs you sample size; MAR and MNAR quietly move the answer — here by up to +1.1 hours per week.

### Visualization (canvas `c2`, 720×300)

Four-bar chart comparing the true mean with the observed mean under each mechanism, with a dashed truth line.

- **Title (bold 15px, `#1a5276`, top center):** "Reported Mean Exercise Hours Under Each Mechanism (truth = 4.0)".
- **Data:** bars `Truth 4.0`, `MCAR 4.1`, `MAR 4.9`, `MNAR 5.1`.
- **Axis:** origin x=70, baseline y=245, chart height 185, y scale 0–6 with gridlines at 1, 2, 3, 4, 5 in `#e5e9ef` and 12px `#6b7280` tick labels.
- **Bars:** width 90, gap 50, starting at x=100; fills — Truth ink `rgba(26,82,118,0.55)`, MCAR green `rgba(0,131,0,0.45)`, MAR yellow `rgba(201,133,0,0.5)`, MNAR magenta `rgba(213,81,129,0.5)`; bar names 12px `#444` below baseline; value labels bold 13px in each bar's solid color above the bar.
- **Truth line:** dashed ink `#1a5276` (dash 5/4) horizontal line at y for 4.0 across the plot, labeled "truth 4.0" bold 12px ink at its right end.
- **Annotation (bold 13px magenta `#d55181`, above the MNAR bar):** "low answers vanished → mean off by +1.1 h".
- **Caption (11px `#6b7280`, bottom center):** "MCAR: 33/8 · MAR: 34/7 · MNAR: 36/7 (illustrative)".

## Which Fixes Are Safe

**Tags:** `where it's used` (blue), `rule of thumb` (green), `imputation` (orange)

- **Drop rows** — complete-case analysis is unbiased only under MCAR; here it returns 4.1
- **Impute by age** — filling blanks with the age group's mean repairs MAR: 4.9 falls to 3.9
- **MNAR resists** — the same imputation still leaves 4.4, because low values hid inside age groups
- **Model choice** — standard multiple imputation assumes MAR; MNAR needs knowledge from outside the table
- **Sensitivity** — under MNAR, report a range: if the skippers did 0–2 h, the true mean is 3.6–4.2

*Example (italic):* The analyst imputes by age group and confidently reports 3.9 for the MAR clinic, but must publish the range 3.6–4.2 for the MNAR one.

**Key point:** Match the fix to the mechanism — drop under MCAR, impute with the right covariates under MAR, and bound or model the gap under MNAR.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: for each mechanism, the estimate from dropping rows vs imputing by age group, against a dashed truth line at 4.0.

- **Title (bold 15px, `#1a5276`, top center):** "Two Fixes Under Three Mechanisms: Estimated Mean vs Truth 4.0".
- **Data:** groups MCAR `[4.1, 4.1]`, MAR `[4.9, 3.9]`, MNAR `[5.1, 4.4]`; first bar of each pair = "drop missing rows", second = "impute by age group".
- **Axis:** origin x=70, baseline y=245, chart height 185, y scale 0–6, gridlines at 1–5 in `#e5e9ef`, 12px `#6b7280` tick labels.
- **Bars:** width 60, 10px gap within a pair, 60px gap between groups, first group starting at x=110; "drop" bars fill `rgba(42,120,214,0.5)` (blue), "impute" bars fill `rgba(25,158,112,0.5)` (aqua); value labels bold 12px in the bar's solid color above each bar; group names bold 12px `#444` centered below each pair.
- **Truth line:** dashed green `#008300` (dash 5/4) at y for 4.0, labeled "truth 4.0" bold 12px green at its left end.
- **Legend (top right, above y=60 clear of the MNAR bar label, 12px `#444`):** blue swatch "drop missing rows", aqua swatch "impute by age group", 14×14px squares.
- **Annotation (bold 13px orange `#d95926`, above the MNAR pair):** "MNAR stays biased after imputation".

## The Confusion: You Cannot Test Your Way Out

**Tags:** `common mistake` (red), `untestable` (orange)

- **Identical tables** — a MAR clinic and an MNAR clinic can hand you the very same seven values
- **No test** — data can reject MCAR (blanks linked to age), but MAR vs MNAR is untestable
- **Naming trap** — "missing at random" (MAR) does not mean randomly missing; that is MCAR
- **Default danger** — dropna and mean-fill silently assume MCAR, the rarest mechanism in practice
- **The judgment** — choosing MAR vs MNAR comes from knowing how the data was collected, not from the data

*Example (italic):* Both clinics report the identical seven values 2, 3, 4, 5, 6, 8, 8 — only the intake nurse knows whether age or embarrassment made the blanks.

**Common mistake:** Believing a test or a bigger model can identify the mechanism — the observed table is consistent with both MAR and MNAR, so the choice is a domain judgment you must state out loud.

### Visualization (canvas `c4`, 720×300)

One observed number line branching into two hidden-story boxes that contain identical missing values but different reasons.

- **Title (bold 15px, `#1a5276`, top center):** "The Analyst Sees One Table — Two Hidden Stories Fit It Perfectly".
- **Observed row:** heading bold 12px `#444` "the analyst sees:" at x=70, y=75; horizontal 2px `#999` number line at y=105 from x=70 to x=650, hours 0–9 mapped linearly; filled blue `#2a78d6` dots radius 6 at hours `[2, 3, 4, 5, 6, 8, 8]` (the two 8s offset ±6px vertically); 11px `#444` hour labels 0–9 below the line.
- **Branch arrows:** two 2px `#6b7280` arrows from (360, 125) to (200, 165) and from (360, 125) to (520, 165).
- **Story boxes (y=170 to y=255, width 260):** left box x=70, 2px yellow `#c98500` border, heading bold 12px yellow "Story A — MAR"; right box x=390, 2px magenta `#d55181` border, heading bold 12px magenta "Story B — MNAR"; each box shows the same three hidden values as hollow dots (radius 6, box's border color, small × inside) at hours 1, 1, 2 on a mini 0–9 line, the two 1s offset ±6px.
- **Box captions (11px `#444`, inside each box):** left "three older patients never use the portal"; right "the three least active skipped, embarrassed".
- **Takeaway (bold 13px magenta `#d55181`, centered at y=285):** "identical data, different mechanisms — the table alone cannot tell you which".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays — no `Math.random()`; invented numbers carry an "illustrative" label in captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
