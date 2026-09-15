# Retention & Cohorts

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Retention & Cohorts

**Subtitle:** Group users by when they signed up and track each group separately — one blended "% active" number can rise while every single group is quietly getting worse

## One App, Three Signup Cohorts

**Tags:** `core idea` (blue), `cohort triangle` (green), `day-N vs rolling` (orange)

- **The app** — a note-taking app groups its users by signup month: the Jan, Feb, and Mar cohorts
- **The rows** — each row is one cohort with its size: Jan 1,000 signups, Feb 1,000, Mar 4,000
- **The columns** — each column is weeks *since signup* (W1–W4), not calendar weeks
- **The triangle** — younger cohorts have fewer filled cells, so the table ends in a staircase
- **Week-N (day-N style)** — week-4 counts users active during week 4 exactly: 220 of Jan's 1,000 = 22%
- **Rolling** — rolling week-4 counts users active in week 4 *or any week after*: 300 of 1,000 = 30%

*Example (italic):* Jan's row reads 40, 30, 25, 22 — of its 1,000 signups, 400 came back in week 1 and 220 were still opening notes in week 4.

**Key point:** The triangle separates two questions a single number mixes up — read down a column to compare cohort quality, and along a row to watch one cohort age.

### Visualization (canvas `c1`, 720×300)

A cohort triangle drawn as a table on canvas: rows = signup cohorts, columns = weeks since signup, cells shaded by retention, with the empty "too young" staircase visible.

- **Title (bold 15px, `#1a5276`, top center):** "The Cohort Triangle: Monthly Signups, Tracked Week by Week".
- **Header row (bold 12px `#1a5276`, y=70):** "cohort" at x=30, "signups" at x=140, then "W1" / "W2" / "W3" / "W4" centered over cells at x = 250, 360, 470, 580.
- **Cells:** width 100, height 46, 1px `#e5e9ef` border; rows top-aligned at y = 82 (Jan), 134 (Feb), 186 (Mar). Row labels 12px `#444`: "Jan — 1,000", "Feb — 1,000", "Mar — 4,000" (label at x=30, signup count part at x=140).
- **Cell values (bold 13px `#1a5276`, centered):** Jan: 40% / 30% / 25% / 22%; Feb: 38% / 28% / 23% / 20%; Mar: 35% / 25% / — / —. Fill each numeric cell `rgba(42,120,214, pct/100)` (e.g. 0.40 for 40%); the two Mar blanks get white fill, dashed 1px `#6b7280` border, 11px `#6b7280` text "too young".
- **Caption (12px `#444`, bottom right):** "signups and retention illustrative".

## Blended Goes Up While Every Cohort Goes Down

**Tags:** `worked example` (blue), `blended vs cohort` (green), `growth spurt` (orange)

- **The setup** — end of Feb: 2,000 signups to date (Jan 1,000 + Feb 1,000); March adds 4,000 more
- **Active in Feb** — Feb's 1,000 brand-new users + 45% of Jan = 450 → 1,450 of 2,000 active = 72.5%
- **Active in Mar** — Mar's 4,000 new + 40% of Feb = 400 + 25% of Jan = 250 → 4,650 of 6,000 = 77.5%
- **The blend rises** — 72.5% → 77.5%, up 5 points, purely because 4,000 fresh users joined the pool
- **Every cohort fell** — month-1 retention dropped 45% (Jan) → 40% (Feb), and Jan aged 45% → 25%
- **Month-1 vs W1** — month-1 counts activity any time in the month, so it sits above the W1 numbers

*Example (italic):* The dashboard headline says retention improved 5 points in March; the triangle says every single cohort got worse.

**Key point:** Blended retention is a weighted average whose weights are cohort sizes and ages — change the mix with a signup spurt and it rises even while every cohort's behavior worsens.

### Visualization (canvas `c2`, 720×300)

Two stacked bars (calendar Feb vs calendar Mar): active users split into new signups vs returners from each older cohort, drawn inside an outline showing total signups to date, blended % on top.

- **Title (bold 15px, `#1a5276`, top center):** "Same Months, Two Stories: the Blend Rises as Cohorts Decay".
- **Scale:** y = users 0 to 6,000 mapped to plot height 180, baseline y=245; gridlines `#e5e9ef` at 2,000 and 4,000 with 12px `#444` labels at x=50.
- **Feb bar (x=180, width 120):** outline-only rect (2px `#6b7280`) up to 2,000 (height 60 = "all signups to date"); inside, stacked from baseline: green `rgba(0,131,0,0.45)` segment 1,000 (height 30, 12px label "new: 1,000") then blue `rgba(42,120,214,0.45)` segment 450 (height 13.5, label "Jan @45%: 450"); bold 14px `#1a5276` "72.5% blended" above the outline.
- **Mar bar (x=440, width 120):** outline to 6,000 (height 180); stack: green 4,000 (height 120, "new: 4,000"), blue 400 (height 12, "Feb @40%: 400"), violet `rgba(74,58,167,0.45)` 250 (height 7.5, "Jan @25%: 250"); bold 14px `#1a5276` "77.5% blended" above.
- **Annotation (bold 13px red `#e74c3c`, near x=310, y=95):** "returning slices shrink — the fresh-user flood lifts the blend".
- **Caption (12px `#444`, bottom right):** "cohort sizes illustrative, percentages exact for these counts".

## The Flattening Curve Is the Real Prize

**Tags:** `where it's used` (blue), `product-market fit` (green)

- **The curve** — plot one cohort's retention week by week; the *shape* matters more than any point
- **Flattening** — Jan's curve 40, 30, 25, 22 then 21, 20, 20, 20: a core has made notes a habit
- **Leaky** — a curve that keeps sliding (40, 26, 17, 11, 7, 5, 3, 2) means nobody stays for good
- **The signal** — a plateau above zero is the classic product-market-fit read; its height is the core
- **The lever** — marketing lifts where the curve starts; the product decides where it flattens

*Example (italic):* Two apps both start at 40% in week 1; by week 8 one holds 20% of its cohort indefinitely and the other holds 2% — same top of funnel, opposite businesses.

**Key point:** A retention curve that flattens above zero means a durable core of users; a blended number can never show a plateau because it keeps mixing newcomers into the pool.

### Visualization (canvas `c3`, 720×300)

Line chart of two cohort retention curves over 8 weeks: one flattening at 20% (product-market fit), one sliding toward zero (leaky bucket), same week-1 start.

- **Title (bold 15px, `#1a5276`, top center):** "Two Curves, Same Start: Plateau vs Leaky Bucket".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = "W1" to "W8" with 12px `#444` tick labels each week; y = retention 0 to 40%, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels.
- **Flattening line:** green `#008300` 3px line with 4px dots through weeks `[1,2,3,4,5,6,7,8]`, retention `[40, 30, 25, 22, 21, 20, 20, 20]`.
- **Leaky line:** red `#e74c3c` 3px line with 4px dots through the same weeks, retention `[40, 26, 17, 11, 7, 5, 3, 2]`.
- **Plateau guide:** dashed `#6b7280` (dash 4/3) horizontal line at 20% from W5 to W8.
- **Annotations:** bold 13px green `#008300` "plateau at 20% = product-market fit" near (W5, y≈130); bold 13px red `#e74c3c` "sliding to zero = leaky bucket" near (W6, y≈215).
- **Caption (12px `#444`, bottom right):** "retention % illustrative".

## Celebrating the Blended Number

**Tags:** `common mistake` (red), `vanity metric` (orange)

- **The celebration** — the March review shows one rising line: "retention up 72.5% → 77.5%"
- **The reality** — month-1 retention fell 45% → 40% → 35% across the Jan, Feb, Mar cohorts
- **The engine** — the rise came from 4,000 fresh signups, active by definition in their first month
- **The unwind** — pause signups in April: 1,400 + 200 + 150 = 1,750 of 6,000 active → 29.2% blended
- **The fix** — report the triangle (or per-cohort curves) first; treat blended as a mix-shift number

*Example (italic):* The April all-hands opens with retention "collapsing" from 77.5% to 29.2% — nothing changed in the product; only the signup spurt ended.

**Common mistake:** Celebrating blended retention without reading the triangle. Blended retention borrows from growth — while signups accelerate it can rise as every cohort decays, and it pays those points back the moment growth stops.

### Visualization (canvas `c4`, 720×300)

Two-panel chart: left, the blended retention line over Feb–Apr with its cliff when signups pause; right, the per-cohort month-1 retention bars falling the whole time.

- **Title (bold 15px, `#1a5276`, top center):** "The Dashboard vs the Triangle".
- **Left panel (x 60–380, baseline y=245, plot height 170, y = 0–100%):** panel label bold 12px `#444` "blended '% active this month'" at top; blue `#2a78d6` 3px line with 5px dots through months `["Feb","Mar","Apr"]` at x = 100, 220, 340, values `[72.5, 77.5, 29.2]`; the Mar→Apr segment drawn red `#e74c3c`; 12px `#444` value labels at each dot; vertical dashed `#6b7280` (dash 4/3) line at x=280 with 11px `#6b7280` label "signups paused".
- **Right panel (x 440–690, same baseline, y = 0–50%):** panel label bold 12px `#444` "month-1 retention by cohort"; three bars width 56 at x = 450, 535, 620 for Jan/Feb/Mar, heights for `[45, 40, 35]` (scale 3.4 px per point), fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` top edge, bold 12px `#1a5276` value labels above, 12px `#444` cohort labels below; bold 13px red `#e74c3c` down-arrow annotation "falling every month" near (x 540, y 100).
- **Annotation (bold 13px magenta `#d55181`, centered near y=280):** "the blend borrowed its rise from growth — the triangle knew all along".
- **Caption (12px `#444`, bottom right):** "counts illustrative, percentages exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); cohort sizes (1,000 / 1,000 / 4,000), triangle percentages, and retention curves are invented and labeled illustrative; the blended percentages are exact arithmetic on those counts — Feb 1,450/2,000 = 72.5%, Mar 4,650/6,000 = 77.5%, Apr 1,750/6,000 = 29.2% (1,400 + 200 + 150 returners) — and the day-N vs rolling counts (220 vs 300 of 1,000) are exact for the stated percentages.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
