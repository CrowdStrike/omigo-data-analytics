# Survival Analysis

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Survival Analysis

**Subtitle:** Measuring time-until-an-event when most rows say "hasn't happened yet" — a still-active customer is evidence of at least this long, not a missing value

## Ten Gym Members and Six Question Marks

**Tags:** `core idea` (blue), `time-to-event` (green), `censoring` (orange)

- **The gym** — ten members join on Jan 1; six months later only four have cancelled
- **The question** — "how long does a member stay?" must use the six who haven't left yet
- **Censoring** — a still-active member's row says "at least 6 months", not a full answer
- **Time-to-event** — each member is a running clock that stops at cancel or at today
- **The trap** — averaging only the four quitters throws away six of the ten clocks

*Example (italic):* Member M5 has stayed 6 months and counting — her row reads "≥ 6", not "6" and not "unknown".

**Key point:** Survival analysis measures time until an event while treating "hasn't happened yet" as real evidence — a lower bound on the answer, never a blank.

### Visualization (canvas `c1`, 720×300)

Swimlane timeline: one horizontal line per member from month 0 to their cancel month (X) or to month 6 with an arrow (still active).

- **Title (bold 15px, `#1a5276`, top center):** "Ten Gym Members Over Six Months: 4 Cancels, 6 Still Active".
- **Data:** members and outcomes `[["M1",6,"active"],["M2",4,"quit"],["M3",1,"quit"],["M4",6,"active"],["M5",6,"active"],["M6",6,"active"],["M7",2,"quit"],["M8",6,"active"],["M9",5,"quit"],["M10",6,"active"]]`.
- **Layout:** month scale 0–6 mapped to x = 100 + m×90 (axis from x=100 to x=640); rows at y = 58 + i×20 for i = 0..9; member labels 12px `#444` right-aligned at x=92; month tick labels "0".."6" 12px `#444` at y=272 under their x positions, with a 1px `#e5e9ef` vertical gridline per month from y=50 to y=250.
- **Quit rows (M2, M3, M7, M9):** magenta `#d55181` 3px line from x(0) to x(end month), bold magenta 14px "✕" glyph centered at the line's end.
- **Active rows:** blue `#2a78d6` 3px line from x(0) to x(6), 6px filled arrowhead at the right end pointing right.
- **Legend (top right, x≈470, y=40 and y=54):** bold magenta 12px "✕ = cancelled (event)"; bold blue 12px "→ = still active (censored)".
- **Annotation (bold 12px orange `#d95926`, bottom right near x=420, y=290):** "6 of 10 rows only say 'at least 6 months'".
- **Caption (12px `#6b7280`, bottom left):** "illustrative data".

## The Kaplan-Meier Curve, Step by Step

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **At risk** — at each cancel month, count members still around: 10, then 9, then 8, then 7
- **First step** — month 1: 1 of the 10 cancels, so 9/10 = 0.90 of members survive the step
- **Multiply down** — month 2: 0.90 × 8/9 = 0.80; month 4: × 7/8 = 0.70; month 5: × 6/7 = 0.60
- **Read it** — after six months an estimated 60% of members are still active
- **Median** — the curve never falls below 0.50, so median tenure is beyond 6 months

*Example (italic):* By hand it is four multiplications, one per cancellation: 0.90 → 0.80 → 0.70 → 0.60.

**Key point:** The curve drops only when an event happens, and each drop divides by the members still at risk — that is exactly how the censored six keep contributing.

### Visualization (canvas `c2`, 720×300)

Kaplan-Meier step function for the gym cohort: share still active vs months, with a dashed 50% line that is never crossed.

- **Title (bold 15px, `#1a5276`, top center):** "Kaplan-Meier: Share of Members Still Active".
- **Data:** step values `[[0,1.00],[1,0.90],[2,0.80],[4,0.70],[5,0.60],[6,0.60]]` — hold each value flat until the next cancel month, then drop vertically.
- **Axes:** origin x=70, baseline y=245, plot width 560 (to x=630), plot height 185 (top y=60); y from 0 to 1 with 1px `#e5e9ef` gridlines and 12px `#444` labels at 0, 0.25, 0.50, 0.75, 1.00; x months 0–6 labeled 12px `#444` below the baseline.
- **Curve:** green `#008300` 3px stepped line; 4px green dots at each drop landing, each with a bold green 12px value label above-right: "0.90", "0.80", "0.70", "0.60".
- **Median line:** dashed magenta `#d55181` (dash 5/4) horizontal line at the y of 0.50 across the plot; bold magenta 12px label above it at x≈290: "50% never crossed — median tenure > 6 months".
- **Censor tick:** blue `#2a78d6` 3px vertical tick 10px tall centered at (month 6, 0.60), with blue 11px label "6 members censored here" to its left.
- **Caption (12px `#444`, bottom center):** "drops only at cancel months 1, 2, 4, 5".
- **Caption (12px `#6b7280`, bottom left):** "illustrative data".

## Three Averages, Two of Them Wrong

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Drop the actives** — the four quitters average (1+2+4+5)/4 = 3.0 months, far too low
- **Fake a date** — calling the six actives "quit at month 6" gives 4.8 months, still biased low
- **KM answer** — median tenure exceeds 6 months; over half the cohort never quit in the window
- **Why the bias** — long-stayers are exactly the ones still active, so both shortcuts trim the top
- **Where it's used** — churn, hardware failure, patient survival, loan default, ticket resolution

*Example (italic):* A dashboard reporting "average member lifetime: 3.0 months" understates this gym's truth by more than half.

**Common mistake:** Resolving "hasn't happened yet" by deleting the row or inventing an end date — either recipe biases every lifetime metric downward, often severely.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing three answers to "how long do members stay?" — two biased shortcuts vs the Kaplan-Meier median.

- **Title (bold 15px, `#1a5276`, top center):** "Three Ways to Report 'How Long Do Members Stay?'".
- **Data:** `[["average of quitters only", 3.0], ["count actives as quitting today", 4.8], ["Kaplan-Meier median", 6.0]]` (the third is a lower bound, drawn open-ended).
- **Axes:** month scale 0–7 mapped from x=250 to x=660; month ticks 0..7 labeled 12px `#444` at y=262 with 1px `#e5e9ef` gridlines from y=60 to y=245; row labels 13px `#2c3e50` right-aligned at x=240.
- **Bars (26px tall):** at y=85 magenta fill `rgba(213,81,129,0.55)` to 3.0 with bold magenta 13px "3.0 mo" just past the end; at y=145 orange fill `rgba(217,89,38,0.50)` to 4.8 with bold orange 13px "4.8 mo"; at y=205 green fill `rgba(0,131,0,0.45)` to 6.0 followed by a dashed green 3px arrow from 6.0 to 6.8 and bold green 13px "> 6 mo (not yet reached)".
- **Annotation (bold 13px `#1a5276`, bottom center at y=292):** "both shortcuts undershoot; quitters-only by half or more".
- **Caption (12px `#6b7280`, bottom left):** "illustrative data".

## Censored Is Not Missing

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Not missing** — a censored row carries real information: this member lasted at least this long
- **Delete them** — with only the 4 quitters the curve falls 0.75 → 0.50 → 0.25 → 0 by month 5
- **Same gym** — the correct curve ends at 60% active; the deleted-rows curve claims 100% churn
- **Other direction** — stamping actives with today's date turns every loyal member into a quitter
- **The rule** — a censored member stays in the at-risk count until the moment you lose sight of them

*Example (italic):* Deleting still-active members turns a gym with 60% six-month retention into one where everyone quits.

**Common mistake:** Treating "hasn't happened yet" as either missing data (drop the row) or as the event itself (stamp it with today) — censoring is a third category with its own arithmetic.

### Visualization (canvas `c4`, 720×300)

Two overlaid step curves from the same 10 members: the correct Kaplan-Meier vs the curve you get after deleting the six censored rows.

- **Title (bold 15px, `#1a5276`, top center):** "Same Gym, Two Curves: Keep vs Delete the Still-Active".
- **Data:** correct curve `[[0,1.00],[1,0.90],[2,0.80],[4,0.70],[5,0.60],[6,0.60]]`; deleted-censored curve `[[0,1.00],[1,0.75],[2,0.50],[4,0.25],[5,0.00],[6,0.00]]` — both stepped (hold flat, drop vertically at each cancel month).
- **Axes:** identical to `c2` — origin x=70, baseline y=245, plot width 560, plot height 185; y 0–1 with gridlines and 12px labels at 0, 0.25, 0.50, 0.75, 1.00; x months 0–6 labeled 12px `#444`.
- **Correct curve:** green `#008300` 3px steps with 4px dots at drops; bold green 13px label near (6, 0.60): "keep censored: 60% still active".
- **Deleted-rows curve:** magenta `#d55181` 3px steps with 4px dots at drops; bold magenta 13px label near (5, 0.00), offset above the baseline: "delete censored: 0% left by month 5".
- **Caption (12px `#444`, bottom center):** "both curves computed from the same 10 members".
- **Caption (12px `#6b7280`, bottom left):** "illustrative data".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays — no `Math.random()`; step curves are drawn hold-flat-then-drop, never diagonal interpolation.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
