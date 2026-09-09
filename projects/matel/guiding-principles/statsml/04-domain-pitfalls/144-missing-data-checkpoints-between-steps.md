# Missing Data Checkpoints Between Transformation Steps

**Page type:** detail page (h2 section per topic, each with a two-column obj-table: text left 50%, canvas right 50%; final section is a 2-column text-only examples table)
**HTML title tag:** 144. Missing Data Checkpoints Between Transformation Steps

**Subtitle:** When intermediate transformation outputs aren't persisted, a wrong final result leaves you with only source and output — the steps in between are uninspectable.

## Callout (philosophy box)

**The core problem:** If only raw input and final output are saved, "which middle step went wrong?" is unanswerable — you must re-run and hope the issue reproduces, and often it doesn't because the upstream data or transient state is gone.

## Only Source and Final Output Exist — The Gap Is Uninspectable

**5-Step Pipeline, 2 Checkpoints. 3 Blind Spots.**

- **The symptom:** Model accuracy drops 8%; step 5 looks wrong, step 1 looks fine.
- **The blind spot:** The three middle steps persisted nothing, so the fault region is uninspectable.
- **Without checkpoints:** You can only guess — join fanout, aggregate hitting NULLs, or clean dropping rows.
- **The re-run fallacy:** Re-running gives a different result; upstream data changed and transient state is gone.
- **No evidence either way:** A fresh run says nothing about what the failing run actually produced.
- **With checkpoints:** Inspect step 3 output directly and spot the join fanout in minutes.

### Visualization (canvas `c1`, 720×340)

Two-row pipeline diagram comparing checkpoint coverage; light gray background `#f9f9f9`.

- **Title (bold 14px, top center, `#1a5276`):** "5-step pipeline: where is the data saved?"
- **Steps (5 boxes per row, evenly spaced, 50px tall, margins left/right 30, top 50):** labels "Raw", "Clean", "Join", "Aggregate", "Model Input"; gray arrows (`#999`) with triangular heads between boxes.
- **Top row** headed by bold red (`#e74c3c`) label "Without checkpoints (typical):". Boxes 1 and 5 (Raw, Model Input) are saved — fill `rgba(39,174,96,0.25)`, stroke `#27ae60` width 2.5, sub-label "✓ SAVED" in green; boxes 2–4 are ephemeral — fill `rgba(231,76,60,0.1)`, stroke `#e74c3c` width 1.5, sub-label "✗ EPHEMERAL" in red. Step names in bold 11px `#333`.
- **Bottom row** headed by bold green (`#27ae60`) label "With checkpoints at every step:". All 5 boxes green-saved style with "✓ SAVED".
- **Bug indicator:** below the bottom row under the third box (Join), bold red text "↑ Bug here".
- **Comparison lines (bottom center, 12px):** red "Without: have step 1 and step 5. Steps 2-4 are gone. Must re-run." then green "With: inspect step 2→3 diff directly. No re-run needed."

## The Data Changed — You Can't Go Back

**By the Time You Investigate, the Source Has Moved**

- **Mutable sources:** The pipeline read a production database at 3am; that source has been updated since.
- **Different answer now:** Query it during the investigation and it returns data the run never saw.
- **Temporal debugging is impossible:** "What did this stage produce Tuesday?" is answerable only if you saved it.
- **Logs are not data:** They prove the step ran, not what the rows looked like going through it.
- **The months-later discovery:** A September problem with a June root cause dies once June is overwritten.

### Visualization (canvas `c2`, 720×300)

Timeline diagram with an unrecoverable gap; background `#f9f9f9`.

- **Title (bold 14px, center, `#1a5276`):** "Source data mutates — past pipeline state is unrecoverable"
- **Timeline:** horizontal line (`#333`, width 2) at y = top margin (55) + 40, spanning left margin 50 to right margin 30; tick marks per event.
- **Events** (fraction along timeline, label, color, above/below the line):
  - 0.1 — "Pipeline runs / (3am Tuesday)", blue `#3498db`, above
  - 0.3 — "Source DB / updated", orange `#e67e22`, below
  - 0.5 — "More source / updates", orange `#e67e22`, below
  - 0.75 — "Problem detected / (9am Thursday)", red `#e74c3c`, above
  - 0.9 — "Investigation / starts", red `#e74c3c`, above
- **Gap box:** dashed red rectangle (dash 4/4, fill `rgba(231,76,60,0.08)`) spanning from x=0.1 to x=0.75 of the timeline, 80px tall below the line, containing bold red "This window is GONE" and gray `#555` 11px lines "Source mutated. Intermediate state never saved. Cannot reconstruct." and "Query source NOW → get different data than pipeline saw Tuesday."
- **Caption (bottom center, italic gray 12px):** "Checkpoints are snapshots of what the pipeline ACTUALLY SAW at each step. Without them, that state is lost."

## Aggregation Destroys Debugging Information

**After Aggregation, You Cannot Decompose the Number**

- **The problem:** A cohort average comes out at $450 instead of the expected $200.
- **Nothing to open up:** Without the pre-aggregation rows you cannot decompose which users drove it.
- **The JOIN amplification variant:** A bad join duplicates 5 transactions 10×; post-averaging that reads as 50.
- **The general principle:** Every aggregation is lossy compression — save the rows before it, or lose them.

### Visualization (canvas `c3`, 720×300)

Before/after aggregation diagram; background `#f9f9f9`.

- **Title (bold 14px, center, `#1a5276`):** "Aggregation is lossy — can't decompose without pre-aggregation data"
- **Left block** headed "Step 3 output (row-level):" (bold 12px `#1a5276`); five monospace 11px rows in 240px-wide highlight strips:
  - `user_1  $50   purchase` (green tint `rgba(39,174,96,0.1)`, text `#333`)
  - `user_1  $50   purchase (dupe!)` (red tint `rgba(231,76,60,0.15)`, text `#e74c3c`)
  - `user_1  $50   purchase (dupe!)` (red tint, red text)
  - `user_2  $80   purchase` (green tint, `#333`)
  - `user_3  $120  purchase` (green tint, `#333`)
- **Center arrow** (`#333`, width 2) labeled bold 11px "GROUP BY" above and "AVG()" below.
- **Right block** headed "Step 4 output (aggregated):"; one red-tinted strip with monospace `avg_spend = $70`; below it red 11px lines "Expected: $83", "Got: $70 (dupes diluted avg)", then bold "Without step 3 data: can't see the dupes" and "Only see wrong aggregate, not WHY".
- **Caption (bottom center, italic gray 12px):** "Every GROUP BY / SUM / AVG destroys row-level evidence. Save BEFORE aggregating."

## Partial Re-run Requires Checkpoints as Starting Points

**Fix Step 3, Re-Run Steps 3-5. But Without Step 2 Output, You Must Start From Step 1.**

- **The scenario:** You fix a bug in step 3, but its input is step 2's output — which was never saved.
- **The cost without:** You re-run steps 1 and 2 (4hr + 2hr) just to reach the fix you want to test.
- **The cost with:** Start from step 2's saved output and the 1-hour fix run is all you pay.
- **The gap:** 7 hours to first test versus 1 hour — 12 experiments a day instead of 2.
- **The experimentation cost:** Debugging velocity is directly proportional to checkpoint density.

### Visualization (canvas `c4`, 720×300)

Horizontal stacked-bar time comparison; background `#f9f9f9`; left margin 180 for row labels, bars 60px tall, 40px gap.

- **Title (bold 14px, center, `#1a5276`):** "Bug in step 3: how long to get back to testing the fix?"
- **Row 1** (label right-aligned, 12px `#1a5276`, two lines "Without step 2" / "checkpoint:"): stacked segments with white bold 10px in-bar labels and `#333` 1px outlines —
  - "Re-run step 1 (4hr)", 44% of track width, red `#e74c3c`
  - "Re-run step 2 (2hr)", 22%, orange `#e67e22`
  - "Run fix (1hr)", 11%, green `#27ae60`
  - Total labeled to the right in bold red 12px: "7 hours".
- **Row 2** (label "With step 2" / "checkpoint:"): single green segment "Run fix (1hr)" at 11% width; bold green total label "1 hour".
- **Caption (bottom center, bold 13px `#555`):** "7× faster debugging. 12 experiments/day vs 2 experiments/day."

## The Storage Excuse vs The Calendar Cost Reality

**"We Don't Checkpoint Because Storage Is Expensive"**

- **The math:** Checkpoint storage runs $23 a month; two weeks of blind debugging costs $8,000+.
- **The hidden line item:** Meanwhile models keep training on wrong data, which no storage bill captures.
- **Retention policy:** Keep 7 days at full granularity and weekly snapshots for 3 months, then purge.
- **The compromise that fails:** "Checkpoint on demand when something goes wrong" is useless in practice.
- **Why it fails:** A checkpoint only helps if it already exists at the moment you discover you need it.

### Visualization (canvas `c5`, 720×280)

Two horizontal bars contrasting costs; background `#f9f9f9`; left margin 200, bars 55px tall, 40px gap.

- **Title (bold 14px, center, `#1a5276`):** "Cost: checkpoints vs no checkpoints"
- **Bar 1** (label two lines, right-aligned 12px `#1a5276`: "Checkpoint storage" / "(1TB × 30 days):"): tiny green bar (0.3% of track, min 8px, fill `#27ae60`, `#333` outline) with bold green label to its right: "$23/month".
- **Bar 2** (label: "2 weeks debugging" / "without checkpoints:"): red bar `#e74c3c` at 80% of track width with centered white bold 12px in-bar text "$8,000+ engineer time + weeks of models training on bad data".
- **Caption (bottom center, italic gray 12px):** "$23/month buys time-travel debugging. The \"storage is expensive\" excuse costs 350× more."

## Real-World Examples

Text-only obj-table, 2 columns × 3 rows, each cell an `.obj-title` plus bullets (no canvases).

### home-buying platform's $500M Loss (2021)

- An ML pricing pipeline (comps → features → scoring → offer) kept no auditable intermediate data.
- When offers systematically overpaid, no one could pinpoint which step had diverged.
- The candidates — stale comps, wrong transforms, model drift — were all equally unfalsifiable.
- Thousands of homes were bought at inflated prices; the division shut down with a ~$500M loss.
- **With full checkpoints:** "Which comps did the model see for this house?" answerable in days, not months.

### a trading firm — $440M in 45 Minutes (2012)

- A deploy accidentally activated dead code trading on stale parameters from a retired system.
- Only market data in and executed trades out were recorded — no checkpoint of strategy signals.
- $7B in erroneous trades and a $440M loss before humans noticed from the output alone.
- **With signal-level checkpoints:** The 1000× abnormal order volume would show up in seconds.

### social network's 2019 Ad Metrics Bug

- Video view metrics were inflated for over a year by a bug in an intermediate filtering step.
- The bug sat in a time-threshold filter, between raw events and the published aggregates.
- Only raw events and final aggregates were retained, so scoping the overcount took months.
- Advertisers who had shifted millions in budget couldn't get accurate historical corrections.
- **With post-filter checkpoint:** The inflated count stands out immediately against raw event ratios.

### government enrollment portal Launch Failure (2013)

- The enrollment pipeline ran identity → eligibility → subsidy → confirmation with no checkpointing.
- When enrollments failed silently, nothing showed where the hand-off had dropped users.
- Debugging meant manually replaying user journeys across systems — weeks of investigation.
- **With per-step checkpoints:** "4,000 users verified, none reached eligibility" is a 5-minute query.

### music platform's "Discover Weekly" Retraining Issue (reported in engineering blog)

- When recommendation quality degraded, the team could not identify which stage caused it.
- Stale features, corrupted training data, and a ranking config change were all live suspects.
- They spent days on questions that diffing stage outputs against historical runs makes obvious.
- **Lesson they shared:** Checkpoint each stage, compare distribution stats across runs — days became hours.

### Every ETL Pipeline That Joins Fact + Dimension Tables

- The most common silent corruption: a fact-dimension JOIN yields 15M rows instead of 10M.
- The cause is duplicates in the dimension table, and the final output looks like ordinary growth.
- Telling growth from duplication needs pre-join row counts and the dimension state at join time.
- **Real frequency:** Monthly at any sizable data team, and every incident ends the same way.
- **The postmortem line:** "Now we save the intermediate state" — written after the damage, every time.

## Regeneration instructions

- **Layout:** h1 + `.subtitle` + `.philosophy` callout, then one `h2` per section (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by a `.obj-table` with one `<tr>`: left `<td>` (40%) holds `.obj-title` + `<ul>` bullets, right `<td>` (60%, centered) holds the canvas. The final "Real-World Examples" section is a `.obj-table` with 3 rows × 2 text-only cells (each `.obj-title` + bullets, no canvas).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; `ul` 0.9em `#333`; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows background `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Each chart fills its background `#f9f9f9`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#3498db`, gray text `#666`/`#555`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions.
