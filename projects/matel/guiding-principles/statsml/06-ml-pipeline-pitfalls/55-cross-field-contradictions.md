# Pitfall: Cross-Field Contradictions

**Page type:** detail page (three `.card-section` blocks, each a two-column layout table: text left 50%, canvas right 50%)
**HTML title tag:** Cross-Field Contradictions

**Subtitle:** Every field is individually valid, but the combination is impossible.

## The Problem

**Tags:** `the trap` (red), `row consistency` (blue)

- **Column-scoped validation** — types, nulls and ranges are checked one column at a time, so nothing trips
- **No column is wrong** — every value sits inside its own allowed domain; only the relation between two is broken
- **Temporal impossibility** — delivered_at earlier than ordered_at, or an end_date preceding its own start_date
- **Status contradiction** — status='cancelled' on a row whose delivered_at timestamp is populated anyway
- **Arithmetic that does not close** — line items sum below the invoice total, or discount exceeds gross price
- **Exclusive flags both lit** — is_new_customer=1 sitting next to a prior purchase count above zero
- **Dependent-field violation** — dependents_count=0 with a populated dependents list, or a country/currency mismatch

*Example:* Eight orders pass all five column checks at 100%, yet three rows are internally impossible, so whole-row validity is 62.5%.

**Impact:** A per-column pass rate of 100% is reported as green while more than a third of rows are logically impossible.

### Visualization (canvas `c1`, 720×300)

Row-level contradiction table: every cell passes its column check, a red arc spans the two cells that contradict each other.

- **Caption:** "Illustrative Example" in 9px `#999`, top-right.
- **Title (bold 14px, `#1a5276`, centered):** "Every Column 100% Valid — Row 62.5% Valid".
- **Literal data (hardcoded array, never generated).** Eight order rows, columns `order_id`, `ordered_at`, `delivered_at`, `status`, `gross`, `discount`:
  | order_id | ordered_at | delivered_at | status | gross | discount |
  |---|---|---|---|---|---|
  | 1041 | 2026-03-04 | 2026-03-07 | delivered | 200 | 20 |
  | 1042 | 2026-03-05 | 2026-03-02 | delivered | 150 | 15 |
  | 1043 | 2026-03-06 | 2026-03-09 | cancelled | 300 | 30 |
  | 1044 | 2026-03-07 | 2026-03-08 | delivered | 250 | 25 |
  | 1045 | 2026-03-09 | 2026-03-12 | delivered | 100 | 140 |
  | 1046 | 2026-03-10 | 2026-03-13 | delivered | 400 | 40 |
  | 1047 | 2026-03-11 | 2026-03-14 | delivered | 180 | 18 |
  | 1048 | 2026-03-12 | 2026-03-15 | delivered | 220 | 22 |
- **Column predicates (evaluated in JS over the literals, all 8/8):** `ordered_at` and `delivered_at` parse as dates inside March 2026; `status` is in `{delivered, cancelled}`; `gross > 0`; `discount >= 0`.
- **Row invariants (evaluated in JS over the same literals):** `I1: delivered_at >= ordered_at` fails on 1042; `I2: status='cancelled' ⇒ delivered_at IS NULL` fails on 1043; `I3: discount <= gross` fails on 1045. Three distinct rows fail, so row pass = 5/8.
- **Table render:** header baseline y=52 in bold 10px `#1a5276`; rows at 20px pitch starting y=72; column left-edges x=28 (order_id), 88 (ordered_at), 158 (delivered_at), 228 (status), 308 (gross), 360 (discount), 430 (row check); table right edge x=465. `ordered_at` / `delivered_at` render as `MM-DD`. Passing cell text 10px `#333`; the two cells involved in a contradiction drawn bold 10px `#e74c3c`.
- **Cell tint:** every row band gets a `rgba(39,174,96,0.10)` fill to show the per-column checks passed.
- **Contradiction arcs:** for each failing row, a 1.8px `#e74c3c` quadratic curve sagging ~7px below the row baseline, joining the measured text centers of the two offending cells; a bold 11px red `✗` in the row-check column. Passing rows get a bold 11px green `✓`.
- **Column footer (y=244):** under each of the five field columns, a bold 9px `#27ae60` `✓` plus the pass count computed at render time as `n_pass + "/8"` → "8/8".
- **Summary box (x=500, y=70, 200×120):** 2px `#e74c3c` border, white fill. Bold 10px `#27ae60` "column pass rate" with the computed value on the next line (bold 15px green, `100.0%`); bold 10px `#e74c3c` "whole-row pass rate" with the computed value (bold 15px red, `62.5%`); 9px `#666` line "3 of 8 rows impossible", the 3 computed.
- **Bottom annotation (bold 11px `#e74c3c`, centered, y=285):** computed string "5 columns × 8 rows = 40 cell checks pass. 3 rows are still impossible." (40 computed as columns × rows.)

## Why It Happens

**Tags:** `root cause` (orange), `derived features` (blue)

- **Validation is per column** — assertion frameworks iterate columns and never look at column pairs
- **Invariants are undeclared** — the cross-field rule lives in someone's head instead of in the schema
- **Combinatorial excuse** — pair checks get skipped because "there are too many pairs to enumerate"
- **Upstream partial writes** — a status update lands without its companion timestamp being cleared
- **Timezone skew** — two systems stamp the same event, so the ordering inverts by a few hours
- **Derived features hide it** — a duration built from two contradictory fields returns a number, not an error
- **Coercion launders it** — `abs()` or `max(0, x)` turns an impossible value into a confidently wrong one

*Example:* Order 1042 was delivered 3 days before it was ordered, so delivery_days = −3, which `abs()` silently reports as 3 days.

**Root Cause:** No single column is out of range, so nothing raises; the contradiction only surfaces once two fields are read together.

### Visualization (canvas `c2`, 720×300)

Backwards timeline: a valid order and an impossible order on the same axis, with the derived duration computed from the date literals.

- **Caption:** "Illustrative Example" in 9px `#999`, top-right.
- **Title (bold 14px, `#1a5276`, centered):** "delivered_at Before ordered_at → A Derived Feature That Cannot Exist".
- **Axis:** horizontal 1.5px `#999` line y=200 from x=50 to x=470; a tick for each of 2026-03-01 … 2026-03-16, labels every 3rd day in 9px `#666` at y=217 ("Mar 1", "Mar 4", …). Day-to-x mapping computed from the `Date` values, not hardcoded pixel positions.
- **Lane A — valid (green `#27ae60`, y=92):** order 1041, marker circles r=5 at ordered_at 2026-03-04 and delivered_at 2026-03-07, joined by a 2px forward arrow (arrowhead at delivered). Labels 9px `#333`: "ordered" and "delivered" above the markers; bold 11px green at x=488: `order 1041: delivery_days = +3` with the 3 computed as `(delivered − ordered)/86400000`, plus 9px `#666` "valid".
- **Lane B — impossible (red `#e74c3c`, y=145):** order 1042, markers at ordered_at 2026-03-05 and delivered_at 2026-03-02, joined by a 2.5px **backwards** arrow (arrowhead at the earlier delivered marker). Labels as above; bold 11px red at x=488: `order 1042: delivery_days = −3`, computed the same way, plus 9px `#666` "impossible".
- **Column verdict strip (y=176, 9px `#27ae60`, centered):** "ordered_at: valid date ✓   delivered_at: valid date ✓   → both columns pass".
- **Three coercion boxes (y=232, 96×46, centered at x=180, 360, 540):** white fill, 2px border, bold 10px heading + bold 12px computed value:
  1. `raw` — border `#e74c3c`, value the computed `−3` with 9px `#666` "impossible".
  2. `abs(x)` — border `#e67e22`, value the computed `3` with 9px `#666` "fabricated".
  3. `max(0, x)` — border `#e67e22`, value the computed `0` with 9px `#666` "fabricated".
- **Bottom annotation (bold 11px `#1a5276`, centered, y=293):** "The pipeline never errored. It just returned a number that is not true.".

## The Correct Approach

**Tags:** `the fix` (green), `invariants` (blue)

- **Declare invariants** — cross-field rules live next to the schema as first-class assertions, not tribal lore
- **Validate the row** — the unit of validity is the whole row, never a single column in isolation
- **Derive early, assert bounds** — compute durations and totals at ingest, then assert their sign and ceiling
- **Never coerce** — no `abs()` and no clipping on a value that has already violated an invariant
- **Quarantine, don't fix** — route contradictory rows to a holding table with the rule they broke attached
- **Track the violation rate** — the share of rows breaking an invariant is a first-class data-quality metric
- **Alert on jumps** — a sudden rise almost always means an upstream logic change, not bad luck

*Example:* The violation rate sits at a 0.20% baseline for four days, then averages 3.30% after an upstream deploy — a 16.5× rise.

**Fix:** Assert row invariants at ingest, quarantine the rows that break them, and alert on the violation rate rather than patching values.

### Visualization (canvas `c3`, 720×300)

Two panels: declared row invariants on the left, the violation-rate alert on the right.

- **Caption:** "Illustrative Example" in 9px `#999`, top-right.
- **Title (bold 14px, `#1a5276`, centered):** "Declare Row Invariants · Quarantine the Row · Alert on the Rate".
- **Left panel (x=25, y=48, 305×202):** 1px `#e0e0e0` border, `#f8f9fa` fill. Bold 10px `#1a5276` heading "row invariants, declared with the schema" at y=68. Five 9px monospace `#27ae60` lines at 20px pitch from y=90, each prefixed by a bold green `✓`:
  - `delivered_at >= ordered_at`
  - `status='cancelled' ⇒ delivered_at IS NULL`
  - `discount <= gross`
  - `sum(line_items) == invoice_total`
  - `is_new_customer=1 ⇒ prior_orders == 0`
  Below them a 2px `#e67e22` bordered strip (x=38, y=198, 279×40) with bold 10px `#e67e22` "violating row → quarantine table" and 9px `#666` "keep the row + the rule it broke; never coerce".
- **Right panel (x=360..706):** bar chart of daily invariant-violation rate.
  - **Literal data (hardcoded, never generated).** 8 days, rows checked and violations:
    | day | rows | violations | rate |
    |---|---|---|---|
    | D1 | 12,000 | 24 | 0.20% |
    | D2 | 12,000 | 18 | 0.15% |
    | D3 | 12,000 | 30 | 0.25% |
    | D4 | 12,000 | 24 | 0.20% |
    | D5 | 12,000 | 390 | 3.25% |
    | D6 | 12,000 | 402 | 3.35% |
    | D7 | 12,000 | 378 | 3.15% |
    | D8 | 12,000 | 414 | 3.45% |
    Every `rate` is computed at render time as `violations / rows`, never written as a literal.
  - **Bars:** baseline y=235, 26px wide at 38px pitch from x=372; height = `rate / (max_rate × 1.02) × 150`; days 1-4 fill `rgba(39,174,96,0.55)` stroke `#27ae60`; days 5-8 fill `rgba(231,76,60,0.55)` stroke `#e74c3c`. Day labels 8px `#666` at y=247; each bar's own computed rate printed above it in 8px, green pre-deploy and red post-deploy.
  - **Deploy marker:** dashed (5,4) 2px `#e67e22` vertical line between D4 and D5, with bold 9px `#e67e22` "upstream deploy" above at y=62.
  - **Computed callouts (bold 9px):** green "pre 0.20%" over the D1-D4 group and red "post 3.30%" over the D5-D8 group, both computed as `mean(violations)/12000`; bold 11px `#e74c3c` centered at y=270 the computed ratio string "16.5× jump — upstream logic changed".
- **Bottom annotation (bold 10px `#1a5276`, centered, y=292):** "Pre-deploy mean 24 violations/day · post-deploy mean 396 — both computed from the 8 daily counts.".

### Arithmetic behind the numbers

- Canvas 1: 5 field columns × 8 rows = 40 cell checks, 40 pass → 100.0%. Rows failing an invariant: 1042 (I1), 1043 (I2), 1045 (I3) = 3 distinct rows; 8 − 3 = 5 pass; 5/8 = 62.5%.
- Canvas 2: (2026-03-07 − 2026-03-04) = +3 days; (2026-03-02 − 2026-03-05) = −3 days; abs(−3) = 3; max(0, −3) = 0.
- Canvas 3: pre-deploy violations 24+18+30+24 = 96; 96/4 = 24/day; 24/12,000 = 0.20%. Post-deploy 390+402+378+414 = 1,584; 1,584/4 = 396/day; 396/12,000 = 3.30%. Ratio 396/24 = 16.5×.

## Regeneration instructions

- **Layout:** `.card-section` per section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (border-collapse, full width) with one `<tr>`: `td.text-col` (**50%**) holding `.tags` pills + `<ul>` bullets + `.example` italic paragraph + `.key-point` callout; `td.viz-col` (**50%**) holding the canvas. To shrink a visual, constrain the canvas with max-width/max-height — never narrow the viz column below 50%.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. No nav bar, no back/home links, no cross-page links — this is a leaf page.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `li b` colored `#1a5276`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Data rule:** all three canvases use hardcoded literal arrays — the counts and the shape carry the lesson. No `Math.random()` anywhere. Every rate, total, count and ratio printed on a canvas is computed in JS from those literals at render time, so text, table and chart reconcile to the digit.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
