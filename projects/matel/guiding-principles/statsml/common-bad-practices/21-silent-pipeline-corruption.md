# Ignoring Silent Pipeline Corruption

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, single row containing four titled blocks)
**HTML title tag:** Silent Data Poisoning — Common Bad Practices

**Subtitle:** Negligent Practice — Not validating data semantics at pipeline boundaries. Bugs that produce wrong-but-plausible values pass every structural check and poison months of downstream work.

## The Practice

- Pipeline has schema checks (types, nulls, row counts). No semantic checks (distributions, value ranges, cross-column relationships).
- A JOIN fanout silently doubles revenue. A timezone shift moves all timestamps +8 hours. A default change flips NULL to 0. No alert fires because row counts match, schema is valid, nulls are absent.
- Months of dashboards, models, and business decisions built on corrupted data. Discovered during quarterly reconciliation — 3 months too late.

## Why It's Common

- "We have data validation!" — Yes, for STRUCTURE. Not for MEANING. Schema checks are necessary but nowhere near sufficient.
- "The tests pass!" — Tests check that code runs. They don't check that values make sense in context.
- "Row count matches expected." — 100K rows came in, 100K rows came out. But each row is wrong.

## The Data-Specific Damage

- **Models trained on poison:** ML model learns the corrupted pattern as ground truth. Accuracy drops gradually. By the time it's noticed, the model has been serving bad predictions for weeks.
- **Decisions made on fiction:** "Revenue is up 40%!" → hire more people → discover it was a JOIN bug → revenue is flat → layoffs. The business decision was irreversible.
- **Calendar cost:** Corrupt data can't be uncomputed. If raw data wasn't retained, or if the corruption window overlaps with a model training cycle, you've lost months of work.

## What's Missing

- Distribution drift checks at every pipeline boundary (mean, variance, percentiles).
- Cross-column consistency checks (revenue = price × quantity).
- Row count change budgets (±5% expected, flag if exceeded).
- Value range assertions (latitude between -90 and 90, age between 0 and 120).

**Why it persists:** Semantic validation is harder than structural validation. It requires domain knowledge to write the checks. "What SHOULD this number be?" is a harder question than "IS this number a number?" Teams default to the easy checks and call it "validated."

**The tell:** Ask: "If the mean of column X shifted by 50%, would any alert fire?" If the answer is no — your pipeline is vulnerable to silent poisoning.

### Visualization (canvas `c1`, 720×380)

Time-series of the poisoning itself: 90 days of daily revenue with a silent JOIN-fanout level shift at day 30, under a flat row-count line proving every structural check kept passing.

- **Title (bold 14px, top center, `#1a5276`):** "The bug every check missed".
- **Plot area:** x = days 0-90 (ticks every 15 days, 11px `#666`), y = revenue index 60-160 (gridlines `#eee` at 80/100/120/140, right of a 55px left margin).
- **Row-count line:** flat gray `#999` width-1.5 line near the top of the plot (constant level ~155), labeled left-aligned 11px `#666` above it: "row count: unchanged — all checks pass ✓".
- **Revenue line (blue `#1a5276`, width 2):** days 0-30: `100 + sin(i*1.7)*3 + sin(i*0.4)*2`; days 31-90: same formula **+40** (wrong-but-plausible level ~140 with identical noise).
- **Bug marker:** orange `#e67e22` dashed vertical line at day 30, bold 11px orange label "JOIN fanout bug ships".
- **Corrupted window:** days 30-90 shaded `rgba(231,76,60,0.08)`.
- **Discovery marker:** red `#e74c3c` dashed vertical line at day 90, bold 11px red label "found at quarterly reconciliation" (right-aligned, kept inside the canvas).
- **Insight annotation (bold 13px red, centered over the shaded window, below the revenue line):** "Every structural check passed for 60 days — every number was wrong."
- **Caption (bottom center, italic 12px, `#666`):** "Illustrative daily revenue index — the +40% level shift is duplicate rows from a JOIN, not growth."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds four `.obj-title` blocks (the ones after the first get `style="margin-top:14px;"`) each followed by a `<ul>`, then the two `<p><strong>` paragraphs ("Why it persists:", "The tell:"); right `<td>` (60%, centered, `vertical-align: middle`) holds canvas `c1`.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width="720" height="380"`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`/`#555`.
