# Pitfall: Label Maturity (Censoring Window)

**Page type:** detail page (three card-sections, each a two-column layout table: text left 50%, canvas right 50%)
**HTML title tag:** Label Maturity (Censoring Window)

**Subtitle:** The outcome has not had time to happen yet, so "not yet" is recorded as "no".

## The Problem

Tags: `the trap` (red), `right-censoring` (blue)

- **Windowed label** — a 90-day-default or 30-day-churn label needs its full observation window to close
- **Young rows** — an account only 12 days old cannot yet show a 30-day event, however it behaves
- **Not wrong, not missing** — the label is present and plausible, it is simply not yet knowable
- **Recorded as zero** — the ETL writes `0` for "no event found", conflating "no" with "not yet"
- **One-directional error** — censoring only ever manufactures false negatives, so the positive rate is biased downward
- **Recency gradient** — the newest cohorts look safest purely because they are the youngest, a recency artifact
- **This is right-censoring** — the event time exceeds the observation time, so the true outcome stays unseen

*Example:* Illustrative Example — pooling seven cohorts aged 15 to 120 days gives a 11.1% default rate against a matured 18.0%, hiding 391 events.

**Impact:** 80.7% of rows are immature, so the pooled positive rate reads 11.1% instead of 18.0% — understated by 6.9 points, a 1.62x error.

### Visualization (canvas `c1`, 720×300)

A Gantt-style ladder of six accounts whose 90-day observation windows are sliced by a vertical "today" line, with the truncated ones stamped "recorded as NO". Not a conventional chart. All values are hardcoded literals; every derived number is computed in JS at render time.

- **Title (bold 14px `#1a5276`, centered, y=22):** "90-Day Observation Windows Cut Short by 'Today'".
- **Day axis:** days 0–180 mapped at 2.5 px/day from x=70 (`px(d) = 70 + 2.5*d`); 1px `#999` baseline at y=252 with tick marks and 10px `#666` labels "d0", "d30", "d60", "d90", "d120", "d150", "d180".
- **Six account rows** (height 16, first at y=64, spacing 30), literal opened-day values `[0, 8, 35, 58, 76, 88]`, label `A-1`…`A-6` in 10px `#2c3e50` right-aligned at x=62:
  - Full 90-day window drawn as a dashed (4,3) 1px `#999` outline from `opened` to `opened+90`.
  - Observed portion = `min(opened+90, 100) - opened`, filled `rgba(39,174,96,0.45)` with a 1px `#27ae60` border when the window completed by day 100, otherwise `rgba(231,76,60,0.35)` with a 1px `#e74c3c` border.
  - Left-aligned at x=534, 9px status text: matured rows show `#27ae60` "matured · label valid"; truncated rows show `#e74c3c` "recorded as NO · N% of window" with the share computed as `Math.round(obs/90*100)`.
  - Computed observed days per row: 90, 90, 65, 42, 24, 12 → shares 100%, 100%, 72%, 47%, 27%, 13%.
- **"Today" line:** 3px `#e74c3c` vertical line at day 100 from y=52 to the baseline, bold 11px red "TODAY" centered above at y=46, with a small filled downward arrowhead at the base.
- **Bottom annotation (bold 11px `#e74c3c`, centered, y=284):** computed truncated count — "4 of 6 windows are still open — each writes a 0 the data cannot support.".

## Why It Happens

Tags: `root cause` (orange), `passes every check` (blue)

- **Nothing is null** — a censored label is a valid `0`, so null checks and range checks all pass
- **Balance looks plausible** — 11.1% positives reads as "a bit low", not as a broken column
- **No age column** — the training table rarely carries cohort age, so nobody can group the rate by it
- **Spurious protection** — the model learns "recent = safe" because recency correlates with a written 0
- **Confidence inversion** — it is most certain about exactly the rows where it has least information
- **"Use all data"** — the naive fix keeps immature rows and bakes the censoring straight into the weights
- **Dropping is costly** — excluding them discards the freshest, most on-distribution signal you have
- **Real tension** — completeness and recency trade off directly, which is why censoring has no free fix

*Example:* Illustrative Example — account A-6 is 12 days into a 30-day churn window, so `churned_30d = 0` is not an observation, it is an absence of one.

**Root Cause:** A fixed observation window meets rows younger than it, and the pipeline encodes "event not observed yet" with the same symbol it uses for "event will not occur".

### Visualization (canvas `c2`, 720×300)

The same account row stamped two ways — "NO" versus "NOT YET" — above the validation checks it slips past. Not a conventional chart.

- **Title (bold 14px `#1a5276`, centered, y=22):** "One Row, Two Readings: 'NO' vs 'NOT YET'".
- **Source row card:** 420×34 centered at y=42, white fill, 2px `#2980b9` border; 11px `#1a5276` centered text "account A-6 · opened day 88 · age 12 d · churn_30d = 0".
- **Two elbow connectors** in 2px `#e74c3c` (left) and `#e67e22` (right) from the card bottom out to x=190 and x=530, down to y=118.
- **Two verdict boxes** 250×86 at y=118, white fill, 3px border, centered at x=190 and x=530:
  1. Left, red `#e74c3c`: bold 15px "NO" (y=142), 11px `#333` "what the label file says" (y=162), 10px `#666` "trains as a confirmed negative" (y=178) and "18 days of window unobserved" (y=192).
  2. Right, orange `#e67e22`: bold 15px "NOT YET" (y=142), 11px `#333` "what the data supports" (y=162), 10px `#666` "outcome is right-censored" (y=178) and "exclude from training, still score" (y=192).
- **Checks strip:** 10px `#666` label "validation checks on this column:" at x=60, y=231; then four pills (height 20 at y=240, radius 10, 9px bold text, width = measured text + 18) laid out left to right from x=60 with 8px gaps — "0 nulls", "0 out of range", "0 duplicates" in `rgba(39,174,96,0.15)`/`#27ae60`, and a fourth in `rgba(230,126,34,0.15)`/`#e67e22` whose text is computed from the same cohort literals as canvas `c3`: "11.1% positive (expected 18.0%)".
- **Bottom annotation (bold 11px `#1a5276`, centered, y=284):** computed gap — "Three checks pass; the fourth is a 6.9-point gap no schema rule can see.".

## The Correct Approach

Tags: `the fix` (green), `survival analysis` (blue)

- **Declare the window** — write the maturity period for each label as an explicit, versioned constant
- **Gate on age** — exclude rows younger than the window from training, but never from scoring
- **Point-in-time join** — attach a label only once its observation window has actually closed
- **Rate by cohort age** — plot positive rate against age; a rising curve exposes censoring immediately
- **Convergence check** — the curve should flatten at the window, and here it holds 18.0% from day 90 on
- **Survival analysis** — Kaplan-Meier and Cox proportional hazards models are built for censored time-to-event data
- **Discrete-time hazard** — reframing as per-period hazard uses partial windows without the downward bias
- **Never mix ages** — comparing a 15-day cohort's rate against a 120-day cohort's rate is meaningless

*Example:* Illustrative Example — the 90-day and 120-day cohorts both land at exactly 18.0%, confirming 90 days is where the label matures.

**Fix:** Define the maturity window per label, train only on matured rows via point-in-time joins, monitor positive rate by cohort age, and model the censoring directly with survival analysis when you cannot afford to wait.

### Visualization (canvas `c3`, 720×300)

Cohort staircase — the one conventional chart on the page. Apparent positive rate by cohort age, with the pooled rate and the matured truth drawn as reference lines. Both are computed in JS from the same hardcoded literals, never asserted.

- **Literal cohort table** (age in days, n accounts, matured default events observed):

  | Cohort age | n | Events | Apparent rate |
  |---|---|---|---|
  | 15 d | 1,200 | 42 | 3.5% |
  | 30 d | 1,000 | 72 | 7.2% |
  | 45 d | 900 | 99 | 11.0% |
  | 60 d | 800 | 112 | 14.0% |
  | 75 d | 700 | 112 | 16.0% |
  | 90 d | 600 | 108 | 18.0% |
  | 120 d | 500 | 90 | 18.0% |
  | **Pooled** | **5,700** | **635** | **11.1%** |

  Arrays are `n = [1200,1000,900,800,700,600,500]` and `ev = [42,72,99,112,112,108,90]`; each rate is `ev[i]/n[i]`, the pooled rate is `sum(ev)/sum(n) = 635/5700 = 11.14%`, and the matured rate is `(108+90)/(600+500) = 198/1100 = 18.00%`. The rate labels printed on the bars are formatted from these divisions, not typed in.
- **Title (bold 14px `#1a5276`, centered, y=22):** "Apparent Default Rate Rises With Cohort Age (Illustrative Example)".
- **Axes:** baseline 1px `#999` at y=245 from x=88 to x=668; y-axis ticks every 5% from 0% to 20% (`y = 245 - rate/0.20*170`), 9px `#666` labels, 1px `#eee` gridlines.
- **Seven bars:** slot width `580/7`, bar width 52, centered in slot; height `rate/0.20*170`. Immature cohorts (15–75 d) fill `rgba(231,76,60,0.35)` with a 1px `#e74c3c` border; matured cohorts (90, 120 d) fill `rgba(39,174,96,0.45)` with a 1px `#27ae60` border. Above each bar the computed rate in bold 10px (bar's border color); below the baseline the age in 10px `#333` at y=260 and `n = …` in 9px `#666` at y=274.
- **Matured-truth line:** dashed (5,4) 2px `#27ae60` horizontal line at 18.00% across x=88…668, right-aligned bold 9px `#27ae60` label "matured truth 18.0%" just above it at x=666.
- **Pooled line:** dashed (5,4) 2px `#e67e22` horizontal line at the computed pooled rate, left-aligned bold 9px `#e67e22` label "pooled 11.1%" just above it at x=92.
- **Bottom annotation (bold 11px `#1a5276`, centered, y=292):** computed at render time — "Pooling ages understates the rate by 6.9 pts (1.62x); 391 events have not happened yet." where the gap is `18.00 − 11.14`, the ratio `0.1800/0.1114`, and the missing count `round(0.18*5700) − 635 = 1026 − 635 = 391`.

## Regeneration instructions

- **Layout:** `.card-section` per section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (border-collapse, full width) with one `<tr>`: `td.text-col` (50%) holding `.tags` pills + `<ul>` bullets + `.example` italic paragraph + `.key-point` callout; `td.viz-col` (50%) holding the canvas. Shrink a visual via canvas `max-width`/`max-height`, never by narrowing the column.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. No nav bar, no back/home/see-also links — this is a leaf page.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `li b` colored `#1a5276`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Data rule:** all chart data is hardcoded literal arrays — no `Math.random()`, no seeded generation needed here, because the cohort counts themselves carry the lesson. Every rate, ratio, gap and total printed on a canvas is computed from those literals at draw time.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- In regenerated HTML, any card links use `.html` extensions.
