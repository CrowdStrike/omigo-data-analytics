# Abandoned Custom Features Polluting Pipelines

**Page type:** detail page (h2 section headers, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 149. Abandoned Custom Features Polluting Pipelines

**Subtitle:** Short-term research projects add custom tracking and features. The project ends or the feature doesn't launch. The instrumentation stays — consuming compute, adding latency, polluting schemas, confusing future engineers. Nobody removes it because nobody owns it.

## Callout (philosophy box)

**The core problem:** Adding a feature to a pipeline takes 1 day. Removing it requires: understanding all downstream dependencies, confirming nothing else uses it, getting approval from the pipeline owner, and accepting risk that removal breaks something silently. The asymmetry (easy to add, hard to remove) means features accumulate monotonically. After 3 years: 40% of pipeline compute serves dead features.

## The "Temporary" Addition That Becomes Permanent

**Added for Q3 Research → Still Running 2 Years Later**

- **The pattern:** "I need scroll-depth tracking for my personalization experiment." Added in 2 days.
- **The verdict:** Experiment concluded scroll depth isn't predictive, yet the event was never turned off.
- **The volume:** It still fires on every page load, 50M times/day, and the rows are stored forever.
- **Why nobody removes it:** The person who added it moved teams, and the history left with them.
- **The new owner:** The current pipeline owner sees the event and does not know where it came from.
- **The safe choice:** "Maybe something depends on it." Leaves it — safe today, expensive long-term.
- **The compound effect:** 10 researchers/year × 3 custom features each × 3 years = 90 abandoned features.
- **The combined bill:** 5 hours daily compute, 200GB/day storage, 40ms latency from dead instrumentation.

### Visualization (canvas `c1`, 720×260)

Horizontal 5-step lifecycle timeline of equal-width outlined boxes on a light gray `#f9f9f9` background.

- **Title (bold 13px `#1a5276`, top center):** "Feature lifecycle: added quickly, never removed".
- **Layout:** margins left/right 30, top 40, bottom 30; step width = (width − 60)/5; each box drawn at y = 70 (margin.top + 30), height 140, inset 4px each side; fill is the step color at 15% alpha, stroke is the step color at width 2. Between consecutive boxes a short gray `#999` connector line (width 2) at box mid-height.
- **Steps** (multi-line bold 11px label in step color, centered; small 10px `#555` time/cost caption near box bottom at y offset +125):
  1. "Day 1: / Add feature" — green `#27ae60`, caption "2 hours"
  2. "Month 3: / Project ends" — orange `#e67e22`, caption "—"
  3. "Month 6: / Creator / leaves team" — orange `#e67e22`, caption "—"
  4. "Year 1: / Still running / \"just in case\"" — red `#e74c3c`, caption "$360 wasted"
  5. "Year 3: / Nobody knows / what it does" — dark red `#922b21`, caption "$1,080 wasted"
- **Caption (bottom center, italic 12px `#555`):** "Adding: 2 hours. Removing: "too risky." Net: features only accumulate."

## Schema Pollution — Dead Columns That Confuse Everyone

**200 Columns in the Feature Store. 80 Are From Dead Projects.**

- **The confusion:** New hire sees `exp_q3_2024_scroll_v2`, `tmp_user_affinity_test`, `model_candidate_b_score`.
- **No way to tell:** Are these active? Deprecated? Safe to use? No documentation, and the creator is gone.
- **The accidental usage:** AutoML picks up a dead feature and trains a production model on it.
- **Why it got picked:** It happens to correlate with the target, since it was computed from related data.
- **The silent break:** The feature stops updating 6 months later when its compute is decommissioned.
- **The degradation:** The model quietly loses accuracy because a key input has gone stale, not missing.
- **The cleanup cost:** Removing 1 dead column means tracing every downstream consumer of it.
- **What that means:** Dashboards, models, reports and exports all checked, then zero dependencies confirmed.
- **The arithmetic:** 2-4 hours PER COLUMN, so 80 dead columns = 160-320 hours of cleanup work.
- **Why it never happens:** Nobody has that bandwidth, and the columns stay in the schema by default.

### Visualization (canvas `c2`, 720×260)

Single horizontal stacked bar (100% width bar) split into active vs dead columns, on `#f9f9f9` background.

- **Title (bold 13px `#1a5276`, top center):** "Feature store columns: active vs abandoned".
- **Bar:** full width minus margins (left/right 40), height 50, at y = 70 (margin.top 50 + 20). Left segment: 120/200 = 60% wide, solid green `#27ae60`. Right segment: 80/200 = 40% wide, solid red `#e74c3c`. Whole bar outlined `#333` at width 1.5.
- **In-bar labels (bold 12px white, centered in each segment):** "120 active columns (60%)" and "80 dead columns (40%)".
- **Red note (11px `#e74c3c`, centered, 30px below bar):** "Dead columns: no owner, no docs, unknown dependencies. Too risky to remove, too costly to keep."
- **Caption (bottom center, italic 12px `#555`):** "New ML engineer: "Which columns can I use?" Answer: "Nobody knows for sure.""

## Latency and Cost Creep — Death by a Thousand Cuts

**Each Addition Is Small. Together They're Crippling.**

- **Per-feature cost:** 2ms latency, 10min compute/day, 5GB storage/day, $30/month — negligible alone.
- **50 abandoned features:** 100ms of latency, which users actually notice on every request.
- **Resource total:** 8 hours compute/day (entire cluster capacity) and 250GB/day storage at $170/month.
- **Monthly bill:** $1,500/month all in, which is no longer a rounding error on anyone's budget.
- **The invisible tax:** The pipeline takes 12 hours to run and stakeholders complain that it is slow.
- **Where it goes:** 40% of that runtime computes features that no downstream consumer ever reads.
- **Unattributable:** Nobody knows WHICH 40%, so you cannot speed it up without auditing everything.
- **The irony:** The team requests more compute budget because "the pipeline is too slow." Gets approved.
- **The real fix:** Remove the dead features — effort nobody is incentivized to spend.
- **The incentive gap:** No one gets promoted for deleting code, so the cheap fix stays undone.

### Visualization (canvas `c3`, 720×260)

Stepped area/line chart of cumulative monthly cost over 36 months, on `#f9f9f9` background.

- **Title (bold 13px `#1a5276`, top center):** "Cumulative cost of abandoned features over time".
- **Axes:** L-shaped axes (`#333`, width 1.5); margins left 60, right 30, top 45, bottom 40.
- **Data:** 36 monthly points; cost starts at $0 and increases by $30/month at every 3rd month (each quarter another $30/mo added), producing a staircase reaching $330/month at month 36. Y scale max = 400.
- **Series:** stepped red line `#e74c3c` width 2.5 with area fill `rgba(231,76,60,0.2)` down to the baseline.
- **End label (bold 11px `#e74c3c`, right-aligned at top right of plot):** "$330/month" (formatted as "$" + final cost value + "/month").
- **X labels (10px `#555`):** "Year 1" at 17% of plot width, "Year 2" at 50%, "Year 3" at 83%, just below the axis.
- **Caption (bottom center, italic 12px `#555`):** "Staircase growth: each abandoned project adds permanent cost. Never decreases without active cleanup."

## Regeneration instructions

- **Layout:** detail page. h1 + `.subtitle` + `.philosophy` callout, then one `h2` per section, each followed by a `.obj-table` (full-width table, single `<tr>`): left `<td>` (40%) holds `.obj-title` div + `<ul>` of bullets, right `<td>` (60%, centered) holds the canvas.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border, padding-bottom 8px; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark red `#922b21`, gray text `#555`/`#333`; chart backgrounds `#f9f9f9`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
