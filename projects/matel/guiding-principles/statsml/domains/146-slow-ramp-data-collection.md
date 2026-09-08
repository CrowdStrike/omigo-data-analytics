# Slow Feature Ramp & Impact on Data Collection

**Page type:** detail page (h2 section per topic, each with a two-column obj-table: text left 50%, canvas right 50%)
**HTML title tag:** 146. Slow Feature Ramp & Impact on Data Collection

**Subtitle:** Features roll out at 1% → 100% over months, so ramp-phase data comes from a tiny, non-representative slice of users. Decisions made on 5% data get applied to everyone.

## Callout (philosophy box)

**The core problem:** Slow ramps are good engineering but bad data science: a 5% ramp generates 20× less data, and the first-ramped users (power users, specific regions/platforms) are not the full population. Models built during the ramp learn the biased subpopulation.

## Insufficient Sample Size During Ramp — Can't Reach Significance

**5% Ramp = 20× Longer to Reach Statistical Power**

- **The math:** Reaching 10,000 events takes 2 days at 100% ramp, 40 days at 5%, 200 days at 1%.
- **Launched but unmeasurable:** The feature ships and runs for months before any metric can be read.
- **The decision pressure:** Leadership asks "is it working?" just 2 weeks in, with data still 20× short.
- **Two bad options:** Answer "we don't know yet," or make the call on 1/20th of the needed data.
- **Rare events are invisible:** At a 5% ramp a rare event's observed rate is effectively zero.
- **False null result:** The feature looks like it does nothing when the signal is only too sparse to see.
- **The calendar cost:** By the time a 5% ramp has enough data, seasonality and competitors have moved on.
- **Stale answer:** User behavior has also shifted, so the result may describe a world that no longer exists.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart of time to significance by ramp percentage; background `#f9f9f9`; left margin 140, bars 32px tall, 10px gap; scale max 200 days.

- **Title (bold 13px, center, `#1a5276`):** "Time to statistical significance at different ramp percentages"
- **Bars** (row label right-aligned 11px `#1a5276`; value in bold 11px `#333` to the right of each bar):
  - "100% ramp" — 2 days, green `#27ae60`
  - "50% ramp" — 4 days, green `#27ae60`
  - "25% ramp" — 8 days, orange `#e67e22`
  - "10% ramp" — 20 days, orange `#e67e22`
  - "5% ramp" — 40 days, red `#e74c3c`
  - "1% ramp" — 200 days (full width), dark red `#922b21`
- All bars outlined `#333` width 1; bar width proportional to days/200.
- **Caption (bottom center, italic gray 12px):** "Same feature, same metric. 5% ramp = 20× longer to answer \"does it work?\""

## Ramp Population Is Not Representative

**Who Gets Ramped First ≠ Who Uses the Product**

- **Engineering-biased ramp:** Employees first, then one region, then one platform — each a distinct profile.
- **Phases don't transfer:** Phase-1 data therefore says essentially nothing about phase-5 behavior.
- **Power user bias:** Early recipients are more engaged and more bug-tolerant than the casual majority.
- **Models fail late:** Trained on those users, the model degrades on the casual majority it never saw.
- **Regional bias:** US-first data trains a "global" model that is only ever validated on US behavior.
- **Worse abroad:** Quality is lower for EU and APAC users when the ramp finally reaches them.
- **The survivorship problem:** Ramp users who hate the feature churn, so 5% data shows only survivors.
- **Churners appear late:** The would-be churners only enter the data once the ramp hits 100%.

### Visualization (canvas `c2`, 720×300)

Four side-by-side phase boxes; background `#f9f9f9`; boxes 180px tall, evenly divided with 10px gaps, margins 30.

- **Title (bold 13px, center, `#1a5276`):** "Ramp phases serve different populations"
- **Boxes** (each: colored 2px outline, same color at 15% alpha fill; bold 11px phase label, bold 14px percentage, 10px `#333` profile text word-wrapped on comma):
  - "Phase 1: Internal" — 0.1% — "Tech employees, power users" — purple `#8e44ad`
  - "Phase 2: US-iOS" — 5% — "Wealthy, tech-savvy, English" — blue `#3498db`
  - "Phase 3: US-All" — 25% — "US demographics only" — orange `#e67e22`
  - "Phase 4: Global" — 100% — "Full population diversity" — green `#27ae60`
- **Annotations (bold red 11px, centered below boxes):** "↑ Model trained on this data" under Phase 2; "↑ Deployed to this population" under Phase 4.
- **Caption (bottom center, italic gray 12px):** "Model trained on Phase 2 (wealthy US iOS users) deployed to Phase 4 (everyone). Mismatch."

## Model Trained on Ramp Data Fails at Full Scale

**5% Data → Train Model → Deploy to 100% → Model Fails**

- **The cycle:** Train on 2 months of 5% data, the metrics look great, then ramp all the way to 100%.
- **Accuracy drops:** The model had learned the biased subpopulation's patterns, not the product's.
- **Interaction effects:** Network effects, social features, and recommendation diversity all shift with scale.
- **Nothing generalizes:** Behavior measured at a 5% ramp simply does not hold at a 100% ramp.
- **Load-dependent behavior:** At 5% the infrastructure has headroom, so nothing is ever under strain.
- **Unseen degradation:** At 100% latency spikes and caches miss — conditions absent from training data.
- **The irreversibility:** By the time the model fails at 100%, bad predictions have reached every user.
- **Rollback isn't recovery:** Reverting the model restores accuracy but not the user trust already lost.

### Visualization (canvas `c3`, 720×300)

Line chart of model accuracy across ramp stages; background `#f9f9f9`; margins left 50, right 30, top 50, bottom 50; L-shaped axes `#333` width 1.5.

- **Title (bold 13px, center, `#1a5276`):** "Model accuracy: during ramp vs after full deployment"
- **X gridlines** (light `#ddd` verticals with 10px `#666` labels below axis) at fractions/labels: 0 "1%", 0.15 "5%", 0.3 "10%", 0.5 "25%", 0.7 "50%", 0.85 "100%".
- **Accuracy line** (red `#e74c3c`, width 3) through points (x-fraction, accuracy): (0, 0.92), (0.15, 0.91), (0.3, 0.90), (0.5, 0.89), (0.7, 0.85), (0.85, 0.78), (1.0, 0.76); y mapped from 0.70–1.00 over plot height.
- **Y-axis labels** (10px `#333`, right-aligned): "90%" at 1/3 height, "80%" at 2/3 height.
- **Annotations (bold 11px):** green `#27ae60` "92% (on biased 5% population)" near the 5% mark top; red `#e74c3c` "76% (on real full population)" near the 100% mark bottom.
- **Caption (bottom center, italic gray 12px):** "Accuracy looks great during ramp (biased sample). Drops 16 points at full deployment (real population)."

## Feature Interaction — Other Features Also Ramping

**Multiple Features Ramping Simultaneously Create Data Chaos**

- **The combinatorial problem:** Features A, B, and C ramping at once split users into 8 distinct segments.
- **Segments differ:** Each of the 8 has its own data characteristics, so no single sample describes them.
- **Attribution impossible:** A 3% metric lift can't be assigned to A, to B, or to their interaction.
- **Everything is confounded:** Every measurement of one feature is contaminated by the other live ramps.
- **Data contamination:** Feature B's model trains on a mixture of with-A and without-A user behavior.
- **Matches no final state:** That blend corresponds to no configuration the product will ever ship.

### Visualization (canvas `c4`, 720×280)

Single horizontal stacked bar of population segments plus legend; background `#f9f9f9`; bar 50px tall, margins 30.

- **Title (bold 13px, center, `#1a5276`):** "3 features ramping simultaneously = 8 population segments"
- **Segments** (label, share of full width, color; white bold 9px in-segment label if wider than 30px; all outlined `#333` width 0.5):
  - "None" 45% `#ddd`; "A only" 5% `#3498db`; "B only" 15% `#e67e22`; "C only" 20% `#27ae60`; "A+B" 2% `#8e44ad`; "A+C" 4% `#e74c3c`; "B+C" 6% `#f1c40f`; "A+B+C" 3% `#1a5276`
- **Legend:** below the bar, 4 columns × 2 rows of 12px color swatches with 10px `#333` labels "Segment (NN%)" (e.g. "None (45%)").
- **Caption (bottom center, italic red 12px):** "Each segment has different data. Model trained on this mixture represents no final state."

## Regeneration instructions

- **Layout:** h1 + `.subtitle` + `.philosophy` callout, then one `h2` per section (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by a `.obj-table` with one `<tr>`: left `<td>` (40%) holds `.obj-title` + `<ul>` bullets, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; `ul` 0.9em `#333`; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows background `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Each chart fills its background `#f9f9f9`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#3498db`, purple `#8e44ad`, dark red `#922b21`, yellow `#f1c40f`, gray text `#666`/`#555`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions.
