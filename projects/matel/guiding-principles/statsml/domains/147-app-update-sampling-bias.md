# App Update Sampling Bias

**Page type:** detail page (h2 section per topic, each with a two-column obj-table: text left 50%, canvas right 50%)
**HTML title tag:** 147. App Update Sampling Bias

**Subtitle:** New app version released with improved tracking/features. Older users don't update — the current version works fine for them, updating is effort, or they're less tech-savvy. Your "new data" only represents early adopters. The population who doesn't update is systematically different — and invisible.

## Callout (philosophy box)

**The core problem:** App updates are voluntary. Who updates immediately? Tech-savvy users, younger demographics, power users, people with auto-update enabled. Who doesn't? Older users, less engaged users, people in regions with expensive data, users who learned the current UX and don't want change. Your new version's data represents the UPDATERS — a biased subset. Models trained on this data fail for the 40-60% who stay on the old version for weeks or months.

## Updaters ≠ Your User Base

**Who Updates Fast vs Who Updates Never**

- **Fast updaters (day 1-3):** Auto-update enabled, tech enthusiasts, younger, more engaged — ~20-30%.
- **Why they can:** Higher income buys newer devices with more storage, plus curiosity about new features.
- **Gradual updaters (week 1-4):** Moderate engagement, update once prompted, try new things if nudged — ~30-40%.
- **Resistant updaters (month 1-3+):** "If it ain't broke don't fix it" — older demographics, ~30-40%.
- **Their constraints:** Established workflows, limited storage, expensive data plans, distrust of change.
- **The data consequence:** Week 1, 100% of new data comes from fast updaters — 20-30% of the user base.
- **Early adopters only:** Not "how users respond to the new feature"; the majority hasn't seen it yet.
- **The retention illusion:** "New version has 95% day-7 retention!" — but updaters were already the most engaged.
- **Never held broadly:** Force-updated resistant users may retain at 60% — the 95% never held population-wide.

### Visualization (canvas `c1`, 720×300)

Sigmoid adoption-curve area chart; background `#f9f9f9`; margins left 50, right 30, top 50, bottom 50; L-shaped axes `#333` width 1.5.

- **Title (bold 13px, center, `#1a5276`):** "App update adoption over time — who you see vs who exists"
- **Curve:** logistic sigmoid adoption = 1/(1+e^(-8(t-0.4))) for t in [0,1], drawn in `#1a5276` width 3.
- **Areas:** below the curve filled `rgba(39,174,96,0.2)` with bold green 12px label "Updated (visible in new data)" at ~70% width, ~75% height; above the curve filled `rgba(231,76,60,0.1)` with bold red 12px label "Not updated (INVISIBLE)" at ~30% width, ~20% height.
- **Time markers** (10px `#666` below the axis) at fractions: 0.05 "Day 1", 0.2 "Week 1", 0.5 "Month 1", 0.8 "Month 3".
- **Week 1 marker:** vertical dashed orange line (`#e67e22`, dash 4/4, width 2) at x=0.2 with bold orange 10px annotations "← You start making decisions here" and "(only 25% adopted)".
- **Caption (bottom center, italic gray 12px):** "Early data = early adopters only. 40% of users won't update for months."

## "It Works Fine" — The Satisfaction Trap

**Users Who Don't Update Are Satisfied — But You Can't See Them**

- **The paradox:** Users who don't update are often HAPPY — they found a workflow that works for them.
- **Contentment is unlogged:** The update risks breaking it, and their happiness never enters new-version data.
- **The forced-update disaster:** After 3 months you force-update the rest, onto a UI they didn't choose.
- **You caused it:** Churn spikes because you forced change on satisfied users, not because they were at risk.
- **Misattributed blame:** "Those users were already at risk" — but they used the old version happily for months.
- **Older generation specifics:** Learned the current layout through effort, with muscle memory for buttons.
- **Rational, not regressive:** New layout means re-learning everything — protection of invested learning.
- **Concrete case:** A 65-year-old who spent 3 weeks finding the "send" button won't accept aesthetic moves.
- **The invisible majority:** If 40% stay on the old version 2+ months, new tracking gives NO DATA on them.
- **Dashboards see 60%:** Metrics and models rest on the 60% who updated, leaving 40% unobserved.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of reasons for not updating; background `#f9f9f9`; left margin 200, bars 32px tall, 10px gap.

- **Title (bold 13px, center, `#1a5276`):** "Why users don't update (it's rational, not lazy)"
- **Bars** (row label right-aligned 11px `#1a5276`; percentage in bold 11px `#333` right of bar; all outlined `#333` width 1):
  - "Current version works fine" — 35%, green `#27ae60`
  - "Don't want to re-learn UI" — 25%, blue `#3498db`
  - "Insufficient storage" — 15%, orange `#e67e22`
  - "Expensive mobile data" — 12%, purple `#8e44ad`
  - "Distrust (past bad updates)" — 8%, red `#e74c3c`
  - "Didn't notice prompt" — 5%, gray `#999`
- **Caption (bottom center, italic gray 12px):** "These users are systematically different from early updaters. Your data doesn't represent them."

## New Tracking Only in New Version — Historical Comparison Impossible

**Added a New Event in v2.0. Now You Have Zero Baseline.**

- **The scenario:** The new version adds tracking for "time spent reading", untracked in the old version.
- **No yardstick:** One month of data averages 4.2 minutes — good, bad, or improving is unanswerable.
- **The comparison fallacy:** Six weeks later the metric reads 3.8 minutes: "Reading time decreased!"
- **Population shift:** Early data was fast updaters who read more; later data adds low-engagement gradual updaters.
- **Feature impact unmeasurable:** The new version ships a "read later" feature AND the new tracking together.
- **Confounded:** Feature effect and population effect cannot be separated — they moved at the same time.
- **The A/B test gap:** Old vs new A/B testing is impossible since the old version never tracks the metric.
- **Biased within-arm:** Comparing only new-version users compares the subset who chose to update.

### Visualization (canvas `c3`, 720×300)

Declining line chart of the new metric over 12 weeks; background `#f9f9f9`; margins left 50, right 30, top 50, bottom 50; L-shaped axes `#333` width 1.5.

- **Title (bold 13px, center, `#1a5276`):** "New metric added in v2.0 — population shift looks like metric change"
- **Series** (red `#e74c3c` line, width 3), weekly values W1–W12 on a 0–5 scale: `[4.5, 4.4, 4.3, 4.2, 4.0, 3.9, 3.8, 3.7, 3.6, 3.6, 3.5, 3.5]`; x labels 9px `#666` "W1", "W3", "W5", "W7", "W9", "W11" (every other week).
- **Annotations (orange `#e67e22`, 11px, top-left):** "Wk 1-4: early adopters (high engagement)" / "Wk 5-8: gradual updaters join (lower engagement)" / "Wk 9-12: resistant updaters (lowest engagement)".
- **Center-right callout:** bold red 12px "\"Reading time is declining!\"" with green 11px line below: "Reality: population shifted, metric didn't change per-user".
- **Caption (bottom center, italic gray 12px):** "Metric decline = population shift, not behavior change. Simpson's paradox at the version level."

## Model Trained on Updaters, Deployed to Everyone

**Early Adopter Data → Model → Force-Updated Users Experience Poor Predictions**

- **The pipeline:** The new version collects richer features; you train on 2 months of early-adopter data.
- **False confidence:** Accuracy looks great on the population it trained on, so the model ships to ALL users.
- **The distribution shift:** Force-updated users take shorter sessions while still learning the new UI.
- **Unknown features:** Their navigation patterns differ, and they don't know the new features exist yet.
- **Read as anomalies:** A model fit on "comfortable with new UI" users flags these patterns as outliers.
- **Recommendation failure:** It learned "users who spend 5min reading → recommend long articles."
- **Wrong cause:** The force-updated user spent 5min LOST in the new UI, gets long articles, bounces — churn signal.
- **The enterprise variant:** In B2B apps individuals may update, but IT departments batch-deploy months later.
- **Mismatched usage:** Early-adopter models meet shared accounts, admin oversight, compliance constraints.

### Visualization (canvas `c4`, 720×280)

Two horizontal accuracy bars; background `#f9f9f9`; left margin 180, bars 50px tall, 30px gap.

- **Title (bold 13px, center, `#1a5276`):** "Model accuracy: voluntary updaters vs force-updated users"
- **Bar 1** (label right-aligned 11px `#1a5276`, two lines: "Voluntary updaters" / "(trained on this group)"): green `#27ae60`, 92% of track width, centered white bold 13px in-bar text "92% accuracy".
- **Bar 2** (label: "Force-updated users" / "(never in training data)"): red `#e74c3c`, 64% of track width, in-bar text "64% accuracy"; bold red 12px "-28 points" to the right of the bar.
- Both bars outlined `#333` width 1.
- **Caption (bottom center, italic gray 12px):** "Model trained on enthusiastic updaters. Deployed to reluctant users with different behavior. Fails."

## Regeneration instructions

- **Layout:** h1 + `.subtitle` + `.philosophy` callout, then one `h2` per section (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by a `.obj-table` with one `<tr>`: left `<td>` (40%) holds `.obj-title` + `<ul>` bullets, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; `ul` 0.9em `#333`; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows background `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Each chart fills its background `#f9f9f9`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#3498db`, purple `#8e44ad`, gray `#999`, gray text `#666`/`#555`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions.
