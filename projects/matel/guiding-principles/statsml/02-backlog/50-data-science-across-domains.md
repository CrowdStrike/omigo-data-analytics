# Data Science Across Domains

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one `.lang-section` per topic; BACKLOG status badge in h1)
**HTML title tag:** Data Science Across Domains

**Status badge:** BACKLOG (inline in h1)

**Subtitle:** What the work actually looks like per industry — the problems, the constraints, and what differs from textbook ML.

**Intro callout:** The core workflow of data science transfers across industries; the constraint set around it does not. This page frames the shared core, the axes that separate domains, and the per-domain cards to build out.

## 1. The Same Job, Different Constraints

Most of the work is common across domains: defining the problem, cleaning data, engineering features, agreeing on a metric, explaining results. What changes is the constraint set around it.

- The shared part is the bulk of the effort but rarely the reason a project fails.
- Failures cluster in the domain-specific minority — regulatory limits, adversaries, feedback loops, deployment context.
- Technique sophistication tracks business impact weakly. Logistic regression with the right features usually beats a deep model with the wrong ones.

**Key point (red left-border box):** Switching domains is mostly a matter of learning the constraints, not the methods. Roughly months to be productive, longer to have real intuition.

### Visualization (canvas `c1`, 720×300)

Two stacked horizontal proportion bars comparing effort share vs failure share.

- **Title (bold 16px, `#1a5276`, centered at y=24):** "Shared Effort vs Where Projects Fail".
- **Bar geometry:** barX=150, barW=w−260, bar height 52; bar 1 at y=78, bar 2 at y=178. Row labels 13px `#2c3e50`, right-aligned left of bars: "where effort goes" and "where projects fail".
- **Bar 1 (effort):** 80% blue segment (fill `rgba(41,128,185,0.45)`, stroke `#2980b9` width 2) + 20% red segment (fill `rgba(231,76,60,0.45)`, stroke `#e74c3c`). Centered label in blue segment (13px `#1a5276`): "common to every domain".
- **Bar 2 (failure):** 28% blue segment + 72% red segment (same fills/strokes). Centered label in red segment (13px `#c0392b`): "domain constraints".
- **Caption (13px `#4a5866`, centered near bottom):** "the small slice carries most of the risk".

## 2. Axes That Separate Domains

Rather than treating each industry as unique, place it on a few axes. Two domains near each other on these transfer well.

- **Feedback loop speed** — seconds in ads, months in trials, years in climate.
- **Label availability** — abundant clicks, sparse fraud, almost none for novel threats.
- **Adversarial pressure** — none in weather, constant in security and moderation.
- **Explainability** — optional for recommendations, legally required for credit.

**Philosophy callout (blue left-border box):** Adversarial pressure is the axis that most changes the statistics: it makes the data-generating process respond to the model, so past performance stops predicting future performance.

### Visualization (canvas `c2`, 720×330)

Scatter plot of domains on two axes: feedback speed (x) vs adversarial pressure (y).

- **Title (bold 16px, `#1a5276`, centered at y=24):** "Adversarial Pressure vs Feedback Speed".
- **Axes:** L-shaped axes in `#95a5a6` width 1.4; plot area plotX=110, plotY=50, plotW=w−200, plotH=h−116. X label (13px `#4a5866`, centered below): "feedback loop: slow → fast". Y label (rotated, at x=28): "adversarial pressure →".
- **Points (7px radius filled dots, labels 13px `#2c3e50` offset 12px; right-aligned when x-fraction > 0.78):**
  | Domain | x | y | Color |
  |--------|-----|-----|-------|
  | Climate | 0.06 | 0.04 | `#27ae60` |
  | Healthcare | 0.16 | 0.14 | `#27ae60` |
  | Manufacturing | 0.42 | 0.16 | `#2980b9` |
  | Streaming | 0.66 | 0.30 | `#2980b9` |
  | E-commerce | 0.72 | 0.44 | `#e67e22` |
  | Credit | 0.30 | 0.52 | `#e67e22` |
  | AdTech | 0.90 | 0.60 | `#e67e22` |
  | Trading | 0.84 | 0.74 | `#e74c3c` |
  | Moderation | 0.80 | 0.88 | `#e74c3c` |
  | Security | 0.58 | 0.94 | `#e74c3c` |
  (x = fraction of plot width from left; y = fraction of plot height from bottom.)
- **Annotation (13px red `#e74c3c`, left-aligned at top-left inside plot):** "here the data fights back".

## 3. Domains to Cover

Comparison table (`table.compare`, blue header row):

| Domain | Core problems | What makes it different |
|--------|---------------|-------------------------|
| E-commerce | Ranking, recommendation, demand forecasting, pricing, search relevance | Strong feedback loops — the model changes what users see, which changes the data. Cold start, position bias. |
| Finance | Credit scoring, fraud, AML, risk modelling, churn | Explainability is a legal requirement. Fraud near 0.1%. Decisions carry liability. |
| Trading | Signal discovery, execution, microstructure, risk attribution | Non-stationary by nature, tiny signal-to-noise, backtest overfitting is the default failure. |
| Healthcare | Trial analysis, imaging, EHR mining, survival analysis | Small n, high stakes, causal inference matters more than prediction. |
| Cybersecurity | Anomaly detection, threat classification, log analysis | Adversaries adapt to the model. Labels barely exist. Alert fatigue is the real constraint. |
| AdTech | CTR prediction, bidding, attribution, segmentation | Real-time inference at scale, delayed and partial labels, privacy rules removing features. |
| Logistics | ETA, dispatch, surge pricing, routing | Spatio-temporal data, two-sided marketplace coupling, optimisation under uncertainty. |
| Streaming | Recommendation, engagement and churn prediction | Long-term preference versus short-term engagement. Popularity bias. Watch time ≠ satisfaction. |
| Gaming | Matchmaking, churn, economy balancing, bot detection | You control the data-generating process by design — which is both leverage and an ethical line. |
| Climate & energy | Forecasting, grid optimisation, renewable output | Physics-informed models, very long feedback loops, distribution shift is the subject not the bug. |
| Manufacturing / IoT | Predictive maintenance, quality control, sensor fusion | Rare failures, sensor drift, labels need domain experts, edge deployment limits. |
| Social platforms | Ranking, moderation, trend and network analysis | Enormous scale, adversarial creators, cultural context, minutes-not-days latency on harm. |

## 4. Per-Domain Card Structure

- **Real problems** — what teams actually spend time on, not the demo-friendly subset.
- **Data landscape** — what exists, its quality, latency, and labelling situation.
- **Techniques that work** — often simpler than expected.
- **Skills beyond ML** — domain knowledge, regulatory awareness, stakeholder handling.
- **Common mistakes** — what newcomers to this domain specifically get wrong.

## Regeneration instructions

- **Template/layout:** backlog kusto-style detail page. `<h1>` with inline `<span class="status">BACKLOG</span>` badge, `.subtitle` paragraph, `.intro` callout, then four `.lang-section` blocks. Sections 1 and 2 use `table.layout` with one row: left `td.text-col` (45%) with intro paragraph, `<ul>`, and a `.key-point` (section 1) or `.philosophy` (section 2) callout; right `td.viz-col` (55%) with the canvas. Section 3 contains only a `table.compare`; section 4 is a plain `<ul>` (no table, no canvas).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. h2 1.3rem `#1a5276`, 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro` and `.philosophy` background `#f0f4f8`, left border 3px `#2980b9`, 0.9rem. `.key-point` background `#f8f9fa`, left border 3px `#e74c3c`, 0.9rem. `.status` badge: background `#fef9e7`, border 1px `#f39c12`, text `#b7950b`, radius 4px. `table.compare`: th background `#1a5276` white text padding 8px 12px, rows bordered `#eee`, even rows `#f8fafb`, first column bold. `ul` 0.92rem. Canvases `width: 100%`, border 1px `#e0e0e0`, radius 4px.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; also `#2980b9` accent blue.
- **Canvas rendering:** canvases declare intrinsic width/height and are scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; fonts are -apple-system sans-serif.
- Note: in regenerated HTML any card/page links use `.html` extensions (this page has none).
