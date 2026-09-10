# Metric Design Patterns

**Page type:** detail page (two-column obj-table layout: text left 40%, canvas right 60%, one h2 + one-row table per section, closing philosophy callout)
**HTML title tag:** Metric Design Patterns

**Subtitle:** How to design metrics that actually drive good decisions. Checklist, lifecycle, and the meta-principle.

## The Metric Design Checklist

**Obj-title:** 7-Point Metric Design Checklist

- **1. What decision does this metric inform?** If no decision: don't track it.
- **2. Can someone game it trivially?** If yes: add a counter-metric or redesign.
- **3. Does it have a meaningful threshold?** "Above X = healthy, below Y = act." If no threshold: it's informational at best.
- **4. Is it LEADING or lagging?** Leading = time to react. Lagging = postmortem only.
- **5. What's the CI at current sample size?** A metric with CI ±30% is not a metric — it's a guess.
- **6. Who is the audience?** Board: 3 metrics. PM: 10 metrics. Engineer: 50 metrics. Match complexity to consumer.
- **7. What's the COST of being wrong?** Metric says "healthy" but system is degraded: what's the blast radius?

### Visualization (canvas `c7`, 720×240)

Checklist graphic: seven checkbox rows.

- **Title (bold 17px `#1a5276`, top center):** "7-Point Metric Design Checklist".
- **Rows (starting y=45, spacing 27):** checkbox glyph "□" at x=50 — green `#27ae60` for the first four items, blue `#2980b9` for the last three; item text in 17px `#333` at x=75: "1. What decision?", "2. Gameable?", "3. Threshold?", "4. Leading/lagging?", "5. CI width?", "6. Audience?", "7. Cost of wrong?".
- **Bottom line (bold 17px `#e74c3c`, centered):** "If you can't answer all 7: the metric isn't ready to deploy."

## Metric Lifecycle: Create → Validate → Operate → Retire

**Obj-title:** Most organizations only do "Create"

- **Create:** Define precisely. Document: what it measures, how it's computed, who owns it, what the thresholds are, what action to take.
- **Validate:** Does it actually move when the thing it measures changes? Synthetic test: if I break X, does this metric alert? If not: useless.
- **Operate:** Regular review cadence. Alert when threshold crossed. Periodic: "is this metric still relevant?"
- **Retire:** Business changed, metric no longer relevant, replaced by better version. DELETE IT. Dead metrics create noise.

**Result of never retiring:** 500 metrics, 10 useful, 490 noise that slowly erodes trust in all metrics.

### Visualization (canvas `c8`, 720×300)

Four-stage pipeline diagram with arrows.

- **Title (bold 17px `#1a5276`, top center):** "Metric Lifecycle: Create → Validate → Operate → Retire".
- **Stage boxes:** four rounded rects (150×90, radius 6, gap 15, horizontally centered, y=55), fill = stage color at 15% alpha, stroke = stage color width 2; stage name in bold 17px stage color, description in 17px `#555` beneath:
  - Create `#2980b9` — "Define precisely"
  - Validate `#27ae60` — "Does it alert on real issues?"
  - Operate `#e67e22` — "Review cadence + thresholds"
  - Retire `#e74c3c` — "Delete when irrelevant"
- **Arrows:** gray `#999` connector line with small filled arrowhead between consecutive boxes at mid-height (y=100).
- **Bottom line (bold 17px `#e74c3c`, centered):** "Most orgs only do \"Create.\" Never validate, rarely operate, NEVER retire."

## Callout (philosophy box, bottom)

**The meta-principle:** A metric is a LENS on reality — not reality itself. Every lens distorts. The distortion is acceptable if you KNOW what it distorts and have compensating lenses (counter-metrics). A metric without a counter-metric is a one-eyed view. A metric without a threshold is a number without meaning. A metric without an owner is noise with a dashboard.

## Regeneration instructions

- **Layout:** detail page. h1, `.subtitle`, then per section: `<h2>Title</h2>` (h2 1.3em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 6px) followed by a one-row `.obj-table` — left `<td>` (40%) holds `.obj-title`, bullets, and optional result `<p>`; right `<td>` (60%, centered) holds the canvas. `.obj-table tr:nth-child(even) td` background `#fafcfe`. Page closes with a `.philosophy` callout.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`. No nav bar, no back/home links.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Canvas:** intrinsic sizes 720×240 (`c7`) and 720×300 (`c8`); shared `setup(id)` reads the `width`/`height` attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. In-chart text 17px -apple-system. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent `#2980b9`, gray `#555`/`#999`.
