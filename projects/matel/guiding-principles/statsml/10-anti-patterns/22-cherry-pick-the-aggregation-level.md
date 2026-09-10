# Cherry-Pick the Aggregation Level

**Page type:** detail page (anti-pattern-pairs two-section layout: one `.card-section` per pattern, each with a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** Cherry-Pick the Aggregation Level

**Subtitle:** Same metric, same data — per-user +2%, per-session +12%, per-click +40%

## The Anti-Pattern

Pick the aggregation level that makes your case. All numbers are technically correct. Per-click inflated by power users.

**Key point (red-left-border callout):** Domain examples:

- Growth reviews
- Engagement dashboards
- Funnel analysis

### Visualization (canvas `c1`, 720×300)

Vertical bar chart of the same metric at three aggregation levels, with the largest one flagged as cherry-picked.

- **Title (bold 14px `#2c3e50`, centered, y=25):** "Same metric at different aggregation levels".
- **Bars (100px wide, 80px gaps, group centered; baseline at h−50; scale max 50):** Per-User +2% in `#1a5276`; Per-Session +12% in `#1a5276`; Per-Click +40% in `#e67e22`.
- **Labels:** value labels "+2%", "+12%", "+40%" in bold 13px `#2c3e50` above each bar; category labels ("Per-User", "Per-Session", "Per-Click") in 12px `#555` below the baseline.
- **Annotation:** vertical 2px red (`#e74c3c`) arrow with filled downward arrowhead pointing at the top of the Per-Click bar; above it, bold 13px `#e74c3c` label: "cherry-picked!".
- **Baseline:** thin `#ccc` line extending 20px past the bar group on each side.

## The Design Pattern

Let statistical assumptions (independence) choose the level. Declare upfront. Report discrepancy as concentration.

**Key point (red-left-border callout):** Steps:

- Identify the independent unit of observation
- Declare aggregation level before analysis
- Report all levels — flag discrepancies
- Attribute divergence to concentration (power users)

### Visualization (canvas `c2`, 720×300)

Same three bars, left-aligned and smaller, with the correct unit highlighted green and an explanation block on the right.

- **Title (bold 14px `#2c3e50`, left-aligned at x=40, y=25):** "Independence determines the correct unit".
- **Bars (80px wide, 40px gaps, starting at x=40; baseline at h−50; scale max 50):** Per-User +2% in solid `#27ae60` with a 2px `#27ae60` border (highlighted); Per-Session +12% and Per-Click +40% in muted `rgba(26,82,118,0.35)`.
- **Value labels:** bold 12px above each bar — `#27ae60` for the highlighted bar, `#999` for the others. Category labels in 11px `#555` below the baseline.
- **Under the Per-User bar:** bold 11px `#27ae60` "correct unit", then 10px "(independent observations)".
- **Explanation block (right of the bars, starting ~50px past the bar group, from y=70):** heading "Explanation:" in bold 12px `#1a5276`; then 12px `#2c3e50` lines "Per-click +40% but" / "3 power users drove"; then bold 12px `#e74c3c` "80% of new clicks"; then 11px `#666` lines "Report as concentration," / "not as overall lift.".
- **Baseline:** thin `#ccc` line extending 10px past the bar group on each side.

## Regeneration instructions

- **Template/layout:** anti-pattern-pairs detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, then two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"). Each section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` (width 100%, border-collapse) with one row: `td.text-col` (45%) holding a paragraph, a `.key-point` callout used as the "Domain examples:" / "Steps:" lead-in, and a `<ul>`; `td.viz-col` (55%) holding the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem. Canvas elements `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic size 720×300 via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, muted bar fill `rgba(26,82,118,0.35)`, gray text `#555`/`#666`/`#999`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
