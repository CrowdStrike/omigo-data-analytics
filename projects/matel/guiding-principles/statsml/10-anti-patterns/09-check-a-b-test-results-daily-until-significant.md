# Check A/B Test Results Daily Until Significant

**Page type:** detail page (anti-pattern/design-pattern pair: two `.card-section` blocks, each a two-column layout table — text left 45%, canvas right 55%)
**HTML title tag:** Check A/B Test Results Daily Until Significant

**Subtitle:** Peeking inflates false positive rate from 5% to 25-50%

## The Anti-Pattern

Check daily, stop when p < 0.05. FPR inflates because you stop on false significance but never stop on "looks bad". Root cause: never computing test power upfront, so you don't know how long to run — you peek because you have no stopping rule.

**Key point (red-left-border callout):** Every peek is a new hypothesis test. With 9 peeks at α=0.05, your true FPR is 25-50%, not 5%. Not calculating power means you can't know when to stop.

*Domain examples:*

- Every A/B test platform
- Product teams under pressure to "call it early"
- Growth teams that never ran a power calculation
- Tests launched without a pre-committed sample size

### Visualization (canvas `c1`, 720×300)

Line chart: daily p-value trajectory that dips below α on Day 6 (where the team stops), with a dashed gray continuation showing what would have happened.

- **Title (bold 13px `#2c3e50`, top center):** "Daily p-value peeking — optional stopping bias".
- **Plot area:** padding left 60, right 30, top 40, bottom 50; background `#fafafa`.
- **Y-axis:** p-value 0 to 1; gridlines and labels at 0.00, 0.05, 0.20, 0.40, 0.60, 0.80, 1.00 in 11px `#666`, gridlines `#ccc`. Rotated y-axis title "p-value" in 11px `#666` on the left.
- **X-axis:** 9 slots labeled "Day 1" … "Day 9" in 11px `#666`, points centered in slots.
- **Alpha line:** horizontal dashed red (`#e74c3c`, dash 6/4, width 2) at p=0.05, labeled "α = 0.05" in bold 11px red just right of the plot.
- **Data:** p-values by day `[0.45, 0.32, 0.18, 0.12, 0.08, 0.03, 0.07, 0.11, 0.09]`; stop index 5 (Day 6, first dip below 0.05).
- **Solid series (Days 1–6):** connected line `#1a5276` width 2.5 with 4px dots; the Day 6 dot is red `#e74c3c`, the others blue `#1a5276`.
- **Dashed continuation (Days 6–9):** gray `#aaa` dashed line (dash 4/4, width 1.5) with 3px gray `#aaa` dots — the p-value climbs back above 0.05.
- **Annotations:** "STOP!" in bold 16px red `#e74c3c` above the Day-6 point with a small filled red downward triangle pointing at it; "FPR inflated: 5% → 25-50%!" in bold 13px red centered near the bottom (h−32).

## The Design Pattern

Fix sample size upfront (power analysis) — evaluate ONCE at the end. OR use sequential testing with always-valid p-values. No color-coding results until test is COMPLETE.

**Key point (green-left-border callout):** Pre-commit to a sample size. Evaluate once. FPR stays exactly at α.

*Steps:*

- Run power analysis before starting
- Fix n per group (e.g., 5000)
- Collect data — no peeking
- Evaluate once at planned endpoint
- OR use sequential methods (always-valid p-values)

### Visualization (canvas `c2`, 720×300)

Diagram: a full green progress bar, a "no peeking zone" box, and a timeline arrow ending at a single EVALUATE checkpoint.

- **Title (bold 13px `#2c3e50`, top center):** "Fixed sample size — evaluate once at the end".
- **Layout paddings:** 40 on all sides.
- **Progress bar:** full-width rounded rectangle (radius 4, height 36) at y = padT+30; background `#ecf0f1` with `#bdc3c7` border, filled completely green `#27ae60` (2px inset, radius 3). Centered white bold 12px label: "n = 5000 per group (pre-committed)".
- **No-peeking zone:** dashed orange box (`#e67e22`, dash 8/5, width 2), inset 60px each side, height 80, 30px below the progress bar; fill `rgba(230, 126, 34, 0.08)`. Contents: "NO PEEKING ZONE" bold 14px `#e67e22` centered; two lock emoji "🔒" (20px) near the left and right edges; subtext "Data collection in progress — results hidden" in 11px `#e67e22`.
- **Timeline:** horizontal `#666` arrow (width 2, filled arrowhead) 30px below the zone, spanning the plot width. "START" marker: 5px `#1a5276` dot near the left with 10px `#666` label below. "EVALUATE" marker: 8px green `#27ae60` circle near the right end with a white checkmark drawn inside and bold 12px green "EVALUATE" label above.
- **Bottom label (bold 13px `#27ae60`, centered at h−12):** "FPR stays at exactly 5%".

## Regeneration instructions

- **Layout:** anti-pattern-pairs detail page. h1 with 2px `#2980b9` bottom border, `.subtitle` paragraph, then two `.card-section` divs ("The Anti-Pattern", "The Design Pattern"). Each section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a full-width `table.layout` with one row — `td.text-col` (45%) holding the paragraph, `.key-point` callout (design-pattern one overrides border to `#27ae60` via inline style), `.example` italic lead-in and `<ul>`; `td.viz-col` (55%) holding one `<canvas width="720" height="300">`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem; table cells padding 12px, vertical-align top; canvas `width:100%`, border `1px solid #e0e0e0`, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`.
- In regenerated HTML, any card links use `.html` extensions.
