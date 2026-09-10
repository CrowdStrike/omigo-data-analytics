# Attribution Window Misalignment

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Attribution Window Misalignment — A/B Testing Pitfalls

**Subtitle:** Timing Flaw — Conversions measured too early (warm-up contamination) or cut off too soon (maturation truncation).

## Section 1: Warm-Up: Pre-Test Checkouts Attributed to Variants

- User starts checkout Tuesday (pre-test). Test launches Wednesday. User completes Thursday → credited to variant B.
- Previous test's in-flight conversions bleed into the new test's early data.
- Typical contamination window: 3–7 days of residual conversions from prior state.
- Higher-consideration purchases (electronics, travel) have longer tails → worse contamination.

**Fix:** Burn-in period. Discard first 3–7 days. Only count users whose journey started *after* test launch.

**The tell:** Day-1 conversion rate is anomalously high or differs wildly between variants before any real exposure effect.

### Visualization (canvas `c1`, 720×340)

Timeline diagram showing pre-test checkout journeys crossing the launch line into a burn-in zone.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Warm-Up Contamination: Pre-Test Journeys Bleed In".
- **Timeline:** horizontal gray (`#999`, width 2) line from x=80 to x=640 at y=160. Vertical blue (`#1a5276`, width 3) marker at 35% of the span, ±40px tall, with bold 12px label "TEST LAUNCH" below.
- **Journeys:** three dashed red (`#e74c3c`, dash 4/3, width 2) horizontal lines starting before the launch marker and ending after it (start/end at 15%→50% at y=timeline−25, 22%→45% at y−10, 8%→55% at y+15). Each has an orange (`#e67e22`) start dot and a red (`#e74c3c`) end dot (radius 5).
- **Legend (bottom left, 11px):** orange dot + "Checkout started" (`#e67e22`); red dot + "Conversion attributed" (`#e74c3c`).
- **Burn-in zone:** rectangle from launch to 50% of span, ±45px around the timeline, fill `rgba(231,76,60,0.08)`, dashed red outline (dash 3/3), labeled above in bold 11px `#e74c3c`: "BURN-IN (discard)".
- **Clean zone:** from 50% to the right end, fill `rgba(39,174,96,0.08)`, labeled above in bold 11px `#27ae60`: "CLEAN DATA".

## Section 2: Cool-Down: Post-Window Conversions Never Counted

- Test runs 30 days. Results published day 30. But checkout resolution has a 7–10 day tail (fraud holds, BNPL, cart recovery emails).
- Results keep shifting for a week after "final" read. Typically shift upward (unresolved → resolved).
- Variant with longer consideration funnel gets disproportionately penalized.
- If variant B targets higher-AOV users (who deliberate longer), its true lift is systematically undercounted.

**Fix:** Maturation window. Wait 7–10 days after cutoff before reading. Or: only report on users whose full attribution window has closed.

**The tell:** Metrics change >2% after the test is "done." Winning variant flips in the days following the readout.

### Visualization (canvas `c2`, 720×340)

Line chart: two variant conversion-rate lines continuing to rise after the test-end marker, with the winner flipping.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Cool-Down: Conversion Rate Keeps Changing After Test Ends".
- **Axes:** plot area x 80–640, y 50–280; y-axis 0–8% with gridlines (`#eee`) and labels at 0, 2, 4, 6, 8% (10px `#888`); x labels: "Day 28", "Day 30", "Day 32", "Day 34", "Day 37", "Day 40", axis caption "Days after test start" (`#666`).
- **Test end marker:** vertical dashed blue (`#1a5276`, dash 5/3, width 2) line at the "Day 30" position, labeled bold 11px "Test ends" above; plus a 6px-wide orange band `rgba(230,126,34,0.1)` at the same x with bold 11px `#e67e22` label "\"Final\" read: A wins".
- **Variant A line (`#27ae60`, width 3):** data `[4.2, 4.5, 4.9, 5.3, 5.5, 5.6]` across the six day positions.
- **Variant B line (`#e74c3c`, width 3):** data `[3.8, 4.0, 4.6, 5.2, 5.7, 6.1]`.
- **Read markers:** dots (radius 5) on both lines at Day 30 (A at 4.5 green, B at 4.0 red).
- **Legend (top right, 12px):** green "Variant A: 4.5% → 5.6%"; red "Variant B: 4.0% → 6.1%".
- **Annotation (bold 12px `#e74c3c`, near Day 37):** "← Winner flips here".

## Section 3: The Real Calendar Math

- A "30-day test" actually needs ~44 days wall-clock.
- Day 0–7: Burn-in (discard). Day 7–37: Clean measurement. Day 37–44+: Maturation (let conversions resolve).
- Teams that skip this see "unstable results" and lose trust in experimentation.
- Domain-specific: fintech (chargebacks: 45+ days), travel (cancellation window: 14–30 days), subscriptions (trial-to-paid: 7–14 days).

**Correct approach:** Define attribution window upfront in the test design doc. Enforce it in the pipeline — not as a post-hoc analyst decision.

**The tell:** "Can we get results sooner?" pressure from stakeholders leads to premature reads that flip on maturation.

### Visualization (canvas `c3`, 720×340)

Segmented timeline bar (44 days) with a domain-specific maturation table below.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Real Calendar: A \"30-Day Test\" Needs ~44 Days".
- **Timeline bar:** from x=60 to x=660, y=120, height 50, three proportional segments:
  - Burn-in (7/44 of width): fill `rgba(231,76,60,0.2)`, stroke `#e74c3c`, labels "Burn-in" (bold 12px) / "(discard)" (10px).
  - Clean measurement (30/44): fill `rgba(39,174,96,0.2)`, stroke `#27ae60`, labels "Clean Measurement Window" (bold 13px) / "30 days of usable data" (10px).
  - Maturation (7/44): fill `rgba(230,126,34,0.2)`, stroke `#e67e22`, labels "Mature" (bold 11px) / "(wait)" (10px).
- **Day markers below the bar (11px `#444`):** "Day 0", "Day 7", "Day 37", "Day 44" at the segment boundaries.
- **Table (from y=220):** header bold 12px `#1a5276` "Domain-Specific Maturation Windows:", then rows (name bold 11px `#1a5276`, window 11px `#e74c3c`, reason 11px `#666`):
  - Ecommerce (general) / 7–10 days / Cart recovery, BNPL resolution
  - Fintech / 45+ days / Chargebacks, dispute resolution
  - Travel / 14–30 days / Cancellation window, rebooking
  - SaaS (trial) / 7–14 days / Trial-to-paid conversion

## Section 4: Real Example: Facebook's 2021 Window Change

- In January 2021, Facebook changed how long it waits before giving an ad credit for a sale, cutting the default from 28 days after a click to 7 days (the attribution window). Nothing changed about the ads or the shoppers — only the counting rule.
- Advertisers' dashboards showed conversions dropping sharply overnight, even though the sales recorded in their own store systems were unchanged. Purchases that happened between day 8 and day 28 after a click simply stopped being credited to the ad.
- This revealed that a large share of the reported "performance" was manufactured by the width of the window, not by the ads — so any A/B test whose metric depends on such a window inherits that same arbitrariness.

### Visualization (canvas `c4`, 720×300)

Two-bar before/after comparison with a flat dashed "actual sales" reference line.

- **Title (bold 16px `#2a2a2a`, centered, y=26):** "Facebook 2021: Smaller Window, Smaller Number — Same Sales".
- **Geometry:** baseline y=230, max bar height 150, bar width 130.
- **Bar 1** at x=150, full height: fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 2; captions below (bold + regular 14px `#1a5276`): "Before:" / "28-day click window".
- **Bar 2** at x=440, 62% height: fill `rgba(230,126,34,0.3)`, stroke `#e67e22`; captions (`#e67e22`): "After:" / "7-day click window".
- **Drop arrow:** dashed red (`#e74c3c`, dash 5/3, width 2) line from bar 1 top to bar 2 top with a red triangle head; bold 14px centered label above: "reported conversions drop overnight".
- **Sales line:** dashed green (`#27ae60`, dash 8/5, width 2.5) horizontal line at 85% of max bar height from x=90 to x=640, labeled bold 14px `#27ae60`: "actual sales in the store's own books — flat".
- **Scale note (14px `#666`, centered):** "(illustrative scale)".
- **Bottom line (15px `#333`, centered, y=H−12):** "Only the counting rule changed — the metric moved, the reality did not".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas. Sections 1 and 2 are each their own `.obj-table`; sections 3 and 4 share a third `.obj-table` (two rows).
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `ul` 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#333`.
