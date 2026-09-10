# Ratio Metric Traps

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Ratio Metric Traps — A/B Testing Pitfalls

**Subtitle:** Design Flaw — ARPU up! Because you lost your cheapest customers.

## Section 1: Denominator Composition Change Fakes Improvement

- Treatment: ARPU $50 (1000 users). Control: ARPU $40 (1200 users). "ARPU +25%!" But: treatment LOST 200 low-spenders (averaging $10 each, churned from a worse experience). Survivors are higher-spending.
- Total revenue: Treatment $50,000 vs Control $48,000 — only +4.2%, against a 16.7% customer loss.
- **Decompose the +25%:** the 1000 survivors already averaged $46 in control, so dropping the $10 cohort alone lifts ARPU 15% for free.
- Real spending rose just 8.7% ($46 → $50). And 1.15 × 1.087 = 1.25 — most of the headline is denominator composition, not customers spending more.
- Note the direction: with 17% fewer users, revenue could only rise at all *because* spend genuinely increased. Rising revenue does not excuse the churn.

**Correct approach:** Track BOTH ratio AND absolute totals. Check for differential attrition. Report total revenue not just per-user.

**The tell:** If treatment has fewer users AND higher per-user metric → survivorship in the metric itself.

### Visualization (canvas `c1`, 720×340)

Two user-population panels showing that treatment's higher ARPU comes from losing the low-spend (short) segments. **All data is a hardcoded literal array — no randomness, no `Math.random()`, no seeded PRNG needed here, because the tall/short split *is* the ratio being taught.**

- **Data (literal, dollars of spend per segment; each bar = 50 users):**
  `CONTROL = [52,48,10,55,41,50,38,44,10,50,46,43,57,39,49,10,47,42,51,45,36,10,53,34]` — 24 bars, sum $960, mean exactly **$40.00**.
  Segments at or below `CHURN_CUT = 20` churn in treatment: the four `$10` bars (200 users, mean $10.00).
  `TREATMENT` = the 20 retained segments each plus `LIFT = 4`: sum $1000, mean exactly **$50.00**.
- **Derived quantities, all computed in JS from those arrays at render time (never hardcoded beside the bars):**
  | Quantity | Formula | Value |
  |---|---|---|
  | Control users | 24 bars × 50 | 1200 |
  | Treatment users | 20 bars × 50 | 1000 |
  | Control ARPU | 960 / 24 | $40.00 |
  | Treatment ARPU | 1000 / 20 | $50.00 |
  | Control revenue | 960 × 50 | $48,000 |
  | Treatment revenue | 1000 × 50 | $50,000 |
  | ARPU change | 50/40 − 1 | +25.0% |
  | User change | 1000/1200 − 1 | −16.7% |
  | Revenue change | 50000/48000 − 1 | +4.2% |
  | Composition-only lift | mean(retained $46.00) / $40 − 1 | +15.0% |
  | Real spending lift | $50 / $46 − 1 | +8.7% |
  | Reconciliation | 1.15 × 1.087 | = 1.25 ✓ |
- **Bar scale:** `PX = 0.62` pixels per dollar, shared by all three groups (control, treatment, churned) so heights are directly comparable; bars bottom-aligned in a 40px slot.
- **Control panel (left):** header bold 16px blue `#1a5276` centered at (180, 20): "Control: 1200 users" (count printed from the array length); below it 16px "ARPU = $40.00" at (180, 36). 24 bars (12px wide) in 2 rows of 12 from x=70, y=70, row spacing 60px, x-pitch 18px. Retained segments fill `rgba(26,82,118,0.7)`; the four churning `$10` segments fill the lighter `rgba(26,82,118,0.4)`.
- **Treatment panel (right):** header bold 16px green `#27ae60` centered at (520, 20): "Treatment: 1000 users"; below it "ARPU = $50.00" at (520, 36). The 20 retained segments only (14px wide, fill `rgba(39,174,96,0.7)`) in 2 rows of 10 from x=420, y=70, x-pitch 20px.
- **Departed users:** red 16px label "200 low-spend users LEFT ($10.00 each)" centered at (520, 195) — count and mean both computed. Four red bars (12px wide, height $10 × PX, fill `rgba(231,76,60,0.5)`) from x=480, x-pitch 18px, bottom-aligned at y=216, each overlaid with a red `#e74c3c` X (1.5px strokes) sized to the bar.
- **Middle annotation (16px gray `#666`, centered at x=360):** "Each bar = 50 users, height = their spend." at y=155 and "Short bars LEFT." at y=172.
- **Bottom summary (bold 16px red, centered at (360, 240)):** "ARPU up 25.0%.   Customers down 16.7%.   Revenue up 4.2%." — every percentage formatted from the computed values.
- **Decomposition lines (centered at x=360):** 15px `#333` "Revenue: $48,000.00 → $50,000.00" at y=266 (thousands separators added by the `money()` formatter); 15px `#333` "The 25.0% ARPU gain = 15.0% denominator composition × 8.7% real spending" at y=288; 14px `#666` "(1 + 0.1500) × (1 + 0.0870) = 1.25   — Illustrative Example" at y=308.

## Section 2: Real Example: LinkedIn's Two CTR Answers

- LinkedIn's experimentation team published how they handle metrics like click-through rate, because there are two natural ways to compute it: average each user's personal rate, or divide total clicks by total views across everyone.
- A few heavy users with thousands of views can pull the totals-based number one way while the typical user's experience points the other way, so the two methods can genuinely disagree on the exact same data.
- Their answer was to decide up front which question the test is asking — "did the average user improve?" versus "did the average view improve?" — and to use math that accounts for users contributing unequal amounts of data (the delta method).

### Visualization (canvas `c2`, 720×320)

Left: a three-user mini dataset; right: two bars showing the two CTR computations disagreeing ~6x on the same data. **Data is a hardcoded literal array; both printed rates and the fold-gap are computed from it in JS.**

- **Data (literal):** `USERS = [{A: 1 click / 10 views}, {B: 1 click / 10 views}, {C: 10 clicks / 1000 views, heavy}]`. Totals: 12 clicks, 1020 views. Every user has views > 0, so no per-user rate divides by zero.
- **Derived:** average of per-user rates = (0.10 + 0.10 + 0.01)/3 = **7.0%**; pooled = 12/1020 = **1.2%** (1.176%); fold gap = 7.0/1.176 = **5.9x**.
- **Title (bold 17px `#2a2a2a`, centered at (360, 26)):** "Same Three Users, Two Different CTR Answers".
- **Dataset (left, 15px left-aligned at x=50, rows at y=80/108/136):** each row printed from the array as "User X:  N click(s) / M views  =  P%" with P computed; A and B in `#333`, heavy user C in orange `#e67e22`. Bold orange "(one heavy user)" at y=160. `#333` "Totals:  12 clicks / 1020 views" at y=190, both computed.
- **Bars (right):** baseline thin gray `#999` line from x=370 to x=700 at y=235; axis top = `ceil(perUser)` = 7%, so scale is 145px / 7 percentage points (derived, not a magic constant).
  - Bar 1 at x=400, 110px wide, height = 7.0% × scale: fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 2; label "7.0%" bold 18px blue above (printed from the computed value); caption below in 14px gray `#666`: "average of" / "each user's rate".
  - Bar 2 at x=560, 110px wide, height = 1.2% × scale: fill `rgba(230,126,34,0.35)`, stroke `#e67e22` width 2; label "1.2%" bold 18px orange above; caption: "total clicks ÷" / "total views".
- **Gap note (bold 15px red, centered at (535, 55)):** "5.9x apart on identical data" — the multiple is computed as perUser/pooled, not asserted. (The previous hardcoded "6x" was a rounding of 5.95.)
- **Takeaway (bold 16px red, centered at (360, h−14)):** "Decide which average answers your question BEFORE the test".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- **Determinism:** neither chart may call `Math.random()` — both use hardcoded literal data arrays, so the figure is identical on every load. The page also carries the canonical seeded Park-Miller `lcg(seed)` helper (`s = (s * 16807) % 2147483647`) plus `mean()`/`sum()` helpers immediately after `setup()`, available for any future generated series; a seeded PRNG is preferred over `Math.random()` but a literal array is preferred over both whenever the values carry meaning (here they determine the taught ratio).
- **Computed labels:** every ratio, count, dollar figure and percentage on both canvases is derived in JS from the literal arrays at draw time and formatted for display. Do not reintroduce hardcoded statistics next to the bars.
- **Links:** none on this page; if this spec is linked from a grid, regenerated HTML card links use `.html` extensions.
