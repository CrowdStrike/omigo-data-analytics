# Convergence & Divergence

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Convergence & Divergence

**Subtitle:** Adding infinitely many shrinking numbers sometimes gives a finite total and sometimes doesn't — the term-to-term ratio tells you which

## A Cohort That Pays Forever, a Total That Stops

**Tags:** `core idea` (blue), `infinite sum` (green), `running total` (orange)

- **The cohort** — a subscriber cohort pays $100 in month 1, and churn cuts revenue 20% monthly
- **The payments** — $100, $80, $64, $51.20, ... shrinking forever but never exactly zero
- **Running total** — $100, $180, $244, $295, ... each month adds less than the one before
- **The surprise** — infinitely many payments, yet the total never climbs past $500
- **Converges** — a sum whose running total settles on one finite number is said to converge

*Example (italic):* By month 12 the cohort has paid $466 of its $500 total — the remaining infinity of months adds only $34.

**Key point:** An infinite list of payments can have a finite total. What matters is not how many terms there are, but how fast they shrink.

### Visualization (canvas `c1`, 720×300)

Combo chart: 12 monthly payment bars plus a green running-total line that levels off beneath a dashed $500 ceiling.

- **Title (bold 15px, `#1a5276`, top center):** "Monthly Revenue (bars) and Running Total (line), Churn = 20%/month".
- **Data:** payments `[100, 80, 64, 51.2, 41, 32.8, 26.2, 21, 16.8, 13.4, 10.7, 8.6]`; running totals `[100, 180, 244, 295.2, 336.2, 368.9, 395.1, 416.1, 432.9, 446.3, 457.1, 465.6]`.
- **Axes:** origin x=60, width 610, baseline y=250, chart height 195, scale max 560; 1px `#999` L-shaped axis; right-aligned 12px `#444` y labels "$500" and "$0"; month numbers 1–12 in 11px `#444` centered under each bar; "month" 12px centered at h-8.
- **Ceiling:** magenta `#d55181` dashed (dash 5/4, 1.5px) horizontal line at the $500 level, with bold 12px magenta label "$500 ceiling — never crossed" left-aligned above it.
- **Bars:** 12 slots of width 610/12, fill `rgba(42,120,214,0.45)`, 6px inset each side; the first three bars get bold 12px blue `#2a78d6` value labels "$100", "$80", "$64" above them.
- **Running-total line:** green `#008300` 3px polyline through bar centers with 3.5px green dots; bold 13px green annotation right-aligned near the last point: "running total: $466 by month 12".

## The 0.8 Ratio Locks In a $500 Ceiling

**Tags:** `worked example` (blue), `ratio test` (green)

- **One number** — every month's payment is 0.8 × the month before; that ratio decides everything
- **Geometric total** — first payment ÷ (1 − ratio): 100 ÷ 0.2 = $500 exactly, no calculus needed
- **Self-similar gap** — after any month, the money still to come is 0.8 × the previous gap
- **Ratio test** — if the term-to-term ratio settles below 1, the infinite sum is a finite number
- **Above 1** — a ratio that settles above 1 means terms grow, so the total runs off to infinity

*Example (italic):* After month 1 the money still owed to the $500 jar is $400; after month 2 it is $320 — always ×0.8, never gone but always dying.

**Key point:** The ratio test is just this picture: a gap multiplied by something below 1 every step gets squeezed to nothing, so the filled part must stop at a hard ceiling.

### Visualization (canvas `c2`, 720×300)

Two-part diagram: a horizontal segmented "$500 jar" bar filled by the first 8 geometric payments (top), and a bar series of the shrinking unfilled gap after each month (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "Filling the $500 Jar: Each Payment Takes 20% of What Remains".
- **Jar bar (top):** from x=60, width 610, at y=62, 40px tall, x mapped as value/500 of the width; segments `[100, 80, 64, 51.2, 40.96, 32.77, 26.21, 20.97]` with alternating fills `rgba(42,120,214,0.55)` (even) and `rgba(25,158,112,0.55)` (odd), 1px gap between segments; the first five segments carry white bold 12px centered labels "$100", "$80", "$64", "$51", "$41" (last three unlabeled); remaining gap filled `rgba(107,114,128,0.25)`; whole jar outlined 1px `#999`; grey `#6b7280` bold 12px label "$84 still to come" centered above the gap.
- **Jar captions (12px `#444`, at y = bar bottom + 16):** "$0" left-aligned at the jar's left edge, "$500 = 100 ÷ (1 − 0.8)" right-aligned at its right edge, "months 1–8 shown; the jar edge is the exact geometric total" centered.
- **Gap bars (bottom):** gaps `[500, 400, 320, 256, 204.8, 163.84, 131.07, 104.86, 83.89]` with x labels `['start', '1', '2', '3', '4', '5', '6', '7', '8']` (11px `#444`); baseline y=262, max bar height 100 (scaled by gap/500), 9 slots of width 610/9 with 14px insets, fill `rgba(217,89,38,0.5)`; the first three bars get bold 12px orange `#d95926` value labels "$500", "$400", "$320" above them.
- **Annotation (bold 13px orange, right-aligned above the gap bars):** "unfilled gap after each month: ×0.8 every step → squeezed to nothing".

## The Rival Cohort Whose Total Never Stops

**Tags:** `where it's used` (blue), `divergence` (orange), `failure mode` (red)

- **Slow fade** — a rival cohort pays $100/n in month n: $100, $50, $33.33, $25, $20, ...
- **Terms vanish** — its payments also shrink toward zero, just more slowly than the 0.8 cohort's
- **No ceiling** — the running total passes $500 near month 83 and will pass any number you name
- **Log-slow** — it climbs like a logarithm; reaching $1,000 takes roughly 12,000 months
- **LTV trap** — quoting this cohort's total "lifetime value" is meaningless: the sum is infinite

*Example (italic):* At month 40 the slow-fade cohort has paid only $428 — behind the geometric cohort's $500 — yet it is the one heading to infinity.

**Key point:** Divergence means the running total never settles. It may climb absurdly slowly, but no ceiling exists — any formula that assumes a finite total silently breaks.

### Visualization (canvas `c3`, 720×300)

Two-line chart of running totals over 40 months: the geometric cohort flattening at $500 vs the $100/n (harmonic) cohort still climbing.

- **Title (bold 15px, `#1a5276`, top center):** "Two Running Totals, 40 Months: One Settles, One Never Will".
- **Data (geometric running total, 40 values):** `[100, 180, 244, 295.2, 336.2, 368.9, 395.1, 416.1, 432.9, 446.3, 457.1, 465.6, 472.5, 478.0, 482.4, 485.9, 488.7, 491.0, 492.8, 494.2, 495.4, 496.3, 497.0, 497.6, 498.1, 498.5, 498.8, 499.0, 499.2, 499.4, 499.5, 499.6, 499.7, 499.7, 499.8, 499.8, 499.9, 499.9, 499.9, 499.9]`.
- **Data (harmonic running total, 40 values):** `[100, 150, 183.3, 208.3, 228.3, 245.0, 259.3, 271.8, 282.9, 292.9, 302.0, 310.3, 318.0, 325.2, 331.8, 338.1, 344.0, 349.5, 354.8, 359.8, 364.5, 369.1, 373.4, 377.6, 381.6, 385.4, 389.2, 392.7, 396.2, 399.5, 402.7, 405.9, 408.9, 411.8, 414.7, 417.5, 420.2, 422.8, 425.4, 427.9]`.
- **Axes:** origin x=65, width 605, baseline y=252, chart height 195, scale max 580; 1px `#999` L-shaped axis; right-aligned 12px `#444` y labels "$500", "$250", "$0"; x ticks at months 1, 10, 20, 30, 40 (x = lx + (m−1)/39 of width); "month" 12px centered at h-6.
- **Ceiling:** grey `#6b7280` dashed (dash 5/4, 1.5px) horizontal line at the $500 level (unlabeled).
- **Lines:** geometric total in green `#008300` 3px; harmonic total in magenta `#d55181` 3px; no dots on either.
- **Annotations (bold 13px, right-aligned):** green "×0.8 cohort: converges, flat at $500" just below the ceiling; magenta two lines near the harmonic line's end: "$100/n cohort: $428 at month 40," / "passes $500 near month 83, no ceiling".

## Shrinking to Zero Is Not Enough

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Necessary** — if terms don't shrink to zero, the sum certainly diverges; shrinking is required
- **Not sufficient** — the $100/n payments do shrink to zero, yet their total climbs forever
- **Ratio below 1** — a ratio stuck at 0.80 forces fast decay: payments drop under $1 by month 22
- **Ratio at 1** — the $100/n ratio n/(n+1) creeps up to 1, exactly where the ratio test goes silent
- **Quick check** — before trusting any "total forever" number, ask what the term-to-term ratio does

*Example (italic):* Both cohorts' payments fade to zero; only the one whose ratio stays pinned below 1 has a finite total.

**Common mistake:** "The terms go to zero, so the total must be finite." False — that is necessary, never sufficient. The $100/n sum is the standard counterexample everyone should carry around.

### Visualization (canvas `c4`, 720×300)

Term-to-term ratio chart over 20 months: a flat blue line pinned at 0.80 vs a magenta n/(n+1) curve creeping up toward a dashed red danger line at ratio = 1.

- **Title (bold 15px, `#1a5276`, top center):** "The Ratio Test in One Picture: next payment ÷ this payment".
- **Data (harmonic ratios n/(n+1), 20 values):** `[0.500, 0.667, 0.750, 0.800, 0.833, 0.857, 0.875, 0.889, 0.900, 0.909, 0.917, 0.923, 0.929, 0.933, 0.938, 0.941, 0.944, 0.947, 0.950, 0.952]`; the geometric ratio is a constant 0.8 (drawn as a flat line, no array).
- **Axes:** origin x=65, width 605, baseline y=252, chart height 195, value range 0.4–1.08 (y = baseline − (v − 0.4)/(1.08 − 0.4) × height); 1px `#999` L-shaped axis; right-aligned 12px `#444` y labels "1.0", "0.8", "0.5"; x ticks at months 1, 5, 10, 15, 20 (x = lx + (m−1)/19 of width); "month" 12px centered at h-6.
- **Danger line:** red `#e74c3c` dashed (dash 5/4, 1.5px) horizontal line at ratio 1.0, with bold 12px red label left-aligned above it: "ratio = 1: above this, terms grow; at it, the test goes silent".
- **Geometric ratio line:** blue `#2a78d6` 3px flat horizontal line at 0.8 across the full width, with bold 13px blue label left-aligned below it: "×0.8 cohort: ratio pinned at 0.80 → converges".
- **Harmonic ratio curve:** magenta `#d55181` 3px polyline with 3.5px magenta dots at all 20 points; bold 13px magenta annotation right-aligned (at the y of ratio 0.95, offset +30): "$100/n cohort: ratio n/(n+1) creeps to 1 → diverges".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
