# Placebo Effect

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Placebo Effect — A/B Testing Pitfalls

**Subtitle:** Confound — The act of receiving any intervention produces measurable change. You measured belief, not mechanism.

## Section 1: Why It Matters in A/B Testing

- Users who KNOW they're getting a "new feature" behave differently — increased engagement, satisfaction, exploration — regardless of what the feature actually does.
- In medicine this is solved with a placebo arm: patients receive an inert pill that looks identical. The treatment must beat the placebo, not "no treatment."
- In product A/B tests, Control = no change. But "no change" conflates two things: (a) the feature has no effect, and (b) users weren't primed to expect anything. The treatment group may have been told "try our new X" — the announcement itself lifts metrics.

**The trap:** Treatment vs Control = (feature effect + placebo effect) vs nothing. You attribute the full delta to the feature.

### Visualization (canvas `c1`, 720×300)

Decomposition diagram: a single "measured" bar decomposed into placebo and real segments, with a verdict box on the right. Background `#f8f9fa`.

- **Title (bold 15px, centered, `#1a5276`, y=25):** "What you measure vs. what actually happened".
- **Bar 1 (Measured):** at x=80, y=60, 250×60 rounded rect (radius 6), fill `rgba(26,82,118,0.5)`, stroke `#1a5276` width 2; right-aligned label "Measured:" (`#333` bold 13px) left of the bar; centered bold 16px `#1a5276` text inside: "+18% engagement".
- **Arrow between bars:** bold 14px `#e74c3c` centered text "↓  Decomposed  ↓" below bar 1.
- **Bar 2 (Reality):** at x=80, y=160, same 250×60 total, label "Reality:" right-aligned left of bar. Two segments:
  - Placebo segment 70% width: fill `rgba(230,126,34,0.5)`, stroke `#e67e22`; text bold 14px `#e67e22` "+12% placebo" and 12px "(announcement effect)".
  - Feature segment 30% width: fill `rgba(39,174,96,0.5)`, stroke `#27ae60`; text bold 14px `#27ae60` "+6%" and 12px "(real)".
- **Verdict box (right):** rounded rect at (380,60) 310×180, fill `rgba(231,76,60,0.06)`, stroke `#e74c3c` width 2. Centered at x=535: bold 14px `#e74c3c` "Without a placebo arm:"; then 14px `#333` lines "You attribute +18% to the feature", "The feature actually contributed +6%", "The other +12% was users responding", "to attention, not to the mechanism"; then bold 14px `#e67e22` "Overestimate: 3×".
- **Bottom line (bold 14px `#1a5276`, centered, y=H−15):** "Two-arm test (Treatment vs Nothing) cannot separate placebo from mechanism."

## Section 2: Real-World Pattern: Feature Launches with Announcements

- Email: "We've improved your dashboard!" → users engage more for 2 weeks. Was it the improvement or the nudge?
- Onboarding flow: new tooltip shown → completion increases. Was it the content or the mere fact that something appeared?
- Healthcare: patients told "this is a premium treatment" report 30-40% better outcomes even when the treatment is sugar water.

**Correct approach:** Three-arm test: (A) Control — no change, no announcement. (B) Placebo — announcement + cosmetic change only. (C) Treatment — announcement + real feature. True feature effect = C − B.

**The tell:** If B ≈ C and both beat A, the feature has no real effect — the announcement/attention alone drove the lift.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of the three-arm design with bracket annotations. Background `#f8f9fa`.

- **Title (bold 15px `#1a5276`, centered, y=25):** "Three-Arm Design: Isolating the Real Effect".
- **Bars:** start at x=180, max width 400, height 45, gap 20, starting at y=55. Data:
  - "A: Control" / desc "no change" / 0% / stroke `#999`, fill `rgba(150,150,150,0.2)`; inside gray 14px text "baseline".
  - "B: Placebo" / desc "announcement + cosmetic only" / bar 55% of max / stroke `#e67e22`, fill `rgba(230,126,34,0.3)`; value label "+12%" bold 14px in `#e67e22` right of bar.
  - "C: Treatment" / desc "announcement + real feature" / bar 85% of max / stroke `#27ae60`, fill `rgba(39,174,96,0.3)`; value label "+18%" bold 14px in `#27ae60` right of bar.
  - Left of each bar: bold 13px `#333` arm label plus 11px `#666` description, right-aligned.
- **Bracket annotations (right side, x≈460):** orange (`#e67e22`) square bracket spanning bars A–B with bold 13px label "B − A = +12%" and 12px "(placebo effect)"; green (`#27ae60`) bracket at x≈590 spanning bars B–C with bold 13px "C − B = +6%" and 12px "(TRUE feature effect)".
- **Bottom line (bold 14px `#1a5276`, centered, y=H−15):** "True effect = Treatment − Placebo, not Treatment − Control."

## Section 3: Healthcare Origin: Why Placebo Arms Exist

- The human body responds to belief. Pain reduction, immune markers, even tumor shrinkage have been documented in placebo arms.
- A drug that beats "no treatment" by 40% may only beat placebo by 5%. The 35% was the patient's own response to receiving care.
- This is why the FDA requires placebo-controlled trials — not just treatment vs. untreated. The bar is: does the molecule do more than a sugar pill?

**Analogous product test:** Does the algorithm do more than a random recommendation shown with the same UI chrome? Does the ML model outperform a simple heuristic wrapped in the same interface?

### Visualization (canvas `c3`, 720×300)

Two-row bar decomposition: naive comparison vs placebo-controlled. Background `#f8f9fa`.

- **Title (bold 15px `#1a5276`, centered, y=25):** "Why the FDA Requires Placebo-Controlled Trials".
- **Row 1:** left-aligned bold 13px `#333` label "Naive comparison (Drug vs Nothing):" at x=50, y=55. Below it a rounded bar (radius 6) at x=50, width 520×0.8, height 50, fill `rgba(26,82,118,0.4)`, stroke `#1a5276`; centered bold 18px `#1a5276` text: "+40% improvement   →   \"Miracle drug!\"".
- **Row 2:** label "Placebo-controlled (Drug vs Sugar Pill):". Bar split proportionally 35/40 vs 5/40 of the same total width:
  - Placebo segment: fill `rgba(230,126,34,0.4)`, stroke `#e67e22`, centered bold 15px `#e67e22` "+35% (patient belief, care, attention)".
  - Drug segment: fill `rgba(39,174,96,0.5)`, stroke `#27ae60`, centered bold 13px `#27ae60` "+5%".
  - Right of the segments, bold 14px `#e74c3c` left-aligned: "← actual drug contribution".
- **Bottom takeaway (bold 14px `#1a5276`, centered, y=H−15):** "Same principle in product: does your ML model beat a random recommendation in the same UI?"

## Section 4: Illustrative Example: The Update That "Felt Faster"

- A mobile app team shipped an update whose release notes promised the app was "now faster," even though the build contained no performance changes at all.
- Reviews and support tickets quickly filled with users saying the app felt snappier — people experienced the speedup they had been told to expect. (Expectation shaping the experience is exactly the placebo effect.)
- When a genuinely faster version rolled out later with no announcement, hardly anyone commented on speed — the announcement, not the code, had driven the perceived change.

### Visualization (canvas `c4`, 720×320)

Two-bar comparison of perceived speedup by announcement vs real code change. Background `#f8f9fa`.

- **Title (bold 17px `#2a2a2a`, centered, y=28):** "\"Feels Faster\" Follows the Announcement, Not the Code".
- **Subtitle (14px `#666`, centered, y=52):** "share of users who say the app feels faster (illustrative)".
- **Baseline:** thin gray (`#999`) line from x=80 to x=640 at y=235.
- **Bar 1** at x=170, 120 wide, 140 tall: fill `rgba(230,126,34,0.35)`, stroke `#e67e22` width 2; value label "38%" bold 16px `#e67e22` above; captions below (14px `#333`): "Announced \"now faster\"" / "(no real code change)".
- **Bar 2** at x=430, 120 wide, 18 tall: fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 2; value label "4%" bold 16px `#1a5276` above; captions: "Real speedup shipped" / "(silently, no announcement)".
- **Takeaway (15px `#555`, centered, y=H−14):** "Perception tracked the announcement, not the code — the placebo effect in software."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas. Sections 1 and 2 are each their own `.obj-table`; sections 3 and 4 share a third `.obj-table` (two rows).
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `ul` 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
