# Non-Random Assignment as 'A/B Test'

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Non-Random Assignment as A/B Test — A/B Testing Pitfalls

**Subtitle:** Design Flaw — Beta opt-in users get treatment. Self-selected groups ≠ randomized groups.

## Section 1: Self-Selection Masquerading as Randomization

- "We A/B tested the new feature!" How? "Beta users got it." Beta users are early adopters, power users — already more engaged. You tested engaged vs average, not feature vs no-feature.
- "We rolled out to biggest clients first." They have account managers, integrations. They'll succeed with anything.
- "First 1000 users" = time bias (early adopters differ from later ones).
- **Overlap is the giveaway:** opt-ins average 7.3/10 engagement vs 4.4/10 for controls, yet 40% of controls are as engaged as the least-engaged opt-in — the groups differ in composition, not in kind.

**Correct approach:** TRUE random = coin flip per eligible user. No self-selection, no time ordering, no size ordering.

**The tell:** Ask "how were users assigned?" If any mechanism lets users CHOOSE or be SELECTED → observational study, not A/B test.

### Visualization (canvas `c1`, 720×340)

Side-by-side population diagram: a uniform cluster of star-shaped power users (treatment) vs a mixed cloud of controls (control), with a red annotation between them.

- **Determinism:** all values come from a seeded Park-Miller LCG, `var rnd = lcg(20250110)` — never `Math.random()`. Engagement scores are drawn before any positions so the printed means are stable across renders and resizes.
- **Data:** 25 treatment engagement scores uniform on 5.0–9.0 (drawn first), then 40 control engagement scores uniform on 1.0–8.0. With seed 20250110 the means are treatment **7.3/10** and control **4.4/10**, and **16 of 40** controls (**40%**) score at or above the least-engaged opt-in (5.16).
- **Treatment panel (left):** bold 17px `#1a5276` heading centered at (180, 20): "Treatment (Beta opt-in)". A 5-column grid of 25 orange `#e67e22` five-pointed stars (outer radius 8, inner radius 4), starting at (120, 50), x spacing 24, y spacing 30. Below, 16px orange caption centered at (180, 215) printing the computed mean: "(engaged power users) mean 7.3/10".
- **Control panel (right):** bold 17px `#1a5276` heading centered at (520, 20): "Control (everyone else)". 40 circles at seeded positions `x = 420 + rnd()*200`, `y = 40 + rnd()*160`; radius `3 + (score/10)*5` and fill `rgba(26,82,118, 0.2 + (score/10)*0.4)` so both size and opacity encode that user's engagement score rather than being independently random. Below, 16px `#1a5276` caption centered at (520, 215) printing the computed mean: "(mixed engagement) mean 4.4/10".
- **Middle annotation (bold 16px red `#e74c3c`, centered at x=350):** three lines "Not random — you" (y=100), "SELECTED winners" (y=116), "into treatment" (y=132); below them a red arrow (line width 2 with filled triangular head) pointing left from x=310 to x=260 at y=140.
- **Footnote (italic 14px `#666`, centered, y = h−8):** computed at render time from the plotted scores: "16 of 40 controls (40%) are as engaged as the least-engaged opt-in".

## Section 2: Illustrative Example: The Opt-In Beta Mirage

- A large software company offers a new feature as an opt-in beta, and the people who raise their hands are the product's biggest fans — the ones who log in daily and try everything new.
- When the team later compares beta users to everyone else, the beta group looks far more active, but those users were already far more active before the feature even existed, so the feature gets credit for a gap it did not create.
- After launch the 25 volunteers average **8.5 sessions/week** against **3.0** for the other 40 users — a naive gap of **5.5/wk**.
- The same two groups already differed by **5.0/wk** (8.0 vs 3.0) before the feature shipped, so nearly all of that gap pre-dated it.
- Difference-in-differences leaves a true effect of **0.5 sessions/week** — the naive read overstates it about **11×**.
- The honest check is to compare both groups' activity from before the launch: if the gap was already there, the comparison is measuring who volunteered, not what the feature did.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: sessions per week for beta volunteers vs everyone else, before and after launch — the gap pre-exists the feature.

- **Title (bold 17px `#2a2a2a`, centered at x=360, y=28):** "Beta Volunteers Were Already More Active Before the Feature".
- **Determinism:** seeded Park-Miller LCG, `var rnd = lcg(20250302)` — never `Math.random()`.
- **Data generation:** 25 beta volunteers' pre-launch weekly sessions uniform on 5.5–10.0, then 40 non-volunteers uniform on 1.0–5.0. The feature adds a genuine `TRUE_EFFECT = 0.5` to every volunteer and nothing to anyone else; the after-launch series for non-volunteers is a copy of their before series.
- **Computed bar values (all four printed from the group means, not hardcoded):** Before launch — beta **8.0 /wk**, everyone else **3.0 /wk**; After launch — beta **8.5 /wk**, everyone else **3.0 /wk**. Scale: value/10 × 140px height, baseline y=215. The bar labels use `mean.toFixed(1)`, so the printed values are the actual plotted heights.
- **Arithmetic closes:** naive after-launch gap 8.5 − 3.0 = **5.5 /wk**; pre-existing gap 8.0 − 3.0 = **5.0 /wk**; difference-in-differences 5.5 − 5.0 = **0.5 /wk**, exactly the injected effect; the naive read is **11×** the truth.
- **Bars:** width 64, pair gap 16; group centers at x=230 ("Before launch") and x=500 ("After launch"). Beta bars green: fill `rgba(39,174,96,0.35)`, stroke `#27ae60` width 2, bold 15px green computed value labels above. Everyone-else bars blue: fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 2, bold 15px blue computed value labels. Bold 15px `#333` group labels below baseline: "Before launch", "After launch".
- **Gap annotations (bold 14px red `#e74c3c`, at y = baseline+44):** computed from the plotted means — "gap 5.0 /wk" under the before pair (x=230), "gap 5.5 /wk" under the after pair (x=500).
- **Baseline:** thin gray `#999` line from x=80 to x=640 at y=215.
- **Legend (top left, 14px `#333`):** green swatch (16×12, fill `rgba(39,174,96,0.35)`, stroke `#27ae60`) at (120, 48) labeled "Beta volunteers (n=25)"; blue swatch (fill `rgba(26,82,118,0.35)`, stroke `#1a5276`) at (300, 48) labeled "Everyone else (n=40, sessions per week)". Group sizes are printed from the array lengths.
- **Takeaway (bottom center, italic 14px `#666`, computed at render time):** "Naive after-launch gap 5.5 /wk, but 5.0 /wk pre-dated the feature — real effect only 0.5 /wk".

## Regeneration instructions

- **Determinism rule:** no chart on this page may call `Math.random()`. Both draw functions build their data from a local seeded Park-Miller LCG (`lcg(20250110)` for `c1`, `lcg(20250302)` for `c2`), one generator per chart with a distinct fixed seed so the charts are independent and stable. Every statistic printed on a chart or asserted in the prose is computed from the generated arrays at render time.

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; paragraphs 0.95em `#333`; lists 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Immediately after `setup(id)` the script defines the canonical seeded PRNG helper `function lcg(seed)` (Park-Miller, multiplier 16807, modulus 2147483647, returning `s / 2147483647`) plus a `mean(a)` helper used for the computed labels.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#333`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
