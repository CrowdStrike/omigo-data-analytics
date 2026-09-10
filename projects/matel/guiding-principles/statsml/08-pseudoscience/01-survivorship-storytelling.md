# Survivorship Storytelling

**Page type:** detail page (two-column obj-table layout: text left 40%, canvas right 60%, one table per section)
**HTML title tag:** Survivorship Storytelling — Pseudoscience in Data Analysis

**Subtitle:** Success stories without failure comparison create unfalsifiable narratives

## Section 1: "I Did X and Succeeded, Therefore X Works"

- **The pattern:** A successful person attributes success to a specific behavior. Nobody studies the 1000 people who did the same thing and failed.
- **Fitness influencer:** "I got ripped eating 6 meals a day and doing my special workout." Reality: they're 22 and genetically gifted (high testosterone, favorable muscle insertions, fast metabolism) — the real secret is genetics they can't sell you.
- **Startup founder:** "We succeeded because we pivoted 3 times and never gave up." 10,000 dead startups also pivoted 3 times and never gave up — the story shows only where persistence worked, never where it was delusional.
- **Investor:** "My strategy works — 15 years of beating the market!" You never hear from investors with the same strategy who went bankrupt; survivors credit skill, failures stay silent.

**Why it's pseudoscience:** One observed outcome inferred as a rule. Science requires successes and failures of the same strategy, controlling for confounds (genetics, luck, timing) — without the failure comparison, any story is unfalsifiable.

**The data version:** "Our model uses Feature X and has high accuracy" — but you only tested on datasets where X was available, so feature importance is survivorship-biased toward features that happened to work on this test set.

### Visualization (canvas `c1`, 720×340)

Iceberg-style diagram: successes visible above a "waterline", failures invisible below.

- **Title (bold 17px, `#1a5276`, top center):** "You Only See the Survivors — Never the Failures Who Did the Same Thing".
- **Waterline:** horizontal blue line (`#2980b9`, width 2) at y=120 from x=50 to x=w-50; centered on the line in `#2980b9` 17px: "← visibility line →".
- **Above line:** small upward green triangle (vertices at (300,120), (360,50), (420,120)), fill `rgba(39,174,96,0.4)`; bold green (`#27ae60`) 17px labels centered at x=360: "1 success" (y=80) and "(visible, tells story)" (y=100).
- **Below line:** large downward red triangle (vertices at (200,120), (360,220), (520,120)), fill `rgba(231,76,60,0.3)`; bold red (`#e74c3c`) 17px labels centered at x=360: "1000 failures doing the SAME thing" (y=160) and "(invisible, silent, no conference talk)" (y=180).

### Visualization (canvas `c2`, 720×300)

Funnel diagram of survivorship filtering, four horizontally centered bars narrowing downward, connected by small down arrows.

- **Title (bold 17px Arial, `#1a5276`, top center):** "What Survivorship Bias Looks Like in Data".
- **Stages (label / bar width):** "1000 startups" (600px), "100 survive year 1" (400px), "10 survive year 5" (200px), "1 tells the story" (60px). Bars 28px tall, starting at y=35, step 38px, centered horizontally.
- **Bar colors:** first three fill `rgba(26,82,118,alpha)` with alpha = 0.15+0.2×i, stroke `#1a5276`, label bold 16px `#1a5276`; last bar fill `rgba(231,76,60,0.6)`, label bold 16px `#c0392b`.
- **Connectors:** gray (`#7f8c8d`, width 1.5) vertical arrow between consecutive bars at page center.
- **Annotations:** to the right of the last bar, bold 18px `#c0392b`, left-aligned: "← THIS is who writes the book / gives the talk". Right-aligned near the top bar, 16px `#7f8c8d`: "999 are silent. Same strategy. Same effort. →".

## Section 2: Old Buildings — "They Don't Build Them Like They Used To"

- **The claim:** "Look at this 200-year-old cathedral — craftsmanship was superior back then." People genuinely believe past construction was universally better.
- **What's invisible:** Thousands of same-era buildings that collapsed, rotted, burned, or were demolished because they were poorly built — you can't visit what's gone.
- **The filter:** Only the best-built structures survive 200 years, so you compare today's average (good and bad, all standing) against the past's top 1% (the only ones left).
- **The same bias in code:** "Legacy code was better engineered." The well-engineered legacy code is what's still running; the bad code crashed, was rewritten, or was abandoned.

**The general rule:** Comparing present-all against past-remaining compares an unfiltered set against a filtered one — time is the filter, so the past always looks better.

### Visualization (canvas `c3`, 720×340)

Split panel comparing surviving old buildings vs today's full range, drawn as bar-shaped "buildings" on a common baseline. Margins: left/right 60, top 50, bottom 40.

- **Title (bold 14px, `#1a5276`, top center):** ""They Don't Build Them Like They Used To"".
- **Left panel header (bold 12px `#1a5276`):** "1800s (what remains)". Four green buildings (fill `rgba(39,174,96,0.4)`, stroke `#27ae60` width 1.5), 45px wide, at x = left+30/90/150/210 with heights 100/120/90/110, rising from the plot bottom.
- **Left caption (11px `#27ae60`, centered):** "Only the best survived" / "(looks amazing!)".
- **Divider:** vertical dashed gray line (`#ccc`, dash 4/3) at page midline.
- **Right panel header (bold 12px `#1a5276`):** "2020s (everything visible)". Seven buildings 30px wide at x = mid+30/70/110/150/190/230/270 with heights 120/50/90/40/110/60/100 and qualities good/bad/ok/bad/good/bad/ok. Colors by quality: good fill `rgba(39,174,96,0.4)` stroke `#27ae60`; ok fill `rgba(230,126,34,0.3)` stroke `#e67e22`; bad fill `rgba(231,76,60,0.3)` stroke `#e74c3c`.
- **Right caption (11px `#e74c3c`, centered):** "Full range visible" / "(looks average)".

## Section 3: Music & Art — "The Golden Age Was Better"

- **The claim:** "Music in the 60s/70s was so much better than today's garbage." Every generation says this about the one before.
- **What's invisible:** The 60s produced tens of thousands of terrible songs that were simply forgotten; what survives is the curated top 0.1%.
- **The comparison:** Today's entire output (good, bad, mediocre — all on Spotify) versus the past's greatest hits after 50 years of curation.
- **The mechanism:** Time plus forgetting is an automatic quality filter — the past's average was no better, you just can't access it anymore.

**In data science:** "The classic papers were so much better" — you only read the papers that survived citation filtering; thousands of terrible same-era papers sit uncited in forgotten journals.

### Visualization (canvas `c4`, 720×340)

Two vertical stacked columns comparing what is heard from the 1960s vs today's full catalog. Margins: left/right 50, top 50, bottom 35; each column is 40% of the plot width.

- **Title (bold 14px, `#1a5276`, top center):** ""Music Was Better in the 60s" — What You're Actually Comparing".
- **Left column ("1960s"):** full-height ghosted rectangle fill `rgba(200,200,200,0.3)`; only the top 15% slice highlighted green (fill `rgba(39,174,96,0.5)`, stroke `#27ae60` width 2) labeled "What you hear" (bold 12px `#27ae60`) with "(curated over 60 years)" below in 11px `#999`. Mid-column text in `rgba(231,76,60,0.3)` 11px, three lines: "Forgotten: tens of thousands" / "of terrible songs from the 60s" / "(erased from memory)". Bottom label bold 12px `#1a5276`: "1960s".
- **Right column ("2020s"):** full-height orange rectangle (fill `rgba(230,126,34,0.3)`, stroke `#e67e22` width 2) starting at 60% of the plot width, subdivided into three bands labeled in 11px `#333`: "Great (same % as 60s)" (top 15%, fill `rgba(39,174,96,0.4)`), "Mediocre" (next 40%, fill `rgba(230,126,34,0.2)`), "Bad" (remainder, fill `rgba(231,76,60,0.2)`). Bottom label bold 12px `#1a5276`: "2020s".
- **Center annotations:** "vs" in bold 12px `#e74c3c` at plot center; below it in 11px `#666`: "Top 0.1% vs full catalog" / "= unfair comparison".

## Section 4: Mutual Funds — "Our Fund Beat the Market for 10 Years"

- **The claim:** A fund advertises 10 straight years of market-beating returns — looks like genuine skill.
- **What's invisible:** The industry launched 1,000 funds 10 years ago; the ones that lagged were quietly merged or closed, their track records erased.
- **The math:** With a 50% chance of beating the market each year, 1000 × 0.5^10 ≈ 1 fund beats it all 10 years by pure coin-flip luck — that fund gets advertised, the other 999 are gone.
- **The mechanism:** Fund companies merge underperformers into winners, survivors inherit inflated track records, new investors pile in — then the fund reverts to average, because the "skill" was a statistical artifact.

**The data version:** Train 50 models with different random seeds — the best one on your test set just got lucky on this test set, and publishing only that run is mutual fund survivorship bias in ML.

### Visualization (canvas `c5`, 720×340)

Spaghetti chart of 1,000 simulated fund trajectories over 10 points, with a single winner highlighted. Margins: left/right 50, top 50, bottom 40.

- **Title (bold 14px, `#1a5276`, top center):** "1,000 Funds Started — 10 Years Later, 1 Gets Advertised".
- **Simulation:** 1000 random-walk lines starting at value 100 over 11 points (10 steps), seeded LCG PRNG (seed 99, multiplier 16807 mod 2147483647). Non-winner step: uniform in [-7.7, +6.3] (rng()*14-7.7), floored at value 20; drawn in `rgba(231,76,60,0.03)` width 1. Winner (fund index 423) step: +6 to +11 (6+rng()*5); drawn in `#27ae60` width 3. Y-scale maps value/300 to plot height.
- **Labels:** bold 12px `#27ae60` left-aligned at plot mid-right, near top: "★ "Our fund beat the market 10 years straight!"". Centered 11px `#e74c3c` near bottom: "999 funds closed/merged (track records erased)". Centered 11px `#666` at very bottom: "Start 1,000 coin-flipping funds → 1000 × 0.5¹⁰ ≈ 1 will "beat the market" all 10 years".

## Section 5: War — "Armor the Returning Planes"

- **The story:** WWII: bullet holes on returning bombers cluster on wings and fuselage, so engineers propose armoring those areas.
- **Abraham Wald's insight:** You only see planes that survived — holes mark where a plane can be hit and still fly home, while the untouched engines and cockpit are where hit planes never returned.
- **The inversion:** Armor the places with no bullet holes — those are the fatal zones. Surviving planes show what's survivable, not what's dangerous.
- **Why this is the canonical example:** It shows the core mechanism exactly — analyzing only what came back and ignoring what's missing from your dataset.

**In A/B testing:** "Users who saw the new feature and stayed report high satisfaction" — but users who saw it and left aren't in your survey, so the feature may be driving away the low scorers and inflating the remaining scores.

### Visualization (canvas `c6`, 720×340)

Top-down schematic of a bomber with bullet holes scattered on fuselage/wings but not on engines/cockpit.

- **Title (bold 14px, `#1a5276`, top center):** "WWII Bombers: Where to Armor?".
- **Plane shape:** centered ellipses in fill `rgba(200,210,220,0.3)` with `#999` stroke — fuselage (radii 180×40, horizontal) and wings (radii 50×120, vertical). Engines: two small ellipses (radii 20×15) at ±70px vertically from center, fill `rgba(39,174,96,0.2)`, stroke `#27ae60` width 2. Cockpit: ellipse (radii 25×18) at +160px right of center, same green styling.
- **Bullet holes:** 25 candidate red dots (`rgba(231,76,60,0.7)`, radius 3) placed by seeded PRNG (seed 42, same LCG), scattered over fuselage/wings; any dot falling near the engine or cockpit zones is skipped, so those areas stay clean.
- **Legend (bottom):** left, bold 11px `#e74c3c`: "● Bullet holes on returning planes" with 11px `#666` below: "(survivable zones — do NOT armor here)". Right-aligned, bold 11px `#27ae60`: "□ No holes = planes hit here DIDN'T return" with 11px `#666` below: "(fatal zones — ARMOR HERE)".

## Section 6: Medicine — "My Grandfather Smoked and Lived to 95"

- **The claim:** Anecdote against medical statistics: "Smoking can't be that bad — my grandfather did it for 70 years and was fine."
- **What's invisible:** The grandparents who smoked and died at 55, 60, 65 of lung cancer, COPD, or heart disease — they aren't here to be counterexamples.
- **The math:** Smoking raises mortality risk ~2-3×; it shifts the distribution rather than guaranteeing death, and the surviving outliers are the ones alive to tell the story.
- **The mechanism:** Anecdotes come only from survivors — the dead give no testimony — so all available anecdotes are biased toward "it wasn't that bad."

**In data science:** "We deployed this model 2 years ago and it still works fine!" — said only by teams whose models didn't fail; the teams whose models did fail were fired, cancelled, or went under and aren't around to warn you.

### Visualization (canvas `c7`, 720×340)

Histogram of smoker age-at-death with the rare long-lived outliers highlighted. Margins: left/right 60, top 50, bottom 35.

- **Title (bold 14px, `#1a5276`, top center):** ""My Grandfather Smoked and Lived to 95"".
- **Data:** age bins `[45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100]` with relative frequencies `[2, 5, 12, 20, 25, 18, 10, 5, 2, 0.8, 0.3, 0.1]` (max 25); 12 equal-width bars scaled to 80% of plot height.
- **Bar colors:** ages ≥ 90 (outliers) fill `rgba(39,174,96,0.5)` stroke `#27ae60`; all others fill `rgba(231,76,60,0.3)` stroke `rgba(231,76,60,0.5)`. Age labels below each bar in 10px `#555`.
- **X-axis label (11px `#555`, bottom center):** "Age at death (smokers)".
- **Annotations:** vertical green arrow (`#27ae60`, width 2) pointing down to the age-95 bar; bold 11px `#27ae60` near it: "← This one tells the story". Bold 11px `#e74c3c` over the bulk of the distribution: "These are dead. Can't give anecdotes. →".

## Section 7: Success Mantras — "My 5 Rules for Success"

- **The pattern:** A successful person distills their journey into 5 simple rules — wake at 4am, read 1 book/week, say no to everything, cold showers, journal daily — and the audience copies them expecting the same outcome.
- **What's invisible:** Millions following the exact same rules who are still broke and stuck — the rules were co-present with success, not its cause; success made them publishable.
- **The reverse engineering problem:** The winner identifies habits after winning but can't know which mattered. Cold showers didn't make them a billionaire — right market, right time, right product did — but "be lucky" isn't a monetizable mantra.
- **The industry:** Self-help is a $13B/year business built on "here's what I did" without "here's the 10,000 who did the same and failed" — autobiography mistaken for science.
- **The unfalsifiability:** If you follow the 5 rules and fail, the answer is "you didn't follow them hard enough" — the mantra can never be wrong, only the practitioner.

**The test that would disprove it:** Randomize 10,000 people — half follow the 5 rules exactly — and measure outcomes after 5 years; nobody selling a mantra runs this test, because no effect beyond placebo would expose it as survivorship storytelling.

### Visualization (canvas `c8`, 720×340)

Flow diagram: an input box of rule-followers, an arrow, and an outcome distribution split into a tiny success band and a large failure band. Margins: left/right 50, top 50, bottom 30.

- **Title (bold 14px, `#1a5276`, top center):** ""My 5 Rules for Success" — The Full Experiment".
- **Input box (left, ~30% of plot width, full plot height):** fill `rgba(26,82,118,0.08)`, stroke `#1a5276` width 1.5. Header bold 11px `#1a5276`, two lines: "10,000 people follow" / ""The 5 Rules" exactly". Bulleted rules in 10px `#555`: "Wake up 4am", "Read 1 book/week", "Cold showers", "Journal daily", "Say no to all".
- **Arrow:** horizontal gray arrow (`#666`, width 2, 60px long) at vertical center between input and outcomes.
- **Outcome bands (right):** small green band (22px tall, fill `rgba(39,174,96,0.4)`, stroke `#27ae60` width 2) labeled bold 11px `#27ae60`: "10 — success (writes "My 5 Rules")". Large red band (70% of plot height, fill `rgba(231,76,60,0.2)`, stroke `#e74c3c` width 1) labeled bold 12px `#e74c3c`: "9,990 — same rules, no success" with "(invisible, no book deal)" in 11px `#666` below.
- **Bottom annotation (bold 11px `#1a5276`, centered):** "The rules didn't cause success. Success caused the rules to become visible."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one full-width table per section, each with a single `<tr>`; left `<td>` (40%) holds `.obj-title` + `<ul>` bullets + trailing `<p>` paragraphs, right `<td>` (60%, centered) holds the canvas(es). Section 1 stacks two canvases (`c1`, `c2`) in its right cell; all other sections have one canvas each.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Each chart drawn in its own IIFE. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, dark red `#c0392b`, orange `#e67e22`, grays `#666`/`#555`/`#333`/`#999`/`#7f8c8d`; bar fill `rgba(26,82,118,0.35)` family.
