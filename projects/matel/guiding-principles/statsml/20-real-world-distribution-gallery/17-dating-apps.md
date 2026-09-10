# Dating Apps — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, main histogram canvas center 31%, insight canvas right 31%, one table per section)
**HTML title tag:** Dating Apps — Distribution Patterns

## Like Rate (Two Barely-Overlapping Populations)

**Label:** ASYMMETRIC BIMODAL (color `#795548`)

Simulated: men's like rate is a broad distribution centered near 50%; women's is a tight spike around 4-10%. The two shapes barely overlap. One explanation is a feedback loop — a flood of incoming likes pushes selectivity up on one side, which pushes swipe volume up on the other. Whatever the cause, mutual-match probability is capped by the product of the two rates.

- Men: broad, centered ~50% (low selectivity)
- Women: tight spike at 4-10% (high selectivity)
- One explanation: a selectivity feedback loop
- Mutual match rate ≈ product of the two like rates

### Visualization (canvas `canvas1`, 420×340)

Overlaid dual histogram of simulated like rates by gender.

- **Data:** seeded RNG mulberry32(101); 2000 draws each. Men: normal, mean 50, sd 15, clipped to [0,100]. Women: normal, mean 7, sd 3, clipped to [0,100].
- **Bins/axes:** 50 bins over x 0–100; x ticks at 6 evenly spaced values; y "Frequency" with 4 tick gridlines (dashed `#ddd`); axes `#666`; margins top 40 / right 20 / bottom 50 / left 55; white background.
- **Title (bold 13px, `#1a5276`):** "Swipe Right Rate (%) — Men vs Women". **X label:** "Like Rate (%)".
- **Series:** Men bars fill `rgba(41,128,185,0.35)`, stroke `#2980b9`, legend "Men (broad, high)"; Women bars fill `rgba(231,76,60,0.4)`, stroke `#e74c3c`, legend "Women (tight, low)". Legend swatches (14px squares) at top-left of plot, 150px apart.

### Visualization (canvas `canvas1b`, 400×340)

Funnel chart of mutual-match probability.

- **Title (bold 13px, `#1a5276`, centered):** "Mutual Match Probability by Selectivity".
- **Stages (label, pct, fill):** "Men Swipe Right" 50% `rgba(41,128,185,0.7)`; "Women Swipe Right" 7% `rgba(231,76,60,0.7)`; "Mutual Match" 3.5% `rgba(142,68,173,0.75)`; "Both Message" 1.2% `rgba(39,174,96,0.75)`; "Meet IRL" 0.2% `rgba(230,126,34,0.8)`.
- **Geometry:** trapezoid per stage, centered horizontally; top width = (pct/50) × 90% of plot width, bottom width = next stage's width (last stage tapers to half its own width); `#333` 1px outline; margins top 38 / bottom 30 / left 30 / right 30.
- **Labels:** "Label (pct%)" — white bold 11px centered inside bands with pct ≥ 30; dark `#333` bold 10px left-aligned beside the band (8px right of its edge) for narrower bands, which are unreadable in white.
- **Annotation (bold red `#e74c3c`, left-aligned at ~72% plot width, beside stage 3):** "← 96.5% of pairs" / "   never match".

## Message Response Rate (Looks Like a Cold-Outreach Funnel)

**Label:** ZERO-INFLATED (color `#2980b9`)

85% of first messages get exactly 0 replies; among the rest, reply counts decay geometrically (over half stop after one). The same zero-inflated shape shows up in cold-outreach response data — an unflattering but useful comparison: funnel math fits this shape better than conversation metrics do.

- 85% at exactly 0 = no response ever
- Geometric decay among responses (most = one reply)
- Shape resembles cold-outreach response data
- Funnel math fits this shape better than conversation metrics

### Visualization (canvas `canvas2`, 420×340)

Zero-inflated histogram of response counts.

- **Data:** seeded RNG mulberry32(202); 3000 draws: with p=0.85 push 0 (no response); otherwise geometric count starting at 1, incremented while rng()<0.4, capped at 20.
- **Bins/axes:** 21 bins over x 0–21; same axis style as canvas1.
- **Title:** "Message Response Count — Zero-Inflated". **X label:** "Number of Responses". **Y label:** "Frequency".
- **Bars:** fill `rgba(142,68,173,0.35)`, stroke `#1a5276`.
- **Density overlay:** Gaussian-kernel smoothed bin counts (sigma 1.5 bins, radius 3×sigma), dark-red line `#922b21` width 2, plus 95% SE band `rgba(192,57,43,0.18)` where SE = 1.96·smoothed/√effN, effN clamped to [30, 200].

### Visualization (canvas `canvas2b`, 400×340)

Waterfall bar chart of conversation attrition.

- **Title (bold 13px, `#1a5276`):** "Waterfall: Where Conversations Die".
- **Stages (label, value, fill):** "Matched" 1000 `rgba(41,128,185,0.75)`; "Msg Sent" 700 `rgba(52,152,219,0.75)`; "Got Reply" 105 `rgba(142,68,173,0.7)`; "2+ Replies" 42 `rgba(155,89,182,0.7)`; "Conv >5msg" 17 `rgba(39,174,96,0.75)`; "Date Set" 6 `rgba(230,126,34,0.8)`. Values follow the canvas2 simulation: 15% of messages get a reply (85% zero-inflation), then each further reply continues with p = 0.4.
- **Geometry:** vertical bars scaled to max 1000, `#333` outline; bold 12px `#1a5276` value above each bar; 10px `#333` labels below rotated -0.4 rad; margins top 38 / bottom 55 / left 50 / right 20.
- **Drop annotations:** between consecutive bars, bold red `#e74c3c` "-N%" text (computed drops: -30%, -85%, -60%, -60%, -65%) with a small red downward arrow at the midpoint of the taller bar.
- **Summary (bottom center, bold 12px red `#e74c3c`):** "99.4% attrition: Match → Date".

## Match-to-Message Time (30% Never Message)

**Label:** BIMODAL (ACT VS HOARD) (color `#27ae60`)

Spike within 5 minutes (~40% message almost immediately), rapid decay, then a pile at "never messaged" (~30%). One explanation: some matches are collected as validation with no intent to act. Either way, match ≠ message — headline match counts overstate actual conversations by the size of the never-pile (~30% here).

- Spike at <5 min = ~40% message almost immediately
- ~30% never message (one explanation: validation/hoarding)
- Match ≠ message — intent isn't guaranteed
- Messaging conversion runs ~30% below headline match counts

### Visualization (canvas `canvas3`, 420×340)

Bimodal histogram with a "never" pile at the right edge.

- **Data:** seeded RNG mulberry32(303); 2500 draws: r<0.40 → exponential(λ=0.8) capped at 10 (fast messagers); 0.40≤r<0.70 → 5 + exponential(λ=0.05) capped at 95 (slow decay); r≥0.70 → uniform 95–100 (never messaged pile).
- **Bins/axes:** 50 bins over x 0–100.
- **Title:** "Match-to-Message Time (minutes, \"Never\" = rightmost)". **X label:** "Time (min) — right edge = Never". **Y label:** "Frequency".
- **Bars:** fill `rgba(41,128,185,0.35)`, stroke `#e67e22`. Same density overlay as canvas2 (`#922b21` line, `rgba(192,57,43,0.18)` band).

### Visualization (canvas `canvas3b`, 400×340)

ECDF with annotated plateaus.

- **Title (bold 13px, `#1a5276`):** "ECDF: Cumulative % Who Messaged by Time".
- **Curve:** ECDF of the canvas3 data excluding the "never" pile at 95+ but normalized by the total count, so the curve plateaus at ~70% instead of climbing to 100%; orange `#e67e22` line width 2.5 with a dashed (5/4) horizontal asymptote continuing the plateau to the right edge; area under filled `rgba(230,126,34,0.15)` up to the plateau level.
- **Axes:** x 0–100 with ticks 0, 20, 40, 60, 80, 100 and label "Time (min)"; y 0–100% at 25% steps with dashed `#eee` gridlines; margins top 38 / bottom 50 / left 55 / right 20.
- **Annotations:** short dashed green `#27ae60` line at y=40% with bold label "40% msg in <5min"; full-width dashed red `#e74c3c` line at y=70% with bold centered label above it "CEILING: 30% NEVER message" and a red downward arrow pointing at the gap.

## Conversation Length (Constant Abandon Hazard + Bump)

**Label:** GEOMETRIC + BUMP (color `#e74c3c`)

Geometric decay in the body, plus a bump at 15-25 messages. The geometric body is consistent with a roughly constant per-message abandon probability — as if each message were an independent coin flip rather than accumulating interest. One reading of the bump: conversations that survive to the "let's meet" decision point.

- Geometric body ≈ constant abandon probability per message
- Consistent with abandonment that doesn't decline as chats lengthen
- Bump at 15-25 messages — plausibly the "let's meet" decision point
- Most conversations die within a handful of messages

### Visualization (canvas `canvas4`, 420×340)

Histogram: geometric decay plus a mid-range bump.

- **Data:** seeded RNG mulberry32(404); 3000 draws: r<0.75 → geometric length (each message 35% stop probability, cap 60); 0.75≤r<0.90 → integer uniform 15–25 (bump); r≥0.90 → 25 + exponential(λ=0.05) capped at 60 (long tail).
- **Bins/axes:** 60 bins over x 0–60.
- **Title:** "Conversation Length (messages) — Geometric + \"Let's Meet\" Bump". **X label:** "Number of Messages". **Y label:** "Frequency".
- **Bars:** fill `rgba(230,126,34,0.35)`, stroke `#27ae60`. Same density overlay as canvas2.

### Visualization (canvas `canvas4b`, 400×340)

Survival curve vs pure-geometric reference.

- **Title (bold 13px, `#1a5276`):** "Survival Curve: P(conversation still alive)".
- **Curves:** actual survival (fraction of canvas4 data ≥ each message count 0–60) in green `#27ae60` width 3 with fill under `rgba(39,174,96,0.12)`; dashed gray `#aaa` reference line = pure geometric survival (1−0.35)^msg.
- **Highlight zone:** x 15–25 shaded `rgba(230,126,34,0.2)` with dashed `#e67e22` vertical borders; bold orange labels at top: "\"Let's Meet\"" / "Transition Zone".
- **Axes:** x 0–60 with ticks every 10 and label "Messages Exchanged"; y 0–100% at 25% steps with dashed `#eee` gridlines.
- **Legend (bottom-left):** "— Actual survival" in `#27ae60`; "--- Pure geometric (p=0.35)" in `#aaa`.
- **Annotation (right-aligned, bold 10px `#1a5276`):** "Bump = survivors who" / "transition to real life".

## Attractiveness Elo (Wrong Distribution Assumed)

**Label:** ASSUMPTION MISMATCH (color `#8e44ad`)

An Elo-style rating system implicitly expects a roughly Gaussian engagement distribution. Simulated actual engagement follows a power law — here the top 10% of profiles collect ~60% of all likes (Gini ≈ 0.65). Under that mismatch, updates lose information: rejection by a top-decile profile says almost nothing when nearly everyone is rejected by them.

- Elo-style systems implicitly assume Gaussian engagement
- Simulated likes: power law (top 10% ≈ 60% of likes)
- Mismatch = algorithm learns slowly (wrong update model)
- Rejection from a top profile ≈ no information (near-universal event)

### Visualization (canvas `canvas5`, 420×340)

Overlaid histogram: assumed Gaussian vs actual power law.

- **Data:** seeded RNG mulberry32(505); 3000 draws each. Expected Gaussian: 1500 + randn()·200, kept if in (800, 2200). Actual power law: 800 + 200/rng()^0.35, kept if < 2200.
- **Bins/axes:** 50 bins over x 800–2200.
- **Title:** "Attractiveness Score — Expected Gaussian vs Actual Power Law". **X label:** "Elo Score". **Y label:** "Frequency".
- **Series:** Gaussian fill `rgba(41,128,185,0.3)`, stroke `#2980b9`, legend "Expected (Gaussian)"; power law fill `rgba(231,76,60,0.35)`, stroke `#e74c3c`, legend "Actual (Power Law)".

### Visualization (canvas `canvas5b`, 400×340)

Lorenz curve with Gini annotation.

- **Title (bold 13px, `#1a5276`):** "Lorenz Curve: Like Inequality".
- **Data:** likes per profile as 3000 deterministic Pareto quantiles `(1/u)^(1/1.2)` with `u = (i+0.5)/3000` (Elo scores are ranks and would show almost no inequality, so likes themselves are used; quantile sampling instead of random draws because Pareto tail statistics are seed-fragile — this prints top-10% share ~61% and Gini ~0.65 stably, matching the prose).
- **Curves:** perfect-equality diagonal dashed `#aaa`; actual Lorenz curve red `#e74c3c` width 2.5 with the Gini area between curves filled `rgba(231,76,60,0.15)`; Gaussian-expected Lorenz curve dashed blue `#2980b9` width 2 (from the canvas5 Gaussian data).
- **Axes:** both 0–100% with ticks at 25% steps; x label "Cumulative % of Users (ranked low→high)".
- **Annotations:** bold 14px red "Gini = <computed value>" at ~(65% width, 55% height) with a red arrow pointing toward the gap; bold 11px `#1a5276` top-left "Top 10% get <computed>% of likes".
- **Legend (bottom-left):** "— Actual (power law)" red; "--- Expected (Gaussian)" blue `#2980b9`; "--- Perfect equality" `#aaa` (right of center).

## Regeneration instructions

- **Layout:** one `.obj-table` (full-width, `border-collapse: collapse`) per pitfall, single `<tr>` with three `<td>`s: text 38%, main canvas 31% centered, insight canvas 31% centered. Cell borders `1px solid #2980b9`, padding 12px. Each text cell: `.pitfall-label` span, `<h3>`, one `<p>`, one 4-item `<ul>`.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.obj-table h3` 1.0em weight 700 `#1a5276`; p/li 14px, line-height 1.5–1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px. `canvas { width: 100%; height: auto; }`. No nav bar, no back/home links.
- **Label colors:** assigned by section index from the palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script that sets each `.pitfall-label`'s color.
- **Canvases:** intrinsic `width`/`height` attributes as given (420×340 main, 400×340 insight); scale the backing store by `window.devicePixelRatio`, `ctx.scale` back to logical coordinates. All data generated with seeded mulberry32 RNG (Box-Muller `randn()`, inverse-CDF `randExp(lambda)`) so charts are reproducible.
- **Shared histogram helper:** margins {top 40, right 20, bottom 50, left 55}, white background, bold 13px `#1a5276` title, `#666` axes and tick text, dashed `#ddd` y gridlines, per-bin `fillRect`+`strokeRect` bars; single-dataset mode adds the smoothed density line `#922b21` with `rgba(192,57,43,0.18)` SE band; overlay mode draws multiple datasets with legend swatches instead.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, purple `#8e44ad`, bar fills as rgba values listed per chart.
