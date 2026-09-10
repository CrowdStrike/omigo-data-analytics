# Restaurant & Fast Food Analytics — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left ~38%, histogram canvas middle ~31%, insight canvas right ~31%, one table per section)
**HTML title tag:** Restaurant & Fast Food Analytics — Distribution Patterns

**Subtitle:** Simulated distribution shapes from menu psychology, table timing, and checkout UI

## Menu Engineering — Popularity × Margin

**Label:** MENU QUADRANTS (color `#795548`)

50 simulated menu items classified by popularity and margin into 4 quadrants. One explanation for who lands where: placement on the menu shifts popularity, so quadrant position reflects more than food quality.

- Stars (high pop, high margin) — promote aggressively
- Plowhorses (high pop, low margin) — raise price or reduce cost
- Puzzles (low pop, high margin) — better placement/description
- Dogs (low pop, low margin) — remove or redesign

### Visualization (canvas `canvas1`, 420×340)

Histogram of per-item profit margins — the same 50 items the quadrant scatter plots.

- **Title (bold 13px, `#1a5276`, top center):** "Profit Margin per Menu Item (%)".
- **Data:** 50 menu items with popularity ~ Normal(50, 20) clamped [0,100] and margin ~ Normal(40, 15) clamped [0,100], generated with seeded RNG (mulberry32, seed 42); the histogram shows each item's margin directly (the earlier resample-by-popularity view added nothing the scatter doesn't show).
- **Bins/axes:** 20 bins over x range 0-100; x labels integer + "%"; x-axis label "Profit Margin (%)".
- **Bars:** fill `rgba(121,85,72,0.5)`, border `#795548`; Gaussian-smoothed (sigma 1.5 bins) density line `#1a5276` width 2 with 95% SE band filled `rgba(230,126,34,0.22)`.

### Visualization (canvas `canvas1b`, 400×340)

2D scatter: menu engineering matrix with 4 shaded quadrants.

- **Title (bold 13px, `#795548`, top center):** "Menu Engineering Matrix".
- **Quadrants (split at popularity=50 on x, margin=40 on y, divided by dashed (5/4) `#666` 1.5px lines):**
  - Top-right STARS ★: background `rgba(39,174,96,0.12)`, label bold 12px `#27ae60`.
  - Bottom-right PLOWHORSES: background `rgba(41,128,185,0.12)`, label `#2980b9`.
  - Top-left PUZZLES ?: background `rgba(230,126,34,0.12)`, label `#e67e22`.
  - Bottom-left DOGS ✗: background `rgba(231,76,60,0.12)`, label `#e74c3c`.
- **Points:** the same 50 items as 5px-radius dots colored by their quadrant (`#27ae60` / `#2980b9` / `#e67e22` / `#e74c3c`).
- **Axes:** L-shaped gray `#999` axes; padding top 40, right 20, bottom 45, left 50; axis captions 11px `#555`: "Popularity →" below, rotated "Margin →" on the left.
- **Caption (bold 10px, `#333`, centered, two lines below the plot):** "Menu placement shifts popularity --" / "quadrant is not food quality alone".

## Order Decision Time (Bimodal)

**Label:** TWO DECISION MODES (color `#2980b9`)

Two distinct populations: quick deciders who already know what they want (regulars, habitual orderers) and deliberators who read everything. In this simulation deliberators average $13.30 vs $9.50 — about 40% more per order.

- Quick deciders: mostly < 30s, regulars, habitual
- Deliberators: ~3-5 min, read everything, explore
- Deliberators spend ~40% more ($13.30 vs $9.50, simulated)
- "Average" decision time describes neither group

### Visualization (canvas `canvas2`, 420×340)

Bimodal histogram of order decision time.

- **Title:** "Order Decision Time (seconds)".
- **Data:** 1200 samples Exponential(mean 20 s) capped at 400; 800 samples Normal(mean 210 s, sd 50) kept if > 0, capped at 400.
- **Bins/axes:** 35 bins over x range 0-400; x labels formatted "Ns"; x-axis label "Seconds".
- **Bars:** fill `rgba(41,128,185,0.5)`, border `#2980b9`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas2b`, 400×340)

Two-profile comparison panel.

- **Title (bold 13px, `#2980b9`, top center):** "Two Decision Profiles".
- **Left panel (QUICK DECIDER):** box filled `rgba(41,128,185,0.1)` with `#2980b9` 2px border; clock icon (`#2980b9` 22px-radius circle with hour/minute hands); bold 14px `#2980b9` "QUICK DECIDER"; 12px `#333` lines "< 30 seconds", "Knows what they want", "Regular / habitual"; bold 11px `#2980b9` "60% of orders" / "Avg spend: $9.50".
- **Right panel (DELIBERATOR):** box filled `rgba(230,126,34,0.1)` with `#e67e22` 2px border; menu icon (`#e67e22` 30×38 rectangle with 4 horizontal lines); bold 14px `#e67e22` "DELIBERATOR"; 12px `#333` lines "3-5 minutes", "Reads everything", "Higher spend per visit"; bold 11px `#e67e22` "40% of orders" / "Avg spend: $13.30".
- **Bottom caption (bold 11px, `#c0392b`, centered, two lines):** "→ Deliberators spend ~40% more per order —" / "one read: menus are built to slow you down".

## Table Turnover (Erlang)

**Label:** VARIANCE = REVENUE (color `#27ae60`)

Fast food table times cluster around a one-hour mean; fine dining spreads far wider (sd ~94 vs ~35 min). Predictable turnover — not faster turnover — is what makes revenue plannable.

- Fast food: Erlang(k=3, λ=0.05) → mean 60 min, peak ~40
- Fine dining: Erlang(k=2, λ=0.015) → mean ~133 min, wide spread
- Revenue planning rewards low variance, not low mean
- Predictability enables capacity planning

### Visualization (canvas `canvas3`, 420×340)

Erlang-shaped histogram of fast food table occupancy time.

- **Title:** "Fast Food Table Occupancy Time (minutes)".
- **Data:** 2000 samples of Erlang(k=3, λ=0.05) generated as the sum of 3 exponentials with rate 0.05, capped at 150.
- **Bins/axes:** 30 bins over x range 0-150; x labels formatted "Nm"; x-axis label "Minutes".
- **Bars:** fill `rgba(230,126,34,0.5)`, border `#e67e22`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas3b`, 400×340)

Overlaid analytic Erlang density curves: fast food vs fine dining.

- **Title (bold 13px, `#e67e22`, top center):** "Variance Comparison: Fast Food vs Fine Dining".
- **Curves (Erlang PDF `λ^k · x^(k−1) · e^(−λx) / (k−1)!`, 200 points over 0-250 min, normalized to shared max):**
  - Fast food (QSR): Erlang(k=3, λ=0.05) — stroke `#2980b9` width 3, fill `rgba(41,128,185,0.35)`.
  - Fine dining: Erlang(k=2, λ=0.015) — stroke `#e74c3c` width 3, fill `rgba(231,76,60,0.25)`.
- **Curve labels:** upper left in `#2980b9`: bold 11px "Fast food: tight variance" + 10px "(predictable revenue)"; upper right in `#e74c3c`: bold 11px "Fine dining: high variance" + 10px "(unpredictable)".
- **Peak annotation:** downward `#1a5276` arrow at the QSR mode (x=40 min) with bold 9px two-line label "Peak ~40 min," / "mean 60 min".
- **Near-baseline labels (bold 10px):** left in `#2980b9` "narrow, plannable band"; right in `#e74c3c` "wide, hard-to-plan range".
- **Axes:** L-shaped gray `#999` axes; padding top 40, right 20, bottom 50, left 50; x labels 10px `#555`: "0", "60m", "120m", "180m", plus centered caption "Minutes".

## Combo vs À La Carte (Anchoring Effect)

**Label:** ANCHORING UPSELL (color `#e74c3c`)

À la carte orders cluster at $8.50 while combos cluster at $12 — combo buyers spend ~40% more. Consistent with anchoring: posted "savings" compare against à la carte reference prices, nudging orders up to the bundle.

- À la carte mean: $8.50 (what you actually need)
- Combo mean: $12.00 (bundled with extras)
- +$3.50 for items you didn't plan to buy
- Savings framing: measured against à la carte reference prices

### Visualization (canvas `canvas4`, 420×340)

Bimodal histogram of order totals (à la carte + combo mixed).

- **Title:** "Order Total ($) — Combined Distribution".
- **Data:** 1000 samples Normal(mean 8.5, sd 1.2) and 1200 samples Normal(mean 12, sd 1.0), each kept if in (4,18) — sds tight enough that a real valley separates the à la carte and combo modes.
- **Bins/axes:** 30 bins over x range 4-18; x labels formatted "$N"; x-axis label "Order Total ($)".
- **Bars:** fill `rgba(142,68,173,0.5)`, border `#8e44ad`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas4b`, 400×340)

Before/after anchoring diagram.

- **Title (bold 13px, `#8e44ad`, top center):** "The Anchoring Upsell".
- **Left box ("What you NEED"):** 100px tall, filled `rgba(41,128,185,0.1)` with `#2980b9` 2px border; bold 12px `#2980b9` heading "What you NEED"; 11px `#333` "Burger + Water"; bold 18px `#2980b9` "= $8.50".
- **Right box ("What COMBO gives"):** filled `rgba(142,68,173,0.1)` with `#8e44ad` 2px border; bold 12px `#8e44ad` heading "What COMBO gives"; 11px `#333` "Burger + Fries + Lg Drink"; bold 18px `#8e44ad` "= $12".
- **Connector:** horizontal `#e74c3c` 3px arrow from left box to right box with bold 11px `#e74c3c` label "+$3.50" above it.
- **Statements below (centered):** bold 12px `#c0392b` "Combo buyers spend ~40% more" / "than they would alone"; bold 11px `#e67e22` "+$3.50 for items you didn't plan to buy".
- **Callout box (filled `rgba(230,126,34,0.12)`, `#e67e22` 1.5px border, 11px `#333` two-line text):** "\"Savings\" are computed against" / "inflated à la carte prices".

## Tip Distribution (Trimodal — UI Created)

**Label:** UI-CREATED SHAPE (color `#8e44ad`)

The tip distribution has needle spikes at exactly 15%, 18%, and 20% — the three preset buttons on the checkout screen. The shape tracks the UI rather than a smooth preference curve: in this simulation, moving the presets moves the spikes.

- Spikes at 15%, 18%, 20% = the three buttons
- Zero-tip cluster = takeout orders (no prompt)
- ~3% of users tap "Custom"
- Change defaults → change distribution instantly

### Visualization (canvas `canvas5`, 420×340)

Multi-spike histogram of tip percentages.

- **Title:** "Tip Percentage Distribution".
- **Data:** 500 samples Normal(0, 0.3) (zero-tip takeout); 600 samples Normal(15, 0.6); 800 samples Normal(18, 0.6); 900 samples Normal(20, 0.6); 100 samples uniform 5-25 (custom tips).
- **Bins/axes:** 25 bins over x range 0-30; x labels formatted "N%"; x-axis label "Tip %".
- **Bars:** fill `rgba(39,174,96,0.5)`, border `#27ae60`; density overlay disabled (`density: false`) — smoothing would blur the 15/18/20% needle spikes that are the whole point.

### Visualization (canvas `canvas5b`, 400×340)

iPad tip-screen mockup with arrows from buttons to distribution spikes.

- **Title (bold 13px, `#27ae60`, top center):** "The Buttons ARE the Distribution".
- **iPad frame:** rounded-corner (10px radius) dark `#1a1a1a` tablet outline ~80px tall with a light `#f5f5f5` screen inset; 10px `#333` prompt text "Add a tip?" centered at the top of the screen.
- **Buttons (4 rounded 4px-radius buttons, white bold 11px labels):** "15%", "18%", "20%" in `#27ae60`; "Custom" in gray `#95a5a6`.
- **Arrows:** from each percentage button a vertical `#27ae60` 2px arrow points down ~50px to a bold 12px `#27ae60` label "█ SPIKE" with 10px `#555` sub-label "at 15%" / "at 18%" / "at 20%"; from the Custom button a shorter gray `#95a5a6` line to the 10px label "~3% tap this".
- **Bottom caption (bold 12px, `#c0392b`, centered):** "Change defaults → change distribution instantly".

## Regeneration instructions

- **Layout:** one `.obj-table` per section (full-width, border-collapse), each with a single `<tr>` of three `<td>`s: first 38% (text: `.pitfall-label` span, `<h3>` title, `<p>` paragraph, `<ul>` bullets), second 31% centered (histogram canvas), third 31% centered (insight canvas). Section order as above.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `.subtitle` centered `#666` 0.95em; table cell borders `1px solid #2980b9`, padding 12px; h3 `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px. No nav bar, no back/home links.
- **Pitfall label colors:** assigned by index from the cycling palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script that sets each `.pitfall-label`'s color.
- **Canvases:** intrinsic sizes as given (420×340 histograms, 400×340 insight charts), CSS `width: 100%; height: auto`; every canvas scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Shared histogram helper:** white background, centered bold 13px `#1a5276` title, gray `#999` L axes (margins 35/20/40/50), per-bin bars with 1px gap, Gaussian-smoothed (sigma 1.5 bins) density line `#1a5276` width 2 over a 95% SE band filled `rgba(230,126,34,0.22)` (skipped when `density: false` is passed — used on the tip chart), 6 x-tick labels 11px `#555` with optional 12px `#333` x-axis label. Data generated with seeded mulberry32(42) RNG and Box-Muller normal sampler.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, brown `#795548`, gray `#95a5a6`.
