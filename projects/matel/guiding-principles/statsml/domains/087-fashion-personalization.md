# Fashion Personalization Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text + example callout left 50%, canvas right 50%)
**HTML title tag:** Fashion Personalization Data Pitfalls

**Subtitle:** Fashion recommendation breaks standard ML assumptions: size labels are marketing fiction, 30-40% of purchases come back as returns, and the same person carries several context-dependent style profiles.

## Size Inconsistency Across Brands

**Size Inconsistency Across Brands**

- **The Problem:** Size labels (S, M, L, XL) are marketing decisions, not real measurements.
- **The Spread:** One brand's "Medium" can span 38-42 inches of chest measurement.
- **Why It Breaks ML:** Models learn brand-specific label noise instead of actual body fit.
- **Cross-Brand:** Collaborative filtering across brand size labels becomes meaningless.
- **Data Reality:** The same body wears size S at Zara, M at Gap, and L at H&M.
- **Wrong Variable Type:** Size is a categorical variable masquerading as an ordinal one.
- **Impact:** Cross-brand fit recommendations fail 60-70% of the time.
- **Weak Signal:** Size-based similarity has near-zero predictive power for fit.
- **Root Cause:** Brands use vanity sizing to flatter their target demographic.
- **No Standard:** Apparel sizing is unregulated in most markets.

**Example (callout box):** A customer with 40-inch chest buys "Medium" shirts. Model recommends H&M Medium (actually fits 38-inch) → returns it. Zara Large (fits 40-42) → keeps it. The label "Medium" contained zero information about fit—only chest measurement mattered.

### Visualization (canvas `canvas1`, 720×300)

Heatmap of which chest measurements each brand labels "Medium".

- **Title (bold 17px `#1a5276`, centered, y=25):** "What Each Brand Calls "MEDIUM" (Chest Size)".
- **Grid:** columns = brands Zara, H&M, Nike, Gap, Uniqlo (bold 15px `#1a5276` labels below); rows = chest measurements 36"–42" (15px `#2c3e50` labels left, rotated axis title "Chest Measurement"); margins left 80, top 50, bottom 60, right 100; cell borders `#2980b9` 0.5px.
- **Intensity map** (0 = not medium, 1-5 = intensity of "Medium" label; per brand across 36"–42"):
  - Zara: `[0, 0, 3, 5, 5, 3, 0]` (Medium ≈ 38-41)
  - H&M: `[0, 2, 5, 5, 4, 1, 0]` (Medium ≈ 37-40)
  - Nike: `[0, 0, 1, 4, 5, 5, 3]` (Medium ≈ 39-42)
  - Gap: `[1, 3, 5, 5, 3, 1, 0]` (Medium ≈ 36-39)
  - Uniqlo: `[0, 1, 4, 5, 5, 2, 0]` (Medium ≈ 38-41)
- **Cell fill:** intensity 0 = `#f5f5f5`; otherwise blue shades computed as `rgb(26, 82, 255 − intensity·50)` (darker blue = stronger "Medium"); cells with intensity > 0 show an "M" glyph (white for intensity > 3, else `#1a5276`).
- **Right annotation (red `#e74c3c`, 13-14px):** "Same Customer:" / "40" chest" / "Zara: M ✓" / "H&M: M (tight)" / "Nike: S/M" / "Gap: L"; red 2px arrow from the annotation into the grid.
- **Legend (13px `#2c3e50`, bottom left):** "Dark blue = Labeled "Medium"".

## Return Rate Problem (30-40%)

**Return Rate Problem (30-40%)**

- **The Problem:** Online fashion returns run 30-40%, the highest of any e-commerce category.
- **Bad Label:** "Purchase" is therefore an unreliable positive signal for training.
- **Why It Breaks ML:** Collaborative filtering treats every purchase as a positive signal.
- **Sign Flip:** So 30-40% of those "positives" are really negatives.
- **Category Variance:** Dresses return at 45% (fit-sensitive), shoes 28%, outerwear 26%.
- **Systematic Bias:** Weighting all purchases equally skews the model toward fit-sensitive items.
- **Delayed Feedback:** Returns land 7-14 days later, so live models train on wrong labels for weeks.
- **Unit Economics:** Each return costs $10-15 in reverse logistics.
- **Business Impact:** Optimizing purchase volume with no return penalty actively destroys margin.

**Example (callout box):** User orders 5 dresses (model sees 5 positive signals) but keeps only 1. Model doubles down, recommending 10 similar dresses next time. User returns 9 of 10. The system learned "maximize orders" not "maximize satisfaction"—classic Goodhart's Law.

### Visualization (canvas `canvas2`, 720×300)

Grouped bar chart of ordered vs kept percentages per category with average keep-rate line.

- **Title (bold 17px `#1a5276`, centered, y=25):** "Ordered vs. Actually Kept by Category".
- **Plot area:** margins left 70, right 145, top 50, bottom 60; dark axes `#2c3e50` (width 2); y axis 0%–100% every 10% with `#e0e0e0` gridlines.
- **Bars per category** (ordered bar `#1a5276` always 100%, kept bar `#27ae60`; white 14px value labels inside bar tops; bold 15px `#2c3e50` category labels below):
  - Tops: kept 62%
  - Dresses: kept 55%
  - Pants: kept 68%
  - Shoes: kept 72%
  - Outerwear: kept 74%
- **Average line:** dashed red `#e74c3c` (width 2, dash 5/5) at 66.2%, labeled at right in 14px red: "Avg Keep Rate: 66%".
- **Legend (stacked in the right margin, above the plot):** `#1a5276` swatch "Ordered"; `#27ae60` swatch "Kept" — placed outside the plot area so it never sits over the 100% bars.
- **Annotation (11px red, right margin below the average line):** "30-40% of "positives"" / "are really returns".

## Body Shape ≠ Size Label

**Body Shape ≠ Size Label**

- **The Problem:** Size is one label compressing 5+ dimensions of body measurement.
- **The Dimensions:** Bust, waist, hip, shoulder width, height, and torso length.
- **Why It Breaks ML:** Collaborative filtering assumes same size means similar body and fit taste.
- **Reality:** Pear, apple, and hourglass shapes all wear M but need different cuts.
- **Fit Sensitivity:** Tops track the waist-bust ratio, pants waist-hip-rise, dresses 4+ measurements.
- **Where It Works:** Size matching holds only for oversize and loose fits.
- **Dimensionality Gap:** Bodies vary on 10+ anthropometric dimensions; retail keeps size plus height.
- **User Frustration:** "Size M fits" holds for only 40-50% of a category — the rest fit partially.

**Example (callout box):** Three users all wear Size M tops. User A (athletic build) needs room in shoulders. User B (pear shape) needs room in hips. User C (tall) needs extra length. A recommendation engine treating them as "similar" fails spectacularly—only 1 of 3 will find the recommended top wearable.

### Visualization (canvas `canvas3`, 720×360)

Three body silhouettes labeled SIZE M plus a 5-axis radar chart per body.

- **Title (bold 17px `#1a5276`, centered, y=25):** "Three People, All "Size M" — Completely Different Bodies".
- **Silhouettes** (semi-transparent `#1a5276` fill at 30% alpha with 2px `#1a5276` outline; polygonal torso from shoulder → bust → waist → hip, ~90px tall starting y=50, widths scaled 0.6× the values below; centered at x=120, 360, 600; bold 15px `#1a5276` name below and bold 17px red `#e74c3c` "SIZE M" label under each):
  - Athletic: bust 90, waist 70, hip 85, shoulder 95, height 85
  - Pear Shape: bust 75, waist 65, hip 100, shoulder 70, height 80
  - Apple Shape: bust 95, waist 90, hip 85, shoulder 85, height 75
- **Radar charts** (one per body at y=258, radius 42 — a band that starts below the "SIZE M" labels so the two rows never overlap; axes Bust, Waist, Hip, Shoulder, Height; concentric `#e0e0e0` grid at 3 levels, `#d0d0d0` spokes; values from the table above scaled /100): Athletic filled `rgba(26, 82, 118, 0.3)` stroked `#1a5276`; Pear filled `rgba(231, 76, 60, 0.3)` stroked `#e74c3c`; Apple filled `rgba(39, 174, 96, 0.3)` stroked `#27ae60`. Axis labels (12px `#2c3e50`) drawn only around the middle radar.
- **Bottom annotation (bold 15px red, centered):** "Same Label → Different Measurements → Different Fit Needs".

## Trend Half-Life (Months)

**Trend Half-Life (Months)**

- **The Problem:** Fashion trends carry 2-12 month half-lives, so last season's model pushes passé styles.
- **Why It Breaks ML:** Collaborative filtering assumes preferences are stable over time.
- **Sign Reversal:** What was desirable 6 months ago is often anti-desirable now.
- **Anti-Correlation:** Historical similarity runs directly against current intent.
- **Microtrends:** Short-video-driven trends die within 8-12 weeks of peaking.
- **Slower Cycles:** Seasonal trends last 4-6 months; macro aesthetics cycle over 2-3 years.
- **Cold Start Amplified:** New items have no history, so ranking needs visual and text attributes.
- **The Inventory Trap:** Clearance discounts teach the model "people love last season."

**Example (callout box):** Summer 2025: "Quiet luxury" aesthetic peaks—beige, minimal, expensive-looking. Model trains on this. Fall 2025: Trend shifts to bold colors and maximalism. Model still recommends beige minimalism. Users reject recommendations as "boring" or "dated." 6-month lag = total failure.

### Visualization (canvas `canvas4`, 720×300)

Exponential decay curves of trend relevance with half-life markers.

- **Title (bold 17px `#1a5276`, centered, y=25):** "Fashion Trend Decay Curves (Half-Life Analysis)".
- **Plot area:** margins left 70, right 120, top 50, bottom 60; dark axes; y axis "Trend Relevance" (rotated bold label) 0%–100% every 10%; x axis 0–18 months labeled every 3 ("0mo" … "18mo").
- **Threshold:** dashed `#e0e0e0` horizontal line at 50%, labeled "50% threshold" (13px `#7f8c8d`).
- **Curves** (width 3, `relevance = 100·exp(−ln2/halfLife · month)` over 0–18 months; a 5px dot where each curve crosses 50% plus a dashed vertical drop line in the same color):
  - Oversized Blazers — half-life 8 mo, `#1a5276`
  - Y2K Mini Skirts — half-life 3 mo, `#e74c3c`
  - Quiet Luxury — half-life 12 mo, `#27ae60`
  - Barbiecore Pink — half-life 2 mo, `#e67e22`
- **Legend (right margin, 13px):** line swatch + trend name per row with sub-label "t½=8mo", "t½=3mo", "t½=12mo", "t½=2mo" (11px `#7f8c8d`).
- **Bottom annotation (bold 13px red, centered):** "Historical data actively recommends outdated styles".

## Occasion-Dependent Style

**Occasion-Dependent Style**

- **The Problem:** One person holds 3-5 distinct style modes: work, weekend, evening, athletic, vacation.
- **Meaningless Average:** A single user preference vector blends incompatible contexts.
- **Why It Breaks ML:** Collaborative filtering builds exactly one profile per user.
- **Absurd Output:** It learns "likes suits and hoodies," then recommends smart casual nobody wanted.
- **Context Invisibility:** Purchase logs carry no occasion labels at all.
- **Undecidable:** A cocktail dress plus yoga pants reads "eclectic," not "needs both."
- **Recommendation Averaging:** The centroid of all purchases is often the worst possible pick.
- **What's Needed:** Multi-mode user representations, since users keep 3-4 capsule wardrobes.

**Example (callout box):** User purchases structured blazers (work), band t-shirts (weekend), and cocktail dresses (evening). Model computes average: "semi-formal eclectic." Recommends a "business casual printed blouse." User never wears it—too formal for weekend, too casual for work, wrong for evening. The average of three good styles is a bad style.

### Visualization (canvas `canvas5`, 720×370)

Hub diagram: one user radiating to four occasion style boxes, with an averaged "disaster" recommendation box below.

- **Title (bold 17px `#1a5276`, centered, y=25):** "One User, Multiple Style Modes — Averaging = Disaster".
- **Center:** filled `#1a5276` circle (radius 27) at (width/2, 150) with white bold 12px "USER" label.
- **Occasion boxes** (152×82, placed at four *fixed* centers rather than by polar offset, so no box can fall outside the canvas: Work (112, 92), Weekend (608, 92), Vacation (112, 208), Evening (608, 208); color fill at 12% alpha with 2px colored border; bold 12px colored occasion name, one 10px `#2c3e50` descriptor line joined by "·", three 16×13 swatches with `#999` borders, and a 10px `#7f8c8d` "one wardrobe mode" footer):
  - Work (`#1a5276`) — Structured · Neutral · Formal; swatches `#2c3e50`, `#34495e`, `#7f8c8d`
  - Weekend (`#27ae60`) — Casual · Bright · Relaxed; swatches `#27ae60`, `#3498db`, `#f39c12`
  - Vacation (`#e67e22`) — Flowy · Colorful · Comfort; swatches `#e74c3c`, `#f39c12`, `#1abc9c`
  - Evening (`#e74c3c`) — Dark · Fitted · Elegant; swatches `#000000`, `#8e44ad`, `#c0392b`
- **Connectors:** 2.5px colored line from the user circle's edge to each box's *edge* (stopping at `distance − boxW/2 − 6`, not at the box center) with a matching arrowhead; drawn before the boxes so the boxes paint over the line ends.
- **Bottom box:** 430×62 dashed red `#e74c3c` rectangle (top y=268) filled red at 8% alpha, below the box band; three centered lines — bold 12px red "Model averages all four modes → recommends:", bold 13px `#7f8c8d` ""Beige Business-Casual Cardigan"", 11px red "(wrong for every one of the four occasions)".

## Photography vs Reality Gap

**Photography vs Reality Gap**

- **The Problem:** Studio lighting and retouching open a 20-40% gap against the delivered item.
- **Why It Breaks ML:** Ratings and returns reflect reality, not the photo the model learned from.
- **False Similarity:** An accurate photo and an oversaturated one look identical to the model.
- **Color Shift:** Screens differ across sRGB and DCI-P3, so "navy" renders as "royal blue."
- **Return Driver:** 35% of returns cite "color not as shown" rather than any real preference.
- **Model Body Type:** Catalog models run 5'9"+ and size 0-4; average customers are 5'4" and 12-14.
- **Systematic Misfit:** Drape and length therefore misrepresent fit for 80% of customers.
- **Texture Invisibility:** Fabric feel never shows online, and 25% of returns cite fabric quality.

**Example (callout box):** Product photo shows "emerald green" dress on 5'10" model in perfect lighting. Customer's laptop screen shifts it to "teal." Delivered dress (in home lighting) looks "forest green." Customer returns for "wrong color." Model sees: "User bought emerald dress → returned it. Avoid emerald for this user." Wrong lesson—problem was photo accuracy, not color preference.

### Visualization (canvas `canvas6`, 720×340)

Photo-vs-reality color swatch comparison with delta arrows and a satisfaction bar pair.

- **Title (bold 17px `#1a5276`, centered, y=25):** "Product Photo vs. Customer Reality Gap".
- **Photo row** (header bold 15px green `#27ae60`: "PRODUCT PHOTO" / "(Studio Lighting)"; three 140×70 swatch boxes with 2px `#2c3e50` borders and 13px name labels, centered at x=120, 300, 480):
  - Vivid Blue `#2E86DE`; Crisp White `#FFFFFF` (label in `#2c3e50`); Emerald `#10AC84`.
- **Reality row** (header bold 15px red `#e74c3c`: "CUSTOMER REALITY" / "(Home Lighting)"; three 140×70 swatch boxes with 2px `#e74c3c` borders):
  - Dull Navy `#1a5276`; Cream/Off-White `#F5F5DC` (label in `#2c3e50`); Forest `#27ae60`.
- **Delta arrows:** orange `#e67e22` 2px vertical arrows with arrowheads from each photo swatch down to its reality swatch, each with an 11px orange "Δ" label.
- **Right annotations (10px orange, left-aligned at x=566, in the empty column right of the swatches):** "Color shift = 20-40% gap" / "Texture invisible online" / "Model body ≠ customer".
- **Satisfaction bars (bottom band, clear of the reality swatch row):** header bold 11px `#2c3e50` "Customer satisfaction, by whether the item matched its photo:"; green `#27ae60` bar labeled "Matches photo" with white bold "4.2★" (filled 4.2/5 of a 200px outline); red `#e74c3c` bar labeled "Doesn't match photo" with white bold "1.8★" (filled 1.8/5 of a 200px outline); both outlined `#2c3e50` 1px, bar height 32.

## Regeneration instructions

- **Layout:** standard detail-page structure — h1, `.subtitle`, then one `<h2>` per pitfall, each followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` + `<ul>` of labeled bullets + an `.example` callout div, right `<td>` (60%, centered) holds one canvas. Even table rows background `#fafcfe`.
- **Callouts:** `.example` — background `#f0f4f8`, left border `3px solid #2980b9`, padding 10px 12px, 0.9em, with a bold "Example:" lead. `.philosophy` style (background `#f0f4f8`, left border `4px solid #2980b9`) is defined but unused.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvases:** six canvases sized to their content — `canvas1` 720×300, `canvas2` 720×300, `canvas3` 720×360, `canvas4` 720×300, `canvas5` 720×370, `canvas6` 720×340. Heights are set from the tallest drawn y-coordinate in each chart; a declared height smaller than the drawing causes clipped and overlapping elements. Apply the project-standard `window.devicePixelRatio` backing-store scaling (multiply canvas width/height by dpr, `ctx.scale` back to logical coordinates). Each chart begins by filling a white background. Chart fonts sans-serif, 10-14px (bold 14px titles) — the canvas renders at roughly half its 720px logical width inside the 50% column, so anything above 14px reads oversized.
- **Layout invariant:** each canvas is composed of horizontal bands (title / primary figure / secondary figure / footer annotation) whose y-ranges must not intersect, and every drawn element must fall within `0..height`. Verify band arithmetic when changing any font size, radius, or box dimension.
- **Palette:** primary blue `#1a5276`, secondary blues `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`, teal `#1abc9c`, grays `#2c3e50`/`#34495e`/`#7f8c8d`/`#999`/`#e0e0e0`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
