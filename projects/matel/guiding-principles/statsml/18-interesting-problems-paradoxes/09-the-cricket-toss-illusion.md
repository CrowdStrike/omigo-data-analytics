# The Cricket Toss Illusion

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** The Cricket Toss Illusion

**Subtitle:** Winning the toss doesn't matter — until it does. On average, toss-winners win 50.3% of matches. But in specific conditions, the toss decides the game before a ball is bowled.

## Callout (philosophy box)

**Why this is fascinating:** Cricket commentators and analysts have long debated whether the toss matters. The data seems clear: across ~4000 Test matches, toss-winner win rate is barely above 50%. Case closed? No. The toss advantage is a conditional effect — it only activates when weather/pitch conditions create asymmetry between batting first and second. Averaging over all conditions (perfect batting tracks, deteriorating pitches, overcast mornings, flat pitches) washes out a genuine causal mechanism.

## Section: The Unconditional Picture

**Obj-title:** The Naive Test

Across all Test cricket (1877-2024), toss winners win approximately 50.3% of matches (excluding draws, perhaps ~51%). This difference is not statistically significant.

**The naive conclusion:** "The toss is decorative — just a TV ceremony with no real impact on match outcomes."

This unconditional test averages over 147 years of cricket across all venues, weather conditions, pitch types, and eras. It treats every match as equivalent.

### Visualization (canvas `unconditionalChart`, 720×360)

Two-bar chart of overall win rates by toss result.

- **Title (bold 16px `#1a5276`, left-aligned at plot left):** "Overall: No Meaningful Difference".
- **Plot:** margins top 40 / right 40 / bottom 60 / left 80. Y axis 0–100% with 12px `#333` labels every 20% and `#e0e0e0` gridlines; axes stroked `#666` width 1.
- **Reference line:** dashed red (`#e74c3c`, dash 6/4, width 2) horizontal line at 50%, labeled to the right in 11px `#e74c3c`: "50% (no advantage)".
- **Bars (80px wide, fill `rgba(26,82,118,0.35)`, border `#1a5276` width 2):** "Won Toss & Won Match" = 50.3%; "Lost Toss & Won Match" = 49.7%. Bold 14px `#1a5276` value labels "50.3%" and "49.7%" above the bars.
- **X labels (13px `#333`, two lines each, centered):** "Won Toss" / "& Won Match" and "Lost Toss" / "& Won Match".

## Section: The Moderator — Pitch Deterioration

**Obj-title:** The Hidden Mechanism

**The key insight:** In certain conditions, batting first provides a massive advantage because the pitch deteriorates over 5 days:

- **Subcontinent dust bowls:** Pitches crack and turn sharply by days 4-5 (Galle, Ahmedabad, Dhaka)
- **Overcast English conditions:** Morning swing helps seamers on day 1, but conditions flatten later
- **SCG/Adelaide/Pune:** Specific venues where the pitch breaks up significantly

In these conditions (~15-20% of matches), batting first is worth **+15-25% win probability**. The captain who wins the toss can exploit this; the one who loses cannot.

In the remaining 80% of matches (flat pitches, even conditions), the toss genuinely doesn't matter — outcomes are essentially 50/50.

### Visualization (canvas `moderatorChart`, 720×400)

Grouped bar chart: toss-winner vs toss-loser win rates split by pitch condition.

- **Title (bold 16px `#1a5276`, left-aligned at plot left):** "The Moderator Creates the Effect".
- **Plot:** margins top 40 / right 40 / bottom 80 / left 80. Y axis 0–100% with 12px `#333` labels every 20% and `#e0e0e0` gridlines; dashed red 50% reference line (`#e74c3c`, dash 6/4, width 2).
- **Bars:** 60px wide, 20px within-group spacing, two groups centered in each half of the plot. Won-toss bars fill `rgba(26,82,118,0.35)` with `#1a5276` border; lost-toss bars fill `rgba(231,76,60,0.25)` with `#e74c3c` border (all borders width 2).
  - Group 1 "Normal Conditions / (~80% of matches)": Won Toss 50.5%, Lost Toss 49.5%.
  - Group 2 "Deteriorating Pitch / (~20% of matches)": Won Toss 65%, Lost Toss 35%.
- **Value labels (bold 13px, above bars):** "50.5%" and "65%" in `#1a5276`; "49.5%" and "35%" in `#e74c3c`.
- **Group labels (13px `#333`, two lines each, centered below axis).**
- **Legend (bottom left):** blue swatch (fill `rgba(26,82,118,0.35)`, border `#1a5276`) + "Won Toss"; red swatch (fill `rgba(231,76,60,0.25)`, border `#e74c3c`) + "Lost Toss" (12px `#333`).

## Section: The Interaction Effect

**Obj-title:** The Statistical Structure

This is Simpson's Paradox adjacent — the marginal relationship (Toss → Win) is near zero, but the conditional relationship (Toss → Win | Deteriorating Pitch) is large and causal.

**The correct model:**

Math box:

`Win ~ Toss × Condition`
NOT
`Win ~ Toss`

**Analogy:** "Does an umbrella help?"

- Average across all days: barely (most days are sunny)
- Conditional on rain: obviously yes

The moderator (weather/pitch condition) is not a nuisance variable to control for — it IS the story. Testing the main effect when the true mechanism is an interaction is guaranteed to underestimate or miss the effect entirely.

### Visualization (canvas `interactionChart`, 720×420)

2×2 interaction grid (Toss × Condition) with arrows converging on a weighted-average box.

- **Title (bold 16px `#1a5276`, top center):** "The Interaction: Toss × Condition".
- **Grid:** four 160×160 cells, 40px gap, centered horizontally, starting y=50. Each cell has a two-line bold 14px `#1a5276` header, a bold 32px value, and a 12px note.
  - Top-left "Normal Pitch / Won Toss": background `#f8fafb`, border `#1a5276` width 2; value "50.5%" in `#27ae60`; note "No advantage" in `#666`.
  - Top-right "Bad Pitch / Won Toss": background `#e8f5e9`, border `#27ae60` width 3; value "65%" in `#27ae60`; note "Huge advantage!" in `#27ae60`.
  - Bottom-left "Normal Pitch / Lost Toss": background `#f8fafb`, border `#e74c3c` width 2; value "49.5%" in `#e74c3c`; note "No disadvantage" in `#666`.
  - Bottom-right "Bad Pitch / Lost Toss": background `#ffebee`, border `#e74c3c` width 3; value "35%" in `#e74c3c`; note "Huge disadvantage!" in `#e74c3c`.
- **Arrows:** two gray (`#666`, width 2) lines from below the bottom two cells converging to a center point.
- **Result box (200×50, centered, background `#f8fafb`, border `#1a5276` width 2):** bold 11px `#1a5276` "Weighted Average:" over bold 20px "50.3%".

## Closing callout (philosophy box)

**The statistical lesson:** Testing a main effect when the true mechanism is an interaction is guaranteed to underestimate (or miss) the effect. The moderator IS the story. This applies everywhere: "Does vitamin D help?" (only if you're deficient), "Does class size matter?" (only below 15 or above 35), "Does advertising work?" (only for new products in competitive categories). Unconditional tests of conditional effects produce misleading null results — and vice versa, conditional tests within a single context can overestimate effects that don't generalize.

## Regeneration instructions

- **Layout:** detail page. h1, `.subtitle` (a div on this page), opening `.philosophy` callout, three `h2` sections (unnumbered) each holding a `.obj-table` (one `<tr>`: left `<td>` 45% with `.obj-title` + paragraphs/bullets/`.math-box`, right `<td>` 55% centered canvas), closing `.philosophy` callout.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; p 0.95em `#333`; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; obj-table cells `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links.
- **Component styles:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `code` on `#eef2f7`, padding 2px 6px, radius 3px.
- **Canvases:** `unconditionalChart` 720×360, `moderatorChart` 720×400, `interactionChart` 720×420 (width/height attributes). The `setupCanvas(canvas)` helper reads `getBoundingClientRect`, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates, and pins the CSS size.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, bar fills `rgba(26,82,118,0.35)` and `rgba(231,76,60,0.25)`, text grays `#666`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
