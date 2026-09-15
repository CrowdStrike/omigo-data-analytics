# Interaction Effects

**Page type:** detail page (tutorial: card-sections, each a two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** Interaction Effects

**Subtitle:** Sometimes the effect of one thing depends on another thing — a single "average effect" then describes no real situation at all

## One Discount, Two Behaviors

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — a shop runs a 10% discount and tracks daily sales with and without it
- **Weekdays** — sales go from 200 to 210 units: a +5% lift
- **Weekends** — sales go from 300 to 375 units: a +25% lift
- **Same lever, different pull** — the discount's effect depends on the day type
- **Interaction** — when one variable's effect changes with another variable's value

*Example:* Shoppers browsing on a lazy Saturday respond to the nudge; rushed weekday buyers barely do.

**Key point:** With an interaction you cannot state THE effect of the discount — you must say "on which days?"

### Visualization (canvas `c1`, 720×300)

Grouped bar chart — sales by day type, with and without discount.

- **Title (bold 15px, ink `#1a5276`, top center):** "Daily sales with and without the 10% discount"
- **Groups (bar width 85px, in-group gap 16px):**
  - "weekday": no-discount 200 (blue `#2a78d6`), discount 210 (orange `#d95926`); bold 14px magenta lift label "+5%" above the group
  - "weekend": no-discount 300, discount 375; magenta lift label "+25%"
- **Value labels:** bold 13px unit values above each bar; bold 13px group labels below baseline
- **Legend (top left, 11px squares):** blue "no discount", orange "discount"
- **Axes:** y 0 to 400, gridlines (`#e5e9ef`) and muted labels every 100; padding: left 65, top 50, bottom 60
- **Caption (bold 13px magenta `#d55181`, bottom center):** "+5% vs +25% — same discount, different day"

## The Average That Matches Nothing

**Tags:** `worked example` (green), `do it by hand` (blue)

- **A week without discount** — 5×200 + 2×300 = 1,600 units
- **A week with discount** — 5×210 + 2×375 = 1,800 units
- **Pooled lift** — 1,800 / 1,600 = +12.5% "average effect"
- **The problem** — no actual day gets +12.5%: weekdays get +5%, weekends +25%
- **Hidden by pooling** — the average describes the weekly mix, not the discount

*Example:* Report "+12.5% lift" and a manager expects it on Monday; Monday delivers +5%.

**Key point:** A single average effect is a blend — always check whether it splits before acting on it.

### Visualization (canvas `c2`, 720×300)

Three-bar chart comparing per-segment lifts to the pooled average.

- **Title (bold 15px, ink, top center):** "Three \"effects\" of the same discount"
- **Bars (width 130px):**
  - "weekdays", sublabel "200 → 210": +5%, blue `#2a78d6`
  - "weekends", sublabel "300 → 375": +25%, green `#008300`
  - "\"average effect\"", sublabel "1,600 → 1,800 / week": +12.5%, yellow `#c98500`
- **Value labels:** bold 14px "+5%" / "+25%" / "+12.5%" above bars; bold 12px labels and muted 12px sublabels below baseline
- **Axes:** y +0% to +30%, gridlines and muted "+N%" labels every 5%; padding: left 65, top 50, bottom 70
- **Caption (bold 13px magenta, bottom center):** "the blend matches neither kind of day"

## Why It Matters: Act on the Split, Not the Blend

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Targeting** — weekend-only discounting keeps 150 of the 200 extra units: 75% of the lift
- **A/B tests** — an average lift can hide a segment where the change actually hurts
- **ML models** — plain linear models miss interactions unless you add a × term; trees find them
- **Forecasting** — predicting a discounted Saturday with the +12.5% blend comes out ~10% low
- **In regression** — sales = 200 + 10·disc + 100·wkend + 65·disc×wkend; the 65 is the interaction

*Example:* The team moved the promo budget to weekends and kept three-quarters of the lift.

**Key point:** The payoff of finding an interaction is a better decision — aim the lever where it pulls hardest.

### Visualization (canvas `c3`, 720×300)

Two-line interaction plot (discount OFF → ON), lines fanning out.

- **Title (bold 15px, ink, top center):** "Discount effect by day type — the lines fan out"
- **Lines (width 3, dots radius 5, bold 12px value labels above points):**
  - Blue `#2a78d6` weekday line: 200 (OFF) → 210 (ON)
  - Green `#008300` weekend line: 300 (OFF) → 375 (ON)
- **Axes:** y from 150 to 400, gridlines and muted labels every 50; x has two positions labeled 13px "discount OFF" and "discount ON"; L-shaped gray axes `#999`; padding: top 50, bottom 52, left 70, right 200
- **Legend (right margin, 14×4px swatches, 12px):** green "weekend (+75 units)", blue "weekday (+10 units)"
- **Annotation (bold 13px magenta, right margin, two lines):** "non-parallel lines =" / "interaction (the +65)"

## The Common Confusion: What Interaction Is Not

**Tags:** `common mistake` (orange), `core idea` (blue)

- **Not correlation** — it is not "discount and weekend are related"; it's the effect changing
- **Two main effects can coexist** — weekends sell more AND discounts lift sales
- **That alone is no interaction** — the two effects could simply add up
- **Parallel test** — if both day types gained the same +40, lines stay parallel: none
- **Fan test** — +10 vs +75 units: the lines fan out, and one average misleads

*Example:* "Weekends sell more" is a main effect; "discounts work better on weekends" is the interaction.

**Key point:** Interaction = the effect of one variable depends on the level of another — nothing more, nothing less.

This section's viz cell holds both canvases side by side in a `.viz-pair` flex row.

### Visualization (canvas `c4a`, 310×340)

Two parallel lines — no interaction case.

- **Title (bold 15px, ink, top center):** "No interaction: both gain +40"
- **Lines (width 3, dots radius 5, bold 12px value labels):** blue `#2a78d6` 200 → 240; green `#008300` 300 → 340
- **Axes:** y 150 to 400, muted labels every 50; x positions labeled 12px "OFF" and "ON"; L-shaped gray axes `#999`; padding: top 55, bottom 60, left 46, right 12
- **Caption (bold 13px green, bottom center):** "parallel — effects just add up"

### Visualization (canvas `c4b`, 310×340)

Two fanning lines — interaction case.

- **Title (bold 15px, ink, top center):** "Interaction: +10 vs +75"
- **Lines (width 3, dots radius 5, bold 12px value labels):** blue `#2a78d6` 200 → 210; green `#008300` 300 → 375
- **Axes:** same as `c4a` (y 150–400, "OFF"/"ON" x labels)
- **Caption (bold 13px magenta, bottom center):** "fanning — one average misleads"

## Regeneration instructions

- **Template/layout:** tutorials topic-page skeleton. `<h1>` (no index number) with 2px bottom border `#2980b9`, `.subtitle` paragraph, then four `.card-section` blocks each with an `<h2>` (1.3rem, `#1a5276`, 2px bottom border `#2980b9`) and a `table.layout` (one `<tr>`: `.text-col` 50% / `.viz-col` 50%). One section places canvases `c4a`/`c4b` (310×340 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Left column structure:** `.tags` row of pill spans first (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem, 600 weight, 2px 10px padding, radius 10px), then a `<ul>` of one-line bullets each opening with `<b>` (bold terms colored `#1a5276`), an italic `.example` paragraph (`#555`, 0.9rem), and a `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) beginning with `<strong>Key point:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, 1px solid `#e0e0e0` border, 4px radius; ul 0.92rem.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates, and fills a white background.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
