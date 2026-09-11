# The Correlation Coefficient

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table — text left 50%, canvas right 50%)
**HTML title tag:** The Correlation Coefficient

**Subtitle:** One number, r, between −1 and +1, that says how tightly two things move together in a straight line

## Twenty Days of Ads and Sales

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — a small shop logs ad hours and sales ($100s) for 20 days
- **One dot per day** — put ad hours on the x-axis, that day's sales on the y-axis
- **The cloud leans** — days with more ad hours mostly have higher sales
- **r measures the lean** — one number for how tight and which way the cloud tilts
- **Here r = 0.92** — close to +1, so ad hours and sales rise together strongly

*Example:* Day 19 ran 7.5 ad hours and sold $1,250; day 2 ran 1.5 hours and sold $390.

**Core idea:** r compresses a whole scatter plot into one number: +1 is a perfect uphill line, −1 a perfect downhill line, 0 no lean at all.

### Visualization (canvas `c1`, 720×300)

Scatter plot of 20 days of ad hours vs daily sales with a dashed least-squares trend line.

- **Title (bold 15px, `#1a5276`, top center):** "Ad Hours vs Daily Sales — 20 Days (illustrative)".
- **Data (20 points):** ad hours x = `[1, 1.5, 2, 2, 2.5, 3, 3, 3.5, 4, 4, 4.5, 5, 5, 5.5, 6, 6, 6.5, 7, 7.5, 8]`; sales y = `[4.8, 3.9, 6.6, 4.7, 7.1, 5.6, 7.8, 6.9, 8.9, 7.1, 9.7, 8.0, 10.4, 8.7, 11.3, 9.4, 11.7, 10.2, 12.5, 11.6]`.
- **Axes:** x from 0 to 9 (integer tick labels 0–9), y from 0 to 14 (labels every 2); L-shaped gray `#999` axes; padding top 44, bottom 46, left 58, right 24. Axis titles 12px `#444`: "ad hours that day" (bottom center) and rotated "daily sales ($100s)" (left).
- **Trend line:** least-squares fit computed from the data arrays, drawn dashed (dash 6/4, width 1.5) in muted gray `#6b7280` from x=0.5 to x=8.5.
- **Points:** filled blue `#2a78d6` circles, radius 5.
- **Annotation (green `#008300`, bold 13px, upper-left area at data coords ~(0.6, 12.8)):** "r = 0.92 — more ad hours, more sales".

## Computing r by Hand on Five Days

**Tags:** `worked example` (green), `by hand` (blue)

- **Five days** — ad hours x = 1, 2, 3, 4, 5 and sales y = 4, 5, 7, 8, 11 (in $100s)
- **Find the means** — mean of x is 3, mean of y is 7
- **Deviations** — x: −2, −1, 0, 1, 2 and y: −3, −2, 0, 1, 4
- **Multiply pairwise** — products 6, 2, 0, 1, 8 add up to 17
- **Scale it** — divide by √(10 × 30) = 17.3, so r = 17 / 17.3 = 0.98

*Example:* Day 5 is above both means, so its deviations (+2, +4) multiply to a big positive +8.

**The trick:** when a day is high (or low) on both axes, its product is positive; the scaling step guarantees r can never leave −1 to +1.

### Visualization (canvas `c2`, 720×300)

Five-point scatter with mean cross-hairs, per-point deviation products, shaded "agreeing" quadrants, and a right-side computation panel.

- **Title (bold 15px, `#1a5276`):** "Five Days: Deviation Products Around the Means".
- **Data:** x = `[1, 2, 3, 4, 5]`, y = `[4, 5, 7, 8, 11]`; product labels per point: "+6", "+2", "0", "+1", "+8" (green `#008300`, bold 12px, offset up-right of each dot).
- **Axes:** x 0–6 (integer labels), y 0–12 (labels every 3); padding top 44, bottom 46, left 58, right 190 (room for panel). Axis titles: "ad hours", rotated "sales ($100s)".
- **Quadrant shading:** faint green `rgba(0,131,0,0.07)` rectangles covering the "both high" quadrant (x>3, y>7) and the "both low" quadrant (x<3, y<7).
- **Mean cross-hairs:** dashed (dash 5/4, width 1) muted gray lines at x=3 and y=7, labeled in muted 12px "mean x = 3" and "mean y = 7".
- **Points:** blue `#2a78d6` circles radius 6.
- **Right computation panel (starting x ≈ w−178, left-aligned):** ink bold 13px "Add the products:"; `#444` 13px "6 + 2 + 0 + 1 + 8 = 17"; ink bold "Scale by spread:"; `#444` "√(10 × 30) = 17.3"; green bold 14px "r = 17 / 17.3 = 0.98"; muted 12px two lines "every product ≥ 0 —" / "the cloud leans uphill".

## What Different r Values Look Like

**Tags:** `core idea` (blue), `rule of thumb` (orange)

- **r = 0.90** — a tight uphill cloud; knowing x almost pins down y
- **r = 0.55** — an uphill tendency you can see, with plenty of exceptions
- **r ≈ 0** — a shapeless cloud; x tells you nothing about y
- **r = −0.70** — a clear downhill lean; more x tends to mean less y
- **Sign vs strength** — the sign gives direction, the size gives tightness

*Example:* Sales vs ad hours might be r = 0.9, but sales vs the cashier's shoe size sits near 0.

**Rule of thumb:** −0.7 is just as strong a relationship as +0.7 — it simply points downhill instead of uphill.

### Visualization (canvas `c3`, 720×300)

Four side-by-side mini scatter panels (same 15 x-values, four different y-series).

- **Title (bold 15px, `#1a5276`):** "The Same 15 Days Under Four Different Relationships".
- **Panel geometry:** four panels 158×190 px, 18px gaps, starting at x=26, y=48; each with L-shaped light gray `#bbb` axes; dots radius 4; x values 1–15 mapped over panel width (scale 0–16), y mapped over panel height (scale 0–15).
- **Panel 1 — label "r = 0.90", green `#008300`, header "tight uphill":** y = `[3.9, 2.4, 5.6, 3.6, 7.0, 4.9, 8.4, 6.0, 9.1, 7.3, 10.9, 8.6, 11.9, 10.0, 13.1]`.
- **Panel 2 — label "r = 0.55", blue `#2a78d6`, header "loose uphill":** y = `[6.5, 2.1, 9.8, 3.9, 11.2, 4.6, 12.5, 5.8, 7.1, 13.2, 6.4, 13.5, 8.8, 10.1, 13.9]`.
- **Panel 3 — label "r ≈ 0", violet `#4a3aa7`, header "no lean":** y = `[9.2, 3.1, 12.5, 5.8, 13.1, 2.4, 7.9, 13.6, 3.2, 9.7, 4.5, 13.4, 3.4, 7.9, 7.4]`.
- **Panel 4 — label "r = −0.70", orange `#d95926`, header "clear downhill":** y = `[13.5, 9.8, 12.9, 7.9, 12.3, 6.2, 11.1, 4.4, 10.2, 8.9, 3.1, 9.4, 2.2, 7.3, 1.9]`.
- **Labels:** r-value label bold 14px centered below each panel in the panel's color; one-word header bold 13px centered above each panel in the same color.

## r Only Sees Straight Lines

**Tags:** `common mistake` (red), `rule of thumb` (orange)

- **A perfect curve** — suppose sales peak at 5 ad hours, then over-exposure hurts
- **r comes back 0.00** — the uphill half and downhill half cancel exactly
- **Zero r ≠ no relationship** — it only means no straight-line relationship
- **Plot first** — a scatter plot catches curves, clusters, and outliers r hides
- **Same number, many shapes** — very different clouds can share one r value

*Example:* Here sales follow the ad hours perfectly along a curve, yet r reports exactly 0.

**Common mistake:** reading r = 0 as "these two are unrelated" — always look at the scatter plot before trusting the number.

### Visualization (canvas `c4`, 720×300)

Inverted-parabola scatter with connecting curve and a flat dashed best-fit line.

- **Title (bold 15px, `#1a5276`):** "Sales Peak at 5 Ad Hours, Then Fall (illustrative)".
- **Data:** ad hours = `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`; sales = `[0, 9, 16, 21, 24, 25, 24, 21, 16, 9, 0]` (the curve 25 − (x−5)²).
- **Axes:** x 0–10 (labels every 2), y 0–28 (labels every 7); padding top 44, bottom 46, left 58, right 24; axis titles "ad hours that day" and rotated "daily sales ($100s)".
- **Curve:** connecting polyline in translucent violet `rgba(74,58,167,0.35)`, width 2; points as violet `#4a3aa7` circles radius 5.5.
- **Best straight line:** horizontal dashed (dash 6/4, width 1.5) muted gray line at y=15, labeled in muted 12px "best straight line: flat".
- **Annotation (magenta `#d55181`, bold 14px, top center at data coords (5, 27)):** "perfect pattern, yet r = 0.00".

## Regeneration instructions

- **Template:** tutorials topic-page layout. Page: `<h1>` + `.subtitle`, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, bottom border 2px solid `#2980b9`) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%) holding one canvas.
- **Text column structure per section:** `.tags` row of colored pill spans (0.72rem, 600 weight, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (`li b` in `#1a5276`); one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) starting with a `<strong>` label.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each declared 720×300 intrinsic; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Hardcode all data arrays (no `Math.random()`); the c1 trend line is computed by least squares from the hardcoded arrays. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (tutorials `P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette accents: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
