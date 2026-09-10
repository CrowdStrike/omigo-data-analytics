# Unquantified Problem Solving

**Page type:** detail page (two-column obj-table layout: text left 40%, canvas right 60%, one row per section)
**HTML title tag:** Unquantified Problem Solving — Pseudoscience in Data Analysis

**Subtitle:** Building a Solution Without Understanding What Problem Segments You're Actually Solving

## Section 1: The Pattern

- **AI without problem decomposition:** "We'll use AI to solve fraud!" Which fraud — card-not-present (40%), account takeover (30%), friendly fraud (20%), bot attacks (10%)? The model picks up the easiest segment, and you report "60% detection rate!" without ever mapping the problem space.
- **Tech-first, problem-second:** "We have a recommendation engine!" For whom, in what context? It works because today's users are desktop power users; six months later, mobile-first users from a new market arrive with different behavior, and the system breaks because you never quantified which segment it served.
- **Customer adoption shifts distribution:** You build for early adopters (tech-savvy, small), then growth brings an enterprise customer that generates more data than your first 500 combined. Your model was trained on the small-customer distribution and breaks silently under enterprise patterns.

### Visualization (canvas `c1`, 720×340)

Two horizontal stacked segment bars (today vs 6 months later) with dashed coverage brackets.

- **Title (bold 14px, `#1a5276`, centered):** "Problem Segments: Today vs After Distribution Shift".
- **Bar geometry:** bars span x=100 to w−40, height 40. Row labels bold 13px `#333`, right-aligned at x=90: "TODAY:" (bar at y=55) and "6 MONTHS:" (bar at y=160).
- **TODAY segments** (each with white 1px separators, white bold 12px centered labels if wide enough):
  - Type A: 40%, `rgba(26,82,118,0.6)`, label "Type A: 40%"
  - Type B: 30%, `rgba(39,174,96,0.5)`, label "Type B: 30%"
  - Type C: 20%, `rgba(230,126,34,0.5)`, label "Type C: 20%"
  - Type D: 10%, `rgba(231,76,60,0.5)`, label "Type D: 10%"
- **Today coverage bracket:** green `#27ae60` dashed rectangle (width 2, dash 4/3) around the first 70% of the bar; green bold 12px caption below: "Your model covers A+B (70%) — you don't know this".
- **6 MONTHS segments** (same colors, bold 11px white labels if segment > 50px):
  - A: 15% ("A: 15%"), B: 10% ("B: 10%"), C: 45% ("Type C grew: 45%"), D: 30% ("Type D: 30%")
- **Shifted coverage bracket:** same green dashed rectangle around only the first 25%; red `#e74c3c` bold 12px caption below: "Same model now covers only 25% — metrics collapsed".
- **Bottom annotations (centered):** red bold 13px: "\"Nothing changed but metrics dropped 50%!\""; gray `#666` 12px: "The distribution shifted. You never mapped which segments you solved."

## Section 2: The Mechanism: Distribution Shift Invisible Without a Map

- **The symptom:** The metric looks great at launch, slowly degrades over months, and the team is confused ("nothing changed!"). Nothing in the system changed — the distribution of problems shifted.
- **Recommendation systems:** "Our model drives 15% of revenue!" — true for power users viewing 50+ items per session. When mobile grows from 30% to 70% of traffic (3-5 items viewed), the model has zero signal for short sessions and revenue attribution drops to 5%.
- **One large customer changes the mix:** An enterprise onboarding shifts your entire distribution overnight. A system that was solving 70% of the problem is now solving 25%.

### Visualization (canvas `c2`, 720×340)

Two pie charts (before/after) connected by an arrow, showing model coverage collapse.

- **Title (bold 14px, `#1a5276`, centered):** "Model Coverage: Before vs After Distribution Shift".
- **Left pie** (center 160,165, radius 90), slices from 12 o'clock, white 2px separators, white bold 11px labels at 65% radius for slices ≥12%:
  - A: 40% `#1a5276` ("A: 40%"), B: 30% `#27ae60` ("B: 30%"), C: 20% `#e67e22` ("C: 20%"), D: 10% `#e74c3c` ("D: 10%")
  - Caption below (bold 13px `#333`): "NOW — Coverage: 70%"
- **Right pie** (center w−160,165, radius 90):
  - A: 15% `#1a5276` ("A: 15%"), B: 10% `#27ae60` ("B: 10%"), C: 45% `#e67e22` ("C: 45%"), D: 30% `#e74c3c` ("D: 30%")
  - Caption below: "AFTER SHIFT — Coverage: 25%"
- **Arrow between pies:** gray `#999` horizontal line at y=165 with filled arrowhead; red `#e74c3c` bold 12px two-line label above/at center: "Distribution" / "shift".
- **Legend (bottom left):** 14×14 square filled `rgba(26,82,118,0.15)` with green `#27ae60` 2px border, 12px `#333` text: "A + B = segments your model actually handles (unknown to you)".

## Section 3: The Correct Approach

- **Before building:** Decompose the problem into segments — what percentage is each, which does your approach theoretically handle, and which are out of scope?
- **Track distribution:** Know today's segment mix, where it's trending, and what happens when one large customer changes it.
- **State scope:** "We solve segments A and B (55% of current volume), not C and D; if C grows, we need a different approach." That's engineering — building without it is hoping.

**Why it's pseudoscience:** Building without quantifying the problem space is running an experiment with no hypothesis — you can't learn from success or failure because you don't know what you solved, and when it breaks you can't diagnose where. It's throwing technology at an unexamined problem and celebrating when the metric happens to be green.

### Visualization (canvas `c3`, 720×340)

Line chart of metric degradation over 12 months with an underlying segment-share area.

- **Title (bold 14px, `#1a5276`, centered):** "Metric Degradation: \"Nothing Changed!\" (Something Did)".
- **Plot area:** left 80, right w−50, top 50, bottom 270; light gray `#ddd` axes; horizontal gridlines `#f0f0f0` every 10 from 10 to 70; y-scale 0–70 mapped to plot height.
- **Data (x = months M1–M12):**
  - Overall detection rate (reported metric): `[62, 63, 61, 60, 58, 55, 50, 44, 38, 35, 32, 30]` — solid red `#e74c3c` line, width 2.5, 3px red dots.
  - Segment A share (shrinking): `[40, 40, 38, 36, 33, 30, 25, 20, 17, 15, 14, 13]` — dashed green `#27ae60` line (dash 4/3, width 2) with area fill `rgba(39,174,96,0.15)` down to baseline.
- **X labels:** M1…M12 (11px `#666`); **Y labels:** 0%, 35%, 70% right-aligned at axis.
- **Legend (top-left inside plot, 12px `#333`):** red swatch — "Overall detection rate (reported metric)"; green dashed swatch — "Segment A share (shrinking — model only solves A)".
- **Annotations at month M10:** red bold 12px "Team: \"Nothing changed!\"" above the metric point; gray 11px "(Segment A shrank from 40% → 13%)" below it.
- **Caption (bottom center, italic 12px `#666`):** "Without a problem map, you cannot diagnose what shifted."

## Section 4: The Incentive

- **Quantifying is hard and unglamorous:** It requires data analysis, segmentation, and admitting scope limits.
- **"We use AI to detect fraud"** is simple, impressive, and fundable.
- **"We detect 2 of 5 fraud types comprising 40% of current volume"** is honest but less inspiring.
- The incentive is to skip understanding and jump to building; the bill comes due six months later when the distribution shifts.

### Visualization (canvas `c4`, 720×300)

Two side-by-side comparison boxes: unquantified vs quantified approach.

- **Title (bold 14px, `#1a5276`, centered):** "Unquantified vs Quantified Problem Approach".
- **Boxes:** 300×210 each, top y=45; left box at x=40, right box at x = w−340.
- **Left box (Unquantified):** fill `rgba(231,76,60,0.05)`, border red `#e74c3c` 2px; centered red bold 13px header lines "UNQUANTIFIED" / "(Pseudoscience)"; 12px `#333` bullet lines (24px spacing):
  - "• \"We use AI to solve fraud\""
  - "• No segment decomposition"
  - "• Can't predict what breaks"
  - "• Can't diagnose degradation"
  - "• Success is accidental"
  - "• Impressive but fragile"
- **Right box (Quantified):** fill `rgba(39,174,96,0.05)`, border green `#27ae60` 2px; centered green bold 13px header lines "QUANTIFIED" / "(Engineering)"; bullets:
  - "• \"We detect types A+B (55% of volume)\""
  - "• Segments mapped with %"
  - "• Know when distribution shifts"
  - "• Can diagnose which segment grew"
  - "• Success is understood"
  - "• Honest and resilient"
- **Caption (bottom center, italic 12px `#666`):** "Quantifying is unglamorous. Skipping it is fundable. The bill arrives in 6 months."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section (4 rows); left `<td>` (40%) holds `.obj-title` + bullets (and closing paragraph in section 3), right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `ul` 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper returning `{ctx, w, h}`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`; translucent fills `rgba(26,82,118,0.6)`, `rgba(39,174,96,0.5)`, `rgba(230,126,34,0.5)`, `rgba(231,76,60,0.5)`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
