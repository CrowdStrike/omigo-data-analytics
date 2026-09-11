# Residuals

**Page type:** detail page (tutorial page: `.card-section` blocks, each h2 + two-column `table.layout` — text left with tag pills / bullets / example / key-point, canvas right; one section uses a 3-column 38/31/31 layout)
**HTML title tag:** Residuals

**Subtitle:** Each apartment's miss from the fitted rent line — the leftover the model couldn't explain, and the model's report card when you plot it

## Each apartment's miss from the line

**Tags:** `core idea` (blue), `worked example` (green)

- **Setup** — same 15 listings, same fitted line: rent ≈ 400 + 18 × size
- **Residual** — actual rent minus the line's guess; one number per apartment
- **Positive** — the flat costs more than its size suggests (point sits above the line)
- **Negative** — the flat is cheaper than its size suggests (point sits below)
- **They balance** — the 15 residuals add up to exactly zero; the line splits the cloud fairly

*Example:* The 52 m² flat: line says 400 + 18 × 52 = $1,336, actual is $1,586, so its residual is +$250.

**Key point:** A residual is the part of the rent the line could not explain — the "everything else" beyond size.

### Visualization (canvas `c1`, 720×300)

Scatter with fitted line and vertical miss segments from line to each point.

- **Title (bold 15px, `#1a5276`, top center):** "Each point's miss from the line = its residual".
- **Data:** sizes `[30,34,38,41,45,48,52,55,58,61,65,68,72,76,80]` vs rents `[1140,842,994,1258,1190,1124,1586,1360,1194,1328,1750,1694,1946,1528,1880]`; residuals `[200,-170,-90,120,-20,-140,250,-30,-250,-170,180,70,250,-240,40]` vs the line 400 + 18 × size.
- **Axes:** x 25–85 (ticks 30, 40, 50, 60, 70, 80; label "size (m²)"), y 700–2100 (labels $800, $1200, $1600, $2000); padding top 42 / bottom 46 / left 62 / right 22; axis lines `#999`, tick labels 12px `#6b7280`.
- **Miss segments:** vertical line from fitted value to actual point per listing, width 2; aqua `#199e70` when residual ≥ 0, orange `#d95926` when negative.
- **Fitted line:** ink `#1a5276`, width 2.5, from x=27 to x=83.
- **Points:** blue `#2a78d6` circles, radius 4.
- **Callouts (bold 12px):** aqua "+$250 above the line" near the 52 m² point; orange "−$240 below" near the 76 m² point.
- **Annotation:** bold 13px ink `#1a5276` "residual = actual − predicted" near (28, 1960).

## The residual plot: the model's report card

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **Rotate the view** — plot size across, residual up; the line itself becomes the flat zero line
- **Healthy look** — a shapeless band around zero, roughly even from left to right
- **Our data** — all 15 misses sit between −$250 and +$250 with no drift or funnel
- **What that says** — size has given the line everything it has; the leftover looks like noise
- **Grading** — you grade a model by its leftovers, not by how pretty the fitted line looks

*Example:* Reading left to right: +200, −170, −90, +120, −20 ... small flats and big flats miss by similar amounts in both directions.

**Key point:** If the residual plot is a boring, shapeless band around zero — that is exactly what a good fit looks like.

### Visualization (canvas `c2`, 720×300)

Residual plot: size on x, residual on y, healthy shapeless band around zero.

- **Title (bold 15px, `#1a5276`):** "Residual plot for the rent line — a healthy, shapeless band".
- **Data:** sizes as above vs residuals `[200,-170,-90,120,-20,-140,250,-30,-250,-170,180,70,250,-240,40]`.
- **Axes:** x 25–85 (ticks 30, 40, 50, 60, 70, 80; label "size (m²)"), y from −320 to +320 (labels "−$250", "$0", "+$250"); padding top 42 / bottom 46 / left 62 / right 22; y-axis line `#999`.
- **Band:** shaded rectangle from −250 to +250 in `rgba(42,120,214,0.08)` with dashed (5/4) `#e5e9ef` edge lines at ±250.
- **Zero line:** ink `#1a5276`, width 2, full width; 12px `#6b7280` label just above left end: "the fitted line, seen edge-on".
- **Points:** radius 4.5; aqua `#199e70` when residual ≥ 0, orange `#d95926` when negative.
- **Value labels (12px):** aqua "+250" above the 52 m² point; orange "−250" below the 58 m² point.
- **Annotation (bold 13px blue `#2a78d6`, top area):** "no shape, no drift — nothing left for the line to learn".

## When the leftovers form a pattern

**Tags:** `common mistake` (red), `where it's used` (orange)

- **A curved market** — a second city where big flats get pricier per m² as size grows
- **Force a line** — best straight line is 449 + 17.6 × size; it looks reasonable on the scatter
- **The smile** — residuals run +62, +31 ... −36, −39 ... +31, +60: down, then up
- **Diagnosis** — a pattern means the line is missing something — here, a curve
- **Same trick** — patterns can also reveal a missing predictor, a time drift, or growing spread

*Example:* The line overshoots every mid-sized flat and undershoots both ends — no single number like R² would shout that at you.

**Key point:** Any visible shape in the residuals is the data saying "your model is missing something" — fix the model, don't ignore the plot.

### Visualization (canvas `c3a`, 420×300)

Curved-market scatter with a straight line forced through it.

- **Title (bold 15px, `#1a5276`):** "A curved market, forced straight".
- **Data:** sizes as above vs curved-market rents `[1040,1080,1130,1170,1220,1270,1330,1380,1440,1500,1580,1640,1730,1820,1920]` (illustrative second city).
- **Axes:** x 25–85 (ticks 30, 50, 70; label "size (m²)"), y 900–2050; padding top 42 / bottom 44 / left 54 / right 14; axis `#999`, labels 12px `#6b7280`.
- **Straight fit:** violet `#4a3aa7`, width 2.5, line 449 + 17.63 × size from x=27 to x=83.
- **Points:** magenta `#d55181` circles, radius 3.5.
- **Labels (bold 12px):** violet "449 + 17.6×s" near (28, 1350); magenta two lines "too high in the middle," / "too low at both ends" near (42, 1030)/(42, 960).

### Visualization (canvas `c3b`, 400×300)

U-shaped residual plot for the forced straight line.

- **Title (bold 15px, `#1a5276`):** "Its residuals form a smile".
- **Data:** sizes as above vs residuals `[62,31,11,-2,-23,-26,-36,-39,-32,-25,-15,-8,11,31,60]`.
- **Axes:** x 25–85 (ticks 30, 50, 70; label "size (m²)"), y from −80 to +90 (labels "−$50", "$0", "+$50"); padding top 42 / bottom 44 / left 50 / right 14.
- **Zero line:** ink `#1a5276`, width 1.5, full width.
- **Smile guide:** thick (width 6) translucent magenta `rgba(213,81,129,0.4)` polyline connecting the residual points.
- **Points:** magenta `#d55181` circles, radius 4.
- **Annotation (bold 13px magenta, top center):** "pattern = the line is missing a curve".

## Regeneration instructions

- **Template/layout:** tutorial topic page (see `tutorials/CLAUDE.md`; skeleton copied from `most-powerful-signals/07-social-graph-connections.html`). h1 + `.subtitle`, then three `.card-section` blocks each with an `<h2>` (bottom border `2px solid #2980b9`) and a `table.layout`. Sections 1 and 2 use two columns: `.text-col` 50% / `.viz-col` 50%. Section 3 uses three columns: `.text-col3` 38% / two `.viz-col3` at 31% each (canvases c3a 420×300 and c3b 400×300, cells centered).
- **Left column structure per section:** `.tags` row of colored pill spans (`.tag.blue` bg rgba(26,82,118,0.12) text `#1a5276`; `.tag.green` bg rgba(39,174,96,0.15) text `#27ae60`; `.tag.red` bg rgba(231,76,60,0.12) text `#e74c3c`; `.tag.orange` bg rgba(230,126,34,0.15) text `#e67e22`; 0.72rem, weight 600, radius 10px), then a `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`, an italic `.example` paragraph (`#555`, 0.9rem), and a `.key-point` callout (bg `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; table cells padding 12px, no borders; canvases `width:100%` with border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange (used in tag pills and key-point border).
- In regenerated HTML, any card links use `.html` extensions (this page has no links).
