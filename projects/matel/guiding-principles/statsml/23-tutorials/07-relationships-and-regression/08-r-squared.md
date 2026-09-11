# R-Squared

**Page type:** detail page (tutorial page: `.card-section` blocks, each h2 + two-column `table.layout` — text left with tag pills / bullets / example / key-point, canvas right; first section uses a 3-column 38/31/31 layout)
**HTML title tag:** R-Squared

**Subtitle:** The rent model has R² = 0.72: size explains 72% of why rents differ — the other 28% is location, floor, and luck

## Two ways to miss: around the mean vs around the line

**Tags:** `core idea` (blue), `worked example` (green)

- **No model** — knowing nothing, your best guess for any flat is the mean rent, $1,388
- **Total spread** — squared misses from that flat guess add up to 1,519,586
- **With the line** — guessing 400 + 18 × size instead, squared misses shrink to 425,200
- **The comparison** — R² asks: how much of the spread did the line remove?
- **Answer** — R² = 1 − 425,200 / 1,519,586 = 0.72

*Example:* The $1,946 flat misses the mean by $558, but misses the line by only $250 — most of its "surprise" was just being big (72 m²).

**Key point:** R² compares your model against the dumbest guess (the mean) — 0.72 means the line removed 72% of the squared spread.

### Visualization (canvas `c1a`, 420×300)

Scatter panel showing vertical miss segments from every point to the mean line.

- **Title (bold 15px, `#1a5276`):** "Misses from the mean".
- **Data:** sizes `[30,34,38,41,45,48,52,55,58,61,65,68,72,76,80]` vs rents `[1140,842,994,1258,1190,1124,1586,1360,1194,1328,1750,1694,1946,1528,1880]`; mean rent 1387.6.
- **Axes:** x 25–85 (ticks 30, 50, 70; label "size (m²)"), y 700–2100 (labels $800 and $2,000); padding top 42 / bottom 44 / left 54 / right 14; axis `#999`, labels 12px `#6b7280`.
- **Miss segments:** vertical lines from the mean to each point in `rgba(217,89,38,0.55)`, width 2.
- **Reference line:** horizontal violet `#4a3aa7` line, width 2.5, at $1,388 from x=27 to x=83, labeled bold 12px violet "mean $1,388".
- **Points:** blue `#2a78d6` circles, radius 3.5.
- **Annotation (bold 13px, `#d95926`, bottom center):** "squared misses total 1,519,586".

### Visualization (canvas `c1b`, 400×300)

Same scatter panel but with miss segments measured to the fitted line.

- **Title (bold 15px, `#1a5276`):** "Misses from the line".
- **Same data, axes and layout as c1a.**
- **Miss segments:** vertical lines from the fitted value 400 + 18 × size to each point in `rgba(217,89,38,0.55)`, width 2.
- **Reference line:** fitted line 400 + 18 × size in green `#008300`, width 2.5, from x=27 to x=83, labeled bold 12px green "400 + 18×s".
- **Points:** blue `#2a78d6` circles, radius 3.5.
- **Annotation (bold 13px, `#008300`, bottom center):** "total 425,200 — 72% of the spread gone".

## 72% explained, 28% left over

**Tags:** `worked example` (green), `where it's used` (orange)

- **Explained 72%** — the part of rent differences that tracks apartment size
- **Leftover 28%** — everything the line can't see: location, floor, renovation, luck
- **Scale-free** — R² is a fraction between 0 and 1, so people quote it across problems
- **Reading it** — 0 = the line is no better than the mean; 1 = every point exactly on the line
- **Field norms** — 0.72 is strong for housing data; physics expects 0.99, human behavior is happy with 0.2

*Example:* The 58 m² flat "should" rent for $1,444 by size alone, but goes for $1,194 — that $250 gap lives entirely in the 28%.

**Key point:** "Size explains 72% of the variation" — that is the whole sentence R² = 0.72 entitles you to say.

### Visualization (canvas `c2`, 720×300)

Single horizontal stacked bar splitting the variation 72/28, with the formula above it.

- **Title (bold 15px, `#1a5276`, top center):** "Where the variation in rents goes".
- **Formula (bold 14px `#2c3e50`, centered below title):** "R² = 1 − 425,200 / 1,519,586 = 0.72".
- **Stacked bar:** x from 70 to width−70, y=110, height 56px; left 72% segment blue `#2a78d6`, right 28% segment orange `#d95926`; outlined in ink `#1a5276` width 1.5; white bold in-bar labels "explained by size — 72%" (14px, centered in blue segment) and "28%" (13px, centered in orange segment); thin bracket lines above each segment in the segment's color.
- **Labels under the bar (12px):** blue, left-aligned: "bigger flat → higher rent: the part the line captures"; orange, right-aligned: "location, floor, renovation, luck".
- **Takeaway (bold 13px orange, centered):** "the 28% is not error in the data — it is everything size alone cannot know".

## What R² = 0.72 does not promise

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Not accuracy** — the typical miss is still about $168/mo; R² never mentions dollars
- **Not causation** — size and rent move together; R² can't say adding a m² causes +$18
- **Not correctness** — a curved market can score a high R² with a badly shaped line
- **Not comparable** — the same model scores lower R² on a narrower range of sizes
- **The habit** — always pair R² with the typical error in real units and a residual plot

*Example:* A landlord asking "what will my flat rent for?" cares about the ±$168, not the 0.72.

**Common confusion:** High R² ≠ good predictions ≠ causation — it only measures the share of variation the model soaks up.

### Visualization (canvas `c3`, 720×300)

Fitted line with a ±$168 typical-miss band around it.

- **Title (bold 15px, `#1a5276`):** "R² = 0.72 still leaves real dollar misses".
- **Data:** same 15 sizes/rents as c1a.
- **Axes:** x 25–85 (ticks 30, 40, 50, 60, 70, 80; label "size (m²)"), y 700–2100 (labels $800, $1200, $1600, $2000); padding top 42 / bottom 46 / left 62 / right 22; axis `#999`, labels 12px `#6b7280`.
- **Band:** filled parallelogram ±168 around the line 400 + 18 × size from x=27 to x=83 in `rgba(0,131,0,0.12)`.
- **Fitted line:** green `#008300`, width 3.
- **Points:** blue `#2a78d6` circles, radius 4.
- **Annotations (bold 13px):** green "typical miss ≈ ±$168/mo" near (30, 1900); orange `#d95926` "on a ~$1,400 rent that is a ~12% miss" near (46, 1010).

## Regeneration instructions

- **Template/layout:** tutorial topic page (see `tutorials/CLAUDE.md`; skeleton copied from `most-powerful-signals/07-social-graph-connections.html`). h1 + `.subtitle`, then three `.card-section` blocks each with an `<h2>` (bottom border `2px solid #2980b9`) and a `table.layout`. Section 1 uses three columns: `.text-col3` 38% / two `.viz-col3` at 31% each (canvases c1a 420×300 and c1b 400×300, cells centered; both drawn by one shared `scatterPanel(id, title, mode, annot, annotColor)` helper). Sections 2 and 3 use two columns: `.text-col` 50% / `.viz-col` 50%.
- **Left column structure per section:** `.tags` row of colored pill spans (`.tag.blue` bg rgba(26,82,118,0.12) text `#1a5276`; `.tag.green` bg rgba(39,174,96,0.15) text `#27ae60`; `.tag.red` bg rgba(231,76,60,0.12) text `#e74c3c`; `.tag.orange` bg rgba(230,126,34,0.15) text `#e67e22`; 0.72rem, weight 600, radius 10px), then a `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`, an italic `.example` paragraph (`#555`, 0.9rem), and a `.key-point` callout (bg `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem). The third section's callout opens with "Common confusion:" instead of "Key point:".
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; table cells padding 12px, no borders; canvases `width:100%` with border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange (used in tag pills and key-point border).
- In regenerated HTML, any card links use `.html` extensions (this page has no links).
