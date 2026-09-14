# The Rendering Pipeline

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Rendering Pipeline

**Subtitle:** A browser turns a page from text into pixels in five stages — parse, style, layout, paint, composite — and every change re-runs only the stages after the one it touches

## Five Stages Between the Menu File and the Screen

**Tags:** `core idea` (blue), `five stages` (green), `browser internals` (orange)

- **The page** — a coffee shop's online menu: a header, 24 drink cards, one "order" button
- **Parse** — the browser reads the HTML text and builds a tree of elements (the DOM), 12ms
- **Style** — it matches CSS rules to each element: this card is white, that button green, 6ms
- **Layout** — it computes where everything goes: each card's exact x, y, width, height, 9ms
- **Paint** — it fills in the pixels: backgrounds, borders, text, the latte photo, 14ms
- **Composite** — it stacks the painted layers in order and ships the frame to the screen, 3ms

*Example (italic):* The menu page goes from raw HTML to visible pixels in 44ms — parse 12, style 6, layout 9, paint 14, composite 3.

**Key point:** The pipeline is a one-way assembly line: parse, style, layout, paint, composite — a change made at any stage forces every stage after it to run again.

### Visualization (canvas `c1`, 720×300)

Horizontal flow diagram: five stage boxes left to right with arrows, each box labeled with the stage name, what it produces, and its cost on the menu page's first load.

- **Title (bold 15px, `#1a5276`, top center):** "The Menu Page's First Load: Five Stages, 44ms Total".
- **Boxes:** five rounded boxes (110px wide, 64px tall, 8px radius) centered at y=140, x centers ≈ 90, 225, 360, 495, 630; connected by 3px `#6b7280` arrows.
  - "Parse" fill `rgba(42,120,214,0.15)`, border 2px blue `#2a78d6`, sub-label 11px `#2c3e50` "HTML → DOM"
  - "Style" fill `rgba(74,58,167,0.12)`, border violet `#4a3aa7`, sub-label "CSS → rules matched"
  - "Layout" fill `rgba(217,89,38,0.12)`, border orange `#d95926`, sub-label "x, y, width, height"
  - "Paint" fill `rgba(213,81,129,0.12)`, border magenta `#d55181`, sub-label "pixels filled in"
  - "Composite" fill `rgba(0,131,0,0.12)`, border green `#008300`, sub-label "layers → screen"
- **Cost bars:** under each box at y=215, a horizontal bar 12px tall in the box's border color, widths proportional to `[12, 6, 9, 14, 3]` ms at 6px/ms (72, 36, 54, 84, 18px), with bold 12px labels "12ms", "6ms", "9ms", "14ms", "3ms" beside each bar.
- **Annotation (bold 13px ink `#1a5276`, centered near y=260):** "one-way street: touch a stage, and everything after it re-runs".
- **Caption (12px `#444`, bottom right):** "stage timings illustrative".

## Color, Width, Transform: Three Changes, Three Different Bills

**Tags:** `worked example` (blue), `paint vs layout` (green), `composite only` (orange)

- **The setup** — the menu page is already on screen; we now change one thing and watch what re-runs
- **Change a color** — the order button turns green: nothing moved, so skip layout; paint 4ms + composite 1ms = 5ms
- **Change a width** — a drink card grows 40px: neighbors shift, so layout 6ms + paint 4ms + composite 1ms = 11ms
- **Change a transform** — the card slides over with `translateX`: the painted layer just moves; composite 1ms
- **The pattern** — the earlier in the pipeline a change lands, the more stages it drags along behind it

*Example (italic):* On the live menu, a color change bills 5ms, a width change 11ms, and a transform slide only 1ms — same visual nudge, 11× cost difference.

**Key point:** Color-like changes re-run paint + composite; geometry changes (width, left, font-size) re-run layout + paint + composite; transform and opacity re-run composite alone.

### Visualization (canvas `c2`, 720×300)

Stacked horizontal bar chart: three rows (transform, color, width), each bar built from stage segments colored to match the c1 stage colors, showing which stages re-run and their millisecond costs.

- **Title (bold 15px, `#1a5276`, top center):** "One Change, Three Bills: Which Stages Re-Run".
- **Axis:** baseline at x=200, bars extend right, scale 32px/ms, max width ~352px; x gridlines `#e5e9ef` at 4ms and 8ms with 12px `#444` labels "4ms", "8ms".
- **Rows (bar height 26px, at y = 90, 150, 210), each with a right-aligned 12px `#2c3e50` label at x=190:**
  - "transform (slide card)": one green `#008300` segment 1ms wide (32px), bold 12px green total "1ms" at bar end
  - "color (button turns green)": magenta `#d55181` paint segment 4ms (128px) + green composite segment 1ms (32px), total label "5ms"
  - "width (card grows 40px)": orange `#d95926` layout segment 6ms (192px) + magenta paint 4ms (128px) + green composite 1ms (32px), total label "11ms"
- **Legend (11px, top right under title):** orange square "layout", magenta square "paint", green square "composite".
- **Annotation (bold 13px green `#008300`, near x=380, y=95):** "transform skips straight to composite".
- **Caption (12px `#444`, bottom right):** "millisecond costs illustrative".

## The 16.7-Millisecond Frame Budget

**Tags:** `why it matters` (blue), `60fps` (green), `jank` (orange)

- **The budget** — a smooth screen redraws 60 times a second, so each frame gets 1000/60 ≈ 16.7ms
- **The animation** — the menu's sidebar slides open; done with `width`, every frame re-lays-out all 24 cards
- **The bill** — on the full menu each width frame costs ~24ms: layout + paint + composite, every frame
- **The miss** — 24ms > 16.7ms, so every frame blows the budget and the page shows ~30-40fps, not 60
- **The stutter** — missed frames arrive late or get dropped; the eye reads that as jank
- **The fix** — the same slide via `transform` costs ~2ms per frame and never comes near the budget line

*Example (italic):* Sliding the sidebar with `width` bills ~24ms a frame (~40fps, visibly choppy); the identical slide with `transform` bills ~2ms and holds 60fps.

**Key point:** Smoothness is a per-frame deadline, not an average — an animation that re-triggers layout every frame overspends 16.7ms and stutters, while a composite-only one glides.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: 8 animation frames on the x axis; for each frame a pair of bars — width-animation cost (orange, over budget) vs transform-animation cost (green, tiny) — against a dashed 16.7ms budget line.

- **Title (bold 15px, `#1a5276`, top center):** "Per-Frame Cost vs the 16.7ms Budget".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 180; y = ms 0 to 30, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels; x = frames 1–8, 12px `#444` tick labels centered under each pair.
- **Width bars:** orange `#d95926`, per-frame ms `[24, 23, 25, 24, 26, 23, 24, 25]`, bar width 24px.
- **Transform bars:** green `#008300`, per-frame ms `[2, 3, 2, 2, 3, 2, 2, 3]`, bar width 24px, drawn beside each orange bar.
- **Budget line:** dashed red `#e74c3c` (dash 6/4) horizontal line at 16.7ms across the plot, bold 12px red label "16.7ms budget (60fps)" above its right end.
- **Annotation (bold 13px orange `#d95926`, near frame 4, y=60):** "width blows every frame — ~40fps jank".
- **Annotation (bold 12px green `#008300`, below the axis, right-aligned at the plot right edge, y=277):** "transform: 2–3ms, smooth 60fps".
- **Caption (12px `#444`, bottom right):** "frame costs illustrative; 16.7ms budget exact for 60fps".

## Animating `width` When `transform` Would Do

**Tags:** `common mistake` (red), `layout thrash` (orange)

- **The confusion** — treating all CSS properties as equal; visually identical moves can differ 10× in cost
- **The trap** — animating `left`, `top`, `width`, or `margin` re-enters the pipeline at layout every frame
- **The ripple** — layout is contagious: resizing one drink card can reposition all 24 below it
- **The cheap twins** — `transform` moves or scales a finished layer and `opacity` fades it, composite-only
- **The tell** — an animation that is smooth on an empty test page but stutters on the real, full menu

*Example (italic):* `left: 0 → 260px` re-runs layout + paint + composite on all 24 cards every frame; `transform: translateX(260px)` moves one painted layer and touches composite alone.

**Common mistake:** Reaching for the property that names the effect ("I want it wider, so animate `width`") instead of the property that names the cheap stage — move and fade with `transform` and `opacity`, and save geometry changes for moments, not animations.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same 260px slide expressed two ways, each shown as the pipeline stages it re-enters — the `left` path lighting up three stages, the `transform` path one.

- **Title (bold 15px, `#1a5276`, top center):** "Same Slide, Two Routes Through the Pipeline".
- **Row 1 (y=95), label 12px `#444` at x=20:** "left: 0 → 260px"; three rounded boxes (120px wide, 44px tall, 8px radius) at x = 200, 360, 520 labeled "Layout" (orange border `#d95926`, fill `rgba(217,89,38,0.12)`), "Paint" (magenta `#d55181`, fill `rgba(213,81,129,0.12)`), "Composite" (green `#008300`, fill `rgba(0,131,0,0.12)`), joined by 3px `#6b7280` arrows; bold 12px orange label "every frame, all 24 cards" under the row at y=150.
- **Row 2 (y=205), label:** "transform: translateX(260px)"; two grey dashed ghost boxes at x = 200, 360 labeled "Layout skipped" / "Paint skipped" (1px dashed `#6b7280` border, 11px `#6b7280` text, no fill), then one solid green box at x=520 labeled "Composite" with bold 12px green "✓ 1 stage" beneath at y=260.
- **Box text:** 12px `#2c3e50` centered.
- **Annotation (bold 13px ink `#1a5276`, centered near y=285):** "enter the pipeline as late as you can".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); first-load stage timings `[12, 6, 9, 14, 3]` ms (total 44), update bills 5 / 11 / 1 ms, and per-frame costs `[24, 23, 25, 24, 26, 23, 24, 25]` vs `[2, 3, 2, 2, 3, 2, 2, 3]` ms are invented and labeled illustrative; the 16.7ms figure is the exact 60fps frame budget (1000/60). Stage colors are consistent across c1, c2, c4: layout orange, paint magenta, composite green (parse blue, style violet in c1 only).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
