# Browser Scorecard

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Browser Scorecard

**Subtitle:** Comparing browsers on documented dimensions — openness, IT manageability, privacy defaults — turns a favorite-browser debate into a grid of checkable facts

## Five Hundred Laptops Need One Default Browser

**Tags:** `core idea` (blue), `comparison hub` (green), `IT decision` (orange)

- **The fleet** — an IT team must pick one default browser for 500 laptops: 400 Windows, 100 macOS
- **The debate** — five engineers each champion a different browser; opinions loop for weeks
- **The move** — replace opinions with a grid: browsers as rows, verifiable dimensions as columns
- **The columns** — open source, policy management, tracking defaults, update cadence, platforms
- **The rule** — every cell must cite a documented, checkable fact, never a preference

*Example (italic):* The team fills its first grid in one afternoon from vendor documentation, and a month of circular debate ends the same day.

**Key point:** A browser scorecard is a feature matrix — one row per candidate, one column per dimension you can verify from documentation, with nothing in the cells but facts.

### Visualization (canvas `c1`, 720×300)

Flow diagram: the fleet box on the left feeds five dimension boxes in the middle, which feed the single scorecard box on the right.

- **Title (bold 15px, `#1a5276`, top center):** "From 500 Laptops and Five Opinions to One Grid".
- **Fleet box:** rounded box at x=30, y=110, 175×56, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; two 12px `#2c3e50` lines "500 laptops" (bold) and "400 Windows + 100 macOS".
- **Dimension boxes (middle column, x=280, 190×30, 8px radius, stacked at y = 48, 92, 136, 180, 224):** labels 12px bold, each in its own color matching the c3 segment palette — "open-source browser?" blue `#2a78d6`, "policy management?" aqua `#199e70`, "tracking defaults?" green `#008300`, "update cadence?" yellow `#c98500`, "platform coverage?" violet `#4a3aa7`; fills are the same colors at 0.12 alpha.
- **Scorecard box:** rounded box at x=530, y=110, 165×56, fill `rgba(0,131,0,0.12)`, 2px `#008300` border; lines "the scorecard" (bold 12px `#008300`) and "4 browsers × 5 facts" (12px `#2c3e50`).
- **Arrows:** 2px `#6b7280` lines with small arrowheads from the fleet box to each dimension box and from each dimension box to the scorecard box.
- **Annotation (bold 13px magenta `#d55181`, centered near y=282):** "documented facts only — every cell must be checkable".
- **Caption (12px `#444`, bottom right):** "fleet counts illustrative".

## Scoring Four Browsers on Five Documented Dimensions

**Tags:** `worked example` (blue), `feature matrix` (green)

- **Open source** — Firefox is fully open; Chrome and Edge ship closed parts atop open-source Chromium; Safari opens only WebKit
- **Policy management** — Chrome, Edge and Firefox ship Windows Group Policy templates; Safari uses Apple MDM
- **Tracking defaults** — Edge, Firefox and Safari block known trackers out of the box; Chrome allows third-party cookies
- **Update cadence** — Chrome, Edge and Firefox release about every 4 weeks with extended channels; Safari ships with OS updates
- **Platforms** — Chrome, Edge and Firefox cover Windows, macOS, Linux, Android, iOS; Safari covers macOS and iOS

*Example (italic):* Scoring yes=2, partial=1, no=0 across the five columns gives Firefox 10, Edge 8, Chrome 6, Safari 5.

**Key point:** The cells are documented facts; the yes/partial/no scoring on top of them is a choice the team makes — and states — out loud.

### Visualization (canvas `c2`, 720×300)

Feature matrix as colored cells: 4 browser rows × 5 dimension columns, each cell a rounded rectangle reading yes / partial / no.

- **Title (bold 15px, `#1a5276`, top center):** "The Scorecard: Four Browsers × Five Documented Dimensions".
- **Column headers (11px `#6b7280`, two lines, centered over columns starting x=150, column pitch 112):** "open-src / browser", "policy / mgmt", "tracking / defaults", "update / cadence", "platform / coverage" at y=52/64.
- **Row labels (bold 13px `#2c3e50`, x=20, vertically centered on rows at y = 90, 130, 170, 210):** "Chrome", "Edge", "Firefox", "Safari".
- **Cells:** 104×30 rounded rects (6px radius) at x = 150, 262, 374, 486, 598, 12px bold centered text; values row by row —
  - Chrome: `["no", "yes", "no", "yes", "yes"]`
  - Edge: `["no", "yes", "yes", "yes", "yes"]`
  - Firefox: `["yes", "yes", "yes", "yes", "yes"]`
  - Safari: `["no", "partial", "yes", "partial", "partial"]`
- **Cell styles:** "yes" text `#008300` on fill `rgba(0,131,0,0.18)`; "partial" text `#c98500` on fill `rgba(201,133,0,0.18)`; "no" text `#6b7280` on fill `rgba(107,114,128,0.15)`.
- **Annotation (bold 12px ink `#1a5276`, centered near y=262):** "each cell cites vendor documentation, not opinion".
- **Caption (12px `#444`, bottom right):** "documented defaults at time of writing — recheck cells before deciding".

## One Grid Ends the Favorite-Browser Debate

**Tags:** `why it matters` (blue), `weights` (green), `audit trail` (orange)

- **Argue about weights** — the grid moves the fight from "which browser" to "which column matters most"
- **Visible drivers** — stacking each browser's points by column shows what earned the total
- **Audit trail** — a year later anyone can see why the choice was made and which fact would change it
- **Cheap updates** — when a vendor changes a default, the team edits one cell, not the whole decision
- **Same trick everywhere** — databases, cloud vendors, laptops: rows, documented columns, stated weights

*Example (italic):* A privacy-first team doubles the tracking-defaults column; a Windows-only shop doubles policy management — the grid makes either weighting explicit.

**Key point:** The scorecard's value is not the winner it picks but the argument it makes inspectable — facts in cells, judgment in weights.

### Visualization (canvas `c3`, 720×300)

Stacked horizontal bar chart: each browser's total score built from five colored segments, one per dimension.

- **Title (bold 15px, `#1a5276`, top center):** "Same Facts, Visible Drivers: Where Each Total Comes From".
- **Rows (bar height 22, bars start at x=130, 38px per point):** left-aligned 12px `#2c3e50` labels at x=20 —
  - "Firefox — 10" at y=72, segment points `[2, 2, 2, 2, 2]` (widths `[76, 76, 76, 76, 76]`)
  - "Edge — 8" at y=116, segment points `[0, 2, 2, 2, 2]` (widths `[0, 76, 76, 76, 76]`)
  - "Chrome — 6" at y=160, segment points `[0, 2, 0, 2, 2]` (widths `[0, 76, 0, 76, 76]`)
  - "Safari — 5" at y=204, segment points `[0, 1, 2, 1, 1]` (widths `[0, 38, 76, 38, 38]`)
- **Segment colors (in column order):** open-source blue `#2a78d6`, policy aqua `#199e70`, tracking green `#008300`, cadence yellow `#c98500`, platforms violet `#4a3aa7`.
- **Legend (y=248, five swatches 12×12 with 11px `#444` labels, spread from x=130):** "open source", "policy", "tracking", "cadence", "platforms".
- **Annotation (bold 12px magenta `#d55181`, right side near y=90):** "a single total hides which columns did the work".
- **Caption (12px `#444`, bottom right):** "yes=2, partial=1, no=0 — scoring scheme illustrative; cell values documented".

## A High Score Can't Fix a Missing Platform

**Tags:** `common mistake` (red), `hard constraint` (orange)

- **The average trap** — summing columns treats every gap as tradeable, but some gaps are absolute
- **The missing platform** — Safari has no Windows version; its last Windows release was in 2012
- **Filter first** — hard requirements ("runs on all 500 laptops") eliminate rows before any scoring
- **Score second** — only the survivors of the filter earn a place in the weighted comparison
- **Open ≠ open** — an open-source engine (Blink, WebKit) does not make the branded browser open source

*Example (italic):* Safari scores 5 of 10 on the grid, yet it cannot be the default because it runs on only 100 of the 500 laptops.

**Common mistake:** Averaging across a hard constraint — no number in the other columns rescues a browser that cannot run on 400 of your machines; gate on requirements, then score the rest.

### Visualization (canvas `c4`, 720×300)

Two-panel horizontal bar chart: grid score on the left, fleet coverage on the right, same four browser rows.

- **Title (bold 15px, `#1a5276`, top center):** "Filter Before You Score: Coverage Is a Gate, Not a Column".
- **Panel headers (bold 12px `#6b7280`):** "score (of 10)" centered near x=210 at y=55; "laptops covered (of 500)" centered near x=545 at y=55.
- **Rows at y = 85, 125, 165, 205, bar height 18, row labels 12px `#2c3e50` at x=20:** "Firefox", "Edge", "Chrome", "Safari".
- **Left panel bars (start x=105, 22px per point, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, 11px value labels at bar ends):** scores `[10, 8, 6, 5]` → widths `[220, 176, 132, 110]`.
- **Right panel bars (start x=420, 0.5px per laptop):** Firefox, Edge, Chrome covered `[500, 500, 500]` → green `#008300` bars width 250 with 11px "500" labels; Safari covered `100` → red `#e74c3c` bar width 50 with bold 12px red label "400 uncovered" to its right.
- **Divider:** vertical 1px `#e5e9ef` line at x=395 from y=65 to y=225.
- **Annotation (bold 13px orange `#d95926`, centered near y=255):** "Safari's 5 points can't reach the 400 Windows machines".
- **Caption (12px `#444`, bottom right):** "scores illustrative; Safari's platform list documented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded arrays above (no randomness); the 500-laptop fleet (400 Windows + 100 macOS), the yes=2/partial=1/no=0 scoring scheme, and the resulting totals (Firefox 10, Edge 8, Chrome 6, Safari 5) are invented and labeled illustrative; the matrix cells themselves reflect documented characteristics (Firefox fully open source; Chrome/Edge/Firefox Group Policy support and ~4-week release cycles with extended channels; Safari managed via Apple MDM, updated with OS releases, available on macOS/iOS only, last Windows release 2012; Edge/Firefox/Safari tracker blocking on by default; Chrome third-party cookies allowed by default) — text bullets and chart cells must stay in sync.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
