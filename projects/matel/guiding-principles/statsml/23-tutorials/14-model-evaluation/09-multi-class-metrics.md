# Multi-Class Metrics

**Page type:** detail page (tutorial layout: h1 + subtitle, then one `.card-section` per concept, each an h2 + two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Multi-Class Metrics

**Subtitle:** A support-ticket router picks one of four queues — one accuracy number hides four different stories, and the averages you build from them disagree on purpose

## One Router, Four Unequal Piles

**Tags:** `core idea` (blue), `running example` (green)

- **The data** — 1,000 support tickets, auto-routed into 4 queues
- **Class sizes** — billing 500, bug 300, refund 150, other 50
- **Overall** — 855 tickets land in the right queue: accuracy 85.5%
- **Per class it's uneven** — billing 92% right, but "other" only 50%
- **One blended number** — accuracy can't say which queue is failing

*Example:* The team celebrating 85.5% never noticed that half the "other" tickets were landing in the wrong queue.

**Key point:** With more than two classes, the first move is always the same — break the one number into per-class numbers.

### Visualization (canvas `c1`, 720×300)

Horizontal stacked bars: class volume with the correctly-routed share filled in.

- **Title (bold 15px, `#1a5276`, top center):** "Four Queues: Volume, and How Much Was Routed Right".
- **Data:** classes `['billing', 'bug', 'refund', 'other']`, sizes `[500, 300, 150, 50]`, correct `[460, 255, 115, 25]`, recall `[92, 85, 77, 50]`.
- **Layout:** 4 horizontal bars starting at x=130, 0.8px per row unit, bar height 30, first bar at y=56, 48px vertical spacing between bars.
- **Bars:** full class-size bar in light gray `rgba(107,114,128,0.18)`, overlaid with a solid segment for the correct count in the class color; class colors: billing `#2a78d6` (blue), bug `#199e70` (aqua), refund `#4a3aa7` (violet), other `#d95926` (orange); each bar outlined 1px `#6b7280`.
- **Labels:** class name bold 13px `#2c3e50` right-aligned left of each bar; to the right of each bar in the class color bold 12px: "460 / 500 right (92%)", "255 / 300 right (85%)", "115 / 150 right (77%)", "25 / 50 right (50%)".
- **Legend line (12px `#6b7280`, below bars):** "solid = correctly routed, light = the rest of that class".
- **Callout (bold 13px `#e74c3c`, centered at y=288):** "overall accuracy 855/1000 = 85.5% — but \"other\" is a coin flip at 50%".

## Drawing the 4×4 Confusion Matrix

**Tags:** `worked example` (green), `core idea` (blue)

- **Rows are truth**, columns are the router's pick — one cell per combination
- **Diagonal = correct** — 460 + 255 + 115 + 25 = 855 tickets
- **Rows sum to class size** — the billing row adds up to 500
- **Reading a miss** — cell (bug, billing) = 30: bug tickets sent to billing
- **The 2×2 grows to 4×4** — 12 distinct ways to be wrong instead of 2

*Example:* Every off-diagonal cell is a specific complaint: 30 bug reports queued to billing agents who can't fix bugs.

**Key point:** The confusion matrix is the full story — every metric on this page is just a different summary of these 16 cells.

### Visualization (canvas `c2`, 720×300)

Heatmap grid: the 4×4 confusion matrix with per-row recall column and one highlighted miss cell.

- **Title (bold 15px, `#1a5276`, top center):** "Rows = Truth, Columns = the Router’s Pick".
- **Matrix data (rows = actual, columns = predicted, order billing/bug/refund/other):**
  - billing: `[460, 20, 15, 5]`
  - bug: `[30, 255, 10, 5]`
  - refund: `[20, 10, 115, 5]`
  - other: `[10, 10, 5, 25]`
- **Grid geometry:** origin x=200, y=76; cell 95 wide × 44 tall; white 2px cell borders.
- **Headers:** "predicted →" bold 12px `#6b7280` above the columns; column names in class colors (`#2a78d6`, `#199e70`, `#4a3aa7`, `#d95926`); row labels right-aligned in class colors as "billing (500)", "bug (300)", "refund (150)", "other (50)"; rotated vertical label "actual ↓" bold 12px `#6b7280` at the left.
- **Cell fills:** diagonal cells green `rgba(0,131,0, 0.25 + 0.55·v/460)`; off-diagonal cells orange `rgba(217,89,38, 0.08 + 0.5·v/30)`; cell value bold 13px, white text on dark diagonal cells (v > 200), otherwise `#2c3e50`.
- **Recall column:** header "recall" bold 12px `#6b7280` at the right; values 92%, 85%, 77% in green `#008300` and 50% (other) in red `#e74c3c`.
- **Highlight:** the (bug, billing) cell outlined 2.5px magenta `#d55181`, with magenta bold 12px annotation below the row labels: "30 bug tickets → billing queue".
- **Callout (bold 13px `#e74c3c`, centered at y=288):** "diagonal: 460 + 255 + 115 + 25 = 855 correct → accuracy 85.5%".

## Per-Class Scores, Three Ways to Average

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **Recall per class** — diagonal ÷ row: billing 92%, bug 85%, refund 77%, other 50%
- **Precision per class** — diagonal ÷ column: 88%, 86%, 79%, 63%
- **Macro recall** — plain mean of the four: (92+85+77+50)/4 = 76%
- **Micro** — pool all 1,000 tickets first: 855/1000 = 85.5%
- **Weighted** — mean weighted by class size: also 85.5%, echoing the big classes

*Example:* Macro treats "other" (50 tickets) exactly like billing (500) — one bad rare class pulls it from 85.5% down to 76%.

**Key point:** Micro asks "what share of tickets went right"; macro asks "how good is the router on a typical class". They disagree exactly when classes are unequal.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: per-class recall with macro and micro dashed reference lines.

- **Title (bold 15px, `#1a5276`, top center):** "Per-Class Recall, and Where the Averages Land".
- **Data:** recall `[92, 85, 77, 50]` for billing (500), bug (300), refund (150), other (50).
- **Axes:** padding top 56, bottom 54, left 70, right 195; y scale 0 to 105% of plot height with tick labels 0%, 50%, 100% (12px `#6b7280`); light gridlines `#e5e9ef` at 50% and 100%; axis lines `#999`.
- **Bars:** 66px wide, one per quarter of the plot width, filled in class colors `#2a78d6`, `#199e70`, `#4a3aa7`, `#d95926`; value label bold 13px `#2c3e50` above each bar ("92%" etc.); x labels 12px "billing (500)", "bug (300)", "refund (150)", "other (50)".
- **Reference lines (dashed 7/5, width 2, labeled bold 12px to the right of the plot):** at 85.5% in `#1a5276` labeled "micro = 855/1000 = 85.5%"; at 76% in `#e74c3c` labeled "macro = mean = 76%".
- **Callout (bold 13px `#e74c3c`, centered at bottom):** "\"other\" at 50% drags macro 9.5 points below micro".

## Which Average Matches Which Goal

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Volume goal** — "route most tickets right" → micro / weighted (85.5%)
- **Every-class goal** — "no queue silently broken" → macro (76%)
- **Rare classes that matter** — fraud types, diseases: macro, never micro alone
- **The gap is a signal** — macro 76% vs micro 85.5% means rare classes lag
- **Common mistake** — quoting "F1" without saying macro or micro

*Example:* A report's "F1 = 0.85" turned out to be micro; the macro F1 was near 0.76 — the reviewer caught it by asking which.

**Common mistake:** Picking the average after the experiment, from whichever number looks better — choose it before, from the business goal.

### Visualization (canvas `c4`, 720×300)

Two horizontal 100%-stacked weight bars comparing the class weights behind micro vs macro averaging.

- **Title (bold 15px, `#1a5276`, top center):** "Same Matrix, Different Weights per Class".
- **Geometry:** bars start at x=70, width 440, height 44; first bar at y=74, second at y=180.
- **Micro bar (y=74):** segment weights `[0.5, 0.3, 0.15, 0.05]` in class colors `#2a78d6`, `#199e70`, `#4a3aa7`, `#d95926`; sub-label 12px `#6b7280` above: "micro / weighted — each ticket counts equally"; in-segment white bold 12px labels "billing 50%", "bug 30%", "refund 15%" (segments >55px); orange bold 11px "other 5%" below the bar's right end; result bold 16px `#1a5276` at right: "→ 85.5%".
- **Macro bar (y=180):** segment weights `[0.25, 0.25, 0.25, 0.25]` in the same class colors; sub-label: "macro — each class counts equally"; in-segment white labels "billing 25%", "bug 25%", "refund 25%", "other 25%"; result bold 16px `#e74c3c` at right: "→ 76.0%".
- **Bar outlines:** 1px `#6b7280`.
- **Callouts (centered):** bold 13px `#e74c3c` at y=272: "macro hands \"other\" 5x the weight micro gives it — pick the average from the goal"; 12px `#6b7280` at y=292: "recall averages from the same router: micro 85.5%, macro 76%".

## Regeneration instructions

- **Template:** tutorials topic-page layout. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` line, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one canvas.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` line, one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Canvas:** intrinsic 720×300 attributes; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette:** shared `P` object — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; site palette `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange. Class colors are blue/aqua/violet/orange for billing/bug/refund/other.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
