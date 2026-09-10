# Aggregation Hides Detail

**Page type:** detail page (tutorial page: h1 + subtitle, then card-sections each with a two-column table layout — text left 50%, canvas right 50%; last section uses a 3-column 38/31/31 layout with two canvases)
**HTML title tag:** Aggregation Hides Detail

**Subtitle:** A flat total can hide one part doubling and another collapsing — sums cancel stories, so slice before you conclude.

## Six flat months on the dashboard

Tags: `core idea` (blue), `running example` (green)

- **The dashboard** — monthly revenue: $100k in January, $100k in February … $100k in June
- **The instinct** — a flat line reads as "stable business, nothing to see here"
- **The catch** — a total is a sum, and a sum can sit still while its parts race apart
- **Lost detail** — six numbers now stand in for thousands of orders
- **The question** — "flat" says how much came in; it says nothing about what is going on

*Example (italic):* The exec review took thirty seconds: "revenue is flat — next slide."

**Key point:** An aggregate answers "how much in total?" It cannot answer "what is happening underneath?" — those are different questions.

### Visualization (canvas `c1`, 720×300)

Line chart of the flat monthly total.

- **Shared data (fixed illustrative revenue in $k; online + in-store = 100 every month, Jan–Jun):**
  - months: `['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']`
  - ONLINE: `[20, 24, 28, 32, 36, 40]`
  - STORE: `[80, 76, 72, 68, 64, 60]`
  - TOTAL: `[100, 100, 100, 100, 100, 100]`
- **Title (bold 15px `#1a5276`, top center):** "Total monthly revenue — the whole dashboard".
- **Shared axis frame (all charts on this page):** L-shaped `#999` axis, padding top 54 / bottom 46 / left 56 / right 24; y scale 0–120 with gridlines (`#e5e9ef`) and mute labels "$0k"–"$120k" step $40k; month labels along the x axis (12px mute `#6b7280`).
- **Series:** TOTAL as ink `#1a5276` line, width 3, with 3.5px-radius dots; "100" printed in bold 12px `#2c3e50` above every point.
- **Annotation (orange `#d95926`, bold 14px, centered below the line):** "perfectly flat — so nothing is happening, right?".

## Slice it: two stories cancelling out

Tags: `worked example` (green), `do it yourself` (blue)

- **Slice by channel** — online: 20, 24, 28, 32, 36, 40 ($k); in-store: 80, 76, 72, 68, 64, 60
- **Check January** — 20 + 80 = 100; check June — 40 + 60 = 100; the total never moved
- **Story one** — online revenue doubled in six months (20 → 40)
- **Story two** — in-store lost a quarter of its revenue (80 → 60)
- **The cancellation** — online gains $4k a month, in-store loses $4k: +4 − 4 = 0

*Example (italic):* One slice turned "nothing happening" into two urgent stories at once.

**Key point:** Nothing was hidden in the data — it was hidden by the addition. The flat line is two steep lines summed.

### Visualization (canvas `c2`, 720×300)

Multi-line chart: the two channel slices plus the flat total.

- **Data:** TOTAL, STORE, ONLINE arrays from c1; same 0–120 axis frame and Jan–Jun labels.
- **Title:** "Same months, sliced by channel".
- **Series:** TOTAL as dashed mute `#6b7280` line (dash 6/4, width 2); STORE as orange `#d95926` line, width 3; ONLINE as green `#008300` line, width 3; all with dots at each point.
- **Labels (bold 13px, left-aligned):** mute "total: flat 100" near the total line's start; orange "in-store 80 → 60: lost a quarter" below the store line; green "online 20 → 40: doubled" above the online line.
- **Annotation (magenta `#d55181`, bold 13px, right-aligned mid-chart):** "+4 and −4 each month cancel exactly".

## Why the flat line is dangerous

Tags: `what happens next` (orange), `late warning` (red)

- **Growth saturates** — online cannot add $4k every month forever; suppose it slows to +$2k
- **Decline persists** — in-store keeps losing $4k a month; nothing stops it on its own
- **The bend** — total goes 98, 96, 94 … the flat line finally moves, months too late
- **The lag** — by December in-store is at $36k, down 55% from January's $80k
- **Early warning** — the channel slice flagged the decline in February; the total never did

*Example (italic):* When the total finally drops, the turnaround costs far more than a February fix would have.

**Key point:** The aggregate is always the last number to move. Watching only the total means reacting only after the damage is done.

### Visualization (canvas `c3`, 720×300)

Twelve-month projection: growth slows, decline persists, the total bends late.

- **Data:** months Jan–Dec; online12 `[20, 24, 28, 32, 36, 40, 42, 44, 46, 48, 50, 52]`; store12 `[80, 76, 72, 68, 64, 60, 56, 52, 48, 44, 40, 36]`; total12 `[100, 100, 100, 100, 100, 100, 98, 96, 94, 92, 90, 88]`. Same 0–120 axis frame.
- **Title:** "If online growth slows to +$2k and in-store keeps falling".
- **Divider:** vertical dashed `#bbb` line (dash 4/4, width 1.5) at Jun, with mute 12px label "what comes next →" to its right.
- **Series:** total12 in ink `#1a5276` width 3; store12 in orange `#d95926` width 2.5; online12 in green `#008300` width 2.5; dots at each point. End labels in bold 12px matching colors: "total", "online", "in-store" near the right edge.
- **Annotation (red `#e74c3c`, bold 13px, centered around month ~Aug, two lines):** "the total finally bends here —" / "in-store is already down 55%".

## Slice before you conclude

Tags: `best practice` (green), `common mistake` (red)

- **Try several cuts** — channel, region, customer type: each slices the same orders differently
- **A null slice helps** — by region, North and South are both flat $50k: story ruled out
- **Another story** — new customers grew 10 → 25 ($k) while returning fell 90 → 75
- **Two problems at once** — the same flat $100k hid a channel shift and a retention leak
- **The habit** — never say "stable" from a total; say "stable in every slice we cut"

*Example (italic):* Three slices, ten minutes: one showed nothing, two showed different fires.

**Common confusion:** flat does not mean stable. Flat means the movements summed to zero — and that is a claim you can only test by slicing.

This section uses the 3-column layout: text 38%, two canvases at 31% each.

### Visualization (canvas `c4a`, 420×340)

The region slice: two flat lines, story ruled out.

- **Data:** North `[50, 50, 50, 50, 50, 50]`, South `[50, 50, 50, 50, 50, 50]`; Jan–Jun, same 0–120 axis frame.
- **Title:** "Slice by region: nothing".
- **Series:** North as violet `#4a3aa7` solid line, width 3, with dots; South as yellow `#c98500` dashed line (dash 8/6, width 3) drawn 5px below the North line so both flat lines are visible.
- **Labels (bold 12px):** violet "North: flat $50k" above the lines; yellow "South: flat $50k" below them.
- **Annotation (mute `#6b7280`, bold 13px, two centered lines near the top):** "a slice that shows nothing" / "still earns its keep: story ruled out".

### Visualization (canvas `c4b`, 420×340)

The customer-type slice: a second hidden story.

- **Data:** newCust `[10, 13, 16, 19, 22, 25]`, returning `[90, 87, 84, 81, 78, 75]`; Jan–Jun, same 0–120 axis frame.
- **Title:** "Slice by customer type: another story".
- **Series:** returning as blue `#2a78d6` line, width 3; newCust as aqua `#199e70` line, width 3; dots at each point.
- **Labels (bold 12px):** blue "returning 90 → 75: leaking" below the returning line; aqua "new 10 → 25: growing" above the new-customer line.
- **Annotation (magenta `#d55181`, bold 13px, two lines mid-chart):** "same flat total also hid" / "a retention problem".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). `<h1>` + `.subtitle`, then four `.card-section` blocks, each an `<h2>` with a bottom border and a `table.layout` row: `.text-col` (50%) with `.tags` pills, one-line `<ul>` bullets opening with `<b>` terms, an italic `.example` line, and a `.key-point` callout; `.viz-col` (50%) holds one canvas. Section 4 uses the 3-column variant: `.text-col3` (38%) plus two `.viz-col3` (31%) cells each holding a 420×340 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem, `li b` in `#1a5276`. Canvas CSS `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = bg `rgba(26,82,118,0.12)` / `#1a5276`, green = bg `rgba(39,174,96,0.15)` / `#27ae60`, red = bg `rgba(231,76,60,0.12)` / `#e74c3c`, orange = bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvases:** intrinsic `width`/`height` attributes as specified per chart; shared `setup(id)` helper scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). A shared `frame(s, title, labels, maxY)` helper draws title, axes, $-labeled gridlines, month labels, and exposes `X(i)`/`Y(v)`/`line(vals, color, width, dash)` used by every chart. All data hardcoded (no `Math.random()`); values labeled illustrative in the JS comment. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions (this page has none).
