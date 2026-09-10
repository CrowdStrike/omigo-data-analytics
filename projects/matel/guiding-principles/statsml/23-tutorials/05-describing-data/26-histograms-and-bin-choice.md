# Histograms & Bin Choice

**Page type:** detail page (tutorial page: h1 + subtitle, then card-sections each with a two-column table layout — text left 50%, canvas right 50%; one section uses a 3-column 38/31/31 layout with two canvases)
**HTML title tag:** Histograms &amp; Bin Choice

**Subtitle:** A histogram is counting values into buckets — and the picture you get depends on how many buckets you chose.

## 200 checkout times, sorted into buckets

Tags: `core idea` (blue), `running example` (green)

- **The data** — 200 checkout times from one store; fastest 1.2 minutes, slowest 8.2
- **The trick** — split 0–10 minutes into equal buckets, count the times landing in each
- **The bars** — each bar's height is a count; a taller bar means more checkouts took that long
- **The shape** — with 20 buckets, two humps appear: quick trips near 2 min, slow ones near 6
- **The name** — this bucket-and-count picture is a histogram; each bucket is called a bin

*Example (italic):* The tallest bar says 40 of the 200 checkouts took between 2.0 and 2.5 minutes.

**Key point:** A histogram shows the shape of your data — here, two kinds of checkouts (express and full cart), not one average kind.

### Visualization (canvas `c1`, 720×300)

Histogram of the fixed 200 checkout times with 20 bins over 0–10 minutes.

- **Shared data (every chart on this page uses this exact array of 200 checkout times, in minutes):**
  `[3.6,2.2,1.7,2.2,2.1,2.7,1.8,1.6,1.4,3, 2.8,2.8,2,2.2,3.2,1.8,2.6,2.6,2.5,1.6, 2.8,2.3,1.6,2.8,2,1.7,2.9,2.1,1.6,2.9, 2.6,2.5,2.6,1.2,2.6,2.3,2.7,2.1,2.5,1.9, 1.8,2.6,2.4,1.8,1.8,2.3,3.7,2.3,2.7,1.8, 2.6,3,2.4,2.4,3.2,1.7,1.4,2.5,2.2,2.7, 2.2,2,1.5,2.4,1.3,1.6,2.2,1.6,3.1,3.1, 3,2.3,1.4,2.1,1.7,2.1,1.9,1.9,2.1,2.3, 2.2,2.3,1.9,2.8,1.7,2.5,2.9,1.7,1.5,2.8, 2.7,1.9,2,2,2.4,1.9,2.5,2,1.8,2.6, 2.2,2,2.2,2.5,3.1,1.4,2,1.9,1.9,2.1, 1.9,2,1.6,2.6,2.3,2,1.8,2.3,2.7,1.4, 5.8,5.7,6.4,6.5,6,7.7,5.9,5.9,5.9,4, 6.2,4.5,5.8,4.5,4.1,4.9,6.2,5.5,6.5,5.7, 5.9,5.7,7.1,7.9,6,6.9,5.1,6.3,6.3,6.4, 6.4,8.2,5.6,5.7,4.3,4.3,6.4,4.1,7.2,5.9, 5.6,6.4,6.4,5.8,5.7,5.7,4.9,5.6,4.5,5.6, 5.7,5.9,5,6.5,4.3,5.5,6.8,7.1,5.3,5.7, 6.2,5.4,4.4,7,6,6.2,6.5,5.3,6.1,3.2, 5.7,6.9,4.7,5.8,4.7,4.2,5.5,6,6.2,6]`
- **Binning:** counts computed by dividing 0–10 into `bins` equal buckets (`width = 10/bins`), each value placed in `floor(value/width)` (overflow clamped to last bin).
- **Chart type:** vertical bar histogram, 20 bins. Bar fill `rgba(42,120,214,0.55)` (blue). Bar height scaled to max count over the plot height. Bars have a 2px gap (0.5px when bins > 40).
- **Title (bold 15px, ink `#1a5276`, top center):** "200 checkout times — 20 bins".
- **Axes:** L-shaped axis in `#999`, padding top 52 / bottom 46 / left 52 / right 20. X ticks at minutes 0, 2, 4, 6, 8, 10 (12px mute `#6b7280`); x-axis label "checkout time (minutes)". Y labeled only with the max count (top) and "0" (bottom), right-aligned left of the axis.
- **Annotations (bold 13px):** green `#008300` "express lane ~2 min" at x=2.2 min near the top; violet `#4a3aa7` "full carts ~6 min" at x=6.6 min slightly lower; orange `#d95926` "two humps = two kinds of checkout" at x=6.3 min lower in the plot.

## Counting the buckets by hand

Tags: `worked example` (green), `do it yourself` (blue)

- **Pick a width** — make each bucket 1 minute wide: 0–1, 1–2, … up to 9–10
- **Count** — 39 checkouts land in 1–2, 71 in 2–3, 11 in 3–4, 15 in 4–5, and so on
- **Check** — add all ten counts: 0+39+71+11+15+31+26+6+1+0 = 200, nothing lost
- **Draw** — ten bars with those heights; that is the entire recipe
- **Notice** — even at 1-minute width the two humps show: peaks at 2–3 and 5–6

*Example (italic):* A 3.6-minute checkout goes in the 3–4 bucket; a 5.9-minute one goes in 5–6.

**The recipe:** choose a bucket width, count values into buckets, draw the counts as bars. Every histogram, every time.

### Visualization (canvas `c2`, 720×300)

Same histogram routine with 10 bins (1-minute buckets) and each bar's count printed above it.

- **Data:** the shared 200 checkout times; 10 bins give counts `[0, 39, 71, 11, 15, 31, 26, 6, 1, 0]`.
- **Chart type:** vertical bar histogram, bar fill `rgba(25,158,112,0.55)` (aqua). Count labels in bold 12px `#2c3e50` above each nonzero bar.
- **Title:** "Same 200 times — ten 1-minute buckets, counted by hand".
- **Axes:** same axis frame as c1 (minute ticks 0–10 step 2, "checkout time (minutes)" label, max-count and 0 on the y side).
- **Annotation:** orange `#d95926` bold 13px, centered at ~68% plot width near the top: "0+39+71+11+15+31+26+6+1+0 = 200".

## Same 200 times, different knob: hill or noise

Tags: `the knob` (orange), `common mistake` (red)

- **Too few** — 5 bins melt both groups into one lumpy hill; the humps vanish
- **Too many** — 80 bins scatter the counts; 35 buckets end up empty
- **Same data** — all three pictures use the identical 200 checkout times
- **The trap** — a chart reader assumes the shape is the data; it is data + your knob
- **Real cost** — 5 bins would hide that express and full-cart lanes need different staffing

*Example (italic):* With 5 bins the 2–4 bucket holds 82 checkouts from both groups — one fake hump.

**Key point:** Smooth hill, two humps, or noise — the story changed while the data never did.

This section uses the 3-column layout: text 38%, two canvases at 31% each.

### Visualization (canvas `c3a`, 420×340)

- **Data:** shared 200 times, 5 bins (counts printed above bars).
- **Chart type:** vertical bar histogram, fill `rgba(201,133,0,0.55)` (yellow). Same axis frame (minute ticks, x label, max/0 y labels).
- **Title:** "5 bins: one smooth hill".
- **Annotation:** orange `#d95926` bold 13px, two lines centered at ~62% plot width: "the two groups merged —" / "humps hidden".

### Visualization (canvas `c3b`, 420×340)

- **Data:** shared 200 times, 80 bins (no count labels).
- **Chart type:** vertical bar histogram, fill `rgba(213,81,129,0.75)` (magenta), 0.5px bar gap. Same axis frame.
- **Title:** "80 bins: jagged noise".
- **Annotation:** magenta `#d55181` bold 13px at ~60% plot width: "35 of 80 buckets are empty".

## How many bins should you use?

Tags: `rule of thumb` (blue), `best practice` (green)

- **Starting point** — try roughly the square root of the count: √200 ≈ 14 bins
- **Always try three** — half it and double it too (7, 14, 28); look at all three pictures
- **Trust what persists** — a hump that survives several bin widths is real, not an artifact
- **Watch for empties** — when many buckets hold 0 or 1 value, you have too many bins
- **Defaults are guesses** — your plotting library picks a bin count; it never saw your data's story

*Example (italic):* Here the two humps show up at 10, 14, 20, and 28 bins alike — so they are real.

**Common confusion:** more bins is not more truth. Past the point where buckets run empty, you are drawing noise, not detail.

### Visualization (canvas `c4`, 720×300)

Line chart: number of empty buckets vs number of bins for the same 200 times.

- **Data:** bins tried `[5, 10, 20, 40, 80]` → empty-bucket counts `[0, 2, 5, 14, 35]` (computed from the 200 times; hardcoded).
- **Title (bold 15px `#1a5276`):** "Empty buckets as the bin count grows (same 200 times)".
- **Axes:** L-shaped `#999` axis, padding top 56 / bottom 50 / left 60 / right 30; x scale 0–80 (tick labels at each tried bin count, 12px mute), x label "number of bins"; y scale 0–40, rotated y label "empty buckets" at the left.
- **Marker:** vertical dashed green `#008300` line (dash 5/4, width 2) at x=14, labeled to its right in bold 12px green: "√200 ≈ 14: a good start".
- **Series:** connected blue `#2a78d6` line, width 3, with 5px-radius blue dots; each point's empty count printed above it in bold 12px `#2c3e50`.
- **Annotation:** magenta `#d55181` bold 13px, right-aligned near the y=35 point: "at 80 bins, 35 buckets hold nothing — you are drawing noise".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`; skeleton copied from `most-powerful-signals/07-social-graph-connections.html`). `<h1>` + `.subtitle`, then four `.card-section` blocks, each an `<h2>` with a bottom border and a `table.layout` row: `.text-col` (50%) with `.tags` pills, a `<ul>` of one-line bullets opening with `<b>` terms, an italic `.example` line, and a `.key-point` callout; `.viz-col` (50%) holds one canvas. Section 3 uses the 3-column variant: `.text-col3` (38%) plus two `.viz-col3` (31%) cells each holding a 420×340 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem, `li b` in `#1a5276`. Canvas CSS `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = bg `rgba(26,82,118,0.12)` / `#1a5276`, green = bg `rgba(39,174,96,0.15)` / `#27ae60`, red = bg `rgba(231,76,60,0.12)` / `#e74c3c`, orange = bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvases:** intrinsic `width`/`height` attributes as specified per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates, and clears. Histograms drawn by a shared `drawHist` helper over the fixed 0–10 minute range using the hardcoded 200-value array (no `Math.random()`).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions (this page has none).
