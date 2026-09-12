# Autoencoders

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Autoencoders

**Subtitle:** A network that squeezes its input through a tiny bottleneck and then rebuilds it — whatever survives the squeeze is the essence, and whatever won't rebuild is unusual

## Six Facts Through a Two-Number Phone Line

**Tags:** `core idea` (blue), `bottleneck` (orange), `compress & rebuild` (green)

- **The hotline** — a rental agent must describe each apartment over a line that carries only 2 numbers
- **The listing** — apartment A has six facts: 62 m², 2 rooms, floor 3, $1,150 rent, 1.2 km out, 27 yrs old
- **The squeeze** — the agent encodes all six facts into the code [0.8, −0.3] and sends just that
- **The rebuild** — a colleague decodes it back: 60 m², 2 rooms, floor 3, $1,180, 1.3 km, 25 yrs
- **The name** — encoder + bottleneck + decoder trained to copy its own input is an autoencoder
- **Self-taught** — no labels needed; the input itself is the answer the network is graded against

*Example (italic):* Two numbers can't store six facts literally — the pair works only because typical apartments follow patterns the network has learned.

**Key point:** An autoencoder is forced to summarize: the bottleneck is too small to copy, so it must learn what usually goes together and rebuild from that.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: a 6-box input column, arrows converging into a 2-box bottleneck, arrows fanning out to a 6-box reconstructed column.

- **Title (bold 15px, `#1a5276`, top center):** "Apartment A: Six Facts In, Two Numbers Across, Six Facts Back".
- **Input column (x=45, boxes 150×26, stacked from y=55, 8px gaps):** six rounded boxes, 1.5px blue `#2a78d6` border, fill `rgba(42,120,214,0.10)`, 12px `#2c3e50` text: "62 m²", "2 rooms", "floor 3", "$1,150 rent", "1.2 km out", "27 yrs old"; heading bold 12px `#2a78d6` "input (6 facts)" above.
- **Bottleneck (x=310, boxes 100×30, at y=125 and y=163):** two rounded boxes, 2px orange `#d95926` border, fill `rgba(217,89,38,0.12)`, bold 13px orange text "0.8" and "−0.3"; heading bold 12px `#d95926` "code (2 numbers)" above.
- **Output column (x=530, same box geometry as input):** six boxes, 1.5px green `#008300` border, fill `rgba(0,131,0,0.10)`, text: "60 m²", "2 rooms", "floor 3", "$1,180", "1.3 km", "25 yrs"; heading bold 12px `#008300` "rebuilt (6 facts)" above.
- **Arrows:** thin 1px `#6b7280` lines from each input box right edge to each bottleneck box left edge, and from each bottleneck box right edge to each output box left edge (12 + 12 lines).
- **Annotations:** bold 12px blue "encoder squeezes" centered at x=240, y=280; bold 12px green "decoder rebuilds" centered at x=470, y=280.
- **Caption (12px `#444`, bottom right):** "illustrative code values".

## Scoring the Rebuild by Hand

**Tags:** `worked example` (blue), `reconstruction error` (green)

- **The score** — reconstruction error: for each fact take |original − rebuilt| / original, then average
- **Size** — 62 m² came back as 60 m²: gap 2/62 = 3.2%; rooms and floor came back exact: 0%
- **The rest** — rent $1,150 → $1,180 is 2.6%, distance 1.2 → 1.3 km is 8.3%, age 27 → 25 is 7.4%
- **Apartment A** — average of 3.2, 0, 0, 2.6, 8.3, 7.4 gives a small 3.6% error: it compresses well
- **The houseboat** — 40 m², 1 room, floor 1, $900, 4.5 km, 60 yrs rebuilds terribly: 55% average

*Example (italic):* The houseboat comes back as 52 m², 2 rooms, floor 2, $1,080, 2.7 km, 36 yrs — gaps of 30, 100, 100, 20, 40, 40 percent, averaging 55%.

**Key point:** Reconstruction error is just "how far off is the copy" — small for listings that fit the learned patterns, large for ones that don't.

### Visualization (canvas `c2`, 720×300)

Dual-panel bar chart: per-feature percent gaps for apartment A (left) vs the houseboat (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Per-Feature Rebuild Gap: Apartment A vs the Houseboat".
- **Shared scale:** y axis 0–110%, baseline y=240, chart height 175; feature labels 11px `#444` below bars: "size", "rooms", "floor", "rent", "dist", "age".
- **Left panel (apartment A):** axis origin x=55, width 280; gaps `[3.2, 0, 0, 2.6, 8.3, 7.4]`; bars fill `rgba(42,120,214,0.45)`, 1px blue `#2a78d6` stroke; each bar's value labeled 11px blue above (zeros shown as "0"); blue bold 13px annotation "avg gap 3.6% — compresses well"; caption 12px `#444` "typical two-room flat".
- **Right panel (houseboat):** axis origin x=400, width 280; gaps `[30, 100, 100, 20, 40, 40]`; bars fill `rgba(217,89,38,0.5)`, 1px orange `#d95926` stroke; values labeled 11px orange above; orange bold 13px annotation "avg gap 55% — won't compress"; caption "the houseboat".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Listing That Won't Compress

**Tags:** `where it's used` (blue), `anomaly detection` (orange)

- **The trick** — run every listing through the autoencoder and flag the ones with a big rebuild error
- **Ten listings** — errors 3.6, 2.9, 4.8, 3.1, 5.2, 4.1, 2.5, 3.8, 55, 4.4 percent; nine sit under 6%
- **The flag** — a 10% threshold catches exactly one listing: the houseboat at 55%
- **Why it works** — the network only learned typical apartments, so oddballs rebuild badly
- **Same recipe** — fraud transactions, faulty sensors, and defective parts are caught the same way
- **Other jobs** — the 2-number codes also serve as compact features, plots, and denoised inputs

*Example (italic):* Nobody told the network what a houseboat is — it flagged the listing purely because it could not rebuild it.

**Key point:** An autoencoder turns "is this normal?" into a number you can threshold — no labeled examples of the weird cases required.

### Visualization (canvas `c3`, 720×300)

Bar chart of reconstruction error for ten listings with a dashed threshold line; one bar towers above it.

- **Title (bold 15px, `#1a5276`, top center):** "Rebuild Error per Listing: One Won't Compress".
- **Data:** errors `[3.6, 2.9, 4.8, 3.1, 5.2, 4.1, 2.5, 3.8, 55, 4.4]` with labels "A", "B", "C", "D", "E", "F", "G", "H", "boat", "J".
- **Axes:** origin x=60, width 600, baseline y=245, chart height 185, y scale 0–60%; y ticks at 0, 10, 20, 30, 40, 50, 60 with 11px `#444` labels and light `#e5e9ef` gridlines.
- **Bars:** nine bars fill `rgba(42,120,214,0.45)` with 1px blue `#2a78d6` stroke; the "boat" bar fill `rgba(231,76,60,0.5)` with 2px red `#e74c3c` stroke; labels 12px `#444` below each bar; "55" labeled bold 12px red above the boat bar.
- **Threshold:** dashed orange `#d95926` (dash 5/4) horizontal line at 10%, bold 12px orange label "flag threshold 10%" at its right end.
- **Annotation:** bold 13px red near the boat bar: "the houseboat — flagged with zero labels".
- **Caption (12px `#444`, bottom right):** "illustrative errors".

## The Bottleneck Is the Whole Point

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The temptation** — a wider code means better copies, so why not use 6 numbers for 6 facts?
- **The trap** — with a 6-wide code the network learns to copy everything, houseboat included
- **The numbers** — at width 6 errors drop to 0.1% (typical) and 0.3% (boat): the gap disappears
- **The sweet spot** — width 2 keeps typical error low (3.6%) while the boat stays high (55%)
- **Reading rule** — a tiny rebuild error is not success; it may mean the bottleneck taught nothing
- **Not zip** — unlike zip it is lossy and dataset-specific: great on apartments, useless on songs

*Example (italic):* A team widened the code until errors were near zero, then wondered why their anomaly detector stopped flagging anything.

**Common mistake:** Judging an autoencoder only by low reconstruction error. If the bottleneck is wide enough to copy, it compresses nothing, learns nothing, and flags nothing.

### Visualization (canvas `c4`, 720×300)

Two-line chart: rebuild error vs bottleneck width for a typical listing and for the houseboat; the useful gap between the lines collapses as the code widens.

- **Title (bold 15px, `#1a5276`, top center):** "Rebuild Error vs Code Width: the Gap Is the Value".
- **Data:** widths `[1, 2, 3, 4, 5, 6]`; typical-listing error `[12, 3.6, 2.0, 1.1, 0.4, 0.1]`; houseboat error `[70, 55, 30, 12, 3, 0.3]`.
- **Axes:** origin x=65, width 560, baseline y=245, chart height 185, y scale 0–75%; x labels "1".."6" 12px `#444` under ticks with axis caption "numbers in the code (bottleneck width)" 12px `#444` centered below; y ticks 0/25/50/75 with light `#e5e9ef` gridlines.
- **Typical line:** blue `#2a78d6` 3px line with 4px dots; bold 12px blue label "typical listing" near its left end.
- **Houseboat line:** orange `#d95926` 3px line with 4px dots; bold 12px orange label "houseboat" near its left end.
- **Gap marker:** vertical dashed green `#008300` (dash 4/3) segment at width 2 between the two lines (3.6% up to 55%), bold 13px green label "width 2: gap 3.6% vs 55%".
- **Annotation:** bold 12px red `#e74c3c` near width 6: "width 6 = plain copying — gap gone".
- **Caption (12px `#444`, bottom right):** "illustrative errors".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
