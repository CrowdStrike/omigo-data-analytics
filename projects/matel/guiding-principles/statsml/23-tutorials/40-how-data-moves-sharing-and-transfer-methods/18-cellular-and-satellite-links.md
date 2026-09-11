# Cellular & Satellite Links

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cellular & Satellite Links

**Subtitle:** Data over the air — from 2G trickles to 5G streams on the phone network, and satellite links where no cable or tower reaches

## Alice's Field Kit Climbs the Ladder: 2G to 5G

**Tags:** `core idea` (blue), `generations` (green), `typical speeds` (orange)

- **The running example** — Alice's field kit uploads over whatever network each site has, 2G to 5G
- **2G (~40 kbit/s)** — enough for texts and ringtones; a single photo took minutes
- **3G (~2 Mbit/s)** — the mobile web and email arrived; pages loaded in seconds
- **4G (~30 Mbit/s)** — video streaming and the app economy; the phone became the computer
- **5G (100+ Mbit/s)** — feels like home broadband; 150 Mbit/s is typical here, not the peak
- **Tethering** — a laptop borrows the phone's link; the tower cannot tell the difference

*Example (italic):* Alice's 2005 phone took a minute to fetch one email; her 5G phone streams a match in HD on a moving train.

**Key point:** Each generation is a new radio system, not a tune-up — typical speeds climbed roughly 5–50× per step.

### Visualization (canvas `c1`, 720×300)

Staircase of four steps, one per generation, step heights proportional to log10 of typical speed.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "Four Generations, One Ladder (typical real-world speeds)".
- **Annotation (bold 13px orange `#d95926`, centered, y=55):** "each generation multiplies typical speed ~5–50× — step heights on a log scale".
- **Steps:** baseline y=245; four blocks 130px wide with 10px gaps, heights = log10(speed in kbit/s) × 38 px:
  - 2G, x 90–220, height 60 (log10(40)≈1.6): fill `rgba(74,58,167,0.22)`, 2px `#4a3aa7` border
  - 3G, x 230–360, height 125 (log10(2000)≈3.3): fill `rgba(42,120,214,0.22)`, 2px `#2a78d6` border
  - 4G, x 370–500, height 170 (log10(30000)≈4.5): fill `rgba(0,131,0,0.18)`, 2px `#008300` border
  - 5G, x 510–640, height 197 (log10(150000)≈5.2): fill `rgba(217,89,38,0.22)`, 2px `#d95926` border
- **Per step:** generation name bold 13px in the border color inside the block near the bottom (y=235); typical speed 12px `#444` just above the block's top edge ("~40 kbit/s", "~2 Mbit/s", "~30 Mbit/s", "~150 Mbit/s"); what it enabled 12px `#444` centered below the baseline at y=265 ("texts, ringtones", "mobile web, email", "streaming, apps", "broadband-class").
- **Baseline:** 1px `#999` from x=90 to x=640 at y=245.
- **Caption (12px `#6b7280`, bottom center, y=290):** "typical real-world speeds, not peaks — actual rates vary by network and signal".

## One 100 MB Video, Sent Four Times

**Tags:** `worked example` (blue), `do the division` (green), `log scale` (orange)

- **The task** — Alice sends one 100 MB clip; 100 MB × 8 = 800 megabits to push
- **The formula** — time = megabits ÷ speed in Mbit/s; one division per generation
- **2G at 0.04** — 800 ÷ 0.04 = 20,000 s ≈ 5 h 33 min; nobody sent video on 2G
- **3G at 2** — 800 ÷ 2 = 400 s ≈ 6.7 min; possible, painful
- **4G at 30** — 800 ÷ 30 ≈ 27 s; sharing video became casual
- **5G at 150** — 800 ÷ 150 ≈ 5.3 s; done before the phone is back in the pocket

*Example (italic):* The same clip that would fill a 2G lunch break uploads on 5G in about the time of one blink-and-scroll.

**Key point:** Divide megabits by Mbit/s: the same video goes from ~5.5 hours to ~5 seconds — about 3,750× faster from 2G to 5G.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of send time per generation on a log time axis.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "Sending One 100 MB Video (log time scale)".
- **Annotation (bold 13px orange `#d95926`, centered, y=52):** "same video, ~3,750× faster — a 2G lunch break becomes a 5G blink".
- **Axis:** log10(seconds) 0–4.5 mapped to px x=170 (1 s) through x=660; baseline 1px `#999` at y=250; `#e5e9ef` vertical gridlines with 12px `#444` tick labels at y=266: "1 s" (x=170), "10 s" (x=279), "1 min" (x=364), "10 min" (x=472), "1 h" (x=557); axis caption 12px `#444` centered at y=286: "time to send, log scale →".
- **Rows (bars 30px tall centered on row y, drawn from x=170 to X(log10(seconds)); left-aligned 12px `#444` two-line labels at x=20; hardcoded seconds):**
  - y=85: "2G / 0.04 Mbit/s", 20,000 s → bar end x≈638, fill `rgba(74,58,167,0.25)`, 2px `#4a3aa7` border, value bold 13px `#4a3aa7` "5 h 33 min" drawn inside the bar's right end
  - y=130: "3G / 2 Mbit/s", 400 s → bar end x≈453, fill `rgba(42,120,214,0.25)`, 2px `#2a78d6` border, value "6.7 min" right of bar end
  - y=175: "4G / 30 Mbit/s", 26.7 s → bar end x≈325, fill `rgba(0,131,0,0.20)`, 2px `#008300` border, value "27 s" right of bar end
  - y=220: "5G / 150 Mbit/s", 5.3 s → bar end x≈249, fill `rgba(217,89,38,0.25)`, 2px `#d95926` border, value "5.3 s" right of bar end
- **Caption (12px `#6b7280`, bottom right):** "typical speeds; line rate only, no protocol overhead".

## Where No Tower Reaches: Satellites Fill the Gaps

**Tags:** `satellite` (blue), `latency` (orange), `emergency text` (red)

- **Geostationary (GEO)** — one satellite parked 36,000 km up sees a third of the planet
- **The price of altitude** — the signal climbs and falls twice: ~600 ms round trip at light speed
- **Low-earth orbit (LEO)** — constellations at ~550 km cut the round trip to ~40 ms
- **Moving targets** — a LEO satellite crosses the sky in minutes; the link hands off constantly
- **Direct to phone** — ordinary phones now send emergency texts by satellite: bytes, not megabytes
- **Minutes per message** — a clear sky and patience; it is a lifeline, not a data link

*Example (italic):* Bob's cargo ship logs its position over a GEO link; every ping carries a built-in half second of pure distance.

**Key point:** Satellite trades speed and latency for coverage — most of the delay is distance, not equipment.

### Visualization (canvas `c3`, 720×300)

Split panel: left = altitude schematic (dish on the ground, LEO low, GEO high, dashed signal paths); right = round-trip latency bars for GEO vs LEO.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "The Delay Is the Distance: GEO vs LEO".
- **Divider:** dashed 1px `#bdc3c7` vertical line at x=400 from y=40 to y=285.
- **Left panel:** ground line 2px `#008300` from x=30 to x=380 at y=255; dish = small triangle at (110, 255) in `#1a5276` with 12px `#444` label "ground dish" below at y=272; satellites drawn as a small body rect with a solar panel rect either side:
  - LEO at (180, 175), body/panels in `#199e70`, label bold 13px `#199e70` at right: "LEO — ~550 km up"
  - GEO at (270, 70), body/panels in `#d95926`, label bold 13px `#d95926` at right offset left so it fits: "GEO — 36,000 km up"
  - dashed 1.5px signal lines from the dish tip to each satellite, in each satellite's color
  - note 11px `#6b7280` centered x=205, y=290: "altitudes not to scale"
- **Right panel:** header 12px `#444` centered x=550, y=62 two lines: "round trip, ground → satellite → ground" / "(typical)"; horizontal bars from x=430, scale 0.38 px per ms:
  - y=105: GEO bar 600 ms → 228px, fill `rgba(217,89,38,0.25)`, 2px `#d95926` border, 20px tall, value bold 13px `#d95926` "GEO ~600 ms" left-aligned just under the bar (y=140)
  - y=155: LEO bar 40 ms → 15px, fill `rgba(25,158,112,0.30)`, 2px `#199e70` border, 20px tall, value bold 13px `#199e70` "LEO ~40 ms" right of bar end
- **Annotation (bold 13px ink `#1a5276`, centered x=550, two lines y=205/223):** "four legs of 36,000 km each —" / "≈ 0.5 s of physics before any processing".
- **Bottom note (12px `#6b7280`, centered x=550, two lines y=258/274):** "direct-to-phone emergency text:" / "bytes per message, minutes to send".

## Full Bars, Slow Data: What the Data Scientist Receives

**Tags:** `where it's used` (blue), `shared cell` (orange), `common mistake` (red)

- **Distance costs speed** — the tower's signal weakens with range; the cell edge gets a trickle
- **The cell is shared** — one tower's capacity splits among every device on it; rush hour bites
- **Bars ≠ speed** — signal bars measure radio strength, not how crowded the cell is
- **Field telemetry** — sensors, trucks, and ships upload over cellular or satellite links
- **Cost shapes data** — per-MB pricing pushes devices to send summaries, not raw streams
- **Design upstream** — a missing field was often trimmed at the modem, never lost in the pipeline

*Example (italic):* Alice's fleet trucks compress a day of engine readings into a few kilobytes — sending the raw stream over cellular would cost too much.

**Key point (Common mistake callout):** Full bars promise nothing about speed — and the link's cost and capacity decide what telemetry ever exists.

### Visualization (canvas `c4`, 720×300)

Line chart: download speed vs distance from one tower, a quiet cell vs the same cell at rush hour.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "One Tower: Speed vs Distance, Quiet vs Rush Hour (illustrative)".
- **Axes:** x = distance 0–10 km mapped to px 70–560; y = 0–160 Mbit/s mapped to baseline y=245 up to y=60; 1px `#999` axis lines; `#e5e9ef` horizontal gridlines with 12px `#444` labels at 0 / 40 / 80 / 120 / 160; x ticks 12px `#444` at 0 / 2 / 4 / 6 / 8 / 10 with caption "km from the tower" centered at y=289.
- **Distances sampled (11 points):** 0.2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10.
- **Quiet cell line (green `#008300`, 3px, 3.5px dots):** [150, 120, 90, 65, 45, 30, 20, 12, 7, 4, 2].
- **Rush hour line (orange `#d95926`, 3px, 3.5px dots):** [15, 12, 9, 6.5, 4.5, 3, 2, 1.2, 0.7, 0.4, 0.2].
- **Annotation (bold 13px orange `#d95926`, centered x=330, two lines y=185/203):** "at 8 km: 7 Mbit/s quiet, 0.7 at rush hour" / "— the phone shows full bars either way".
- **Legend (x=575, swatch rows at y=62/82, 12px `#2c3e50` labels):** green "quiet cell (few users)", orange "rush hour (~60 users)".
- **Caption (12px `#6b7280`, bottom right):** "illustrative — one macro cell, capacity split evenly".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" for sections 1–3, "Common mistake:" for section 4).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. No red in any chart — no genuine error state on this page.
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). Generation speeds (0.04 / 2 / 30 / 150 Mbit/s) are typical real-world figures, labeled "typical, not peaks" in captions; the coverage curves in c4 are labeled illustrative. Documented physics: GEO altitude ≈ 35,786 km stated as "36,000 km"; light-speed round trip 4 × 36,000 km ÷ 300,000 km/s ≈ 0.48 s, stated as "≈ 0.5 s" with total "~600 ms" including processing; LEO ≈ 550 km, "~40 ms" typical. Arithmetic that must stay consistent between text and charts: 100 MB × 8 = 800 Mb; 800 ÷ 0.04 = 20,000 s ≈ 5 h 33 min; 800 ÷ 2 = 400 s ≈ 6.7 min; 800 ÷ 30 ≈ 26.7 s stated "27 s"; 800 ÷ 150 ≈ 5.3 s; 20,000 ÷ 5.3 ≈ 3,750×; per-step multipliers 50× / 15× / 5× stated "~5–50×"; c4 values at 8 km (7 vs 0.7 Mbit/s) match the section-4 annotation.
- This page has no links.
