# The Bandwidth Ladder

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Bandwidth Ladder

**Subtitle:** From 300 bps modems to gigabit fiber — fifty years of the pipe getting steadily wider, one technology handing off to the next

## From Whistling Modems to Glass

**Tags:** `core idea` (blue), `dial-up to fiber` (green), `~50%/year` (orange)

- **The start** — 1980s home modems whistled data over voice phone lines at 300–2,400 bits per second
- **Dial-up's ceiling** — by 1997 the 56k modem hit the practical limit of a voice telephone line
- **Broadband** — DSL and cable reused phone and TV wiring to jump from kilobits to megabits
- **Fiber** — light in glass took homes from megabits to a gigabit: ~3 million× the 1982 modem
- **The pattern** — ~50% faster every year for four decades: a straight line on a log chart

*Example (italic):* The 1997 modem was about 200× the 1982 one; gigabit fiber is another 18,000× on top of that.

**Key point:** Bandwidth grew roughly 50% per year for forty years (Nielsen's law) — no single technology did it; each new one took over the climb as the last hit its ceiling.

### Visualization (canvas `c1`, 720×300)

Line chart of typical home bandwidth 1980–2025 on a log y-axis, each point tagged with its technology.

- **Title (bold 15px, `#1a5276`, top center):** "Home Bandwidth, 1980–2025 (log scale)".
- **Mapping:** `xOf(year) = 70 + (year − 1980) × (590 / 45)`; `yOf(log10bps) = 245 − (log10bps − 2) × 24.67` (baseline y=245 at 100 bps, top ≈ log 9.5).
- **Gridlines** `#e5e9ef` at log 3 / 6 / 9 with right-aligned 12px `#444` labels "1 kbps", "1 Mbps", "1 Gbps" at x=62; 1px `#999` axes; x ticks 1980/1990/2000/2010/2020 labeled 12px `#444` below baseline.
- **Wired line (blue `#2a78d6`, 3px, 4px dots) through (year, log10 bps):** (1982, 2.48) 300 bps modem; (1986, 3.38) 2.4k modem; (1992, 4.16) 14.4k modem; (1997, 4.75) 56k dial-up; (2000, 6.0) DSL 1 Mbps; (2005, 6.9) cable 8 Mbps; (2010, 7.7) cable 50 Mbps; (2013, 8.0) fiber 100 Mbps; (2018, 9.0) fiber 1 Gbps.
- **Tech tags:** 11px `#444` beside each point, alternating above/below to avoid collisions: "300 bps modem", "2.4k modem", "14.4k", "56k dial-up", "DSL 1 Mbps", "cable 8 Mbps", "cable 50 Mbps", "fiber 100 Mbps", "fiber 1 Gbps".
- **Annotation (bold 13px magenta `#d55181`, upper left near x=200, y=70):** "~50% faster every year for four decades".
- **Caption (12px `#444`, bottom right):** "typical retail speeds, illustrative".

## Mobile Ran the Same Race, a Few Years Behind

**Tags:** `2G to 5G` (blue), `catch-up` (green), `where it's used` (orange)

- **Late start** — mobile data arrived in 2001 at ~40 kbps, where the wire had been in the mid-90s
- **The chase** — 3G caught early DSL, LTE caught cable, and 5G plays in fiber territory
- **Closing gap** — six-plus years behind at the start, only a couple by the LTE era
- **WiFi bridge** — indoors, phones skip the cellular tower and ride the wired line over WiFi
- **Leapfrogging** — mobile-first countries skipped wires entirely and met the internet on 3G/4G

*Example (italic):* A 2011 LTE phone often out-downloaded the 2008 cable modem in the living room it walked past.

**Key point:** Wireless traced the same exponential as the wire, a few years behind — so every wired product category (web, music, video, cloud) replayed on phones shortly after.

### Visualization (canvas `c2`, 720×300)

The c1 chart with a second line: mobile bandwidth per technology, converging toward the wired line.

- **Title (bold 15px, `#1a5276`, top center):** "The Wire vs the Tower (log scale)".
- **Same axes, gridlines, and mapping as `c1`.**
- **Wired line:** same nine points as `c1`, blue `#2a78d6` 3px with 3px dots, untagged except a bold 12px blue label "home wire" above the line near x=250.
- **Mobile line (orange `#d95926`, 3px, 4px dots) through (year, log10 bps):** (2001, 4.6) GPRS 40 kbps; (2003, 5.58) 3G 384 kbps; (2006, 6.3) HSPA 2 Mbps; (2011, 7.3) 4G LTE 20 Mbps; (2020, 8.3) 5G 200 Mbps. Bold 12px orange label "mobile" below the line near x=430.
- **Tech tags (11px `#444`, below/right of each mobile point):** "GPRS 40 kbps", "3G 384 kbps", "HSPA 2 Mbps", "4G LTE 20 Mbps", "5G 200 Mbps".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "the same climb, a few years later — nearly closed by 2020".
- **Caption (12px `#444`, bottom right):** "typical speeds, illustrative".

## What Fits in Ten Seconds

**Tags:** `worked example` (blue), `media ladder` (green)

- **The yardstick** — how much data a steady 10-second download delivers at each era's speed
- **56 kbps (1997)** — 70 KB: one small web image; pages were text because they had to be
- **1 Mbps (2001 DSL)** — 1.25 MB: a phone photo, or about a minute of MP3
- **20 Mbps (2010 cable)** — 25 MB: a short video clip; streaming stops being exotic
- **1 Gbps (2018 fiber)** — 1.25 GB: a full-length SD movie; the cloud starts feeling local

*Example (italic):* A 700 MB movie was a 28-hour dial-up download; on gigabit fiber it takes under 6 seconds.

**Key point:** Every ~100× in bandwidth changed the default medium — text, then images, then music, then video — the pipe's width decided what the internet was for.

### Visualization (canvas `c3`, 720×300)

Horizontal log-scale bars: bytes delivered in 10 seconds at each era's speed, labeled with the medium that fits.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Seconds of Downloading, Four Eras".
- **Rows (bar height 22, tops at y = 78, 120, 162, 204), left 12px `#444` row labels at x=20:** "56 kbps — dial-up (1997)", "1 Mbps — DSL (2001)", "20 Mbps — cable (2010)", "1 Gbps — fiber (2018)".
- **Bars start at x=240; log scale, 80 px per 10× of bytes above 10 KB (log10 bytes − 4):** 70 KB → width 68 orange `#d95926`; 1.25 MB → 168 yellow `#c98500`; 25 MB → 272 blue `#2a78d6`; 1.25 GB → 408 green `#008300`.
- **End labels (bold 12px, bar color, right of bar):** "70 KB — a web image", "1.25 MB — a phone photo", "25 MB — a video clip"; for the last (widest) bar the label "1.25 GB — an SD movie" sits inside its right end in white.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=258):** "every ~100× changed what the internet was for — bar scale is logarithmic".
- **Caption (12px `#444`, bottom right):** "exact arithmetic: speed × 10 s ÷ 8".

## Megabits Are Not Megabytes

**Tags:** `common mistake` (red), `÷ 8` (orange)

- **Bits vs bytes** — networks are sold in megaBITS per second; files come in megaBYTES — divide by 8
- **The letdown** — a "100 Mbps" plan moves at most 12.5 MB per second; 1 GB takes ~80 s, not 10
- **"Up to"** — the advertised number is the link's ceiling; WiFi, sharing, and the far server set the pace
- **Asymmetry** — ADSL's A means asymmetric: ~8 Mbps down but ~1 up; uploads still lag downloads today
- **Which bandwidth** — engineers also say "bandwidth" for spectrum width in Hz; on this page it means bps

*Example (italic):* The customer who expected a 1 GB game in 10 seconds on "100 Mbps" waits 80 — before WiFi and sharing take their cut.

**Common mistake:** Reading Mbps as MB/s. The marketing number is megabits; your files are megabytes — so every download takes 8× longer than the naive math says, before real-world overheads slow it further.

### Visualization (canvas `c4`, 720×300)

Conversion pictorial plus a two-bar comparison of expected vs actual time for a 1 GB download on a 100 Mbps plan.

- **Title (bold 15px, `#1a5276`, top center):** "A '100 Mbps' Plan Downloading a 1 GB File".
- **Row 1 (rounded boxes 40px tall, centered y=95):** blue-bordered box at x=150 "the plan: 100 Mbps" (fill `rgba(42,120,214,0.15)`, border `#2a78d6`), 3px grey arrow to a yellow-bordered box at x=360 "÷ 8 bits per byte" (fill `rgba(201,133,0,0.12)`, border `#c98500`), arrow to a green-bordered box at x=560 "12.5 MB per second" (fill `rgba(0,131,0,0.12)`, border `#008300`); 12px `#2c3e50` box text.
- **Row 2 (bars height 22, x from 190):** at y=170, grey dashed-border bar width 50 with 12px `#444` left label "if it were MB/s:" and end label "10 s (wrong)" in red `#e74c3c` bold 12px; at y=210, solid blue `#2a78d6` bar width 400 with left label "at 12.5 MB/s:" and bold 12px blue end label "80 s (real)". Bar widths proportional to time (5 px per second).
- **Annotation (bold 13px orange `#d95926`, centered near y=262):** "the pipe is 8× narrower than it sounds — bits, not bytes".
- **Caption (12px `#444`, bottom right):** "1 GB = 8,000 megabits; 8,000 ÷ 100 = 80 s".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded (year, log10 bps) pairs above — no randomness. Speeds are typical retail offers labeled illustrative; the ten-second payloads and the 80-second download are exact arithmetic from the stated speeds (speed × 10 s ÷ 8; 1 GB = 8,000 Mb ÷ 100 Mbps = 80 s; 700 MB at 56 kbps = 100,000 s ≈ 28 h). The ~50%/year growth line is Nielsen's law, an empirical observation about high-end home connections.
- **Era color coding is consistent across `c1`–`c3`:** dial-up orange, DSL yellow, cable blue, fiber green (in `c1`/`c2` the whole wired line is blue and mobile is orange).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
