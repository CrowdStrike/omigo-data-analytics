# Cellular Generations

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cellular Generations

**Subtitle:** From analog calls to 5G — how phones share the airwaves, and why each "G" made a new kind of app possible

## One Song, Five Generations of Phones

**Tags:** `core idea` (blue), `1G to 5G` (green), `10× per generation` (orange)

- **The test** — download the same 5 MB song on a typical phone from each decade
- **1G (1980s)** — analog voice only: a call rode a radio wave like FM; data did not exist
- **2G (1990s)** — digital voice and SMS on GSM or CDMA; at ~40 kbps the song takes ~17 minutes
- **3G (2000s)** — the first real mobile internet: at ~2 Mbps the song lands in ~20 seconds
- **4G / LTE (2010s)** — the network becomes pure internet: ~20 Mbps brings it down in ~2 seconds
- **5G (2020s)** — ~200 Mbps and much quicker round trips: the song arrives in ~0.2 seconds

*Example (italic):* The song that took a 2G phone a whole train ride downloads before a 4G user's thumb leaves the screen.

**Key point:** A "G" is a generation of radio technology, each roughly 10× the speed of the last — and each jump unlocked a product (SMS, the mobile web, streaming video) that was impossible one G earlier.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart of download time for the 5 MB song per generation, log scale.

- **Title (bold 15px, `#1a5276`, top center):** "One 5 MB Song: Download Time by Generation".
- **Rows (bar height 20, tops at y = 70, 105, 140, 175, 210), left-aligned 12px `#444` row labels at x=20:** "1G — voice only", "2G — 40 kbps", "3G — 2 Mbps", "4G LTE — 20 Mbps", "5G — 200 Mbps".
- **Bars start at x=190; log scale, 100 px per 10× (0.1 s = 0 px):** 2G 1000 s → width 400 orange `#d95926`; 3G 20 s → 230 yellow `#c98500`; 4G 2 s → 130 blue `#2a78d6`; 5G 0.2 s → 30 green `#008300`.
- **1G row:** no bar — 12px italic grey `#6b7280` text "no data at all — calls only" at x=196.
- **End labels (bold 12px, matching bar color, right of each bar):** "≈17 min", "≈20 s", "≈2 s", "≈0.2 s".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=262):** "each generation ≈ 10× faster — bar scale is logarithmic".
- **Caption (12px `#444`, bottom right):** "typical real-world speeds, illustrative".

## GSM vs CDMA: Two Ways to Share One Tower

**Tags:** `worked example` (blue), `time slots` (green), `codes` (orange)

- **The problem** — hundreds of phones near one tower must share a narrow slice of radio spectrum
- **GSM: take turns** — each 200 kHz channel is sliced into 8 repeating time slots, one call per slot
- **CDMA: talk at once** — calls overlap on one wide band, each scrambled by its own unique code
- **The analogy** — GSM is a meeting with speaking turns; CDMA is a party of different languages
- **The split** — most of the world ran GSM; some US carriers ran CDMA; the phones never mixed
- **The SIM legacy** — the swappable SIM card is a GSM invention: your identity lives on the card

*Example (italic):* A traveler landing abroad in 2005 swapped a cheap local SIM into a GSM phone; a CDMA phone stayed locked to its home carrier.

**Key point:** GSM and CDMA solve the same sharing problem in two incompatible ways — time slots vs codes — and that 2G-era split still explains SIM cards, "world phone" labels, and which old handsets worked where.

### Visualization (canvas `c2`, 720×300)

Two side-by-side panels: GSM time slots on the left, CDMA overlapping coded calls on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Sharing One Tower: Time Slots vs Codes".
- **Left panel (x 40–328):** bold 13px `#1a5276` header "GSM — take turns" at x=40, y=62. A strip at y=110, height 44, of 16 cells (two rounds of 8 slots), each 18 px wide, filled by cycling 8 colors `['#2a78d6','#008300','#d55181','#c98500','#199e70','#d95926','#4a3aa7','#888888']`, each cell showing its call number 1–8 in 11px white centered text; 1px white gaps between cells. Below the strip (12px `#444`, x=40, y=178): "one 200 kHz channel → 8 calls, one per slot"; (y=196): "your phone speaks only in its own slot".
- **Right panel (x 392–680):** bold 13px `#1a5276` header "CDMA — talk at once" at x=392, y=62. Three translucent full-width bands all spanning x 392–680, overlapping: blue `rgba(42,120,214,0.30)` y=95 h=70, magenta `rgba(213,81,129,0.30)` y=115 h=70, green `rgba(0,131,0,0.30)` y=135 h=70; each band labeled at its right-inside edge in bold 12px of its solid color: "call A · code A", "call B · code B", "call C · code C". Below (12px `#444`, x=392, y=238): "one 1.25 MHz band → many coded calls at once".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "same air, two incompatible ways to share it".
- **Caption (12px `#444`, bottom right):** "channel and slot sizes are the real 2G numbers".

## Speed Is Two Numbers: Bandwidth and Latency

**Tags:** `bandwidth` (blue), `latency` (orange), `where it's used` (green)

- **Bandwidth** — bits per second through the pipe: ~0.04 → 2 → 20 → 200 Mbps across 2G–5G
- **Latency** — one round trip to the server and back: ~600 → 200 → 50 → 20 ms across 2G–5G
- **Different jobs** — downloads care about bandwidth; taps, calls, and games care about latency
- **Round trips add up** — 30 back-and-forths cost ~18 s of pure waiting on 2G, ~1.5 s on 4G
- **The feel** — a wide but laggy pipe still feels slow: every tap waits a full round trip first

*Example (italic):* Streaming video needs ~5 Mbps but tolerates seconds of buffering; a video call needs less bandwidth yet breaks past ~200 ms of delay.

**Key point:** Each generation didn't just widen the pipe — it shortened the wait; bandwidth sets how much fits through, latency sets how fast the network feels.

### Visualization (canvas `c3`, 720×300)

Two side-by-side vertical bar charts: bandwidth (log scale) on the left, round-trip latency (linear) on the right, same generation colors as `c1`.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ladders: the Pipe Widens, the Wait Shrinks".
- **Generation colors everywhere:** 2G orange `#d95926`, 3G yellow `#c98500`, 4G blue `#2a78d6`, 5G green `#008300`.
- **Left panel:** header (bold 13px `#1a5276`, centered at x=195, y=56) "bandwidth (Mbps, log scale)"; baseline 2px `#999` at y=245 from x=70 to x=320; bars 40 px wide at x = 90, 150, 210, 270 for 2G/3G/4G/5G; log-scale heights 40 px per 10× from 0.01 Mbps: 0.04 → 24, 2 → 92, 20 → 132, 200 → 172; value labels (bold 12px, bar color, centered above each bar): "0.04", "2", "20", "200"; 12px `#444` generation labels below baseline.
- **Right panel:** header (bold 13px `#1a5276`, centered at x=525, y=56) "round trip (ms) — smaller is better"; baseline 2px `#999` at y=245 from x=400 to x=650; bars 40 px wide at x = 420, 480, 540, 600; linear heights 0.28 px per ms: 600 → 168, 200 → 56, 50 → 14, 20 → 6; value labels above: "600", "200", "50", "20"; generation labels below baseline.
- **Annotation (bold 13px magenta `#d55181`, centered near y=275):** "2G→5G: 5,000× the bandwidth, 30× less waiting".
- **Caption (12px `#444`, bottom right):** "typical values, illustrative".

## Reading the Alphabet on Your Status Bar

**Tags:** `common mistake` (red), `LTE` (blue), `marketing Gs` (orange)

- **LTE** — "Long Term Evolution": the upgrade path toward true 4G; early LTE missed the official 4G bar
- **Marketing Gs** — fast 3G shipped as "H+", early LTE as "4G", one carrier's 4G even as "5G E"
- **The status icon** — E is 2G-era EDGE, 3G/H/H+ are 3G flavors, then LTE/4G, then 5G
- **Two "bandwidths"** — engineers mean spectrum width in Hz; everyone else means data rate in bps
- **5G fine print** — low-band 5G can trail good 4G; headline speeds need short-range high bands

*Example (italic):* A phone can show "5G" on a low-frequency band and measure slower than yesterday's LTE at the same street corner.

**Common mistake:** Reading the G as a speedometer. The G names a generation of radio standards; the speed you actually get depends on the band, the tower's load, and your signal — which is why 4G in a quiet suburb can beat 5G in a packed stadium.

### Visualization (canvas `c4`, 720×300)

Horizontal timeline 1980–2025 of generation blocks with the standards inside and what each unlocked above.

- **Title (bold 15px, `#1a5276`, top center):** "Five Generations on One Timeline".
- **Time mapping:** x = 60 + (year − 1980) × (600 / 45); 2px `#999` baseline at y=200 from x=60 to x=660 with 12px `#444` year ticks at 1980, 1991, 2001, 2010, 2019, 2025.
- **Blocks (y=120, height 70, 1px white gaps), fills and bold white centered labels:** 1G 1980–1991 grey `#6b7280` "1G — analog"; 2G 1991–2001 orange `#d95926` "2G — GSM · CDMA"; 3G 2001–2010 yellow `#c98500` "3G — UMTS · HSPA"; 4G 2010–2019 blue `#2a78d6` "4G — LTE"; 5G 2019–2025 green `#008300` "5G — NR". Generation letter bold 13px, standard names 11px on a second line.
- **"What it unlocked" labels (11px `#444`, centered above each block at y=105):** "calls", "SMS", "mobile web", "apps · streaming", "real-time".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=250):** "a new G roughly every decade — and the old ones linger for another one".
- **Caption (12px `#444`, bottom right):** "boundary years approximate".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all numbers are the hardcoded values above (no randomness). Download times follow exactly from 5 MB = 40 Mbit at the stated speeds (40 kbps → 1,000 s ≈ 17 min; 2 Mbps → 20 s; 20 Mbps → 2 s; 200 Mbps → 0.2 s). Speeds and latencies are "typical real-world" figures labeled illustrative; GSM's 200 kHz / 8 slots and CDMA's 1.25 MHz channel are the real 2G standard numbers; timeline boundary years are approximate by design.
- **Generation color coding is consistent across all four canvases:** 2G orange, 3G yellow, 4G blue, 5G green (1G grey).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
