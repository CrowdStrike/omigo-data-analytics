# NFC & QR Codes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** NFC & QR Codes

**Subtitle:** Tap or scan — tiny payloads measured in bytes that mostly exist to bootstrap a bigger connection somewhere else

## The Menu the Code Never Contained

**Tags:** `core idea` (blue), `bytes not files` (orange), `tap to pair` (green)

- **The running example** — Alice scans a table QR at a cafe and the menu appears on her phone
- **What the code held** — one 33-character web address, nothing else; the menu lives on a server
- **QR ceiling** — the largest QR code stores 2,953 bytes, under 3 KB; most carry far less
- **NFC ceiling** — a common NFC sticker tag holds 144 bytes; even large tags stay under 1 KB
- **Bootstrapping** — both exist to hand off: a URL, a Bluetooth pairing, a payment token
- **Tap to pair** — headphones swap pairing info in one NFC tap, then Bluetooth moves the audio

*Example (italic):* The cafe's 2 MB photo menu never touched the QR code — only its 33-byte address did.

**Key point:** A QR code or NFC tag carries bytes, not files — its job is to point at where the data really lives.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart on a log scale comparing payload sizes in bytes, with a dashed "code ceiling" line separating what codes hold from what networks deliver.

- **Title (bold 15px, `#1a5276`, top center):** "How Big Is the Payload? (bytes, log scale)".
- **Axis:** x = log10(bytes) 0–7 mapped to px 225 (10^0) through 680 (10^7); 1px `#999` baseline at y=245; 12px `#444` ticks at "1 B" (0), "100 B" (2), "10 KB" (4), "1 MB" (6); axis caption "bytes (log scale)" 12px `#444` centered under the ticks.
- **Rows (bars 28px tall, log-scaled length from x=225; left-aligned 12px `#444` labels at x=15):**
  - y=58: "URL in the cafe's QR" — 33 B (log10 1.52), fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, bold 12px `#2a78d6` value "33 B" right of bar end
  - y=104: "NFC sticker tag (NTAG213)" — 144 B (log10 2.16), fill `rgba(0,131,0,0.25)`, 2px `#008300` border, bold 12px `#008300` value "144 B"
  - y=150: "largest possible QR code" — 2,953 B (log10 3.47), fill `rgba(74,58,167,0.25)`, 2px `#4a3aa7` border, bold 12px `#4a3aa7` value "2,953 B"
  - y=196: "one menu photo (server)" — 2,000,000 B (log10 6.30), fill `rgba(217,89,38,0.30)`, 2px `#d95926` border, bold 12px `#d95926` value "2 MB"
- **Ceiling line:** dashed 2px ink `#1a5276` vertical line at log10 3.47 from y=48 to y=245, bold 12px `#1a5276` label "code ceiling ~3 KB" above it at y=44.
- **Annotation (bold 13px orange `#d95926`, above the photo bar near x=430, y=190):** "the photo is ~60,000× the pointer".
- **Caption (12px `#6b7280`, bottom right):** "QR and tag capacities documented; photo size illustrative".

## Count the Characters: URL vs Photo

**Tags:** `worked example` (blue), `QR payloads` (green)

- **Count it yourself** — https://menu.example/cafe/table12 is 33 characters, so 33 bytes in a QR
- **Version 3 is enough** — a small 29×29 QR at light error correction holds 53 bytes; 33 fits
- **The ceiling** — version 40 at light error correction holds 2,953 bytes on a 177×177 grid
- **The photo** — a 2 MB menu photo is 2,000,000 bytes: 2,000,000 ÷ 2,953 ≈ 677 max-size codes
- **The ratio** — the photo is roughly 60,000 times bigger than the address that points to it

*Example (italic):* Bob tries to QR-encode the photo itself — at 677 dense codes to scan in order, printing the menu is faster.

**Key point:** Do the division before trusting a code with data: what fits is a sentence, never a file.

### Visualization (canvas `c2`, 720×300)

Line chart of QR byte capacity by version number, with a dashed line marking Alice's 33-byte URL near the bottom.

- **Title (bold 15px, `#1a5276`, top center):** "QR Capacity by Version — Byte Mode, Light Error Correction".
- **Axes:** x = version 1–40 mapped to px 90–670; y = bytes 0–3,000 mapped to baseline y=245 up to y=62; 1px `#999` axis lines; x ticks 12px `#444` at versions 1 / 10 / 20 / 30 / 40 with caption "QR version (grid size 21×21 → 177×177)"; y labels "0", "1,000", "2,000", "3,000" 12px `#444` at left.
- **Data points (hardcoded, documented):** (1, 17), (5, 106), (10, 271), (20, 858), (30, 1732), (40, 2953).
- **Line:** 3px blue `#2a78d6` through the points; 4.5px blue dots; each dot labeled bold 12px `#1a5276` with its byte count ("17 B" … "2,953 B").
- **URL marker:** dashed 2px green `#008300` horizontal line at y(33 B), bold 12px `#008300` label "Alice's 33-byte URL — fits from version 3" above it near x=300.
- **Annotation (bold 13px orange `#d95926`, centered near x=330, y=95):** "a 2 MB photo ≈ 677 codes at max size — nobody does this".
- **Caption (12px `#6b7280`, bottom right):** "capacities from the QR spec (error correction level L)".

## Every Tap and Scan Is a Logged Row

**Tags:** `where it's used` (blue), `trackable events` (orange), `proximity security` (green)

- **Every scan logs** — the QR's short link records a timestamp, table number, and device type
- **Footfall for free** — menu-QR scans turn walk-in traffic into hourly event data, no app needed
- **Transit taps** — each NFC card tap is a row: card ID, gate, time — origin-destination data
- **Payment tokens** — a contactless tap sends a one-time token; the transaction lands in a log
- **Proximity is security** — NFC works under ~4 cm by design; the tap itself proves presence
- **UWB, the next tap** — ultra-wideband radios range to the centimeter: car keys, item finders

*Example (italic):* The cafe never installed sensors — the lunch rush shows up as a spike in table-QR scans.

**Key point:** To a data scientist, QR and NFC are event generators: tiny payloads, but every use leaves a timestamped row.

### Visualization (canvas `c3`, 720×300)

Bar chart of table-QR scans by hour of day for one cafe over a week, lunch and dinner peaks highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Table-QR Scans by Hour — One Cafe, One Week (illustrative)".
- **Axes:** x = hours 8–22, 15 bars mapped across px 75–680; y = scans 0–80 mapped to baseline y=245 up to y=62; 1px `#999` axis lines; hour labels 12px `#444` under each bar ("8" … "22"); axis caption "hour of day" 12px `#444`; y labels "0", "40", "80" at left.
- **Data (hardcoded, hours 8→22):** 3, 5, 8, 15, 44, 69, 52, 20, 11, 13, 24, 57, 72, 48, 17.
- **Bars:** fill `rgba(42,120,214,0.30)` with 1.5px `#2a78d6` border; the two peak bars (13:00 = 69, 20:00 = 72) filled `rgba(25,158,112,0.45)` with 2px `#199e70` border; peak values labeled bold 12px `#199e70` above their bars ("69", "72").
- **Peak labels:** bold 12px `#199e70` "lunch" above the 13:00 bar value and "dinner" above the 20:00 bar value.
- **Annotation (bold 13px orange `#d95926`, upper left near x=200, y=52):** "no sensors installed — the QR made footfall measurable".
- **Caption (12px `#6b7280`, bottom right):** "counts illustrative".

## Scanning Downloads Nothing but Text

**Tags:** `common mistake` (red), `pointer not data` (blue)

- **The confusion** — "I scanned the menu, so I downloaded it" — no, the camera only read text
- **Decode is local** — the camera turns pixels into a 33-byte string; that works in airplane mode
- **Fetch is network** — the 2 MB menu arrives over Wi-Fi or cellular after the scan, not from ink
- **The offline test** — scan with no signal: the address appears instantly, the page never loads
- **Same for NFC** — a tap hands over pairing info or a token; the data moves on another channel

*Example (italic):* Alice scans the same QR in a basement with no signal — the address pops up, the menu never does.

**Key point:** The payload is the pointer, not the data — printed ink holds a sentence; the server holds the rest.

### Visualization (canvas `c4`, 720×300)

Left-to-right flow diagram of a scan: three boxes joined by labeled arrows, with a dashed divider separating the offline decode from the network fetch.

- **Title (bold 15px, `#1a5276`, top center):** "Scan = Read Text. Download = Network.".
- **Boxes (rounded 8px, 178px wide, 62px tall, top y=105, bold 12px two-line centered labels):**
  - x=28: "printed QR code (ink on paper)" — fill `rgba(0,131,0,0.12)`, 2px `#008300` border
  - x=271: "33-byte text string on the phone" — fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border
  - x=514: "2 MB menu from the server" — fill `rgba(217,89,38,0.18)`, 2px `#d95926` border
- **Arrows:** solid 2.5px `#6b7280` arrows with filled arrowheads between consecutive boxes; 11px `#6b7280` two-line labels above each arrow: "camera decodes pixels" / "(no network needed)" and "browser fetches URL" / "(network required)".
- **Divider:** dashed 2px ink `#1a5276` vertical line at x=482 from y=60 to y=225; bold 12px `#008300` label "works in airplane mode" centered at x=250, y=215; bold 12px `#d95926` label "needs a network" centered at x=600, y=215.
- **Annotation (bold 13px ink `#1a5276`, centered near y=258):** "the scan moved 33 bytes; the network moved 2,000,000".
- **Caption (12px `#6b7280`, bottom center near y=285):** "an NFC tap follows the same shape: tag → tiny payload → bigger channel".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" or "Common mistake:").
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` appears only in the red tag pill / key-point border, never in charts.
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). Documented facts: QR byte-mode capacities at error correction level L — version 1 = 17 B (21×21), version 3 = 53 B (29×29), version 5 = 106 B, version 10 = 271 B, version 20 = 858 B, version 30 = 1,732 B, version 40 = 2,953 B (177×177); NTAG213 usable memory 144 B; NFC operating range roughly ≤ 4 cm. Invented/illustrative figures (the 2 MB photo, hourly scan counts) are labeled illustrative in their charts. Arithmetic that must stay consistent between text and charts: the URL https://menu.example/cafe/table12 = 33 characters = 33 bytes; 2,000,000 ÷ 2,953 ≈ 677; 2,000,000 ÷ 33 ≈ 60,000×.
- This page has no links.
