# Visual Machine-Readable Codes

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one `.card-section` per code format; no status pill on this page)
**HTML title tag:** Visual Machine-Readable Codes — Discussion Backlog

**Subtitle:** Encode binary/structured data into scannable visual patterns — from 1D barcodes to 2D matrices to circular formats.

**Intro callout:** **Core problem:** Encode a short string (URL, product ID, routing number) into a pattern a camera can decode faster and more reliably than OCR on printed text. The design space is geometry (1D lines vs 2D grid vs radial), error correction level, and how much data fits.

## 1. 1D Barcode (UPC / EAN / Code 128)

Parallel vertical lines of varying width. The original — optimized for a single laser-line scanner sweeping across a surface.

- **Geometry:** 1D linear — bars + spaces encode digits
- **Capacity:** ~20-40 characters
- **Error correction:** Check digit only — no redundancy
- **Requires:** Orientation alignment + quiet zone

**How it's decoded:** A laser sweeps horizontally. It measures the ratio of bar widths to space widths. Each digit is encoded as a specific pattern of 2 bars + 2 spaces that sum to 7 units wide. The scanner detects the start guard (bar-space-bar), reads left-to-right measuring relative widths, hits the center guard, continues through the right half, and validates with a modulo-10 check digit. Width ratios — not absolute size — so it works at any print scale.

**Failure mode:** Any damage fails the entire read. No graceful degradation — you get a valid read or nothing.

*Example: Every product on a grocery shelf — UPC-A encodes a 12-digit product number.*

### Visualization (canvas `c1`, 720×300)

Schematic UPC-A barcode with quiet zones, guards, and laser-sweep annotation.

- **Title (bold 14px, `#1a5276`, top center):** "UPC-A Barcode Structure".
- **Quiet zones:** light-blue `#f0f8ff` rectangles (40px wide) flanking the code, each with a rotated vertical 10px orange `#e67e22` label "QUIET ZONE".
- **Bars:** black `#1a1a1a`; start/center/end guard bars extend 15px taller than data bars (bar area y=50, height 180). Data bar widths follow a fixed module pattern (left digits, 5-module center guard, right digits) at 2.2px per module, alternating bar/space.
- **Number display (14px monospace `#333`, centered below bars):** "0   74527   01524   3".
- **Annotation:** dashed (4/3) red `#e74c3c` horizontal line under the digits spanning the code width, with 11px red caption "single laser sweep reads entire code" centered below.

## 2. QR Code

Square 2D matrix with three corner finder patterns. Designed for fast detection from any angle. Reed-Solomon error correction up to 30% damage tolerance.

- **Geometry:** 2D grid with 3 position detection patterns
- **Capacity:** ~4,000 alphanumeric chars
- **Error correction:** Reed-Solomon (L/M/Q/H levels)
- **Orientation:** Any angle — finders handle rotation

**How it's decoded:** Camera finds three 1:1:3:1:1 ratio squares (works at any rotation/scale). These three corners define the grid geometry — the fourth corner is inferred. Timing patterns (alternating modules between finders) calibrate the grid spacing. The decoder maps each module to a bit, reads format info (error level + mask pattern), XORs the mask to recover raw data, then applies Reed-Solomon to correct errors. Data is read in a zigzag path from bottom-right upward.

**Why dominant:** Degrades gracefully. You can cover 30% of it and it still reads. Open standard, no app lock-in.

*Example: Restaurant menus post-COVID. Payment links. WiFi sharing.*

### Visualization (canvas `c2`, 720×300)

Annotated QR code anatomy diagram.

- **Title (bold 14px, `#1a5276`, top center):** "QR Code Anatomy".
- **Grid:** 25×25 modules at 9px cells, centered, white background with `#e0e0e0` 1px border (8px quiet zone). Data modules black `#1a1a1a`, pseudo-randomly filled via `sin(r·7 + c·13 + 1) > 0.05`; finder corner zones skipped.
- **Timing patterns:** row 6 and column 6 alternate black/white.
- **Finder patterns:** three 7×7 nested squares (black outer, white 5×5, black 3×3) at top-left, top-right, bottom-left.
- **Alignment pattern:** 5×5 nested square (black, white 3×3, black center cell) near bottom-right.
- **Labels (11px, right of the code, with 1px leader lines):** "Finder pattern (×3)" in `#e74c3c`; "Timing pattern" in `#1a5276`; "Alignment pattern" in `#27ae60`.
- **Caption (11px `#666`, bottom center):** "Up to 30% of modules can be damaged and still decode".

## 3. Data Matrix

Square or rectangular 2D code without the large finder patterns of QR. Smaller footprint for the same data. Common in industrial marking.

- **Geometry:** L-shaped finder (solid bottom + left), alternating clock borders
- **Capacity:** ~2,300 alphanumeric chars
- **Error correction:** Reed-Solomon ECC 200
- **Use:** Etched on metal parts, PCBs, pharmaceuticals

**How it's decoded:** The solid L-shaped border (bottom + left edges always filled) tells the scanner "this is the origin corner." The opposite two edges alternate black/white — this is the clock track that tells the decoder exactly how many rows and columns exist. Once the grid is established, modules are read in a diagonal pattern using 8 cells per codeword arranged in an L-shape. Reed-Solomon then corrects any read errors from surface damage.

**Advantage:** Smallest physical footprint for equivalent data. Favored where space is at a premium.

*Example: Tiny codes laser-etched on surgical instruments for tracking sterilization cycles.*

### Visualization (canvas `c3`, 720×300)

Data Matrix schematic highlighting the L-finder and clock borders.

- **Title (bold 14px, `#1a5276`, top center):** "Data Matrix — L-Finder + Clock Borders".
- **Grid:** 20×20 modules at 11px cells, centered, background `#fafafa`.
- **L-border:** bottom row and left column solid black `#1a1a1a`.
- **Clock track:** top row and right column alternate black / `#fafafa` per cell.
- **Data modules:** interior cells filled `rgba(26,82,118,0.6)` when `sin(r·5 + c·11 + 3) > 0.2`.
- **Annotations:** solid red `#e74c3c` 2px lines tracing the bottom and left edges, labeled "← solid L-border (finder)" (11px red, right of grid near bottom); dashed (3/3) orange `#e67e22` 2px lines tracing the top and right edges, labeled "← alternating (clock track)" (11px orange, right of grid near top).

## 4. Apple Visual Code (Circular)

Concentric ring pattern Apple introduced for device pairing. Encodes a short payload in a radial layout designed for camera-based scanning at close range.

- **Geometry:** Radial — concentric rings with segmented data
- **Capacity:** Small — pairing token or URL
- **Error correction:** Proprietary
- **Priority:** Aesthetic design over data density

**How it's decoded:** Camera detects the circular boundary and center point to establish the radial coordinate system. Each ring is divided into angular segments — a filled segment is 1, empty is 0. The decoder reads bits ring-by-ring from outer to inner (or vice versa). The center circle provides orientation and scale reference. Because it's proprietary, the exact encoding scheme is undocumented — only Apple's frameworks can interpret it.

**Trade-off:** Sacrifices capacity and openness for visual appeal. Only works within Apple ecosystem.

*Example: Apple Watch pairing screen — swirling particle animation that's actually a scannable code.*

### Visualization (canvas `c4`, 720×300)

Radial code schematic: concentric rings with purple segmented data arcs.

- **Title (bold 14px, `#1a5276`, top center):** "Apple Visual Code — Radial Data Encoding".
- **Structure (centered, cy offset +15):** outer decorative circle r=110 stroked `#ddd`; solid concentric filled rings at r = 95, 80, 65, 50, 35, 20 alternating `#1a1a1a` / `#fff`; black center dot r=8.
- **Data ring 1:** 20 angular segments between r 80–90, filled `#8e44ad` when `sin(i·4 + 1) > −0.2` (0.8 duty cycle per segment).
- **Data ring 2:** 20 segments between r 65–75, filled `rgba(142,68,173,0.7)` when `cos(i·3 + 2) > −0.3`.
- **Data ring 3:** 24 segments between r 50–60, filled `rgba(142,68,173,0.5)` when `sin(i·5 + 7) > 0`.
- **Caption (11px `#666`, bottom center):** "data in segmented rings — aesthetic over density".

## 5. Aztec Code

Square 2D code with a bullseye center finder. IATA standard for boarding passes. No quiet zone required — can be printed edge-to-edge.

- **Geometry:** Concentric square rings at center + data grid
- **Capacity:** ~3,000 alphanumeric chars
- **Error correction:** Reed-Solomon, configurable 5-95%
- **Key feature:** No quiet zone — edge-to-edge printing

**How it's decoded:** The bullseye center (concentric dark/light square rings) is detected first — unlike QR which locates corners, Aztec finds the middle. The decoder reads outward from center in a spiral. A "mode message" ring around the bullseye tells the decoder the symbol size and number of data layers. Data layers spiral outward. Because the finder is central, no quiet zone is needed — data extends to the very edge of the symbol.

**Why airlines chose it:** Reads well on phone screens at varied brightness. No margin required. Compact for the data needed.

*Example: Every mobile boarding pass on your phone uses Aztec, not QR.*

### Visualization (canvas `c5`, 720×300)

Aztec code schematic: square bullseye finder surrounded by data modules extending to a marked edge.

- **Title (bold 14px, `#1a5276`, top center):** "Aztec Code — Bullseye Finder, No Quiet Zone".
- **Bullseye (centered, cy offset +15):** concentric filled squares with half-sizes 5, 4, 3, 2, 1 cells (9px cell), alternating `#1a1a1a` / `#fff` / `#1a1a1a` / `#fff` / `#1a1a1a`.
- **Data modules:** grid radius 11 cells around the bullseye (central 11×11 zone skipped), filled `rgba(26,82,118,0.55)` when `sin(r·7 + c·9 + 5) > 0.1`, clipped to the frame.
- **Edge frame:** red `#e74c3c` 2px rectangle at (60, 40, w−120, h−65) showing data reaching the edge; 11px red caption right-aligned at bottom: "no margin needed — prints edge-to-edge".

## 6. PDF417

Stacked linear barcode — rows of 1D-style patterns stacked vertically. Hybrid: more data than 1D, but still scans with a line sweep per row.

- **Geometry:** Multiple 1D barcode rows stacked (3-90 rows)
- **Capacity:** ~1,800 characters
- **Error correction:** Reed-Solomon per row
- **Aspect:** Tall and narrow — fits label formats

**How it's decoded:** Each row is a self-contained 1D barcode with start/stop patterns. The scanner reads row by row — each row contains a row indicator codeword (which row am I?), data codewords, and error correction. Codewords are 17 modules wide, grouped as 4 bars + 4 spaces. The "417" in the name means each pattern is one of 929 possible codewords. Rows can be read in any order and reassembled using row indicators — so partial scans can be combined.

**Niche:** When you need more than 1D can hold but the scanner is still line-based. Bridge technology.

*Example: US driver's licenses (back), FedEx shipping labels, event tickets.*

### Visualization (canvas `c6`, 720×300)

PDF417 schematic: 16 stacked rows of bar patterns with start/stop columns and a row-scan arrow.

- **Title (bold 14px, `#1a5276`, top center):** "PDF417 — Stacked Rows of 1D Patterns".
- **Rows:** 16 rows, 14px row height, starting at (80, 50), code width 540px. Each row: orange `#e67e22` start codeword bars (widths 4, 2, 6), black `#1a1a1a` data bars with pseudo-random widths (`sin`/`cos` of row/codeword indices, up to 24 codewords), orange stop codeword bars (widths 6, 2, 4) at the right edge.
- **Annotations:** dashed (3/3) orange 1.5px rectangles around the start and stop columns, with 10px orange labels "start" and "stop" below them.
- **Scan arrow:** solid green `#27ae60` 1.5px horizontal arrow across row 3 (left to right, filled arrowhead); 10px green caption below the code: "each row scans like independent 1D barcode →".

## 7. MaxiCode

Hexagonal grid with a central bullseye. UPS created it for package sorting — optimized for reading at high speed on conveyor belts from a fixed scanner.

- **Geometry:** Hexagonal dot grid with concentric-ring center
- **Capacity:** ~93 chars (fixed size, single purpose)
- **Error correction:** Reed-Solomon
- **Design goal:** One job — route a package at conveyor speed

**How it's decoded:** The bullseye (3 concentric circles) is detected for location and scale. The hexagonal grid has 866 modules arranged in 33 rows. Each hexagonal cell is either filled or empty (1 bit). The primary message (ZIP code + service class) is encoded with heavy redundancy near the center so it reads even if edges are damaged. The secondary message (tracking details) fills the outer cells. The hex grid packs ~15% more cells per area than a square grid — geometry chosen for density at fixed size.

**Philosophy:** Don't generalize. Fixed size, fixed purpose, maximized for exactly one scanning scenario.

*Example: The small square code on every UPS package label — encodes ZIP and service class.*

### Visualization (canvas `c7`, 720×300)

MaxiCode schematic: circular bullseye inside a hexagonally-offset dot grid, with a fixed-size frame.

- **Title (bold 14px, `#1a5276`, top center):** "MaxiCode — Hexagonal Grid + Bullseye".
- **Bullseye (centered, cy offset +15):** concentric filled circles r = 30, 24, 18, 12, 6 alternating `#1a1a1a` / `#fff`.
- **Dot grid:** rows −9…9 (12px vertical spacing) × cols −14…14 (14px horizontal spacing, odd rows offset half a step); dot radius 4; only dots at distance 36–115 from center drawn; filled dots `rgba(26,82,118,0.65)` (with faint `rgba(26,82,118,0.3)` 0.5px stroke) when `sin(row·5 + col·8 + 2) > −0.1`, else empty `#e8e8e8`.
- **Fixed-size frame:** dashed (4/3) gray `#999` 1px rectangle 250×230 centered on the code.
- **Caption (11px `#666`, bottom center):** "fixed 1\" × 1\" size — optimized for high-speed conveyor scanning".

## 8. App Codes (Spotify / Snapchat / Instagram)

Proprietary app-specific visual codes. Spotify's dot-pattern circles, Snapchat's Snapcode ghost with dots. Require the specific app to scan.

- **Geometry:** Circular dot-patterns or custom branded shapes
- **Capacity:** User/content ID only — not arbitrary data
- **Error correction:** App-specific, undocumented
- **Ecosystem:** Walled garden — requires specific app

**How it's decoded:** The app's camera detects the branded shape (ghost outline, green circle, gradient ring) as an anchor. Dots or bars around/within the shape encode a short identifier — typically just a user ID or content URI. The app sends this ID to its own servers to resolve the full resource. There's no universal standard — each app implements its own detection, encoding, and lookup. If the app isn't installed, no generic scanner can interpret the code. It's less "encoding data" and more "encoding a lookup key for one specific database."

**Silent failure:** If the app isn't installed, the code is inert. The failure isn't "can't read" but "opens the wrong handler" or nothing at all.

*Example: Spotify codes on concert posters — useless without Spotify installed.*

### Visualization (canvas `c8`, 720×300)

Side-by-side brand-code schematics: Spotify-style circle (left) and Snapcode ghost (right).

- **Title (bold 14px, `#1a5276`, top center):** "Proprietary App Codes — Branded Scannable Identifiers".
- **Spotify code (left, centered at w/4):** filled circle r=85 in Spotify green `#1db954`; white inner circle r=30 containing three white-gapped green sound-wave arcs (3px, round caps); ring of 28 white dots at r=60 with varying radii 2–7px (data encoding). Label "Spotify Code" (11px `#333`) below.
- **Snapcode (right, centered at 3w/4):** simplified ghost silhouette (arc top + quadratic-curve scalloped bottom) filled Snapchat yellow `#fffc00` with `#333` 2px outline; 7×7 dot pattern inside (dots within radius 3.2 cells; filled dots `#333` 4px, empty `#ccc` 2.5px, per `sin(row·4 + col·6) > −0.2`). Label "Snapcode" (11px `#333`) below.
- **Caption (11px `#e74c3c`, bottom center):** "⚠ requires specific app — fails silently without it".

## 9. Ephemeral Pairing Codes (WhatsApp Web / Discord / Telegram Login)

Standard QR format, but the payload is a short-lived cryptographic pairing token — not data. The code exists to link two devices, then becomes worthless.

- **Geometry:** Standard QR — open format, any scanner decodes the bits
- **Payload:** Ephemeral reference + public key (base64), meaningless outside the app
- **Lifetime:** Rotates every ~20-60 seconds, single use
- **Purpose:** Out-of-band channel proving physical proximity

**How it works:** The browser generates a fresh keypair and asks the server for an ephemeral reference. Both are encoded into the QR. The phone — already authenticated — scans it with the in-app camera, extracts the reference and public key, and performs a key exchange relayed through the server. The result is an end-to-end encrypted session where the browser becomes a linked device. The trust direction is inverted from normal QR use: the scanner (phone) is the trusted party authorizing the displayer (browser), not the other way around. Scanning the code with a generic QR reader yields a valid decode of an unusable string.

**Key difference from sections 1-8:** Every other code here is a static data carrier — print it once, scan it forever. This one is a moving target by design: an expired or reused code must fail. Staleness is the security feature.

*Example: WhatsApp Web login, Discord/Telegram QR login, TOTP seed QRs in authenticator apps, WhatsApp "add contact" codes.*

### Visualization (canvas `c9`, 720×300)

Device-linking diagram: browser monitor showing a rotating QR, phone scanning it, key-exchange arrows.

- **Title (bold 14px, `#1a5276`, top center):** "Device Linking — QR as Ephemeral Pairing Channel".
- **Browser (left):** monitor outline 170×130 at (130, 70) stroked `#1a5276` 2px with stand; inside, a 13×13 mini QR (6px cells, black `#1a1a1a` pseudo-random modules per `sin(r·7 + c·13 + 1) > 0.05`, three mini finder patterns). Orange `#e67e22` timer arc (3px) at the monitor's top-right with 10px caption "rotates ~30s". Labels below: "Browser (untrusted)" (11px `#333`) and "shows: ref + public key" (10px `#666`).
- **Phone (right):** rounded-device outline 80×150 at (w−210, 60) stroked green `#27ae60` 2px, with camera lens circles (r=16 outline, r=7 filled green). Labels below: "Phone (trusted)" (11px `#333`) and "already authenticated" (10px `#666`).
- **Arrows:** dashed (5/4) green `#27ae60` arrow from phone to the QR labeled "1. scan (proves proximity)" (10px green); double-headed purple `#8e44ad` arrow between the devices lower down labeled "2. key exchange via server relay" and "3. E2E-encrypted linked session" (10px purple).
- **Caption (11px `#666`, bottom center):** "trust is inverted: the scanner authorizes the displayer".

## Closing callout (`.key-point`, full width)

**The analytical insight:** These codes differ in failure modes. A damaged barcode fails completely (no redundancy). A QR code degrades gracefully (error correction). A proprietary code fails silently if the required app isn't installed. An ephemeral pairing code fails deliberately after expiry — staleness is the point. The choice of code format encodes assumptions about the scanning environment, damage probability, and whether the ecosystem is open or closed.

## Regeneration instructions

- **Layout:** backlog detail page without a status pill. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`); `.subtitle` (`#666`, 0.95rem); `.intro-callout` (background `#f8f9fa`, left border `3px solid #2980b9`, 10px 14px padding, 0.93rem). One `.card-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 45% and `td.viz-col` 55%, both `vertical-align: top`, 12px padding. A final full-width `.key-point` div (30px top margin) closes the page.
- **Text blocks per section:** lead `<p>`, `<ul>` of four labeled bullets (0.92rem), a "How it's decoded" paragraph (0.9rem, bold lead-in), `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 8px 12px padding, 0.9rem, bold label), and an italic `.example` line (`#555`, 0.9rem). HTML entities used in source: `&mdash;`, `&rsquo;`, `&ldquo;`/`&rdquo;`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; purple `#8e44ad`; code-black `#1a1a1a`; brand colors Spotify `#1db954` and Snapchat `#fffc00` in section 8 only; gray captions `#666`.
- **Canvas:** intrinsic 720×300 for all nine; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), `ctx.scale` back to logical coordinates via a shared `setup(id)` helper that also clears the rect; module fill patterns are deterministic sin/cos threshold functions of grid indices.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
