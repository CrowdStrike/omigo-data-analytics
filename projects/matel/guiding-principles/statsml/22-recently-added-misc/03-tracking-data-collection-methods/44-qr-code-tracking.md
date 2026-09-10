# Tracking Data: QR Code Tracking

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows: What is it? / What does it collect? / Why is it collected?)
**HTML title tag:** Tracking Data: QR Code Tracking

**Subtitle:** Scanning a code issues an HTTP request to the URL printed inside it. The request carries the usual web fields, and the code's physical placement supplies a location the request itself does not contain.

## What is it?

Lede: A printed URL. The tracking is the web request it triggers.

- **Mechanism:** scanning opens the URL, producing an ordinary web request
- **The request carries** IP address, user agent, referrer, and whatever cookies the destination already has
- **Nothing about the square** is itself a tracking mechanism
- **What placement adds** is a location: a code fixed to table 14 of a venue means a request with that campaign parameter almost certainly originated at that table

**The scan population changed:** use became routine during COVID, when codes replaced shared surfaces for menus and check-ins. A mechanism scanned occasionally is now scanned habitually, so the population went from people motivated to scan to nearly everyone present. Historical and current scan rates are not comparable series.

### Visualization (canvas `c1`, 720×320)

Hub diagram: a simplified QR code in the center with dotted lines to the four fields a scan record carries, each tagged with its source.

- **Header strip:** tinted ink band `rgba(26,82,118,0.07)` full width, 28px tall; bold 15px ink `#1a5276` centered title "Fields a scan request carries".
- **QR code:** 50×50 ink `#1a5276` square centered at canvas center, with a white checkerboard pattern (alternating 6×6 cells) and three corner finder squares (14×14 ink, 8×8 white inset, 4×4 ink center) at top-left, top-right, bottom-left. Ink, not a series colour — the printed square is the subject.
- **Four field targets** (dotted 1.5px lines dash 4/4 from center; 24-radius circles with 0.22-alpha tinted fill and 2px stroke; emoji icon; 14px `#2c3e50` label; 12px source line in the field's hue), one hue each in SERIES order:
  - (120,60), blue `#2a78d6`, 📱: "Device & network" — "from the request"
  - (600,60), green `#008300`, 📍: "Placement" — "from the print"
  - (120,180), violet `#4a3aa7`, 🕐: "Timestamp" — "from the request"
  - (600,180), orange `#d95926`, 🪪: "Cookie on destination" — "from the page"
- **Footer band:** green-tinted band `rgba(0,131,0,0.10)` full width, 30px tall at the bottom; 13px green `#008300` centered text "Placement is supplied by the print, not by the device".

## What does it collect?

- **IP address and user agent** from the request
- **Timestamp** of the request
- **Which printed code** was used, and therefore where it is mounted
- **Repeat check** — whether the same device requested that code before
- **Campaign variant** the code belongs to, one code per placement
- **Behaviour on the destination page**, subject to that page's own tracking

**The interesting field is `redirect_to`:** the printed square is fixed once it is on the table, but it encodes a short link whose destination is a server-side setting. The same code can point at a menu today and elsewhere next month, with no visible change to the thing that was scanned. Consent given to the placement does not carry to the destination.

### Visualization (canvas `c2`, 720×320)

Dot matrix of days × places: recorded scans (solid dots) vs visits with no scan (hollow dashed dots) — an irregularly sampled series.

- **Header strip:** tinted ink band, 26px; bold 15px ink centered title "A scan series samples only the moments someone chose to scan (schematic)".
- **Grid:** x-axis days Mon–Sun (7 columns at x = 100 + i×88, vertical gridlines `#e5e9ef`, 15px mute labels at y=258); y-axis five places, one row each at y = 54 + i×36 with horizontal gridlines and a 4×16 hue swatch at x=78: Cafe (blue `#2a78d6`), Office (green `#008300`), Gym (violet `#4a3aa7`), Restaurant (orange `#d95926`), Store (aqua `#199e70`). Row labels 14px in the row hue, right-aligned.
- **Recorded scans** (solid 7px-radius dots in the row's hue), at (day, place): (Mon, Cafe), (Mon, Office), (Tue, Office), (Wed, Cafe), (Wed, Restaurant), (Thu, Office), (Fri, Cafe), (Fri, Restaurant), (Sat, Store), (Sun, Gym).
- **Visits with no scan** (hollow white 7px dots, dashed 1.5px outline dash 2/2 in the row hue at 0.55 alpha), at: (Tue, Gym), (Wed, Office), (Thu, Gym), (Fri, Office), (Sat, Restaurant), (Sun, Cafe). Present-in-data vs absent stays the fill distinction.
- **Legend** (y≈288, hue-neutral in ink so it reads as the encoding key rather than a sixth place): solid ink dot + `#2c3e50` 13px "recorded scan (filled, row colour)"; hollow dashed mute dot + mute text "visit with no scan — absent from the data".

### Payload (below canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// A scan is just an HTTP request to the URL printed
// inside the code. Redirect services do not publish a
// common schema, so field names here are generic.
{
  // ── present in the raw record ──
  "short_code":   "r/7fQ2x",          // what the ink encodes
  "campaign_id":  "menu-table-14",
  "ts":           "2026-08-22T12:41:09Z",
  "client_ip":    "203.0.113.42",
  "user_agent":   "Mozilla/5.0 (iPhone; …) Safari/…",
  "redirect_to":  "https://…/menu?src=qr&c=menu-table-14",
  "http_status":  302,

  // ── inferred / plausible ──
  "placement":    { "venue": "…", "table": 14 },
  "geo_city":     "Austin",           // IP lookup, not GPS
  "repeat_scan":  true,               // same code, same device
  "first_seen":   "2026-06-02"
}
```

## Why is it collected?

**Stated purpose** (label pill `.lbl-purpose`)

- **A contactless way** to reach a page
- **Attribution:** a distinct code per placement tells which poster, table or mailer produced a visit — print returns no click log, so the code is the log

**Additional consequence** (label pill `.lbl-effect`)

- **Attribution and presence are the same record:** a code that identifies the poster also identifies where the scanner stood
- A series of scans becomes a **sparse location history**, from marketing data rather than a location permission

**Selection effect in the placement comparison:** the measured quantity is scans per placement, but the people who scan are not the people who saw it. A code where people are already waiting out-performs one that requires stopping, whatever the poster says. And the places that can appear at all follow a print run, not the layout of a town.

### Visualization (canvas `c3`, 720×320)

Horizontal bar chart: relative ease of scanning by placement type, showing that scan rate confounds message with convenience.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "Scan rate confounds the message with the ease of scanning".
- **Bars** (bars start at x=250, max width 330, height 34, vertical gap 24, starting y=52; full-width `#e5e9ef` track behind each bar; fill = 0.30-alpha tint of the row hue, 1.5px stroke; 13px `#2c3e50` right-aligned label left of the bar; 12px mute note inside the track at the bar's left edge), one hue per placement in SERIES order:
  - "Table tent, seated diner" — ease 0.92, blue `#2a78d6` — note "phone already out, no time pressure"
  - "Waiting-room poster" — ease 0.78, green `#008300` — note "stationary, nothing else to do"
  - "Shop window at eye level" — ease 0.45, violet `#4a3aa7` — note "requires stopping"
  - "Roadside billboard" — ease 0.12, orange `#d95926` — note "brief, often not a passenger"
- **Axis label** (13px mute, centered, y=288): "relative ease of scanning — illustrative shape, not measured rates".
- **Footer band:** orange-tinted band `rgba(217,89,38,0.10)` full width, 26px tall at the bottom; 13px orange `#d95926` centered text "Ranking placements by scans ranks their convenience, not their creative".

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width bordered table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas; the "What does it collect?" row also carries the `.payload-note` caption and `.payload` pre block under its canvas (both left-aligned). The `&` in the payload's redirect URL is HTML-escaped (`&amp;`).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li b` `#1a5276` weight 600, li 0.93em. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads the element's attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette tokens, `rr()` rounded-rect. Charts hardcode literal data arrays (no Math.random).
- **Palette (declared once as tokens):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and is not used on this page. Site-wide accent palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
