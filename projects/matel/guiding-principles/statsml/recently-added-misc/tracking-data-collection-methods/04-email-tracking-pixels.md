# Tracking Data: Email Tracking Pixels

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows)
**HTML title tag:** Tracking Data: Email Tracking Pixels

**Subtitle:** A remote image referenced by an HTML email. Fetching it produces a server log line at the sender, keyed to the recipient. There is no separate "open" signal in email — this request is the entire measurement.

## What is it?

A remote image request standing in for a read.

- **Mechanism:** HTML email references a remote image, often 1x1 and transparent
- **The URL carries** an encoded recipient and campaign identifier
- **Rendering the body** issues an HTTP GET for that URL
- **Unique per recipient**, so the log line attributes the fetch to one address on the list
- **Nothing comes back** beyond an ordinary image — the request itself is the signal

**Key point callout:** **The event is a request, not a read:** open rate, open time, device and location are inferences layered on that one request, and each layer can fail independently.

### Visualization (canvas `c1`, 720×320)

Schematic illustration: an email envelope with a hidden 1×1 pixel, dashed data-flow arrows to a tracking server box.

- **Envelope:** body rect from (60,60) to (320,190), fill `#f0f4f8`, stroke `#2a78d6` width 2; flap triangle (60,60)–(190,130)–(320,60) fill `#dce6f0`; three gray `#bbb` content lines at (85,140) 200×8, (85,155) 180×8, (85,170) 160×8.
- **Hidden pixel:** magenta `#d55181` dot radius 5 at (300,178) with two pulse rings (radius 10 at 0.4 alpha, radius 16 at 0.2 alpha); bold 13px magenta label "1x1 pixel" below at (300,200).
- **Data flow:** three dashed (5/4) magenta quadratic-curve arrows from the envelope's right edge to arrowheads at (520,50), (520,100), (520,150).
- **Server:** blue `#2a78d6` rounded rect (540,30) 150×145 radius 8; white bold 16px heading "Tracking Server"; white 14px lines: "IP: 72.134.xx.xx", "Time: 2:14:03 AM", "Device: iPhone 15", "Client: Gmail App", "Opens: 3rd time".
- **Caption (bottom center, 14px `#6b7280`):** "You open the email. The pixel phones home."

## What does it collect?

- **The request** — that the beacon URL was fetched, and when
- **Recipient and campaign** encoded in the URL path
- **Source IP** of whatever issued the request
- **User-Agent**, which may name the client or a proxy
- **Repeat requests**, subject to caching
- **Precision** — a point event, usually logged to the second; how long the message stayed open is not observed

**Key point callout:** **The IP is often a proxy's:** Gmail routes remote images through its own proxy, and Apple's Mail Privacy Protection uses Apple-operated relays. Geolocation and device attribution from a proxied open describe the proxy.

**Key point callout:** **Bias runs both ways:** a relay can load content with no human opening the message, inflating the numerator; proxy caching means a genuine second read may generate no request.

**Key point callout:** **Open rate is biased:** the sign depends on the mailbox provider mix in the list.

### Visualization (canvas `c2`, 720×320)

Timeline chart: one recipient's opens across a day with device icons ("Your opens tracked across time and devices").

- **Title (bold 16px `#2a78d6`, top center):** "Your opens tracked across time and devices".
- **Axis:** horizontal line at y=150 from x=50 to x=670, stroke `#2a78d6` width 2, arrowhead at the right end.
- **Opens (x, time, device icon, label):** 110 "7:02 AM" phone "Phone"; 240 "9:15 AM" laptop "Work PC"; 370 "12:30 PM" phone "Phone"; 500 "6:45 PM" laptop "Home PC"; 620 "11:58 PM" phone "Phone".
- **Marks:** magenta `#d55181` dot radius 5 on the axis + magenta tick up 50px; above each tick a small line-drawn device icon in `#2a78d6` (phone: 20×32 rounded rect with translucent screen and home-button dot; laptop: 32×24 rounded screen with trapezoid base); below the axis the time in 14px `#2c3e50` and the device label in 13px `#6b7280`.
- **Badge (bottom center):** magenta pill (rounded rect (280,195) 160×26 radius 13) with white bold 15px text "5 opens logged in one day".

**Payload note (below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, verbatim):**

```
// The GET as it lands in the sender's access log.
GET /o/EXAMPLE_BASE64_TOKEN_redacted.gif  // Note: masked for illustration; real value is base64-encoded JSON with recipient + campaign IDs
Host: track.example-shop.com

// ── documented / standard ── HTTP request headers
User-Agent: Mozilla/5.0 (Windows NT 5.1; rv:11.0)
  Gecko Firefox/11.0 (via ggpht.com GoogleImageProxy)
Accept: image/avif,image/webp,image/*,*/*;q=0.8
X-Forwarded-For: 66.249.93.…
Date: Fri, 22 Aug 2026 02:14:03 GMT

// The open record derived from that one line:
{
  // ── documented / standard ── read off the request
  "recipient_id":  "48219",     // decoded from path
  "campaign_id":   "aug-sale",
  "requested_at":  "2026-08-22T02:14:03Z",
  "source_ip":     "66.249.93.…",

  // ── inferred / plausible ──
  "event":         "email_open",   // assumed, not observed
  "client":        "Gmail",        // from UA substring
  "device_type":   "unknown",      // proxy UA hides it
  "geo_city":      "Mountain View" // IP is Google's, not
                                   // the recipient's
}
```

## Why is it collected?

**Label (purpose pill):** Stated purpose

- **Reach** — email has no built-in delivery receipt, so the request stands in
- **Subject-line comparison**, and suppression of addresses that never respond

**Label (effect pill):** Additional consequence

- **Engagement scoring** — send frequency, re-targeting and list pruning run on open history, so the request rate shapes how much mail an address later receives
- Timestamps accumulate into a **daily activity pattern** for the address

**Key point callout:** **The model partly ranks mailbox providers, not people:** a segment on providers that prefetch images scores as engaged and gets more mail; a segment on clients that block images scores as dormant and gets pruned. Neither score is about the person reading.

### Visualization (canvas `c3`, 720×320)

Combined bar + dot-line chart over 24 hours: the observed "daily routine" is sampled only in the hours the campaign sent.

- **Title (bold 14px `#1a5276`, center):** "A routine read off five hours of the day"; subtitle (12px `#6b7280`): "a request can only arrive in an hour the campaign sent in".
- **Data (24 hourly values, index = hour):**
  - Sends: `[0,0,0,0,0,0, 0,0,140,0,0,0, 0,110,0,0,0,90, 0,0,60,0,40,0]` (nonzero at hours 8, 13, 17, 20, 22).
  - Opens (requests recorded): `[0,0,0,0,0,0, 0,0,38,0,0,0, 0,21,0,0,0,29, 0,0,17,0,4,0]`.
- **Plot:** x from 62 to 672, 24 equal slots; baseline y=214, top y=76. Sends scaled to max 140 as bars, fill `#2a78d6` at 0.30 alpha with solid blue stroke. Opens scaled to max 40 as orange `#d95926` dots radius 5 connected by a dashed (4/3) orange line across the five sent hours, with bold 12px orange annotation "the curve read as \"their daily routine\"" near the hour-8 point.
- **Unsampled hours:** every hour not in {8,13,17,20,22} shaded with `#6b7280` at 0.07 alpha over the full plot height.
- **Axes:** ink `#1a5276` L-shaped axis; hour labels every 3 hours "0:00" … "21:00" in 11px `#6b7280`; x-axis title "hour of day →".
- **Legend (three swatches, 12px):** translucent-blue square "messages sent that hour"; orange dot "requests recorded"; light-gray square "no send, so no observation possible".
- **Captions (bottom center):** italic 12px `#2c3e50` "The shaded hours are unsampled, not quiet — the grid is the campaign calendar."; italic 11px `#6b7280` "Illustrative counts — the shape of the sampling gap, not a measured campaign."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` + bullets/`.key-point` callouts, right `<td>` (55%, `text-align:center`) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre below the canvas, left-aligned).
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; li 0.93em with `li b` in `#1a5276` weight 600; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase — `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em `#666` italic. No nav bar, no back/home links.
- **Palette:** charts declare a tokens object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, violet:#4a3aa7, orange:#d95926, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; red is deliberately excluded from the series rotation, reserved for alarm states. Site palette anchors: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setupCanvas(id)` helper reads the element's own attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex,a)` translucent fill and `rr()` rounded-rect path.
- No `Math.random()` in charts — all data arrays are hardcoded literals; invented numbers are labeled "illustrative".
