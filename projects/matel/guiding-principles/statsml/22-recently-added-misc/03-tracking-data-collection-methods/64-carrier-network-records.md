# Tracking Data: Carrier Network Records

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Carrier Network Records

**Subtitle:** The call log on your phone is a copy. The record the network keeps was created separately, for billing and routing, and the delete button on the handset does not reach it.

## Section 1: What is it?

**Lede:** To carry a call, the network writes it down.

- **Why it exists:** a call has to be connected, so the network needs which number called which, from which tower, for how long
- **Call Detail Record (CDR)** is that written-down version, with equivalents for messages and data sessions
- **Local view:** the handset's call log and message thread — convenient, editable, and not the record the network runs on

**Key point — User-generated and tracking data at once:** the user made the call, so the user generated it — but a second copy was created independently, for a purpose the user was not part of.

**Key point — One delete path, two copies:** there is usually no delete path — and often no read path — to the network's copy. "I deleted my call log" and "that call is gone" are two different statements.

### Visualization (canvas `c1`, 720×320)

Schematic diagram: one call event fanning out into two record boxes, with a delete arrow reaching only one.

- **Title (bold 16px, `#1a5276`, top center):** "One call, two records, one delete button".
- **Origin event:** centered at ~y=48, green (`#008300`) bold 14px text "a call is placed" with a filled green 5px-radius dot below it at y=62.
- **Two boxes** (250×82px, top y=104): left box at x=60, right box at x=(width−60−250). Each box: translucent fill of its accent at alpha 0.14, 2px accent stroke, bold 15px accent-colored title, then two 13px lines in `#2c3e50`.
  - Left box (accent blue `#2a78d6`): title "Local view on the handset", lines "call log entry, message thread" / "editable by the person".
  - Right box (accent violet `#4a3aa7`): title "Record on the network", lines "detail record written for billing" / "created independently".
- **Arrows:** two green (`#008300`) 1.5px lines from the call dot (y=70) down to the top center of each box, each ending in a small filled green arrowhead.
- **Delete indicators** (at y ≈ box bottom + 34):
  - Under left box: bold 14px blue (`#2a78d6`) label "delete" with a solid blue 2px vertical arrow (filled arrowhead) pointing up into the box.
  - Under right box: bold 14px violet (`#4a3aa7`) label "no delete path from the handset" with a dashed (5/5) violet 2px vertical line and no arrowhead.
- **Caption (bottom center, 13px, muted gray `#6b7280`):** "Schematic — the two copies were never the same object."

## Section 2: What does it collect?

- **Call Detail Records** — calling and called number, start time, duration, direction, and the cell site that carried the call
- **Message records** — sender, recipient, timestamp, typically message length
- **Content retention is a separate question** from metadata retention; the answer varies by operator and by jurisdiction
- **Cell-site registration** — a phone must say which tower it is under, or an incoming call cannot reach it, so the trail is a byproduct of the network functioning
- **Data-session records** — access point, open and close times, volume moved
- **Line and device identifiers** — SIM subscriber identity and handset equipment identity, which is how a device or SIM swap becomes visible
- **Satellite direct-to-device** extends the same trail past tower coverage — emergency SOS and satellite messaging on recent phones create routing and location records from places that previously produced none

**Key point — Nothing about what was said:** a CDR is a metadata record. That is not a reassurance — the fields above are enough to reconstruct who someone talks to, when, and roughly where from, which is often more analytically useful than content.

### Visualization (canvas `c2`, 720×320)

Timeline strip chart: four record-type rows across one 24-hour day, plus a footer callout band.

- **Title (bold 16px, `#1a5276`, top center):** "What one ordinary day writes on the network side".
- **Time axis:** x from 168 to 700 mapping fraction-of-day 0→1; muted 1px axis line below the rows with 5px ticks and 13px muted labels at "00:00" (0), "06:00" (0.25), "12:00" (0.5), "18:00" (0.75), "24:00" (1).
- **Rows** (top y=44, row height 38, each with a light gridline `#e5e9ef` and a right-aligned bold 14px label in the row hue):
  - "Call records" — blue `#2a78d6`, span segments (18px tall boxes, hue-tinted fill at alpha 0.20, hue stroke) at day fractions: [0.06–0.10], [0.34–0.36], [0.62–0.71], [0.88–0.90].
  - "Message records" — green `#008300`, tick marks (2px vertical strokes, 16px tall) at fractions: 0.03, 0.12, 0.13, 0.41, 0.55, 0.56, 0.79, 0.93.
  - "Cell attachments" — violet `#4a3aa7`, tick marks at: 0.01, 0.08, 0.19, 0.25, 0.33, 0.40, 0.47, 0.52, 0.60, 0.68, 0.74, 0.81, 0.87, 0.95.
  - "Data sessions" — orange `#d95926`, span segments at: [0.00–0.28], [0.30–0.58], [0.60–0.99].
- **Footer band** (full width minus 20px margins, 40px tall, violet tint fill at alpha 0.12 with a 4px solid violet left bar): bold 13px violet line "Cell attachments (violet) are written so the network can reach the handset —" then 13px `#2c3e50` line "that row fills in whether or not the person places a call, sends a message or opens a session."
- **Caption (bottom center, 13px muted):** "Illustrative".

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
{
  // ── core billing fields — well documented as a concept ──
  "record_type": "voice",
  "calling_number": "+1415555…",
  "called_number":  "+4479005…",
  "direction": "outbound",
  "start_time": "2026-08-14T19:41:08Z",
  "duration_sec": 214,
  "serving_cell_id": "310-260-41207-3",   // the cell, not a position
  "rated": true,

  // ── inferred / plausible operator enrichment ──
  "device_id_hash": "e7c1…",       // handset identity, hashed
  "roaming": false,
  "cell_class": "urban_small",     // coverage size band
  "peer_degree_30d": 43,           // distinct numbers contacted
  "peer_is_international": true
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Billing** — a per-minute or per-message charge cannot be computed without a per-call record
- **Routing** — a call cannot be delivered to a handset the network cannot locate

**Label (effect pill):** Additional consequence

- A schema built to settle **"what do we charge for this call"** gets queried for **"who does this person know, and where were they on Tuesday"**
- The fields did not change; **the question did**

**Key point — Fields good enough for money can be wrong for inference:** a duration rounded to a billable unit is fine for a charge and biased for a study of conversation length. Cell size was set by capacity and coverage rather than by locating anyone, so how much position the cell implies varies widely across one network.

### Visualization (canvas `c3`, 720×320)

Horizontal bar chart on a log x-axis: how wide an area one serving cell covers, across four coverage bands.

- **Title (bold 14px, `#1a5276`, top center):** "How wide an area one serving cell covers".
- **Subtitle (12px muted, centered):** "same field in every record — the width was set by capacity, not by locating anyone".
- **X-axis:** log10 scale from 0.05 km to 20 km; plot area from x=158 to width−92, baseline y=244. Light gridlines (`#e5e9ef`) with 11px muted labels at: "100 m" (0.1), "500 m" (0.5), "1 km" (1), "5 km" (5), "20 km" (20).
- **Bars** (26px tall, 18px gap, starting at y=72; each bar hue-tinted fill at alpha 0.32 with 1.2px hue stroke; row name right-aligned 13px `#2c3e50` left of plot; bold 13px hue-colored value label just right of the bar end):
  - "Small cell, indoors" — 0.1 km, label "~100 m", aqua `#199e70`
  - "Dense urban" — 0.4 km, label "~400 m", blue `#2a78d6`
  - "Suburban" — 1.5 km, label "~1.5 km", violet `#4a3aa7`
  - "Rural" — 8 km, label "~8 km", orange `#d95926`
- **Caption 1 (italic 12px `#2c3e50`, centered, below axis):** "A mobility model reads all four rows as the same kind of observation."
- **Caption 2 (italic 11px muted, centered):** "Illustrative widths — log scale, so the spread fits. Not measured from any network."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and not used here. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
