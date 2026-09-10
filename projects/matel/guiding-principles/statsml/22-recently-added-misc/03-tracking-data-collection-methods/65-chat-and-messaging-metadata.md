# Tracking Data: Chat & Messaging Metadata

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Chat & Messaging Metadata

**Subtitle:** End-to-end encryption covers the message body. The routing envelope — who, when, how large, over which connection — has to be readable by the server for delivery to work.

## Section 1: What is it?

**Lede:** Encryption covers the body; delivery still needs a readable envelope.

- **What E2EE covers:** the message body, unreadable to the server
- **What delivery needs:** a sender, a recipient, a timestamp, a size, a connection
- **Structural, not a design gap:** a router that cannot read an address cannot route
- **Why the envelope suits analysis:** timing, frequency and size are numeric and complete, so easier to compute over at scale than text
- **What that supports:** relational and temporal inferences — not statements about content

**Key point — A message event, not a conversation:** two people who move to a phone call, a different app, or a meeting in person produce a gap that looks identical to a lapse in the relationship.

**Key point — So closeness read off frequency is conditional:** it assumes that platform is where the relationship lives — an assumption the data cannot check.

### Visualization (canvas `c1`, 720×320)

Social graph: a central account connected to seven contacts, edge thickness encoding message count, a lock icon on each edge marking the encrypted body.

- **Title (bold 15px, blue `#2a78d6`, top center):** "Bodies encrypted; the edges are readable".
- **Nodes** (circles: hue fill at 20% alpha, 2px hue stroke, 13px `#2c3e50` label below each):
  - "Account" — center node at (360, 120), radius 22, violet `#4a3aa7`
  - "Contact A" (180, 60) r=16 blue `#2a78d6`; "Contact B" (540, 60) r=16 blue; "Contact C" (150, 170) r=14 blue; "Contact D" (280, 200) r=14 blue; "Contact E" (500, 180) r=16 blue; "Contact F" (600, 150) r=13 blue
  - "Contact G" (220, 130) r=13, muted `#6b7280` — an address-book account the record never sees exchange a message
- **Edges:** all from the center Account to each contact, stroked in magenta `#d55181` tinted to alpha 0.55; line widths (message count): A=3, B=4, C=2, D=2, E=6, F=1, G=1.5. A 13px lock emoji (🔒) drawn at each edge midpoint in green `#008300`.
- **Caption (13px muted, centered, y=234):** "Thickness = message count. The relationship behind an edge is not in the record."

## Section 2: What does it collect?

- **Contacts messaged** — phone numbers, address-book entries
- **Exact time** of every message sent and received
- **Response latency** — how quickly a reply arrives
- **Message frequency** between any two accounts
- **Group membership**, and who talks to whom within groups
- **Online/offline status** and "last seen" timestamps
- **Device and IP address**

**Key point — Encryption hides the body but not its length:** `ciphertext_bytes` and the two timestamps survive, and that is enough to work with.

**Key point — What size and timing yield:** two records of near-identical size seconds apart in different conversations look like a forward, and a reply arriving consistently within a few seconds separates close contacts from distant ones.

**Key point — A `null` read receipt is informative too:** its absence is a setting, and a setting is a signal.

### Visualization (canvas `c2`, 720×320)

Radial 24-hour activity clock on the left; two-column annotation list ("common reading" vs "also consistent with") on the right.

- **Title (bold 15px, blue `#2a78d6`, top center):** "Observed send times, and the readings applied to them".
- **Clock:** circle centered at (200, 125), radius 90, light `#e5e9ef` 1px stroke; 24 muted hour tick marks; 13px muted hour labels every 6 hours ("0:00", "6:00", "12:00", "18:00").
- **Activity bars:** radiating outward from inner radius 30, length scaled to max 50px at value 15, 8px round-cap strokes; hourly values (hours 0–23): `[0,0,0,0,0,0, 2,5,15,12,8,6, 10,4,3,5,8,12, 15,10,6,3,1,0]`. Bar hue keyed to band: hour 2 violet `#4a3aa7`; hours 23–5 muted `#6b7280`; hours 9–16 aqua `#199e70`; hours 18–21 magenta `#d55181`; all other hours blue `#2a78d6`.
- **Clock caption (12px muted, below clock):** "illustrative shape".
- **Right column headers (bold 13px):** "common reading" in blue at x=420, "also consistent with" in muted at x=516, y=40.
- **Annotation rows** (each: a 4px dot in the band hue at x=410, 14px `#2c3e50` reading text at x=420, 13px muted alternative at x=516):
  - y=72, muted: "Quiet 11pm–6am" / "phone in another room"
  - y=112, aqua: "Steady 9am–5pm" / "a work account, not a person"
  - y=152, magenta: "Peak 6pm–10pm" / "commute, not social time"
  - y=192, violet: "A 2am message" / "a different time zone"
- **Caption (13px muted, bottom center):** "The clock is measured. The column on the right is never ruled out."

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// End-to-end encryption covers the message body. The
// routing envelope below is what a server must read to
// deliver it. No platform publishes this schema, so the
// whole block is reconstruction.
{
  // ── inferred / plausible ──
  "msg_id":            "3EB0…",
  "from":              "+1512…",
  "to":                "+1737…",
  "sent_ts":           "2026-08-22T02:14:07Z",
  "delivered_ts":      "2026-08-22T02:14:09Z",
  "read_ts":           null,            // receipts disabled
  "ciphertext_bytes":  1184,
  "attachment":        { "kind": "image/jpeg", "bytes": 208441 },
  "conversation":      { "type": "group", "id": "g-91f…" },
  "client":            { "platform": "ios", "app_ver": "…" },
  "ip":                "198.51.100.7"
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Delivery** — routing needs an addressee, thread order needs timestamps, a retry needs delivery state
- **Billing and rate limits** need per-account counts; encrypting the envelope would break all of it

**Label (effect pill):** Additional consequence

- The same envelope is a **graph with weights and timing**, so relationships and routines can be inferred without reading a word
- **Size is part of it** — a forward is visible while the forwarded text is not

**Key point — Inferences that are never scored:** reply latency reads as closeness, but it also tracks time zone and work schedule, and a phone left charging in another room produces the same quiet stretch as a cooling friendship. The platform never learns the true relationship, so nothing in the data checks the guess.

### Visualization (canvas `c3`, 720×320)

Slope graph: the same six contacts' median reply latencies ranked two ways, crossings showing rank changes.

- **Title (bold 14px, `#1a5276`, top center):** "The same reply latencies, ranked two ways".
- **Column headers (bold 12px):** left, right-aligned at x=250 in blue `#2a78d6`: "Ranked by measured" / "reply speed"; right, left-aligned at x=470 in orange `#d95926`: "Ranked over messages that" / "arrived while they were awake".
- **Data** (contact, all-messages median, awake-only median, series hue):
  - Contact A — 4 min / 4 min, blue `#2a78d6`
  - Contact B — 9 min / 9 min, green `#008300`
  - Contact C — 22 min / 6 min, violet `#4a3aa7`
  - Contact D — 31 min / 31 min, orange `#d95926`
  - Contact E — 55 min / 12 min, aqua `#199e70`
  - Contact F — 140 min / 140 min, magenta `#d55181`
- **Layout:** left column sorted by all-messages median, right column sorted by awake-only median; rows start at y=100 with 30px step. Row labels bold 13px in the contact hue with a 3.5px dot: left side right-aligned "Name  N min" (all), right side left-aligned "Name  N min" (awake).
- **Slope lines:** each contact keeps its hue between its left and right rank position; contacts whose rank changed (C and E) drawn at alpha 0.85 width 2, unchanged contacts at alpha 0.30 width 1.2.
- **Caption 1 (italic 12px `#2c3e50`, centered):** "Two contacts look distant on the left only because messages reached them at night."
- **Caption 2 (italic 11px muted, centered):** "Illustrative medians — the crossings show the shape, not measured latencies."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and not used here. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
