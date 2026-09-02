# Twilio

**Page type:** detail page (two-column obj-table: text left 45%, code sample + canvas right 55%, one row)
**HTML title tag:** Twilio — Platform APIs

**Subtitle:** Send and track text messages and phone calls — each one comes back as a record with its status and cost attached.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- A record per text message and per call, with its cost
- Delivery status updates pushed to your server as they change
- Call recordings and voice-quality stats
- A ready-made one-time-passcode (OTP) verification service

**Key-point callout:** **"Sent" is not "delivered", and "delivered" is hearsay.** "Sent" only means a phone carrier accepted the message; "delivered" depends on the carrier reporting back, which some carriers and whole regions never do. A delivery-rate dashboard therefore measures carrier reporting habits as much as actual delivery — and comparing that rate across countries compares two different measuring instruments.

### Watch out for

- A message can be billed, marked "sent", then silently dropped by carrier spam filtering — success and cost do not share a denominator
- Status updates arrive out of order and sometimes twice — only ever move a message's state forward
- Prices fill in late; pulling cost data right after sending undercounts
- A long message, or a single emoji, silently splits a text into multiple billed segments

### Code sample (right column)

Heading: **A message record — status and cost on one row**

```
{
  "sid": "SM_MASKED",
  "from": "+14155550142",
  "to": "+447700900123",
  "body": "Order 4417 shipped. Track: exmpl.co/t/4417",
  "status": "sent",
  "num_segments": "1",
  "price": "-0.04000",
  "price_unit": "USD",
  "error_code": null,
  "date_sent": "Sat, 22 Aug 2026 14:02:13 +0000"
}
// "sent" may be the final word: if the carrier never
// reports back, the message stays "sent" forever
```

### Visualization (canvas `smsLifecycle`, responsive width × 380)

State-machine flow diagram of SMS message statuses, with solid vs dashed edges distinguishing Twilio-observed transitions from carrier-reported ones.

- **Title (bold 13px `#1a5276`, top center):** "SMS Message Status Lifecycle — and where reporting stops"
- **Nodes:** rectangular boxes (~74–112px wide × 30px tall, bold 11px `#2c3e50` labels) laid out on a 5-column × 4-row grid (row y-centers 72, 142, 214, 286):
  - `accepted` — col 0, row 1, stroke `#1a5276`, fill `rgba(26,82,118,0.35)`
  - `queued` — col 1, row 1, stroke `#1a5276`, fill `rgba(26,82,118,0.35)`
  - `sending` — col 2, row 1, stroke `#1a5276`, fill `rgba(26,82,118,0.35)`
  - `sent` — col 3, row 1, stroke `#e67e22`, fill `rgba(230,126,34,0.18)`
  - `delivered` — col 4, row 0, stroke `#27ae60`, fill `rgba(39,174,96,0.20)`
  - `undelivered` — col 4, row 2, stroke `#e67e22`, fill `rgba(230,126,34,0.18)`
  - `failed` — col 3, row 3, stroke `#e74c3c`, fill `rgba(231,76,60,0.16)`
- **Edges** (1.6px lines with filled triangular arrowheads; straight when same row, bezier curve otherwise; 9px `#666` mid-edge labels):
  - accepted → queued, solid `#1a5276`, label "callback"
  - queued → sending, solid `#1a5276`, label "callback"
  - sending → sent, solid `#1a5276`, label "callback"
  - sent → delivered, dashed (5/4) `#27ae60`, label "carrier DLR"
  - sent → undelivered, dashed (5/4) `#e67e22`, label "carrier DLR + ErrorCode"
  - sending → failed, solid `#e74c3c`, label "rejected pre-handoff"
- **Silent third outcome:** short dashed (3/3) `#888` vertical line dropping from the bottom of the `sent` box, with italic 9px `#888` two-line note beside it: "no DLR ever returned" / "stays at \"sent\" forever".
- **Legend** (left-aligned, starting y=322, 10px `#2c3e50` text):
  - solid `#1a5276` line swatch: "solid: Twilio observes it directly — StatusCallback fires"
  - dashed `#e67e22` line swatch: "dashed: carrier-reported — optional, delayed, or absent"
- **Takeaway (italic 10px `#e74c3c`, bottom left):** "\"delivered / total\" measures carrier DLR coverage as much as delivery. Not comparable across regions."

## Official API References

- [Twilio docs](https://www.twilio.com/docs) — top-level documentation portal for all products
- [Messaging](https://www.twilio.com/docs/messaging) — SMS/MMS product docs including the Message resource and status callbacks

## Regeneration instructions

- **Layout:** single page: h1, `.subtitle`, `.verified` badge, then `h2` "Overview" followed by a one-row `.obj-table` (left `<td>` 45% with `.section-title` headings, bullet lists, and a `.key-point` callout; right `<td>` 55% with a `.section-title`, a `<pre>` code sample, and the canvas), then `h2` "Official API References" with a link list.
- **Page style:** body system sans-serif, `#2c3e50` text, white background, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` inline badge — background `#eaf2f8`, border 1px `#2980b9`, color `#1a5276`, 0.8em, radius 4px, padding 2px 10px; `.section-title` bold `#1a5276` 1.05em; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `.key-point` background `#f8f9fa`, left border 3px `#e74c3c`, padding 10px 14px, 0.93em; li 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="smsLifecycle" height="380">`, CSS `width: 100%`; redraws on window resize using `getBoundingClientRect()` width; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and `ctx.scale` back to logical coordinates (with `setTransform` reset before scaling).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#666`/`#888`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions.
