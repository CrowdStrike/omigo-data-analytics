# Tracking Data: Bluetooth Item Trackers

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows: What is it? / What does it collect? / Why is it collected?)
**HTML title tag:** Tracking Data: Bluetooth Item Trackers

**Subtitle:** A coin-sized tag with no GPS and no cellular radio. It is located by other people's phones, which supply the position it cannot measure itself.

## What is it?

Lede: A tag that broadcasts, and strangers' phones that supply the position.

- **The tag:** attached to keys, a bag or a bike. AirTag, Tile and Galaxy SmartTag work the same way
- **All it holds** is a battery and a Bluetooth radio
- **All it does** is broadcast a short identifier that changes over time
- **The finder:** any passing phone running the vendor's software hears the broadcast, pairs it with *its own* location, and uploads the pair

**The tag never knows where it is:** it cannot compute a position and has no way to send one. The location in the record was measured by a stranger's phone, so the record is only as good as that phone's own fix.

### Visualization (canvas `c1`, 720×320)

Chain-of-custody flow diagram for a single location report: four party boxes joined by labeled arrows, with a dashed "no path back" annotation.

- **Title** (bold 16px ink `#1a5276`, centered, y=22): "Where one location report actually comes from".
- **Four boxes** (128×76, y=66, tinted fills at 0.16 alpha, 1.5px strokes, bold 15px title in the party hue, two 13px `#2c3e50` lines), one hue each in SERIES order:
  - x=26, blue `#2a78d6`: "Tag" / "Bluetooth only" / "no GPS, no SIM"
  - x=194, green `#008300`: "Passing phone" / "adds ITS OWN" / "location + time"
  - x=362, violet `#4a3aa7`: "Vendor server" / "relays the report" / "cannot read the fix"
  - x=530, orange `#d95926`: "Owner's device" / "decrypts locally" / "shows a map pin"
- **Arrows** between boxes at mid-height, each drawn in the hue of the party it arrives at, with a 13px label above the shaft: "rotating ID" (green), "encrypted" (violet), "upload" (orange).
- **Return-path annotation:** dashed mute `#6b7280` bracket (dash 4/4) running below the boxes from under the last box back to under the first, with 13px mute caption "no path back — the tag is never told where it was seen". Drawn in neutral ink because it is a structural fact, not an error condition.
- **Bottom caption** (mute 13px, centered, y=228): "Schematic. The position in the record was measured by the finder, not by the tag."

## What does it collect?

- **A rotating identifier** broadcast by the tag — the same tag looks like a different tag later
- **The finder phone's own location**, plus an accuracy radius for that fix
- **A timestamp** for when the broadcast was heard
- **Nothing from the tag** beyond the identifier — there is no sensor to read

**Two documented design properties:** reports are end-to-end encrypted so only the owner's device can decrypt them, and the identifier rotates so a third party cannot follow one tag across sightings. Both show in the shape of the record — the finder uploads something it cannot read itself.

### Visualization (canvas `c2`, 720×320)

Horizontal segmented bar showing who supplies each part of a report, with leader lines to field lists.

- **Title** (bold 16px ink `#1a5276`, centered, y=22): "Who supplies each part of a report".
- **Segmented bar:** x from 40, total width 640, y=78, height 44. Three segments (tinted fill 0.22 alpha, 1.5px stroke, bold 14px label in segment hue), same parties/hues as C1:
  - "The tag", fraction 0.20, blue `#2a78d6` — fields: rotating identifier
  - "The finder phone", fraction 0.55, green `#008300` — fields: latitude, longitude, accuracy radius, timestamp
  - "The owner's device", fraction 0.25, orange `#d95926` — fields: the key that decrypts it
- **Leader lines** (1px, segment hue) drop from each segment's center to staggered rows (y=142, 168, 194) carrying 13px `#2c3e50` text: "The tag: rotating identifier"; "The finder phone: latitude, longitude, accuracy radius, timestamp"; "The owner's device: the key that decrypts it" (x clamped to 150…570).
- **Bottom caption** (mute 13px, centered, y=228): "Schematic — segment widths show how much of the record each party contributes, not bytes."

### Payload (below canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// One report as the owner's device sees it AFTER local
// decryption. Field names are PLACEHOLDERS — no wire
// format is published.
{
  // ── documented design: the tag broadcasts a rotating
  // public identifier; the finder encrypts its own fix
  // to that key, so only the owner can decrypt ──
  "tag_key_id":  "rot_pk_9c41…",   // rotates over time
  "payload":     "<encrypted by the finder phone>",
  "heard_at":    "2026-08-22T14:23:07Z",
  "location": {                    // the finder's fix
    "lat":        "<finder latitude>",
    "lon":        "<finder longitude>",
    "accuracy_m": "<radius of the finder's fix>"
  },
  // ── inferred / plausible ──
  "source":       "finder_network",
  "tag_gps":      null,   // no such sensor exists
  "tag_cellular": null    // no such radio exists
}
```

## Why is it collected?

**Stated purpose** (label pill `.lbl-purpose`)

- **Finding a lost object** — one good fix is enough to go and collect it
- **Borrowed position:** the tag cannot locate itself, so every participating phone is a potential reporter for every tag

**Additional consequence** (label pill `.lbl-effect`)

- A tag on a car or in a bag **reports wherever that object goes**, including when the object is a person's
- Vendors added **unwanted-tracker alerts** for that case — a delay-based threshold that trades nuisance alerts against missed ones, and it only fires on phones running the matching software

**The gaps are about bystanders, not the object:** a report exists only where someone else's phone passed within range, so the sequence samples how many people were around. Read as a movement trace, a quiet street looks like a stop.

### Visualization (canvas `c3`, 720×320)

Timeline of report arrivals over 90 minutes carrying one bag through three zones, showing the gap in a quiet zone.

- **Title** (bold 13px ink `#1a5276`, centered, y=26): "When a report arrived, over 90 minutes carrying one bag". Subtitle (12px mute, y=44): "the bag moved the whole time; the marks are where a passing phone heard it".
- **Zones** (rectangles y=96, height 46, tinted fills, 1px strokes; bold 12px zone label, 12px mute phones line, bold 13px report count above), time axis 0–90 min mapped to plot width (padding left 46, right 34):
  - 0–26 min, "busy high street", "many phones nearby", blue `#2a78d6` (fill alpha 0.14) — 9 reports
  - 26–67 min, "quiet lane", "almost none", orange `#d95926` (fill alpha 0.16) — 2 reports
  - 67–90 min, "station concourse", "many phones nearby", blue `#2a78d6` — 8 reports
- **Report ticks** (2px vertical marks, ±13px around axis y=188) at minutes: 2, 5, 7, 11, 14, 15, 19, 22, 25, 33, 61, 68, 70, 73, 77, 78, 82, 86, 89. Ticks inside the 33–61 gap window drawn orange, others blue.
- **Gap bracket:** orange 1.5px bracket under the axis from minute 33 to 61 (y≈222), labeled in bold orange 12px: "28 minutes with no report".
- **Axis ticks** (11px mute, centered): "0 min", "30 min", "60 min", "90 min"; thin `#e5e9ef` axis line.
- **Bottom captions (centered, italic):** `#2c3e50` 12px "Read as a movement trace, the quiet lane looks like a stop that never happened."; mute 11px "Illustrative — the spacing shows the mechanism, not a recorded journey."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width bordered table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas; the "What does it collect?" row also carries the `.payload-note` caption and `.payload` pre block under its canvas (both left-aligned). HTML-escape the angle-bracket placeholders in the payload (`&lt;…&gt;`).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li b` `#1a5276` weight 600, li 0.93em. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads the element's attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette tokens, `rr()` rounded-rect, `arrow()` for labeled horizontal arrows. Charts hardcode literal data arrays (no Math.random).
- **Palette (declared once as tokens):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and is not used on this page. Site-wide accent palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
