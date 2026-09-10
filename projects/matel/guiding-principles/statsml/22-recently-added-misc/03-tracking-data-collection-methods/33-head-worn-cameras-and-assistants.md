# Tracking Data: Head-Worn Cameras and Assistants

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas/payload right 55%, one row per section)
**HTML title tag:** Tracking Data: Head-Worn Cameras and Assistants

**Subtitle:** Eyewear with an outward-facing camera and a voice assistant. Every other mechanism in this set records the person who owns the device; this one records whoever is in front of it.

## Section 1: What is it?

Lede: A camera, microphone and speaker in a pair of glasses, paired to a phone.

- **Examples:** Ray-Ban Meta is the widely sold one; Snap Spectacles and earlier attempts work the same way
- **Capture is deliberate** — a button press or a spoken command
- **Indicator light** turns on while the camera runs
- **Sensor faces outward:** a fitness band or heart-rate strap measures its wearer; this measures the people and places in front of the wearer

Key point callout: **The bystander is the unit of observation:** the wearer is the account holder; the subject of the recording generally is not. Every consent screen, privacy setting and deletion control attaches to the wearer's account, so the usual reasoning that a user agreed to the terms does not describe most of the people who appear in the data.

### Visualization (canvas `c1`, 720×320)

Two-panel schematic: sensor direction decides who the unit of observation is (inward-facing vs outward-facing wearables).

- **Title (bold 16px `#1a5276`, centered at y=24):** "The direction the sensor faces decides who is measured".
- **Panels:** two 322×232 rounded rects (radius 6) at x=26 and x=372, y=40, white fill, 1.6px border in the panel hue, with a filled 30px header band in the hue and white bold 14px title text.
  - Left panel, hue aqua `#199e70`, title "Inward-facing": kind line (13px `#6b7280`): "Fitness band, heart-rate strap, biometric reader". A stroked head circle (radius 15) with an aqua 2.4px arrow that curls back onto the wearer (quadratic curve with filled arrowhead pointing at the head). Bold 13px `#2c3e50`: "Unit of observation: the wearer". 13px mute: "Account holder and subject are the same person". Bottom pill (rounded rect, hue fill at 12% alpha, hue 1px border, 12px hue text): "Has an account, a setting, and a delete button".
  - Right panel, hue magenta `#d55181`, title "Outward-facing": kind line: "Camera and microphone in eyewear". Head circle with a straight magenta arrow pointing away at a second, dashed-outline (3/3) head circle labeled "in frame" (12px magenta). Bold 13px: "Unit of observation: whoever is in frame". 13px mute: "Account holder and subject are different people". Bottom pill: "No account, no setting, no notification".
- **Caption (13px `#6b7280`, bottom center):** "Schematic. Both are wearables; only one of them measures its wearer."

## Section 2: What does it collect?

- **Stills and short clips**, on a button press or voice command
- **Audio** alongside a clip, including the voices of people nearby
- **Voice queries** to the assistant, and the replies
- **Scene description** — for a multimodal query, text describing what the camera framed
- **Object and scene labels** produced by a model, not read off the image
- **Device state** — battery, charge cycles, firmware, pairing history
- **Timestamps and location**, supplied by the paired phone rather than the glasses

Key point callout: **The description outlives the image:** asking the assistant what it is looking at converts a frame into a sentence — small, searchable, and easy to keep long after the frame is gone. A description that mentions who was present is a record about those people.

### Visualization (canvas `c2`, 720×320)

Flow diagram: one capture event fans out into four record boxes with relative-size bars — the smallest records last longest.

- **Title (bold 16px `#1a5276`, centered at y=24):** "One capture, four records, different lifespans".
- **Capture node:** 156×30 rounded rect (radius 5) centered at (360,58), fill `#2a78d6`, white bold 13px label "one capture". A 1.4px connector line in each record's hue runs from the node to each record box.
- **Record boxes (156×52, radius 5, at y=130, 14px gaps, centered as a row):**
  - "Image frame" — blue `#2a78d6`, note "large, often local", heavy record (fill alpha 0.10, 1px border, size bar 116px wide labeled "large")
  - "Audio track" — green `#008300`, note "voices nearby", heavy (bar "large")
  - "Voice query" — violet `#4a3aa7`, note "what was asked", light record (fill alpha 0.20, 2px border, size bar 26px wide labeled "small")
  - "Scene description" — orange `#d95926`, note "model text, people included", light (bar "small")
  - Box label bold 13px in the hue; note 12px `#2c3e50`; size bar is a rounded 9px-high bar in the hue below the box with an 11px `#6b7280` size word.
- **Summary strip:** full-row rounded rect at y=244 (34 high), orange fill at 10% alpha, orange 1.2px border, 13px orange text: "A sentence is cheap to store, easy to search, and outlives the frame it came from."
- **Caption (13px `#6b7280`, bottom center):** "Schematic. Bar widths show relative size only, not measured bytes."

Below the canvas, payload note (italic gray): "Sample payload — illustrative structure, not real captured data."

Payload block:

```
{
  // Field names are placeholders. No wire format for this class of
  // device is public, so the record below is a reconstruction of shape.
  "capture_id": "cap_7f2a…",
  "account_id": "acct_4b19…",       // the wearer — the only party with an account
  "device":     { "model": "<glasses model>", "fw": "<version>" },

  // ── observable on the device or in the companion app ──
  "media_type":      "image",
  "captured_at":     "2026-08-23T17:41:22Z",
  "indicator_state": "on",           // capture light, not defeatable in software
  "audio_included":  true,
  "location":        { "src": "paired_phone",
                       "lat": "<deg>", "lon": "<deg>",
                       "accuracy_est": "<phone's own estimate>" },

  // ── inferred / plausible ──
  "assistant_query":   "<spoken question about the scene>",
  "scene_description": "<model text describing the frame, people included>",
  "object_labels":     ["<label>", "<label>"],
  "people_in_frame":   "<count>",   // a count of people who hold no account
  "subject_consent":   null           // no such field, and no path to populate one
}
```

(In the HTML source the angle brackets inside the payload are escaped as `&lt;`/`&gt;` entities.)

## Section 3: Why is it collected?

Label pill (Stated purpose):

- **Hands-free capture** of a moment without reaching for a phone, plus **live translation and captioning**
- **Answering questions** about what is in front of the wearer, which for someone with low vision is genuinely useful and hard to do any other way

Label pill (Additional consequence):

- **First-person imagery** of ordinary streets, shops and gatherings, at eye level — **pre-selected by attention**, and close to an ideal corpus for training multimodal models
- **Bystanders are in it**

Key point callout: **The indicator light is a detection problem, not a guarantee:** it notifies only someone looking at it, close enough to see it, in light where it shows, who knows what it means. Each condition can fail, so the light carries two error types — missed and false. What fraction of the people in frame resolve it correctly is not established here.

### Visualization (canvas `c3`, 720×320)

Two stacked panels reading the same distance two ways: the indicator light shrinks with distance (line chart, top) while the camera frame widens (bar chart, bottom). Pure geometry, computed from a 5 mm indicator and a 90° horizontal field of view.

- **Title (bold 14px `#1a5276`, centered at y=22):** "Distance from the wearer, read two ways".
- **Distances (x positions):** 1, 2, 3, 4, 6, 8 metres, mapped linearly across the plot area (left pad 92, right pad 168).
- **Top panel (y 46–132), apparent size of the indicator:** violet (`#4a3aa7`) 2px line with 3.5px dots at `arcmin(d) = 2·atan(0.0025/d)·(180/π)·60` — approximately 17.2, 8.6, 5.7, 4.3, 2.9, 2.1 arcmin; y-scale 0–18 arcmin; `#e5e9ef` baseline. Right-side labels: bold 12px violet "how large it looks", 11px `#6b7280` "17 arcmin at 1 m", bold 12px violet "2 arcmin at 8 m". Y-axis unit label "arcmin" (11px mute, right-aligned at the pad).
- **Bottom panel (y 168–236), width of the frame:** orange (`#d95926`) bars 34px wide, fill orange tint 0.30, at `frameM(d) = 2·d` — 2, 4, 6, 8, 12, 16 m; y-scale 0–18 m; value labels "2 m"…"16 m" (11px `#2c3e50`) above bars and distance labels "1 m"…"8 m" (11px mute) below the baseline. Right-side labels: bold 12px orange "how wide the frame is", 11px mute "more people in it". Y-axis unit label "metres"; x-axis label "distance from the wearer".
- **Captions (bottom center):** italic 12px `#2c3e50`: "The people least able to resolve the light are the ones most likely to be in frame."; italic 11px `#6b7280`: "Illustrative geometry — a 5 mm indicator and a 90° field of view. Not measured from a device."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, text-align center) holds the canvas, and in the "What does it collect?" row also the `.payload-note` and `.payload` `<pre>` (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em italic `#666` directly above; angle-bracket placeholders escaped as HTML entities.
- **Canvas:** 720×320 intrinsic attributes; a shared `setupCanvas(id)` reads the element's own width/height attributes and scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Charts use hardcoded literal data or closed-form geometry (no Math.random), with a `tint(hex, alpha)` helper for translucent fills and an `rr()` rounded-rect helper.
- **Palette (tracking-set tokens, declared once as `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for alarm states; navy `#1a5276` is ink only (headings, axes, callout borders). Project-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links elsewhere use `.html` extensions.
