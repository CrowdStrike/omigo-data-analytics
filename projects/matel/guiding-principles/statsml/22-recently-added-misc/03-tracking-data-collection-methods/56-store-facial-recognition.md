# Tracking Data: Store Facial Recognition

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Store Facial Recognition

**Subtitle:** An entrance camera compares each passing face against a watchlist gallery. Whether an alert is likely to be correct depends less on the matcher than on how rare a real match is in the stream being searched.

## What is it?

An entrance camera searches each face against a stored gallery.

- **Template:** each detected face is converted to a numeric vector
- **Gallery:** the template is compared against a watchlist of stored templates
- **Threshold:** a similarity score above the configured value raises an alert
- **Attribute estimators** also run, outputting an age band and similar labels for every face, matched or not

**1:N search, not 1:1 verification:** unlocking a phone compares one face against one template. Here each face is compared against every gallery entry, so the comparison count — and the chances of clearing the threshold by coincidence — scales with gallery size and footfall.

### Visualization (canvas `c1`, 720×320)

Pipeline diagram: camera icon, three detected faces with detection boxes, a gallery database cylinder, connecting comparison lines, a stage legend, and summary text. Hue encodes pipeline stage: capture violet `#4a3aa7`, probe/compare blue `#2a78d6`, gallery aqua `#199e70`, above-threshold outcome orange `#d95926`.

- **Camera icon (violet, top left):** filled rectangle 60×40 at (30, 40) with a triangular lens flap on the right; white circle radius 12 with a violet inner circle radius 6 at (60, 60); bold 10px violet label "capture" at (70, 96).
- **Faces (three, circles radius 20 with 56×56 detection boxes and 9px labels above):**
  - (200, 70) — "no match": circle filled blue tint `rgba(42,120,214,0.35)`, dashed box (dash 3/3, width 1.2) in blue tint 0.6, blue label.
  - (310, 90) — "no match": same styling.
  - (420, 60) — "above threshold": solid orange `#d95926` circle, solid orange box width 2, bold orange label.
- **Gallery database (aqua `#199e70`, right):** cylinder made of two filled ellipses (50×15) at y=80 and y=130 with a 100×50 body at x=550, plus a stroked mid ellipse at y=100; white bold 14px centered text "GALLERY" at (600, 110).
- **Comparison lines:** dashed (dash 4/4, width 1.5) blue tint `rgba(42,120,214,0.35)` lines from each face's right edge to the gallery at (550, 100) — drawn for all three faces, since every face is compared.
- **Stage legend (row of 10×10 swatches with 11px labels starting at x=120, y=143, 140px apart):** violet "capture", blue "template + compare", aqua "gallery search", orange "above threshold".
- **Text lines (left-aligned at x=120):** bold 15px blue at y=178: "1:N search — each face is compared against every gallery entry"; 13px muted `#6b7280` at y=200: "Comparisons per hour = faces past the camera × entries in the gallery."; bold 13px aqua at y=218: "Both multiplicands are set by the operator, not by the matcher."

## What does it collect?

- **Face template** — the numeric vector, not a photograph
- **Timestamp and camera** per detection
- **Similarity score** against the top gallery entry, and the threshold in force
- **The alert decision,** and whether a human reviewed it
- **Estimated attributes** — age band and similar, as model outputs
- **Re-detections** of the same template, within a store and across stores on one gallery
- **Co-presence** — faces detected together in the same frame

**The threshold is a store setting,** not a property of the face. Lower it and alerts rise; raise it and known matches are missed — same camera, same shopper, different outcome.

**Base rate is the harder problem:** the gallery is small relative to the stream of shoppers, so even a low per-comparison error rate produces far more false alerts than true ones. Precision is governed by how rare a real match is, and no accuracy figure for the matcher alone states what fraction of alerts are correct.

### Visualization (canvas `c2`, 720×320)

Store floor plan with three unattributed movement trails between aisles.

- **Store shell:** ink `#1a5276` rectangle 600×180 at (60, 30), width 2, with bold 13px ink label "ENTRANCE" at (62, 25).
- **Aisles:** five vertical bars 20×120 at x = 120, 230, 340, 450, 560 (y=60), filled ink tint `rgba(26,82,118,0.1)`; centered 11px muted labels at y=195: "Produce", "Dairy", "Snacks", "Drinks", "Checkout".
- **Trails (quadratic-curve paths at globalAlpha 0.6):**
  - Longest path, blue `#2a78d6`, width 4: from (80,120) curving through (180,90), (350,100), (560,130) to (640,150).
  - Medium path, orange `#d95926`, width 3: from (80,140) through (240,130), (350,140) to (640,155).
  - Short path, violet `#4a3aa7`, width 2: from (80,100) through (300,80), (380,110), ending at (400,150) — leaves without reaching checkout.
- **Legend (13px, y=225, each entry in its path's hue):** "━ longest path" (blue, x=80), "━ medium path" (orange, x=220), "━ short path — left without reaching checkout" (violet, x=380).
- **Caption (11px muted, x=62, y=238):** "Paths are unattributed traces between instrumented cameras — schematic. Gaps between cameras are interpolated, not observed."

### Payload (under canvas `c2`)

Caption (italic, gray): "Sample payload — illustrative structure, not real captured data."

```
// Vendor match logs are not published. Field names
// are reconstruction; the scoring/threshold structure
// is standard to any gallery-matching system.
{
  // ── inferred / plausible ──
  "alert_id":       "al_2c84…",
  "store_id":       "store-0417",
  "camera_id":      "entrance-north",
  "ts":             "2026-08-22T17:06:44Z",
  "probe_template": "<binary, 512-dim>",
  "gallery":        "watchlist-regional",
  "top_match": {
    "gallery_entry": "wl_0093…",
    "similarity":    0.71,
    "threshold":     0.68,   // operator-configurable
    "decision":      "alert"
  },
  "reviewed_by":    null,    // no human check recorded
  "attributes":     { "age_band": "35-44", "…": "…" }
}
```

## Why is it collected?

**Stated purpose** (label pill)

- **Loss prevention and staff safety** — a small gallery of people under a trespass notice or in an active incident, matched at the door
- **Footfall counting,** unattributed, needing no identity at all

**Additional consequence** (label pill)

- A **persistent template for every face** that passes, not only gallery hits
- **Re-identification across visits and sites**, and **demographic labels for the whole stream** — the estimators run on everyone

**Enrolment has no audit:** the record shows a match against an entry, not why the entry exists or on what evidence. So the gallery decides who counts as a match without recording why — and an alert draws attention, meaning a wrongly enrolled person is watched more closely, generating more observations that look like confirmation.

### Visualization (canvas `c3`, 720×320)

Two-pool flow schematic: a very large non-gallery pool and a tiny gallery pool both feeding an alert queue that is mostly false alerts. Exactly two hues carry meaning: non-gallery pool blue `#2a78d6`, gallery pool orange `#d95926`; frames and headings ink `#1a5276`.

- **Heading (bold 14px ink, x=20, y=18):** "WHERE ALERTS COME FROM — SCHEMATIC, NOT TO SCALE". Subheading (11px muted): "No accuracy figure is asserted. The argument is about the relative size of the two pools."
- **Pool A (large, blue):** rectangle 300×62 at (40, 46), fill `rgba(42,120,214,0.18)`, blue stroke width 1.5. Inside: bold 12px blue "Shoppers NOT on the gallery"; 11px muted "almost everyone who walks in"; bold 11px blue "a very small error rate × a very large pool".
- **Pool B (tiny, orange):** rectangle 52×30 at (40, 130), fill `rgba(217,89,38,0.22)`, orange stroke. Right of it: bold 12px orange "On the gallery"; 11px muted "rare — this is the base rate".
- **Arrows to the queue:** thick blue arrow (width 5) from Pool A to the queue labeled "false alerts" (11px blue); thin orange arrow (width 2) from Pool B labeled "true alerts" (11px orange). Arrowheads are filled triangles.
- **Alert queue:** ink-stroked rectangle 210×116 at (460, 60), bold 12px ink title above: "THE ALERT QUEUE". Stacked fill: top 82% in blue tint `rgba(42,120,214,0.30)` with bold 12px blue "mostly false alerts" and 11px muted "drawn from the large pool"; bottom 18% in orange tint `rgba(217,89,38,0.45)` with bold 11px orange "true alerts".
- **Closing text (left-aligned at x=40):** bold 12px ink at y=206: "Precision is governed by how rare a real match is — not by the matcher alone."; 11px muted lines at y=222/234/246: "Two operator settings move the queue without the population changing:" / "the match threshold, and how many faces are on the list." / "Raising either raises the alert count. Neither tells you whether one alert is right."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` paragraph + bullets + `.key-point` callouts, right `<td>` (55%, text-align center) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption and `.payload` `<pre>` block (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; bullets 0.93em with bold lead terms (`li b`) in `#1a5276`. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre` (angle brackets in the payload are HTML-escaped as `&lt;`/`&gt;`); `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width`/`height` attributes (720×320); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own attributes. Include helpers: `tint(hex, a)` producing an rgba tint of a palette token, and a rounded-rect path helper `rr()`.
- **Palette:** page charts use the tracking-set validated categorical palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` for headings/axes, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and does not appear. Site-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
