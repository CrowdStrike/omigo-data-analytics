# Tracking Data: Smart TV Content Recognition

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Smart TV Content Recognition

**Subtitle:** TV firmware samples on-screen audio or video, matches it against a reference database to identify the title, and reports the result to the manufacturer.

## What is it?

The panel samples what is on screen and matches it to a title.

- **The technique** is automatic content recognition (ACR)
- **The sample:** a short audio clip or a low-resolution frame hash
- **The match:** compared against a reference database to identify the exact title
- **Source-independent** — streaming app, cable box, games console, or a disc
- **Reported** to the manufacturer on a schedule

**It runs below the apps:** at the panel level, which is why it sees content a single streaming app's own analytics cannot, including input from devices plugged into HDMI.

### Visualization (canvas `c1`, 720×380)

Flow diagram: a TV set samples audio into a fingerprint, sends it to the manufacturer server, matches against a reference database, and produces a result; two summary bands at the bottom.

- **Title (bold 16px ink `#1a5276`, centered at y=28):** "Audio Fingerprinting: How the Set Identifies Content". Subtitle (14px mute `#6b7280`, centered at y=48): "one hue per stage — sampled on the set, matched against a reference, reported back".
- **Stage hues (SERIES palette):** the set blue `#2a78d6`, sample orange `#d95926`, server aqua `#199e70`, reference set green `#008300`, result magenta `#d55181`, fingerprint violet `#4a3aa7`.
- **TV:** dark body rect (80,95,200×140) in text `#2c3e50`, screen inset filled with blue tint at 85% alpha; stand rects below. On-screen fingerprint symbol: four violet concentric arcs (radii 10–40, angle 0.8π to 2.2π) around a 4px violet dot at (180,165). Captions centered at x=180: "The set" (15px text color, y=272), "short sample → fingerprint" (14px violet, y=292).
- **Sample leaving the set:** four orange arcs radiating from (280,155), radii 36–84 over −0.4π to 0.4π; labels at (420,192)/(420,210): bold 14px "fingerprint sent" / 14px "on a schedule" in orange. Dashed orange arrow (8/4, 3px) from (330,120) to (500,100).
- **Manufacturer server:** cloud shape from overlapping circles around (540–596, 78–92), filled aqua tint 30% alpha; label bold 15px aqua "Manufacturer server" at (568,132).
- **Reference set:** dashed green connector (5/3) from (568,142) to (568,178); database cylinder (two ellipses 34×12 at y=194 and y=222 plus connecting rect) filled green tint 28% alpha with 1.5px green strokes; label 15px green "Reference set" at (568,254).
- **Result band (right):** rounded rect (372,288,316×54, radius 8), magenta tint 14% fill, 2px magenta stroke; bold 14px magenta "Result: a title, plus a confidence" at (530,309); 13px text color "the title is looked up; the confidence is modelled" at (530,329).
- **Counterpart band (left):** rounded rect (32,288,316×54, radius 8), blue tint 10% fill; bold 14px blue "Runs in the firmware, below the apps" at (190,309); 13px text "no on-screen indicator while sampling" at (190,329).
- **Bottom caption (13px mute, centered at y=366):** "Source is irrelevant to the match: app, cable box, console, or disc all reach the panel."

## What does it collect?

- **Title on screen** at any moment
- **Channel changes**, and which channel was switched to
- **Watch duration** before a switch
- **Pause and fast-forward** through ads
- **Which apps** the set uses
- **Viewing schedule patterns**

**Title and interaction both recorded:** a pause, a rewind and a channel change are separate events with their own timestamps.

**The unit of observation is the set, not a viewer:** every identifier resolves to equipment — panel, firmware, household address — and there is no person field at all. A late-night children's title and a late-night thriller can be the same device with two different people in front of it, and nothing in the record distinguishes them.

### Visualization (canvas `c2`, 720×380)

Pie chart with legend: which source the set was showing (illustrative shares).

- **Title (bold 16px ink, centered at y=28):** "Which source the set was showing (illustrative shares)". Subtitle (14px mute, centered at y=48): "one hue per source — each is logged as its own category".
- **Data (label, value %, SERIES hue in order):** Streaming 42% blue `#2a78d6`; Cable/Live TV 22% green `#008300`; Gaming 15% violet `#4a3aa7`; YouTube/Social 12% orange `#d95926`; Blu-ray/DVD 5% aqua `#199e70`; Other Apps 4% magenta `#d55181`.
- **Pie:** center (250,205), radius 118, starting at −π/2, slices stroked white 2px; slices with value ≥10% carry a bold 15px white percentage label at 0.62 radius along the mid-angle.
- **Legend (right, starting y=96, rows 30px apart at x=420):** 14×14 rounded swatch in slice hue, label bold 16px in the slice's own hue, percentage right-aligned 15px mute at x=690.
- **Note (15px mute, centered at y=356):** "Each category carries its own timestamps; none of them carries a person".

Below the canvas (right column):

Sample payload — illustrative structure, not real captured data.

```
// No public ACR API — the whole block is reconstruction.
// ── inferred / plausible ──
{
  "event":         "acr_match",
  "device_id":     "TV-9F3C…",     // the set
  "household_ip":  "203.0.113.42",
  "panel_model":   "…-55Q",
  "firmware":      "4.2.117",

  "sample_interval_s": 2,          // fingerprint taken this often
  "match_confidence":  0.91,
  "matched_title": "Example Series S2E4",
  "matched_source": "hdmi_2",      // not an app; external box
  "match_offset_s": 1284,          // position within the title
  "ts":            "2026-08-22T23:16:40Z",

  "user_id":       null            // no such field exists
}
```

## Why is it collected?

**Stated purpose** (label pill, blue)

- **Recommendations** on the set
- **Measuring** which content is watched

**Additional consequence** (label pill, orange)

- **Licensed onward** to advertisers and measurement firms, and used for ad targeting on the TV and on other devices in the household
- **Regulatory action** has followed over collection without adequate disclosure; current sets generally present a setup prompt with an opt-out

**Census of sets, not a panel of households:** ratings panels sample households and weight them, which is what lets an estimate be projected to a population. ACR instead covers every set with the feature left enabled — far wider coverage, but no design behind who is in it, so a claim about people has nothing to rest on.

### Visualization (canvas `c3`, 720×380)

Two-band timeline: one evening on one set — an occupancy step-line (not a field in the record) above the title blocks the panel matched, with totals compared at the bottom.

- **Title (bold 16px ink, centered at y=28):** "One evening on one set, measured two ways". Subtitle (14px mute, centered at y=50): "the panel records the lower band; the upper band is not a field in the record".
- **Blocks (start minute from 18:00, duration min, title, people in room, SERIES hue):** `(0, 45, "cooking show", 1, blue)`, `(45, 45, "evening news", 2, green)`, `(90, 30, "children's film", 2, violet)`, `(120, 40, "film still on", 0, violet)`, `(160, 60, "drama series", 2, orange)`, `(220, 80, "late thriller", 1, aqua)`. Time span 300 minutes; padL 60, padR 40.
- **Upper band (occupancy):** baseline y=168, 26px per person; gridlines with right-aligned 12px mute labels 0/1/2. Dashed violet `#4a3aa7` step line (6/4, 2.5px) tracing people-per-block. Label bold 13px violet at top-left of band: "people in the room — no field for this", with 11px mute "illustrative" beneath.
- **Empty-room highlight:** the people=0 block (minutes 120–160) shaded `rgba(201,133,0,0.16)` with 1.5px yellow `#c98500` outline from y=100 down to the occupancy baseline; centered bold 12px yellow labels "set on," / "room empty" and 12px text-color "40 min".
- **Lower band (titles):** blocks at y=200, height 40, each filled with its hue at 0.38 alpha and 1px hue stroke; band label bold 13px ink "titles matched on the panel" above; title labels 12px in block hue centered below each block, alternating between two baseline rows so they fit.
- **Time axis:** gray line under the blocks; 11px mute hour labels "18:00", "19:00", "20:00", "21:00", "22:00", "23:00" at hourly positions.
- **Totals (bold 14px, y=322):** ink "300 set-minutes recorded" (left), violet "425 person-minutes watched", yellow "40 minutes with nobody there" (y=342); 12px mute "what the panel can report" under the first.
- **Captions (centered, italic):** 12px text color "The two totals come from the same evening, and only one of them is in the data." (h−24); 11px mute "Illustrative evening — the shape, not a measured household." (h−8).

## Regeneration instructions

- **Layout:** tracking detail page `.obj-table` — full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, `text-align: center`) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` caption plus `<pre class="payload">` block below the canvas, both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, first `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** declare intrinsic `width="720" height="380"`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Shared helpers: `rr()` rounded-rect path and `tint(hex, alpha)` rgba derivation from palette hexes.
- **Chart palette (tracking pages):** categorical CVD-checked tokens — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states, not in the series rotation. Page/site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links elsewhere use `.html` extensions.
