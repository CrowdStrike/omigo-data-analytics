# Tracking Data: Traffic Cameras (Pure Video)

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Traffic Cameras (Pure Video)

**Subtitle:** Municipal cameras at instrumented intersections record continuous video of passing traffic, held for a retention window measured in days to weeks.

## Section 1: What is it?

**Lede:** On a pure-video installation, the recording itself is the product.

- **Who installs them:** city and state transport departments, at traffic intersections
- **What is recorded:** continuous video of the approaches — vehicles, pedestrians and cyclists alike
- **Storage:** local or regional servers, held for a fixed retention window, then rolled off
- **Analysis on top** is usually vehicle counting for signal timing
- **Retrieval** of a specific segment happens after the fact, on request

### Visualization (canvas `c1`, 720×320)

Overhead schematic of a four-way intersection with four corner cameras and their coverage cones.

- **Background:** full-canvas light gray `#f5f5f5`.
- **Roads:** a horizontal band (60px tall) and a vertical band (60px wide) crossing at canvas center, filled `#e5e9ef`; white dashed (15/10, 2px) center lane markings on each road, broken at the intersection.
- **Crosswalks:** rows/columns of small 8×8 white squares (5 per side) on all four approaches.
- **Cameras:** four magenta `#d55181` dots (5px radius, 1.5px stroke) at the four corners (±55px from center in x and y), each with a coverage cone — a 90px-radius arc sector (±0.5 rad about the diagonal toward the intersection) filled in magenta tinted to alpha 0.12.
- **Vehicles:** filled rectangles on the roads — two blue `#2a78d6` (30×14) on the horizontal road, one green `#008300` (14×28) on the vertical road above center, one orange `#d95926` (14×28) below center.
- **Label (bold 15px blue `#2a78d6`, left-aligned at y=230):** "Schematic — coverage per approach depends on how the site is instrumented".

## Section 2: What does it collect?

- **Continuous video** of all traffic at the intersection
- **Every vehicle** that passes, with make, model and colour visible
- **Pedestrians and cyclists**
- **Timestamps** for every frame
- **Licence plates**, readable if close enough
- **Volume patterns** — traffic counts, peak hours

**Key point — What leaves the camera is a count, not a list:** a row is vehicles per lane per fifteen minutes. Once a bin says 143, no query recovers which vehicles they were or whether one car passed twice.

**Key point — Aggregation is one-directional:** the identity is still in the video file but not in the counts, so analyses built on the counts cannot be reopened at the vehicle level later.

**Key point — The null lane:** a detector that did not report reads as absent, not as zero traffic.

### Visualization (canvas `c2`, 720×320)

Stepped retention timeline: daily bars over 30 days showing footage availability decaying in stages.

- **Background:** full-canvas `#fafafa`.
- **Title (bold 16px blue `#2a78d6`, top center):** "Footage Retention Timeline".
- **Axes:** L-shaped axis in `#2c3e50` 1px, plot from x=80 to width−40, y=50 (top) to y=190 (bottom); x tick marks and 14px muted labels "Day 0", "Day 5", "Day 10", "Day 15", "Day 20", "Day 25", "Day 30"; rotated y-axis label (14px muted): "Footage Available".
- **Bars:** one bar per day (30 days), full plot height at first, stepping down and fading:
  - Days 0–6 ("Full retention"): solid blue `#2a78d6`, full height.
  - Days 7–13 ("Partial — some cameras still have it"): `rgba(26, 82, 118, 0.5)`, starting 40px below the top.
  - Days 14–20 ("Mostly gone"): `rgba(26, 82, 118, 0.25)`, starting 80px below the top.
  - Days 21–29 ("Deleted"): `rgba(26, 82, 118, 0.08)`, starting 110px below the top.
- **Stage annotations (14px, above the plot):** "Full" in blue at x=left+70, "Fading" in orange `#d95926` at x=left+220, "Mostly expired" in blue at x=left+400.
- **Caption (bold 14px blue, left-aligned below axis):** "Schematic — the window length is set by policy, and varies by operator".

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// No public API for a municipal video archive.
// Whole block is reconstruction from how such
// archives are normally organised.
// ── inferred / plausible ──
{
  "site_id":      "INT-0417",   // intersection, not a person
  "segment_file": "INT-0417/2026-08-22/14.mp4",
  "segment_start":"2026-08-22T14:00:00Z",
  "duration_s":   3600,
  "fps":          15,
  "retention_expires": "2026-09-12",

  // the count table derived from the same footage
  "interval_s":   900,          // 15-minute bins
  "counts": [
    { "lane": "NB-1", "vehicles": 143, "occupancy_pct": 11.2 },
    { "lane": "NB-2", "vehicles": 118, "occupancy_pct": 9.4  },
    { "lane": "SB-1", "vehicles": null, "occupancy_pct": null }
  ]
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Signal timing** — the counts feed the signal plan
- **Incident response** — footage is what an investigator reviews after a collision

**Label (effect pill):** Additional consequence

- Within the retention window the archive holds **every passing vehicle**, so one vehicle on one past afternoon is a query — **nothing had to be targeted at it**
- Across several cameras on a corridor, sightings assemble into a **partial route**

**Key point — A route is a guess about the gaps:** the stretches between cameras are never observed, so a vehicle that stayed on the corridor and one that left and came back produce the same chain of sightings. Cameras also sit on arterial roads, so their coverage samples where the money went, not the road network.

### Visualization (canvas `c3`, 720×320)

Corridor diagram: four camera sightings along a road with the unobserved gap between two of them, and two candidate routes through that gap.

- **Title (bold 14px `#1a5276`, top center):** "Four sightings, and the road between them".
- **Subtitle (12px muted, centered):** "the eight minutes from 14:07 to 14:15 are not recorded anywhere".
- **Road:** horizontal 10px `#e5e9ef` line at y=132 from x=46 to width−46, overlaid with a white dashed (10/9, 1.5px) center line; x maps 0–6.0 km along the corridor.
- **Cameras** (green `#008300`: 5.5px dot on a 1.5px stalk above the road, bold 12px green time label above, 11px muted km label below): 0.4 km at "14:02"; 1.8 km at "14:07"; 3.9 km at "14:15"; 5.6 km at "14:20".
- **Unobserved stretch:** the span between cameras 2 and 3 (1.8–3.9 km) shaded with a 60px-tall orange `#d95926` tint band at alpha 0.13.
- **Route A (stayed on corridor):** solid blue `#2a78d6` 2.5px line 20px above the road across the gap, labeled in bold 12px blue above it: "stayed on the corridor".
- **Route B (detour):** dashed (5/4) violet `#4a3aa7` 2px bezier dipping ~76px below the road between the same two cameras; two violet labels beneath: bold "or off the corridor and back", regular "about 3 km of side streets fits in eight minutes".
- **Caption 1 (italic 12px `#2c3e50`, centered):** "Both routes produce the same four rows. The table cannot tell them apart."
- **Caption 2 (italic 11px muted, centered):** "Schematic — distances and times are illustrative, not a measured trip."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and not used here. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
