# OpenStreetMap, Mapbox & HERE

**Page type:** detail page (two-column obj-table layout: text left 45%, code + canvas right 55%, one Overview row)
**HTML title tag:** OpenStreetMap, Mapbox & HERE — Platform APIs

**Subtitle:** Map data two ways: OpenStreetMap gives you the whole world map (and its full edit history) for free to host yourself; Mapbox and HERE sell routing, travel times, and live traffic per call.

**Verified badge:** Last verified: August 2026

## Overview

Left column:

**What you can get**

- The complete OpenStreetMap database as downloadable files — including every past edit, so you can reconstruct the map as it stood on any date
- Targeted OSM queries ("all cafes in this area") via a shared community query service
- Routing, travel-time tables, and reachable-area maps from Mapbox and HERE
- Live traffic speeds and incidents — commercial vendors only; OSM describes the road, never its current state

**Key point (callout):** **OSM feature counts partly measure the mappers, not the world.** Coverage depends on where volunteers are active: a city with twice the mapped cafes may simply have twice the mappers. Never read raw counts as ground truth, and expect one-off mapping campaigns to show up as spikes in any time series.

**Watch out for**

- OSM's open licence obliges you to share databases derived from it — a legal question to settle before building, not after shipping
- OSM is a volunteer project: no warranty, no support contract, and tag conventions vary by region
- The commercial road graphs can't be downloaded or mirrored — you rent the answer, and the answer can change silently between versions
- Commercial results usually carry storage restrictions; check what you're allowed to keep

Right column:

**Overpass QL — cafes in a bounding box, with provenance** (section-head)

Code block (pre, verbatim):

```
[out:json][timeout:60];
node["amenity"="cafe"](52.51,13.38,52.53,13.42);
out meta;

// Each result element carries its full history:
{ "type": "node", "id": 2419312345,
  "version": 7,             // six prior states,
  "changeset": 158204411,   //   each retrievable
  "timestamp": "2025-11-04T18:22:41Z",
  "tags": { "amenity": "cafe",
            "name": "Kaffeehaus" } }

// That per-element history has no commercial
// equivalent — and note what is NOT here:
// no traffic, no travel time, no SLA.
```

**Capability matrix — nobody has all of it** (section-head above canvas)

### Visualization (canvas `capMatrix`, responsive width × 380)

Capability matrix: 10 capability rows × 3 provider columns, each cell a colored pill reading "yes" / "partial" / "no".

- **Title (bold 13px, `#1a5276`, top left):** "OSM owns the substrate and the past; the vendors own traffic and the SLA".
- **Columns (bold 11px headers, `#555`):** OpenStreetMap, Mapbox, HERE.
- **Cell states:** 0 = absent, red `#e74c3c`, mark "no"; 1 = partial / conditional, orange `#e67e22`, mark "partial"; 2 = available, green `#27ae60`, mark "yes". Cells are rounded-rect pills (≤92px wide, ≤20px tall), fill at alpha 0.85 with thin `rgba(0,0,0,0.12)` stroke, white bold 9.5px mark text.
- **Rows (label — OSM, Mapbox, HERE values):**
  - Bulk dataset download — yes, no, no
  - Full edit history — yes, no, no
  - Ad-hoc attribute query — yes, partial, partial
  - Redistributable derived data — partial, no, no
  - Routing / directions — partial, yes, yes
  - Isochrone / isoline — partial, yes, yes
  - Travel-time matrix — partial, yes, yes
  - Live traffic flow — no, partial, yes
  - Incident feed — no, no, yes
  - SLA / support contract — no, yes, yes
- **Row labels:** 10.5px right-aligned; first four rows colored `#1a5276`, remaining rows `#2c3e50`. Odd rows zebra `rgba(26,82,118,0.04)`. Grid border and column separators `#ddd`. Padding: top 52, right 20, bottom 46, left min(200, 36% of width).
- **Divider:** dashed 5/4 purple `#8e44ad` horizontal line (width 1.5) between row 4 and row 5, with italic 9.5px `#8e44ad` labels: "above: own the database" (just above the line) and "below: rent the answer" (just below).
- **Legend (bottom left, 11px color squares, labels `#666`):** "available" `#27ae60`, "partial / conditional" `#e67e22`, "absent" `#e74c3c`.
- **Bottom-right callout (italic 10px, `#e74c3c`):** "no column is all green — a geo stack is always a blend".

## Official API References

- [Planet.osm — OSM wiki](https://wiki.openstreetmap.org/wiki/Planet.osm) — planet dumps, extracts, and replication diffs
- [Mapbox API documentation](https://docs.mapbox.com/api/) — Directions, Map Matching, Isochrone, Matrix, Geocoding

## Regeneration instructions

- **Layout:** platform-apis detail page. h1, `.subtitle` paragraph, `.verified` badge span, then `h2` "Overview" followed by a full-width `.obj-table` with one `<tr>`: left `<td>` (45%) with `.section-head` headings ("What you can get", "Watch out for"), bullet lists, and one `.key-point` callout; right `<td>` (55%) with `.section-head` labels above a `<pre>` code block and the canvas. Then `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge inline-block, background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; obj-table cells `1px solid #e0e0e0` border, padding 16px; `.section-head` bold `#1a5276` 0.95em; `pre` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, padding 16px, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; list items 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="capMatrix" height="380">`, CSS `display:block; width:100%`; drawing script measures `getBoundingClientRect()`, scales backing store by `window.devicePixelRatio` via `setTransform(dpr,0,0,dpr,0,0)`, fixes CSS height to 380px, and redraws on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, zebra fill `rgba(26,82,118,0.04)`, gray text `#666`/`#555`.
