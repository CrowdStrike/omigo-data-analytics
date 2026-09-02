# Tracking Data: Genetic Relative Matching

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Genetic Relative Matching

**Subtitle:** A DNA profile identifies relatives, not just its owner. So a database that covers a fraction of a population can reach a much larger share of it — including people who never submitted a sample.

## What is it?

Comparing two profiles yields one number: the total length of shared DNA.

- **Shared stretches:** relatives share long identical stretches inherited intact from a common ancestor
- **Falls off predictably:** full siblings share about half their DNA, and the expected share roughly halves with each further step of relatedness
- **Note "expected":** parent and child share almost exactly half, fixed by descent, but two full siblings vary around half depending on which segments each inherited
- **So a total maps to a set** of candidate relationships, not to one
- **Records close the gap:** public family trees, census records, obituaries and social media build out the branch until one candidate remains — forensic genetic genealogy

Key-point callout: **DNA narrows, records name:** the genetic step places an unknown person at an estimated distance in a family tree. The naming happens in the records step, using ordinary public documents.

### Visualization (canvas `c1`, 720×320)

Pedigree diagram: one enrolled node makes the whole branch reachable. Expected DNA share is an ordered quantity, so it is encoded as a one-hue light-to-dark blue ramp; the one categorical distinction — enrolled vs never tested — gets its own hue (green).

- **Title (bold 16px `#1a5276`, centered):** "One enrolled profile, a whole branch made reachable".
- **Nodes (circles r=16, hardcoded positions):** grandparent (1/4) at 300,62; grandparent (1/4) at 392,62; parent (1/2) at 246,130; aunt/uncle (1/4) at 446,130; enrolled ("E") at 168,196; sibling (1/2) at 300,196; cousin (1/8) at 420,196; cousin (1/8) at 530,196.
- **Node fill:** enrolled node solid green `#008300` with white bold "E" and 2.5px green stroke; all others blue `#2a78d6` at ramp alpha `0.16 + (frac/0.5)*0.64` (darker = larger expected share), showing the fraction text ("1/2", "1/4", "1/8") — white on 1/2 nodes, `#1a5276` otherwise. Label under each node (13px `#2c3e50`): grandparent, grandparent, parent, aunt/uncle, enrolled, sibling, cousin, cousin.
- **Edges:** right-angled connector lines `#e5e9ef` 1.5px joining gp1/gp2 to parent and aunt/uncle, parent to enrolled and sibling, aunt/uncle to both cousins; plus a partner line between the two grandparents.
- **Legend (right side):** a green dot with "submitted a sample"; the lines "never tested, now reachable" and "darker = larger DNA share"; then a 3-swatch blue ramp (40×14 each) labeled "1/8", "1/4", "1/2".
- **Bottom caption (13px `#6b7280`, centered):** "Fractions are the expected share of DNA in common — about 1/2 for full siblings, roughly halving per further step."

## What does it collect?

- **A profile per person** — the set of marker values used for comparison
- **Total shared length** with each other profile, in centimorgans (cM), the standard unit for genetic distance
- **Segment count** — how many separate shared stretches
- **Longest segment** — the length of the longest single shared stretch
- **Everything else derived** — relationship, ancestry, a name

Key-point callout: **One measured quantity does the work:** a length. The relationship label, the alternatives and the ancestry percentages are model output on top of it, and the intervals belong with them.

### Visualization (canvas `c2`, 720×320)

Shared-segment tracks plus a measured/inferred split. Page-wide convention: green = measured, blue = inferred from the measurement, orange = the one derived summary the chart calls out.

- **Title (bold 16px `#1a5276`, centered):** "The measurement is a length. Everything else is derived."
- **Two genome tracks** ("Profile A" at y=58, "Profile B" at y=96; labels right-aligned 13px `#2c3e50`): gray bars `#eef2f5` (560×22 starting at x=130) with `#e5e9ef` outline, overlaid by identical green `#008300` shared segments (fill alpha 0.28, green outline) at offsets/widths: (8,74), (118,26), (176,14), (236,46), (330,18), (402,32), (486,22).
- **Bracket:** orange `#d95926` bracket under the first (longest) segment, labeled "longest shared stretch" (13px orange).
- **Two columns** below (headed bold 15px): "Measured" in green at x=130 with bullets "total shared length (cM)", "number of shared stretches", "longest stretch"; "Inferred from it" in blue `#2a78d6` at x=430 with bullets "relationship distance", "ancestry composition", "candidate identity". Dashed `#e5e9ef` vertical divider between the columns.

### Sample payload (right column, under canvas `c2`)

Caption (italic): "Sample payload — illustrative structure, not real captured data."

```
// Relative-match record. Field names are placeholders,
// not a vendor schema. Every number is illustrative.
{
  // ── measured by the comparison ──
  "profile_a":            "P-1042…",
  "profile_b":            "Q-7781…",
  "shared_total_cm":      1740,
  "shared_segment_count": 44,
  "longest_segment_cm":   118,

  // ── inferred from the numbers above ──
  "relationship_best": "second-degree",
  "relationship_set":  [ { "half-sibling": 0.40 },  // relative
                         { "aunt/uncle":   0.35 },  // weights,
                         { "grandparent":  0.25 } ],// not probs
  "ancestry_estimate": { "panel_group_1": "38% ± 9",
                         "panel_group_2": "27% ± 14" },
  // interval width differs by panel coverage
  "identified_person": null   // needs records, not DNA
}
```

## Why is it collected?

Label pill: STATED PURPOSE

- **Finding relatives**, building family trees, and ancestry or health reports for the person who submitted the sample
- **Unknown remains and cold-case work** use the same comparison

Label pill: ADDITIONAL CONSEQUENCE

- **Coverage is not reach:** every enrolled person makes their untested relatives findable, so reach grows much faster than enrollment
- **It saturates** — past some enrollment fraction, nearly everyone in that population has a findable relative in the database, tested or not

Key-point callout: **Consent does not scale with the inference:** the submitter consented; their relatives did not and cannot, because the information about them sits in someone else's sample. And who is in the database is set by who bought a test, so which families are reachable tracks purchases.

### Visualization (canvas `c3`, 720×320)

Enrollment vs reach curve: steep rise then saturation (schematic). Color is categorical here — the two regions are different populations: green = people who submitted a sample (matching C1), blue = people reachable through a relative who never tested, violet = the saturation ceiling.

- **Title (bold 16px `#1a5276`, centered):** "Enrollment is not reach".
- **Plot area:** L=190, R=620, T=40, B=190; axes stroked `#2c3e50` 1.5px.
- **Curve (hardcoded, unit-square coordinates):** `[0.00,0.00], [0.05,0.20], [0.10,0.37], [0.15,0.50], [0.20,0.61], [0.25,0.69], [0.30,0.76], [0.40,0.85], [0.50,0.91], [0.60,0.945], [0.70,0.965], [0.80,0.978], [0.90,0.986], [1.00,0.99]` — stroked blue `#2a78d6` 2.5px.
- **Regions:** triangle below the 45° diagonal filled green `#008300` alpha 0.18 (reach that is just the enrolled themselves); region between the curve and the diagonal filled blue alpha 0.3 (people reachable through someone else's sample).
- **Reference lines:** dashed green 45° diagonal (dash 5/4) — reach == enrollment; dashed violet `#4a3aa7` horizontal ceiling at y=0.99 (dash 3/3).
- **Annotations:** green 13px "if reach equalled enrollment" near the diagonal; blue bold 13px two-line "this gap is people reachable" / "through a relative’s sample" inside the gap; violet bold 13px right-aligned "saturates — almost everyone has a findable relative" above the ceiling.
- **Axis labels (13px `#2c3e50`):** x — "share of the population enrolled  (low → high)"; y (rotated, two lines) — "share with at least one" / "identifiable relative  (low → high)".
- **Bottom caption (13px `#6b7280`, centered):** "Schematic — shape, not measured values. No axis is scaled to a real figure."

## Regeneration instructions

- **Layout:** tracking detail page — `<table class="obj-table">`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede`, bullets with bold lead terms (`li b` in `#1a5276`), `.key-point` callouts, `.lbl` label pills; right `<td>` (55%, text-align center) holds the canvas, and for the "What does it collect?" row also the `.payload-note` caption plus `<pre class="payload">` block (left-aligned).
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold 1.1em `#1a5276`; td borders `1px solid #2980b9`, padding 16px; li 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em, `strong:first-child` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase radius 3px — `.lbl-purpose` `#eaf2fb`/`#1a5276`, `.lbl-effect` `#fdf0e6`/`#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, white-space pre; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Rounded-rect helper `rr()` available.
- **Palette:** charts use the validated categorical palette `P = { blue: #2a78d6, green: #008300, magenta: #d55181, yellow: #c98500, aqua: #199e70, orange: #d95926, violet: #4a3aa7, ink: #1a5276, text: #2c3e50, mute: #6b7280, grid: #e5e9ef }`; red is deliberately not in the rotation (reserved for genuine alarm states). Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links elsewhere use `.html` extensions.
