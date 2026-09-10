# Tracking Data: DNA & Genetic Data

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas/payload right 55%, one row per section)
**HTML title tag:** Tracking Data: DNA & Genetic Data

**Subtitle:** Consumer genetic testing turns a saliva sample into a data file. The file is about one customer, but the information in it is about a family — and unlike a password, a genotype cannot be rotated.

## Section 1: What is it?

Lede: Saliva-sample genetic testing sold direct to consumers.

- **Mechanism:** a mailed-in tube is read on a genotyping array at a fixed panel of marker sites
- **Returned:** ancestry estimates and health-risk reports
- **Retained:** the data file, and usually the physical sample, under the consent given at signup
- **Not reversible** — unlike a password, a genotype cannot be rotated

Key point callout: **Unit of observation:** the unit that pays is one customer; the unit the data describes is a pedigree. A genotype is shared in predictable proportions with parents, siblings, children and cousins, so one person's decision to test enters partial information about relatives who were never asked — the record is not solely the subject's to give.

### Visualization (canvas `c1`, 720×320)

Conceptual illustration: a DNA double helix transitioning into binary digits (biological sample becoming a digital copy).

- **Helix (left, x=40 to 360):** two sinusoidal strands of 1.5px-radius dots centered at y=160, amplitude 40, frequency 0.04, phase-offset by π — strand 1 in `#2a78d6` (blue), strand 2 in `#008300` (green). Vertical connecting rungs every 24px in `rgba(42,120,214,0.35)`, width 2.
- **Transition zone (from x=360):** 8 fragmenting base letters cycling A, T, G, C in 16px monospace `#2a78d6`, positions stepping 18px right with pseudo-scattered y offsets, opacity fading from 1.0 down by 0.1 per letter. A solid blue arrow (width 2.5) from (370,160) to (420,160) with arrowhead.
- **Binary grid (right, starting x=440):** 8 rows × 14 columns of '1'/'0' characters in bold 15px monospace, fixed deterministic pattern (`((row*13 + col*7) % 5) < 2` gives '1'), rows at y=50+22 per row, columns 18px apart; '1' in `#2a78d6`, '0' in `rgba(42,120,214,0.35)`.
- **Labels (bold 14px, centered):** "Biological" at (180,225) and "Digital Copy" at (560,225), both in `#2a78d6`.

## Section 2: What does it collect?

- **Genotype calls** at a fixed panel of marker sites — typically a few hundred thousand positions out of a genome of about three billion, not a whole-genome sequence
- **Risk estimates** for the variants the panel covers
- **Ancestry proportions**, estimated against a reference panel
- **Relative matches** — other customers sharing segments
- **Trait predictions** — eye colour, hair type and similar

Key point callout: **The array ages:** it reads the sites it was built to read and is silent about the rest, so a result is a joint function of the sample and the `array_version` that measured it. Re-run the same spit years later on a newer array and the answer legitimately changes.

Key point callout: **Two separate consents:** agreeing to one analysis and agreeing to keep the physical material for the next one are different decisions — which is why `sample_storage` is its own field.

Key point callout: **A no-call is not a negative:** `--` means the probe failed, not that the variant is absent — easy to lose once the file becomes a table.

### Visualization (canvas `c2`, 720×320)

Branching tree diagram: central "Panel" node fanning out to four category nodes, each with a left-aligned column of caveat bullets.

- **Central node:** ellipse labeled "Panel" at (120,160), fill `#2a78d6`, white 2px stroke, white bold 13px label; ellipse x-radius widens to fit text (min radius 30).
- **Branch nodes** (ellipses at x=250, min radius 26, same style, each connected to the central node by a 2px line in its own color, and to its caveat column by a 50%-alpha tinted connector ending at x=320):
  - "Risk" at y=45, `#d95926` (orange) — caveats: "Covered variants only", "Shifts a probability", "Silent elsewhere"
  - "Ancestry" at y=111, `#4a3aa7` (violet) — caveats: "Estimated vs a reference panel", "Uneven coverage by population"
  - "Relatives" at y=177, `#2a78d6` (blue) — caveats: "Shared segments", "Only those enrolled", "Degree is estimated"
  - "Traits" at y=243, `#199e70` (aqua) — caveats: "Predicted, not observed"
- **Caveat column:** starts at x=330; each caveat is a 2.5px-radius dot in the branch color plus 12px sans-serif text in `#2c3e50`, one per line at 15px spacing, vertically centered on the branch node.

Below the canvas, payload note (italic gray): "Sample payload — illustrative structure, not real captured data."

Payload block (monospace, left-aligned, `#f8f9fa` background, 3px left border `#1a5276`):

```
// Consumer testing genotypes a fixed set of probe sites
// on an array — it is not whole-genome sequencing. The
// export columns are documented; the rest is inferred.
{
  // ── documented in raw-data export ──
  "sample_id":     "S-4471…",
  "genome_build":  "GRCh37",
  "genotypes": [
    { "rsid": "rs4988235", "chrom": "2",  "pos": "…", "genotype": "AG" },
    { "rsid": "rs1815739", "chrom": "11", "pos": "…", "genotype": "CT" },
    { "rsid": "rs…",       "chrom": "…",  "pos": "…", "genotype": "--" }
  ],

  // ── inferred / plausible ──
  "array_version": "v5",
  "call_rate":     0.981,       // probes that returned a read
  "no_call":       "--",        // probe failed; not "no variant"
  "consent": { "research": true, "partner_sharing": true,
               "sample_storage": true }
}
```

## Section 3: Why is it collected?

Label pill (Stated purpose):

- **Deliver the reports** the customer paid for — ancestry and health
- **Research cohorts**, under a separate consent — association studies need many genotyped people alongside self-reported traits

Label pill (Additional consequence):

- The relative-matching database supports **identification by relatedness** — someone who never tested can be narrowed down through a cousin who did
- Research consent and relative-matching are **separate switches over the same stored genotypes**

Key point callout: **Volunteer sample, not a population sample:** participants could afford a kit, were curious, and opted in; the self-reported traits come from that same group. An association estimated there is estimated on its selection, so effect sizes do not transfer to a general population without adjustment. The marker sites available are also the ones a product needed, not the ones a study would have chosen.

### Visualization (canvas `c3`, 720×320)

Horizontal funnel bar chart: who is left in the cohort an association is estimated over, with faint gray remainders showing what each step removed.

- **Title (bold 13px `#1a5276`, centered at y=26):** "Who is left in the group an association is estimated over"; subtitle (12px `#6b7280`, y=44): "starting from 10,000 adults".
- **Data (label / count):** "adults" 10000; "kit is affordable to them" 5200; "interested enough to buy" 900; "opted in to research" 690; "filled in the trait survey" 310.
- **Layout:** bars start at x=268, max width 388 (scaled by n/10000), rows at top y=68 stepping 36px, bar height 24. Right-aligned labels at x=256; bold count labels to the right of each bar (blue `#2a78d6`, last row orange `#d95926`).
- **Colors:** surviving bars fill `rgba(42,120,214,0.30)` stroke `#2a78d6`; last (final cohort) bar fill `rgba(217,89,38,0.45)` stroke `#d95926`; removed portion (from previous bar width to current) fill `rgba(107,114,128,0.14)`.
- **Annotations:** below the last row (left-aligned, 12px `#6b7280`): "grey = removed at that step, by a choice the study did not make".
- **Captions (bottom center):** italic 12px `#2c3e50`: "Every step is voluntary, so the last bar is a self-selected group, not a smaller population."; italic 11px `#6b7280`: "Illustrative counts — the shape of the narrowing, not a measured cohort."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, text-align center) holds the canvas, and in the "What does it collect?" row also the `.payload-note` and `.payload` `<pre>` (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em italic `#666` directly above.
- **Canvas:** 720×320 intrinsic attributes; a shared `setupCanvas(id)` reads the element's own width/height attributes and scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Charts use hardcoded literal data arrays (no Math.random), with a `tint(hex, alpha)` helper for translucent fills and an `rr()` rounded-rect helper.
- **Palette (tracking-set tokens, declared once as `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for alarm states; navy `#1a5276` is ink only (headings, axes, callout borders). Project-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links elsewhere use `.html` extensions.
