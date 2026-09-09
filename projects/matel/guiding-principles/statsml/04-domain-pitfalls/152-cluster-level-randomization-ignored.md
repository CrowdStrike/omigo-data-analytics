# Cluster-Level Randomization Ignored (Health / Epidemiology)

**Page type:** detail page (h2 section headers, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Cluster-Level Randomization Ignored (Health / Epidemiology)

**Subtitle:** When the outcome is transmissible or shared within a natural group, randomizing at the individual level violates independence — the experiment is measuring spillover, not treatment effect.

## The Core Problem

**The Core Problem**

- **Clusters:** Communicable diseases spread within households, wards, schools, and villages.
- **Person-level split:** Randomizing a hygiene intervention inside one household changes exposure for all.
- **No independence:** The treated person's outcome is not independent of untreated family members'.
- **Shared exposure:** Household members share the same air, surfaces, water, and food preparation.
- **Not contamination:** This is not control accidentally getting the treatment — the failure runs deeper.
- **Wrong level:** The outcome itself is a cluster-level phenomenon measured at the individual level.

Callout (philosophy box, inside the left cell): **Key insight:** The unit of randomization must match the level at which the outcome is generated. If infection risk is determined by household behavior, the household is the experimental unit — not the person.

### Visualization (canvas `c1`, 720×300)

Household diagram: one dashed house outline containing 5 family-member circles with mixed treatment assignments and transmission arrows. White (unpainted) background.

- **Title (bold 14px `#1a5276`, top center):** "Household: Individual Randomization Fails".
- **House outline:** dashed gray `#999` rectangle (dash 6/4, width 2) at (60, 40) size 600×200; caption below in 12px `#888`, centered: "One Household — Shared Environment".
- **Members:** circles radius 25, name in 11px `#333` centered inside, assignment tag in bold 10px below (treated: fill `rgba(39,174,96,0.2)`, stroke `#27ae60`, tag "TREAT"; control: fill `rgba(231,76,60,0.2)`, stroke `#e74c3c`, tag "CTRL"; stroke width 2.5):
  - Mom (150, 120) — TREAT
  - Dad (280, 100) — CTRL
  - Child 1 (410, 120) — TREAT
  - Child 2 (540, 100) — CTRL
  - Grandma (350, 180) — CTRL
- **Transmission arrows:** dashed orange `#e67e22` lines (dash 3/2, width 1.5) connecting member pairs Mom–Dad, Dad–Child 1, Child 1–Child 2, Dad–Grandma, Child 2–Grandma.
- **Legend line (11px `#e67e22`, bottom center):** "↔ pathogen transmission between all members".

## Examples Where This Fails

**Examples Where This Fails**

- **Hand-washing trial:** Randomize individuals within households to receive soap plus training.
- **Diluted and biased:** One washer cuts diarrheal pathogen load on shared surfaces for everyone.
- **Masking in hospital wards:** Randomize nurses to mask or no-mask within the same ward.
- **Ward-level airborne spread:** One masked nurse lowers respiratory load for control nurses too.
- **Vaccination in schools:** Randomize students to vaccine or none within a single classroom.
- **Herd immunity:** Unvaccinated students are protected by their vaccinated classmates nearby.
- **Underestimated effect:** Vaccine effect shrinks because the control group benefits from treatment.
- **Water purification:** Randomize households within a village that all share a single well.
- **Recirculation:** Treated households cut pathogen return into the shared source, treating everyone.

**Correct approach:** Cluster-randomized trial (CRT). Randomize at household / ward / village / school level. All members of a cluster get the same assignment. Analyze with mixed-effects models or GEE accounting for intra-cluster correlation.

### Visualization (canvas `c2`, 720×300)

Two-household diagram showing cluster-level randomization. White (unpainted) background.

- **Title (bold 14px `#1a5276`, top center):** "Correct: Randomize Entire Cluster".
- **Treatment household:** green `#27ae60` rectangle outline (width 3) at (40, 45) size 280×180; label below (bold 13px green, centered at x=180): "Household A → Treatment". Inside: 5 circles radius 20 in a 3+2 grid (x = 90 + (i%3)×80, y = 100 + floor(i/3)×70), fill `rgba(39,174,96,0.2)`, stroke `#27ae60` width 2.
- **Control household:** blue `#1a5276` rectangle outline (width 3) at (390, 45) size 280×180; label (bold 13px blue at x=530): "Household B → Control". Inside: 5 circles radius 20 in the same grid pattern (x = 440 + (i%3)×80), fill `rgba(26,82,118,0.15)`, stroke `#1a5276` width 2.
- **Caption (13px `#333`, bottom center):** "No within-cluster contamination — transmission stays within one arm".

## Statistical Consequence

**Statistical Consequence**

- **Design effect:** DE = 1 + (m − 1) × ICC, with m = cluster size, ICC = intra-cluster correlation.
- **Worked value:** A 5-person household with ICC = 0.3 gives DE = 1 + 4 × 0.3 = 2.2.
- **Cluster count:** You therefore need 2.2× as many clusters as a naive calculation suggests.
- **Effective N:** N_eff = N / DE, so real precision is far below the raw headcount you enrolled.
- **Worked trial:** 500 individuals in 100 households (m=5, ICC=0.3) give effective N ≈ 227, not 500.
- **If you ignore this:** Confidence intervals come out too narrow and p-values too small.
- **Type I error inflation:** You declare effects significant when they are not — the classic failure.

**The tell:** Ask "does one person's outcome in this cluster depend on whether other cluster members were treated?" If yes — individual randomization is invalid.

### Visualization (canvas `c3`, 720×300)

Two horizontal bars comparing naive N vs effective N, with formula text. White (unpainted) background.

- **Title (bold 14px `#1a5276`, top center):** "Design Effect: Naive N vs Effective N".
- **Naive bar:** at (120, 60) size 550×40, fill `rgba(231,76,60,0.3)`, stroke red `#e74c3c` width 2. In-bar left label (bold 13px red): "Naive N = 500 individuals"; in-bar right label: ""p = 0.003"". Row label to the left (12px `#666`): "Naive N".
- **Effective bar:** at (120, 130), width 550 × (227/500) ≈ 250, height 40, fill `rgba(39,174,96,0.3)`, stroke green `#27ae60` width 2. In-bar left label (bold 13px green): "Effective N ≈ 227 (after design effect)"; right-aligned label at bar end: "p = 0.08". Row label (12px `#666`): "Effective N".
- **Formula (14px `#1a5276`, centered):** "DE = 1 + (m − 1) × ICC = 1 + (5 − 1) × 0.3 = 2.2" at y=200, then "N_eff = 500 / 2.2 ≈ 227" at y=225.
- **Warning (bold 13px red `#e74c3c`, centered, y=265):** "Ignoring clustering: "significant" result is actually non-significant".

## Regeneration instructions

- **Layout:** detail page. h1 + `.subtitle`, then one `h2` per section, each followed by a `.obj-table` (full-width table, single `<tr>`): left `<td>` (40%) holds `.obj-title` div + `<ul>` of bullets (plus a `.philosophy` callout inside the first section's cell and a trailing `<p>` in sections 2 and 3), right `<td>` (60%, centered) holds the canvas. The `.obj-title` text duplicates the h2 text on this page.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border, padding-bottom 8px; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Canvases on this page have no painted background (white). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, blue fill `rgba(26,82,118,0.15)`, gray text `#666`/`#333`/`#888`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
