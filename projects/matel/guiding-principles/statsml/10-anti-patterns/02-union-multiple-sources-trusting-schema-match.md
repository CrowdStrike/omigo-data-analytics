# UNION Multiple Sources Trusting Schema Match

**Page type:** detail page (card-section layout: h2 section headings, two-column layout table with text left 45% / canvas right 55%)
**HTML title tag:** UNION Multiple Sources Trusting Schema Match

**Subtitle:** Same column names + same types ≠ same semantics — 'amount' in USD vs cents vs local currency

## The Anti-Pattern

Same column names + same types assumed = same data. No validation of semantic meaning.

**Key-point callout (red left border):** Schema compatibility does not imply semantic compatibility. Two sources can have identical DDL yet encode fundamentally different things in identically-named columns.

**Domain examples:** (bold lead-in)

- Multi-hospital labs — "glucose" measured in mg/dL vs mmol/L
- Cross-region billing — "amount" in USD vs cents vs local currency
- Multi-platform analytics — "duration" in seconds vs milliseconds

### Visualization (canvas `c1`, 720×300)

Chaotic bar chart mixing values from three different scales on one axis.

- **Title (bold 14px red `#e74c3c`, top center):** "All labeled 'amount'".
- **Bars (interleaved, 40px wide, 12px gaps, starting x=60, scale max 5500):** value/source pairs in order: 45 USD, 5200 cents, 3800 INR, 52 USD, 4800 cents, 48 USD, 4200 INR, 5500 cents, 55 USD, 4500 INR, 4900 cents, 3900 INR.
- **Bar style:** fill `rgba(26,82,118,0.35)`, red `#e74c3c` border (width 1.5); red 10px value label above each bar; gray `#555` 9px source label ("USD"/"cents"/"INR") below each bar.
- **Axis:** thin gray `#ccc` baseline at bottom of the bars.
- **Annotation (bold 12px red, bottom right):** "← 100× scale difference on same axis →".

## The Design Pattern

Add `source_system_id` to every row. Profile distributions PER SOURCE before combining.

Compare mean, std, null rate per source — if different for "same" column → semantic mismatch.

**Key-point callout (red left border):** If two sources share a column name but their distributions differ by orders of magnitude, it is a semantic conflict — not a data quality issue.

- Tag every row with its origin system
- Compute per-source summary stats before UNION
- Flag scale mismatches (mean ratio > 10×)
- Require explicit unit reconciliation before combining

### Visualization (canvas `c2`, 720×300)

Side-by-side per-source histograms with a scale-mismatch annotation between them.

- **Left histogram — Source A:** bold 12px `#1a5276` title "Source A: μ=$50" centered over the left half; 7 bins with counts `[8, 22, 45, 38, 25, 12, 5]` (max 45), bars 30px wide with 4px gaps starting at x=40, fill `rgba(26,82,118,0.35)`, stroke `#1a5276`; gray `#666` 10px x-axis labels "$20" (first bin), "$50" (middle), "$80" (last); thin `#ccc` baseline.
- **Right histogram — Source B:** bold 12px `#1a5276` title "Source B: μ=5000¢" centered over the right half; 7 bins with counts `[5, 18, 40, 42, 30, 15, 7]` (max 42), same bar style, starting at x = midpoint + 40; x-axis labels "2000¢", "5000¢", "8000¢"; thin `#ccc` baseline.
- **Center annotation (bold 13px orange `#e67e22`, two lines at horizontal center):** "100× scale" / "mismatch!" — flanked by orange horizontal arrows (width 2 with filled triangular arrowheads) pointing left toward Source A and right toward Source B.

## Regeneration instructions

- **Layout:** anti-pattern-pairs detail page: h1 with 2px `#2980b9` bottom border, `.subtitle`, then two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"), each an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) above a `table.layout` with one row: `td.text-col` (45%) and `td.viz-col` (55%).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `ul` 0.92rem; `.example` italic `#555` 0.9rem. No nav bar, no back/home links.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and `ctx.scale` back to logical coordinates via a shared `setup(id)` helper.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
