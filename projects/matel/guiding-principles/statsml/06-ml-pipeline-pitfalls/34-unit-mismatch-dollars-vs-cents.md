# Pitfall: Unit Mismatch (Dollars vs Cents, Seconds vs Milliseconds)

**Page type:** detail page (card-section layout: one `.card-section` per h2 with a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** Unit Mismatch (Dollars vs Cents, Seconds vs Milliseconds)

**Subtitle:** When the same column name is stored in different units across sources or time, causing silent 100x or 1000x errors.

## The Problem

**Tags:** `the trap` (red), `units` (blue)

- **Same name, two units** — "amount" stores dollars in one table and cents in another
- **No enforcement** — no schema metadata or validation exists to catch the mismatch at merge time
- **Currency** — a payment API returns cents (2500) while the internal DB stores dollars (25.00)
- **Time** — Unix timestamps arrive in seconds from one source and milliseconds from another
- **Distance** — meters vs kilometers and degrees vs radians hide behind identical column names

*Example:* Merging Stripe amounts in cents (4500) with internal dollars (45.00) inflates revenue 100x, so the model sees an average purchase of $2,272.50 instead of $45.

**Impact:** Revenue comes out off by 100x and latency by 1000x, and every downstream aggregate silently inherits the error.

### Visualization (canvas `c1`, 720×300)

Box-and-arrow merge diagram showing two sources with the same column name in different units merging into a garbage dataset, plus a third time-unit example.

- **Title (bold 14px `#1a5276`, top center):** "Unit Mismatch: Same Column Name, Different Units".
- **Source A box:** stroked rect `#1a5276` (2px) at (30,60) 180×100, centered at x=120; bold 12px `#1a5276` label "Source A: Internal DB"; 11px `#444` "Column: amount"; bold 13px `#27ae60` value "45.00"; 10px `#666` "(in dollars)".
- **Source B box:** stroked rect `#1a5276` at (270,60) 180×100, centered at x=360; label "Source B: Stripe API"; "Column: amount"; green value "4500"; gray "(in cents)".
- **Source C box:** stroked rect `#1a5276` at (480,60) 180×100, centered at x=570; label "Source C: Logs"; "Column: duration"; green value "1500"; gray "(milliseconds)" and "vs seconds elsewhere".
- **Merge arrow:** vertical gray `#666` (2px) arrow at page center from y=170 to y=195 with arrowhead; 11px `#444` label "UNION / MERGE" at center.
- **Result box:** stroked rect `#e74c3c` (3px) at (180,205) 360×80; bold 13px `#e74c3c` "Merged Dataset"; 11px `#444` rows "Row 1: amount = 45.00   (dollars)" and "Row 2: amount = 4500     (cents, but treated as dollars!)"; bold 11px `#e74c3c` "Model sees: avg = $2272.50  (garbage!)".

## Why It Happens

**Tags:** `root cause` (orange), `conventions` (blue)

- **Social convention** — each system's unit choice is sensible alone; mismatch appears at seams
- **No central registry** — teams pick unit conventions independently and never document them
- **Migrations** — a platform change from cents to dollars keeps the old column name and docs
- **API vs DB** — external APIs return cents while internal tables store dollars in one column
- **Bare schemas** — column definitions lack unit annotations, valid ranges, or semantic types

**Root Cause:** The name "amount" carries no unit semantics, so every consumer must guess — and guesses diverge across teams and time.

### Visualization (canvas `c2`, 720×300)

Three source boxes flowing into one merged column with no unit conversion.

- **Title (bold 14px `#1a5276`, top center):** "Why It Happens: Multiple Sources, No Unit Coordination".
- **Three source boxes** (stroked `#1a5276`, 2px, each 180×60 at y=50): "Payment API" (x=20, centered x=110) with 11px `#444` "amount = 4500" and 10px `#e67e22` "(cents)"; "Internal DB" (x=270, centered x=360) with "amount = 45.00" and "(dollars)"; "Partner Feed" (x=520, centered x=610) with "amount = 38.50" and "(euros)".
- **Arrows:** gray `#666` (1.5px) vertical arrows from each box (x = 110, 360, 610) down to the merge box at y=155.
- **Merge box:** stroked rect `#e74c3c` (3px) at (150,155) 420×70; bold 12px `#e74c3c` "Merged Column: \"amount\""; a bold 18px red "✖" mark near the box's right edge; 11px `#444` values line "4500  |  45.00  |  38.50"; bold 11px `#e74c3c` "No unit conversion!".
- **Bottom note (11px `#666`, centered, y = h−20):** "All treated as same unit → 100x errors in aggregation, model features, and reporting".

## The Correct Approach

**Tags:** `the fix` (green), `units` (blue)

- **Units in names** — amount_cents, latency_ms, and distance_km are self-documenting columns
- **Range checks** — a rule like amount_cents > 0 AND < 10,000,000 catches 100x errors at once
- **Catalog metadata** — record the unit, valid range, and source system for every column
- **Convert at ingestion** — normalize units at the point of entry, never downstream in queries
- **Outlier checks** — an average revenue jump from $50 to $5,000 exposes a mismatch early

**Fix:** Make the unit impossible to ignore — a column name that states the unit cannot mix dollars with cents by accident.

### Visualization (canvas `c3`, 720×300)

Three validated schema boxes with unit-suffixed column names, an annotation box, and an ingestion-boundary conversion flow.

- **Title (bold 14px `#1a5276`, top center):** "Correct Approach: Units in Column Names + Validation".
- **Schema boxes** (stroked `#27ae60`, 2px, each 180×90 at y=50, with a bold 18px green "✓" in the top-right corner):
  - x=60: bold 12px `#1a5276` "amount_cents"; 11px `#444` "Type: INT"; 10px `#27ae60` "Valid: > 0 AND < 10,000,000"; 10px `#666` "Unit in name ✓".
  - x=280: "latency_ms"; "Type: FLOAT"; "Valid: > 0 AND < 30,000"; "Unit in name ✓".
  - x=500: "distance_km"; "Type: DECIMAL"; "Valid: >= 0 AND < 50,000"; "Unit in name ✓".
- **Annotation box:** fill `rgba(39,174,96,0.08)` with 1px `#27ae60` stroke at (100,165) 520×50; bold 12px `#1a5276` "Units in name = self-documenting + prevents mix-ups"; 11px `#444` "Any merge attempt between amount_cents and amount_dollars triggers immediate type error".
- **Ingestion boundary pattern:** bold 11px `#1a5276` heading "Ingestion Boundary Pattern" (centered, y=240); left box stroked `#1a5276` (1.5px) at (150,250) 140×35 with 10px `#444` "Raw: amount=4500" / "(cents from API)"; green `#27ae60` arrow (2px) from x=295 to x=370 at y=267 labeled "convert" (9px green); right box stroked `#27ae60` (1.5px) at (375,250) 170×35 with bold 10px `#27ae60` "Stored: amount_cents = 4500" and 10px `#444` "validated + unit-tagged".

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) containing one `<tr>`: left `td.text-col` (45%) with `.tags` pills, a `<ul>` of labeled bullets, optional `.example` italic paragraph, and a `.key-point` callout; right `td.viz-col` (55%) with one canvas.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. Bullets 0.92rem with `<b>` labels in `#1a5276`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 each, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#444`/`#666`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
