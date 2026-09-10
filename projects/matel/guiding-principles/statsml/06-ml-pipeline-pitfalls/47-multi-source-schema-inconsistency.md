# Pitfall: Multi-Source Schema Inconsistency

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Multi-Source Schema Inconsistency

**Subtitle:** Multiple sources fit same schema but encode values differently, creating semantic confusion.

## The Problem

Tags: `the trap` (red), `schema` (blue)

- **Same schema, different meaning** — sources share column names but encode values differently
- **Semantic drift** — "status=active" means logged-in in source A, not-suspended in source B
- **Encoding chaos** — dates arrive as ISO strings, Unix timestamps, and plain text in one column
- **Unit mismatch** — source A logs revenue in cents (4500) while source B logs dollars (45.00)
- **Ambiguous NULLs** — NULL means not-applicable, unknown, or still-processing depending on source
- **Blind union** — the pipeline merges rows without normalizing, so the model learns source noise

*Example:* An engagement score arrives as 0-100 from web but as 0-5 stars times 20 from mobile, so mobile retention predictions come out 35% off.

**Impact:** Predictions become biased by which source a row came from, and the corruption is silent because data types still match.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three sources with different revenue encodings merging into one incoherent unified column.

- **Title (bold 14px `#1a5276`, top center):** "Multi-Source Schema Inconsistency".
- **Source boxes (140×35, 2px stroke, stacked at x=40):** "Source A" / "revenue: cents (4500)" in `#27ae60` at y=55; "Source B" / "revenue: dollars (45.00)" in `#2980b9` at y=110; "Source C" / "revenue: EUR (38.50)" in `#e67e22` at y=165. Gray `#666` 1.5px lines from each box converging to the unified table.
- **Unified table box (280, 70, 160×120) with red `#e74c3c` 3px border:** headers "Unified Table" / "revenue column" in bold 13px `#1a5276`; mixed values in 11px red monospace: `4500`, `45.00`, `38.50`, `5200`; annotation in 10px `#666` centered: "same field," / "different units" / "& semantics".
- **Chaos indicator:** red 2px circle (radius 50) centered at (500, 130) containing bold 16px red "CHAOS" and 10px "Model sees" / "incoherent data".
- **Bottom annotation (bold 12px red, centered):** "Same schema, incompatible encodings → Model learns noise, not signal".

## Why It Happens

Tags: `root cause` (orange), `validation` (blue)

- **Syntax-only checks** — schema validation confirms names and types, never units or meaning
- **False confidence** — structural compatibility gets mistaken for semantic compatibility
- **Unit mismatch** — "amount" is cents in system A and dollars in system B; both pass as numeric
- **Timezone mismatch** — UTC in one source, local time in another; both are valid TIMESTAMP values
- **Code mismatch** — status=1 means "active" for vendor A but "pending" for vendor B

*Example:* Merging web clickstream in UTC with mobile clickstream in device-local time passes the schema check but silently computes wrong session durations.

**Root Cause:** Schema validation checks syntax only, so two sources can be schema-compatible yet semantically incompatible.

### Visualization (canvas `c2`, 720×300)

Flow diagram: two sources pass a schema check, then merge into a table with 100x unit errors.

- **Title (bold 14px `#1a5276`, top center):** "Schema Passes, Semantics Diverge".
- **Source A box (40, 50, 150×80, `#27ae60` 2px border):** header "Source A" in bold 12px green; 11px `#2c3e50`: `column: "amount"`, `type: INTEGER`; value `1500` in bold 13px green monospace with "(cents)" in 10px `#666`.
- **Source B box (40, 160, 150×80, `#2980b9` 2px border):** header "Source B" in bold 12px blue; `column: "amount"`, `type: FLOAT`; value `15.00` in bold 13px blue monospace with "(dollars)" in 10px `#666`.
- **Schema check box (240, 100, 130×50, `#27ae60` 2px border):** "Schema Check" in bold 11px green and "✓ PASS" in 18px green. Gray 1.5px arrows from both source boxes into it, and a gray arrow from it to the merged table.
- **Merged table box (430, 70, 160×110, red `#e74c3c` 2.5px border):** header "Merged Table" in bold 12px `#1a5276`; 10px monospace rows: `amount`, `------`, then alternating `1500    (cents!)` (green), `15.00   (dollars!)` (blue), `2300    (cents!)` (green), `42.50   (dollars!)` (blue).
- **Warning:** solid red triangle at (620, ~105) with white bold "!" inside; below it 10px red: "100x" / "error".
- **Bottom annotation (bold 11px red, centered):** "Schema validates syntax (name, type) — not semantics (units, meaning)".

## The Correct Approach

Tags: `the fix` (green), `data contracts` (blue)

- **Separate until normalized** — treat each source as its own entity until explicitly converted
- **Tag provenance** — add a source_id column to every row so distributions stay traceable
- **Profile per source** — compare each source's distributions before merge to expose unit gaps
- **Semantic contracts** — specify units, timezone, encoding, and null meaning, not just types
- **Split when divergent** — genuinely different meanings get separate features or models

*Example:* A contract states web sends cents in UTC and mobile sends dollars in local time, so ETL normalizes both to dollars/UTC before the merge.

**Fix:** Validate a per-source data contract covering units, timezone, value ranges, and null meaning before any union.

### Visualization (canvas `c3`, 720×300)

Pipeline diagram: sources flow through semantic validation and normalization into a clean merged output.

- **Title (bold 14px `#1a5276`, top center):** "Semantic Validation + Normalization".
- **Source A box (20, 50, 110×55, `#27ae60` 2px border):** "Source A" bold 11px green; 10px `#2c3e50`: "amount: cents", "tz: UTC".
- **Source B box (20, 125, 110×55, `#2980b9` 2px border):** "Source B" bold 11px blue; "amount: dollars", "tz: local". Gray 1.5px arrows from both into the validation layer.
- **Semantic Validation box (170, 60, 130×100, fill `#f0f4f8`, `#1a5276` 2px border):** header "Semantic" / "Validation" in bold 11px `#1a5276`; 10px `#2c3e50`: "Check contracts:", "units, tz, ranges", "encoding, nulls". Gray arrow to normalization.
- **Normalization box (340, 70, 130×80, fill `#fef9e7`, `#e67e22` 2px border):** header "Normalization" in bold 11px orange; 10px `#2c3e50`: "cents → dollars", "local → UTC", "+ add source_id". Gray arrow to output.
- **Clean Merged Output box (510, 55, 180×120, fill `#eafaf1`, `#27ae60` 2.5px border):** header "Clean Merged Output" in bold 11px green; 9px monospace table: header `amount  tz   source_id` with `------  ---  ---------` rule (in `#1a5276`), rows in `#2c3e50`: `15.00   UTC  web`, `15.00   UTC  mobile`, `23.00   UTC  web`, `42.50   UTC  mobile`. Bold 24px green "✓" at (670, 170).
- **Contract note (10px `#1a5276`, centered at y=205):** "Data Contract: units=dollars, tz=UTC, source_id required".
- **Bottom annotation (bold 11px green, centered):** "Semantic contracts + normalization + source_id = consistent, auditable data".

## Regeneration instructions

- **Layout:** three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"), each with an h2 underlined by `2px solid #2980b9` and a `table.layout` (one `<tr>`): left `<td class="text-col">` (45%) holds `.tags` pills, a `<ul>` of labeled bullets, a `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (55%) holds one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; `li b` in `#1a5276`; `ul` 0.92rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Callouts:** `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, `1px solid #e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; accent `#2980b9`; text `#2c3e50`/`#666`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
