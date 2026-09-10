# No source_id Logged with Ingested Data

**Page type:** detail page (anti-pattern-pairs two-section layout: one `.card-section` per pattern, each with a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** No source_id Logged with Ingested Data

**Subtitle:** 5 upstream systems → one table. When something looks wrong, you can't trace which source produced it

## The Anti-Pattern

Data flows from multiple upstream systems into a single combined table with no provenance metadata. When anomalies appear, there is no way to investigate which source produced the problematic rows.

**Example (italic `.example`):** Illustrative Example: of the five rows shown, one carries `val: 812.4` on a measure bounded 0–100. Five feeds could have written it, so every one of the five must be audited.

**Key point (red-left-border callout):** Without source identification, debugging data quality issues becomes a guessing game across all upstream systems simultaneously.

**Domain examples:**

- Multi-hospital lab results merged into one analytics table
- Cross-region billing feeds combined without origin tags
- Merged datasets from partner organizations with no lineage

### Visualization (canvas `c1`, 720×300)

Flow diagram: five identical source boxes feeding one combined table where an anomalous row cannot be traced.

- **Source boxes:** five 70×24 solid `#1a5276` boxes stacked on the left (centered at x=60, first at y=30, 55px vertical spacing), each with white 11px centered label: "Src A", "Src B", "Src C", "Src D", "Src E".
- **Arrows:** gray `#7f8c8d` 1.5px lines with filled arrowheads from each source box converging onto the left edge of the table (one per table row).
- **Combined table:** 200×160 box at x=420, vertically centered; fill `#f0f4f8`, 2px `#1a5276` border; header band 24px tall in `#1a5276` with white bold 11px centered text "Combined Table".
- **Rows:** five 22px-tall white rows with 0.5px `#ddd` borders, each showing 10px `#2c3e50` text of the form `row_N  |  val: XX.X`. Values come from a single shared literal array `ROW_VALS = [42.7, 58.3, 812.4, 37.6, 63.2]` — **no `Math.random()`**; the identical array is rendered in `c2` so both tables hold the same rows and only `source_id` distinguishes them. The flagged row index is *computed* as the first value exceeding `VALID_MAX = 100` (row 3, `812.4`), and that row gets a light red `#fdecea` background plus a 2.5px red `#e74c3c` outline.
- **Annotations:** large bold 36px red "?" to the right of the flagged row; below the table, bold 12px `#e74c3c` two-line label with every number interpolated from the data: "val 812.4 > max 100 — which source?" / "→ Unknown! 1 flagged row of 5, 5 possible sources" (drawn at x=130, y=266 and y=284).

## The Design Pattern

Tag every row with `source_system_id` + `ingestion_timestamp` at the point of ingestion. Filter by source to profile each feed independently and isolate anomalies to their origin.

**Example (italic `.example`):** Illustrative Example: the same five rows, now tagged. The `val: 812.4` row carries `source_id = C`, so the audit is one feed (Src C) instead of five.

**Key point (green-left-border callout, `#27ae60`):** Provenance makes every data quality investigation a scoped, tractable problem instead of a haystack search.

**Steps:**

- Add `source_system_id` column to every ingestion pipeline
- Add `ingestion_timestamp` for temporal traceability
- Profile each source independently before combining
- When anomalies appear, filter by source to isolate the culprit
- Set up per-source data quality monitors

### Visualization (canvas `c2`, 720×300)

Same flow diagram but with color-coded sources and per-row source tags making the anomaly traceable.

- **Source boxes:** five 70×24 boxes as in `c1` but each in its own color — Src A `#1a5276`, Src B `#27ae60`, Src C `#e67e22`, Src D `#8e44ad`, Src E `#e74c3c` — white 11px labels.
- **Arrows:** same geometry as `c1` but each arrow (line + arrowhead) drawn in its source's color.
- **Combined table:** 220×160 box at x=420, vertically centered; fill `#f0f4f8`, 2px `#1a5276` border; header band in `#1a5276` with white bold 11px text "Combined Table + source_id".
- **Rows:** five rows as in `c1`, but each begins with an 18×14 colored tag chip carrying a white bold 9px source letter; row order of tags top-to-bottom is A, D, C, B, E (source indices [0,3,2,1,4]). Row text `row_N  |  val: XX.X` in 10px `#2c3e50` uses the **same shared literal array** `ROW_VALS = [42.7, 58.3, 812.4, 37.6, 63.2]` as `c1` — identical rows, so the only new information is the tag. The flagged row is the same computed index (first value > `VALID_MAX = 100`, i.e. row 3) with a light green `#e8f8f0` background and a 2.5px green `#27ae60` outline; its tag is therefore `C`.
- **Annotations:** bold 24px green `#27ae60` "✓" to the right of the flagged row; below the table, bold 12px `#27ae60` two-line label with the value and source letter interpolated from the data: "val 812.4 > max 100 → source_id = C" / "1 suspect, not 5 — investigate Src C only" (drawn at x=130, y=266 and y=284).

## Regeneration instructions

- **Template/layout:** anti-pattern-pairs detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, then two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"). Each section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` (width 100%, border-collapse) with one row: `td.text-col` (45%) holding a paragraph (inline `<code>` for column names), an italic `.example` paragraph, a `.key-point` callout, a bold "Domain examples:"/"Steps:" lead-in (margin-top 12px, weight 600, 0.92rem) and a `<ul>`; `td.viz-col` (55%) holding the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c` (design-pattern callout overrides border-left-color to `#27ae60`), padding 8px 12px, 0.9rem; ul 0.92rem. Canvas elements `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic size 720×300 via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, plus purple `#8e44ad` for the fourth source and gray `#7f8c8d` for untagged arrows; row-highlight tints `#fdecea` (red) and `#e8f8f0` (green).
- **Data determinism:** both canvases share one module-level literal array `ROW_VALS = [42.7, 58.3, 812.4, 37.6, 63.2]` plus `VALID_MAX = 100`; `BAD_ROW` is computed as the index of the first value above `VALID_MAX`. No `Math.random()` anywhere on the page — every printed statistic (flagged value, threshold, row count, source count, culprit letter) is interpolated from these variables at render time, never hardcoded in a label.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
