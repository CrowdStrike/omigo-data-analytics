# Use Current State Queries for Historical Analysis

**Page type:** detail page (card-section layout: h2 section headings, two-column layout table with text left 45% / canvas right 55%)
**HTML title tag:** Use Current State Queries for Historical Analysis

**Subtitle:** Current values overwrite history — restated earnings, changed addresses, updated codes

## The Anti-Pattern

Query the database today for what happened last quarter. Earnings restated, addresses changed, codes updated — current state overwrites history.

**Domain examples:** (bold lead-in)

- **Finance** — restated earnings
- **HR** — changed job titles
- **E-commerce** — updated prices

**Key-point callout (red left border):** You cannot reproduce last quarter's analysis if the underlying data has mutated since then.

### Visualization (canvas `c1`, 720×300)

Drawn current-state table with red lost-history annotations.

- **Table:** at (40, 30), columns 160px and 140px wide, rows 36px tall; header row filled `#1a5276` with white bold 14px headers "product" and "price (today)"; data rows alternate `#f8f9fa`/white with `#e0e0e0` borders, text `#2c3e50` 13px.
- **Rows:** Widget A / $120; Widget B / $45; Gadget C / $200; Gadget D / $75.
- **Annotations (italic 12px red `#e74c3c`, right of the table, with a red strikethrough line, width 1.5, drawn across the price cell):** row 1 "Was $80 in Q1 → lost!"; row 2 "Was $25 → lost!"; row 3 none; row 4 "Was $60 → lost!".
- **Bottom:** a thick red X mark (`#e74c3c`, line width 3) followed by bold 16px red text "History destroyed".

## The Design Pattern

Every record versioned by timestamp (SCD Type 2 / event sourcing). Query "as-of date X" — not "what is it now?"

Requires append-only storage + temporal query capability.

- Each change creates a new row, not an overwrite
- Queries specify a point-in-time reference
- Full audit trail preserved

**Key-point callout (green left border `#27ae60`):** Reproducible analysis: any past report can be regenerated exactly.

### Visualization (canvas `c2`, 720×300)

Drawn versioned table with a highlighted as-of query result.

- **Table:** at (30, 20), columns 130px, 100px, 130px wide, rows 32px tall; header row filled `#1a5276` with white bold 13px headers "product", "price", "valid_from"; data rows alternate `#f0f8f0`/white with `#d0e0d0` borders, text `#2c3e50` 12px.
- **Rows (product / price / valid_from):** Widget A / $80 / Jan 1; Widget A / $100 / Feb 15; Widget A / $120 / Apr 1; Widget B / $25 / Jan 1; Widget B / $35 / Mar 10; Widget B / $45 / May 1.
- **Highlight:** second row (Widget A, $100, Feb 15) outlined in green `#27ae60`, stroke width 2.5.
- **Query text (bold 14px green, below table):** "SELECT price WHERE product='Widget A' AND as_of='Mar 15'" followed by bold 16px green "→ $100 ✓" and a green-stroked circle (12px radius) around the checkmark area.

## Regeneration instructions

- **Layout:** anti-pattern-pairs detail page: h1 with 2px `#2980b9` bottom border, `.subtitle`, then two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"), each an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) above a `table.layout` with one row: `td.text-col` (45%) and `td.viz-col` (55%).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `ul` 0.92rem; `.example` italic `#555` 0.9rem. No nav bar, no back/home links.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; the Design Pattern key-point overrides the border color inline to `#27ae60`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and `ctx.scale` back to logical coordinates via a shared `setup(id)` helper.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
