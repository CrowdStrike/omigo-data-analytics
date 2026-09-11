# Structured Output

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column `table.layout`, text left 50%, canvas right 50%)
**HTML title tag:** Structured Output

**Subtitle:** Ask the model for fixed JSON fields instead of a sentence — free text is for people, structure is for pipelines

## One Invoice, Two Kinds of Answer

**Tags:** `core idea` (blue), `running example` (green)

- **The task** — pull vendor, date, and amount out of an emailed invoice
- **The invoice** — "Invoice #883 from Acme Supplies, dated March 4, 2026. Total due: $1,284.50."
- **Free-text answer** — "This invoice is from Acme Supplies, dated March 4th, for $1,284.50"
- **Fine for a person** — a human reads it instantly; a program has to parse the sentence
- **Structured answer** — the same three facts as fixed JSON with named fields
- **Pipeline-ready** — JSON loads straight into a table; every invoice fills the same columns

*Example:* The prose answer changes shape from invoice to invoice; the JSON answer never does.

**Key point:** Free text is for people, structure is for pipelines — ask for the shape the next program needs.

### Visualization (canvas `c1`, 720×300)

Branching flow: one invoice source box splitting into a prose path (top) and a JSON path (bottom) that ends in a table.

- **Title (bold 15px, `#1a5276`, centered):** "Two Answers From the Same Invoice — Only One Fits a Pipeline".
- **Invoice source:** box (30, 90) 170×76, stroke mute `#6b7280`, fill `#fdfdf6`, 11px monospace lines: "Invoice #883 from" / "Acme Supplies, dated" / "March 4, 2026." / "Total due: $1,284.50".
- **Top path (prose):** mute arrow to box (254, 48) 250×52, stroke mute, fill `#f4f6f8`, italic 11px: "\"This invoice is from Acme Supplies," / " dated March 4th, for $1,284.50\"" → red `#e74c3c` arrow → bold 13px red "parser: ?" with 11px red lines: "every invoice needs" / "custom parsing".
- **Bottom path (JSON):** green `#008300` arrow to box (254, 152) 250×66, stroke green, fill `rgba(0,131,0,0.04)`, 11px monospace green: "{ \"vendor\": \"Acme Supplies\"," / "  \"invoice_date\": \"2026-03-04\"," / "  \"amount\": 1284.50 }" → green arrow → a small 2-column table drawn at (556, 162), 142px wide, 3 rows of 18px, ink `#1a5276` 1px grid: header row bold 11px "date" | "amount", data row 11px monospace "2026-03-04" | "1284.50", third row mute "..." | "...".
- **Annotation (bold 13px orange `#d95926`, bottom center):** "same facts, two shapes — one goes straight into a table, the other needs code per invoice".

## From Words to Fields, By Hand

**Tags:** `worked example` (green)

- **The schema** — `vendor`: text, `invoice_date`: YYYY-MM-DD, `amount`: number
- **vendor** — "Acme Supplies" is copied as-is from the header line
- **invoice_date** — "March 4, 2026" is rewritten to "2026-03-04"
- **amount** — "$1,284.50" becomes the number 1284.50: no $, no comma, no quotes
- **What's dropped** — invoice #883 isn't in the schema, so it is simply ignored

*Example:* The date and amount are rewritten, not copied — normalizing is part of the extraction.

**Key point:** A schema is a contract: field names, types, and formats are fixed before the first invoice is processed.

### Visualization (canvas `c2`, 720×300)

Span-to-field mapping diagram: highlighted source-text spans on the left, arrows to schema field boxes on the right.

- **Title (bold 15px, `#1a5276`, centered):** "Each Text Span Maps to One Schema Field — Two Get Rewritten".
- **Source text (left, 12px monospace, starting at x=40):** mute "Invoice #883 from"; bold blue `#2a78d6` "Acme Supplies" followed by mute ", dated"; bold violet `#4a3aa7` "March 4, 2026" followed by mute ". Total due:"; bold yellow `#c98500` "$1,284.50" followed by mute "within 30 days.".
- **Ignored note (italic 11px mute):** "\"#883\" and \"30 days\": not in the schema → ignored".
- **Field boxes (right, each 260×44 at x=420, white fill, colored stroke, bold 11px monospace field line + 11px note below):**
  - y=48, blue: "\"vendor\": \"Acme Supplies\"" — note (mute): "copied as-is"
  - y=112, violet: "\"invoice_date\": \"2026-03-04\"" — note (bold orange `#d95926`): "rewritten: March 4, 2026 → YYYY-MM-DD"
  - y=176, yellow: "\"amount\": 1284.50" — note (bold orange): "rewritten: $, comma dropped; now a number"
- **Arrows:** blue, violet, and yellow arrows from each highlighted span to its field box.
- **Annotations (centered):** bold 13px orange: "extraction = pick the span, then normalize it to the contract"; 12px mute: "the schema decides what is kept, what is rewritten, and what is ignored".

## Validation: the Seatbelt on Every Output

**Tags:** `where it's used` (blue), `best practice` (green)

- **The check** — every output is validated against the schema before entering the table
- **First pass** — 184 of 200 invoices validate cleanly on the first try
- **The 16 rejects** — 8 amounts as "$..." strings, 5 dates in the wrong format, 3 missing vendor
- **The retry** — sending the error message back fixes 14; the last 2 go to a human
- **Nothing sneaks by** — a bad record is stopped at the gate, not found in the dashboard

*Example:* `"amount": "$1,284.50"` reads fine to a human and crashes the sum downstream — the validator catches it.

**Key point:** Validate, then retry with the error message — the schema check turns a silent data bug into a loud, fixable one.

### Visualization (canvas `c3`, 720×300)

Validation funnel flow diagram.

- **Title (bold 15px, `#1a5276`, centered):** "200 Invoices Through the Validation Gate (illustrative)".
- **Stages:** box (40, 60) 120×46, stroke blue `#2a78d6`, fill `rgba(42,120,214,0.06)`: bold 13px blue "200 outputs" + 11px mute "from the model" → mute arrow → box (220, 60) 110×46, stroke violet `#4a3aa7`, fill `rgba(74,58,167,0.06)`: bold 13px violet "schema" / "check".
- **Pass branch:** green arrow → box (422, 52) 120×40, stroke green `#008300`, fill `rgba(0,131,0,0.05)`: bold 13px green "184 pass".
- **Fail branch:** red arrow angling down → box (422, 132) 190×78, stroke red `#e74c3c`, fill `rgba(231,76,60,0.04)`: bold 12px red "16 rejected", then 11px left-aligned lines: "8  amount is a \"$...\" string" / "5  date in the wrong format" / "3  vendor field missing".
- **Retry:** aqua `#199e70` arrow out of the reject box with stacked bold 11px aqua label "retry" / "with the" / "error"; two outcome boxes below: (422, 226) 120×34 green "14 fixed on retry" and (556, 226) 124×34 yellow `#c98500` (fill `rgba(201,133,0,0.06)`) "2 → human review", with green and yellow arrows into them.
- **Tally (left, at x=60):** bold 13px ink "into the table:", then 12px monospace: "184 + 14 = 198 valid" / "  2 held for a human".
- **Annotation (bold 13px orange `#d95926`, bottom center):** "every bad record failed loudly at the gate — none crashed the dashboard later".

## Valid Is Not the Same as Correct

**Tags:** `common mistake` (red), `watch out` (orange)

- **Shape vs truth** — validation proves the JSON has the right fields, not the right values
- **Confidently wrong** — the model can put the due date into `invoice_date` and still validate
- **Range checks help** — amount > 0 and a recent date catch many wrong values cheaply
- **Spot-check** — sample outputs against the source text, especially dates and totals
- **Two seatbelts** — the schema checks the shape; sanity rules and sampling check the truth

*Example:* "2026-04-03" validates perfectly — right shape, wrong date: day and month swapped.

**Common mistake:** Treating "passed the schema" as "correct". The schema is a shape check; correctness still needs range rules and a human sample.

### Visualization (canvas `c4`, 720×300)

Two side-by-side JSON output cards, split by a vertical dashed divider at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px, `#1a5276`, centered):** "Both Outputs Pass the Schema — Only One Is True".
- **Card layout (each 280×78 box, stroke grid `#e5e9ef`, fill `#fafbfc`, bold 12px mute title above, 11px monospace JSON inside):**
  - **Left, "output A" at x=45:** "{ \"vendor\": \"Acme Supplies\"," / "  \"invoice_date\": \"2026-03-04\"," / "  \"amount\": 1284.50 }" — verdicts below, centered: bold 12px green `#008300` "schema check: ✓ valid" and green "matches the invoice: ✓ March 4".
  - **Right, "output B" at x=395:** same JSON but the date line is bold red `#e74c3c`: "  \"invoice_date\": \"2026-04-03\"," — verdicts: green "schema check: ✓ valid", red "wrong: day and month swapped", plus 11px mute: "\"2026-04-03\" is April 3, not March 4".
- **Annotations (centered):** bold 13px orange `#d95926`: "validation checks the shape; only a range rule or a human sample checks the truth"; 12px mute: "cheap extra guards: amount > 0, date within the last year, vendor non-empty".

## Regeneration instructions

- **Layout:** tutorial page — `<h1>` + `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets each opening with a `<b>` term (`#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300 (CSS `width:100%`, `1px solid #e0e0e0` border, 4px radius).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; bullets 0.92rem; inline `code` in ui-monospace on `#f4f6f8`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared `box()` (fill + 1.5px stroke rect) and `arrow()` (2px line with filled triangular head) helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange. All numbers are hardcoded literal arrays (no `Math.random()`); invented numbers carry an "illustrative" label.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
