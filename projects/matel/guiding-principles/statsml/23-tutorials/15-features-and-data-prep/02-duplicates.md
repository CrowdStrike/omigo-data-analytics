# Duplicates

**Page type:** detail page (tutorial layout: h2 card-sections, each a two-column table with text left 50% / canvas right 50%)
**HTML title tag:** Duplicates

**Subtitle:** The same customer under two slightly different names counts twice in everything you compute

## One Customer, Two Rows

**Tags:** `core idea` (blue), `running example` (green)

- **The merge** — two CRM systems were combined into one customer table
- **System A** — saved him as "Jon Smith"; System B saved "John Smith " with a trailing space
- **Same person** — same address, same card, same orders; only the name strings differ
- **The computer's view** — "Jon Smith" ≠ "John Smith ", so it happily keeps both rows
- **Invisible flaw** — a trailing space cannot be seen by scrolling the spreadsheet

*Example:* Print the name in quotes and it appears: 'John Smith ' — that space is why the dedup missed him.

**Key point:** To a human these are obviously one person. To code they are two different strings — and therefore two different customers.

### Visualization (canvas `c1`, 720×300)

Drawn data table with side annotations: the merged customer table with the two John rows highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "The Merged Customer Table — Spot the Extra Customer".
- **Table:** starts at x=60, y=50; row height 30px; name column 190px wide, spend column 110px wide. Muted gray (`#6b7280`) bold 12px column headers: "name (printed in quotes)" and "total spend".
- **Rows (name / spend / duplicate-flag):** `'Ana Lee'` $100 (no), `'Ben Cho'` $200 (no), `'Cara Diaz'` $300 (no), `'Jon Smith'` $800 (yes), `'John Smith '` $800 (yes). Duplicate rows get a magenta tint background `rgba(213,81,129,0.12)` and bold 13px monospace magenta (`#d55181`) names; other names plain 13px monospace `#2c3e50`. Cell borders `#e5e9ef`, 1px.
- **Trailing-space highlight:** orange (`#d95926`) 2px rectangle (14×22) around the trailing-space position in the last row's name, with bold 12px orange label to the right: "← a trailing space hides here".
- **Right-side notes (x=420):** violet (`#4a3aa7`) bold 13px, two lines: "same address, same card," / "same orders — one person". Below in muted 12px: "\"Jon\" vs \"John\": one letter apart" / "plus one invisible space".
- **Bottom annotations (centered):** magenta bold 13px "the table claims 5 customers — only 4 exist" (y=240); muted 12px "neither row is \"wrong\" on its own; the pair is the bug" (y=262).

## Every Duplicate Votes Twice

**Tags:** `worked example` (green)

- **Four real customers** — Ana $100, Ben $200, Cara $300, John $800 in total spend
- **Five rows** — John's $800 sits in the table under both spellings
- **Wrong mean** — (100 + 200 + 300 + 800 + 800) / 5 = 2,200 / 5 = $440
- **Right mean** — (100 + 200 + 300 + 800) / 4 = 1,400 / 4 = $350
- **Wrong count** — the dashboard says 5 customers; the business has 4

*Example:* One duplicate row inflated average spend by 26% — and John, the biggest spender, got double weight.

**Key point:** A duplicate is a vote cast twice. Averages, counts, and totals all shift toward whoever got duplicated — and it is usually not a random someone.

### Visualization (canvas `c2`, 720×300)

Two side-by-side bar panels separated by a vertical dashed divider (`#bdc3c7`, dash 4/3, at x=360).

- **Title (bold 15px, `#1a5276`, top center):** "One Extra Row Moves Both the Count and the Average".
- **Left panel (x=60, width 260), title "customers on the dashboard":** two bars 92px wide on a gray baseline at y=236 (chart height 150, scale max 6) — "with dupe" = 5 (magenta `#d55181`), "deduped" = 4 (aqua `#199e70`). Bold value labels above bars, 12px category labels below.
- **Right panel (x=400, width 260), title "average spend per customer":** same layout, scale max 500 with $ prefix — "with dupe" = $440 (magenta), "deduped" = $350 (aqua).
- **Annotations (y=82):** orange (`#d95926`) bold 13px centered at x=530: "2,200 / 5 = $440  vs  1,400 / 4 = $350 — the dupe added 26%"; muted 12px at x=190: "John counted twice".

## Why a Data Scientist Cares

**Tags:** `where it's used` (blue), `leakage` (orange)

- **Dashboards drift up** — customer counts and averages inflate a little with every merge
- **Models double-learn** — a duplicated customer is twice as important during training
- **Train/test leakage** — John lands in train as "Jon Smith" and in test as "John Smith "
- **The score lies** — the model is graded partly on a customer it already memorized
- **Dedup first** — remove duplicates BEFORE splitting into train and test, not after

*Example:* A model that memorized John gets his test row "right" — the accuracy number goes up, the honesty goes down.

**Key point:** A duplicate that straddles the train/test split turns evaluation into an open-book exam. Deduplicate before you split.

### Visualization (canvas `c3`, 720×300)

Two-box train/test diagram with a dashed arc linking the duplicated person across the split.

- **Title (bold 15px, `#1a5276`, top center):** "The Random Split Put the Same Person on Both Sides".
- **Boxes:** 220×170 outlined boxes at y=50 — left at x=70, blue (`#2a78d6`) 2px border, bold centered header "TRAIN (model learns)"; right at x=430, green (`#008300`) border, header "TEST (model graded)".
- **Train contents (13px monospace):** `'Ana Lee'`, `'Cara Diaz'` in `#2c3e50`; `'Jon Smith'` in bold magenta `#d55181`.
- **Test contents:** `'Ben Cho'` in `#2c3e50`; `'John Smith '` in bold magenta.
- **Arc:** magenta dashed (dash 7/5, width 2.5) quadratic curve connecting the two John rows beneath the boxes, labeled bold 12px magenta "same person" centered at x=360.
- **Bottom annotations (centered):** orange bold 13px "the model memorizes John in training, then gets tested on him" (y=252); muted 12px "his test prediction looks brilliant — the reported accuracy is inflated" (y=274).

## The Common Confusion: Exact vs Fuzzy Duplicates

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **"drop_duplicates() ran, we're clean"** — it only removes rows that match EXACTLY
- **Exact dupe** — identical after cleanup: trimming spaces catches "John Smith "
- **Fuzzy dupe** — still different after cleanup: "Jon Smith" is one letter off "John Smith"
- **Cheap fixes first** — trim spaces and normalize case before anything fancy
- **Best evidence** — match on something stable: email, phone, card — not the typed name

*Example:* Trimming catches 'John Smith ' instantly; linking 'Jon' to 'John' needed the shared email to be sure.

**Common mistake:** Believing exact-match dedup caught everything. Normalize first, fuzzy-match the survivors, then confirm with a stable key like email.

### Visualization (canvas `c4`, 720×300)

Matrix table ("matching ladder"): four lookalike pairs (rows) against four cleanup steps (columns), showing which step catches each pair.

- **Title (bold 15px, `#1a5276`, top center):** "Four Lookalike Pairs — Which Step Catches Each One".
- **Rows (pair text in 12px monospace, and index of the step that catches it):** `'John Smith '  vs  'John Smith'` → step 0; `'JOHN SMITH'  vs  'John Smith'` → step 1; `'Jon Smith'   vs  'John Smith'` → step 2; `'J. Smith'    vs  'John Smith'` → step 3.
- **Column headers (bold 12px, colored):** "trim" aqua `#199e70`, "lowercase" blue `#2a78d6`, "fuzzy" orange `#d95926`, "email key" violet `#4a3aa7`.
- **Layout:** table at x=40, y=46; row height 36; pair column 330px wide; each step cell 82px wide; grid borders `#e5e9ef`.
- **Cells:** a bold colored "✓" (15px, step color) in the catching step's cell; a light gray "—" (`#c8ced6`) in every earlier step's cell; later cells empty.
- **Bottom annotations (centered):** aqua bold 13px "cheap steps first: trim and lowercase are one line of code each" (y=228); orange bold 13px "fuzzy matching guesses; a shared email or phone confirms" (y=250); muted 12px "raw exact-match catches none of these; only after trimming does row 1 become exact" (y=276).

## Regeneration instructions

- **Template:** tutorial topic page (tutorials/CLAUDE.md conventions, social-graph reference skeleton). `<h1>` concept name (no index number), `.subtitle` line, then four `.card-section` blocks, each `<h2>` + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%) holding one canvas.
- **Left column structure per section:** `.tags` row of colored pills first, then a `<ul>` of one-line bullets each opening with a `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) whose `<strong>` lead-in is "Key point:" or "Common mistake:".
- **Tag pill CSS:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `ul` 0.92rem; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Card links in regenerated HTML (if referenced from grids) use `.html` extensions.
