# Reflection & Annotations

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Reflection & Annotations

**Subtitle:** Reflection is a running program reading its own parts — field names, types, attached notes — and annotations are the sticky notes you write on those parts for it to find

## A Program That Reads Its Own Labels

**Tags:** `core idea` (blue), `runtime lookup` (green), `metadata` (orange)

- **The moving day** — good movers handle a stranger's boxes fast because each label says what's inside
- **The exporter** — a report tool meets a Customer record it has never seen and must print it as a table
- **The trick** — nobody told it the fields; it asks the running program "what fields does this thing have?"
- **Reflection** — that is the word for a program reading its own structure — names, fields, types — while running
- **Annotations** — sticky notes attached to a field or function, sitting there until some code looks for them
- **Time travel** — the exporter keeps working on record types written years after the exporter itself

*Example (italic):* The exporter ships knowing nothing about Customer, asks it for its field list at runtime, and prints a perfect table anyway.

**Key point:** Reflection is the program reading its own structure while it runs; annotations are the notes you attach to that structure for it to find.

### Visualization (canvas `c1`, 720×300)

Diagram: a Customer record box on the left, an "asks at runtime" arrow in the middle, and the answer — a plain list of field names — on the right, showing the exporter learning structure it was never taught.

- **Title (bold 15px, `#1a5276`, top center):** "The Exporter Never Saw This Record — It Asks".
- **Left box:** rounded rect x=50 to x=270, y=65 to y=250, 2px `#2a78d6` border, fill `rgba(42,120,214,0.08)`; bold 13px `#1a5276` header "Customer record" centered at y=85; below it four field rows in 13px `#2c3e50` at x=70, y = 120, 152, 184, 216: "name", "email", "password", "signup_date"; the password row carries a small orange `#d95926` 11px tag "@skip" to its right.
- **Arrow:** 3px `#2a78d6` horizontal arrow with arrowhead from x=285 to x=435 at y=155; bold 13px `#2a78d6` label above (y=138): "what fields do you have?"; 12px `#6b7280` label below (y=172): "asked at runtime".
- **Right box:** rounded rect x=450 to x=690, y=65 to y=250, 2px `#008300` border, fill `rgba(0,131,0,0.08)`; bold 13px `#008300` header "answer: 4 field names" centered at y=85; below, 12px `#2c3e50` monospace-style lines at x=470, y = 120, 152, 184, 216: `"name"`, `"email"`, `"password"  + note: @skip`, `"signup_date"`.
- **Annotation (bold 12px orange `#d95926`, centered near y=278):** "no Customer code inside the exporter — it reads the structure while running".
- **Caption (11px `#444`, bottom right):** "illustrative — one record type shown".

## Exporting a Customer Record, Field by Field

**Tags:** `worked example` (blue), `annotations` (orange)

- **The record** — one Customer holds 4 fields: name, email, password, signup_date
- **The note** — password carries an @skip annotation meaning "never put this field in a report"
- **Step 1: list** — reflection returns the 4 field names; no Customer-specific code was written
- **Step 2: filter** — the exporter checks each name for @skip; password is dropped, 3 fields remain
- **Step 3: write** — header "name, email, signup_date", then "Maya Rao, maya@example.com, 2026-03-14"
- **Hand check** — 4 fields in, 1 note found, 3 columns out; you can trace every step on paper

*Example (italic):* Maya Rao's row exports as 3 columns — her password value never leaves the program, all because of one sticky note.

**Key point:** The whole export is list, filter, write: 4 fields minus 1 @skip = 3 columns, and the exporter never names a single Customer field itself.

### Visualization (canvas `c2`, 720×300)

Flow chart: four field pills on the left, an arrow from each toward the output, with the password arrow blocked at the @skip check, and the finished 3-column table on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Four Fields In, One @skip Note, Three Columns Out".
- **Field pills (left, x=60 to x=230):** four rounded 26px-tall pills at y = 78, 122, 166, 210, fill `rgba(42,120,214,0.12)`, 12px `#1a5276` bold text: "name", "email", "password", "signup_date"; the password pill also shows an 11px orange `#d95926` tag "@skip" at its right edge.
- **Arrows:** 2px `#2a78d6` arrows from each pill's right edge (x=235) toward x=395 at the pill's row height; the password arrow is `#e74c3c`, stops at x=330, and ends in a bold 14px `#e74c3c` "×" at x=340, y=179 with an 11px `#e74c3c` label "dropped" underneath.
- **Skip check:** vertical dashed `#6b7280` (dash 4/3) line at x=330 from y=60 to y=240; 11px `#6b7280` label rotated or above at y=52: "check for @skip".
- **Output table (right, x=410 to x=690):** header row of three cells at y=100, 24px tall, fill `rgba(0,131,0,0.15)`, bold 12px `#008300` text "name", "email", "signup_date"; one data row of three cells at y=130, 24px tall, 1px `#e5e9ef` border, 11px `#2c3e50` text "Maya Rao", "maya@example.com", "2026-03-14".
- **Annotation (bold 12px green `#008300`, near x=410, y=205):** two lines: "3 of 4 fields exported —" / "the @skip note did its job".
- **Caption (11px `#444`, bottom right):** "illustrative — one record shown".

## Why Your Tools Already Do This

**Tags:** `where it's used` (blue), `serializers` (green), `data frames` (orange)

- **Serializers** — JSON and CSV writers turn any object into text by reflecting over its fields
- **Data frames** — asking a table for its column names and types is reflection on the table itself
- **Validators** — libraries read notes like "must be positive" off a field and check your data for you
- **Model summaries** — printing a network layer by layer works by reading the model's own structure
- **The payoff** — one 18-line reflective exporter replaces 96 hand-written lines across 8 record types
- **The cost** — asking at runtime is slower than fixed code, and typos surface only when it runs

*Example (italic):* A team with 8 record types maintains 96 lines of copy-pasted exporters — or one 18-line loop that asks each record for its fields.

**Key point:** Reflection trades a little speed and early error-checking for tools that handle data shapes they have never seen — which is most tools a data scientist touches.

### Visualization (canvas `c3`, 720×300)

Single-panel line chart: lines of exporter code versus number of record types, one climbing line for hand-written exporters and one flat line for the single reflective exporter.

- **Title (bold 15px, `#1a5276`, top center):** "Hand-Written Exporters Grow; the Reflective One Stays 18 Lines".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x axis = record types 0 to 8 with 12px `#444` tick labels "0", "2", "4", "6", "8" every 2; y axis = lines of code 0 to 100, 12px `#444` tick labels "0", "25", "50", "75", "100" with light `#e5e9ef` gridlines at 25, 50, 75, 100; 12px `#444` axis captions "record types" (bottom center) and "lines of code" (rotated, left).
- **Hand-written line:** orange `#d95926` 3px line with 5px dots through hardcoded points at types = `[1, 3, 5, 8]`, lines = `[12, 36, 60, 96]`; 12px orange label "one exporter per type" near types≈4.5, lines≈62.
- **Reflective line:** green `#008300` 3px line with 5px dots through types = `[1, 3, 5, 8]`, lines = `[18, 18, 18, 18]`; 12px green label "one reflective exporter" near types≈5.5, lines≈24.
- **Crossover marker:** small 11px `#6b7280` note near types≈1.3, lines≈15: "below 2 types, hand-written is shorter".
- **Annotation (bold 13px blue `#2a78d6`, near types≈6, lines≈88):** "8 record types: 96 lines vs 18".
- **Caption (11px `#444`, bottom right):** "illustrative line counts".

## A Sticky Note Nobody Reads

**Tags:** `common mistake` (red), `inert metadata` (orange)

- **The belief** — the team writes @skip on the password field and feels the data is now protected
- **The truth** — an annotation is plain metadata; by itself it changes nothing about the program
- **The reader** — the note only takes effect when the exporter's loop actually checks for @skip
- **The leak** — with no reader, the "protected" export still prints 4 columns, password included
- **The contract** — for every annotation you write, be able to name the code that reads it

*Example (italic):* The team annotated password with @skip, never updated the old exporter, and shipped 4-column reports for a month.

**Common mistake:** Believing the annotation acts on its own. It is a note, not a guard — some code must read it, or nothing happens.

### Visualization (canvas `c4`, 720×300)

Two-row comparison on a shared layout: the same @skip note with no reader (4 columns out, password leaked) versus with a reader (3 columns out), making the inertness of annotations visible.

- **Title (bold 15px, `#1a5276`, top center):** "Same @skip Note — the Difference Is Whether Anything Reads It".
- **Row 1 (y=95), label 12px `#444` at x=20, two lines:** "note written," / "no reader"; then four table cells from x=250 to x=690 (each 105px wide, 30px tall, 1px border): "name", "email", "password", "signup_date" in 12px; the "password" cell fill `rgba(231,76,60,0.18)` with bold 12px `#e74c3c` text and an 11px `#e74c3c` tag "leaked" above it.
- **Row 2 (y=185), label 12px `#444` at x=20, two lines:** "note written," / "exporter checks notes"; then three table cells from x=250 to x=580 (each 105px wide, 30px tall): "name", "email", "signup_date", fill `rgba(0,131,0,0.12)`, 12px `#008300` text; a dashed 1px `#6b7280` empty cell outline from x=585 to x=690 with 11px `#6b7280` text "password dropped".
- **Column counts:** bold 13px labels at x=700 right-aligned beside each row: `#e74c3c` "4 cols" (row 1), `#008300` "3 cols" (row 2).
- **Annotation (bold 13px magenta `#d55181`, centered near y=265):** "the note didn't protect the password — the reader did".
- **Caption (11px `#444`, bottom right):** "illustrative — same record, same note".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red appears only for the genuine error state (the leaked password).
- **Data:** all field names, table cells, and line-chart points are the hardcoded literals above (no randomness); the worked example's numbers (4 fields, 1 @skip, 3 columns; 96 vs 18 lines) must match between text and charts; invented counts carry an "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: realistic credential strings on this page were converted to generic placeholders — for illustration only, and to avoid false positives from secret scanners."
