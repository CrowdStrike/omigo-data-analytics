# Unit & Type Errors

**Page type:** detail page (tutorial layout: h2 card-sections, two-column table with text left 50% / canvas right 50%; one section uses a 3-column 38/31/31 layout with two canvases)
**HTML title tag:** Unit & Type Errors

**Subtitle:** A column can hold values that aren't the same kind of thing — kilograms next to pounds, digits stored as text, dates that never parsed

## Eight Packages, One Column, Two Units

**Tags:** `core idea` (blue), `running example` (green)

- **The data** — a warehouse logs package weights; the form never asked which unit
- **Four people** — weighed in kilograms and wrote: 2, 3, 4, 5
- **Four people** — weighed same-size packages in pounds: 4.4, 6.6, 8.8, 11.0
- **Same packages** — 11.0 lbs IS 5 kg; each weight appears twice, in disguise
- **No wrong cell** — every value is a real, correct measurement on its own

*Example:* Scrolling the column, 4.4 sits comfortably between 4 and 5 — nothing about any single number gives the mix away.

**Key point:** A unit error is invisible row by row. It only shows up in the shape of the whole column — or in the pairing, if you know what to look for.

### Visualization (canvas `c1`, 720×300)

Number-line dot plot: kg dots and lbs dots on one axis, with dashed arcs pairing each package's two recordings.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Four Packages, Recorded Twice in Disguise".
- **Axis:** horizontal line at y=200 from value 0 to 12 (padding left 60, right 30), gray `#999`; ticks and muted 12px labels at 0, 2, 4, 6, 8, 10, 12; axis caption below: "value written in the weight column (unit unknown)".
- **Data:** kg values `[2, 3, 4, 5]` as blue (`#2a78d6`) dots (radius 7) just above the axis, each labeled bold 12px above; lbs values `[4.4, 6.6, 8.8, 11.0]` as orange (`#d95926`) dots, same style.
- **Pairing arcs:** four dashed muted-gray quadratic arcs (dash 5/4, width 1.5) joining kg[i] to lbs[i] above the axis, peaks staggered upward per pair.
- **Arc caption (muted bold 12px, centered, y=52):** "each arc joins one package to its own weight in the other unit (× 2.2)".
- **Legend (y=262):** blue dot "entered in kilograms"; orange dot "entered in pounds — same packages"; magenta (`#d55181`) bold 13px at right: "one column, two rulers".

## The Average of Kilograms and Pounds Is Nothing

**Tags:** `worked example` (green)

- **Mixed mean** — (2 + 3 + 4 + 5 + 4.4 + 6.6 + 8.8 + 11) / 8 = 44.8 / 8 = 5.6
- **True mean in kg** — the packages average (2 + 3 + 4 + 5) / 4 = 3.5 kg
- **True mean in lbs** — the very same packages average 7.7 lbs
- **5.6 is neither** — 60% above the kg truth, 27% below the lbs truth
- **The fix** — convert first (lbs ÷ 2.2 = kg), then average: back to 3.5 kg

*Example:* The report said "average package: 5.6" — 5.6 whats? A number with no unit has no meaning.

**Key point:** You can only average numbers that measure the same thing in the same unit. Convert first, compute second.

### Visualization (canvas `c2`, 720×300)

Three-bar chart: kg truth, mixed mean, lbs truth.

- **Title (bold 15px, `#1a5276`, top center):** "The Mixed Mean Lands Between Two Truths and Equals Neither".
- **Axes:** y from 0 to 9 with gridlines (`#e5e9ef`) and muted labels at 0, 2, 4, 6, 8; L-shaped gray axes; padding top 60, bottom 66, left 60, right 25.
- **Bars (140px wide):** "true mean in kg" = 3.5 blue `#2a78d6` (value label "3.5 kg", sub-caption "(2+3+4+5)/4"); "mean of the mixed column" = 5.6 violet `#4a3aa7` (label "5.6 ???", sub "44.8 / 8"); "true mean in lbs" = 7.7 orange `#d95926` (label "7.7 lbs", sub "(4.4+6.6+8.8+11)/4"). Bold 13px value labels above bars; 12px category labels and muted formula sub-captions below the baseline.
- **Annotation (violet bold 13px, centered, y=46):** "5.6 has no unit: 60% above the kg truth, 27% below the lbs truth".

## Text Pretending to Be Numbers and Dates

**Tags:** `running example` (green), `where it's used` (blue)

- **The price column** — an export wrote prices with commas: "1,299" is text, not a number
- **Text math** — sum() errors out, or the sort files "1,299" before "899"
- **Why** — text sorts character by character, and "1" comes before "8"
- **The date column** — 190 of 200 rows parsed; 10 rows (5%) like "March 5th" stayed text
- **Silent loss** — the monthly sales chart just drops those 10 rows, no warning

*Example:* Top of the "cheapest first" list: the $1,299 headphones — sorted as text, 1 beats 8.

**Key point:** A column of digits is not automatically a number column. One comma or stray word quietly turns math into text games.

### Visualization (canvas `c3a`, 420×340)

Two side-by-side sorted columns comparing text sort vs numeric sort of prices.

- **Title (bold 14px, `#1a5276`, top center):** "\"Cheapest First\" — Text vs Number".
- **Left column (x=30, header "sorted as text" in orange `#d95926` bold 13px):** three 160×36 cells in 14px monospace: `"1,299"`, `"250"`, `"899"`; the first cell (the wrong winner) tinted `rgba(217,89,38,0.15)` with bold orange text. Grid borders `#e5e9ef`.
- **Right column (x=230, header "sorted as numbers" in green `#008300`):** cells `250`, `899`, `1299`, no highlight.
- **Annotations (bold 12px, centered per column):** orange, two lines under left column: "the most expensive item won" / "\"cheapest\" — \"1\" sorts before \"8\""; green, two lines under right column: "strip the comma, cast to number," / "and the order is honest".
- **Caption (muted 12px, bottom center):** "same three prices in both columns".

### Visualization (canvas `c3b`, 420×340)

Stacked horizontal bar for date parse rate plus examples of unparsed values.

- **Title (bold 14px, `#1a5276`, top center):** "200 Dates: 190 Parsed, 10 Stayed Text".
- **Bar (x=40, y=60, 340×44, muted outline):** green `#008300` segment 190/200 of the width with white bold 13px label inside "190 parsed as real dates (95%)"; orange `#d95926` segment for the rest, labeled below in bold orange 12px "10 rows (5%) still text".
- **Failure examples (header muted bold 12px "what the 10 stragglers look like:", y=158):** orange 13px monospace list: `"March 5th"`, `"2026/13/02"`, `"n/a"`, `"yesterday"`.
- **Side note (magenta `#d55181` bold 12px, three lines at x≈230):** "every date chart quietly" / "drops these 10 rows —" / "no error is ever raised".
- **Caption (muted 12px, bottom center):** "a strict parse + a failure count makes the 5% visible".

## Type Checks Catch What Eyeballing Misses

**Tags:** `rule of thumb` (blue), `common mistake` (red)

- **Eyeballing passes** — "1,299" and 899 look equally fine scrolling a spreadsheet
- **Check the type** — one text value makes a whole column load as text ("object")
- **Check min and max** — identical boxes ranging 2 to 11 is a unit-mix flag
- **Check the parse rate** — parse numbers and dates strictly, then count the failures
- **Every load** — three checks, three lines of code, run on every new file

*Example:* df.dtypes said the price column was "object" — that single word was the entire bug report.

**Common mistake:** Trusting a column because its values look right. The declared type is a claim — machines verify it in milliseconds; eyes never do.

### Visualization (canvas `c4`, 720×300)

Three-row check table mapping each cheap check to the bug it caught on this page.

- **Title (bold 15px, `#1a5276`, top center):** "Three Cheap Checks vs the Three Bugs on This Page".
- **Column headers (muted bold 12px):** "the check", "the question it asks", "what it caught here".
- **Rows (check name in bold 13px colored, question in 12px `#2c3e50`, catch in bold 12px orange `#d95926`; a green `#008300` bold 17px "✓" leads each row):**
  - "check the type" (blue `#2a78d6`) / "is the column text or numeric?" / "caught \"1,299\" — price was text"
  - "check min / max" (aqua `#199e70`) / "is the range physically sensible?" / "caught kg+lbs — same boxes, 2 to 11"
  - "check the parse rate" (violet `#4a3aa7`) / "how many rows failed strict parsing?" / "caught the 10 unparsed dates (5%)"
- **Layout:** table at x=40, y=48; row height 54; column widths 170+40 / 240 / 240; grid borders `#e5e9ef`.
- **Bottom annotations (centered):** magenta bold 13px "none of the three bugs is visible by scrolling — all three fall to one-line checks" (y=252); muted 12px "run them on every new file, before any chart or model sees the data" (y=276).

## Regeneration instructions

- **Template:** tutorial topic page (tutorials/CLAUDE.md conventions). `<h1>` concept name, `.subtitle`, four `.card-section` blocks each `<h2>` + `table.layout`. Sections 1, 2, 4 use two columns (`td.text-col` 50% / `td.viz-col` 50%, one 720×300 canvas); section 3 uses the 3-column layout (`td.text-col3` 38% / two `td.viz-col3` 31% each, canvases `c3a` and `c3b` at 420×340).
- **Left column structure per section:** `.tags` pill row, `<ul>` of one-line bullets with `<b>` lead terms (colored `#1a5276`), italic `.example` line, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) with bold lead-in ("Key point:" / "Common mistake:").
- **Tag pill CSS:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes as given per chart, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper reading the width/height attributes.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Card links in regenerated HTML (if referenced from grids) use `.html` extensions.
