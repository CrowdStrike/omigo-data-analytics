# XML and Its Schema Stack

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** XML and Its Schema Stack

**Subtitle:** XML wraps every value in a named tag and lets a schema (DTD, then XSD) reject bad documents at the door — rigor that JSON traded away for lightness

## One Coffee Order, Written in Angle Brackets

**Tags:** `core idea` (blue), `elements & attributes` (green), `namespaces` (orange)

- **The order** — a coffee shop's web store sends order 4172: two lattes at 4.50, one muffin at 3.25
- **Elements** — named tags that nest: `<order>` holds `<customer>`, `<item>`, `<total>` like folders
- **Attributes** — small facts riding on the tag itself: `id="4172"` on order, `qty="2"` on item
- **Text content** — the actual values sit between open and close tags: `<total>12.25</total>`
- **Namespaces** — `xmlns="http://shop.example/orders"` says whose vocabulary these tag names come from
- **Self-describing** — a stranger can read the document cold; nothing depends on column order

*Example (italic):* Order 4172 arrives as one XML document — the parser knows "12.25" is a total, not a price, purely from the tag wrapped around it.

**Key point:** XML is a tree of named elements with attributes and text; every value carries its own label, so the document explains itself — at the cost of repeating those labels everywhere.

### Visualization (canvas `c1`, 720×300)

Annotated document diagram: the order-4172 XML source in monospace with colored callouts naming each kind of markup.

- **Title (bold 15px, `#1a5276`, top center):** "Anatomy of <order>: One Document, Three Kinds of Markup".
- **Code block:** 13px monospace `#2c3e50` lines, left edge x=70, first baseline y=72, line height 22, drawn from the hardcoded array:
  - `<order id="4172" xmlns="http://shop.example/orders">`
  - `  <customer>Priya</customer>`
  - `  <item sku="LAT-01" qty="2">`
  - `    <name>latte</name> <price>4.50</price>`
  - `  </item>`
  - `  <item sku="MUF-03" qty="1"> ... </item>`
  - `  <total>12.25</total>`
  - `</order>`
- **Callouts (bold 12px labels, 1.5px arrow lines in the same color):** green `#008300` "attribute" at (560, 60) pointing to `id="4172"`; violet `#4a3aa7` "namespace" at (560, 95) pointing to the `xmlns` string; blue `#2a78d6` "element" at (560, 135) pointing to `<customer>`; aqua `#199e70` "text content" at (560, 170) pointing to `Priya`.
- **Annotation (bold 13px magenta `#d55181`, x=70, y=272):** "every value wears a named tag — self-describing, but heavy".
- **Caption (12px `#444`, bottom right):** "order values illustrative".

## The XSD Gate: Catching qty="two" Before Any Code Runs

**Tags:** `worked example` (blue), `XSD` (green), `validation` (orange)

- **The schema** — an XSD for orders: `qty` must be `xs:positiveInteger`, `price` an `xs:decimal`
- **Occurrence** — the XSD demands 1..many `<item>` children and exactly one `<total>` per order
- **The pass** — order 4172 (qty 2 and 1, prices 4.50 and 3.25, total 12.25) validates cleanly
- **The catch** — order 4188 arrives with `qty="two"`; the validator rejects it, naming line and rule
- **DTD vs XSD** — DTD (older) mostly checks structure; XSD adds real datatypes and exact occurrence counts
- **The bytes** — this order is 264 bytes as XML and 158 bytes as equivalent JSON, about 40% lighter

*Example (italic):* The same fields as JSON — `{"id":4172,"items":[...],"total":12.25}` — cost 158 bytes to XML's 264, one big reason browsers and APIs drifted to JSON.

**Key point:** An XSD is a contract a machine enforces: bad types, missing fields, or wrong counts are rejected before your code ever parses them — JSON won on weight, not on rigor.

### Visualization (canvas `c2`, 720×300)

Split panel: left, two order documents flowing through an XSD gate (one passes, one bounces); right, horizontal byte-count bars for the same order in XML vs JSON.

- **Title (bold 15px, `#1a5276`, top center):** "One Schema, Two Fates — and the Byte Bill That Decided the Web".
- **Left panel (x 30–390):** rounded boxes 150×36, 8px radius, 12px `#2c3e50` text: "order 4172  qty=2" at (40, 80) fill `rgba(42,120,214,0.15)`; "order 4188  qty=\"two\"" at (40, 170) fill `rgba(42,120,214,0.15)`. Vertical gate box "XSD gate" at (250, 70), 70×140, fill `rgba(74,58,167,0.12)`, 12px bold violet `#4a3aa7` label. 3px arrows from each order into the gate; from the gate, green `#008300` arrow to bold 12px green "✓ accepted" at (345, 98) and red `#e74c3c` arrow to bold 12px red "✗ xs:positiveInteger violated" at (300, 232).
- **Right panel bars (14px tall, 11px `#444` value labels at bar ends):** baseline x=430; "XML 264 B" blue `#2a78d6` fill `rgba(42,120,214,0.30)` with 2px blue edge, width 264px at y=110; "JSON 158 B" aqua `#199e70` fill `rgba(25,158,112,0.30)` with 2px aqua edge, width 158px at y=160; 12px `#444` row labels above each bar.
- **Annotation (bold 13px aqua `#199e70`, x=430, y=215):** "same order: 158 vs 264 bytes — 40% lighter in JSON".
- **Caption (12px `#444`, bottom right):** "byte counts and orders illustrative".

## Where XML Still Runs the Pipes

**Tags:** `where it's used` (blue), `enterprise feeds` (green), `data quality gate` (orange)

- **Finance** — SEC filings ship as XBRL and interbank payments as ISO 20022, both XML with XSDs
- **SOAP** — enterprise partner APIs still speak SOAP: XML envelopes validated against WSDL/XSD
- **Health & docs** — HL7 clinical documents, DOCX/XLSX internals, RSS and sitemaps are all XML
- **The nightly feed** — a shop ingests 12,000 order records; XSD validation rejects 37 malformed ones
- **The gate** — 11,963 clean rows load; each reject carries the line and the violated rule for free
- **The lesson** — validating at the door beats discovering `qty="two"` inside a notebook at 2am

*Example (italic):* Of 12,000 records in the nightly partner feed, the XSD gate bounces 37 with named reasons and lets 11,963 into the warehouse untouched.

**Key point:** A data scientist rarely chooses XML but constantly receives it — and its schema stack is a ready-made data-quality gate that names every bad row before it lands.

### Visualization (canvas `c3`, 720×300)

Pipeline flow diagram: three enterprise XML sources feeding an XSD validation gate that splits the 12,000-record feed into accepted and rejected streams.

- **Title (bold 15px, `#1a5276`, top center):** "The Nightly Feed: XSD as the Data-Quality Gate".
- **Source boxes (150×34, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text) at x=30:** "ISO 20022 payments" y=75, "XBRL filings" y=130, "SOAP partner API" y=185.
- **Gate box:** "XSD validate — 12,000 records" at (280, 115), 170×54, fill `rgba(74,58,167,0.12)`, bold 12px violet `#4a3aa7` text; 3px `#6b7280` arrows from each source box into its left edge.
- **Accepted path:** 3px green `#008300` arrow from gate to a green box "warehouse — 11,963 rows" at (520, 100), 170×40, fill `rgba(0,131,0,0.12)`, bold 12px green text.
- **Rejected path:** 3px red `#e74c3c` arrow angling down from gate to a red box "37 rejected + reasons" at (450, 220), 160×36, fill `rgba(231,76,60,0.12)`, bold 12px red text.
- **Annotation (bold 13px aqua `#199e70`, x=60, y=272):** "every reject names its line and rule — free data profiling".
- **Caption (12px `#444`, bottom right):** "record counts illustrative; the formats are real XML standards".

## Attributes vs Elements — and Why Verbose Is Not Rigorous

**Tags:** `common mistake` (red), `attributes vs elements` (orange), `verbosity` (green)

- **Two spellings** — `qty="2"` (attribute) and `<qty>2</qty>` (element) are both legal XML for one fact
- **No winner** — XML never says which is "correct"; feeds that mix both break naive parsers
- **Different paths** — XPath reads `@qty` for the attribute but `qty` for the element; code must match
- **The bigger myth** — believing 264 heavy bytes are inherently safer than 158 light ones
- **Rigor is opt-in** — an unvalidated XML file is exactly as trustworthy as a bare JSON blob
- **Both can gate** — JSON Schema gives JSON the same contract power XSD gives XML

*Example (italic):* One partner sends `qty="2"`, another `<qty>2</qty>`; the parser written for attributes silently reads the second feed's quantity as missing.

**Common mistake:** Equating angle brackets with safety. The 264-byte order with no XSD is unchecked; the 158-byte JSON with a JSON Schema is validated — rigor lives in the schema, not the format.

### Visualization (canvas `c4`, 720×300)

Quadrant scatter: document size (bytes) on x, validation rigor on y, placing the same order in four setups — XML/JSON with and without a schema.

- **Title (bold 15px, `#1a5276`, top center):** "Rigor Comes From the Schema, Not the Angle Brackets".
- **Axes:** origin x=80, baseline y=245, plot width 560, plot height 175; x = document size 0–300 bytes with 12px `#444` ticks at 0/100/200/300; y = two 12px `#444` band labels "unchecked" (y≈215) and "machine-validated" (y≈110) left of the axis; dashed `#e5e9ef` quadrant lines at x-mid (150 bytes) and y-mid.
- **Points (radius 7 dots, bold 12px labels beside each, plotted at hardcoded byte/rigor positions):** "bare JSON" mute `#6b7280` at (158, low band); "XML, no XSD" orange `#d95926` at (264, low band); "JSON + JSON Schema" green `#008300` at (158, high band); "XML + XSD" blue `#2a78d6` at (264, high band).
- **Annotation (bold 13px magenta `#d55181`, x=95, y=70):** "validation is a choice in both formats — JSON just made the default lighter".
- **Caption (12px `#444`, bottom right):** "byte counts from the section-2 order; rigor axis schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); order 4172 (2 lattes at 4.50, 1 muffin at 3.25, total 12.25), the rejected order 4188 with `qty="two"`, byte counts 264 (XML) vs 158 (JSON), and the nightly feed 12,000 / 37 rejected / 11,963 loaded are invented and labeled illustrative; the XML source lines in c1 and the four quadrant points in c4 are the exact arrays given in each spec.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
