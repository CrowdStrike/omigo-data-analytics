# XML

**Page type:** detail page (single card-section with two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** XML

**Subtitle:** Verbose structured markup with schemas and namespaces.

## Callout (intro)

**Core trade-off:** Validation and contractual precision that lets two organizations agree on a data format — at the cost of weight and ceremony that makes simple things complex.

## How It Works

Nested-tag markup descended from SGML, with an ecosystem of standards around it: XSD for validation, XSLT for transformation, XPath/XQuery for querying, namespaces for cross-organization composition.

- **Schema:** External XSD/DTD — validation is contractual
- **Layout:** Prolog, one root element, nested tags that all must close
- **Size:** 3-10× equivalent JSON
- **Best for:** Formal contracts between organizations

**How it's parsed:** A conforming parser tokenizes tags, enforces well-formedness (every tag closes, one root element), resolves entities and namespaces, then optionally validates against an XSD — which checks element order, types, and cardinality like a type-checker for documents. That validation step is why finance and healthcare still use it: two organizations can sign a schema and reject bad data at the boundary. It's also why parsers are heavy — and why entity resolution enabled a whole class of XXE attacks.

**Trade-off:** Contractual precision at the cost of ceremony. The perpetual attributes-vs-elements debate, namespace confusion, and a spec ecosystem (XSD, XSLT, XPath, XQuery, DTD, SAX, DOM) large enough to be its own career. *(styled as key-point callout, red left border)*

*Example: ISO 20022 payments, FIXML, HL7 healthcare messages, EPUB, tax filings, SOAP services, Maven pom.xml.*

### Visualization (canvas `c1`, 720×300)

Annotated XML document tree with syntax coloring by namespace, plus an external XSD validation box.

- **Title (bold 14px, top center, `#1a5276`):** "XML — Nested Tags + External Schema Validation".
- **Document listing (12px monospace, starting x=50, y=55, 22px line height, indent 24px per depth level; depths per line: 0,1,2,2,1,1,2,1,0; light `#eee` vertical guide lines at each indent level):**
  1. `<order ` in `#1a5276`, `xmlns:pay="…" xmlns:ship="…"` in `#8e44ad`, `>` in `#1a5276`
  2. `<item sku="A-100" qty="2">` in `#1a5276`
  3. `<name>` in `#1a5276`, `Widget` in `#333`, `</name>` in `#1a5276`
  4. `<pay:price currency="USD">` in `#8e44ad`, `29.99` in `#333`, `</pay:price>` in `#8e44ad`
  5. `</item>` in `#1a5276`
  6. `<ship:address type="residential">` in `#27ae60`
  7. `<ship:city>` in `#27ae60`, `Portland` in `#333`, `</ship:city>` in `#27ae60`
  8. `</ship:address>` in `#27ae60`
  9. `</order>` in `#1a5276`
- **XSD box:** dashed red (`#e74c3c`, dash 5/4, width 1.5) rectangle 195×120 at (480, 62). Bold 11px red header: "XSD schema (external)". Below in 11px `#555`, four lines:
  - "✓ element order"
  - "✓ types (decimal, date…)"
  - "✓ cardinality (1..n)"
  - "✗ reject at the boundary"
- **Arrow:** red 1.5px line with filled triangular head from the document (near line 4/5) to the XSD box, labeled "validate" in 10px red.
- **Namespace legend (11px system-ui, bottom left):**
  - "■ pay: namespace — payment team owns these elements" in `#8e44ad`
  - "■ ship: namespace — shipping team, no name collisions" in `#27ae60`

## Regeneration instructions

- **Layout:** single-page detail doc: h1 with `2px solid #2980b9` bottom border, `.subtitle` paragraph, `.intro-callout` div, then one `.card-section` containing an h2 (also `2px solid #2980b9` bottom border) and a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) holding intro paragraph, `<ul>` bullets, "How it's parsed" paragraph, `.key-point` div, `.example` paragraph; right `<td class="viz-col">` (55%) holding the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `.intro-callout` background `#f8f9fa`, left border `3px solid #2980b9`, padding 10px 14px, 0.93rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `code` background `#f0f4f8`, 2px 6px padding, 3px radius. Canvas `width: 100%`, `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad` (namespace accent).
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has none).
