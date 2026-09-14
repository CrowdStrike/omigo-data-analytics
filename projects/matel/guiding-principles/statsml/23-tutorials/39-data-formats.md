# Data Formats

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Data Formats

**Subtitle:** How data gets written down — from raw bits and serialized bytes, through human-readable text formats and their schemas, up to graphs of machine-readable facts.

## Cards

Each card links to a topic page under `data-formats/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | BITS & BYTES | Binary & Two's Complement | [39-data-formats/01-binary-and-twos-complement.md](39-data-formats/01-binary-and-twos-complement.md) | An integer in memory is a row of on/off switches — and negative numbers are the patterns that make addition wrap around to zero. | binary, negative numbers, overflow |
| 2 | BITS & BYTES | Serialization | [39-data-formats/02-serialization.md](39-data-formats/02-serialization.md) | Serialization flattens an object full of in-memory pointers into bytes that can cross a wire or land on disk — and back. | objects to bytes, wire format, deserialization |
| 3 | TEXT FORMATS & SCHEMAS | JSON, YAML, TOML | [39-data-formats/03-json-yaml-toml.md](39-data-formats/03-json-yaml-toml.md) | Three spellings of the same maps-and-lists data — the structure is identical, and one spelling silently turns Norway into false. | config files, maps and lists, gotchas |
| 4 | TEXT FORMATS & SCHEMAS | XML and Its Schema Stack | [39-data-formats/04-xml-and-its-schema-stack.md](39-data-formats/04-xml-and-its-schema-stack.md) | XML wraps every value in a named tag and lets a schema reject bad documents at the door — rigor that JSON traded away for lightness. | angle brackets, DTD, XSD |
| 5 | TEXT FORMATS & SCHEMAS | JSON Schema | [39-data-formats/05-json-schema.md](39-data-formats/05-json-schema.md) | A JSON Schema is a JSON document that describes what other JSON documents must look like — teaching JSON to validate itself. | validation, self-describing, required fields |
| 6 | SCHEMA CONTRACTS | Interface Definition Languages | [39-data-formats/06-interface-definition-languages.md](39-data-formats/06-interface-definition-languages.md) | Protobuf, Avro, and Thrift define a message's structure once in a neutral schema file — every language gets generated code that agrees on the bytes. | protobuf, avro, code generation |
| 7 | SCHEMA CONTRACTS | OpenAPI | [39-data-formats/07-openapi.md](39-data-formats/07-openapi.md) | One YAML file describes every path, parameter, and response of an API — the schema of a whole API, readable by humans and machines alike. | API contract, paths and responses, generated docs |
| 8 | SEMANTIC WEB | RDF & Triples | [39-data-formats/08-rdf-and-triples.md](39-data-formats/08-rdf-and-triples.md) | RDF stores data as tiny three-part facts — subject, predicate, object — so any two datasets can merge by simply stacking their facts. | triples, atomic facts, data merging |
| 9 | SEMANTIC WEB | Ontologies & OWL | [39-data-formats/09-ontologies-and-owl.md](39-data-formats/09-ontologies-and-owl.md) | An ontology is a schema for meaning — it declares what kinds of things exist and how they may relate, so a machine can deduce facts nobody typed. | classes, relations, inference |
| 10 | SEMANTIC WEB | Knowledge Graphs & SPARQL | [39-data-formats/10-knowledge-graphs-and-sparql.md](39-data-formats/10-knowledge-graphs-and-sparql.md) | Store every fact as a tiny subject-predicate-object sentence, and questions become graph patterns you match against the web of facts. | graph queries, pattern matching, linked data |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "BITS & BYTES" `#2980b9`, "TEXT FORMATS & SCHEMAS" `#27ae60`, "SCHEMA CONTRACTS" `#e67e22`, "SEMANTIC WEB" `#8e44ad`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#e67e22`, `#8e44ad`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
