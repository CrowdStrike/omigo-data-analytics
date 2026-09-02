# Data File Formats

**Page type:** grid page (card navigation grid, auto-fit columns min 280px)
**HTML title tag:** Data File Formats — Discussion Backlog

**Subtitle:** How data gets written down — plain text that refuses to die, binary schemas for speed, columnar storage for analytics, and formats that carry their own history.

## Cards

Each card links to a detail page under `file-formats/`. The card shows a colored uppercase category label, a numbered title, a one-sentence description, and a row of topic tag pills.

| # | Category | Title | Link | Description | Tags |
|---|----------|-------|------|-------------|------|
| 1 | PLAIN TEXT | CSV / TSV | [file-formats/01-csv-tsv.md](file-formats/01-csv-tsv.md) | Delimited text that needs nothing installed — and breaks when data contains the delimiter. | tabular, universal, no-schema |
| 2 | PLAIN TEXT | Fixed-Width Records | [file-formats/02-fixed-width.md](file-formats/02-fixed-width.md) | Column positions defined in a separate document — the schema lived outside the file. | legacy, banking, mainframe |
| 3 | PLAIN TEXT | Markdown | [file-formats/03-markdown.md](file-formats/03-markdown.md) | Readable as plain text, renderable to anything, diffable in version control. | documentation, prose, LLM-native |
| 4 | PLAIN TEXT | JSONL / NDJSON | [file-formats/04-jsonl-ndjson.md](file-formats/04-jsonl-ndjson.md) | One JSON object per line — appendable, splittable, streamable. | streaming, logs, training-data |
| 5 | STRUCTURED | XML | [file-formats/05-xml.md](file-formats/05-xml.md) | Capable, verbose, and still standard where validation is contractual. | schema, enterprise, SOAP |
| 6 | STRUCTURED | JSON | [file-formats/06-json.md](file-formats/06-json.md) | The smallest format that carries nested data — won by being just enough. | web, APIs, nested |
| 7 | STRUCTURED | YAML | [file-formats/07-yaml.md](file-formats/07-yaml.md) | Pleasant to read, dangerous to write — meaning depends on invisible indentation. | config, DevOps, implicit-typing |
| 8 | STRUCTURED | TOML | [file-formats/08-toml.md](file-formats/08-toml.md) | Explicit, minimal, no surprises — configuration without YAML's hazards. | config, Rust, explicit-types |
| 9 | BINARY | Protocol Buffers / Thrift | [file-formats/09-protobuf-thrift.md](file-formats/09-protobuf-thrift.md) | Declared shape, generated code, compact encoding — what makes independent deployment possible. | RPC, schema-evolution, gRPC |
| 10 | BINARY | Avro | [file-formats/10-avro.md](file-formats/10-avro.md) | Schema travels with the data — a stored file remains readable without hunting for the definition. | Hadoop, self-describing, schema-evolution |
| 11 | COLUMNAR | Parquet / ORC | [file-formats/11-parquet-orc.md](file-formats/11-parquet-orc.md) | Store by column, not by row — changed what analytical queries cost, not merely how fast they run. | analytics, compression, predicate-pushdown |
| 12 | COLUMNAR | Arrow | [file-formats/12-arrow.md](file-formats/12-arrow.md) | In-memory columnar — tools share data without serialising between them. | zero-copy, interop, in-memory |
| 13 | ML FORMATS | safetensors / GGUF | [file-formats/13-safetensors-gguf.md](file-formats/13-safetensors-gguf.md) | Model weights as a storage class — fast partial loading, memory-mapping, and safety over pickle. | model-weights, inference, quantization |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(280px, 1fr))`, 14px gap.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, and `<div class="topics">` holding one `<span class="topic-tag">` per tag.
- **Category label colors** (applied via a small script mapping `.card-num` text to color): PLAIN TEXT `#27ae60`; STRUCTURED `#2980b9`; BINARY `#8e44ad`; COLUMNAR `#e67e22`; ML FORMATS `#e74c3c`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 18px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. `.card-num` 0.72em bold; h3 `#1a3a4a` 1em; description `#555` 0.84em; `.topic-tag` pills background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange. Canvases (none on this page) would use `window.devicePixelRatio` scaling.
