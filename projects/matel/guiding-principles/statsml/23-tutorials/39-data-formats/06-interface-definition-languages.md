# Interface Definition Languages

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Interface Definition Languages

**Subtitle:** Protobuf, Avro, and Thrift let you write a message's structure once in a neutral schema file — every language then gets generated code that agrees on the bytes

## The Order Written Down Once

**Tags:** `core idea` (blue), `schema first` (green), `Protobuf / Avro / Thrift` (orange)

- **The shop** — a coffee chain's register app is written in Java; the kitchen display runs Python
- **The message** — an order (id, drink, size, qty) must travel from register to kitchen intact
- **The old way** — each team hand-writes its own Order class, and the two drift apart within a month
- **The schema** — one `order.proto` file declares the fields once: `int32 id = 1; string drink = 2; ...`
- **The compiler** — `protoc` generates a matching Order class for Java and for Python from that one file
- **The family** — Protobuf, Avro, and Thrift all work this way: schema first, code generated from it

*Example (italic):* The register serializes Order(id=118, drink="latte", size="large", qty=2) from its generated Java class; the kitchen's generated Python class reads exactly the same bytes back.

**Key point:** An interface definition language declares a message's structure once in a language-neutral file; compilers generate matching code, so the schema — not any one codebase — is the source of truth.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one schema file at the top fanning out to two generated classes, which then exchange the actual order bytes along the bottom.

- **Title (bold 15px, `#1a5276`, top center):** "One order.proto File, Two Generated Codebases That Always Agree".
- **Schema box:** rounded box at x=265, y=50, 190×46, fill `rgba(74,58,167,0.10)`, 2px violet `#4a3aa7` border, 12px `#2c3e50` two-line label "order.proto" / "Order {id, drink, size, qty}".
- **Fan-out arrows:** two 3px `#6b7280` arrows from the schema box's bottom corners down to the two class boxes, each with an 11px `#6b7280` midpoint label "protoc".
- **Java box:** rounded box at x=70, y=160, 190×46, fill `rgba(42,120,214,0.15)`, 2px blue `#2a78d6` border, 12px label "Java class Order" / "(register app)".
- **Python box:** rounded box at x=460, y=160, 190×46, fill `rgba(0,131,0,0.12)`, 2px green `#008300` border, 12px label "Python class Order" / "(kitchen display)".
- **Message arrow:** 3px ink `#1a5276` arrow from the Java box's right edge to the Python box's left edge, bold 12px ink label above it "order #118 — bytes both sides understand".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=265):** "the schema is the contract — the code is generated, never hand-written".
- **Caption (12px `#444`, bottom right):** "layout schematic, order illustrative".

## 49 Bytes of JSON, 18 Bytes on the Wire

**Tags:** `worked example` (blue), `field numbers` (green), `wire format` (orange)

- **Field numbers** — each field gets a permanent numeric tag: id=1, drink=2, size=3, qty=4
- **On the wire** — protobuf sends tag+value pairs: id costs 2 bytes; "latte" costs 7 (tag, length, 5 letters)
- **The tally** — 2 + 7 + 7 + 2 = 18 bytes for the whole order
- **The JSON rival** — `{"id":118,"drink":"latte","size":"large","qty":2}` weighs 49 bytes
- **Why smaller** — field names never travel; only the one-byte numeric tags do
- **Adding a field** — `string milk = 5;` is safe: an old reader sees unknown tag 5 and skips those bytes

*Example (italic):* Milk is added to the schema at 9am and only the register is redeployed — the un-upgraded kitchen display keeps working all day, skipping the tag-5 bytes it does not know.

**Key point:** Field numbers are the real names on the wire; because unknown numbers are skipped, the two sides can upgrade on different days without breaking each other.

### Visualization (canvas `c2`, 720×300)

Byte-strip comparison: the same order as one long JSON strip vs a short protobuf strip segmented by field, drawn to a shared bytes-per-pixel scale.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Order: 49 Bytes of JSON vs 18 Bytes of Protobuf".
- **Scale:** 8px per byte, strips start at x=120, strip height 34; row labels 12px `#444` at x=20 ("JSON" at y=95, "Protobuf" at y=195).
- **JSON strip (y=78):** one block 392px wide (49 bytes), fill `rgba(107,114,128,0.18)`, 1px `#6b7280` border; 11px `#2c3e50` text inside: `{"id":118,"drink":"latte","size":"large","qty":2}`; 12px `#444` label at its right end "49 bytes".
- **Protobuf strip (y=178), four segments left to right:**
  - blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 16 (2 bytes), 11px label below "id=118 (2)"
  - green `#008300` fill `rgba(0,131,0,0.22)`, width 56 (7 bytes), 11px label below "\"latte\" (7)"
  - aqua `#199e70` fill `rgba(25,158,112,0.22)`, width 56 (7 bytes), 11px label below "\"large\" (7)"
  - violet `#4a3aa7` fill `rgba(74,58,167,0.20)`, width 16 (2 bytes), 11px label below "qty=2 (2)"
  - each segment gets a 1px border in its solid color; 12px `#444` label at the strip's right end "18 bytes".
- **Annotation (bold 13px green `#008300`, near x=380, y=250):** "63% fewer bytes — field names never travel, only tags 1–4 do".
- **Caption (12px `#444`, bottom right):** "byte counts exact for these strings; order illustrative".

## Contracts for Services That Deploy on Different Days

**Tags:** `where it's used` (blue), `schema registry` (green), `evolution` (orange)

- **Microservices** — dozens of services in different languages share messages; the IDL file is the contract
- **Kafka + Avro** — a producer registers each schema version with a schema registry before publishing
- **The gate** — the registry rejects incompatible changes, like deleting a field old consumers still read
- **Evolution rules** — add optional fields freely; never delete or renumber what readers depend on
- **gRPC** — Protobuf also declares the service calls themselves, not just the data they carry
- **Data lakes** — Avro files embed their schema, so files written years ago stay readable today

*Example (italic):* A producer tries to publish orders without qty; a registry set to forward (or full) compatibility rejects the new schema — old consumers still expect qty — and the change never reaches the topic.

**Key point:** Schema-first data turns "will this change break someone?" from a 2am production incident into a compile-time or registry-time check.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: a compatible schema change passing the registry check vs a breaking change being rejected before it can reach the Kafka topic.

- **Title (bold 15px, `#1a5276`, top center):** "Kafka + Avro: the Schema Registry Blocks the Breaking Change".
- **Row 1 (y=95), label 12px `#444` at x=20:** "add optional milk"; blue `#2a78d6` rounded box at x=150 labeled "producer submits v2" (12px), 3px arrow to a green `#008300` box at x=350 labeled "registry: compatible ✓", 3px arrow to a green box at x=555 labeled "v1 consumer still reads".
- **Row 2 (y=205), label:** "delete qty"; blue box at x=150 labeled "producer submits v3", 3px arrow to a red `#e74c3c` box at x=350 labeled "registry: rejected ✗", with bold 12px red text at x=560 "never reaches the topic".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the registry enforces evolution rules so consumers never crash".
- **Caption (12px `#444`, bottom right):** "flow schematic".

## A Field Number Is Forever

**Tags:** `common mistake` (red), `reused tags` (orange), `required fields` (red)

- **The temptation** — you delete `string size = 3;` and later hand a new field the "free" number 3
- **The collision** — archived messages still carry 3 = "large" (a string); new code reads 3 as an int
- **The symptom** — no error at the boundary: garbage values or a decode crash deep in the pipeline
- **The fix** — mark retired numbers `reserved 3;` so the compiler refuses to ever hand them out again
- **Required's ghost** — proto2's `required` made fields undeletable forever; proto3 dropped the keyword

*Example (italic):* A week-old archived order with tag 3 = "large" hits the new decoder expecting an int discount at tag 3 and crashes the nightly replay job.

**Common mistake:** Treating field numbers as editable labels. They are permanent wire-format addresses — retire them with `reserved`, never recycle them.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: old bytes meeting a recycled field number (garbage/crash) vs the same deletion done with `reserved` and a fresh number (safe skip).

- **Title (bold 15px, `#1a5276`, top center):** "Reusing Field 3: Old Bytes, New Meaning, Silent Garbage".
- **Row 1 (y=95), label 12px `#444` at x=20:** "number recycled"; blue `#2a78d6` rounded box at x=160 labeled "old bytes: 3 = \"large\"" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "new code: 3 = discount int" with bold 12px red text below it "✗ garbage or decode crash".
- **Row 2 (y=205), label:** "number retired"; blue box at x=160 labeled "old bytes: 3 = \"large\"", 3px arrow to a green `#008300` box at x=360 labeled "reserved 3; discount = 6", then arrow to a green box at x=565 labeled "old tag skipped ✓".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "delete a field, retire its number — reserved keeps it off-limits".
- **Caption (12px `#444`, bottom right):** "flow schematic, order illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); the byte counts are exact for these strings — JSON `{"id":118,"drink":"latte","size":"large","qty":2}` is 49 bytes, protobuf field costs 2 + 7 + 7 + 2 = 18 bytes (63% fewer), drawn at 8px per byte; the coffee-shop order itself is invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
