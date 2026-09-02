# Protocol Buffers / Thrift

**Page type:** detail page (single card-section, two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Protocol Buffers / Thrift

**Subtitle:** Binary schema-first serialization with code generation.

## Intro callout

**Core trade-off:** Compact encoding and safe schema evolution via field numbering — but the data is not human-readable and requires tooling to inspect.

## How It Works

Write a schema file (`.proto` or `.thrift`), run a code generator, get type-safe serialization in your target language. Protocol Buffers (Google) and Thrift (Facebook/Apache) solve the same problem.

- **Schema:** External file, compiled into code — required to decode
- **Layout:** Binary; field *numbers* on the wire, never names
- **Size:** 5-10× smaller than JSON
- **Best for:** Inter-service RPC where both ends evolve independently

**How it's decoded:** Each field is written as a varint tag combining the field number and a wire type, followed by the value. The reader walks tag by tag: field 1 is a string of length 5, field 2 is a varint, and so on. A field number it doesn't recognize is skipped by wire type — that skip is the whole schema-evolution story. Old code reading new data skips unknown fields; new code reading old data fills in defaults. Names never touch the wire, so renaming `email` to `contact_email` is free — but reuse a field *number* and old data decodes into the wrong field.

**Key point (red-left-border callout):** **The rule that makes it work:** Field numbers are permanent identity. Never reuse, never renumber. Everything else — independent deployment, backward and forward compatibility — follows from that one discipline.

*Example: gRPC between microservices, mobile payloads on cellular, high-throughput event pipelines — most large tech companies use these internally for nearly everything.*

### Visualization (canvas `c1`, 720×300)

Two-panel diagram: a `.proto` schema box on the left and the corresponding wire-format byte cells on the right, with a schema-evolution annotation.

- **Title (bold 14px system-ui, `#1a5276`, centered at y=25):** "Protobuf Wire Format — Numbers on the Wire, Names in the Schema".
- **Schema panel (left):** light gray box (`#f8f9fa` fill, `#ccc` border) at x=40, y=55, 240×130, containing 12px monospace lines:
  - `message User {` in `#1a5276`
  - `  string name  = 1;` in `#333`
  - `  int64  id    = 2;` in `#333`
  - `  string email = 3;` in `#333`
  - `  string dept  = 5; //new` in `#27ae60`
  - `}` in `#1a5276`
  - Caption below in 11px `#666`, centered: ".proto schema — compiled into code, never shipped"
- **Wire bytes (right):** starting at x=330, y=62, four rows of cells 30px tall with 6px gaps. Each row: a colored tag cell (130px wide, white 10px text), an optional length cell (46px, fill `rgba(26,82,118,0.15)`, `#333` 10px monospace), and a value cell (130px, fill `#f0f0f0`, `#ccc` border, `#333` 11px monospace):
  1. tag "tag: field 1, wire 2" (`#1a5276`), len "len 5", value `"Alice"`
  2. tag "tag: field 2, wire 0" (`#e67e22`), no len cell, value `varint 12345`
  3. tag "tag: field 3, wire 2" (`#8e44ad`), len "len 15", value `"alice@ex.com"`
  4. tag "tag: field 5, wire 2" (`#27ae60`), len "len 5", value `"Eng"`
- **Old-reader annotation:** dashed green rectangle (`#27ae60`, dash 3/3, width 1.5) around the field-5 row, with green 11px label below: "old reader: unknown field 5 → skip by wire type".
- **Bottom note (red `#e74c3c`, 11px, centered at bottom):** "names never appear in the bytes — rename freely; reuse a NUMBER and old data decodes into the wrong field".

## Regeneration instructions

- **Layout:** single `.card-section` with h2 "How It Works" (1.3rem `#1a5276`, 2px `#2980b9` bottom border), containing a `table.layout` (100% width, border-collapse) with one `<tr>`: left `td.text-col` (45%) holds paragraph, `<ul>` bullets, a `.key-point` div, and a `.example` paragraph; right `td.viz-col` (55%) holds the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro-callout` background `#f8f9fa`, left border 3px solid `#2980b9`, padding 10px 14px, 0.93rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem. `code` background `#f0f4f8`, padding 2px 6px, radius 3px. Canvas `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple accent.
- **Canvas:** intrinsic 720×300; use `window.devicePixelRatio` scaling (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper.
- In regenerated HTML, any card links use `.html` extensions.
