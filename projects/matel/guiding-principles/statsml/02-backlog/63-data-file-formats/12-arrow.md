# Arrow

**Page type:** detail page (single card-section, two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Arrow

**Subtitle:** In-memory columnar format for zero-copy sharing.

## Intro callout

**Core trade-off:** Zero-copy data sharing between tools and languages eliminates serialization overhead — but the columnar memory layout has overhead for small datasets, and Arrow is not a persistence format.

## How It Works

A language-agnostic specification for how columnar data is laid out in RAM, so any tool (Pandas, DuckDB, Spark, Polars, R) can read the same memory without copying or converting. A memory format, not a file format — Arrow IPC and Feather provide serialization when needed.

- **Schema:** Carried with each record batch
- **Layout:** Contiguous typed buffers per column + validity bitmap for nulls; strings as offsets + data buffers
- **Alignment:** SIMD- and cache-line-friendly, uncompressed by design
- **Best for:** Pipelines where the tool-boundary tax is the bottleneck

**How sharing works:** Traditionally, DuckDB handing a result to Pandas means serialize → deserialize → copy strings → rebuild — O(n) work and a memory spike at every library boundary, paid again at scikit-learn and again at Polars. With Arrow both sides agree on the memory layout itself, so the handoff is an O(1) pointer exchange: Pandas reads the *same* buffers DuckDB produced. A 10M-row handoff drops from seconds and hundreds of MB to effectively free.

**Key point (red-left-border callout):** **What it is not:** Not storage — that's Parquet's job (compressed, on disk); Arrow is its uncompressed in-RAM counterpart. The two are designed to convert cheaply into each other.

*Example: DuckDB ↔ Pandas ↔ Polars interop, DataFusion/Velox engine internals, Flight RPC transfers, Python ↔ R shared memory.*

### Visualization (canvas `c1`, 720×300)

Two-row comparison diagram: a copy-at-every-boundary pipeline on top versus one shared Arrow buffer on the bottom.

- **Title (bold 14px system-ui, `#1a5276`, centered at y=25):** "Arrow — Copy at Every Boundary vs One Shared Buffer".
- **Top row ("without Arrow:", red `#e74c3c` 11px label at left):** four tool boxes `DuckDB`, `Pandas`, `sklearn`, `Polars` (110×40, white fill, `#1a5276` 1.5px border, bold 12px `#1a5276` labels), horizontally centered with 66px gaps. Between adjacent boxes: red (`#e74c3c`) arrows with two-line 9px labels "serialize +" / "copy O(n)".
  - Red caption below (11px, centered): "the tax is paid 3 times — 10M rows: ~2.1s, ~800MB peak per boundary".
- **Bottom row ("with Arrow:", green `#27ae60` 11px label at left):** the same four tool boxes (110×28, white fill, `#1a5276` border, bold 11px labels) each with a short green downward pointer arrow into one shared memory block spanning the full row width (44px tall, fill `rgba(39,174,96,0.15)`, border 2px `#27ae60`).
  - Inside the block, three buffer cells (fill `rgba(39,174,96,0.35)`, 10px monospace `#1a5276` labels): "col: id (int64 buffer)", "col: name (offsets+data)", "validity bitmaps".
  - Bold green caption below the block (11px, centered): "ONE Arrow record batch in shared memory".
- **Bottom note (gray `#666`, 11px, centered):** "every tool reads the SAME buffers — O(1) pointer handoff, zero additional memory".

## Regeneration instructions

- **Layout:** single `.card-section` with h2 "How It Works" (1.3rem `#1a5276`, 2px `#2980b9` bottom border), containing a `table.layout` (100% width, border-collapse) with one `<tr>`: left `td.text-col` (45%) holds paragraph, `<ul>` bullets, a `.key-point` div, and a `.example` paragraph; right `td.viz-col` (55%) holds the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro-callout` background `#f8f9fa`, left border 3px solid `#2980b9`, padding 10px 14px, 0.93rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem. `code` background `#f0f4f8`, padding 2px 6px, radius 3px. Canvas `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; translucent green fills `rgba(39,174,96,0.15)`/`rgba(39,174,96,0.35)` for shared-memory blocks.
- **Canvas:** intrinsic 720×300; use `window.devicePixelRatio` scaling (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper.
- In regenerated HTML, any card links use `.html` extensions.
