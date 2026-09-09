# Avro

**Page type:** detail page (single card-section, two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Avro

**Subtitle:** Binary format where schema travels with the data.

## Intro callout

**Core trade-off:** Self-describing files that any consumer can decode without external documentation — but schema management becomes its own distributed systems problem at scale.

## How It Works

Row-oriented binary from the Hadoop ecosystem. Unlike Protobuf, the writer's schema is embedded in the file header as JSON — any reader can decode the file with no external documentation.

- **Schema:** Embedded in the file, resolved against a reader-side schema
- **Layout:** Header + sync-marker-separated data blocks
- **Evolution:** Reader/writer schema *resolution*, by field name
- **Best for:** Kafka messages, long-lived stored data

**How it's decoded:** The reader parses the writer's schema from the header, then *resolves* it against its own expected schema by field name: fields added with defaults are filled in, removed fields are ignored, int promotes to long, float to double. A file written with schema v1 stays readable by code expecting v3 without rewriting old data. Sync markers between blocks let a distributed job split the file and decode blocks in parallel — the binary analogue of JSONL's newline splits.

**Key point (red-left-border callout):** **Trade-off vs Protobuf:** Protobuf optimizes the message (schema compiled away, smallest bytes); Avro optimizes the *file* (schema travels along, decodable forever). At Kafka scale, per-message schemas are too heavy, so a schema registry replaces the header — and schema management becomes its own problem.

*Example: Kafka + Confluent Schema Registry, Hadoop storage, pipelines where producers and consumers deploy independently.*

### Visualization (canvas `c1`, 720×300)

Two-part diagram: a horizontal file-layout strip (header + sync markers + data blocks) on top, and a writer-vs-reader schema resolution panel below.

- **Title (bold 14px system-ui, `#1a5276`, centered at y=25):** "Avro File — The Schema Travels With the Data".
- **File layout strip:** contiguous segments starting at x=50, y=55, height 60, each with white 2px borders and white labels:
  - "header:" / "writer schema (JSON)" (200px wide, fill `#1a5276`; second line in 10px monospace)
  - "sync" (34px, fill `#e67e22`)
  - "data block 1" (130px, fill `rgba(26,82,118,0.45)`)
  - "sync" (34px, `#e67e22`)
  - "data block 2" (130px, `rgba(26,82,118,0.45)`)
  - "sync" (34px, `#e67e22`)
  - "…" (40px, `rgba(26,82,118,0.25)`)
- **Parallel-split annotation (orange `#e67e22`, 11px, centered below strip):** "sync markers = split points for parallel decode (binary JSONL)".
- **Schema resolution panel** below the strip:
  - Left heading (bold 12px, `#1a5276`): "writer schema (in file, v1)"; right heading (bold 12px, `#27ae60`) at +400px: "reader schema (in code, v3)".
  - Writer column (11px monospace `#333`): `id:    long`, `name:  string`, `email: [null,string]`.
  - Reader column: `id:    long`, `name:  string` in `#333`; `dept:  string = "?"` in `#27ae60`.
  - Purple (`#8e44ad`) horizontal arrow between the two columns, width 1.5, with 10px label above it: "resolve by NAME".
- **Bottom note (gray `#666`, 11px, centered):** "added field → default fills in · removed field → ignored · int→long, float→double promote · v1 file readable by v3 code, no rewrite".

## Regeneration instructions

- **Layout:** single `.card-section` with h2 "How It Works" (1.3rem `#1a5276`, 2px `#2980b9` bottom border), containing a `table.layout` (100% width, border-collapse) with one `<tr>`: left `td.text-col` (45%) holds paragraph, `<ul>` bullets, a `.key-point` div, and a `.example` paragraph; right `td.viz-col` (55%) holds the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro-callout` background `#f8f9fa`, left border 3px solid `#2980b9`, padding 10px 14px, 0.93rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem. `code` background `#f0f4f8`, padding 2px 6px, radius 3px. Canvas `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple accent, `rgba(26,82,118,0.35)`-family translucent blues for block fills.
- **Canvas:** intrinsic 720×300; use `window.devicePixelRatio` scaling (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper.
- In regenerated HTML, any card links use `.html` extensions.
