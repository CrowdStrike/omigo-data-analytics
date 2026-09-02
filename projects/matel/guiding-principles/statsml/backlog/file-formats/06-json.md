# JSON

**Page type:** detail page (single card-section with two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** JSON

**Subtitle:** Smallest format carrying nested data.

## Callout (intro)

**Core trade-off:** Just enough types to be universally useful — but no dates, no comments, and precision loss past 53 bits silently corrupts large IDs.

## How It Works

JavaScript Object Notation — six types (string, number, boolean, null, array, object) in a spec that fits on one page. The dominant interchange format of the web since the mid-2010s.

- **Schema:** None built-in (JSON Schema is a separate add-on)
- **Layout:** Nested text, maps to dicts/objects in every language
- **Numbers:** All IEEE 754 doubles — no int/float distinction
- **Best for:** Web APIs, config, document stores

**How it's parsed:** A recursive-descent parser over a grammar so small it fits on json.org's single page — that simplicity is why every language has a native parser and why it displaced XML. But the number rule hides a trap: all numbers become IEEE 754 doubles, which hold integers exactly only up to 2^53 (rendered as superscript in HTML). A 64-bit database ID above that threshold survives serialization but rounds silently on parse — which is why Twitter's API ships every ID twice, as `id` and `id_str`.

**Failure mode:** Silent precision loss. `9007199254740993` parses as `9007199254740992` with no error. Dates have no type at all — they're strings the consumer must know to parse. No comments allowed. *(styled as key-point callout, red left border)*

*Example: Every REST API body, package.json, MongoDB documents, localStorage/Redux state, microservice messages.*

### Visualization (canvas `c1`, 720×300)

Syntax-colored JSON document with annotations on the precision trap and the missing date type, plus a color legend.

- **Title (bold 14px, top center, `#1a5276`):** "JSON — Six Types, One Silent Trap".
- **Document listing (13px monospace, starting x=50, y=58, 24px line height; multi-color tokens per line):**
  1. `{` in `#333`
  2. `  "user_id": ` in `#1a5276`, `9007199254740993` in `#e74c3c`, `,` in `#333`
  3. `  "name": ` in `#1a5276`, `"Alice Chen"` in `#27ae60`, `,` in `#333`
  4. `  "roles": ` in `#1a5276`, `["admin", "analyst"]` in `#27ae60`, `,` in `#333`
  5. `  "created_at": ` in `#1a5276`, `"2024-03-15T10:22:01Z"` in `#e67e22`, `,` in `#333`
  6. `  "deleted_at": ` in `#1a5276`, `null` in `#999`
  7. `}` in `#333`
- **2^53 annotation (red `#e74c3c`, 11px, with a 1.5px red pointer line from the user_id value):** "> 2^53 — parses as …740992, no error" plus a second line "IEEE 754 double: exact integers end at 9007199254740992".
- **Date annotation (orange `#e67e22`, 11px, with a 1.5px orange pointer line from the created_at value):** "just a string — no date type exists".
- **Type legend strip (12px system-ui, one row near bottom left, each entry prefixed "■ "):** "string" `#27ae60`, "number (double!)" `#e74c3c`, "date-as-string" `#e67e22`, "null" `#999`, "key" `#1a5276`.
- **Bottom note (centered, `#666`, 11px):** "the whole grammar fits on one page — that simplicity, not richness, is why it won the web".

## Regeneration instructions

- **Layout:** single-page detail doc: h1 with `2px solid #2980b9` bottom border, `.subtitle` paragraph, `.intro-callout` div, then one `.card-section` containing an h2 (also `2px solid #2980b9` bottom border) and a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) holding intro paragraph, `<ul>` bullets, "How it's parsed" paragraph (2^53 uses `<sup>53</sup>`), `.key-point` div, `.example` paragraph; right `<td class="viz-col">` (55%) holding the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `.intro-callout` background `#f8f9fa`, left border `3px solid #2980b9`, padding 10px 14px, 0.93rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `code` background `#f0f4f8`, 2px 6px padding, 3px radius. Canvas `width: 100%`, `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray `#999` (null).
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has none).
