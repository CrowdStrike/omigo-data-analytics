# Serialization

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Serialization

**Subtitle:** An object living in memory is full of pointers that mean nothing outside the program — serialization flattens it into bytes that can cross a wire or land on disk, and back

## The Order That Has to Leave the Register

**Tags:** `core idea` (blue), `structures to bytes` (green), `pointers` (orange)

- **The object** — the register holds order #4127 in memory: a customer field and a list of two line items
- **The pointers** — "list of items" is really a memory address like 0x7f3a that points somewhere in RAM
- **The problem** — the payment server needs this order, but address 0x7f3a means nothing on another machine
- **Serialize** — walk the object, follow every pointer, and write the values out as one flat run of bytes
- **Deserialize** — the receiver reads the bytes and rebuilds an equivalent object with its own fresh pointers

*Example (italic):* The register serializes order #4127 (Maya: 2 lattes, 1 muffin) into a byte stream, sends it, and the payment server rebuilds the same order in its own memory.

**Key point:** Serialization turns a linked, pointer-filled structure into a self-contained flat sequence of bytes; deserialization is the reverse trip — the pointers never travel, only the values do.

### Visualization (canvas `c1`, 720×300)

Flow diagram: an in-memory object graph on the left, a flat byte stream in the middle, and a rebuilt object graph on the right, with serialize/deserialize arrows between them.

- **Title (bold 15px, `#1a5276`, top center):** "Order #4127: Pointer Graph → Flat Bytes → Pointer Graph".
- **Left graph (register memory, x≈30–210):** blue `#2a78d6` rounded box at (40, 80) labeled "order 4127 · cust ● · items ●" (12px), with 2px blue arrows from the two ● dots to two smaller boxes below: (40, 160) "Maya" and (40, 215) "latte ×2 · muffin ×1"; tiny 11px `#6b7280` address tags "0x7f3a" and "0x91c0" beside the arrows.
- **Middle stream (x≈260–460, y=150):** a single horizontal row of 10 small squares (18px each, 1px `#6b7280` border, fill `rgba(42,120,214,0.15)`) representing the byte stream, 12px `#2c3e50` label "flat bytes — no addresses" beneath at y=185.
- **Right graph (payment server memory, x≈510–690):** mirror of the left graph in green `#008300` boxes/arrows with new address tags "0x22b1", "0x4d08" (11px `#6b7280`).
- **Arrows:** 3px `#1a5276` arrow from left graph to stream labeled bold 12px "serialize" above it; 3px `#1a5276` arrow from stream to right graph labeled bold 12px "deserialize".
- **Section labels (11px `#6b7280`):** "register memory" above left graph, "wire / disk" above stream, "server memory" above right graph.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the values travel; the pointers are rebuilt on arrival".
- **Caption (12px `#444`, bottom right):** "addresses illustrative".

## One Order, Two Encodings: 68 Bytes vs 27

**Tags:** `worked example` (blue), `JSON vs binary` (green)

- **As JSON** — `{"id":4127,"cust":"Maya","items":[["latte",2,450],["muffin",1,325]]}` is exactly 68 bytes
- **The split** — of those 68: field labels 20 bytes, quotes/brackets/commas 21, actual values only 27
- **As binary** — 2-byte id, length-prefixed strings, 1-byte counts and quantities, 2-byte prices in cents
- **Hand-count** — id 2 + "Maya" 5 + count 1 + latte item 9 + muffin item 10 = 27 bytes total
- **The ratio** — the binary order is 27/68 of the JSON one, about 40% of the size, carrying the same facts
- **The trade** — JSON is readable in any text editor; the binary form needs its layout spec to decode

*Example (italic):* Order #4127 costs 68 bytes as JSON but only 27 bytes in the compact binary layout — the missing 41 bytes were labels and punctuation, not data.

**Key point:** Text formats spend most of their bytes naming and quoting the data; binary formats spend nearly all of them on the data — the JSON's 27 value bytes equal the size of the whole binary message (4 B framing + 23 B values).

### Visualization (canvas `c2`, 720×300)

Horizontal stacked bar chart: the same order's byte budget as JSON (three segments) vs compact binary (two segments), at a shared 6-pixels-per-byte scale.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Order: 68 Bytes of JSON, 27 Bytes of Binary".
- **Geometry:** bars start at x=140, 6px per byte, bar height 34px; row labels 12px `#444` right-aligned at x=130; JSON row at y=95, binary row at y=185.
- **JSON bar (total 408px):** yellow `#c98500` segment width 120 (20 bytes, "labels"), mute `#6b7280` segment width 126 (21 bytes, "punctuation"), blue `#2a78d6` segment width 162 (27 bytes, "values"); 11px white segment labels inside, 12px `#444` "68 bytes" at the bar end.
- **Binary bar (total 162px):** aqua `#199e70` segment width 24 (4 bytes, "framing"), green `#008300` segment width 138 (23 bytes, "values"); 12px `#444` "27 bytes" at the bar end.
- **Legend (11px, under each bar):** segment name + byte count, colored to match its segment.
- **Dashed guide:** vertical dashed `#e5e9ef` line at x=140+162 (the 27-byte mark) spanning both rows, 11px `#6b7280` label "27 B" at top.
- **Annotation (bold 13px green `#008300`, near x=380, y=235):** "binary ≈ 40% of the JSON size — same order, fewer bytes".
- **Caption (12px `#444`, bottom right):** "JSON byte count exact for the string shown; binary layout illustrative".

## Every Hop Is a Serialize and a Deserialize

**Tags:** `where it's used` (blue), `pipelines` (green), `hidden cost` (orange)

- **API calls** — every request and response is an object serialized on one side, deserialized on the other
- **Caches** — storing the order in a cache serializes it; every cache hit pays a deserialize to use it
- **Queues** — a message queue only moves bytes, so producers encode and every consumer decodes
- **Files** — CSV, JSON, Parquet: a saved dataset is just serialization with a name and a file extension
- **The bill** — reading 1M order records: parsing JSON takes 62s, decoding binary 18s, summing totals 9s
- **The surprise** — the JSON pipeline spends about 7× longer unpacking bytes than doing the actual math

*Example (italic):* A nightly revenue job over 1M orders spends 62 of its 71 seconds parsing JSON and only 9 seconds adding up the money.

**Key point:** In data pipelines the decode step is often the dominant cost — choosing the wire and file format is choosing how much CPU every downstream reader will burn before it does any real work.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: seconds to process 1M order records, comparing JSON parsing, binary decoding, and the actual computation.

- **Title (bold 15px, `#1a5276`, top center):** "Reading 1M Orders: Unpacking the Bytes vs Doing the Math".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; y = seconds 0 to 70, gridlines `#e5e9ef` at 20/40/60 with 12px `#444` labels.
- **Bars (width 110, centered at x = 190, 380, 570):** "parse JSON" orange `#d95926` height for 62s, "decode binary" blue `#2a78d6` height for 18s, "sum the totals" green `#008300` height for 9s; bold 13px value labels "62s" / "18s" / "9s" above each bar in the bar's color; 12px `#444` category labels below the baseline.
- **Dashed guide:** horizontal dashed `#6b7280` (dash 4/3) line at the 9s level across the plot, 11px `#6b7280` label "the actual work" at its right end.
- **Annotation (bold 13px orange `#d95926`, near x=190, y=60):** "7× more time parsing than computing".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Bytes Written Today Aren't Bytes Readable Tomorrow

**Tags:** `common mistake` (red), `versions` (orange), `portability` (blue)

- **The assumption** — "it's just bytes, anything can read it later" — only true if the layout is agreed on
- **Language lock-in** — pickle-style formats snapshot one language's objects; other languages can't rebuild them
- **Version drift** — v2 of the app renames a field or changes a type, and yesterday's bytes stop parsing
- **Code execution** — some object-snapshot formats run code while loading, so decoding untrusted bytes is unsafe
- **The fix** — a language-neutral format with an explicit, versioned schema that both sides check against

*Example (italic):* Orders archived as language-native object snapshots become unreadable when the analytics team tries to load them from a different language a year later.

**Common mistake:** Treating serialized bytes as self-explanatory. Bytes only mean something under a spec — pin the format and version it, or the writer's upgrade quietly breaks every reader.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: order bytes written with a language-locked snapshot format (readers fail) vs a language-neutral versioned schema (readers succeed).

- **Title (bold 15px, `#1a5276`, top center):** "Who Can Read the Bytes a Year Later?".
- **Row 1 (y=95), label 12px `#444` at x=20:** "object snapshot"; blue `#2a78d6` rounded box at x=160 labeled "order bytes (lang A objects)" (12px), 3px arrows to two boxes at x=430: red `#e74c3c` box "lang B reader — can't rebuild" with bold 12px red "✗", and red box at y offset +48 "lang A v2 — field renamed, fails" with bold 12px red "✗".
- **Row 2 (y=215), label:** "versioned schema"; blue box at x=160 labeled "order bytes + schema v1", 3px arrow to a green `#008300` box at x=430 labeled "any language, checks v1 — reads" with bold 12px green "✓".
- **Box style:** 170–200px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=278):** "bytes are portable only when the spec travels with them".
- **Caption (12px `#444`, bottom right):** "scenario illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all numbers are the hardcoded values above (no randomness); the JSON byte count 68 is exact for the literal string shown, with segments labels 20 / punctuation 21 / values 27; the binary layout totals 27 bytes (framing 4 + values 23) at 6px per byte in c2; pipeline timings 62s / 18s / 9s in c3 are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
