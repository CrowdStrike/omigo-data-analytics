# Document Stores

**Page type:** detail page (tutorial: 4 `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%; section 1's viz column has a `pre.payload` code block above its canvas)
**HTML title tag:** Document Stores

**Subtitle:** Keep the whole product — specs, variants, reviews — as one nested JSON document instead of slicing it across five tables, and read it back in one fetch

## Section 1: One Product, One Document

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — an online shop stores product 88, a desk lamp, with all its detail
- **The relational way** — 5 tables: products, specs, variants, reviews, images
- **The document way** — one JSON blob holds the lamp and everything nested inside it
- **Nesting is allowed** — lists inside objects inside lists: reviews live IN the product
- **Still queryable** — unlike a key-value blob, the store can filter on fields inside

*Example (italic):* The document below is the entire lamp — what a shop's product page needs, in one piece.

**Key point:** A document store saves data the way your code already shapes it — one nested object per thing — instead of shredding it into flat tables.

Payload block above the canvas (`pre.payload`, monospace, left border `#1a5276`):

```
// collection "products", document _id: 88  (illustrative)
{
  "_id": 88,
  "name": "desk lamp",
  "price": 34.00,
  "specs":    { "wattage": 9, "color": "black", "height_cm": 42 },
  "variants": [ { "sku": "LAMP-BK", "stock": 12 },
                { "sku": "LAMP-WH", "stock":  0 } ],
  "reviews":  [ { "user": 1042, "stars": 5, "text": "bright!" },
                { "user": 7781, "stars": 3, "text": "wobbly base" } ],
  "images":   [ "88-front.jpg", "88-side.jpg", "88-on.jpg", "88-box.jpg" ]
}
```

### Visualization (canvas `c1`, 720×300)

Two-panel diagram — one nested document vs five flat tables — split by a vertical dashed divider `#bdc3c7` (dash 4/3) at x=300.

- **Title (bold 15px `#1a5276`, top center):** "The Same Lamp, Stored Two Ways".
- **Left panel:** one large blue `#2a78d6` box at (45,55), 210×185, fill `rgba(42,120,214,0.10)`, labeled bold "product 88 \"desk lamp\"". Inside, four nested 180×26 white boxes (11px monospace labels in their stroke color, 36px apart starting y=90):
  - `specs {wattage, color, ...}` — aqua `#199e70`
  - `variants [LAMP-BK, LAMP-WH]` — violet `#4a3aa7`
  - `reviews [5★, 3★]` — magenta `#d55181`
  - `images [4 urls]` — yellow `#c98500`
  - Caption bold 12px blue centered: "1 document — nesting keeps it together".
- **Right panel:** five mini tables (95px wide, header label bold 11px in table color, one thin `#e5e9ef` row box per row): products (1 row, blue, at 490,55), specs (3 rows, aqua, at 340,120), variants (2 rows, violet, at 450,120), reviews (2 rows, magenta, at 560,120), images (4 rows, yellow, at 450,195). Gray `#b9c2cc` FK lines connect specs/variants/reviews up to products, and images up to variants. Gray 11px note: "product_id = 88 in every child row"; caption bold 12px violet: "5 tables, 12 rows — joins put the lamp back together".
- **Bottom caption (bold 13px orange `#d95926`, centered):** "same information, opposite layouts".

## Section 2: Loading the Lamp Page: One Fetch vs Five Joins

**Tags:** `worked example` (green)

- **Document store** — `find(_id: 88)`: 1 lookup returns the whole lamp
- **Relational** — read products, then join specs, variants, reviews, images: 5 table reads
- **Row counts** — 1 product row + 3 spec rows + 2 variants + 2 reviews + 4 images = 12 rows
- **Reassembly** — those 12 flat rows must be stitched back into one object in code
- **The document skips both** — no join, no stitching: it was stored pre-assembled

*Example (italic):* The product page shows one lamp — exactly the unit the document already is.

**Key point:** Documents win when you read whole things by id; tables win when you ask across things — "average stars per brand" is easy in SQL, clumsy here.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of reads needed per approach.

- **Title (bold 15px `#1a5276`, top center):** "Reads Needed to Render the Product Page for Lamp 88".
- **Bars** (plot x=270, width 320, scale max 5; 20px tall at 0.7 alpha, 32px row pitch from y=52 with a 14px gap after the first row; monospace 12px row labels at x=30; bold value labels in bar color after each bar):
  - `document: find(_id: 88)` = 1 (labeled "1 fetch"), green `#008300`, note "whole lamp, pre-assembled"
  - `SQL: products row` = 1 row, violet `#4a3aa7`
  - `SQL: + specs` = 3 rows, violet
  - `SQL: + variants` = 2 rows, violet
  - `SQL: + reviews` = 2 rows, violet
  - `SQL: + images` = 4 rows, violet, note "12 rows, stitched in code"
  - Notes in gray 11px after the value labels.
- **Bracket:** violet 2px bracket left of the five SQL bars with rotated bold 11px violet label "5 table reads + 4 joins".
- **Caption (bold 13px orange, bottom center):** "read-by-id workloads: the document is one fetch because it was stored assembled".

## Section 3: Where a Data Scientist Meets Documents

**Tags:** `where it's used` (blue)

- **API responses** — most web APIs hand you JSON; a document store keeps it as-is
- **Event logs** — clickstream events with different fields per event type fit naturally
- **Catalogs & profiles** — products, users, listings: read-whole-thing-by-id workloads
- **Your job: flattening** — models want flat rows, so nested docs get unnested first
- **One doc, many rows** — lamp 88 explodes into 2 review rows: know your row unit

*Example (italic):* Flattening 3 products with 2, 0, and 3 reviews yields 5 review rows — not 3.

**Key point:** Unnesting changes the unit of analysis — a per-review table over-represents heavily-reviewed products, the classic fan-out trap.

### Visualization (canvas `c3`, 720×300)

Unnesting diagram: 3 document boxes → arrow → flat 5-row review table.

- **Title (bold 15px `#1a5276`, top center):** "Unnesting Documents Changes the Row Unit".
- **Left:** three 190×48 document boxes at x=50 (y=60, 62px pitch), fill `#f4f6f8`, 2px strokes: product 88 (blue `#2a78d6`, `reviews: [★ ★]`), product 89 (aqua `#199e70`, `reviews: []`), product 90 (violet `#4a3aa7`, `reviews: [★ ★ ★]`); bold monospace id label in the box color, review list in `#444` 11px monospace. Gray 12px caption below: "3 documents".
- **Arrow:** thick orange `#d95926` arrow (2.5px, triangular head) from x=265 to x=350 at y=150, labeled bold "unnest".
- **Right:** flat table at (390,58), 250 wide, rows 28px, header bold `#1a5276` "product_id      stars"; five rows (alternating `#fff`/`#f4f6f8`, stroked in the source-product color): 88/5★ (blue), 88/3★ (blue), 90/4★ (violet), 90/4★ (violet), 90/2★ (violet). Gray 12px caption: "5 review rows — product 89 vanished, product 90 appears 3x".
- **Bottom caption (bold 13px red `#e74c3c`, centered):** "a per-review average now weights product 90 three times — fan-out bias".

## Section 4: Flexible Schema Cuts Both Ways

**Tags:** `common mistake` (red), `watch out` (orange)

- **No gatekeeper** — nothing checks field names, so every writer invents their own
- **Three spellings** — across 10,000 products: `color`, `colour`, and `clr` all mean the same
- **Silent misses** — a query on `specs.color` quietly skips the other two spellings
- **Missing vs absent** — no `stock` field: is it zero, unknown, or renamed? The store can't say
- **Types drift too** — `price: 34.00` in one doc, `price: "34 USD"` in another

*Example (italic):* A "flexible schema" really means the schema moved out of the database and into everyone's heads.

**Common mistake:** Trusting a field name. Before analysis, profile which fields actually appear and how often — the counts below are the first query to run.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of field-name variants.

- **Title (bold 15px `#1a5276`, top center):** "One Concept, Four States — 10,000 product docs (illustrative)".
- **Axis:** x from 0 to 7,000 documents, gridlines every 1,750 (light `#ccc` verticals, gray tick labels 0/1750/3500/5250/7000), axis label "documents"; plot x=190, width 380, rows from y=62, row height 46.
- **Bars** (24px tall at 0.7 alpha; monospace 12px row labels at x=30; bold count label in bar color, then gray 11px note):
  - `specs.color` = 6,100, blue `#2a78d6`, note "the \"official\" field"
  - `specs.colour` = 2,400, yellow `#c98500`, note "UK-team writer"
  - `specs.clr` = 900, orange `#d95926`, note "mobile app v1"
  - `no field at all` = 600, gray `#8b95a1`, note "zero? unknown? renamed?"
- **Caption (bold 13px red `#e74c3c`, bottom center):** "a query on specs.color silently misses 3,900 of 10,000 products — 39%".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem `#1a5276`, 2px solid `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `.text-col` (50%) holding `.tags`, a `<ul>` of bullets (inline `<code>` allowed), `.example` italic line, and `.key-point` callout; `.viz-col` (50%) holding the canvas (section 1 places a `pre.payload` block before its canvas).
- **Text styles:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; bullets 0.92rem with bold lead terms `<b>` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem; `pre.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em; inline `code` monospace 0.9em on `#f4f6f8` with 3px radius.
- **Tag pills:** `.tag` inline pill, 0.72rem bold, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** 720×300 intrinsic, CSS `width:100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette (JS `P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
