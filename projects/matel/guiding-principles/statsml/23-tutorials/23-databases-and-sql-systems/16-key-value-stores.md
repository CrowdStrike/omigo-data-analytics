# Key-Value Stores

**Page type:** detail page (tutorial: 4 `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%; section 2 adds a `pre.payload` code block under its canvas)
**HTML title tag:** Key-Value Stores

**Subtitle:** A giant dictionary: hand it a key like `user:1042:cart`, get the stored blob back in a microsecond — and that's the whole feature list

## Section 1: The Session Cache: One Key, One Blob

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — a shop keeps each visitor's cart in a cache while they browse
- **The key** — a string you invent: `user:1042:cart` names one user's cart
- **The value** — whatever blob you saved under it: here a small JSON cart
- **The lookup** — hand over the exact key, get the exact blob back — nothing else
- **A dictionary** — it works like a Python dict or a hash map, just on a server

*Example (italic):* GET `user:1042:cart` returns the cart blob; GET `user:1042:cort` (typo) returns nothing.

**Key point:** The store never looks inside the value — it only matches the key, which is exactly why it is so fast.

### Visualization (canvas `c1`, 720×300)

Dictionary diagram: key boxes on the left, arrows to value boxes on the right.

- **Title (bold 15px `#1a5276`, top center):** "The Store Is a Giant Dictionary: exact key in, stored blob out".
- **Column headers (gray `#6b7280` 12px, left-aligned):** "KEY (a string you invent)" over the key column (x=40), "VALUE (opaque blob — never inspected)" over the value column (x=380).
- **Four rows** (starting y=58, row height 48; key boxes 220×32 at x=40, value boxes 300×32 at x=380, connected by arrows with triangular heads):
  - `user:1042:cart` → `{ items: [mug, socks], ... }` — blue `#2a78d6`, highlighted "hot" row: key fill `rgba(42,120,214,0.16)`, 2.5px strokes, colored arrow
  - `user:1042:session` → `{ token: "(masked)", ttl: 1800 }` — aqua `#199e70`
  - `user:7781:cart` → `{ items: [lamp], ... }` — violet `#4a3aa7`
  - `views:home:today` → `184203` — yellow `#c98500`
  - Non-hot rows: key fill `#f4f6f8`, 1.2px strokes, gray `#b9c2cc` arrows; key text bold 13px monospace in the row color; value text 12px monospace `#444` on white.
- **Caption (bold 13px blue, bottom center):** "GET user:1042:cart follows one arrow — it never reads the other rows".

## Section 2: A Shopping Session in Five Operations

**Tags:** `worked example` (green)

- **Three verbs only** — `PUT` (save), `GET` (read), `DELETE` (remove); real stores add a few atomic helpers
- **Step 1** — user 1042 adds a mug: `PUT user:1042:cart` with a 1-item blob
- **Step 2** — adds socks: `PUT` the same key again, the new blob replaces the old
- **Steps 3–4** — page reload, then checkout page: each `GET user:1042:cart` returns the 2-item blob
- **Step 5** — order placed: `DELETE user:1042:cart` — the key is gone
- **Overwrite, not edit** — to change one item you read the blob, change it, PUT it whole

*Example (italic):* The value below is what step 3 returns — the store treats it as opaque bytes.

**Key point:** Every operation touches exactly one key, so each one costs about the same tiny amount — lookups stay nearly constant as data grows.

### Visualization (canvas `c2`, 720×300)

Timeline of five operations on one key.

- **Title (bold 15px `#1a5276`, top center):** "One Session of user:1042:cart — every step touches one key".
- **Timeline:** horizontal 2px `#b9c2cc` line at y=130 from x=50 to x=670; five evenly spaced dots (radius 7) from x=70 to x=650:
  | Op | Label (gray, above) | State (monospace `#444`, below) | Dot color |
  |----|----|----|----|
  | PUT | add mug | `[mug]` | green `#008300` |
  | PUT | add socks | `[mug, socks]` | green `#008300` |
  | GET | page reload | `[mug, socks]` | blue `#2a78d6` |
  | GET | checkout page | `[mug, socks]` | blue `#2a78d6` |
  | DELETE | order placed | `(key gone)` | orange `#d95926` |
  - Op names bold 13px monospace above each dot; gray left-aligned note under the line: "value stored under the key after each step:".
- **Annotation:** dashed green leader (dash 4/3) dropping from the second PUT dot, with bold green centered text: "second PUT replaces the whole blob — there is no \"append one item\"".
- **Caption (bold 13px blue, bottom center):** "each operation: ~0.2 ms, whether the store holds 10 keys or 2,000,000".

Payload block under the canvas (`pre.payload`, monospace, left border `#1a5276`):

```
// value stored under key "user:1042:cart"  (illustrative)
{
  "items": [
    { "sku": "MUG-07",  "name": "coffee mug", "qty": 1, "price": 12.50 },
    { "sku": "SOCK-3",  "name": "wool socks", "qty": 2, "price":  9.00 }
  ],
  "updated_at": "2026-08-24T10:41:07Z"
}
```

## Section 3: Where a Data Scientist Meets One

**Tags:** `where it's used` (blue), `speed` (orange)

- **Feature store** — key `user:1042:features` serves a model's inputs at predict time
- **Session state** — carts, login tokens, "recently viewed" — anything per-user and hot
- **Counters** — page views, rate limits: read-bump-write one key millions of times
- **Cache in front of SQL** — the answer to a slow query gets saved under a key for reuse
- **The speed gap** — one key lookup ~0.2 ms vs a 5-table SQL join ~20 ms — about 100x

*Example (italic):* A fraud model with a 50 ms budget can't wait 20 ms for SQL — it reads precomputed features by key.

**Key point:** When a model must answer in milliseconds, its features are almost always sitting in a key-value store, precomputed and keyed by user id.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing lookup latencies.

- **Title (bold 15px `#1a5276`, top center):** "Fetching a User's Model Features at Predict Time (illustrative)".
- **Axis:** x from 0 to 25 ms, gridlines every 5 ms (light `#ccc` verticals, gray tick labels 0/5/10/15/20/25), axis label "milliseconds" centered below; plot x starts at 250, width 380, rows start y=70, row height 62.
- **Bars** (28px tall, fill at 0.75 alpha, bold value label in bar color to the right):
  - "key-value GET" (sub-label `user:1042:features`) = 0.2 ms, green `#008300` (bar clamped to minimum 4px width)
  - "SQL, 5-table join" (sub-label "computed on the fly") = 20 ms, violet `#4a3aa7`
  - Row labels bold 13px `#333` at x=30 with 11px gray sub-labels.
- **Annotations:** bold 14px orange `#d95926` centered at y=250: "~100x faster — that gap is why online features live behind a key"; gray 12px at y=275: "the join still runs — once, in a nightly batch that PUTs the result under the key".

## Section 4: No Queries, No Joins — the Wrong-Tool Trap

**Tags:** `common mistake` (red), `watch out` (orange)

- **The question** — "which carts contain SKU MUG-07?" sounds easy — it isn't here
- **No WHERE clause** — the store matches keys only; it cannot search inside values
- **The only way** — fetch all 2,000,000 cart blobs and open each one yourself
- **The cost** — 2M GETs instead of 1: minutes of work for a one-line SQL question
- **The rule** — if you don't know the exact key, a key-value store has nothing for you

*Example (italic):* In SQL it's `WHERE sku = 'MUG-07'`; in a key-value store it's a scan of every value.

**Common mistake:** Needing to search, filter, or join values means you picked the wrong tool — copy the data into a database built for queries instead.

### Visualization (canvas `c4`, 720×300)

Two-panel heat-grid comparison split by a vertical dashed divider `#bdc3c7` (dash 4/3) at x=360.

- **Title (bold 15px `#1a5276`, top center):** "\"Which carts contain MUG-07?\" — blobs touched to answer".
- **Grids:** each panel a 12×5 grid of 20px squares (4px gaps); cold cells `rgba(42,120,214,0.18)`, touched cells `rgba(217,89,38,0.75)`.
- **Left panel** (heading bold 13px green centered at x=185): "know the key: GET one blob" — grid at (40,70) with exactly one hot cell (row 2, col 5). Bold green "1 read of 2,000,000"; gray 12px "(each square = many carts)".
- **Right panel** (heading bold 13px red `#e74c3c` centered at x=545): "search inside values: open every blob" — grid at (400,70) with ALL cells hot. Bold red "2,000,000 reads, then filter yourself"; gray 12px "no index, no WHERE, no shortcut".
- **Caption (bold 13px orange, bottom center):** "the store finds keys, never values — needing the right side means switching tools".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem `#1a5276`, 2px solid `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem; inline `<code>` styled monospace on `#f4f6f8`), then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `.text-col` (50%) holding `.tags`, a `<ul>` of bullets (inline `<code>` allowed), `.example` italic line, and `.key-point` callout; `.viz-col` (50%) holding the canvas (and in section 2, a `pre.payload` block after it).
- **Text styles:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; bullets 0.92rem with bold lead terms `<b>` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem; `pre.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em; inline `code` monospace 0.9em on `#f4f6f8` with 3px radius.
- **Tag pills:** `.tag` inline pill, 0.72rem bold, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** 720×300 intrinsic, CSS `width:100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette (JS `P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: realistic credential strings on this page were converted to generic placeholders — for illustration only, and to avoid false positives from secret scanners."
