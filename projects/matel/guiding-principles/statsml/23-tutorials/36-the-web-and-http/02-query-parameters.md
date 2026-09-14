# Query Parameters

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Query Parameters

**Subtitle:** Everything after the `?` in a web address is key=value data riding along with the request — the URL doubles as a data channel, and it's where analytics tags live

## The Search URL at Brew Beans

**Tags:** `core idea` (blue), `URL anatomy` (green), `key=value` (orange)

- **The shop** — Brew Beans sells coffee online at brewbeans.example, with a search page
- **The click** — a customer filters for dark roast espresso; the page's URL grows a tail after `?`
- **The tail** — `?q=espresso&roast=dark&size=250g` is three key=value pairs joined by `&`
- **The channel** — the server reads those pairs to build the page; no form body or cookie needed
- **The bonus** — the URL is copyable: paste it to a friend and they see the same filtered page

*Example (italic):* A customer bookmarks `brewbeans.example/search?q=espresso&roast=dark&size=250g` and next month the same three filters apply the instant it opens.

**Key point:** Everything after the `?` is data the browser sends inside the address itself — the URL doubles as a tiny form submission the server unpacks on arrival.

### Visualization (canvas `c1`, 720×300)

Anatomy diagram: the running-example URL drawn as labeled segments, with arrows down to the three key/value pairs the server extracts.

- **Title (bold 15px, `#1a5276`, top center):** "One URL, Two Jobs: the Address and the Data It Carries".
- **URL row (y=80):** rounded boxes side by side, 13px monospace text, 1.5px borders: `https://brewbeans.example` (mute `#6b7280` border, fill `rgba(107,114,128,0.08)`) at x=30 width 200, `/search` (ink `#1a5276`) at x=235 width 70, `?` (bold 15px orange `#d95926`, standalone) at x=310, then three param boxes: `q=espresso` blue `#2a78d6` at x=330 width 105, `roast=dark` green `#008300` at x=455 width 105, `size=250g` violet `#4a3aa7` at x=580 width 105, with 13px mute `&` glyphs in the 20px gaps between them.
- **Segment labels (11px `#6b7280`, above boxes at y=55):** "where" over the host+path, "data" over the three param boxes.
- **Arrows:** 2px mute arrows from each param box down to a table at y=190.
- **Server table (x=200, y=190, three rows 26px apart, 12px `#2c3e50`):** "q → espresso", "roast → dark", "size → 250g", each row's key tinted to match its box color; 12px ink header "server sees:" at y=175.
- **Annotation (bold 13px aqua `#199e70`, right side near x=520, y=225):** "three filters travel inside the address".
- **Caption (12px `#444`, bottom right):** "example URL, illustrative".

## Decoding ?q=cold%20brew By Hand

**Tags:** `worked example` (blue), `percent-encoding` (green), `repeated keys` (orange)

- **The string** — a busier search produces `?q=cold%20brew&roast=dark&roast=medium&page=2`
- **Step 1: split on `&`** — the string breaks into 4 segments, one per parameter
- **Step 2: split on `=`** — each segment splits once into a key and a value
- **Step 3: decode** — `%20` is a percent-encoded space, so `cold%20brew` becomes `cold brew`
- **The plus twist** — in query strings a `+` also means space, a leftover from old form encoding
- **Repeated keys** — `roast` appears twice, so the server reads it as the list [dark, medium]

*Example (italic):* `?q=cold%20brew&roast=dark&roast=medium&page=2` decodes by hand to q = "cold brew", roast = [dark, medium], page = 2 — 4 segments in, 3 keys out.

**Key point:** Parsing is mechanical — split on `&`, split on `=`, percent-decode — and a repeated key is how a flat string smuggles in a list.

### Visualization (canvas `c2`, 720×300)

Three-stage flow: the raw query string at top, the 4 split segments in the middle, and the decoded 3-key table at the bottom, with the two `roast=` segments merging.

- **Title (bold 15px, `#1a5276`, top center):** "Split on &, Split on =, Decode %20".
- **Raw string box (y=55, centered, width 560):** rounded box, fill `rgba(26,82,118,0.08)`, 13px monospace ink text `?q=cold%20brew&roast=dark&roast=medium&page=2`; 11px mute label "raw" at its left.
- **Segment row (y=135):** four rounded boxes 26px tall, 12px monospace: `q=cold%20brew` blue `#2a78d6` at x=40 width 150, `roast=dark` green `#008300` at x=210 width 120, `roast=medium` green at x=350 width 140, `page=2` violet `#4a3aa7` at x=510 width 90; 2px mute arrows from the raw box down to each; 11px mute label "split on &" at x=630, y=150.
- **Decoded table (y=215, three rows 26px apart starting x=180, 12px `#2c3e50`):** `q → "cold brew"` (blue key, 11px green note `%20 → space` to its right), `roast → [dark, medium]` (green key), `page → 2` (violet key); arrows from segments down, with the two green segment boxes' arrows converging on the roast row.
- **Annotation (bold 12px magenta `#d55181`, right side near x=520, y=245):** "two roast= segments merge into one list".
- **Caption (12px `#444`, bottom right):** "query string illustrative".

## Where the utm_ Tags End Up

**Tags:** `where it's used` (blue), `campaign attribution` (green), `log analysis` (orange)

- **The links** — Brew Beans' newsletter links carry `?utm_source=newsletter&utm_campaign=spring_sale`
- **The trick** — the landing page ignores `utm_` keys; they exist only to be written into the logs
- **The logs** — every access-log line keeps the full URL, so analysts parse `utm_source` back out later
- **The count** — one week of orders by source: newsletter 412, social 268, search 187, no tag 133
- **The lesson** — without the parameters, all 1,000 orders would look identical in the access log

*Example (italic):* The analyst groups the week's 1,000 orders by `utm_source` and finds the newsletter drove 412 — more than 3× the 133 untagged ones.

**Key point:** Query parameters are the URL's analytics channel: the page renders fine without them, but the logs keep them, and campaign attribution is just parsing them back out.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: one week of orders grouped by the `utm_source` value parsed from the access log.

- **Title (bold 15px, `#1a5276`, top center):** "One Week of Orders, Grouped by utm_source".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; 12px `#444` row labels left-aligned at x=20.
- **Rows (top to bottom at y = 70, 120, 170, 220), 22px tall bars, 12px value labels at bar ends:**
  - "utm_source=newsletter": blue `#2a78d6` bar width 440, label "412"
  - "utm_source=social": aqua `#199e70` bar width 286, label "268"
  - "utm_source=search": violet `#4a3aa7` bar width 200, label "187"
  - "(no utm tag)": mute `#6b7280` bar width 142, fill `rgba(107,114,128,0.45)`, label "133"
- **Bar widths** — hardcoded pixels scaled as value × 440 / 412.
- **Annotation (bold 13px green `#008300`, near x=420, y=250):** "412 of 1,000 orders traced to the newsletter".
- **Caption (12px `#444`, bottom right):** "order counts illustrative".

## The Ampersand That Splits Your Data

**Tags:** `common mistake` (red), `encoding` (orange)

- **The value** — a customer searches for "cream & sugar"; that `&` is part of the search text
- **The naive URL** — `?q=cream & sugar&page=1` splits on `&` into 3 pieces, not the 2 intended
- **The damage** — the server sees q = "cream " plus a mystery key named " sugar" with no value
- **The fix** — encode reserved characters inside values: `&` → `%26`, space → `%20`
- **The rule** — encode each key and value first, then join; the joining `&` and `=` stay bare
- **The mirror bug** — decoding twice turns a legitimate `%2526` into a bare `&` and corrupts data the other way

*Example (italic):* Searching "cream & sugar" without encoding returns results for "cream " — the sugar half silently becomes a bogus parameter nobody reads.

**Common mistake:** Trusting raw text inside a URL. Anything that goes into a value must be percent-encoded first, or the URL's own delimiters will tear it apart.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same search sent without encoding (value torn into 3 pieces) vs with `%26` (arrives intact).

- **Title (bold 15px, `#1a5276`, top center):** "Unencoded & Tears One Value into Two Parameters".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no encoding"; blue `#2a78d6` rounded box at x=140 width 190 labeled `q=cream & sugar&page=1` (12px monospace), 3px arrow to three small boxes at x=390: `q=cream ` (blue), ` sugar` (red `#e74c3c`, bold 12px red note "✗ bogus key" above it), `page=1` (mute), with bold 12px red "3 pieces" at the row's right edge.
- **Row 2 (y=205), label:** "with %26"; blue box at x=140 width 230 labeled `q=cream%20%26%20sugar&page=1`, 3px arrow to a green `#008300` box at x=430 width 220 labeled `q = "cream & sugar"` with bold 12px green "✓ intact".
- **Box style:** 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)` / `rgba(107,114,128,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "encode the value first — the URL's & is a delimiter, not a character".
- **Caption (12px `#444`, bottom right):** "example search text illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all strings and numbers are the hardcoded literals above (no randomness); the URLs use the generic domain brewbeans.example; the c3 order counts (412 / 268 / 187 / 133, total 1,000) are invented and labeled illustrative, with bar pixel widths at value × 440 / 412 (440 / 286 / 200 / 142); the c2 query string decodes to exactly 4 segments and 3 keys as stated in the text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
