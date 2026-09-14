# Anatomy of a URL

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Anatomy of a URL

**Subtitle:** Every web address is six labeled parts glued together by fixed punctuation — learn the three delimiters and you can read any link like a sentence

## One Coffee Order Link, Six Labeled Parts

**Tags:** `core idea` (blue), `six parts` (green), `web basics` (orange)

- **The link** — a coffee shop's espresso page is one 76-character address made of six parts
- **Scheme** — `https` says how to talk: the encrypted web protocol, not `ftp` or `mailto`
- **Host & port** — `shop.brewbean.example:8443` says which machine, and which numbered door on it
- **Path** — `/menu/espresso` says which page on that machine, read like a folder path
- **Query** — `size=large&milk=oat` carries the order's options as key=value pairs
- **Fragment** — `#reviews` names a spot inside the page; the browser scrolls straight to it

*Example (italic):* Typing `https://shop.brewbean.example:8443/menu/espresso?size=large&milk=oat#reviews` fetches the espresso page over https and lands on the reviews section.

**Key point:** A URL is `scheme://host:port/path?query#fragment` — six parts, each with one job, joined by fixed punctuation that never changes meaning.

### Visualization (canvas `c1`, 720×300)

One horizontal bar showing the full 76-character link split into colored segments, widths proportional to character counts, with part names above and the literal text below.

- **Title (bold 15px, `#1a5276`, top center):** "The 76-Character Order Link, Split into Its Six Parts".
- **Bar:** one row at y=130, 44px tall, starting x=60, total width 600; segments drawn left to right as (x, width, fill): scheme x=60 w=40 blue `#2a78d6`; `://` x=100 w=24 mute `#6b7280`; host x=124 w=166 green `#008300`; `:` x=290 w=8 mute; port x=298 w=32 yellow `#c98500`; path x=330 w=110 violet `#4a3aa7`; `?` x=440 w=8 mute; query x=448 w=150 orange `#d95926`; `#` x=598 w=8 mute; fragment x=606 w=54 magenta `#d55181`. Segment fills at 25% alpha with a solid 2px top edge in the same hue.
- **Part names:** bold 12px labels above the bar in each segment's solid color, staggered between y=95 and y=112 so none overlap: "scheme", "host", "port", "path", "query", "fragment".
- **Literal text:** 12px monospace `#2c3e50` under each segment at y=195: `https`, `shop.brewbean.example`, `8443`, `/menu/espresso`, `size=large&milk=oat`, `reviews`, angled 30° where too wide to fit flat.
- **Annotation (bold 13px ink `#1a5276`, centered near y=250):** "six parts, three delimiters — every link reads the same way".
- **Caption (12px `#444`, bottom right):** "brewbean.example is a made-up domain; segment widths proportional to character counts".

## Splitting the 76 Characters by Hand

**Tags:** `worked example` (blue), `parse by hand` (green), `delimiters` (orange)

- **Count it** — the full link is exactly 76 characters; every part has a fixed start and end
- **First cut** — everything before `://` is the scheme: characters 1–5 are `https`
- **Second cut** — the host runs 9–29; the `:` at 30 hands off to the port `8443` at 31–34
- **Third cut** — the path is 35–48; `?` at 49 opens the query 50–68; `#` at 69 opens the fragment 70–76
- **Query split** — split `size=large&milk=oat` on `&`, then each piece on `=`: exactly two pairs
- **Hand-check** — `size=large` is 10 characters (50–59), `&` sits at 60, `milk=oat` is 8 (61–68)

*Example (italic):* Reading left to right, the first `?` you meet ends the path — so `/menu/espresso` stops at character 48 and nothing after 49 is a page name.

**Key point:** Parsing is three delimiter scans — `://`, then `?`, then `#` — done in order; the first occurrence of each delimiter makes the cut, so every character has exactly one home.

### Visualization (canvas `c2`, 720×300)

Two-level diagram: the full link as a position ruler on top, then the query zoomed and exploded into its two key–value pairs below.

- **Title (bold 15px, `#1a5276`, top center):** "Three Delimiter Scans — Then Split the Query on & and =".
- **Ruler row (y=70, 30px tall, x=60, width 600):** the same ten segments and colors as `c1` at reduced height; 12px `#444` character-position labels below the bar at the segment starts: "1", "9", "31", "35", "50", "70" and "76" at the right end.
- **Zoom lines:** two dashed `#6b7280` (dash 4/3) guide lines from the query segment's corners (x=448 and x=598 at y=100) down to the zoom box corners (x=150 and x=570 at y=150).
- **Zoom row (y=150, 36px tall):** orange `rgba(217,89,38,0.20)` box x=150 w=200 labeled `size=large` (13px monospace) with 12px `#444` "chars 50–59" beneath; mute `#6b7280` 20px box for `&` at x=350 labeled "60"; aqua `rgba(25,158,112,0.20)` box x=370 w=200 labeled `milk=oat` with "chars 61–68" beneath.
- **Pair row (y=235, 32px tall):** two rounded key–value boxes: orange-edged box at x=150 w=200 reading "size → large", aqua-edged box at x=370 w=200 reading "milk → oat", 13px `#2c3e50` text, connected to the zoom row by 2px arrows.
- **Annotation (bold 12px violet `#4a3aa7`, right side near x=590, y=230):** "first ://, ?, # make the cuts — positions are unambiguous".
- **Caption (12px `#444`, bottom right):** "positions are exact character counts of the example link".

## Reading URLs in Logs, Analytics, and Phish Checks

**Tags:** `where it's used` (blue), `analytics` (green), `security` (red)

- **Analytics** — group order links by the `size` value: large 340, medium 210, small 150 of 700 orders
- **Debugging** — a 404 in the logs is usually a path typo; a wrong answer is usually a query typo
- **Caching** — servers cache by path plus query, so `size=large` and `size=small` are separate entries
- **Security** — the host decides who you trust: `brewbean.example.evil.example` belongs to evil, not brewbean
- **Read right to left** — the owner is the last two labels; three under public suffixes like `co.uk`

*Example (italic):* One grouping pass on the `size` key across 700 logged order links shows large drinks are 340 orders — nearly half the day's business.

**Key point:** Every server log line, campaign tag, and phishing check is an exercise in reading URL parts — the part you group by or trust is a slice between two delimiters.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: one day's 700 order links grouped by the value of the `size` query key.

- **Title (bold 15px, `#1a5276`, top center):** "700 Order Links Grouped by the size Query Key".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = orders 0 to 400 with gridlines `#e5e9ef` at 100/200/300/400 and 12px `#444` tick labels.
- **Bars (120px wide, centered at x=180, 360, 540):** `size=large` 340 orders, blue `#2a78d6`, height 153px; `size=medium` 210 orders, aqua `#199e70`, height 95px; `size=small` 150 orders, violet `#4a3aa7`, height 68px. Fills at 30% alpha with solid 2px top edges; bold 13px value labels "340", "210", "150" in each bar's solid color just above the bar tops; 12px `#444` category labels `size=large` / `size=medium` / `size=small` below the baseline.
- **Annotation (bold 13px green `#008300`, near x=430, y=75):** "large = 340 of 700 — nearly half, and one query key told us".
- **Caption (12px `#444`, bottom right):** "order counts illustrative".

## The Fragment Never Leaves the Browser

**Tags:** `common mistake` (red), `fragment` (orange), `client-side only` (blue)

- **The surprise** — the browser strips `#reviews` before sending; the server never sees fragments
- **What is sent** — the request line is `GET /menu/espresso?size=large&milk=oat` — path and query only
- **The mistake** — counting visits to `#reviews` from server logs: the count is always zero
- **The mix-up** — putting data the server needs after `#` instead of `?`; it silently vanishes
- **The rule** — server-bound data rides in the query; browser-only position or UI state rides in the fragment

*Example (italic):* An analyst counts `#reviews` hits in the server logs and reports zero — every visitor scrolled to reviews, but the server was never told.

**Common mistake:** Treating `?` and `#` as interchangeable. The query travels to the server; the fragment stays on the client — anything placed after `#` never arrives.

### Visualization (canvas `c4`, 720×300)

Flow diagram: the browser holds the full URL, sends only path + query to the server, and keeps the fragment for itself.

- **Title (bold 15px, `#1a5276`, top center):** "What the Server Actually Receives".
- **Browser box (x=40, y=90, 260×70):** blue `rgba(42,120,214,0.15)` rounded box, 8px radius, bold 12px `#1a5276` label "browser holds the full URL"; inside, 12px monospace `/menu/espresso?size=large&milk=oat` in `#2c3e50` and `#reviews` immediately after it in magenta `#d55181`.
- **Request arrow:** 3px `#2c3e50` arrow from the browser box to the server box, 12px `#444` label above it: "GET /menu/espresso?size=large&milk=oat".
- **Server box (x=440, y=90, 240×70):** green `rgba(0,131,0,0.12)` rounded box, bold 12px green `#008300` label "server sees path + query", 12px `#2c3e50` text "no fragment in the log line".
- **Fragment box (x=40, y=210, 260×46):** magenta-edged `rgba(213,81,129,0.12)` dashed-border box reading "#reviews — stays here, browser scrolls" in 12px `#2c3e50`; dashed `#6b7280` (dash 4/3) arrow curving from the browser box down into it.
- **Annotation (bold 13px red `#e74c3c`, near x=440, y=240):** "server logs will never contain #reviews — that count is zero".
- **Caption (12px `#444`, bottom right):** "HTTP never transmits the fragment; diagram schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness). Character positions are exact for the 76-character example link `https://shop.brewbean.example:8443/menu/espresso?size=large&milk=oat#reviews`: scheme 1–5, host 9–29, port 31–34, path 35–48, query 50–68 (`size=large` 50–59, `&` 60, `milk=oat` 61–68), fragment 70–76. Segment pixel widths in `c1` (40/24/166/8/32/110/8/150/8/54, summing to 600) are proportional to character counts. Order counts (340/210/150 of 700) are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
