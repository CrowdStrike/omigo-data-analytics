# Redirects

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Redirects

**Subtitle:** A redirect is the web's "we moved" note — the server answers with a 3xx code and a new address in the Location header, and the browser follows it before you notice

## The Note Taped to the Old Door

**Tags:** `core idea` (blue), `Location header` (green), `web basics` (orange)

- **The move** — the coffee shop rebuilds its site; the menu moves to `menu.brewbean.example`
- **The old bookmark** — regulars saved `brewbean.example/menu.html` back when the menu lived there
- **The note** — the old server answers "301 Moved Permanently" with the new address attached
- **The follow** — the browser reads the `Location` header and silently requests the new URL
- **The result** — one click, two requests, and the customer sees the menu with no error page

*Example (italic):* A bookmark saved in 2024 still opens today's menu — the browser follows the 301 in one extra round trip the customer never notices.

**Key point:** A redirect is a server reply that says "not here — go there": a 3xx status code plus a `Location` header, which the browser follows automatically.

### Visualization (canvas `c1`, 720×300)

Sequence diagram: browser lane on the left, server lane on the right, four labeled request/response arrows showing one bookmark click becoming two requests.

- **Title (bold 15px, `#1a5276`, top center):** "One Bookmark Click, Two Requests: Following the 301".
- **Lanes:** bold 13px ink `#1a5276` labels "browser" at x=110 and "server" at x=590, y=55; vertical dashed `#e5e9ef` (dash 4/3) lifelines from y=65 to y=255 at x=110 and x=590.
- **Arrow 1 (y=100):** blue `#2a78d6` 3px arrow left→right, 12px `#2c3e50` label above: "GET brewbean.example/menu.html".
- **Arrow 2 (y=145):** orange `#d95926` 3px arrow right→left, 12px `#2c3e50` label above: "301 Moved Permanently", 12px monospace `#6b7280` label below: "Location: https://menu.brewbean.example/".
- **Arrow 3 (y=195):** blue `#2a78d6` 3px arrow left→right, 12px `#2c3e50` label above: "GET menu.brewbean.example".
- **Arrow 4 (y=240):** green `#008300` 3px arrow right→left, 12px `#2c3e50` label above: "200 OK — the menu".
- **Annotation (bold 13px green `#008300`, near x=350, y=272):** "the customer clicks once — the browser does the rest".
- **Caption (12px `#444`, bottom right):** "brewbean.example is a made-up domain; timing schematic".

## 301, 302, 307, 308: Four Flavors of "We Moved"

**Tags:** `worked example` (blue), `status codes` (green), `permanent vs temporary` (orange)

- **301 permanent** — "the menu lives there now, for good"; browsers cache it and stop asking
- **302 temporary** — "try there for now"; the browser asks the original URL again next visit
- **307 temporary** — like 302, but a POST stays a POST instead of being rewritten to a GET
- **308 permanent** — like 301, with the same keep-the-method guarantee that 307 gives
- **The header** — every 3xx reply carries `Location: <new URL>`; without it the browser is lost
- **The chain** — a redirect can point at another redirect; browsers give up after about 20 hops

*Example (italic):* The shop's checkout form POSTs to `/pay`; behind a 301 the browser re-sends it as a GET and the order body is dropped — a 308 keeps it a POST.

**Key point:** All four codes mean "go there instead"; they differ on exactly two promises — is the move permanent (so caches may remember it), and does the browser keep the original HTTP method.

### Visualization (canvas `c2`, 720×300)

Comparison grid: one row per status code, three check/cross columns for the two promises each code makes.

- **Title (bold 15px, `#1a5276`, top center):** "Same 'We Moved' Message — Different Promises".
- **Column headers (bold 12px `#444`, y=62, centered):** "permanent?" at x=330, "browser caches it?" at x=480, "keeps POST?" at x=630; thin 1px `#e5e9ef` vertical separators between columns from y=70 to y=250.
- **Rows (baseline y = 100, 150, 200, 250), each with a left-aligned label at x=20:** bold 13px code in color + 12px `#6b7280` name:
  - "301" blue `#2a78d6` "Moved Permanently": ✓ ✓ ✗
  - "302" aqua `#199e70` "Found": ✗ ✗ ✗
  - "307" violet `#4a3aa7` "Temporary Redirect": ✗ ✗ ✓
  - "308" magenta `#d55181` "Permanent Redirect": ✓ ✓ ✓
- **Cells:** bold 16px marks centered under each header — ✓ in green `#008300`, ✗ in red `#e74c3c`; each row sits on a 1px `#e5e9ef` gridline.
- **Annotation (bold 13px violet `#4a3aa7`, near x=330, y=282):** "pick by two questions: permanent? and does the method survive?".
- **Caption (12px `#444`, bottom right):** "301/302 rewriting POST to GET is long-standing browser behavior".

## Where Short Links, Trackers, and SEO Live

**Tags:** `where it's used` (blue), `tracking` (orange), `SEO` (green)

- **Short links** — `go.example/x9` stores nothing but a redirect to the real, much longer URL
- **Trackers** — the newsletter's link hops through `track.example`, which logs the click first
- **SEO** — a 301 tells search engines to transfer the old page's ranking to the new URL
- **A 302 instead** — a temporary move: search engines initially keep ranking the old URL
- **The latency bill** — each hop is a full round trip: 90 + 110 + 140 ms = 340 ms before content

*Example (italic):* One newsletter click travels `go.example/x9` → `track.example/r?id=42` → `shop.brewbean.example/mugs`: two redirects, one logged click, 340 ms.

**Key point:** Redirect chains are where the link economy hides — every URL shortener and click tracker is just a server whose only job is to answer 3xx and write a log line.

### Visualization (canvas `c3`, 720×300)

Chain diagram of the three hops with status codes and per-hop latency, plus a stacked horizontal time bar showing where the 340 ms goes.

- **Title (bold 15px, `#1a5276`, top center):** "One Click, Three Hops: Where the 340 ms Goes".
- **Chain row (boxes 190×46, y=85, 8px radius, 12px monospace `#2c3e50` text):** blue `rgba(42,120,214,0.15)` box at x=30 "go.example/x9" with bold 12px blue `#2a78d6` tag "short link" above; magenta `rgba(213,81,129,0.12)` box at x=265 "track.example/r?id=42" with bold 12px magenta `#d55181` tag "click tracker" above; green `rgba(0,131,0,0.12)` box at x=500 "shop.brewbean.example/mugs" with bold 12px green `#008300` tag "landing page" above.
- **Arrows:** 3px `#2c3e50` arrows between the boxes; 12px `#6b7280` labels under them: "301 · 90 ms" after box 1, "302 · 110 ms" after box 2; under box 3, 12px `#6b7280` "200 · 140 ms".
- **Time bar (y=200, 26px tall, x=60, total width 600 = 340 ms):** three segments left to right — blue `#2a78d6` width 159 labeled "90 ms", magenta `#d55181` width 194 labeled "110 ms", green `#008300` width 247 labeled "140 ms"; fills at 30% alpha with solid 2px top edges, bold 11px labels in each segment's solid color; 12px `#444` axis labels "0 ms" at x=60 and "340 ms" at x=660 below the bar.
- **Annotation (bold 13px orange `#d95926`, centered near y=265):** "340 ms gone before the first byte of the real page".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## Cached Forever: Why a Temporary Move Should Never Be a 301

**Tags:** `common mistake` (red), `caching` (orange)

- **The shortcut** — a 301 invites the browser to cache the jump and never ask the server again
- **The sale** — the shop points its homepage at `/sale-2026` with a 301 for a two-week promotion
- **The trap** — the sale ends, but returning visitors' browsers still jump straight to the dead page
- **No undo** — the server can change its answer, yet cached 301s live in browsers you don't control
- **The rule** — 302/307 for anything temporary; 301/308 only when the old URL is truly retired

*Example (italic):* Two weeks after the sale ends, a regular's browser still auto-jumps to `/sale-2026` and hits a 404 — the server was never asked again.

**Common mistake:** Reaching for 301 because "it's the redirect code." Permanence is a promise made to caches; break the promise and stale copies of your mistake outlive the fix.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same two-week sale wired with a 302 (recovers cleanly) vs a 301 (browser keeps jumping to a dead page).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Sale, Two Codes: 302 Recovers, 301 Gets Stuck".
- **Row 1 (y=95), label bold 12px `#444` at x=20:** "302 for the sale"; blue `rgba(42,120,214,0.15)` rounded box at x=140 (150×40) "brewbean.example", 3px arrow labeled 12px `#6b7280` "302", aqua `rgba(25,158,112,0.15)` box at x=340 "/sale-2026 (2 weeks)", 3px arrow labeled "sale ends", green `rgba(0,131,0,0.12)` box at x=540 (160×40) "asks again — homepage ✓" with the ✓ bold 12px green `#008300`.
- **Row 2 (y=205), label:** "301 for the sale"; blue box at x=140 "brewbean.example", 3px arrow labeled "301 — cached", aqua box at x=340 "/sale-2026 (2 weeks)", dashed red `#e74c3c` (dash 4/3) 3px arrow labeled "sale ends", red `rgba(231,76,60,0.12)` box at x=540 (160×40) "cached jump → 404 ✗" with bold 12px red `#e74c3c` "returning visitors stuck" beneath it at y=258.
- **Box style:** 8px radius, 12px `#2c3e50` text, 2px edges in each fill's solid hue.
- **Annotation (bold 13px orange `#d95926`, centered near y=285):** "302 is a question asked every visit; 301 is an answer the browser memorizes".
- **Caption (12px `#444`, bottom right):** "cache behavior typical of major browsers; diagram schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness). The `c2` promise grid is exact HTTP semantics: 301 ✓✓✗, 302 ✗✗✗, 307 ✗✗✓, 308 ✓✓✓ for permanent / cacheable / keeps-POST. Chain hop latencies (90 / 110 / 140 ms, total 340 ms) are invented and labeled illustrative; `c3` segment widths 159 / 194 / 247 px sum to 600 px and are proportional to those milliseconds. Domains (`brewbean.example`, `go.example`, `track.example`) are made up; the ~20-hop browser limit is typical browser behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
