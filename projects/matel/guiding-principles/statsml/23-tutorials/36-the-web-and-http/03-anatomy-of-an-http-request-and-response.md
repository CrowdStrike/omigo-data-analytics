# Anatomy of an HTTP Request & Response

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Anatomy of an HTTP Request & Response

**Subtitle:** Every HTTP/1.1 message is plain text in four parts — a first line, some headers, one blank line, and an optional body — and you can read every byte

## One GET, Line by Line: Asking for the Menu

**Tags:** `core idea` (blue), `request line` (green), `headers` (orange)

- **The order** — you open a cafe's online menu; the browser sends a short text message to the server
- **The request line** — `GET /menu HTTP/1.1` names the verb, the path, and the protocol version
- **The headers** — `Host: cafe.example.com` picks the site; `Accept: application/json` asks for a format
- **The blank line** — one empty line means "headers done"; it is the only separator in the message
- **The body** — a GET carries no body; everything it asks for fits in the path and the headers

*Example (italic):* Loading the menu sends exactly five short text lines — a request line, three headers, and a blank line — under 100 bytes in total.

**Key point:** An HTTP/1.1 request is plain text in four parts — request line, headers, one blank line, optional body — there is no binary framing to decode.

### Visualization (canvas `c1`, 720×300)

Annotated message diagram: the five lines of the GET request rendered as monospace text, with colored side brackets naming the four anatomical parts.

- **Title (bold 15px, `#1a5276`, top center):** "One GET Request: Four Parts, Five Lines, Under 100 Bytes".
- **Message lines (13px monospace, left-aligned at x=90, first line y=78, line spacing 30), hardcoded array:** `["GET /menu HTTP/1.1", "Host: cafe.example.com", "Accept: application/json", "User-Agent: curl/8.4", "", "(empty — a GET sends no body)"]`.
- **Line colors:** request line bold blue `#2a78d6`; the three header lines text `#2c3e50`; blank line drawn as a horizontal dashed orange `#d95926` band (dash 4/3, x=80 to x=420) with an 11px orange label "blank line = end of headers"; body placeholder italic 12px mute `#6b7280`.
- **Part brackets (right side, vertical 2px lines at x=450 with bold 12px labels at x=465):** "request line" blue `#2a78d6` beside line 1; "headers" aqua `#199e70` spanning lines 2–4; "body (empty)" mute `#6b7280` beside line 6.
- **Annotation (bold 13px green `#008300`, near x=440, y=255):** "plain text you can read — no binary, no magic".
- **Caption (12px `#6b7280`, bottom right):** "line endings are CR-LF; sizes illustrative".

## The POST and Its 201 Reply: Placing an Order

**Tags:** `worked example` (blue), `POST` (green), `status codes` (orange)

- **The verb changes** — placing an order sends data, so the first line is `POST /orders HTTP/1.1`
- **New headers** — `Content-Type: application/json` tells the server how to parse the incoming body
- **The length** — `Content-Length: 40` is the exact byte count of the body; count it, it is 40
- **The body** — after the blank line: `{"drink":"latte","size":"large","qty":2}`
- **The reply** — the response mirrors the shape: `HTTP/1.1 201 Created`, headers, blank line, body
- **The receipt** — response body `{"id":1042,"total":9.90}` plus header `Location: /orders/1042`

*Example (italic):* Two large lattes at 4.95 each come back as total 9.90 under new order id 1042 — 2 × 4.95, checkable by hand.

**Key point:** Request and response share one anatomy — first line, headers, blank line, body — only the first line's meaning differs (verb + path going out, status code coming back).

### Visualization (canvas `c2`, 720×300)

Side-by-side panels: the full POST request on the left, its 201 response on the right, each rendered line by line with the blank-line separator marked.

- **Title (bold 15px, `#1a5276`, top center):** "POST /orders and Its 201 Reply, Side by Side".
- **Panels:** request panel x=40 width 310, response panel x=380 width 310; 1px `#e5e9ef` rounded borders; bold 13px `#1a5276` panel headers at y=58: "request →" and "← response".
- **Request lines (12px monospace, x=52, first line y=84, spacing 24), hardcoded array:** `["POST /orders HTTP/1.1", "Host: cafe.example.com", "Content-Type: application/json", "Content-Length: 40", "", "{\"drink\":\"latte\",", "  \"size\":\"large\",\"qty\":2}"]` — first line bold blue `#2a78d6`, headers `#2c3e50`, blank line a dashed orange `#d95926` rule, body lines green `#008300`.
- **Response lines (12px monospace, x=392, same y grid), hardcoded array:** `["HTTP/1.1 201 Created", "Content-Type: application/json", "Location: /orders/1042", "Content-Length: 24", "", "{\"id\":1042,\"total\":9.90}"]` — first line bold violet `#4a3aa7`, headers `#2c3e50`, dashed orange rule, body green `#008300`.
- **Byte-count label (bold 11px aqua `#199e70`, under the request body):** "body = 40 bytes ↔ Content-Length: 40 (shown wrapped)".
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=272):** "same anatomy both ways — only the first line differs".
- **Caption (12px `#6b7280`, bottom right):** "order values illustrative".

## Where You'll Read These Lines: curl and DevTools

**Tags:** `where it's used` (blue), `debugging` (green), `curl` (orange)

- **curl -v** — prints every request line prefixed `>` and every response line prefixed `<`; nothing hidden
- **DevTools** — the browser's Network tab shows these same lines for every page and API call a site makes
- **Status classes** — 2xx means success, 4xx means "your request is wrong", 5xx means "the server broke"
- **Header bugs** — of 100 illustrative API tickets, 61 are header issues: Content-Type 34, auth 27
- **The rest** — wrong method accounts for 18 tickets, malformed JSON body 14, wrong path just 7

*Example (italic):* A failing upload turns out to be `Content-Type: text/plain` on a JSON body — one header line, spotted in ten seconds of curl -v output.

**Key point:** Debugging an API means reading these lines; the tools change (curl, DevTools, proxies) but the four-part message never does.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: where 100 illustrative API bug tickets actually pointed, split by which line of the message was wrong.

- **Title (bold 15px, `#1a5276`, top center):** "Where 100 API Bug Tickets Actually Pointed".
- **Axis:** row labels left-aligned 12px `#2c3e50` at x=20; bars start at a 2px `#999` vertical baseline x=230, width = count × 4.4 px, 16px tall; 11px `#444` count labels just past each bar end.
- **Rows (top to bottom at y = 70, 110, 150, 190, 230), hardcoded counts `[34, 27, 18, 14, 7]`:**
  - "wrong Content-Type": blue `#2a78d6` bar width 150, label "34"
  - "missing/expired auth header": aqua `#199e70` bar width 119, label "27"
  - "wrong method (GET vs POST)": violet `#4a3aa7` bar width 79, label "18"
  - "malformed JSON body": yellow `#c98500` bar width 62, label "14"
  - "wrong path": magenta `#d55181` bar width 31, label "7"
- **Annotation (bold 13px orange `#d95926`, right side near y=95):** "61 of 100 live in the headers".
- **Caption (12px `#6b7280`, bottom right):** "ticket counts illustrative".

## 200 OK Doesn't Always Mean OK

**Tags:** `common mistake` (red), `status codes` (orange)

- **The trust** — beginners assume `200 OK` in the status line means the operation truly succeeded
- **The tunnel** — some APIs answer 200 with an error in the body: `{"status":"error","msg":"card declined"}`
- **The silent failure** — a client that checks only the status code shows the customer a success page
- **The mirror trap** — others treat 404 as a bug, when it is a valid answer meaning "no such order"
- **The rule** — read the status line first, then the body; the two can disagree and both matter

*Example (italic):* The cafe app shows "order placed!" for a declined card because its code never read past the 200 in the first line.

**Common mistake:** Trusting either the status line or the body alone — a well-behaved API keeps them consistent, but your client must check both before declaring success.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same 200-with-error response handled by a status-only client (silent failure) vs a client that reads the body too (error surfaced).

- **Title (bold 15px, `#1a5276`, top center):** "Status Line Says 200, Body Says Declined: Check Both".
- **Row 1 (y=95), label 12px `#6b7280` at x=20:** "status-only client"; blue `#2a78d6` rounded box at x=160 labeled "HTTP/1.1 200 OK" (12px), 3px arrow to a magenta `#d55181` box at x=350 labeled "body: \"card declined\"", 3px arrow to an orange `#d95926` box at x=545 labeled "shows: order placed" with bold 12px orange "✗ silent failure" beneath it.
- **Row 2 (y=210), label:** "status+body client"; the same blue "HTTP/1.1 200 OK" and magenta "body: \"card declined\"" boxes, then a green `#008300` box at x=545 labeled "shows: card declined" with bold 12px green "✓ error surfaced" beneath it.
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(213,81,129,0.12)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=278):** "the status line and the body are two separate claims — verify both".
- **Caption (12px `#6b7280`, bottom right):** "API behavior illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all message lines and counts are the hardcoded literal arrays above (no randomness); the bug-ticket breakdown `[34, 27, 18, 14, 7]` of 100 and the order values (2 × 4.95 = 9.90, order id 1042) are invented and labeled illustrative; `Content-Length: 40` and `Content-Length: 24` are the true byte counts of the two JSON bodies shown; the domain `cafe.example.com` is a reserved example domain.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
