# Same-Origin Policy & CORS

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Same-Origin Policy & CORS

**Subtitle:** A browser only lets a page read replies from its own scheme + host + port — CORS is how a server explicitly invites other origins in

## The Dashboard That Dies with a Red Console Error

**Tags:** `core idea` (blue), `browser security` (green), `cross-origin` (orange)

- **The dashboard** — a coffee chain's sales page at https://app.example.com charts yesterday's orders
- **The API** — the numbers live at https://api.example.net/orders, a different domain entirely
- **The fetch** — the page's JavaScript calls the API, and the request really does reach the server
- **The block** — the browser hides the reply because the two origins don't match
- **The console** — the familiar red error: "No 'Access-Control-Allow-Origin' header is present"
- **The fix** — the API must reply with Access-Control-Allow-Origin: https://app.example.com

*Example (italic):* The API returns the 4,200-order total just fine — the browser receives it, sees no Allow-Origin header, and throws it away before the chart code ever runs.

**Key point:** The same-origin policy is the browser's default wall: a page may only read responses from its own origin unless the other server opts in with CORS headers.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the same page making a same-origin fetch (readable) and a cross-origin fetch (server answers, browser blocks the read).

- **Title (bold 15px, `#1a5276`, top center):** "One Page, Two Fetches: Same Origin Readable, Cross Origin Blocked".
- **Row 1 (y=95), label 12px `#444` at x=20:** "same origin"; blue `#2a78d6` rounded box at x=130 labeled "page on app.example.com" (12px), 3px arrow to a green `#008300` box at x=400 labeled "GET app.example.com/data → 200" with bold 12px green "✓ response readable" at its right.
- **Row 2 (y=205), label:** "cross origin"; identical blue box, 3px arrow to an aqua `#199e70` box at x=370 labeled "GET api.example.net/orders → 200 OK, total 4,200", then a red `#e74c3c` barrier box at x=590 labeled "no Allow-Origin header" with bold 12px red "✗ read blocked".
- **Box style:** 150–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(25,158,112,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "the server answered — the browser refused to hand it over".
- **Caption (12px `#444`, bottom right):** "flow schematic, order total illustrative".

## Scheme + Host + Port: What Counts as the Same Origin

**Tags:** `worked example` (blue), `origin triple` (green), `preflight` (orange)

- **The triple** — an origin is scheme + host + port; all three must match exactly, the path is ignored
- **Host miss** — https://api.example.net fails against https://app.example.com: different host
- **Scheme miss** — http://app.example.com fails: http is not https, even on the very same host
- **Port miss** — https://app.example.com:8443 fails: 8443 is not the implied default 443
- **Path pass** — https://app.example.com/reports matches: paths never enter the comparison
- **The preflight** — before a PUT with JSON, the browser first sends an OPTIONS asking permission

*Example (italic):* Before PUT /orders/17, the browser sends OPTIONS with Origin: https://app.example.com; the API answers 204 with Access-Control-Allow-Origin: https://app.example.com and Access-Control-Allow-Methods: GET, PUT — only then does the PUT run.

**Key point:** Same-origin is an exact scheme+host+port match; anything else is cross-origin, and non-simple requests must pass the preflight OPTIONS handshake before they are sent.

### Visualization (canvas `c2`, 720×300)

Split panel: left, an origin-match table of four candidate URLs against https://app.example.com; right, the three-step preflight handshake as a vertical sequence.

- **Title (bold 15px, `#1a5276`, top center):** "Against https://app.example.com: Who Matches, and the Preflight Handshake".
- **Left panel (x 20–340):** header row at y=65, 12px bold `#1a5276` column labels "candidate / scheme / host / port"; four rows at y = 100, 140, 180, 220, each a 12px `#2c3e50` URL label plus per-column marks (bold 13px, green `#008300` "✓" or red `#e74c3c` "✗") and a verdict pill at x=310:
  - "…/reports": marks `["✓","✓","✓"]`, green pill "same"
  - "http://app…": marks `["✗","✓","✓"]`, red pill "cross"
  - "api.example.net": marks `["✓","✗","✓"]`, red pill "cross"
  - "app…:8443": marks `["✓","✓","✗"]`, red pill "cross"
- **Right panel (x 380–700), 12px labels, arrows 3px:** step 1 at y=100, blue `#2a78d6` right arrow "OPTIONS /orders/17 + Origin: app.example.com"; step 2 at y=155, green `#008300` left arrow "204 + Allow-Origin: app.example.com, Allow-Methods: GET, PUT"; step 3 at y=210, violet `#4a3aa7` right arrow "PUT /orders/17 → 200, readable"; thin `#e5e9ef` vertical divider at x=360.
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=265):** "path never matters — scheme, host, port decide everything".
- **Caption (12px `#444`, bottom right):** "URLs illustrative, match rules exact".

## Why curl Works but the Browser Doesn't

**Tags:** `where it's used` (blue), `debugging` (green), `curl vs browser` (orange)

- **The classic ticket** — "the API is broken in prod" — yet curl returns the JSON instantly
- **No origin, no rule** — curl has no page context, so the same-origin policy never applies to it
- **Server-side too** — backend-to-backend calls skip CORS; only in-browser JavaScript is policed
- **The tell** — the network tab shows 200 or "CORS error"; the console shows the red error
- **Whose bug** — the fix is a server header change, not a frontend code change
- **Local dev** — localhost:3000 vs localhost:8080 are different origins, so dev setups hit it first

*Example (italic):* The support engineer runs curl against api.example.net/orders, gets the 4,200-order JSON back in 80 ms, and closes the ticket as "works for me" — while every dashboard user still sees a blank chart.

**Key point:** CORS is enforced by the browser, not the network or the server — any client without a browser's origin rules (curl, Postman, backend services) sails straight through.

### Visualization (canvas `c3`, 720×300)

Four-row comparison: the same API called by four clients; every request succeeds, but only the browser's read is blocked.

- **Title (bold 15px, `#1a5276`, top center):** "Same API, Four Clients: Only the Browser Is Blocked".
- **Rows (y = 80, 125, 170, 215), each with a left-aligned 12px `#444` client label at x=20:** labels `["curl", "backend service", "Postman", "browser JS (app.example.com)"]`; each row draws a 3px arrow from x=210 to a blue `#2a78d6` rounded box at x=330 labeled "api.example.net → 200 OK" (12px), then a result at x=545:
  - curl: bold 12px green `#008300` "✓ reads 4,200-order JSON"
  - backend service: bold 12px green "✓ response readable"
  - Postman: bold 12px green "✓ response readable"
  - browser JS: bold 12px red `#e74c3c` "✗ read blocked by CORS"
- **Box style:** 190px wide, 34px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=265):** "the request always succeeds — only the browser withholds the reply".
- **Caption (12px `#444`, bottom right):** "outcomes schematic, timings and counts illustrative".

## CORS Protects the User, Not Your API

**Tags:** `common mistake` (red), `credentials` (orange)

- **The confusion** — teams treat CORS as an API firewall; attackers with curl ignore it entirely
- **Who it guards** — it stops a malicious page from reading your data using the visitor's cookies
- **The scenario** — evil.example.org fetches shop.example.com riding a SameSite=None login cookie
- **The wildcard trap** — browsers reject Allow-Origin: * combined with Allow-Credentials: true
- **Real auth** — protect the API with authentication; CORS only decides which pages may read
- **The echo trap** — blindly reflecting any Origin back with credentials rebuilds the wildcard hole

*Example (italic):* A visitor logged in to shop.example.com wanders onto evil.example.org; that page silently fetches shop.example.com/account with her SameSite=None session cookie — CORS is what stops the script from reading her order history.

**Common mistake:** Opening CORS wide ("just set *") to make the error go away. CORS never blocked attackers with curl — it protects your users' cookie-backed sessions from hostile pages, and the * + credentials combo is banned by browsers precisely because it would give those sessions away.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: an attacker with curl reaches the API directly (CORS never consulted), while a hostile page's credentialed fetch is blocked by the visitor's browser.

- **Title (bold 15px, `#1a5276`, top center):** "What CORS Actually Stops: the Hostile Page, Not the Hostile Client".
- **Row 1 (y=95), label 12px `#444` at x=20:** "attacker with curl"; red `#e74c3c` rounded box at x=150 labeled "curl, scripted client" (12px), 3px arrow to a blue `#2a78d6` box at x=390 labeled "api.example.net" with 12px `#6b7280` text "CORS never consulted — auth must do the work" at its right.
- **Row 2 (y=205), label:** "hostile page"; red box at x=150 labeled "page on evil.example.org", 3px arrow labeled 11px `#444` "fetch with visitor's cookies" to a green `#008300` barrier box at x=400 labeled "browser: origin not allowed — read blocked" with bold 12px green "✓ user's data safe" at its right.
- **Box style:** 150–200px wide, 40px tall, 8px radius, fills `rgba(231,76,60,0.12)` / `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "CORS guards the visitor's cookies, not your server".
- **Caption (12px `#444`, bottom right):** "flow schematic, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all labels, arrays, and marks are the hardcoded literals above (no randomness); domains are generic example.com / example.net / example.org placeholders; the 4,200-order total and 80 ms timing are invented and labeled illustrative; the origin-match verdicts in c2 (scheme/host/port must all match, path ignored) and the browser rejection of `*` with credentials in c4 are exact per the CORS specification.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
