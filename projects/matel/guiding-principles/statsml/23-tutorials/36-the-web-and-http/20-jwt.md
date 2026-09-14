# JWT

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** JWT

**Subtitle:** A JWT is a signed note the client carries with every request — the server checks the signature instead of looking the user up

## The Signed Note the Kitchen Checks Itself

**Tags:** `core idea` (blue), `authentication` (green), `stateless` (orange)

- **The pizza app** — a customer, Maria, logs into an online pizza shop's app once at 7:00pm
- **The old way** — every request carries a session id; the server looks it up in a session table
- **The signed note** — the login service instead hands Maria a token: name, role, expiry, signed
- **The check** — the orders service verifies the signature with its key; no table, no lookup
- **The trust** — a valid signature proves the login service wrote the note and nobody changed it

*Example (italic):* At 7:04pm Maria's "place order" request carries the token; the orders service verifies it in-process and never calls the login service.

**Key point:** A JWT is a self-contained, signed claim of identity — the server verifies who you are by checking math on the token, not by looking you up.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: a session-id request needing a session-store hop vs a JWT request verified locally by the same service.

- **Title (bold 15px, `#1a5276`, top center):** "Who Does the Server Ask? Session Lookup vs Signed Token".
- **Row 1 (y=95), label 12px `#444` at x=20:** "session id"; blue `#2a78d6` rounded box at x=140 labeled "browser: sid=SID-0001" (12px), 3px arrow to a blue box at x=340 labeled "orders service", 3px arrow to an orange `#d95926` box at x=540 labeled "session DB lookup" with bold 12px orange "extra hop, every request".
- **Row 2 (y=205), label:** "JWT"; blue box at x=140 labeled "browser: signed token", 3px arrow to a green `#008300` box at x=380 labeled "orders service verifies signature locally" with bold 12px green "✓ no lookup".
- **Box style:** 140–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the proof of identity travels inside the request".
- **Caption (12px `#444`, bottom right):** "flow schematic, illustrative".

## Three Base64url Pieces Joined by Dots

**Tags:** `worked example` (blue), `base64url` (green), `signature` (orange)

- **Three parts** — a JWT is three base64url strings joined by dots: header.payload.signature
- **The header** — the first part stands in for `{"alg":"HS256","typ":"JWT"}`, base64url-encoded
- **The payload** — the second part stands in for Maria's claims: sub, role, iat, exp
- **The clock** — iat 1700000000 is when it was issued; exp 1700003600 is one hour (3600 s) later
- **The signature** — HMAC-SHA256 over "header.payload" with a secret key; change one byte and it fails
- **Hand-check** — base64url is plain encoding: paste any real JWT's first two parts into a decoder

*Example (italic):* Flip Maria's role claim from "customer" to "admin" and the signature no longer matches — the tampered token is rejected.

**Key point:** The first two parts are just encoded JSON anyone can read; only the third part — the signature — is what the server actually trusts.

### Visualization (canvas `c2`, 720×300)

Token dissection diagram: the three dot-separated segments on top, each with an arrow down to its decoded content box.

- **Title (bold 15px, `#1a5276`, top center):** "One Token, Three Parts: header.payload.signature".
- **Token bar (y=60, 30px tall, 8px radius, 12px monospace text):** blue `#2a78d6` segment at x=60 width 180 labeled "<HEADER-B64>"; magenta `#d55181` segment at x=258 width 210 labeled "<PAYLOAD-B64>"; green `#008300` segment at x=486 width 174 labeled "<SIGNATURE-B64>". Bold 16px `#2c3e50` dots at x=250 and x=478; segment fills `rgba(42,120,214,0.15)` / `rgba(213,81,129,0.12)` / `rgba(0,131,0,0.12)` with 2px matching borders.
- **Arrows:** 2px `#6b7280` vertical arrows from each segment down to its box at y=150.
- **Decoded boxes (y=150, 56px tall, 8px radius, 12px `#2c3e50` text, fills matching their segment):** blue box `{"alg":"HS256","typ":"JWT"}`; magenta box `{"sub":"maria","role":"customer", iat, exp}`; green box `HMAC-SHA256(header "." payload, secret)`.
- **Timestamp detail (12px `#444`, under the payload box at y≈228):** "iat 1700000000 → exp 1700003600 (+3600 s = 1 hour)".
- **Annotation (bold 13px orange `#d95926`, near y=262, left half):** "no key needed to read — only to sign".
- **Caption (12px `#444`, bottom right):** "token parts are placeholders; timestamps illustrative".

## One Login, Many Services, No Lookup

**Tags:** `where it's used` (blue), `microservices` (green), `API gateway` (orange)

- **Microservices** — gateway, menu, orders, and payment each verify the same token independently
- **No shared table** — services need only the verification key, not a hop to a session store
- **The gateway** — an API gateway can reject bad tokens at the front door before any service runs
- **The math** — a session-store lookup costs ~8 ms; a local signature check costs ~0.2 ms
- **Scaling** — a fifth service can authenticate on day one: give it the key, nothing else

*Example (italic):* One pizza order touches 4 services; session lookups add 32 ms of auth overhead where JWT verification adds 0.8 ms.

**Key point:** JWTs make authentication stateless — identity travels with each request, so services scale out without sharing a session database.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: per-service auth cost for one request, session-store lookup vs local JWT verification, four service rows.

- **Title (bold 15px, `#1a5276`, top center):** "Auth Overhead per Request: 8 ms Lookup vs 0.2 ms Verify".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 40px per ms (max width 320).
- **Rows (top to bottom at y = 70, 115, 160, 205), each with a left-aligned 12px `#444` label at x=20:** "gateway", "menu service", "orders service", "payment service"; each row has a blue `rgba(42,120,214,0.30)` bar width 320 (8 ms session lookup) and, 14px below it, a solid green `#008300` bar width 8 (0.2 ms JWT verify).
- **Bar style:** 12px tall, 11px `#444` value labels "8 ms" / "0.2 ms" at bar ends.
- **Annotation (bold 13px green `#008300`, near x=280, y=255):** "one pizza order: 32 ms of lookups become 0.8 ms".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## Readable to Anyone, Revocable by No One

**Tags:** `common mistake` (red), `security` (orange)

- **Encoded, not encrypted** — anyone holding the token can decode the payload; never put secrets in it
- **No recall** — the signature stays valid until exp; the server cannot "un-sign" an issued token
- **The timeline** — token issued at minute 0, stolen at minute 10, Maria logs out at minute 12
- **The gap** — until exp at minute 60, the stolen token verifies for 48 more minutes
- **alg none** — old libraries accepted a header of `"alg":"none"` with no signature; always reject it
- **The fix** — short expiries plus refresh tokens shrink the gap; a denylist reintroduces a lookup

*Example (italic):* An attacker pastes Maria's token into a base64url decoder and reads "sub":"maria" instantly — reading needed no key, only forging does.

**Common mistake:** Treating a JWT as a sealed envelope. It is a postcard with a tamper-proof signature — readable by all, changeable by none, and impossible to recall before it expires.

### Visualization (canvas `c4`, 720×300)

Timeline chart of one token's 60-minute life: validity band with markers for issue, theft, logout, and expiry, with the post-logout window shaded red.

- **Title (bold 15px, `#1a5276`, top center):** "Logout Can't Recall a Signed Token".
- **Axes:** origin x=60, baseline y=220, plot width 600 (10px per minute); x = minutes 0 to 60 with 12px `#444` tick labels every 10 minutes.
- **Validity band (y=150, 36px tall):** green fill `rgba(0,131,0,0.25)` from minute 0 to 12, red fill `rgba(231,76,60,0.15)` from minute 12 to 60 with centered bold 12px red `#e74c3c` label "stolen token still verifies".
- **Markers (vertical dashed `#6b7280` lines, dash 4/3, from y=120 to y=220, 12px labels above):** minute 0 "issued" (`#444`), minute 10 "token stolen" (`#d95926`), minute 12 "Maria logs out" (`#2a78d6`), minute 60 "exp" (`#444`).
- **Annotation (bold 13px red `#e74c3c`, near x=250, y=90):** "48 minutes of valid use after logout — unless a denylist adds the lookup back".
- **Caption (12px `#444`, bottom right):** "timeline illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); latencies (8 ms / 0.2 ms per service, 32 ms / 0.8 ms per order) and timeline minutes (issue 0 / stolen 10 / logout 12 / exp 60 → 48-minute gap) are invented and labeled illustrative; the three token parts are written as the placeholders `<HEADER-B64>` / `<PAYLOAD-B64>` / `<SIGNATURE-B64>` rather than real base64url strings (deliberate, so no page text resembles a credential); exp − iat = 1700003600 − 1700000000 = 3600 s is exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: session ids, cookie values, tracking ids, token parts, and example passwords on this page are made-up placeholders — for illustration only, and to avoid false positives from secret scanners."
