# Cookie Attributes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cookie Attributes

**Subtitle:** A cookie is just a name=value pair the browser re-sends — the attributes after the semicolons (HttpOnly, Secure, SameSite) are rules about when and to whom it gets re-sent

## One Login Cookie, Read Lock by Lock

**Tags:** `core idea` (blue), `session security` (green), `HTTP header` (orange)

- **The login** — a customer signs in and gets `session=SID-0001`; whoever presents it is that customer
- **The header** — `Set-Cookie: session=SID-0001; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=86400`
- **HttpOnly** — page JavaScript cannot read this cookie through `document.cookie`
- **Secure** — the browser attaches it only to https:// requests, never to plain http://
- **SameSite=Lax** — requests started from other sites mostly do not carry it; link clicks do
- **Path & Max-Age** — sent only for URLs under `/`, deleted after 86,400 seconds (one day)

*Example (italic):* The customer opens "my orders" the next morning and the cookie rides along, so the shop still knows them — at hour 25 it is gone and they must sign in again.

**Key point:** Everything after the first semicolon is a delivery rule, not data — the cookie's job is identification, and the attributes decide which requests are allowed to carry it.

### Visualization (canvas `c1`, 720×300)

Annotated dissection of the running example's Set-Cookie header: the raw string split into colored segments with callout labels above and below.

- **Title (bold 15px, `#1a5276`, top center):** "One Set-Cookie Header: the Value Plus Five Delivery Rules".
- **Header strip:** six rounded boxes in a row centered at y=150, 30px tall, 6px radius, 12px monospace `#2c3e50` text, laid left to right from x=25 with 4px gaps; segment texts and border colors: `session=SID-0001` blue `#2a78d6` (width 128), `HttpOnly` green `#008300` (width 82), `Secure` aqua `#199e70` (width 66), `SameSite=Lax` violet `#4a3aa7` (width 110), `Path=/` yellow `#c98500` (width 62), `Max-Age=86400` orange `#d95926` (width 122); box fills = border color at 0.12 alpha.
- **Callouts:** 1px `#6b7280` connector lines from each box to alternating 12px labels — above at y=85: "the ticket itself" (blue, over box 1), "no script access" (green, over box 2), "SameSite=Lax: cross-site sends stripped" (violet, over box 4), "expires in 1 day" (orange, over box 6); below at y=225: "https only" (aqua, under box 3), "only URLs under /" (yellow, under box 5). Label colors match their segment.
- **Annotation (bold 13px ink `#1a5276`, centered at y=262):** "everything after the first semicolon is a delivery rule, not data".
- **Caption (12px `#444`, bottom right):** "header from the coffee-shop example; session id is a made-up placeholder".

## What Lax, Strict, and None Actually Block

**Tags:** `worked example` (blue), `SameSite` (green), `cross-site` (orange)

- **Same-site request** — browsing the shop itself: Strict, Lax, and None all send the cookie
- **Top-level link click** — a link from a blog to the shop: Lax and None send it; Strict does not
- **Cross-site form POST** — a hidden form on evil-site posting to the shop: only None sends it
- **Cross-site img/fetch** — an `<img>` or fetch aimed at the shop from another site: only None sends it
- **None's price** — browsers accept `SameSite=None` only when the cookie is also marked `Secure`
- **HttpOnly check** — separately, `document.cookie` on the shop's own page returns nothing for it

*Example (italic):* An attacker's page auto-submits a form to the shop's checkout URL; with SameSite=Lax the browser strips the session cookie and the shop sees a logged-out request.

**Key point:** SameSite decides whether the cookie rides on requests that start from another site — Strict says never, Lax says only top-level navigations like link clicks, None says always (and demands Secure).

### Visualization (canvas `c2`, 720×300)

Matrix chart: four request scenarios (rows) against the three SameSite values (columns), each cell marked "sent" or "blocked".

- **Title (bold 15px, `#1a5276`, top center):** "Does the Browser Attach the Cookie? Strict vs Lax vs None".
- **Column headers (bold 13px `#1a5276`):** "Strict" at x=390, "Lax" at x=500, "None" at x=610, at y=75; thin 1px `#e5e9ef` gridlines separating columns at x=340, 450, 560.
- **Rows (12px `#444` labels at x=20, row centers at y = 105, 150, 195, 240):** "same-site request", "top-level link click", "cross-site form POST", "cross-site img / fetch"; 1px `#e5e9ef` gridline under each row.
- **Cells (bold 12px, centered under each column header):** row 1: "sent" blue `#2a78d6`, "sent" blue, "sent" blue; row 2: "blocked" green `#008300`, "sent" blue, "sent" blue; row 3: "blocked" green, "blocked" green, "sent" orange `#d95926`; row 4: "blocked" green, "blocked" green, "sent" orange.
- **Color logic:** blue = normal same-site or user-initiated send, green = protection engaged, orange = cross-site credentialed send (the risky cells).
- **Annotation (bold 13px green `#008300`, centered at y=272):** "Lax blocks the classic forged-POST attack by default".
- **Caption (12px `#444`, bottom right):** "sent/blocked rows follow the SameSite rules; scenarios illustrative".

## Stolen Sessions, Forged Clicks, and 400-Day Cookies

**Tags:** `where it's used` (blue), `session security` (green), `lifetimes` (orange)

- **Session hijack** — steal the value and you are the customer; HttpOnly blocks the easiest theft, an injected script reading `document.cookie`
- **Wire sniffing** — Secure keeps the cookie out of plain-HTTP requests on shared wifi
- **CSRF defense** — SameSite=Lax means a forged cross-site POST arrives with no credentials
- **Analytics lifetimes** — Max-Age decides how long a visitor counts as "the same visitor"
- **The 400-day cap** — Chrome and Safari (newer cookie spec) clamp Max-Age over 400 days to 400

*Example (italic):* An analytics cookie set with a two-year Max-Age actually expires after 400 days, so year-two "returning visitor" counts quietly turn into "new visitors".

**Key point:** The same three attributes are the front line for three different jobs — HttpOnly against theft, Secure against eavesdropping, SameSite against forgery — while Max-Age quietly shapes your metrics.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of cookie lifetimes for four cookie types, showing the ~400-day browser cap clipping an over-long analytics cookie.

- **Title (bold 15px, `#1a5276`, top center):** "Max-Age Decides How Long the Browser Keeps It".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 0.6 px per day (440px = 730 days); vertical dashed `#6b7280` (dash 4/3) cap line at x=471 (day 400) with 12px `#6b7280` label "~400-day cap" at its top.
- **Rows (top to bottom at y = 80, 125, 170, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "session cookie — no Max-Age": blue `#2a78d6` bar width 8 with 11px mute label "until browser closes"
  - "login remember-me — 30 days": blue bar width 18
  - "consent banner — 180 days": blue bar width 108
  - "analytics visitor id — asks 730 days": aqua `#199e70` solid bar width 241 (kept 400 days), dashed orange `#d95926` outline continuing to width 440 with 11px orange label "clipped"
- **Bar style:** 14px tall, solid bars fill `rgba(42,120,214,0.30)` (aqua row `rgba(25,158,112,0.30)`), 11px day labels at bar ends.
- **Annotation (bold 13px orange `#d95926`, right side near y=255):** "asking for 2 years gets you 400 days".
- **Caption (12px `#444`, bottom right):** "day counts illustrative except the ~400-day cap; pixel widths schematic; session bar symbolic".

## Secure Does Not Mean Encrypted

**Tags:** `common mistake` (red), `bearer token` (orange)

- **The belief** — developers read "Secure" as "the cookie value is encrypted or hashed"
- **The reality** — Secure only picks which trips may carry it: https yes, plain http no
- **Still plain text** — the value sits readable in the browser's cookie jar and in server logs
- **The bearer rule** — anyone who presents `SID-0001`, attacker or customer, is that session
- **The twin trap** — HttpOnly hides the cookie from scripts; it does not stop a forged request the browser sends itself — that is SameSite's job

*Example (italic):* A support engineer pastes a HAR file into a public ticket; the Secure, HttpOnly cookie is right there in plain text, and replaying it signs the attacker in.

**Common mistake:** Treating attributes as protection for the value. HttpOnly, Secure, and SameSite only control who can read the cookie or trigger its sending — the value itself is a plain-text bearer token, so it still needs to be random, short-lived, and revocable.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same Secure cookie on a plain-HTTP trip (withheld) vs an HTTPS trip (sent — and still readable at the far end).

- **Title (bold 15px, `#1a5276`, top center):** "'Secure' Controls the Trip, Not the Contents".
- **Row 1 (y=95), label 12px `#444` at x=20:** "plain http://"; blue `#2a78d6` rounded box at x=160 labeled "GET /orders over http" (12px), 3px arrow to a green `#008300` box at x=420 labeled "browser withholds cookie" with bold 12px green "✓ Secure did its job".
- **Row 2 (y=205), label:** "over https://"; blue box at x=160 "GET /orders over https", 3px arrow to a blue box at x=380 labeled "cookie sent: session=SID-0001", then arrow to an orange `#d95926` box at x=575 labeled "readable in logs & HAR files".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "the trip is protected; the value never is".
- **Caption (12px `#444`, bottom right):** "session id is a made-up placeholder".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the session id `SID-0001`, Max-Age 86400, and the lifetime days 30/180/730 are invented and labeled illustrative; the c2 sent/blocked matrix follows the SameSite rules (Strict never cross-site, Lax only top-level link clicks, None always but requires Secure); the ~400-day Max-Age cap is a real modern-browser limit; c3 bar widths use 0.6 px/day with the session bar symbolic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: session ids, cookie values, tracking ids, token parts, and example passwords on this page are made-up placeholders — for illustration only, and to avoid false positives from secret scanners."
