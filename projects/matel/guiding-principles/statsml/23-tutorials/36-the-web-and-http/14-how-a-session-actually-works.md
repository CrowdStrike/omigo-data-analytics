# How a Session Actually Works

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** How a Session Actually Works

**Subtitle:** HTTP forgets you after every request — a session is the server's memory of one login, carried back and forth as a tiny random id in a cookie

## One Login, Traced Request by Request

**Tags:** `core idea` (blue), `cookies` (green), `HTTP is stateless` (orange)

- **The login** — Maya POSTs her email and password once to a coffee shop's online order site
- **The stateless problem** — HTTP forgets her the moment the response ends; each request starts blank
- **The session** — the server invents a random id `SID-0001` and files "SID-0001 = maya" in a table
- **The cookie** — the response says `Set-Cookie: sid=SID-0001`; her browser attaches it from then on
- **The lookup** — on `GET /orders` the server reads the cookie, finds the row, and knows it's Maya
- **The logout** — `POST /logout` deletes the row; the same cookie now matches nothing

*Example (italic):* Maya types her password exactly once at 9:00; the cookie SID-0001 answers for her on every request after that.

**Key point:** A session is a server-side row keyed by a random id; the cookie only carries the id back, which is how the server recognizes an anonymous next request as Maya's.

### Visualization (canvas `c1`, 720×300)

Sequence diagram of one login traced end to end: two vertical lifelines (browser left, server right) with six horizontal message arrows between them.

- **Title (bold 15px, `#1a5276`, top center):** "One Login, Six Messages: the Life of Cookie sid=SID-0001".
- **Lifelines:** vertical 2px `#1a5276` lines at x=150 (browser) and x=570 (server), from y=60 to y=275; bold 13px `#1a5276` labels "Maya's browser" and "server" above them at y=50.
- **Arrows (3px, filled triangle heads, one per row; 12px label above each arrow, colored like it):**
  - y=90, right, blue `#2a78d6`: "POST /login  email + password"
  - y=120, left, green `#008300`: "200 OK  Set-Cookie: sid=SID-0001"
  - y=155, right, blue `#2a78d6`: "GET /orders  Cookie: sid=SID-0001"
  - y=185, left, green `#008300`: "200 OK  Hi Maya — 3 past orders"
  - y=220, right, orange `#d95926`: "POST /logout  Cookie: sid=SID-0001"
  - y=250, left, mute `#6b7280`: "200 OK  row SID-0001 deleted"
- **Server-side note:** small rounded box (fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text) at x=556 width 156 to the right of the server lifeline at y=105, "table: SID-0001 = maya".
- **Annotation (bold 13px violet `#4a3aa7`, centered between lifelines at y=270):** "the password crosses the wire once; the id crosses every time".
- **Caption (12px `#444`, bottom right):** "session id is a made-up placeholder; real ids are long and random".

## Thirty Minutes on the Clock: the Session Table

**Tags:** `worked example` (blue), `expiry` (green), `sliding timeout` (orange)

- **The row** — sid `SID-0001`, user maya, created 9:00, expires 9:30: one table row per login
- **Sliding timeout** — every request pushes the expiry 30 minutes forward from that moment
- **9:05 request** — the expiry slides from 9:30 to 9:35
- **9:12 request** — the expiry slides again, from 9:35 to 9:42
- **The gap** — Maya walks away; no request lands between 9:12 and 9:50
- **9:50 request** — it arrives after 9:42, so the row is dead: 401, back to the login page

*Example (italic):* Two quick clicks at 9:05 and 9:12 keep Maya's session alive, but a 38-minute coffee break out-waits the 30-minute timer.

**Key point:** Expiry lives on the server row, not in the cookie — the browser can keep sending SID-0001 forever; the server just stops honoring it after 9:42.

### Visualization (canvas `c2`, 720×300)

Top half: the session table drawn as one header row plus one data row. Bottom half: a 9:00–10:00 timeline showing each request sliding the expiry forward until the timeout wins.

- **Title (bold 15px, `#1a5276`, top center):** "One Row per Login: sid SID-0001 and Its Sliding 30-Minute Expiry".
- **Table (y=55 to y=115):** four columns at x = 80, 240, 400, 560, width 150 each; header cells fill `rgba(26,82,118,0.12)`, bold 12px `#1a5276` labels `sid` / `user` / `created` / `expires`; data cells fill `rgba(42,120,214,0.10)`, 12px `#2c3e50` values `SID-0001` / `maya` / `9:00` / `9:30 → 9:35 → 9:42`.
- **Timeline:** horizontal 2px `#999` axis at y=225 from x=60 to x=660 mapping 9:00–10:00 linearly (10 px per minute); 12px `#444` tick labels at 9:00 / 9:15 / 9:30 / 9:45 / 10:00.
- **Request markers:** blue `#2a78d6` filled circles (r=5) on the axis at minutes `[0, 5, 12]` (9:00 login, 9:05, 9:12), each with a 12px blue label above.
- **Expiry arcs:** dashed 2px green `#008300` arrows from each request marker to its expiry minute — pairs `[[0,30],[5,35],[12,42]]` — with a small green tick at each landing point.
- **The miss:** red `#e74c3c` X (bold, 16px) at minute 50 labeled "9:50 request — 401" in bold 12px red; the span from minute 42 to 50 shaded `rgba(231,76,60,0.12)` and labeled "expired at 9:42" in 12px `#6b7280`.
- **Annotation (bold 13px green `#008300`, near x=180, y=160):** "every click buys 30 more minutes".
- **Caption (12px `#444`, bottom right):** "times illustrative; 30-min idle timeout".

## Why "I Got Logged Out" Lands on Your Desk

**Tags:** `where it's used` (blue), `debugging` (orange), `analytics` (green)

- **The bug report** — "the site logged me out" is one symptom with at least four different root causes
- **Idle expiry** — the commonest cause: the user out-waited the timeout, working exactly as designed
- **Server restart** — sessions kept only in memory vanish on deploy; everyone is logged out at once
- **Cookie loss** — privacy modes, extensions, or a cleared cache drop the sid before the server does
- **Analytics** — session rows are the raw data behind every "average session length" dashboard
- **First question** — did the cookie stop arriving, or did the row stop existing? Always check which

*Example (italic):* 120 logout tickets in one month split into 54 idle expiries, 30 from a restart, 21 lost cookies, and 15 second-device logins.

**Key point:** Every "logged out" bug is either the cookie missing from the request or the row missing from the server — the fix is completely different for each side.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: 120 "I got logged out" support tickets from one month, broken down by root cause.

- **Title (bold 15px, `#1a5276`, top center):** "120 Logout Tickets, Four Root Causes".
- **Layout:** left-aligned 12px `#444` row labels at x=20, bars start at x=230, max bar width 440 (scale: 440 px for 54 tickets); bars 22px tall; bold 12px count labels at each bar end, colored like the bar.
- **Rows (top to bottom at y = 80, 130, 180, 230):**
  - "idle expiry (by design)": green `#008300` bar width 440, label "54"
  - "server restart wiped memory": orange `#d95926` bar width 244, label "30"
  - "cookie blocked or cleared": blue `#2a78d6` bar width 171, label "21"
  - "login on a second device": violet `#4a3aa7` bar width 122, label "15"
- **Bar fills:** solid at 85% opacity (e.g. `rgba(0,131,0,0.85)`), 1px darker stroke.
- **Annotation (bold 13px magenta `#d55181`, right side near y=55):** "only one of the four is actually a bug in your code".
- **Caption (12px `#444`, bottom right):** "ticket counts illustrative".

## The Cookie Holds a Key, Not the Login

**Tags:** `common mistake` (red), `JWT` (orange), `security` (green)

- **The mistake** — thinking the cookie stores the password or username; it stores only the random id
- **Opaque key** — `SID-0001` means nothing by itself; all the meaning lives in the server's table row
- **The JWT twist** — a signed token packs "user=maya, exp=9:42" inside the cookie itself, no table
- **Trade-off** — JWTs skip the table lookup but can't be revoked by deleting a row; they die only at exp
- **Theft** — whoever holds a valid sid IS Maya to the server; hence HTTPS and HttpOnly cookies
- **Clearing** — deleting the cookie logs the browser out but leaves the server row alive until expiry

*Example (italic):* Maya clears her cookies at 9:15, yet row SID-0001 stays valid until 9:42 — a stolen copy of the cookie would still work.

**Common mistake:** Treating "session" as one thing. A session-id cookie is a key into server state; a JWT is the state itself, signed. Revocation, expiry, and theft behave differently for each — logout on one is a delete, on the other a wait.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram comparing the two designs: an opaque session id resolved by a table lookup vs a self-contained signed JWT verified with a secret.

- **Title (bold 15px, `#1a5276`, top center):** "Session Id vs JWT: Key to a Row vs the Row Itself".
- **Row 1 (y=100), label 12px `#444` at x=20:** "session id"; blue `#2a78d6` rounded box at x=140 labeled "cookie: sid=SID-0001" (12px), 3px arrow to a blue box at x=360 labeled "table lookup", arrow to a green `#008300` box at x=560 labeled "row says: maya" with bold 12px green "revoke = delete the row".
- **Row 2 (y=210), label:** "JWT"; orange `#d95926` rounded box at x=140 labeled "cookie: user=maya, exp=9:42, sig", 3px arrow to an orange box at x=390 labeled "check signature", arrow to a green box at x=580 labeled "trusted as-is" with bold 12px red `#e74c3c` "no row to delete".
- **Box style:** 140–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "one design remembers on the server; the other trusts the math".
- **Caption (12px `#444`, bottom right):** "token contents simplified for display".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the session id `SID-0001`, clock times (9:00 login, requests at 9:05 and 9:12, expiries 9:30 → 9:35 → 9:42, miss at 9:50), and the ticket counts (54 / 30 / 21 / 15, total 120) are invented and labeled illustrative; c3 bar widths use the fixed scale 440 px per 54 tickets (244 / 171 / 122).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: session ids, cookie values, tracking ids, token parts, and example passwords on this page are made-up placeholders — for illustration only, and to avoid false positives from secret scanners."
