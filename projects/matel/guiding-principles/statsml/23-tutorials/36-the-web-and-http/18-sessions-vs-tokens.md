# Sessions vs Tokens

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Sessions vs Tokens

**Subtitle:** After you log in, the server can either remember you in its own ledger (a session) or hand you signed proof to carry (a token) — a coat-check ticket vs a stamped wristband

## The Coat-Check Ticket and the Wristband

**Tags:** `core idea` (blue), `session` (green), `token` (orange)

- **The login** — Maya signs into an online bookstore at 9:00am with her password, once
- **The ticket** — a session gives her ticket #4127: a meaningless number, like a coat-check stub
- **The ledger** — the server keeps a row "4127 → Maya"; the ticket only works because the ledger exists
- **The wristband** — a token instead hands her a signed note: "Maya, valid until 9:15" with a stamp
- **The check** — each later request shows the proof: ticket means "look me up", band means "read me"

*Example (italic):* Maya's next click sends ticket #4127 and the server consults its ledger — or sends the stamped band and the server just verifies the stamp.

**Key point:** A session stores the truth on the server and gives you a reference; a token puts the truth in your hand, signed so it can't be forged.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the same request proven two ways — a session ticket resolved through a server-side ledger vs a signed token verified on the spot.

- **Title (bold 15px, `#1a5276`, top center):** "Same Login, Two Proofs: Look It Up vs Read the Stamp".
- **Row 1 (y=95), label 12px `#444` at x=20:** "session (coat check)"; blue `#2a78d6` rounded box at x=160 labeled "browser sends ticket #4127" (12px), 3px arrow to a violet `#4a3aa7` box at x=370 labeled "server opens the ledger", 3px arrow to a green `#008300` box at x=570 labeled "row 4127: Maya ✓".
- **Row 2 (y=205), label:** "token (wristband)"; blue box at x=160 labeled "browser sends band: Maya, exp 9:15 + stamp", 3px arrow to a green box at x=430 labeled "server checks the stamp — no ledger ✓".
- **Box style:** 140–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the ticket means nothing without the ledger; the band carries its own proof".
- **Caption (12px `#444`, bottom right):** "ticket number and times illustrative".

## A Thousand Requests, Two Bills

**Tags:** `worked example` (blue), `lookup cost` (green), `scaling` (orange)

- **The traffic** — Maya's afternoon of browsing sends 1,000 requests to the bookstore
- **Session bill** — every request costs one ledger lookup: 1,000 lookups against the session store
- **Token bill** — every request costs one signature check on the server's own CPU: 0 lookups
- **The clock** — a ledger round trip adds about 2.0 ms per request; a stamp check about 0.2 ms
- **One server** — a ledger kept in one server's memory means only that server can honor #4127
- **Four servers** — the band verifies on any of 4 servers; the ledger must be shared or requests pinned

*Example (italic):* Moving the bookstore from 1 server to 4 changes nothing for token holders, but session tickets now need a shared ledger every server can reach.

**Key point:** Sessions pay a lookup on every request and tie you to wherever the ledger lives; tokens pay only local math, so any server can answer.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: four rows comparing the per-request bill for sessions vs tokens over Maya's 1,000-request afternoon.

- **Title (bold 15px, `#1a5276`, top center):** "The Bill for 1,000 Requests: Ledger Lookups vs Stamp Checks".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 420; pixel widths schematic, not to a shared scale across units.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "session — ledger lookups": blue `#2a78d6` bar width 420, 11px label "1,000" at bar end
  - "token — ledger lookups": green `#008300` bar width 3, 11px label "0"
  - "session — extra time per request": orange `#d95926` bar width 200, 11px label "2.0 ms"
  - "token — extra time per request": aqua `#199e70` bar width 20, 11px label "0.2 ms"
- **Bar style:** 14px tall, fills at full opacity for the thin bars, `rgba(42,120,214,0.30)` and `rgba(217,89,38,0.30)` for the wide session bars, 2px solid edge in the row color.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "the ledger is a round trip on every request; the stamp is local math".
- **Caption (12px `#444`, bottom right):** "counts and times illustrative; widths schematic".

## Picking One for a Web App or an API

**Tags:** `where it's used` (blue), `web apps` (green), `APIs` (orange)

- **Classic web app** — one site, one server pool, browser cookies: the coat-check ticket fits naturally
- **Public API** — phones, scripts, and partners calling 4 services: a band any server can read wins
- **Microservices** — a request that hops 3 services would re-ask the ledger at every hop with sessions
- **Instant logout** — banking-style apps want a kill switch; the ledger gives it, the band does not
- **Middle path** — short-lived tokens plus a refresh step blend the two: cheap checks, bounded risk

*Example (italic):* The bookstore's website keeps sessions for shoppers, but its partner API issues 15-minute tokens so all 4 API servers can verify without a shared ledger.

**Key point:** Choose by who must verify and how fast you must revoke: few verifiers and instant logout favor sessions; many independent verifiers favor tokens.

### Visualization (canvas `c3`, 720×300)

Two-row architecture diagram: a single web server with a ledger (session fit) vs a phone calling four API servers that each verify locally (token fit).

- **Title (bold 15px, `#1a5276`, top center):** "One Door and a Ledger vs Four Doors Reading the Band".
- **Row 1 (y=90), label 12px `#444` at x=20:** "web app + sessions"; blue `#2a78d6` rounded box at x=150 labeled "browser (ticket #4127)", 3px arrow to a blue box at x=350 labeled "web server", 3px arrow to a violet `#4a3aa7` box at x=550 labeled "session ledger".
- **Row 2 (y=175 to y=265), label at x=20:** "API + tokens"; blue box at x=150 (y=210) labeled "phone (signed band)", four 3px arrows fanning to four green `#008300` boxes stacked at x=430, y = 170, 200, 230, 260, each 12px labeled "api-1 ✓" … "api-4 ✓" with 11px `#008300` note "checks stamp locally" beside the stack.
- **Box style:** 130–170px wide, 34–40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, right side near y=130):** "no shared ledger: any of the 4 servers can answer".
- **Caption (12px `#444`, bottom right):** "server counts illustrative".

## The Wristband You Can't Take Back

**Tags:** `common mistake` (red), `revocation` (orange)

- **The disable** — at 3:00pm the bookstore disables Maya's account after her laptop is stolen
- **Session dies** — the ledger row for #4127 is deleted; the thief's very next request is refused
- **Band lives** — the token's stamp is still mathematically valid; nothing on the server changed
- **The window** — with a 15-minute expiry, a band issued just before 3:00pm works until 3:15pm
- **The other trap** — teams forget the ledger needs shared storage; a redeploy logs everyone out
- **Blocklists** — a token deny-list restores instant revocation but re-adds a lookup per request

*Example (italic):* After the 3:00pm disable, the stolen session fails on its next click, but a stolen token keeps working for up to 15 more minutes.

**Common mistake:** Believing tokens can be revoked instantly. A signed token stays valid until it expires — the fix (a deny-list checked on every request) quietly turns your tokens back into sessions.

### Visualization (canvas `c4`, 720×300)

Step-line timeline: whether the thief's requests are accepted in the minutes around the 3:00pm disable — session access dies at once, token access survives to expiry.

- **Title (bold 15px, `#1a5276`, top center):** "Disabled at 3:00pm: the Ticket Dies Instantly, the Band Lives 15 More Minutes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes relative to the disable, -5 to 20, 12px `#444` tick labels at -5/0/5/10/15/20; y = two levels, 12px `#6b7280` labels "accepted" at y=110 and "denied" at y=220, gridlines `#e5e9ef` at both levels.
- **Session step line:** green `#008300` 3px through minutes `[-5, 0]` at the accepted level, vertical drop at minute 0, then `[0, 20]` at the denied level.
- **Token step line:** orange `#d95926` 3px through minutes `[-5, 15]` at the accepted level (drawn 6px above the session segment where they overlap), vertical drop at minute 15, then `[15, 20]` at the denied level.
- **Danger band:** fill `rgba(217,89,38,0.12)` rectangle from minute 0 to 15 between the accepted level and the baseline, bold 13px orange `#d95926` label inside: "stolen band still works".
- **Markers:** vertical dashed `#6b7280` (dash 4/3) lines at minute 0 labeled "account disabled" and minute 15 labeled "token expires" (12px `#6b7280`, at top).
- **Annotation (bold 13px green `#008300`, near x=6 min, y=235):** "session refused on the very next request".
- **Caption (12px `#444`, bottom right):** "15-minute expiry illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); ticket #4127, the 1,000-request afternoon, 1,000 vs 0 lookups, 2.0 vs 0.2 ms, 4 servers, and the 15-minute expiry window are invented and labeled illustrative; c4's step lines use the minute breakpoints `[-5, 0, 15, 20]` exactly as listed.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
