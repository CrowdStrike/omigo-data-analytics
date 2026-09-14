# CSRF

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** CSRF

**Subtitle:** A malicious page can make your browser send a request to your bank — and your browser helpfully attaches your login cookie, so the bank thinks it's you

## The Browser That Never Forgets Your Cookie

**Tags:** `core idea` (blue), `confused deputy` (orange), `cookies` (green)

- **The habit** — your browser attaches bank.example's cookies to every request it sends to bank.example
- **No questions asked** — it attaches them no matter which page triggered the request, bank tab or not
- **The trick** — a malicious page you happen to visit auto-submits a hidden form aimed at bank.example
- **The bank's view** — the request arrives with your valid session cookie; it looks exactly like you
- **Confused deputy** — the attacker never steals the cookie; your browser is tricked into spending its own authority

*Example (italic):* You open a cooking-forum thread at 10:07am; a hidden form in one post fires a transfer request at your bank, and your browser attaches the session cookie it got when you logged in at 10:00.

**Key point:** This is cross-site request forgery (CSRF): a page on one site forges a request to another site, and the browser's automatic cookie-attachment turns that forgery into an authenticated request.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: a legitimate transfer request vs a forged one, showing that the bank receives the same cookie either way.

- **Title (bold 15px, `#1a5276`, top center):** "Two Requests, One Cookie: the Bank Cannot Tell Them Apart".
- **Row 1 (boxes centered on y=100), label 12px `#444` at x=20:** "you click"; blue `#2a78d6` rounded box at x=110 labeled "bank.example transfer page" (12px), 3px `#2a78d6` arrow to a violet `#4a3aa7` box at x=330 labeled "POST /transfer + session cookie", 3px arrow to a green `#008300` box at x=560 labeled "bank: valid session ✓".
- **Row 2 (boxes centered on y=210), label:** "attacker's page"; orange `#d95926` rounded box at x=110 labeled "evil.example hidden form", 3px `#d95926` arrow to a violet box at x=330 labeled "POST /transfer + session cookie", 3px arrow to a green box at x=560 labeled "bank: valid session ✓".
- **Box style:** 170px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(74,58,167,0.10)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, two lines max.
- **Bracket:** dashed `#6b7280` (dash 4/3) vertical brace linking the two violet boxes at x=330, y=125 to y=185.
- **Annotation (bold 13px magenta `#d55181`, centered at y=270):** "the cookie rides along both times — without a token or SameSite the bank cannot tell them apart".
- **Caption (12px `#444`, bottom right):** "cookie value illustrative".

## Forging a $500 Transfer With a Hidden Form

**Tags:** `worked example` (blue), `hidden form` (orange), `blind fire` (red)

- **The login** — at 10:00 you sign in; the bank sets a session cookie, valid for the rest of the day
- **The bait** — at 10:07 a forum post hides `<form action="https://bank.example/transfer" method="POST">`
- **The payload** — hidden fields `to=8841` and `amount=500`; one line of script submits the form on page load
- **The ride** — your browser sends the POST and attaches the session cookie out of habit
- **Money moves** — the bank checks the cookie, finds a valid session, and sends $500 to account 8841
- **Blind fire** — the response returns to your browser, not the attacker: CSRF can write, never read

*Example (italic):* The attacker never sees your balance or the "transfer complete" page — that is why CSRF targets state-changing actions (transfer money, change email, delete account), not data theft.

**Key point:** A forged request needs zero secrets from the attacker — the cookie, the only credential involved, is supplied by the victim's own browser.

### Visualization (canvas `c2`, 720×300)

Left-to-right anatomy of the forged request, with a blocked return path showing the attacker cannot read the response.

- **Title (bold 15px, `#1a5276`, top center):** "Anatomy of the Forgery: Attacker Writes, Never Reads".
- **Attacker box (orange `#d95926`, x=30, y=70, 190×80, fill `rgba(217,89,38,0.12)`):** heading bold 12px "hidden form on evil.example", lines 12px mono `to=8841`, `amount=500`.
- **Request box (violet `#4a3aa7`, x=270, y=60, 200×100, fill `rgba(74,58,167,0.10)`):** heading bold 12px "POST /transfer", lines 12px mono `Cookie: (session id)`, `to=8841&amount=500`, italic 11px `#6b7280` note "cookie added by the browser".
- **Bank box (green `#008300`, x=520, y=70, 180×80, fill `rgba(0,131,0,0.12)`):** heading bold 12px "bank.example", line 12px "session valid ✓", bold 12px green "$500 → account 8841".
- **Forward arrows:** 3px `#2c3e50` from attacker box to request box (y=110), request box to bank box (y=110).
- **Response arrow:** 2px dashed `#6b7280` (dash 5/4) from bank box bottom (x=610, y=150) curving left to a small blue `#2a78d6` box at x=270, y=210, 200×44 labeled "response → victim's browser only".
- **Blocked path:** 2px red `#e74c3c` line from the response box toward x=80, y=232, ending 20px short at a bold 16px red "✗"; bold 12px red label under it "attacker cannot read it".
- **Annotation (bold 13px `#1a5276`, right side at y=270):** "state-changing requests fired blind".
- **Caption (12px `#444`, bottom left):** "account, amount, cookie illustrative".

## Tokens and SameSite: Making Forgeries Detectable

**Tags:** `where it's used` (blue), `defense` (green), `SameSite` (orange)

- **CSRF token** — the bank embeds a per-session secret token inside every legitimate form
- **Why it works** — a cross-site page cannot read bank.example's HTML, so the forged POST arrives token-less
- **Server check** — missing or wrong token means reject with 403, even though the cookie itself is valid
- **SameSite cookies** — modern browsers default to `SameSite=Lax`: cookies stay home on cross-site POSTs
- **The effect** — the forged request arrives cookie-less and looks logged-out; this default killed most classic CSRF
- **Double-submit** — the token also rides in a cookie; the server just checks form value equals cookie value

*Example (italic):* The same hidden form fires after the fixes and fails twice over — SameSite strips the cookie, and the missing token earns a 403 — so the $500 never moves.

**Key point:** Every CSRF defense works the same way: demand something a cross-site attacker can neither read (the token) nor make the browser attach (a SameSite cookie).

### Visualization (canvas `c3`, 720×300)

Three-row gauntlet: the identical forged POST hitting a bank with no defense, with a CSRF token check, and with SameSite cookies.

- **Title (bold 15px, `#1a5276`, top center):** "One Forged POST, Three Banks: Only the Undefended One Pays".
- **Rows centered on y = 85, 160, 235; each starts with the same orange `#d95926` box at x=25, 150×40, fill `rgba(217,89,38,0.12)`, label 12px "forged POST from evil.example".
- **Row 1, defense label bold 12px `#444` at x=210:** "no defense"; 3px arrow to a red `#e74c3c` box at x=480, 210×40, fill `rgba(231,76,60,0.12)`, bold 12px red "✗ $500 sent to 8841".
- **Row 2, label:** "per-request token check"; 3px arrow to a green `#008300` box at x=480, 210×40, fill `rgba(0,131,0,0.12)`, bold 12px green "✓ 403 — token missing".
- **Row 3, label:** "SameSite=Lax cookie"; mid-arrow bold 12px `#6b7280` note at x=300 "browser withholds the session cookie"; 3px arrow to a green box at x=480, bold 12px green "✓ treated as logged out".
- **Box style:** 8px radius, 12px `#2c3e50` text unless colored above; arrows 3px `#2c3e50` with solid triangular heads.
- **Annotation (bold 13px green `#008300`, centered at y=278):** "success for the defender is a rejected request".
- **Caption (12px `#444`, bottom right):** "token and cookie values illustrative".

## The Logout Link in an Img Tag

**Tags:** `common mistake` (red), `GET vs POST` (orange)

- **The prank** — a forum avatar set to `<img src="https://bank.example/logout">` logs out everyone who views it
- **Why it works** — browsers fetch images with a plain GET, cookie attached; if GET changes state, viewing = clicking
- **The rule** — GET must never change state: reads only; every transfer, delete, or logout goes through POST + token
- **The trap** — tokens on your forms don't help if one forgotten GET endpoint like `/delete?id=7` still mutates
- **Harmless demo, real lesson** — the same img tag pointed at `/transfer?to=8841&amount=500` is not a prank

*Example (italic):* One prank avatar silently logs 300 thread readers out of their webmail — annoying, harmless, and mechanically identical to a real CSRF attack.

**Common mistake:** Believing CSRF needs the victim to click anything — an auto-submitting form or an img tag fires the moment the page loads, and a state-changing GET makes the img tag enough.

### Visualization (canvas `c4`, 720×300)

Two-row comparison: a state-changing GET falls to the img-tag prank; a POST-plus-token endpoint shrugs it off.

- **Title (bold 15px, `#1a5276`, top center):** "Why GET Must Never Change State".
- **Row 1 (boxes centered on y=105), label bold 12px `#444` at x=20:** "GET /logout"; blue `#2a78d6` rounded box at x=130 labeled "forum page renders <img src=.../logout>", 3px arrow with 12px `#6b7280` mid-label "GET + cookie" to a red `#e74c3c` box at x=470 labeled "session destroyed", bold 12px red tag at x=470, y=65 "✗ 300 readers logged out".
- **Row 2 (boxes centered on y=215), label:** "POST + token"; blue box at x=130 labeled "same img tag, same page", 3px arrow with 12px `#6b7280` mid-label "img can only GET" to a green `#008300` box at x=470 labeled "endpoint ignores GET", bold 12px green tag at x=470, y=175 "✓ nothing happens".
- **Box style:** 200px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, two lines max.
- **Annotation (bold 13px orange `#d95926`, centered at y=272):** "an img tag is a GET request you never agreed to send".
- **Caption (12px `#444`, bottom right):** "reader count illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all boxes, arrows, and labels use the hardcoded coordinates and strings above (no randomness); the session cookie and token are drawn generically with no literal values; account 8841, the $500 amount, the 10:00/10:07 timestamps, and the 300 logged-out readers are invented and labeled illustrative; text numbers and chart numbers must stay in sync if edited.
- **Framing:** defensive/educational throughout — the page teaches how CSRF works so the reader can defend against it; no real sites or exploit code beyond the schematic hidden-form fields shown.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: realistic credential strings on this page were converted to generic placeholders — for illustration only, and to avoid false positives from secret scanners."
