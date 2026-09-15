# HTTP Basic Auth

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** HTTP Basic Auth

**Subtitle:** The oldest login on the web — no login page, no session, no token: the browser just attaches your name and secret to every single request as one header

## The Login That Is Only a Header

**Tags:** `core idea` (blue), `401 challenge` (green), `Authorization header` (orange)

- **The setup** — Alice opens an internal metrics dashboard at `/reports` with no login page anywhere
- **The refusal** — the server answers `401 Unauthorized` plus a header naming which door is locked
- **The popup** — that header makes the browser itself draw a small name-and-secret box, not the site
- **The retry** — Alice fills it in; the browser repeats the exact same request with one extra header
- **The header** — `Authorization: Basic <encoded name and secret>` is the entire login mechanism
- **The repeat** — there is no session and no token, so that header rides along on every later request

*Example (italic):* Alice's first request to `/reports` is refused with a 401; her second is byte-identical apart from one added `Authorization` header, and it succeeds.

**Key point:** Basic auth has no login page, no server-side memory, and no ticket — the credential itself is re-sent as a header on every request, forever.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the 401 challenge on the first request, then the identical request replayed with one added header.

- **Title (bold 15px, `#1a5276`, top center):** "Attempt 1 Is Refused, Attempt 2 Adds One Header".
- **Row 1 (y=100), label 12px `#444` at x=20:** "attempt 1"; blue `#2a78d6` rounded box at x=150 labeled "GET /reports" / "(no Authorization)" (12px), 3px arrow to a red `#e74c3c`-edged box at x=390 labeled "401 Unauthorized" / "WWW-Authenticate: Basic".
- **Row 2 (y=210), label at x=20:** "attempt 2"; blue box at x=150 labeled "GET /reports" / "+ Authorization: Basic …", 3px arrow to a green `#008300` box at x=390 labeled "200 OK" / "the dashboard renders".
- **Box style:** 170–230px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.10)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "the browser draws the box — the site never renders a login form".
- **Caption (12px `#444`, bottom right):** "illustrative example".

## Encoding Is Not Encryption

**Tags:** `worked example` (blue), `base64` (green), `plainly reversible` (orange)

- **The input** — a 5-character name, one colon separator, and a 12-character secret: 18 bytes total
- **The transform** — base64 turns every 3 input bytes into 4 printable characters, no key involved
- **The output** — 18 bytes ÷ 3 × 4 = exactly 24 characters, and no `=` padding is needed here
- **The reversal** — anyone can decode those 24 characters back to the original 18 bytes instantly
- **The cost** — base64 grows the data by 4/3, about 33%, which is its only real side effect
- **The gap** — encoding makes bytes safe to put in a header; it hides nothing from anyone reading it

*Example (italic):* 18 bytes in → 24 base64 characters out → the same 18 bytes back, with no key at either step.

**Key point:** Base64 is a reformatting, not a lock. The header is exactly as readable as the secret typed into the box, so Basic auth is only ever as safe as the HTTPS carrying it.

### Visualization (canvas `c2`, 720×300)

Round-trip diagram with a size bar: 18 bytes encode to 24 characters and decode straight back, with the 1.33× growth shown.

- **Title (bold 15px, `#1a5276`, top center):** "18 Bytes In, 24 Characters Out, 18 Bytes Back — No Key Anywhere".
- **Round trip (y=110):** blue `#2a78d6` box at x=60 labeled "name : secret" / "18 bytes" (12px); 3px blue arrow right, labeled above in 12px `#444` "base64 encode"; violet `#4a3aa7` box at x=300 labeled "24 base64 chars" / "(what the header carries)"; 3px orange `#d95926` arrow right, labeled above "decode — no key"; orange-filled box at x=545 labeled "name : secret" / "18 bytes again".
- **Size bars (labels 12px `#444` at x=20):** at y=205 a blue bar from x=180 width 180 labeled "18 bytes (typed)"; at y=240 a violet bar from x=180 width 240 labeled "24 chars (sent)"; both 16px tall, fills `rgba(42,120,214,0.30)` and `rgba(74,58,167,0.25)` with 2px edges; 12px `#444` value labels at each bar end.
- **Annotation (bold 13px orange `#d95926`, right of the size bars near y=222):** "+33% size, 0% secrecy".
- **Caption (12px `#444`, bottom right):** "illustrative lengths; no real credential shown".

## Where a Data Scientist Still Meets It

**Tags:** `where it's used` (blue), `scripts and APIs` (green), `internal tools` (orange)

- **Command-line pulls** — a one-line `curl -u` fetch against an internal endpoint is Basic auth
- **Client libraries** — many HTTP clients take a name and secret pair and build the header for you
- **Legacy endpoints** — old internal reporting and admin pages often expose nothing else
- **Scheduled jobs** — a nightly extract that needs no browser is the case Basic auth genuinely fits
- **The leak path** — the same pair pasted into a notebook cell then committed to the repository
- **The URL variant** — some tools accept the pair inside the address, which lands it in access logs

*Example (italic):* A nightly job pulls a CSV from an internal endpoint with Basic auth; the same pair sits in a notebook cell three people later copy into their own scripts.

**Key point:** Basic auth survives because it needs no login flow at all — which is also why its credential ends up in shell history, notebooks, and log files.

### Visualization (canvas `c3`, 720×300)

Fan diagram: one credential pair spreading to the five places it typically comes to rest.

- **Title (bold 15px, `#1a5276`, top center):** "One Pair, Five Resting Places".
- **Source:** violet `#4a3aa7` box at x=40, y=150, 150×46, labeled "one name +" / "one secret" (12px).
- **Targets (x=430, five boxes 210×30 at y = 70, 115, 160, 205, 250):** "scheduled job config" (green `#008300`), "command-line history" (yellow `#c98500`), "notebook cell" (orange `#d95926`), "committed repo file" (red `#e74c3c`), "server access log" (red `#e74c3c`).
- **Arrows:** 2px lines from the right edge of the source box to each target's left edge, coloured per target.
- **Safety marking:** 11px labels right of each box — "intended" beside the first two (`#008300`), "leak path" beside the last three (`#e74c3c`).
- **Annotation (bold 13px red `#e74c3c`, bottom center near y=288):** "the credential is the message — every copy is a full copy".
- **Caption:** none (annotation occupies the bottom line).

## Basic vs Bearer: What Re-Sending the Secret Costs

**Tags:** `common mistake` (red), `no expiry` (orange)

- **The count** — 1,000 requests under Basic auth transmit the secret itself 1,000 times
- **The contrast** — a token flow sends the secret once at login, then a token on the other 999
- **The expiry** — Basic auth has none; the header stays valid until the secret is changed
- **The revocation** — cutting one script off means changing the secret, which breaks every other user
- **The scope** — a token can be limited to read-only; a name-and-secret pair carries full account power
- **The confusion** — `Basic` and `Bearer` look alike in a header, but only one carries the real secret

*Example (italic):* Revoking one nightly job's access under Basic auth means changing the shared secret, which also breaks the four other jobs using it.

**Common mistake:** Treating `Authorization: Basic` as equivalent to `Authorization: Bearer`. Bearer carries a scoped, expiring stand-in; Basic carries the account's actual credential, unscoped and unexpiring, on every request.

### Visualization (canvas `c4`, 720×300)

Grouped comparison: how often the real secret crosses the wire over 1,000 requests, plus a three-row property comparison.

- **Title (bold 15px, `#1a5276`, top center):** "Over 1,000 Requests: Secret Sent 1,000 Times vs Once".
- **Exposure bars (baseline x=210, max width 400):** at y=70 red `#e74c3c` bar width 400 labeled left "basic — secret sent", value "1,000"; at y=112 green `#008300` bar width 1 (drawn 3px minimum) labeled "bearer — secret sent", value "1"; 16px tall, red bar fill `rgba(231,76,60,0.30)` with 2px edge.
- **Property rows (three rows at y = 175, 212, 249, 12px `#444` labels at x=20):** "expires on its own", "can be scoped read-only", "revocable for one caller" — each with a red `#e74c3c` "✗ basic" at x=330 and a green `#008300` "✓ bearer" at x=470, both bold 12px; 1px `#e5e9ef` separator line above each row across x=20..700.
- **Annotation (bold 13px orange `#d95926`, right side near y=140):** "no expiry means the header works until the secret changes".
- **Caption (12px `#444`, bottom right):** "1,000-request figure illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Credential safety:** never print a real-looking name-and-secret pair or a decodable base64 credential string. The header is always shown truncated as `Authorization: Basic …`, and lengths are described as counts (18 bytes, 24 characters) rather than literal strings.
- **Data:** all values are hardcoded literals (no randomness). The 18 → 24 figure is arithmetic: base64 emits 4 characters per 3 input bytes, and 18 ÷ 3 × 4 = 24 with no padding; the 1.33× growth is 24 ÷ 18. The 1,000-request count and the 5+1+12-character lengths are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
