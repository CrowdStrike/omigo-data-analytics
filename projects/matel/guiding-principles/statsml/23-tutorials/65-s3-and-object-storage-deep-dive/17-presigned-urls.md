# Presigned URLs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Presigned URLs

**Subtitle:** Anyone holding a presigned link acts as whoever signed it until it expires — and the link can never outlive the credentials that signed it

## The App Hands Out a Link, Not the File

**Tags:** `core idea` (blue), `download and upload` (green), `no proxying` (orange)

- **The setup** — Alice signs in to a reporting app and asks for one private file
- **No streaming** — the app holds the credentials but sends back a link, not the bytes
- **The signing step** — the app signs the one exact request it is willing to allow
- **The link** — the signature travels inside the link, so it states its own permission
- **Straight to storage** — Alice's browser sends a plain request to S3 with that link
- **Nothing is stored** — S3 keeps no record of the link and recomputes the signature
- **Uploads work too** — a link signed for a write lets Alice send a file straight in
- **No login at S3** — whoever holds the link acts as the app that signed it, until expiry

*Example (illustrative):* The app's answer to "send me my report" is a short link; the 40 MB file then travels from S3 to Alice's browser, never through the app server.

**Key point:** A presigned link sends the permission through the app and the file around it. The app decides who may read one object; S3 does the reading.

### Visualization (canvas `c1`, 720×300)

Three-party flow diagram: browser, app server, S3. The link path runs through the app, the file path goes around it, and the path the bytes do not take is struck out.

- **Title (bold 15.5px, `#1a5276`, centered at y=24):** "Who Talks to Whom: the Link Goes Through the App, the File Does Not".
- **Three boxes (y=78, h=62, w=170, 8px radius, 2px border; bold 13px centered label at y=104, 11.5px sublabel at y=124):**
  - Browser (Alice) at x=30 (centre 115) — fill `rgba(42,120,214,0.12)`, border `#2a78d6`, sublabel "holds no credentials".
  - App server at x=275 (centre 360) — fill `rgba(74,58,167,0.12)`, border `#4a3aa7`, sublabel "holds the credentials".
  - S3 at x=520 (centre 605) — fill `rgba(0,131,0,0.10)`, border `#008300`, sublabel "checks the signature".
- **Arrow 1 (blue `#2a78d6`, 2px, x 200→275 at y=96):** bold 12px blue label "1. ask for a link" centred at (237, 88).
- **Arrow 2 (violet `#4a3aa7`, 2px, x 275→200 at y=126):** bold 12px violet label "2. the signed link" centred at (237, 146).
- **Blocked path (grey `#6b7280`, 1.5px, dashed 5/4, x 445→520 at y=110):** bold 14px red `#e74c3c` `✗` at (482, 106) and bold 11.5px red "no bytes" at (482, 132).
- **Arrow 3 (aqua `#199e70`, 2.5px):** down from (100, 140) to (100, 200), right to (575, 200), up into the S3 box with an arrowhead at (575, 142); bold 12px aqua label "3. request goes straight to S3" centred at (337, 192).
- **Arrow 4 (green `#008300`, 2.5px):** down from (635, 142) to (635, 236), left to (60, 236), up into the browser box with an arrowhead at (60, 142); bold 12px green label "4. the file comes straight back" centred at (347, 254). The return leg turns up at x=60, left of arrow 3's x=100, so the two paths never cross.
- **Annotation (bold 13.5px `#d55181`, centered at y=274):** "the file never passes through the app server".
- **Caption (12px `#444`, bottom right at y=294):** "flow illustrative; the self-describing signed request is documented".

## What the Signed Link Pins Down, and What It Leaves Open

**Tags:** `mechanism` (blue), `signed conditions` (green), `false assumption` (red)

- **What gets signed** — the action, the one file, and the expiry are all hashed together
- **One action only** — a link signed for a download cannot be turned into an upload
- **One file only** — the signature covers one exact key, so it cannot reach another
- **Any change breaks it** — alter one character of the link and the check simply fails
- **No size limit** — a signed upload link accepts a far bigger file than you meant
- **The gap is big** — one upload request accepts up to 5 GiB, or 1,024 times 5 MiB
- **The fix is a form upload** — its policy can cap the size before the write is allowed
- **Other conditions** — the same policy can require an exact file type or encryption

*Example (illustrative):* An upload link meant for a 5 MiB receipt would accept a 5 GiB file, so the app caps the size at 5,242,880 bytes with a form upload instead.

**Key point:** The signature fixes *which* request is allowed, not *how big* it is. A size limit is only possible with the form-upload style of signed link.

### Visualization (canvas `c2`, 720×300)

Schematic of the signed link drawn as four labelled parts, with a bracket marking the three that are hashed, and a right-hand column listing what the signature pins and what it does not. No real or realistic link text appears — only plain-English part names.

- **Title (bold 15.5px, `#1a5276`, centered at y=22):** "What the Signed Link Pins Down".
- **Left header (bold 12px `#1a5276`, left at (40, 48)):** "the signed link, in parts".
- **Four part boxes (x=40, w=300, h=32, 5px radius, tops at y = 60, 100, 140, 180):** label 12.5px `#2c3e50` at (52, top+21).
  - "the file's address" — fill `rgba(42,120,214,0.10)`, 1.5px `#2a78d6`
  - "the action allowed" — fill `rgba(42,120,214,0.10)`, 1.5px `#2a78d6`
  - "how long it stays valid" — fill `rgba(201,133,0,0.14)`, 1.5px `#c98500`
  - "the signature" — fill `rgba(213,81,129,0.14)`, 2.5px `#d55181`
- **Hashed bracket:** 2px `#1a5276` from (360, 62) to (352, 62) down to (352, 170) out to (360, 170); bold 12px `#1a5276` "hashed" left-aligned at (362, 120).
- **Signature tick:** 2px `#d55181` vertical at x=352 from y=182 to y=212; bold 12px `#d55181` "result" left-aligned at (358, 201).
- **Right column (x=430):** bold 12.5px `#1a5276` "the signature pins:" at y=70, then 12px `#2c3e50` lines at y = 92, 112, 132 — "· one action — download only", "· one file — that file alone", "· the expiry, in seconds".
- **Red column note (x=430):** bold 12.5px `#e74c3c` "it does not pin:" at y=170, then 12px `#e74c3c` lines at y = 192, 212 — "the size of an upload — one", "request accepts up to 5 GiB".
- **Fix strip (x=430, y=234, w=250, h=30, 6px radius):** fill `rgba(0,131,0,0.10)`, 2px `#008300` border, bold 11.5px `#008300` centred "a form upload can cap the size" at (555, 253).
- **Caption (12px `#444`, bottom right at y=294):** "the link is drawn as placeholder parts; the 5 GiB request limit is documented".

## Two Clocks: the Expiry You Ask For and the One You Inherit

**Tags:** `common mistake` (red), `worked example` (green), `two limits` (orange)

- **Clock one** — the lifetime you ask for, in seconds; seven days is the maximum
- **Clock two** — how long the credentials that signed the link stay valid themselves
- **Nobody passes it in** — the second clock is never part of the link you hand out
- **Where it bites** — code on a server or in a function signs with a short session
- **The worked case** — the app asks for 12 hours while its session lasts only 1 hour
- **The shorter one wins** — 43,200 s asked for, 3,600 s available, so 3,600 s it is
- **The symptom** — the link works in testing, then fails the next morning untouched
- **The fix** — ask for no more than the session has left, or sign a fresh one on demand

*Example (illustrative):* Alice bookmarks a link the app called good for 12 hours; the signing session ends after 1 hour, so the bookmark fails at hour 1.

**Common mistake:** Reading seven days as a promise. The real life of a link is the smaller of the two clocks, and with a short session that is almost always the second one.

### Visualization (canvas `c3`, 720×300)

Three horizontal bars on one shared 12-hour axis: what was asked for, how much session was left, and the effective expiry drawn as the minimum of the two, with the percentage computed at render time.

- **Title (bold 15.5px, `#1a5276`, centered at y=24):** "Effective Expiry Is the Smaller of Two Clocks (illustrative session length)".
- **Axis geometry:** x0=110 at 0 h, x1=680 at 12 h, so `x(h) = 110 + h × 47.5`; baseline 1.5px `#e5e9ef` at y=240; 12px `#6b7280` tick labels centred at y=256 reading "0h", "2h", … "12h".
- **Grid:** 1px `#e5e9ef` vertical lines at every 2-hour tick from y=68 to y=240, drawn before the bars.
- **Bars (h=30, 5px radius, rows at y = 76, 128, 180):** row label 12px `#2c3e50` right-aligned at x=100 on the baseline y+20.
  - "you asked for" — 12 h → 570 px, fill `#2a78d6` at alpha 0.85; bold 12px white "43,200 s asked for" drawn inside the bar at x0+12.
  - "session left" — 1 h → 47.5 px, fill `#c98500` at alpha 0.9; bold 12px `#c98500` "3,600 s of session left" placed at x0+bar+16.
  - "actually valid" — 1 h → 47.5 px, fill `#008300` at alpha 0.9; bold 12px `#008300` "3,600 s = 1 hour" placed at x0+bar+16.
- **Cut line:** 2px `#e74c3c` dashed (5/4) vertical at x = 110 + 47.5 = 157.5 from y=68 to y=240; bold 12px `#e74c3c` "link dies here" left-aligned at (163.5, 62).
- **Annotation (bold 13px `#4a3aa7`, left-aligned at (110, 222)):** "min(43,200 s, 3,600 s) = 3,600 s — 8.3% of what was advertised", where the percentage is computed in JS from the two second counts.
- **Ceiling note (12px `#6b7280`, left-aligned at (110, 278)):** "the number you ask for cannot exceed seven days, or 604,800 s".
- **Caption (12px `#444`, bottom right at y=294):** "session length illustrative; the minimum is computed".

## Footnote

- **Placeholder notice (0.8rem `#6b7280`, top border 1px `#e5e9ef`, below the last section):** every link on this page is drawn schematically as named parts — no element of the page contains a key, secret, signature or account identifier.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%). A `.footnote` paragraph closes the page.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Section three's callout uses the label "Common mistake:".
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms). Helpers `roundRect`, `arrowHead` and `rgba` exactly as in the sibling pages, plus a small `commas(n)` helper so thousands separators do not depend on the browser locale (`toLocaleString` would print `43.200` in some locales).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine risk: the blocked byte path in `c1`, the missing size limit in `c2`, the expiry cut line in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, so each holds one line at the 50/50 split while still carrying a full clause. An earlier pass clipped them to ~55–70 characters and was rejected for compromising quality; equally, do **not** pad them back up to the folder default of ~90–100. Eight bullets per section.
- **Register and vocabulary — this is a tutorial, not a reference course.** Plain technical English for a first-time reader. No analogies, no invented scenes, no story framing. Say "the signed link" and "the signature", not query-parameter names; say "the action, the file and the expiry are hashed", not a signature-algorithm name; say "a form upload whose policy can cap the size", not the POST-policy field name; say "the credentials that signed the link", not a role-session API. The exact parameter and API names belong on the reference pages, not here.
- **No realistic credential material anywhere.** Never write a plausible signed URL, access key, token, signature value, or `key=value` credential syntax — not in prose, not in a chart label. `c2` names the link's parts in English ("the file's address", "the signature") rather than showing a URL. The footnote states this explicitly.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No leak-channel section.** A fourth section listed every channel a signed link escapes through (screenshots, referrer headers, proxy logs, browser history, CDN caches) with a five-way fan-out chart `c4`; it was cut for making the page longer than the concept needs. That material belongs on a security-focused page, not on the tutorial that introduces the concept.
  - **The bearer-token idea itself stays**, but only as the one bullet "No login at S3" plus the subtitle — do not grow it back into bullets about revocation, source-IP conditions, or never logging the link.
  - **No URL-anatomy listing.** The earlier `c2` drew six named query parameters with placeholder values; the parameter names are reference material and were deliberately replaced with four plain-English parts.
  - **Three sections, eight short bullets each, one canvas per section.**
- **Data:** all values are hardcoded literals, no randomness anywhere.
  - **Documented behaviour the page stands on:** the signed request is self-describing and checked without any stored state; the signature covers the method, the one key and the expiry, so the link cannot be altered or repointed; a signed upload of the plain kind carries no size condition while the form-upload policy supports a size range (and content-type / encryption conditions); one upload request accepts up to 5 GiB; the requested expiry maximum is seven days (604,800 s); the effective life of a link is bounded by the lifetime of the signing credentials; permissions are evaluated when the request arrives, not when the link is signed.
  - **Illustrative and labelled as such:** the reporting app and Alice, the 40 MB report, the 5 MiB receipt, the 12-hour request, and the 1-hour session length.
  - **Computed:** 12 h = 43,200 s; 1 h = 3,600 s; min(43,200, 3,600) = 3,600 s; 3,600 / 43,200 = 8.33% ≈ 8.3% (computed in JS at render time, not hardcoded); 5 GiB / 5 MiB = 1,024; 5 MiB = 5,242,880 bytes; 7 days = 604,800 s.
  - **Computed geometry:** `c1` derives every arrow endpoint from the three box origins (30, 275, 520) and the shared box width 170, so the centres 115, 360 and 605 are not hardcoded twice. `c2` lays the four part boxes on a 40 px pitch from y=60 and derives the bracket span from the first and third box. `c3` maps hours to pixels with `x(h) = x0 + h × (x1 − x0) / 12`, so the 1-hour bars and the cut line all land on the same computed 157.5.
