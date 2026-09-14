# Everything the Browser Already Knows

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Everything the Browser Already Knows

**Subtitle:** Open the network tab and every call your page makes is readable — a key shipped to the client is a published key, whatever the transport

## Alice Opens the Network Tab

**Tags:** `core idea` (blue), `devtools network tab` (orange), `no client-side secrets` (red)

- **The dashboard** — an internal page fetches rows from a JSON web service and renders them as charts
- **The key** — to authenticate, the page attaches the service key to an authorization header on every call
- **The reasoning** — the developer figured the page is internal and the connection is HTTPS, so the key is safe
- **The network tab** — Alice opens developer tools, clicks Network, and reads the whole request, headers and all
- **Nothing was broken** — she did not defeat encryption; the browser decrypted the traffic for her, as designed
- **The response too** — the same panel shows all 50,000 records the service returned, field by field
- **The replay** — she can re-send that exact request from any HTTP client, with the page's own logic out of the picture

*Example (italic):* Alice wanted to know why a chart was empty; two clicks later she was looking at the dashboard's service key and a 50,000-record response (counts illustrative).

**Key point:** Everything a page needs in order to make a request is, by necessity, present in the browser — so the network tab is not a leak, it is an accurate view of what the page was given.

### Visualization (canvas `c1`, 720×300)

Stylized developer-tools network panel: one request, with every line marked readable.

- **Title (bold 15px, `#1a5276`, top center):** "What the Network Tab Shows Alice — Nothing Is Hidden".
- **Panel:** rounded rect x=40, y=42, 640×205, 8px radius, fill `#f8f9fa`, 2px `#6b7280` border.
- **Header strip:** filled rounded top band x=40, y=42, 640×28, fill `rgba(42,120,214,0.12)`; bold 12px `#1a5276` left-aligned at x=56, y=61: "Network — request from the internal dashboard".
- **Lines (left-aligned at x=58, 13px unless noted):** y=96 blue `#2a78d6` "GET /api/records → 200 OK"; y=122 bold 12px `#6b7280` "request headers"; y=146 magenta `#d55181` "authorization: <service-key>"; y=170 yellow `#c98500` "x-dashboard-role: viewer"; y=196 bold 12px `#6b7280` "response body"; y=220 violet `#4a3aa7` "50,000 records, 12 fields each".
- **Readable markers:** right-aligned 12px `#008300` "readable" at x=662 on the y=96, y=146, y=170 and y=220 lines.
- **Annotation (bold 13px orange `#d95926`, left at x=40, y=268):** "every line above is plaintext inside Alice's browser".
- **Caption (12px `#444`, bottom right):** "placeholder values; counts illustrative".

## HTTPS Protects the Wire, Not the Endpoint

**Tags:** `core idea` (blue), `threat model` (orange), `encoding is not secrecy` (red)

- **Two ends** — HTTPS encrypts data between two endpoints, and Alice's browser is one of those two endpoints
- **Plaintext by construction** — the browser must decrypt to render, so whoever operates it holds the plaintext
- **The general rule** — anything the client must know in order to call the service cannot be a secret from its user
- **Encoding is not secrecy** — minifying, obfuscating, base64-encoding, or splitting a key across files is reversible
- **UI is not authorization** — a hidden field, a disabled button, or a client-side role check is convenience only
- **The wire is genuinely safe** — encryption does stop an eavesdropper in the middle; that is the threat it was built for
- **Shipped means published** — a key delivered to a browser is public from the moment it ships, not when someone looks

*Example (italic):* The tunnel between browser and service is unreadable to anyone in between, and completely readable to Alice, who sits at one end of it.

**Key point:** Encryption in transit says nothing about secrecy from an endpoint — expecting HTTPS to hide a key from the user of the page asks it to solve a problem it was never designed for.

### Visualization (canvas `c2`, 720×300)

Two-endpoint diagram: an encrypted tunnel between browser and service, with the browser drawn inside a dashed trust boundary that Alice controls.

- **Title (bold 15px, `#1a5276`, top center):** "HTTPS Protects the Wire — the Browser Is One of the Two Ends".
- **Trust boundary:** dashed violet `#4a3aa7` (dash 6/4) rounded rect x=26, y=96, 208×92, 10px radius, no fill; 12px `#4a3aa7` label left-aligned at (28, 205): "Alice controls everything in here".
- **Browser box:** rounded rect x=40, y=110, 180×64, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; centered 12px `#2c3e50` two lines at y=136 "Alice's browser" and y=156 "holds the plaintext".
- **Service box:** rounded rect x=500, y=110, 180×64, 8px radius, fill `rgba(25,158,112,0.15)`, 2px `#199e70` border; centered 12px `#2c3e50` two lines at y=136 "JSON web service" and y=156 "the other endpoint".
- **Tunnel:** rounded rect x=228, y=118, 264×48, 8px radius, fill `rgba(0,131,0,0.10)`, 2px `#008300` border; centered 12px `#008300` "encrypted in transit" at y=147.
- **Eavesdropper note (12px `#6b7280`, centered at x=360, y=188):** "someone in the middle here sees nothing — that part works".
- **Annotation 1 (bold 13px orange `#d95926`, centered at x=360, y=240):** "encryption hides the message from the middle, not from the ends".
- **Annotation 2 (bold 12px magenta `#d55181`, centered at x=360, y=266):** "minified, base64-encoded, split across files — all reversible with the file".

## One Shared Key vs 200 Scoped Tokens

**Tags:** `worked example` (blue), `token scope` (orange), `blast radius` (green)

- **Setup** — the dashboard has 200 users, and each user's own scope averages 250 records (illustrative figures)
- **The whole set** — 200 × 250 = 50,000 records, and the single shared service key reaches every one of them
- **Blast radius** — 50,000 ÷ 250 = 200, so a leaked shared key exposes 200× what one scoped token exposes
- **Revoking a token** — killing one leaked per-user token disrupts 1 of 200 users: 1 ÷ 200 = 0.5%
- **Rotating the shared key** — the replacement must ship to everyone, so 200 of 200 users are disrupted: 100%
- **Short life helps twice** — a token expiring in minutes shrinks both the exposure window and the cleanup
- **The inversion** — you cannot stop the client from reading its own credential, so make that credential worth little

*Example (italic):* Same leak, two designs: the shared key hands over 50,000 records and forces a redeploy for all 200 users; the scoped token hands over 250 and inconveniences one person.

**Key point:** Assume the credential is visible and minimize what it grants — narrow scope, short life, and server-side enforcement are the controls that survive the client being fully readable.

### Visualization (canvas `c3`, 720×300)

Grouped horizontal bars: reach of one leaked credential, then how many users a revocation disrupts, shared key vs scoped token.

- **Title (bold 15px, `#1a5276`, top center):** "Shared Key vs Per-User Token: Reach and Cleanup Cost".
- **Group header 1 (bold 12px `#2c3e50`, left at x=40, y=62):** "records one leaked credential reaches".
- **Group header 2 (bold 12px `#2c3e50`, left at x=40, y=170):** "users disrupted when it must be revoked".
- **Bars:** start at x=260, 20px tall, right-aligned 12px `#444` row labels ending at x=250; hardcoded pixel widths `[420, 4, 420, 4]`:
  - y=76 "shared service key": fill `rgba(231,76,60,0.45)`, 2px `#e74c3c`, bold 12px `#e74c3c` end label "50,000 records"
  - y=118 "per-user scoped token": fill `rgba(0,131,0,0.55)`, 2px `#008300`, bold 12px `#008300` end label "250 records"
  - y=184 "rotate the shared key": fill `rgba(217,89,38,0.45)`, 2px `#d95926`, bold 12px `#d95926` end label "200 of 200 = 100%"
  - y=226 "revoke one token": fill `rgba(0,131,0,0.55)`, 2px `#008300`, bold 12px `#008300` end label "1 of 200 = 0.5%"
- **Annotation (bold 13px violet `#4a3aa7`, centered at x=430, y=150):** "50,000 ÷ 250 = 200× blast radius".
- **Caption (12px `#444`, bottom right):** "illustrative: 200 users × 250 records = 50,000; short bars widened to 4px (true width 2.1px)".

## Where the Check Belongs — and Why "It's Internal" Isn't One

**Tags:** `common mistake` (red), `server-side enforcement` (green), `where it's used` (blue)

- **Re-check every call** — the server must authorize each request; the client's claim about its role is input, not proof
- **Filter server-side** — returning 50,000 records and hiding 49,750 in JavaScript still ships all 50,000 to the browser
- **Over-returning** — responses often carry whole user objects when the page shows only a name, and the tab reveals it
- **Short-lived tokens** — issue a per-user token after login, scoped to that user's records, expiring in minutes
- **Backend for frontend** — a thin server of your own calls the third-party service so its key never reaches the page
- **CORS is a courtesy** — it is a rule browsers choose to follow, and a non-browser HTTP client ignores it entirely
- **"It's internal"** — a network assumption that ends the moment any employee's laptop browser loads the page

*Example (italic):* The fix was not a better hiding place for the key: the service began issuing per-user tokens at login and returning only that user's 250 records.

**Common mistake:** "It's HTTPS" and "it's minified" answer questions nobody asked. The threat is neither an eavesdropper on the wire nor a casual reader of source — it is the legitimate user of the page, who is an endpoint and already holds the plaintext.

### Visualization (canvas `c4`, 720×300)

Two-row flow: filtering in the browser ships every record, filtering on the server ships only the user's own.

- **Title (bold 15px, `#1a5276`, top center):** "Hiding Rows in JavaScript Still Ships All 50,000".
- **Row labels (bold 12px `#2c3e50`, left at x=30):** y=90 "client-side filter"; y=205 "server-side filter".
- **Row 1 (boxes 200×46, 8px radius, top edge y=68):** orange box at x=180, fill `rgba(217,89,38,0.12)`, 2px `#d95926`, 12px `#2c3e50` two lines "service sends" / "50,000 records"; 3px `#d95926` arrow from x=380 to x=452; red box at x=458, 210×46, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c`, two lines "browser hides 49,750," / "displays 250"; bold 12px `#e74c3c` at (458, 132): "✗ all 50,000 were delivered".
- **Row 2 (boxes 200×46, top edge y=183):** green box at x=180, fill `rgba(0,131,0,0.12)`, 2px `#008300`, two lines "service authorizes," / "sends 250 records"; 3px `#008300` arrow from x=380 to x=452; aqua box at x=458, 210×46, fill `rgba(25,158,112,0.15)`, 2px `#199e70`, two lines "browser displays 250" / "nothing to hide"; bold 12px `#008300` at (458, 247): "✓ 49,750 never left the server".
- **Annotation (bold 13px orange `#d95926`, centered at x=360, y=280):** "\"internal only\" ends at the first browser that loads the page".
- **Caption:** none (annotation occupies the bottom strip); record counts are labeled illustrative in the text.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** every value is a hardcoded literal (no randomness anywhere). The scale figures — 200 users, 250 records per user, 50,000 total, 49,750 hidden, the 200× ratio, 0.5% and 100% — are invented but internally exact: 200 × 250 = 50,000, 50,000 ÷ 250 = 200, 50,000 − 250 = 49,750, 1 ÷ 200 = 0.5%, 200 ÷ 200 = 100%. Text and chart labels must match to the digit.
- **Scope boundary:** this page owns the failure where a credential was *never hidden at all* because it shipped to the browser. Secrets committed to git history, vaults, and rotation discipline belong to the secrets-management page and are not re-taught here.
- **Framing:** defensive/educational. HTTPS is described as working exactly as designed — the error is expecting transport encryption to hide data from an endpoint. No claim that TLS is broken, no operational attack guidance.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: request header values on this page are obvious placeholders such as &lt;service-key&gt; — for illustration only, and to avoid false positives from secret scanners."
