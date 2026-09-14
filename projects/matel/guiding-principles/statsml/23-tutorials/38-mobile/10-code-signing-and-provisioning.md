# Code Signing & Provisioning

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Code Signing & Provisioning

**Subtitle:** A signed app carries cryptographic proof of who built it — the phone checks that proof before it will install anything, so signing is how you prove the app is yours

## The First Build That Refuses to Run

**Tags:** `core idea` (blue), `identity` (green), `first build` (orange)

- **The app** — a developer finishes a loyalty app for a coffee shop and plugs in the owner's phone
- **The error** — the build tool stops the very first device build: "No signing certificate found"
- **The question** — the code compiles fine, so why does the phone refuse to even try it?
- **The answer** — phones only install apps that carry proof of who built them; a bare binary has none
- **The pieces** — the proof is built from three things: a private key, a certificate, and a profile

*Example (italic):* The same app that runs perfectly in the simulator is refused by the real phone, because the simulator never asks "who built this?" and the phone always does.

**Key point:** Code signing is a cryptographic stamp of identity on an app, and provisioning is the permission slip saying where that identity may run — the error means the stamp is missing, not that the code is wrong.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the same build heading to the same phone, once without a signing identity (refused) and once with key + certificate + profile (installs).

- **Title (bold 15px, `#1a5276`, top center):** "Same App, Same Phone: Only the Signed Build Installs".
- **Row 1 (y=95), label 12px `#6b7280` at x=20:** "no identity"; blue `#2a78d6` rounded box at x=150 labeled "loyalty app build" (12px), 3px arrow to a magenta `#d55181` box at x=420 (width 200) labeled "phone: no proof of who built it" with bold 12px magenta "✗ install refused" beneath it.
- **Row 2 (y=205), label:** "key + certificate + profile"; blue box at x=150 "loyalty app build", 3px arrow to a small green `#008300` box at x=360 labeled "signed", then arrow to a green box at x=530 labeled "installs on the shop's phone" with bold 12px green "✓".
- **Box style:** 130–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(213,81,129,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the error is not a bug — the phone is asking 'who built this?'".
- **Caption (12px `#444`, bottom right):** "flow schematic, illustrative".

## From Private Key to Installed App, Step by Step

**Tags:** `worked example` (blue), `key → cert → profile` (green)

- **Step 1: key pair** — the laptop generates a private key (kept secret) and a matching public key
- **Step 2: request** — the public key goes into a certificate signing request sent to the platform
- **Step 3: certificate** — the platform signs it: "this public key belongs to developer account #841"
- **Step 4: profile** — the provisioning profile bundles the certificate, app ID, and 3 test devices
- **Step 5: sign** — the build tool hashes the app, signs the hash with the private key, embeds both
- **Install check** — the phone verifies the signature, the certificate chain, the profile, and expiry

*Example (italic):* The profile for app ID com.shop.loyalty lists 3 registered phones — the shop owner's phone is one of them, so after the four checks pass the install goes through. (The profile and device-list steps are the iOS model; Android apps are also signed, but with self-signed certificates and no profiles.)

**Key point:** The chain is one-directional — private key proves the signature, certificate ties the key to account #841, profile ties the certificate to this app on these devices; break any link and the install fails.

### Visualization (canvas `c2`, 720×300)

Five-box pipeline across the top showing the signing chain, with the phone's four install-time checks listed as a checklist below.

- **Title (bold 15px, `#1a5276`, top center):** "The Chain: Private Key → Certificate → Profile → Signed App".
- **Pipeline (boxes at y=75, height 46, width 128, x = 16, 158, 300, 442, 584), 3px `#6b7280` arrows between:** blue `#2a78d6` box "1. key pair (laptop)", blue box "2. request (public key)", aqua `#199e70` box "3. certificate (account #841)", aqua box "4. profile (app ID + 3 devices)", green `#008300` box "5. signed binary"; fills `rgba(42,120,214,0.15)` / `rgba(25,158,112,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` two-line labels.
- **Checklist (12px `#2c3e50` lines at x=90, y = 175, 197, 219, 241), each prefixed by a bold 12px green `#008300` "✓":** "signature verifies with the certificate's public key"; "certificate chains up to the platform's root"; "profile lists this phone and app ID com.shop.loyalty"; "nothing in the chain has expired".
- **Annotation (bold 13px green `#008300`, right side near y=208):** "four checks pass → the app installs".
- **Caption (12px `#444`, bottom right):** "account number and device count illustrative".

## Why the Phone Refuses Unsigned Apps

**Tags:** `where it's used` (blue), `supply chain` (green), `CI` (orange)

- **Tamper-proofing** — the signature covers a hash of every byte; flip one byte and the check fails
- **Supply chain** — the user gets exactly the binary the account holder signed, or nothing at all
- **No impostors** — nobody can ship an update to your app without your private key
- **Unsigned = unknown** — with no signature there is no identity to check, so the OS refuses outright
- **CI signing** — build servers sign releases too, holding the private key in a protected secret store

*Example (italic):* A build server signs the nightly release at 2am with a key it reads from its secret store — no person handles the key, but every shipped byte is still accounted for.

**Key point:** Signing turns "trust the download" into "verify the download" — the OS checks the signature and the profile at install, and a single failure in either blocks the app.

### Visualization (canvas `c3`, 720×300)

Outcome matrix: four install scenarios as rows, with a signature-check column, a profile-check column, and the resulting install decision.

- **Title (bold 15px, `#1a5276`, top center):** "What the OS Checks at Install: One Failure Blocks Everything".
- **Header (12px `#6b7280` at y=58):** "signature" centered at x=360, "profile" at x=470, "outcome" at x=590.
- **Rows (y = 88, 132, 176, 220), scenario label 12px `#2c3e50` left-aligned at x=20:**
  - "untouched signed build": bold 14px green `#008300` "✓" at x=360, green "✓" at x=470, green rounded pill at x=550 labeled "installs" (fill `rgba(0,131,0,0.12)`)
  - "one byte modified in transit": bold 14px magenta `#d55181` "✗" at x=360, green "✓" at x=470, magenta pill "refused" (fill `rgba(213,81,129,0.12)`)
  - "profile expired": green "✓" at x=360, magenta "✗" at x=470, magenta pill "refused"
  - "no signature at all": magenta "✗" at x=360, magenta "✗" at x=470, magenta pill "refused"
- **Row separators:** 1px `#e5e9ef` horizontal lines between rows, full plot width from x=20 to x=700.
- **Annotation (bold 13px orange `#d95926`, centered near y=262):** "a single flipped byte breaks the signature — tampering can't hide".
- **Caption (12px `#444`, bottom right):** "scenarios illustrative".

## The Day the Build Broke: Expiry and Shared Keys

**Tags:** `common mistake` (red), `expiry` (orange)

- **The clock** — certificates are commonly valid for 365 days; provisioning profiles expire too
- **The symptom** — a build that worked yesterday fails today with a signing error; no code changed
- **The panic** — releases stop until someone renews the certificate and regenerates the profile
- **The leak** — emailing the private key file around means anyone holding it can sign as you
- **The fix** — track expiry dates, keep keys in a secrets manager, revoke and reissue if one leaks

*Example (italic):* Monthly builds 1 through 11 ship cleanly; the 12th fails with "certificate has expired" — the code compiled fine, only the identity lapsed.

**Common mistake:** Treating signing assets as set-and-forget. Keys and profiles are credentials with lifetimes — expiry breaks builds on a calendar schedule, and a carelessly shared key hands your identity to whoever has the file.

### Visualization (canvas `c4`, 720×300)

Gantt-style timeline over 15 months: the certificate and profile validity bars end at month 12, monthly builds succeed until then and fail after.

- **Title (bold 15px, `#1a5276`, top center):** "Day 366: The Certificate Expires and Every Build Fails".
- **Axes:** origin x=90, baseline 2px `#999` at y=245, plot width 600 (40px per month); x = months 0 to 15 with 12px `#444` tick labels every 3 months ("0", "3", "6", "9", "12", "15"); vertical gridlines `#e5e9ef` at the same ticks.
- **Certificate bar (y=85, 16px tall):** fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border from month 0 to month 12; 12px `#2a78d6` label "certificate — valid 365 days" left of/above the bar.
- **Profile bar (y=130, 16px tall):** fill `rgba(25,158,112,0.30)` with 2px `#199e70` border from month 0 to month 12; 12px `#199e70` label "provisioning profile — valid 12 months".
- **Builds row (y=195), 12px `#444` label "monthly release builds" at x=90 above the row:** bold 14px green `#008300` "✓" marks at months `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]`; bold 14px magenta `#d55181` "✗" marks at months `[12, 13, 14]`.
- **Expiry marker:** vertical dashed `#6b7280` (dash 4/3) line at month 12 from y=70 to y=245, 12px `#6b7280` label "day 366" at its top.
- **Annotation (bold 13px magenta `#d55181`, near month 12, y=170):** "nothing changed in the code — only the calendar".
- **Caption (12px `#444`, bottom right):** "monthly build schedule illustrative; 365-day validity typical".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all boxes, check marks, and bars are the hardcoded values above (no randomness); the account number (#841), the 3 registered devices, the four install scenarios, and the monthly build schedule (✓ at months 1–11, ✗ at months 12–14) are invented and labeled illustrative; the 365-day certificate and 12-month profile validity reflect common developer-program terms.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
