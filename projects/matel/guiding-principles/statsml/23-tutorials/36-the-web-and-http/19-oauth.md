# OAuth

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** OAuth

**Subtitle:** OAuth lets an app act on your account with a limited, revocable key — like a valet key that starts the car but never opens the trunk

## The Valet Key for Your Calendar

**Tags:** `core idea` (blue), `delegated access` (green), `no password sharing` (orange)

- **The app** — a scheduling app wants to read your calendar so it can find free 30-minute slots
- **The old way** — you hand the app your calendar password; now it can read, edit, delete, even lock you out
- **The valet key** — OAuth gives the app a token scoped to `calendar.read`; that one door and nothing else
- **No password** — the app never sees your password; you type it only on the calendar provider's own page
- **Revocable** — you can cancel that one token any time without changing your password

*Example (italic):* The scheduling app reads next week's busy slots with its token, but its test delete request comes back "403 — scope is calendar.read".

**Key point:** OAuth is delegated access: instead of your password (a master key to every door), a third-party app gets a token that opens only the doors you approved.

### Visualization (canvas `c1`, 720×300)

Two-row "doors" comparison: what a shared password unlocks vs what a `calendar.read` token unlocks, drawn as four permission boxes per row with allow/deny marks.

- **Title (bold 15px, `#1a5276`, top center):** "Password = Every Door. Token = One Door.".
- **Rows:** row labels 13px `#444` at x=20 — "share password" at y=110, "token: calendar.read" at y=220.
- **Doors (both rows):** four rounded boxes per row, 120px wide, 52px tall, 8px radius, at x = `[190, 325, 460, 595]` (centers), labeled 12px `#2c3e50`: `["read events", "add events", "delete events", "change password"]`.
- **Password row fills:** all four boxes green `rgba(0,131,0,0.12)` with 2px `#008300` border and a bold 13px `#008300` "✓" — but the fourth box ("change password") gets an extra bold 12px orange `#d95926` sublabel "full takeover".
- **Token row fills:** first box green `rgba(0,131,0,0.12)` border `#008300` with "✓"; boxes 2–4 red `rgba(231,76,60,0.12)` with 2px `#e74c3c` border and bold 13px `#e74c3c` "✗ 403".
- **Annotation (bold 13px green `#008300`, right side near y=260):** "one scope, one door — revoke it anytime".
- **Caption (12px `#444`, bottom right):** "permission set illustrative".

## Five Steps from "Connect Calendar" to First API Call

**Tags:** `worked example` (blue), `authorization code` (green)

- **Step 1: redirect** — you click "Connect calendar"; the app sends your browser to the provider's consent page
- **Step 2: consent** — you sign in at the provider and approve exactly one scope, `calendar.read`
- **Step 3: code** — the provider redirects your browser back carrying a one-time code valid for 60 seconds
- **Step 4: exchange** — the app's server trades the code plus its client secret for a 60-minute access token
- **Step 5: API call** — the app calls `GET /events` with the token and receives your busy slots

*Example (italic):* The whole flow is about 5 seconds of clicking; the code lives 60 seconds, the access token 60 minutes, and your password never leaves the provider's page.

**Key point:** The short-lived code travels through the browser, but the real token exchange happens server to server — so the access token never rides in a URL.

### Visualization (canvas `c2`, 720×300)

Swimlane sequence diagram: three vertical lanes (your browser, scheduling app server, calendar provider) with five numbered arrows tracing the authorization-code flow top to bottom.

- **Title (bold 15px, `#1a5276`, top center):** "Authorization-Code Flow: Code in the Browser, Token Server-to-Server".
- **Lanes:** vertical 2px `#e5e9ef` lifelines at x = `[130, 360, 590]`, from y=75 to y=265; lane headers bold 13px `#1a5276` at y=60: `["your browser", "scheduling app", "calendar provider"]`.
- **Arrows (3px, arrowheads, one per step at y = `[100, 133, 166, 199, 232]`), each with a 14px filled circle number badge at its start and a 12px `#444` label above the line:**
  - 1 (blue `#2a78d6`): browser → provider, "redirect to consent page"
  - 2 (blue `#2a78d6`): browser → provider, "sign in + approve calendar.read"
  - 3 (yellow `#c98500`): provider → browser, "one-time code (60 s)"
  - 4 (green `#008300`): app → provider, "code + client secret → access token (60 min)"
  - 5 (aqua `#199e70`): app → provider, "GET /events with token"
- **Badge style:** circle fill matches its arrow color, bold 11px white digit.
- **Annotation (bold 12px violet `#4a3aa7`, centered under step 4 near y=255):** "the token never passes through the browser".
- **Caption (12px `#444`, bottom right):** "timings illustrative; 60 s / 60 min are this example's lifetimes".

## Why Every "Sign in with…" Button Runs on It

**Tags:** `where it's used` (blue), `scopes` (green), `integrations` (orange)

- **One account, many apps** — a single provider account issues separate tokens to many apps, each with its own scopes
- **Scopes as a menu** — the consent screen lists exactly what the app asked for; you approve or walk away
- **API ecosystems** — payment, storage, and social APIs all hand out scoped tokens instead of passwords
- **Least privilege** — a photo-print app that asked only for `photos.read` never gains the power to delete
- **Kill switch** — the provider's "connected apps" page revokes one app's token without touching the others

*Example (italic):* One account feeds three apps at once: the scheduler holds `calendar.read`, a mail-merge tool holds `mail.send`, and a photo printer holds `photos.read`.

**Key point:** Scopes turn one account into many narrow keys — every third-party integration gets its own key, and losing one never means changing the lock.

### Visualization (canvas `c3`, 720×300)

Hub diagram: one provider-account box on the left issuing three scoped tokens, one arrow per connected app, each arrow labeled with its scope.

- **Title (bold 15px, `#1a5276`, top center):** "One Account, Three Apps, Three Different Keys".
- **Hub box:** rounded box centered at (150, 165), 170px wide, 64px tall, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border, bold 13px `#1a5276` label "your provider account".
- **App boxes (right column, x=560 center, 190px wide, 46px tall, 8px radius, 12px `#2c3e50` text) at y = `[85, 165, 245]`:** "scheduling app" (fill `rgba(42,120,214,0.15)`, border `#2a78d6`), "mail-merge tool" (fill `rgba(25,158,112,0.15)`, border `#199e70`), "photo printer" (fill `rgba(74,58,167,0.15)`, border `#4a3aa7`).
- **Arrows:** 3px lines from the hub's right edge to each app box, colored to match each box's border; each carries a 12px bold same-color scope label above its midpoint: `calendar.read`, `mail.send`, `photos.read`.
- **Revoke mark:** small 12px `#6b7280` scissors-style dashed tick (dash 4/3) crossing the photo-printer arrow near x=430, labeled 11px `#6b7280` "revocable per app".
- **Annotation (bold 13px green `#008300`, bottom center near y=285):** "each app holds only the key it asked for".
- **Caption (12px `#444`, bottom right):** "apps and scopes illustrative".

## A Token Proves Permission, Not Identity

**Tags:** `common mistake` (red), `authn vs authz` (orange), `token storage` (red)

- **The mix-up** — OAuth answers "what may this app do?", not "who is this user?"
- **The hole** — accepting any valid token as a login lets a token issued to app A impersonate its user inside app B
- **The fix** — login needs an identity layer; OpenID Connect adds a signed ID token on top of OAuth
- **Storage** — an access token kept in browser localStorage can be read by any injected script
- **Safer homes** — keep tokens server-side or in httpOnly cookies, and let the 60-minute expiry cap the damage

*Example (italic):* A leaked `calendar.read` token can list events for its remaining minutes, but a token wrongly accepted as "proof of login" hands over a whole account.

**Common mistake:** Treating an OAuth access token as a login credential. It proves an app was granted a scope, nothing about who is holding it — identity needs a signed ID token, and stolen bearer tokens work for whoever holds them.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same access token used as a login (impersonation) vs a proper OpenID Connect login (verified identity), shown as boxes flowing left to right.

- **Title (bold 15px, `#1a5276`, top center):** "Same Token, Two Endings: Authorization Is Not Authentication".
- **Row 1 (y=100), label 12px `#444` at x=20:** "token as login"; blue `#2a78d6` rounded box at x=170 labeled "access token (calendar.read)" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "app trusts the holder as the user" with bold 12px red "✗ anyone with the token is 'you'".
- **Row 2 (y=215), label:** "OpenID Connect"; blue box "access token + signed ID token" at x=170, 3px arrow to a green `#008300` box at x=400 labeled "signature verified", then arrow to a green box at x=590 labeled "user identified" with bold 12px green "✓".
- **Box style:** 150–190px wide, 42px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=278):** "a bearer token is a key, not a face — verify identity separately".
- **Caption (12px `#444`, bottom right):** "flow schematic, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all geometry and labels are the hardcoded literals above (no randomness); the code lifetime (60 seconds), token lifetime (60 minutes), scope names (`calendar.read`, `mail.send`, `photos.read`), permission-door sets, and app names are invented and labeled illustrative; the five flow steps match the standard authorization-code grant.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
