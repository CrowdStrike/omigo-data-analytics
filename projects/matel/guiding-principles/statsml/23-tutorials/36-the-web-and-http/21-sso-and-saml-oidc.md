# SSO & SAML/OIDC

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SSO & SAML/OIDC

**Subtitle:** Single sign-on means you prove who you are once — to one identity provider — and every company app accepts its signed note instead of asking for a password; SAML and OIDC are the two dialects that note is written in

## Maya's First Morning: One Login Opens Every App

**Tags:** `core idea` (blue), `identity provider` (green), `log in once` (orange)

- **The new hire** — Maya joins Acme and needs three apps on day one: email, the wiki, the dashboards
- **The old way** — three separate accounts, three passwords, three "forgot password?" links waiting to happen
- **The one door** — instead she signs in once at `login.acme.example`, the company's identity provider (IdP)
- **The pass** — each app redirects her to the IdP, gets back a signed note saying "this is Maya", and lets her in
- **The feel** — after the 9:00am login, email, wiki, and dashboards all open with zero extra prompts

*Example (italic):* Maya types her password exactly once at 9:00am; by 9:02 all three apps are open, and none of them ever saw that password.

**Key point:** SSO moves the login out of every app and into one identity provider — apps stop checking passwords and start trusting the IdP's signed assertion instead.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram comparing Maya's morning without SSO (three password prompts) vs with SSO (one IdP login, three green check marks).

- **Title (bold 15px, `#1a5276`, top center):** "Three Apps, One Morning: 3 Password Prompts vs 1 Login".
- **Row 1 (y=100), label 12px `#444` at x=20:** "without SSO"; three orange `#d95926` rounded boxes at x=170, 350, 530 labeled "email — password", "wiki — password", "dash — password" (12px `#2c3e50`), joined by 2px `#6b7280` arrows; bold 12px orange "3 prompts, 3 passwords" beneath the row at y=140.
- **Row 2 (y=210), label:** "with SSO"; one blue `#2a78d6` rounded box at x=170 labeled "login.acme.example — 1 login", then three small green `#008300` boxes at x=390, 490, 590 labeled "email ✓", "wiki ✓", "dash ✓", joined by 2px `#6b7280` arrows.
- **Box style:** 130–170px wide (small green boxes 80px), 40px tall, 8px radius, fills `rgba(217,89,38,0.12)` / `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, near x=430, y=262):** "one password typed, zero passwords shared with the apps".
- **Caption (12px `#444`, bottom right):** "acme.example is a made-up company; app count illustrative".

## The Redirect Dance: How the Signed Assertion Travels

**Tags:** `worked example` (blue), `SAML vs OIDC` (green), `redirects` (orange)

- **Step 1** — Maya's browser asks `dash.acme.example` (the service provider, SP) for the dashboards
- **Step 2** — the SP has no session, so it answers 302 and redirects her browser to the IdP
- **Step 3** — the IdP checks her password + MFA code, then redirects back carrying a signed assertion
- **Step 4** — the SP verifies the IdP's signature on the assertion, creates a session, answers 200
- **SAML dialect** — the assertion is a signed XML document posted back in a form (enterprise classic)
- **OIDC dialect** — the assertion is a signed JWT called an ID token, fetched over OAuth-style redirects

*Example (italic):* The whole dance is the first request plus four redirect hops — SP, IdP, back to SP, then the page — and Maya only sees the IdP's login form once.

**Key point:** SAML and OIDC do the same dance with different props: browser redirects carry the user to the IdP, and a signature the SP can verify carries the identity back — XML for SAML, a JWT for OIDC.

### Visualization (canvas `c2`, 720×300)

Sequence diagram with three lifelines — browser, SP (dash.acme.example), IdP (login.acme.example) — showing the four-hop redirect dance.

- **Title (bold 15px, `#1a5276`, top center):** "The SSO Dance: SP Redirects to IdP, Signed Assertion Comes Back".
- **Lanes:** bold 13px ink `#1a5276` labels "browser" at x=100, "SP (dash)" at x=360, "IdP (login)" at x=620, y=55; vertical dashed `#e5e9ef` (dash 4/3) lifelines from y=65 to y=250 at x=100, 360, 620.
- **Arrow 1 (y=95):** blue `#2a78d6` 3px arrow x=100→360, 12px `#2c3e50` label above: "1. GET /dashboards".
- **Arrow 2 (y=130):** orange `#d95926` 3px arrow x=360→100, 12px label above: "2. 302 → go log in at the IdP".
- **Arrow 3 (y=165):** blue `#2a78d6` 3px arrow x=100→620, 12px label above: "3. password + MFA at the IdP".
- **Arrow 4 (y=200):** green `#008300` 3px arrow x=620→100, 12px label above: "4. signed assertion (SAML XML / OIDC JWT)".
- **Arrow 5 (y=235):** green `#008300` 3px arrow x=100→360, 12px label above: "5. assertion posted to SP → 200, session set".
- **Annotation (bold 13px violet `#4a3aa7`, near x=480, y=268):** "the SP checks a signature, never a password".
- **Caption (12px `#444`, bottom right):** "hop layout schematic; both dialects follow this shape".

## One Switch for Onboarding, Offboarding, and MFA

**Tags:** `where it's used` (blue), `offboarding` (green), `MFA` (orange)

- **Onboarding** — create Maya once at the IdP and all 14 company apps know her the same day
- **Offboarding** — when someone leaves, disabling the one IdP account locks all 14 apps at once
- **The old cleanup** — without SSO, IT closes 14 accounts one by one; 3 are still open after day 5
- **The straggler** — in the app-by-app world, 1 forgotten account still works at day 10
- **MFA in one place** — enforce a second factor at the IdP and every app behind it inherits it
- **Audit trail** — one login log at the IdP shows who reached what, instead of 14 scattered logs

*Example (italic):* On a departure day, the SSO company flips one IdP switch and all 14 accounts die at day 0; the no-SSO company still has 1 live account 10 days later.

**Key point:** The IdP is the single control point — one account to create, one switch to cut access, one place to require MFA — which is exactly why enterprises buy SSO.

### Visualization (canvas `c3`, 720×300)

Line chart of the 10 days after an employee leaves: accounts still active, app-by-app cleanup vs one IdP switch.

- **Title (bold 15px, `#1a5276`, top center):** "After the Goodbye Party: Accounts Still Active, Day 0 to Day 10".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days after departure 0 to 10, 12px `#444` tick labels at 0/2/4/6/8/10; y = active accounts 0 to 14, gridlines `#e5e9ef` at 4/8/12.
- **App-by-app line:** orange `#d95926` 3px line through days `[0, 1, 2, 3, 5, 7, 10]`, accounts `[14, 10, 7, 5, 3, 2, 1]` — a slow manual decay.
- **SSO line:** green `#008300` 3px line through days `[0, 1, 2, 3, 5, 7, 10]`, accounts `[0, 0, 0, 0, 0, 0, 0]` — flat at zero after the day-0 IdP disable, with a green dot at (0, 0) labeled 12px green "IdP switch flipped".
- **Straggler marker:** red `#e74c3c` filled dot at (10, 1) with bold 12px red label "1 forgotten account still live".
- **Legend (12px, top right of plot):** orange swatch "app-by-app cleanup", green swatch "one IdP disable".
- **Annotation (bold 13px green `#008300`, near x=5 days, y=100):** "one switch closes all 14 on day 0".
- **Caption (12px `#444`, bottom right):** "account counts and cleanup pace illustrative".

## SSO Doesn't Hand Your Password to the Apps

**Tags:** `common mistake` (red), `tokens not passwords` (orange)

- **The confusion** — people picture SSO as the IdP copying their password into every app
- **What really moves** — a short-lived signed assertion; Maya's password never leaves the IdP
- **The lifetime** — the SP's session token expires (say, after 8 hours), so a stolen one goes stale
- **Password-sync trap** — reusing one password across apps by hand is the risk SSO exists to remove
- **The cousin mix-up** — OAuth grants an app permission to act for you; OIDC on top of it proves who you are
- **The real risk** — the IdP becomes the one door, so a weak IdP login is a master-key problem: hence MFA there

*Example (italic):* If the wiki is breached, attackers find 8-hour session tokens, not Maya's password — and disabling her IdP account stops new sessions, while existing ones expire within 8 hours.

**Common mistake:** Thinking SSO shares your password around. It does the opposite — only the IdP ever sees it; apps get expiring signed tokens, which is why one compromised app no longer burns every account.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the wrong mental model (password copied to each app, red) vs what actually happens (password stays at the IdP, apps get expiring signed tokens, green).

- **Title (bold 15px, `#1a5276`, top center):** "What the Apps Actually Receive: a Token, Never the Password".
- **Row 1 (y=95), label 12px `#444` at x=20:** "imagined"; blue `#2a78d6` rounded box at x=170 labeled "password", 3px red `#e74c3c` arrows fanning to three red boxes at x=400, 510, 620 labeled "email", "wiki", "dash", with bold 12px red "✗ one breach leaks it everywhere" beneath at y=140.
- **Row 2 (y=210), label:** "actual"; blue box at x=150 labeled "password stays at IdP", 3px green `#008300` arrow to a green box at x=370 labeled "signed token, 8h expiry", then three thin green arrows to small green boxes at x=540, 600, 660 labeled "email ✓", "wiki ✓", "dash ✓".
- **Box style:** 120–170px wide (small boxes 55px), 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "guard the IdP with MFA — it is the one door everything trusts".
- **Caption (12px `#444`, bottom right):** "8-hour token lifetime illustrative; policies vary".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 3 apps, 14 accounts, offboarding decay `[14, 10, 7, 5, 3, 2, 1]` over days `[0, 1, 2, 3, 5, 7, 10]`, and the 8-hour token lifetime are invented and labeled illustrative; the five-hop redirect dance and the SAML-XML / OIDC-JWT assertion formats are the true protocol shapes.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
