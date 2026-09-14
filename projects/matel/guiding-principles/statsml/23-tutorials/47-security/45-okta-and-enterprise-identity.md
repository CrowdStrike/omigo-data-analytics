# Okta & Enterprise Identity

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Okta & Enterprise Identity

**Subtitle:** SSO is the front door to every internal tool — one identity provider holds the login, and every app trusts what it says about you

## Forty Apps, One Password Problem

**Tags:** `core idea` (blue), `SaaS sprawl` (orange), `single sign-on` (green)

- **The sprawl** — a 900-person company runs 40 SaaS apps: mail, wiki, HR, chat, code, expenses
- **The old way** — every app keeps its own accounts: 40 apps × 900 staff = 36,000 password pairs
- **The weak link** — each of those 36,000 is a phishing target, and people reuse them anyway
- **The IdP** — an identity provider (Okta is the household name of the category) holds ONE login
- **The delegation** — every app stops checking passwords and asks the IdP "who is this?" instead

*Example (italic):* Maya opens the wiki, the expense tool, and the code host in one morning — she typed her password and passed MFA exactly once, at the IdP.

**Key point:** Single sign-on moves authentication out of every app and into one identity provider — apps no longer hold credentials at all; they trust the IdP's signed answer about who you are.

### Visualization (canvas `c1`, 720×300)

Two-panel diagram: left, every app holding its own password store; right, all apps delegating login to one IdP box.

- **Title (bold 15px, `#1a5276`, top center):** "36,000 Passwords Scattered vs 900 Held in One Place".
- **Left panel (x 30–330):** bold 13px `#2c3e50` label "before: every app checks passwords" at top (y=55); six rounded app boxes 80×34, 6px radius, fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border, in two columns (x=60, x=200) at y = 80, 140, 200, labeled 11px `#2c3e50` "mail", "wiki", "HR", "chat", "code", "expenses"; each box carries a small 24×14 chip on its right edge, fill `rgba(231,76,60,0.15)`, 11px `#e74c3c` text "pw"; bold 12px red `#e74c3c` caption at (x≈80, y=262): "36,000 password pairs to steal".
- **Right panel (x 390–690):** bold 13px `#2c3e50` label "after: every app asks the IdP" at top (y=55); central rounded IdP box 130×48 at (475, 130), fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 12px `#008300` two-line label "identity provider" / "password + MFA"; the same six app boxes (60×28, fill `rgba(42,120,214,0.10)`) arranged around it at (400,80), (560,80), (395,155), (605,155), (430,225), (575,225); 2px `#6b7280` arrows from each app box to the IdP box.
- **Annotation (bold 13px green `#008300`, near x=470, y=272):** "900 credentials, one strong door".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=45 to y=280.
- **Caption (12px `#444`, bottom right):** "app count and headcount illustrative".

## The Redirect Dance Behind One Click

**Tags:** `worked example` (blue), `SAML / OIDC` (orange)

- **Step 1** — Maya clicks the wiki; the wiki has no password box and redirects her browser to the IdP
- **Step 2** — the IdP checks her session; first time today, so it asks for password + MFA once
- **Step 3** — the IdP signs an assertion: "this is maya@corp, group: engineering, verified 9:02am"
- **Step 4** — her browser carries the signed assertion back to the wiki
- **Step 5** — the wiki checks the signature against the IdP's public key and starts her session
- **Two dialects** — SAML and OIDC are the open standards for this dance; same shape, different format

*Example (italic):* Five hops, two redirects, and one MFA prompt at 9:02am; when Maya opens the expense tool at 9:40am, step 2's login is skipped — her IdP session still stands, so it's zero prompts.

**Key point:** The app never sees Maya's password — it outsources the question to the IdP and accepts a signed, expiring statement of identity in return. Forging entry means forging the IdP's signature.

### Visualization (canvas `c2`, 720×300)

Three-lane sequence diagram: browser, wiki app, and IdP as columns, with five numbered arrows tracing the SSO login flow.

- **Title (bold 15px, `#1a5276`, top center):** "One Login, Five Hops: the SSO Redirect Flow".
- **Lanes:** three column headers as rounded boxes 150×34 at x-centers 140, 360, 580, y=55: "Maya's browser" (fill `rgba(42,120,214,0.15)`, border `#2a78d6`), "wiki app" (fill `rgba(217,89,38,0.12)`, border `#d95926`), "IdP" (fill `rgba(0,131,0,0.12)`, border `#008300`); 12px bold `#2c3e50` labels; dashed 1px `#6b7280` lifelines (dash 4/4) from each box down to y=270.
- **Arrows (2.5px, horizontal, each with a filled triangle head and a white-backed 11px `#2c3e50` label above midpoint; bold 12px `#1a5276` step number in a 16px circle at the tail):**
  - 1: browser → wiki at y=105, `#2a78d6`, label "open wiki"
  - 2: wiki → browser → IdP drawn as one arrow wiki → IdP passing over the browser lane at y=135, `#d95926`, label "redirect: go ask the IdP"
  - 3: IdP ↔ browser at y=170, `#008300`, label "password + MFA (once per day)"
  - 4: IdP → wiki at y=205, `#008300`, label "signed assertion: maya@corp, engineering"
  - 5: short self-arrow at the wiki lifeline, y=240, `#d95926`, label "verify signature, start session"
- **Annotation (bold 12px violet `#4a3aa7`, near x=140, y=262):** "the wiki never sees the password".
- **Caption (12px `#444`, bottom right):** "SAML and OIDC both follow this shape".

## Disable One Account, Close Forty Doors

**Tags:** `why it matters` (blue), `deprovisioning` (green), `audit` (orange)

- **One MFA** — enforce MFA at the IdP and every one of the 40 apps inherits it on the same day
- **The leaver** — a contractor exits Friday 5pm; without SSO, someone must find all 40 accounts
- **The lag** — manual offboarding leaves accounts alive for weeks — each one a live back door
- **The kill switch** — with an IdP, one disable click ends the session everywhere within minutes
- **One ledger** — every app login flows through the IdP, so "who accessed what, when" is one log
- **Per-app policy** — the IdP can demand extra checks for payroll while keeping the wiki easy

*Example (italic):* In the manual world, 15 of the contractor's 40 accounts are still open a week after departure; with the IdP, the count is 0 within the hour.

**Key point:** Centralizing identity turns 40 slow, separate security chores — MFA rollout, offboarding, access review, audit — into one fast, enforceable control point.

### Visualization (canvas `c3`, 720×300)

Line chart: the departed contractor's still-active app accounts over 30 days — manual per-app offboarding vs one IdP disable.

- **Title (bold 15px, `#1a5276`, top center):** "The Leaver Problem: Accounts Still Open After Departure Day".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days since departure 0 to 30, 12px `#444` tick labels at 0/7/14/21/30; y = open accounts 0 to 40, gridlines `#e5e9ef` at 10/20/30, 12px `#444` y-labels.
- **Manual line:** orange `#d95926` 3px line with 4px dots through days `[0, 1, 3, 7, 14, 21, 30]`, open accounts `[40, 34, 26, 15, 7, 4, 2]` — a slow decay as tickets get worked.
- **IdP line:** green `#008300` 3px line through days `[0, 1, 3, 7, 14, 21, 30]`, open accounts `[0, 0, 0, 0, 0, 0, 0]` — flat on the baseline; bold 12px green label "IdP disable: 0 from hour one" at (x≈day 9, y≈228).
- **Callout:** bold 13px orange `#d95926` at (x≈day 8, y=95): "day 7: 15 doors still open"; thin 1px `#d95926` pointer line to the (7, 15) point.
- **Caption (12px `#444`, bottom right):** "account counts illustrative".

## The Front Door Becomes the Biggest Prize

**Tags:** `common mistake` (red), `concentration risk` (orange), `defenses` (green)

- **The trade** — everything that made the IdP powerful makes it the single most valuable target
- **The record** — publicly reported IdP incidents show why: one front door beats picking 40 locks
- **The mistake** — treating "we bought SSO" as finished security instead of concentrated risk
- **Defense 1** — phishing-resistant MFA (hardware keys, passkeys): a phished password alone opens nothing
- **Defense 2** — device trust: assertions only issue to managed, healthy machines, not any browser
- **Defense 3** — session policies: short lifetimes and re-auth for sensitive apps limit a stolen session

*Example (italic):* An attacker phishes Maya's SSO password; with a hardware key required, the login fails and the count of apps unlocked is 0 — with only the password, it would have been 40.

**Common mistake:** Assuming SSO reduced risk by itself. It concentrated it — the IdP layer now deserves the strongest controls in the company: phishing-resistant MFA, device trust, and tight session policies.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: apps unlocked by one successful attack, under three defensive postures at the IdP layer.

- **Title (bold 15px, `#1a5276`, top center):** "What One Compromise Unlocks — the IdP Layer Sets the Blast Radius".
- **Axis:** vertical 2px `#999` baseline at x=280, bars extend right, scale 0–40 apps mapped to 0–400px; 12px `#444` scale labels "0", "20", "40" at x=280/480/680 along y=258, light `#e5e9ef` gridlines at those x positions from y=55 to y=250.
- **Rows (bar height 26px, 12px `#2c3e50` right-aligned two-line labels ending at x=270, bar value in bold 12px at bar end):**
  - y=85: "one app's own password stolen (no SSO)" — blue `#2a78d6` bar width 10 (1 app), label "1"
  - y=145: "SSO password phished, hardware-key MFA on" — green `#008300` bar width 0 drawn as a 3px green tick at the baseline, bold green label "0 — login fails"
  - y=205: "full IdP account/session compromised" — red `#e74c3c` bar width 400 (40 apps), bold red label "40 — every door"
- **Annotation (bold 13px violet `#4a3aa7`, near x=300, y=250):** "defend the layer that opens everything".
- **Caption (12px `#444`, bottom right):** "app counts illustrative; postures schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); headcount (900), app count (40), password-pair total (36,000 = 40 × 900), leaver decay counts, and blast-radius bars are invented and labeled illustrative; 36,000 must remain the product of the stated app and staff counts.
- **Framing:** defensive/educational only — Okta is named solely as the well-known example of the identity-provider category; incidents against identity providers stay generic ("publicly reported"), no undocumented company behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
