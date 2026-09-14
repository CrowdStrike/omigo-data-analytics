# Kerberos

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Kerberos

**Subtitle:** MIT's 1980s ticket-based authentication protocol (published 1988) — log in once, get a wristband, and never hand your password to another service again

## One Login for the Whole Multiplex

**Tags:** `core idea` (blue), `ticket-based auth` (green), `defense` (orange)

- **The multiplex** — a corporate network is a movie theater: mail, file server, wiki are the screens
- **The old way** — every screen asks for your password, so every screen can lose or leak it
- **The box office** — you prove who you are ONCE to the Key Distribution Center (KDC)
- **The wristband** — the KDC hands back a Ticket-Granting Ticket (TGT), your proof for ~10 hours
- **The screen ticket** — to enter a service, you trade the TGT for a service ticket only that service can open
- **Both ways** — the service proves itself back to you too (mutual authentication)

*Example (italic):* Priya types her password once at 9:00am; for the rest of the day her laptop shows tickets, and no service ever sees the password.

**Key point:** Kerberos moved authentication from "every service checks your password" to "one trusted box office issues cryptographically sealed tickets that services verify".

### Visualization (canvas `c1`, 720×300)

Flow diagram of the three Kerberos exchanges, drawn as a box-office trip: laptop → KDC → TGT → service ticket → file server, with the password never leaving the first box.

- **Title (bold 15px, `#1a5276`, top center):** "The Box-Office Flow: Password Stays Home, Tickets Travel".
- **Row 1 (boxes at y=70):** blue `#2a78d6` rounded box at x=30 labeled "Your laptop / password never sent" (12px, two lines); 3px arrow labeled "1. prove identity once" (12px `#444`) to a violet `#4a3aa7` box at x=290 labeled "KDC (box office)"; return arrow beneath it labeled "2. TGT — wristband, valid 10 h" in bold 12px green `#008300`.
- **Row 2 (boxes at y=185):** the laptop box repeated in outline only at x=30 labeled "show TGT, ask for file server" (12px); 3px arrow labeled "3. service ticket (sealed for that screen)" to a green `#008300` box at x=290 labeled "service ticket / from the KDC"; 3px arrow to an aqua `#199e70` box at x=520 labeled "file server / never sees password"; small return arrow labeled "4. proves itself back" (12px `#6b7280`).
- **Box style:** 170–190px wide, 46px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(0,131,0,0.12)` / `rgba(25,158,112,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "services trust the KDC's seal — not your password".

## Priya's Workday: 23 Service Requests, Zero Passwords Sent

**Tags:** `worked example` (blue), `count it by hand` (green)

- **The login** — 9:00am, Priya types her password once; her TGT is valid 10 hours, until 7:00pm
- **The day** — she opens mail 14 times, the file server 6 times, the wiki 3 times: 23 requests
- **Old world** — password-per-service would send her password over the network 23 times
- **Kerberos world** — 3 service tickets (one per service), reused all day; password sent 0 times
- **Hand-check** — 14 + 6 + 3 = 23 requests, yet only 1 password entry and it never crosses the wire

*Example (italic):* At 4:30pm the wiki asks who Priya is; her laptop replays the sealed wiki ticket from 9:05am — no password, no new login.

**Key point:** One password entry buys a 10-hour TGT; the TGT buys per-service tickets; the tickets carry the whole day — 23 requests, zero password transmissions.

### Visualization (canvas `c2`, 720×300)

Grouped horizontal bar chart per service: requests made during the day vs password transmissions under the old way vs under Kerberos.

- **Title (bold 15px, `#1a5276`, top center):** "One Day, 23 Requests: Passwords Sent = 23 (old way) vs 0 (Kerberos)".
- **Axis:** vertical 2px `#999` baseline at x=180, bars extend right, max width 460 for value 14; x scaled linearly 0–14; left-aligned 12px `#444` row labels at x=20.
- **Rows (three service groups, group tops at y = 60, 130, 200; three 14px-tall bars per group, 4px apart):**
  - "mail — 14 requests": blue `rgba(42,120,214,0.30)` bar width 460 (requests 14), red `#e74c3c` bar width 460 (old-way sends 14), green `#008300` bar width 0 drawn as a bold 12px green "0" at the baseline (Kerberos sends 0)
  - "file server — 6 requests": blue bar width 197 (6), red bar width 197 (6), green "0"
  - "wiki — 3 requests": blue bar width 99 (3), red bar width 99 (3), green "0"
- **Bar labels:** 11px `#444` value at each bar end; legend row (12px) at y=35: blue "requests", red "password sent — old way", green "password sent — Kerberos".
- **Annotation (bold 13px green `#008300`, right side near y=250):** "1 password entry at 9:00am covers all 23 requests".
- **Caption (12px `#444`, bottom right):** "request counts illustrative; the zeros are the protocol".

## The 1988 Ideas Inside Every Modern SSO

**Tags:** `where it's used` (blue), `SSO` (green)

- **Still running** — invented at MIT in the 1980s, Kerberos still authenticates most enterprise networks today
- **Idea one** — authenticate once, centrally, instead of once per service
- **Idea two** — carry short-lived cryptographic proof instead of re-sending a long-lived secret
- **Idea three** — services validate the proof; they never store or check passwords at all
- **Modern echo** — swap "KDC" for "identity provider" and "TGT" for "session token" and you have today's SSO

*Example (italic):* When a web app redirects you to a company login page and back, it is replaying the 1988 pattern: one central login, a sealed token, services that only check tokens.

**Key point:** Every single-sign-on system since — from web login cookies to cloud identity tokens — reuses the Kerberos triad: authenticate once, carry short-lived proof, validate proof not passwords.

### Visualization (canvas `c3`, 720×300)

Three-row mapping diagram: each 1988 Kerberos concept on the left flows by arrow to its modern SSO descendant on the right.

- **Title (bold 15px, `#1a5276`, top center):** "1988 → Today: the Same Three Ideas, Renamed".
- **Rows (y = 80, 155, 230), each: left box at x=40, 3px `#6b7280` arrow, right box at x=420:**
  - blue `#2a78d6` box "log in once to the KDC" → aqua `#199e70` box "log in once to the identity provider"
  - blue box "carry a 10-hour TGT" → aqua box "carry a short-lived session token"
  - blue box "services check the ticket's seal" → aqua box "apps validate tokens, never passwords"
- **Box style:** 250px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` (left) / `rgba(25,158,112,0.12)` (right), 12px `#2c3e50` text; small 11px `#6b7280` era labels "1988" and "today" above the two columns at y=50.
- **Annotation (bold 13px magenta `#d55181`, centered near y=275):** "the vocabulary changed; the protocol shape did not".

## When the Clocks Drift: the Five-Minute Rule

**Tags:** `common mistake` (red), `replay defense` (orange)

- **Why tickets expire** — a stolen ticket replayed later must fail, so every ticket carries timestamps
- **The rule** — Kerberos rejects any request whose timestamp is more than 5 minutes from its own clock
- **The famous break** — a machine whose clock drifts past 5 minutes cannot authenticate to anything
- **The mistake** — debugging "wrong password" errors for hours when the real fault is a drifted clock
- **The fix** — enterprise networks run time sync (NTP) precisely because Kerberos demands it

*Example (italic):* A laptop that sat in a drawer for a month boots with its clock 7.5 minutes slow; every login fails until the clock resyncs — the password was never the problem.

**Common mistake:** Blaming credentials when Kerberos rejects a login. Time-limited tickets are the replay defense, so clock skew beyond 5 minutes is an authentication failure by design — check the clock first.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of clock drift for five machines against the 5-minute skew limit; the one over the line fails.

- **Title (bold 15px, `#1a5276`, top center):** "Clock Drift vs the 5-Minute Skew Limit".
- **Axis:** vertical 2px `#999` baseline at x=200, bars extend right; x = drift in minutes 0 to 9, scale 52 px/min, 12px `#444` tick labels at 0/3/6/9 along y=250; left-aligned 12px `#444` machine labels at x=20.
- **Threshold:** vertical dashed `#e74c3c` (dash 5/4) line at drift 5 min (x=460), bold 12px red label "5-min limit" at its top.
- **Rows (y = 70, 105, 140, 175, 210), 16px-tall bars, drifts `[0.4, 1.8, 3.2, 4.9, 7.5]` minutes:**
  - "desktop-01 — 0.4 min": green `#008300` bar width 21
  - "laptop-07 — 1.8 min": green bar width 94
  - "server-03 — 3.2 min": green bar width 166
  - "kiosk-02 — 4.9 min": orange `#d95926` bar width 255 (close to the line)
  - "drawer-laptop — 7.5 min": red `#e74c3c` bar width 390, bold 12px red label "✗ all logins fail" at the bar end
- **Bar style:** 11px `#444` drift value at each passing bar's end; passing bars fill solid.
- **Annotation (bold 13px orange `#d95926`, right side near y=270):** "not a password problem — a clock problem".
- **Caption (12px `#444`, bottom right):** "drift values illustrative; the 5-minute default is real".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); Priya's request counts (14/6/3 = 23) and the drift minutes `[0.4, 1.8, 3.2, 4.9, 7.5]` are invented and labeled illustrative; the ~10-hour TGT lifetime and 5-minute clock-skew tolerance are real Kerberos defaults; MIT in the 1980s (published 1988) is the real origin.
- **Framing:** defensive/educational only — the page explains how the protocol protects passwords and bounds replay; no attack tooling or exploitation steps.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
