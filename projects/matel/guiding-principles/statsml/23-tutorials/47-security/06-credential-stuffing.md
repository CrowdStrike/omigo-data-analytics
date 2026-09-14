# Credential Stuffing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Credential Stuffing

**Subtitle:** Yesterday's breach is today's attack — leaked email:password pairs from one site get replayed against every other site, one likely guess per account

## Yesterday's Breach Is Today's Attack

**Tags:** `core idea` (blue), `password reuse` (orange), `defensive` (green)

- **The breach** — a hobby forum is breached and 1,000,000 email:password pairs leak onto a resale market
- **The reuse** — many people typed the very same password at the forum and at their shopping account
- **The replay** — a botnet tries each leaked pair exactly once against a shopping site's login page
- **One guess each** — unlike brute force, no single account sees repeated tries, so lockout rules barely fire
- **The yield** — even a 0.8% hit rate on 1,000,000 pairs hands the attacker 8,000 working accounts

*Example (italic):* The forum is breached on Monday; by Friday the list has been replayed against a shopping site and 8,000 reused passwords open 8,000 accounts (counts illustrative).

**Key point:** Credential stuffing replays known-real passwords from one site's breach against other sites — each account gets one highly likely guess, and password reuse does the rest.

### Visualization (canvas `c1`, 720×300)

Flow diagram: a breach at site A produces a combo list that fans out as single-try login replays against sites B, C, and D.

- **Title (bold 15px, `#1a5276`, top center):** "One Breach at Site A Becomes Login Attempts at Sites B, C, D".
- **Site A box:** red-tinted rounded box at x=30, y=115, 165×55, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, 12px `#2c3e50` two-line text "Site A breached" / "passwords leak".
- **Combo-list box:** grey rounded box at x=260, y=115, 165×55, fill `rgba(107,114,128,0.12)`, 2px `#6b7280` border, two-line text "combo list" / "1,000,000 email:password pairs"; 3px `#6b7280` arrow from Site A box into it.
- **Target boxes:** three blue rounded boxes at x=505, each 175×44, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, at y=55 "Site B login", y=125 "Site C login", y=195 "Site D login"; 3px `#2a78d6` arrows fan out from the combo-list box to each, with an 11px `#6b7280` label "1 try per account" along the middle arrow.
- **Box style:** 8px corner radius, centered 12px text, two lines where noted.
- **Annotation (bold 13px red `#e74c3c`, x≈70, y=235):** "same password, new door".
- **Caption (12px `#444`, bottom right):** "pair count illustrative".

## The Math of a Million Pairs

**Tags:** `worked example` (blue), `hit rate` (orange)

- **The list** — 1,000,000 leaked email:password pairs are aimed at one shopping site's login
- **One try each** — every matching account receives exactly one attempt using its leaked password
- **Hand-check** — at a 0.1% reuse-and-match rate, 1,000,000 × 0.001 = 1,000 accounts fall
- **Scaling up** — 0.3% yields 3,000 takeovers, 0.5% yields 5,000, and 1% yields 10,000
- **Below the radar** — one failed try per account never approaches a five-attempt lockout rule

*Example (italic):* A 0.5% hit rate sounds negligible, yet it means 5,000 shoppers find their stored payment methods spent by someone else.

**Key point:** Stuffing succeeds per list, not per account — a hit rate that rounds to zero still yields thousands of takeovers when the list has a million rows.

### Visualization (canvas `c2`, 720×300)

Bar chart: account takeovers from a 1,000,000-pair list at four small hit rates.

- **Title (bold 15px, `#1a5276`, top center):** "One Million Leaked Pairs: Takeovers at Tiny Hit Rates".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = takeovers 0 to 10,000, gridlines `#e5e9ef` at 2,500/5,000/7,500 with 12px `#444` tick labels; x-axis 2px `#999`.
- **Bars (70px wide, centered at x = 150, 300, 450, 600), heights from hardcoded values `[1000, 3000, 5000, 10000]` mapped to `[18, 54, 90, 180]` px:** hit-rate labels "0.1%", "0.3%", "0.5%", "1%" in 12px `#444` below the baseline; first three bars fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border, last bar fill `rgba(231,76,60,0.30)` with 2px `#e74c3c` border.
- **Value labels:** bold 12px `#1a5276` ("1,000", "3,000", "5,000") and bold 12px `#e74c3c` ("10,000") centered above each bar top.
- **Annotation (bold 13px red `#e74c3c`, near x=330, y=55):** "a 1% hit rate = 10,000 stolen accounts".
- **Caption (12px `#444`, bottom right):** "hit rates illustrative of reported ranges".

## Layered Defenses: MFA Comes First

**Tags:** `where it's used` (blue), `MFA` (green), `defense in depth` (orange)

- **MFA** — a second factor stops a correct stolen password cold; it is the single biggest mitigation
- **Breach checks** — screen passwords at signup and login against breach corpora and reject known-compromised ones
- **Bot detection** — rate limiting by IP and device fingerprint throttles the distributed replay traffic
- **Password managers** — one unique password per site ends reuse, removing the attack's raw material
- **Why it dominates** — replaying valid stolen credentials is cheap and quiet, making stuffing the leading account-takeover vector

*Example (italic):* Of 8,000 would-be takeovers, bot detection cuts the figure to 3,200, breached-password screening to 1,300, and MFA leaves roughly 10 (layer effects illustrative).

**Key point:** No single control is enough — MFA, breached-password screening, and bot detection each remove a different slice, and together they collapse the attack's economics.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: successful takeovers remaining as each defense layer is added, shrinking from 8,000 to about 10.

- **Title (bold 15px, `#1a5276`, top center):** "Each Layer Removes a Slice: 8,000 Takeovers Down to ~10".
- **Rows (bars start at x=280, top edges at y = 65, 115, 165, 215), each with a right-aligned 12px `#444` label ending at x=270:** "no defenses", "+ bot detection & rate limits", "+ breached-password checks", "+ MFA".
- **Bars (18px tall) from hardcoded values `[8000, 3200, 1300, 10]` mapped to pixel widths `[420, 168, 68, 5]`:** fills red `rgba(231,76,60,0.55)`, orange `rgba(217,89,38,0.55)`, yellow `rgba(201,133,0,0.55)`, green `rgba(0,131,0,0.65)` with matching 2px solid borders `#e74c3c` / `#d95926` / `#c98500` / `#008300`.
- **Value labels:** bold 12px in the bar's border color at each bar's right end + 8px: "8,000", "3,200", "1,300", "~10".
- **Annotation (bold 13px green `#008300`, near x=340, y=260):** "MFA is the single biggest mitigation".
- **Caption (12px `#444`, bottom right):** "layer effects illustrative".

## It Isn't Brute Force: Reading the Signals

**Tags:** `common mistake` (red), `detection` (orange)

- **The confusion** — account lockout rules were built for brute force: many guesses aimed at one account
- **Inverted shape** — stuffing is one guess at many accounts, so per-account failure counters never trip
- **The tell** — validity rate: stuffing attempts succeed far more often than random password guessing
- **Distributed source** — attempts spread across thousands of proxy IPs each look like a normal user
- **Watch the ratio** — a surge of login attempts with an unusually high success ratio is the alarm bell

*Example (italic):* Unknown-device logins normally succeed near 0% when the password is wrong at random; during a stuffing wave, 0.8% of them suddenly succeed (rates illustrative).

**Common mistake:** Relying on account lockout alone — it defends against the brute-force shape, not the stuffing shape. Monitor fleet-wide login validity rates and device reputation instead.

### Visualization (canvas `c4`, 720×300)

Two-panel dot schematic: brute force stacks attempts on one account and hits the lockout line; stuffing spreads one attempt per account and never touches it.

- **Title (bold 15px, `#1a5276`, top center):** "Many Guesses at One Account vs One Guess at Many".
- **Left panel (x = 40 to 330), header bold 12px `#2c3e50` at (60, 60):** "brute force"; a single column of 12 dots (6px radius, 13px vertical spacing) at x=185, attempt 1 at y=238 up to attempt 12 at y=95; the 5 dots at or below the lockout line are blue `#2a78d6`, the 7 above it are red `#e74c3c`; 12px `#444` label "account #1" under the column at y=262.
- **Lockout line:** dashed red `#e74c3c` (dash 5/4) horizontal line across both panels at y=180, 12px red label "lockout after 5 tries" at (48, 172).
- **Right panel (x = 390 to 690), header bold 12px `#2c3e50` at (410, 60):** "credential stuffing"; a single row of 20 dots (5px radius) at y=238, x from 395 to 680 in steps of 15, all blue `#2a78d6`; 12px `#444` label "20 accounts, 1 attempt each" centered under the row at y=262; 12px `#6b7280` note "never reached" just above the lockout line at (560, 172).
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=360, y=40):** "per-account counters see nothing unusual".
- **Caption (12px `#444`, bottom right):** "attempt patterns schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 1,000,000-pair list, the hit rates (0.1/0.3/0.5/1%), the resulting takeovers (1,000/3,000/5,000/10,000), and the defense-layer funnel (8,000 → 3,200 → 1,300 → ~10) are invented and labeled illustrative; text numbers must match chart numbers exactly.
- **Framing:** defensive/educational throughout — the page explains the attack's mechanism so defenders can recognize the signals and layer mitigations; no operational attack guidance.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
