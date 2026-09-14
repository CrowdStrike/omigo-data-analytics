# Password Spraying

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Password Spraying

**Subtitle:** Flip the loop — one very common password against every account, so no account ever hits its lockout

## Flipping the Loop: One Password, Ten Thousand Doors

**Tags:** `core idea` (blue), `one password many accounts` (orange), `defensive` (green)

- **The portal** — a company's login page serves 10,000 employee accounts and enforces a lockout rule
- **The rule** — five failed attempts on one account within 30 minutes freezes that account
- **The flip** — instead of many passwords at Alice's account, one password is tried at all 10,000 accounts
- **One password per round** — a single seasonal-style common guess, one attempt per account, then move on
- **Under the threshold** — each account's failure counter reaches 1, never the 5 that trips a lockout
- **The name** — trying one likely password broadly across a whole population is called password spraying
- **Slow and wide** — the round is stretched across hours and resumed the next day with the next guess

*Example (italic):* Brute force asks "which of a million passwords is Alice's?"; spraying asks "which of 10,000 employees used this one very common password?" — same login page, opposite loop.

**Key point:** Password spraying inverts brute force: the outer loop runs over accounts instead of passwords, so the attempt count per account stays at 1 while the attempt count per password reaches thousands.

### Visualization (canvas `c1`, 720×300)

Two-panel dot schematic: brute force stacks attempts on one account and crosses the lockout line; spraying lays one attempt across many accounts and never leaves level 1.

- **Title (bold 15px, `#1a5276`, top center):** "Same Attempt Budget, Two Shapes".
- **Annotation (bold 13px violet `#4a3aa7`, centered at x=360, y=46):** "the spray never leaves level 1".
- **Shared vertical scale:** attempts on one account, baseline y=250, 15px per attempt; y for attempt *n* = 250 − 15n. Tick labels 12px `#444` right-aligned at x=52 for 1 (y=235), 5 (y=175), 10 (y=100).
- **Lockout line:** dashed red `#e74c3c` (dash 5/4, 1.5px) from x=58 to x=690 at y=175; 12px red label "lockout: 5 failures / 30 min" at (62, 167).
- **Left panel (x 58–300), header bold 12px `#2c3e50` at (62, 68):** "brute force"; one column of 12 dots (6px radius) at x=180, attempt *n* at y = 250 − 15n (attempt 1 at y=235 up to attempt 12 at y=70); attempts 1–4 blue `#2a78d6`, attempts 5–12 red `#e74c3c`; 12px `#444` centered label "Alice's account" at (180, 272).
- **Right panel (x 340–700), header bold 12px `#2c3e50` at (344, 68):** "password spraying"; one row of 24 dots (5px radius) all at y=235, x = 350 + 14·i for i = 0…23 (last at x=672), all blue `#2a78d6`; 12px `#444` centered label "10,000 accounts, 1 attempt each (24 shown)" at (511, 272).
- **Panel divider:** 1px `#e5e9ef` vertical line at x=320 from y=56 to y=258.
- **Caption (12px `#444`, bottom right):** "attempt pattern schematic".

## Two Rounds on a 10,000-Account Portal

**Tags:** `worked example` (blue), `arithmetic` (orange)

- **The population** — 10,000 accounts, round one, exactly one attempt each: 10,000 attempts on day one
- **Round-one yield** — 0.4% of staff happen to use that one password: 10,000 × 0.004 = 40 accounts open
- **The failures** — the other 10,000 − 40 = 9,960 attempts fail, one apiece, scattered across the workforce
- **Round two** — day two sprays a second common password; a 0.2% rate adds 10,000 × 0.002 = 20 accounts
- **The total** — 20,000 attempts, 2 per account across two days, 40 + 20 = 60 accounts opened
- **Still legal traffic** — the busiest 30-minute window shows 1 failure per account against a cap of 5
- **The mirror image** — those same 20,000 attempts aimed at Alice alone would lock her out at attempt 5

*Example (italic):* Throttled to 5 tries per 30 minutes, 20,000 attempts on Alice's single account need 4,000 windows — 2,000 hours, about 83 days — and fire 4,000 lockout alerts; spread over 10,000 accounts the same 20,000 attempts finish in two days and fire none (rates illustrative, counts exact from them).

**Key point:** The yield is per-password, not per-account — a 0.4% match rate is worthless against one account and worth 40 accounts against ten thousand, at two attempts each.

### Visualization (canvas `c2`, 720×300)

Bar chart: accounts opened in round one, round two, and both, from the hardcoded values `[40, 20, 60]`.

- **Title (bold 15px, `#1a5276`, top center):** "Two Rounds, 20,000 Attempts, 60 Accounts Opened".
- **Axes:** origin x=70, baseline y=240, plot width 590, plot height 180; y = accounts opened 0 to 60 (pixel height = value / 60 × 180), gridlines `#e5e9ef` at 20/40/60 with 12px `#444` right-aligned tick labels at x=64.
- **X-axis:** 2px `#999` line from (70, 240) to (660, 240).
- **Bars (80px wide, centered at x = 180, 340, 520), heights from `[40, 20, 60]` → `[120, 60, 180]` px:** round one fill `rgba(42,120,214,0.35)` / 2px `#2a78d6`; round two fill `rgba(25,158,112,0.35)` / 2px `#199e70`; both-rounds fill `rgba(217,89,38,0.30)` / 2px `#d95926`.
- **Value labels:** bold 13px in each bar's border color, centered 8px above the bar top: "40", "20", "60".
- **X labels (12px `#444`, centered, two lines at y = 258 and y = 274):** "round 1 (day 1)" / "10,000 attempts, 0.4%"; "round 2 (day 2)" / "10,000 attempts, 0.2%"; "both rounds" / "20,000 attempts, 2 per account".
- **Annotation (bold 13px orange `#d95926`, left-aligned at x=90, y=60):** "2 attempts per account — lockout needs 5".
- **Caption (12px `#444`, bottom right):** "match rates illustrative; account counts exact from them".

## Why Per-Account Monitoring Is Structurally Blind

**Tags:** `where it's used` (blue), `lockout blindspot` (orange), `detection` (green)

- **Keyed to the account** — the lockout counter is grouped by account, and every group's count here is 1
- **The aggregate signal** — the tell is one password failing against thousands of distinct accounts
- **Structural blindness** — a per-account monitor cannot compute a cross-account count; the grouping key is wrong
- **What to group by** — group failed logins by attempted-password hash and time window, not by account
- **A workable rule** — 9,960 failures sharing one password in a day sits far above any normal-day baseline
- **Distinct-account count** — alert on how many distinct accounts a single failing password touches
- **Why analysts miss it** — dashboards default to per-user rows, and every single user's row looks fine

*Example (italic):* Every one of the 10,000 per-account counters reads 1 against a cap of 5, while the single per-password counter reads 9,960 against no cap at all — the same day's traffic, two grouping keys, one alarm.

**Key point:** Detection has to change its grouping key, not its threshold — no per-account counter, at any threshold, can see a pattern that is defined across accounts.

### Visualization (canvas `c3`, 720×300)

Two panels of the same day's failed logins, counted two ways; the vertical scales differ deliberately.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Day's Failures, Two Grouping Keys".
- **Panel divider:** 1px `#e5e9ef` vertical line at x=375 from y=46 to y=258.
- **Left panel (x 40–370), header bold 12px `#2c3e50` at (48, 62):** "grouped by account".
  - Baseline 2px `#999` from (70, 240) to (340, 240); scale 20px per failure.
  - Bar 60px wide at x=140 (left edge), value 1 → height 20, top y=220, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6`.
  - Bold 13px `#2a78d6` value label "1" centered at (170, 212).
  - Threshold: dashed red `#e74c3c` (dash 5/4) from x=80 to x=340 at y=140 (5 failures), 12px red label "alert at 5" at (84, 132).
  - Bold 13px `#6b7280` centered "no alert" at (170, 118).
  - 12px `#444` centered label "Alice's account, 30 min" at (190, 262).
- **Right panel (x 380–710), header bold 12px `#2c3e50` at (388, 62):** "grouped by password".
  - Baseline 2px `#999` from (410, 240) to (690, 240).
  - Bar 60px wide at x=520 (left edge), value 9,960 drawn at hardcoded height 170 (top y=70, log-feel), fill `rgba(231,76,60,0.35)`, 2px `#e74c3c`.
  - Bold 13px `#e74c3c` value label "9,960" centered at (550, 62).
  - Threshold: dashed red `#e74c3c` from x=420 to x=690 at y=225 (50 failures, log-feel), 12px red label "alert at 50" at (424, 217).
  - Bold 13px `#e74c3c` left-aligned "alert fires" at (596, 130).
  - 12px `#444` centered label "one password, all accounts, 1 day" at (550, 262).
- **Caption (12px `#444`, bottom right):** "panels use different vertical scales (log-feel on the right)".

## Lockout Is a Per-Account Cap, Not an Anti-Guessing Control

**Tags:** `common mistake` (red), `slow and wide` (orange), `MFA` (green)

- **The confusion** — reading account lockout as a general anti-guessing control rather than a per-account cap
- **What it bounds** — guesses per account per window; it says nothing at all about guesses per password
- **Per-IP limits too** — with 200 source addresses sharing the round, each IP sends 10,000 ÷ 200 = 50 attempts
- **Spread thin** — 50 attempts per IP over a 10-hour round is 5 per hour, against a cap of 60 per hour
- **Under every cap** — 1 failure per account (cap 5) and 5 attempts per IP per hour (cap 60): nothing fires
- **Slow is the point** — stretching a fixed volume over time and sources defeats any rate-based threshold
- **MFA blunts it** — a correctly sprayed password still fails without the second factor, closing the 60 openings
- **Ban the common ones** — rejecting known-common passwords at set time removes the attack's raw material

*Example (italic):* Bob's security review shows both rate limits comfortably green all week, because both are per-something and the campaign is deliberately shaped to sit under whatever the "something" is.

**Common mistake:** Reporting "no lockouts and no rate-limit trips" as evidence that no guessing occurred. Both controls are per-account or per-IP caps; the only counters that would have moved were per-password and per-campaign ones nobody was keeping.

### Visualization (canvas `c4`, 720×300)

Horizontal bars: observed value against its configured cap for three grouping keys; the third has no cap.

- **Title (bold 15px, `#1a5276`, top center):** "Every Threshold Is Per-Something".
- **Rows at y = 80, 140, 200 (bar height 20), row labels 12px `#444` right-aligned ending at x=250:** "failures per account / 30 min", "attempts per IP / hour", "failures per password / day".
- **Bars start at x=260; a full cap is 300px wide, so a bar's width is observed ÷ cap × 300:**
  - Row 1 observed 1 of cap 5 → width 60, fill `rgba(42,120,214,0.30)`, 2px `#2a78d6`.
  - Row 2 observed 5 of cap 60 → width 25, fill `rgba(42,120,214,0.30)`, 2px `#2a78d6`.
  - Row 3 observed 9,960 with no cap → width 340 (uncapped, drawn to the plot edge), fill `rgba(231,76,60,0.45)`, 2px `#e74c3c`.
- **Cap markers (rows 1 and 2 only):** dashed 2px `#6b7280` (dash 4/3) vertical segment at x=560 spanning each row's height ±6px; 12px `#6b7280` label "cap" at (566, 74) above the first marker.
- **Value labels (bold 12px, at bar end + 8, vertically centered):** "1 of 5" and "5 of 60" in `#2a78d6`; "9,960 — no cap" in `#e74c3c`.
- **Annotation (bold 13px green `#008300`, left-aligned at (40, 262)):** "MFA: the 60 correct passwords still can't sign in".
- **Caption (12px `#444`, bottom right):** "bar widths are fraction-of-cap; row 3 has no cap".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red is used only for genuine alarm states (lockout-tripping attempts, the uncapped aggregate counter).
- **Data:** all values are the hardcoded numbers above — no randomness anywhere. The portal size (10,000 accounts), the lockout policy (5 failures / 30 min), the per-IP cap (60/hour), the source count (200 IPs) and round length (10 hours) are the stated setup; the match rates (0.4%, 0.2%) are invented and labeled illustrative. Everything else follows exactly: 10,000 × 0.004 = 40; 10,000 × 0.002 = 20; 40 + 20 = 60; 10,000 − 40 = 9,960 failures; 2 rounds × 10,000 = 20,000 attempts = 2 per account; 20,000 ÷ 5 = 4,000 lockout windows × 0.5 h = 2,000 h ≈ 83 days; 10,000 ÷ 200 = 50 attempts per IP; 50 ÷ 10 h = 5 per hour. Text numbers must match chart numbers exactly.
- **Credential hygiene:** never print an actual password string — always "one very common password" or "a seasonal-style guess". People are Alice and Bob; no real company names.
- **Framing:** defensive/educational — the page explains the attack's shape so defenders re-key their monitoring to the password/campaign level and deploy MFA plus common-password bans; no operational attack guidance beyond the arithmetic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
