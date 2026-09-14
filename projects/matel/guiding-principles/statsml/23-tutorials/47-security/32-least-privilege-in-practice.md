# Least Privilege in Practice

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Least Privilege in Practice

**Subtitle:** Everyone agrees with it and almost nobody reaches it — because granting access pays off visibly today while removing it only pays off invisibly, in an incident that may never come

## The Asymmetry That Ratchets Access Upward

**Tags:** `core idea` (blue), `incentives` (orange), `process` (green)

- **Granting pays now** — Bob is blocked, Alice widens his access, and by lunchtime the work is moving again
- **Removing pays never** — a smaller blast radius in a hypothetical future incident is a benefit nobody ever sees
- **Risk flips sides** — granting carries no visible personal risk; revoking risks breaking production with your name on it
- **The gradient** — so the expected personal payoff of granting is positive and of revoking is negative, every time
- **Urgency lowers scrutiny** — access is requested during an incident, a deadline, or a launch, when review is thinnest
- **Grants outlive reasons** — the project ships, Bob changes teams, the integration is retired, and the access stays
- **Nobody dares revoke** — the information needed to remove a grant safely is exactly the information nobody recorded
- **Role explosion** — teams clone and widen an existing role rather than narrow it, so role count and average breadth both rise

*Example (italic):* Bob needs one table at 2am to end an outage; the fastest safe-looking action is to grant the whole dataset, and two years later that grant is still there because no one can prove nothing depends on it.

**Key point:** Least privilege fails structurally, not from indiscipline — the payoff of granting is immediate and attributable while the payoff of removing is diffuse and invisible, so permissions ratchet in one direction.

### Visualization (canvas `c1`, 720×300)

Two-panel comparison: the payoff profile of granting a permission versus removing one, with a bottom arrow showing the resulting one-way drift.

- **Title (bold 16px, `#1a5276`, top center):** "Why the Gradient Points One Way".
- **Left panel:** rounded box x=40, y=48, 300×170, 10px radius, fill `rgba(0,131,0,0.08)`, 2px `#008300` border; header bold 14px `#008300` centered at (190, 72) "GRANT a permission".
- **Left rows (12px `#2c3e50`, left-aligned at x=58, baselines y=100, 122, 144, 166):** "Benefit: someone is unblocked today", "Visible: yes — a person thanks you", "Attributable: yes — clearly your win", "Risk to you: none that anyone will see".
- **Left verdict (bold 14px `#008300`, centered at (190, 198)):** "personal payoff: +".
- **Right panel:** rounded box x=380, y=48, 300×170, 10px radius, fill `rgba(217,89,38,0.08)`, 2px `#d95926` border; header bold 14px `#d95926` centered at (530, 72) "REMOVE a permission".
- **Right rows (12px `#2c3e50`, left-aligned at x=398, baselines y=100, 122, 144, 166):** "Benefit: smaller blast radius, someday", "Visible: no — nothing observable happens", "Attributable: no — diffuse, shared, later", "Risk to you: you may break production".
- **Right verdict (bold 14px `#d95926`, centered at (530, 198)):** "personal payoff: −".
- **Drift arrow:** 3px `#4a3aa7` horizontal line from (120, 248) to (600, 248) with a filled arrowhead at the right end (size 11); bold 13px `#4a3aa7` centered label at (360, 238) "so access accumulates — nobody behaved badly".
- **Caption (12px `#444`, bottom right):** "illustrative framing".

## Eight Quarters of Plus Six and Minus One

**Tags:** `worked example` (blue), `arithmetic` (orange), `illustrative` (magenta)

- **The setup** — one team starts with 40 standing grants across its data and infrastructure systems
- **The inflow** — 6 new grants land per quarter: onboarding, a launch, an integration, an incident
- **The outflow** — 1 grant is removed per quarter, usually when someone leaves the company outright
- **Net rate** — 6 − 1 = +5 grants per quarter, so the level rises by 5 every three months
- **Two-year total** — 40 + (5 × 8) = 80 grants after eight quarters, an exact doubling of the starting level
- **The series** — 40, 45, 50, 55, 60, 65, 70, 75, 80 at the close of each quarter, all from the same two rates
- **The lesson** — nobody made a single bad decision; the inflow simply exceeded the outflow for eight quarters

*Example (italic):* If the outflow rose from 1 to 6 per quarter the level would stay at 40 forever — the doubling is caused entirely by the 5-grant gap, not by carelessness (figures illustrative).

**Key point:** Access growth is arithmetic, not attitude — with an inflow of 6 and an outflow of 1, a team doubles its standing grants in two years while every individual decision looks reasonable.

### Visualization (canvas `c2`, 720×300)

Staircase line chart: standing grants over eight quarters, each quarter drawn as a +6 rise followed by a −1 fall, ending at 80.

- **Title (bold 16px, `#1a5276`, top center):** "40 Grants Become 80 in Eight Quarters at +6 / −1".
- **Axes:** ox=70, baseline y=250, plot width 600, plot height 190; y maps grants 30 (baseline) to 85 (top) via `y = 250 − (v − 30) × 190 / 55`; x maps quarter index 0–8 via `x = 70 + q × 75`.
- **Gridlines:** `#e5e9ef` 1px horizontal at grant values 40, 50, 60, 70, 80 with 12px `#444` right-aligned tick labels at ox−8; x-axis 2px `#999` along the baseline.
- **X labels (12px `#444`, centered under baseline at y=270):** "Q0", "Q1" … "Q8"; axis caption 12px `#444` centered at (370, 290) "quarters".
- **Staircase path:** starting at (Q0, 40), for each quarter draw a green `#008300` 2.5px vertical segment up 6 grants, then an orange `#d95926` 2.5px vertical segment down 1 grant, then a `#2a78d6` 2.5px horizontal segment to the next quarter's x — quarter-end values are exactly the hardcoded series `[40, 45, 50, 55, 60, 65, 70, 75, 80]`.
- **Quarter-end markers:** 4.5px filled `#2a78d6` circles at each of the 9 quarter-end values; bold 12px `#1a5276` labels "40" above the first (offset −12px) and "80" above the last (offset −12px, right-shifted 8px to stay on canvas).
- **Flow legend (first quarter only):** bold 12px `#008300` "+6 granted" at (86, 196) and bold 12px `#d95926` "−1 removed" at (86, 214).
- **Annotation (bold 13px `#4a3aa7`, at (250, 86)):** "net +5 per quarter → doubling in 2 years".
- **Caption (12px `#444`, bottom right):** "grant counts illustrative".

## Ninety Days of Access Logs

**Tags:** `worked example` (blue), `measurement` (aqua), `caveat` (orange)

- **The question** — of the 80 grants now standing, which ones is anyone actually exercising?
- **The evidence** — access logs over 90 days show 22 grants used at least once and 58 never used at all
- **The share** — 58 / 80 = 72.5% unused; dropping the never-used set leaves 22 / 80 = 27.5% of the original level
- **Burden shifts** — the owner of an unused grant now has to justify keeping it, instead of you justifying removal
- **Not an oracle** — "unused in 90 days" is not "not needed": quarterly closes, disaster-recovery paths, seasonal jobs sit idle
- **Window rule** — the review window must exceed the longest legitimate cycle, so 90 days fits daily work and fails annual work
- **Prefer narrowing** — turning write access into read access is an easier sell than removal and captures most of the risk drop
- **Two metrics** — track grant count and used-share together; a falling used-share reveals drift while the count looks flat

*Example (italic):* A quarterly reconciliation job runs four times a year, so a 90-day window can legitimately show it as unused — its grant needs a 400-day window, not a deletion (counts illustrative).

**Key point:** Usage data turns revocation from a guess into a measurement — 58 of 80 grants unused is evidence, not proof, so pair it with a window longer than the longest legitimate idle cycle.

### Visualization (canvas `c3`, 720×300)

Horizontal stacked bar splitting the 80 standing grants into used and never-used over a 90-day window, with an explicit caveat bracket over part of the unused block.

- **Title (bold 16px, `#1a5276`, top center):** "80 Standing Grants, 90 Days of Access Logs".
- **Bar:** x=60, y=120, total width 600, height 62; used segment first, width 22/80 × 600 = 165px, fill `rgba(25,158,112,0.35)`, 2px `#199e70` border; unused segment next, x=225, width 58/80 × 600 = 435px, fill `rgba(217,89,38,0.30)`, 2px `#d95926` border.
- **In-bar labels (bold 13px, centered vertically at y=157):** `#199e70` "22 used" centered at x=142.5; `#d95926` "58 never used" centered at x=442.5.
- **Percent labels (bold 13px, above the bar at y=110):** `#199e70` "22 / 80 = 27.5%" centered at x=142.5; `#d95926` "58 / 80 = 72.5%" centered at x=442.5.
- **Caveat bracket:** 2px dashed `#4a3aa7` (dash 5/4) bracket under the right portion of the unused block — horizontal line from (430, 200) to (655, 200) with 10px downward end ticks at both x positions; bold 12px `#4a3aa7` centered lines at (542, 222) "some of these are legitimately idle" and (542, 240) "quarterly closes, DR paths, seasonal jobs — share unknown".
- **Window note (12px `#6b7280`, left-aligned at (60, 268)):** "window must exceed the longest legitimate cycle".
- **Annotation (bold 13px `#199e70`, centered at (360, 92)):** "evidence shifts the burden of proof — it is not proof".
- **Caption (12px `#444`, bottom right):** "usage counts illustrative".

## A Rate Problem Dressed Up as a Configuration State

**Tags:** `common mistake` (red), `expiry` (violet), `process` (green)

- **The confusion** — least privilege is treated as a clean state to reach once, when it is a rate of accrual
- **Why it drifts back** — grants arrive continuously, so with no countervailing removal process the level climbs again
- **The bad claim** — "we did an access review last year" resets the level and leaves the rate untouched
- **Numerically** — a Q8 cleanup from 80 down to 22 still reaches 22 + (5 × 8) = 62 grants eight quarters later
- **Make expiry default** — time-bound elevation that lapses on its own removes the need for anyone to volunteer to revoke
- **Break-glass path** — a heavily logged emergency role means the 2am case never has to become a permanent grant
- **Metadata at creation** — attach an owner and an expiry when the grant is made, while the reason still exists
- **Review by role** — review roles rather than people, since individuals silently accumulate roles across job changes

*Example (italic):* Expiry changes the outflow, not the level: if every grant lapses after 90 days unless renewed, the 6 quarterly arrivals are matched by 6 lapses and the line flattens at 22 instead of climbing to 62.

**Common mistake:** Running a one-off cleanup project and declaring least privilege achieved — a project changes the level, a clock changes the rate, and only the rate determines where the system sits next year.

### Visualization (canvas `c4`, 720×300)

Two-line chart over sixteen quarters: a one-off cleanup at Q8 that drifts back, versus a default-expiry policy that holds flat.

- **Title (bold 16px, `#1a5276`, top center):** "A Cleanup Changes the Level; Only a Clock Changes the Rate".
- **Axes:** ox=60, baseline y=250, plot width 600, plot height 190; y maps grants 0 (baseline) to 85 (top) via `y = 250 − v × 190 / 85`; x maps quarter index 0–16 via `x = 60 + q × 37.5`.
- **Gridlines:** `#e5e9ef` 1px horizontal at 20, 40, 60, 80 with 12px `#444` right-aligned tick labels at ox−8; x-axis 2px `#999`; 12px `#444` x labels only at Q0, Q4, Q8, Q12, Q16 (baseline+20) and axis caption "quarters" centered at (360, 290).
- **Ratchet line (2.5px `#2a78d6`):** hardcoded `[40,45,50,55,60,65,70,75,80]` for Q0–Q8, then a vertical drop at Q8 from 80 to 22, then hardcoded `[22,27,32,37,42,47,52,57,62]` for Q8–Q16.
- **Cleanup marker:** 1.5px dashed `#4a3aa7` vertical line at Q8 from baseline to y-of-85; bold 12px `#4a3aa7` label "one-off cleanup" rotated none, centered at (360, 66); 5px filled `#4a3aa7` circles at (Q8, 80) and (Q8, 22).
- **Expiry line (2.5px `#008300`, dash 6/4):** flat at 22 from Q8 to Q16 (nine points, all 22).
- **End labels (bold 13px):** `#2a78d6` "62 grants again" at (Q16 x − 6, y-of-62 − 12), right-aligned; `#008300` "flat at 22" at (Q16 x − 6, y-of-22 + 22), right-aligned.
- **Annotation (bold 13px `#d95926`, left-aligned at (86, 108)):** "same +5 rate resumes the next day".
- **Caption (12px `#444`, bottom right):** "trajectories illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`, violet `rgba(74,58,167,0.12)`/`#4a3aa7`, aqua `rgba(25,158,112,0.15)`/`#199e70`, magenta `rgba(213,81,129,0.15)`/`#d55181`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red reserved for genuine alarm states; this page uses none in charts.
- **Data:** all series are hardcoded literal arrays — no `Math.random()`, no seeded draws. The ratchet series `[40,45,50,55,60,65,70,75,80]` follows exactly from the stated +6 / −1 quarterly rates (net +5); the post-cleanup series `[22,27,32,37,42,47,52,57,62]` applies the same +5 to the post-cleanup level of 22. The usage split is 22 used + 58 unused = 80, giving 58/80 = 72.5% and 22/80 = 27.5%. All figures are invented and labeled illustrative; text numbers must match chart numbers to the digit.
- **Scope boundary:** this page is about incentives and process only. It deliberately does not cover policy evaluation order, deny precedence, wildcard breadth, trust policies, or role-assumption chains.
- **Naming:** no real cloud providers, products, or vendors; no invented brand names; people are Alice and Bob; no credential strings or key=value credential syntax.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
