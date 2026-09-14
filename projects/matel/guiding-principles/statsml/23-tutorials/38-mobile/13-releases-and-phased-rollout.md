# Releases & Phased Rollout

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Releases & Phased Rollout

**Subtitle:** A mobile update ships to 1% of users first and ramps up over a week — because once a binary is on someone's phone, you can never take it back

## The Update That Goes Out One Percent at a Time

**Tags:** `core idea` (blue), `staged release` (green), `mobile` (orange)

- **The app** — a food-delivery app with 2,000,000 users ships version 2.4.0 to the app store
- **The plan** — the store offers it to 1% on day 1, 10% on day 3, 50% on day 5, everyone on day 7
- **The watch** — the team compares crash rate on 2.4.0 against the old version at every stage
- **The spike** — on day 4, crash rate on 2.4.0 hits 2.1% vs the usual 0.2%; something is broken
- **The halt** — the team stops the rollout at 10%; the store stops auto-offering 2.4.0 to new users

*Example (italic):* The rollout freezes on day 4 at 10% — the other 90% of users never see 2.4.0 and keep running the stable old version.

**Key point:** A phased rollout releases a version to a growing slice of users so a bad build is caught while it is still small — the ramp is the safety net.

### Visualization (canvas `c1`, 720×300)

Step chart of the rollout percentage over the week: the planned ramp to 100% (dashed) vs the actual rollout halted at 10% when crashes spike.

- **Title (bold 15px, `#1a5276`, top center):** "Planned Ramp vs the Halt at 10%".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days 1 to 7 with 12px `#444` tick labels "day 1"–"day 7"; y = % of users offered 2.4.0, 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Planned line:** mute `#6b7280` 2px dashed (dash 5/4) step line through days `[1, 2, 3, 4, 5, 6, 7]`, percent `[1, 1, 10, 10, 50, 50, 100]`.
- **Actual line:** blue `#2a78d6` 3px step line through the same days, percent `[1, 1, 10, 10, 10, 10, 10]` — flat after the halt.
- **Crash marker:** vertical dashed red `#e74c3c` (dash 4/3) line at day 4, bold 12px red label "crash rate 2.1% vs 0.2% — halt" at its top.
- **Annotation (bold 13px blue `#2a78d6`, near day 5.5, y=170):** "rollout frozen at 10% — 90% never get the bad build".
- **Caption (12px `#444`, bottom right):** "percentages are the rollout plan; crash rates illustrative".

## Counting Who Got the Bad Build

**Tags:** `worked example` (blue), `blast radius` (green)

- **The base** — 2,000,000 users; each rollout stage is a straight percentage of that base
- **Stage 1** — 1% of 2,000,000 = 20,000 users get 2.4.0 on day 1
- **Stage 2** — 10% of 2,000,000 = 200,000 users have it by day 3; the halt lands here
- **The avoided** — 50% would have been 1,000,000 and 100% the full 2,000,000
- **Hand-check** — halting at 10% exposed 200,000 users, one tenth of a full rollout
- **The spared** — 2,000,000 − 200,000 = 1,800,000 users never receive the crashing build

*Example (italic):* Multiply it yourself: 10% × 2,000,000 = 200,000 exposed at the halt — against 2,000,000 had the release gone straight to everyone.

**Key point:** The blast radius is just percentage × user base — freezing at 10% capped the damage at 200,000 users instead of all 2,000,000.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of users holding 2.4.0 at each rollout stage, with the reached stages solid and the avoided stages hollow.

- **Title (bold 15px, `#1a5276`, top center):** "Users on the Bad Build at Each Stage (of 2,000,000)".
- **Axis:** vertical 2px `#999` baseline at x=170, bars extend right, max width 480 = 2,000,000 users (linear scale, 480px / 2M).
- **Rows (top to bottom at y = 70, 115, 160, 205), each with a left-aligned 12px `#444` label at x=20:**
  - "1% — day 1: 20,000": blue `#2a78d6` solid bar width 5
  - "10% — day 3: 200,000": blue solid bar width 48, bold 12px red `#e74c3c` label "halted here" at bar end
  - "50% — day 5: 1,000,000": hollow bar (2px `#6b7280` dashed outline, no fill) width 240, 11px `#6b7280` label "avoided"
  - "100% — day 7: 2,000,000": hollow dashed bar width 480, 11px `#6b7280` label "avoided"
- **Bar style:** 22px tall, solid bars fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, 11px `#444` count labels at bar ends where not otherwise labeled.
- **Annotation (bold 13px green `#008300`, right side near y=250):** "the halt spared 1,800,000 users".
- **Caption (12px `#444`, bottom right):** "user base 2,000,000 illustrative; stage math exact".

## You Can Halt, but You Can't Recall

**Tags:** `where it's used` (blue), `feature flags` (green), `no un-ship` (orange)

- **The one-way door** — a halt stops new installs, but the 200,000 phones that got 2.4.0 keep it
- **No recall** — no app store lets a developer reach out and delete a version off users' devices
- **The slow fix** — shipping fixed 2.4.1 means store review plus waiting for users to update: days
- **The escape hatch** — a feature flag lets the server switch the broken code path off in minutes
- **The habit** — mobile teams ship risky features dark, behind flags, exactly because of this
- **Web contrast** — a website redeploys for everyone instantly; a mobile binary is out for good

*Example (italic):* With a kill switch, the 4,200 crashing users per day drop to the ~400 baseline within a day; waiting for 2.4.1 keeps thousands crashing all week.

**Key point:** Once shipped, a mobile binary is beyond reach — the phased ramp limits how many get it, and feature flags are the only remote control over code already on phones.

### Visualization (canvas `c3`, 720×300)

Two-line timeline of crashing users per day among the 200,000 exposed: waiting for the fixed binary (slow decay) vs flipping a server-side kill switch (immediate drop).

- **Title (bold 15px, `#1a5276`, top center):** "Killing the Crash: Feature Flag vs Waiting for 2.4.1".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days after the halt 0 to 6, 12px `#444` tick labels "day 0"–"day 6"; y = crashing users per day 0 to 5,000, gridlines `#e5e9ef` at 1,250/2,500/3,750.
- **Binary-fix line:** orange `#d95926` 3px line through days `[0, 1, 2, 3, 4, 5, 6]`, crashes `[4200, 4200, 3400, 2200, 1200, 700, 400]` — flat while 2.4.1 is in review, then decaying as users update.
- **Kill-switch line:** green `#008300` 3px line through the same days, crashes `[4200, 400, 400, 400, 400, 400, 400]` — cliff on day 1.
- **Baseline marker:** horizontal dashed `#6b7280` (dash 4/3) line at 400 crashes, 12px `#6b7280` label "normal baseline ~400" at its left.
- **Annotation (bold 13px green `#008300`, near day 3, y=90):** "the flag fixes in a day what the binary fixes in a week".
- **Caption (12px `#444`, bottom right):** "crash counts illustrative — 2.1% vs 0.2% of 200,000 exposed users".

## Halting Is Not Rolling Back

**Tags:** `common mistake` (red), `version long tail` (orange)

- **The confusion** — "we halted the rollout" gets reported as "the bug is gone"; it is not
- **What a halt does** — it only stops new users from receiving 2.4.0; existing installs stay
- **No downgrade** — app stores will not push an older version over a newer one already installed
- **The long tail** — even after 2.4.1 ships, users on the bad build fade out slowly, never to zero
- **The residue** — a month later 2.5% of users, about 50,000 people, still run crashing 2.4.0
- **The lesson** — dashboards must track crashes by version; the old build haunts the numbers

*Example (italic):* Four weeks after the fix ships, 2.5% × 2,000,000 = 50,000 users still run 2.4.0 — auto-update off, storage full, or simply never opened the store.

**Common mistake:** Treating a halted rollout as a rollback. Nothing was rolled back — the bad build stays on every phone that got it, and only user-by-user updates slowly retire it.

### Visualization (canvas `c4`, 720×300)

Decay line of the share of users still on bad build 2.4.0 in the weeks after fixed 2.4.1 ships — falling fast at first, then flattening into a long tail that never reaches zero.

- **Title (bold 15px, `#1a5276`, top center):** "The Bad Build Never Fully Dies: Share Still on 2.4.0".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = weeks after 2.4.1 ships, 0 to 8, 12px `#444` tick labels "wk 0"–"wk 8" every 2 weeks; y = % of all users on 2.4.0, 0 to 12, gridlines `#e5e9ef` at 3/6/9.
- **Decay line:** violet `#4a3aa7` 3px line with `rgba(74,58,167,0.15)` area fill to the baseline, through weeks `[0, 1, 2, 3, 4, 6, 8]`, percent `[10, 6, 4, 3, 2.5, 2.2, 2]`.
- **Zero marker:** horizontal dashed red `#e74c3c` (dash 4/3) segment along y=0, bold 12px red label "true rollback would be here — unreachable" just above it at the right.
- **Point label (12px `#444`):** at week 4, "2.5% ≈ 50,000 users".
- **Annotation (bold 13px violet `#4a3aa7`, near week 5, y=80):** "halting froze it at 10% — only user updates shrink it".
- **Caption (12px `#444`, bottom right):** "decay curve illustrative; week-4 arithmetic exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 2,000,000 user base, the 1/10/50/100 rollout schedule, crash rates 2.1% vs 0.2%, and the decay percentages are invented and labeled illustrative; the stage exposure counts (20,000 / 200,000 / 1,000,000 / 2,000,000), the 1,800,000 spared, the 4,200 vs 400 daily crash counts, and the week-4 figure (2.5% × 2,000,000 = 50,000) follow exactly from that arithmetic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
