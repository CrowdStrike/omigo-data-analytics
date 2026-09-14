# CI/CD

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** CI/CD

**Subtitle:** Every push is built and tested by an automated pipeline within minutes, and releases roll out — and roll back — as small, rehearsed, reversible steps

## Every Push Meets the Pipeline

**Tags:** `core idea` (blue), `pipeline` (green), `gatekeeper` (orange)

- **The team** — six developers push to a shared orders service several times a day
- **The pipeline** — every push triggers build → lint → tests → artifact, with no human involved
- **The gate** — a red pipeline blocks the merge button; the machine certifies the build, not a person
- **The old world** — pre-CI teams coded apart for weeks, then endured a dreaded "integration phase"
- **The payoff** — an integration break surfaces minutes after it is created, while context is fresh

*Example (italic):* A push at 10:42 breaks a pricing test and the pipeline goes red at 10:50 — in the pre-CI world the same clash would hide until merge week.

**Key point:** Continuous integration means integrating every push and letting an automated pipeline certify it — integration problems are found in minutes, not discovered weeks later at merge time.

### Visualization (canvas `c1`, 720×300)

Flow diagram of one push moving through the pipeline stages, then branching to a green "merge allowed" gate or a red "merge blocked" gate.

- **Title (bold 15px, `#1a5276`, top center):** "The Pipeline Is the Gatekeeper: No Human Certifies a Build".
- **Stage row (y=115):** five rounded boxes, 92px wide × 40px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border, 12px `#2c3e50` centered labels, at x = 28 "push", x = 132 "build", x = 236 "lint", x = 340 "tests", x = 444 "artifact"; 2px `#6b7280` arrows between consecutive boxes.
- **Verdict branch:** from the "artifact" box, a 2px green `#008300` arrow up-right to a green-bordered box at (588, 62) labeled "merge allowed ✓" (fill `rgba(0,131,0,0.12)`), and a 2px red `#e74c3c` dashed (4/3) arrow down-right to a red-bordered box at (588, 168) labeled "merge blocked ✗" (fill `rgba(231,76,60,0.12)`); both boxes 118px wide × 40px tall.
- **Sub-label (12px `#6b7280`, centered under the stage row at y=175):** "runs automatically on every push".
- **Annotation (bold 13px `#1a5276`, centered near y=255):** "8 minutes from push to verdict — on every push".
- **Caption (12px `#444`, bottom right):** "stage order fixed; verdict is binary".

## A Tuesday of Twelve Pushes

**Tags:** `worked example` (blue), `8-minute verdict` (green)

- **The day** — 12 pushes to the orders service between 9:05am and 4:10pm, each running the 8-minute pipeline
- **The break** — push #4 at 10:42 changes a discount rule and fails 3 pricing tests; red at 10:50
- **The stop** — merging is blocked; the author sees the exact failing test names 8 minutes after typing the bug
- **The fix** — a correction pushed at 11:03 goes green at 11:11; main was unmergeable for 21 minutes
- **Hand-check** — 12 pushes × 8 minutes = 96 pipeline-minutes of machine checking, zero human sign-off

*Example (italic):* The same discount bug in a merge-week world would ride along unnoticed for weeks and surface tangled with everyone else's changes.

**Key point:** The pipeline turned a would-be integration surprise into a 21-minute blip, because the failing test ran 8 minutes after the bug was written — not weeks after.

### Visualization (canvas `c2`, 720×300)

Timeline strip of the day's 12 pushes as dots on a clock axis, one red, with a shaded band marking the 21 minutes main was blocked.

- **Title (bold 15px, `#1a5276`, top center):** "Twelve Pushes, One Red: the Pipeline Catches Push #4 in 8 Minutes".
- **Axis:** horizontal 2px `#999` baseline at y=185, from x=60 to x=660; x = clock time 9:00 to 17:00, 12px `#444` tick labels every 2 hours ("9:00", "11:00", "13:00", "15:00", "17:00").
- **Push dots (radius 7, centered on the baseline):** at minutes since 9:00 `[5, 40, 70, 102, 123, 155, 200, 255, 302, 340, 390, 430]` mapped linearly onto the 600px span (480 minutes total); all dots green `#008300` except index 3 (minute 102 = 10:42) red `#e74c3c`.
- **Blocked band:** vertical `rgba(231,76,60,0.12)` band from minute 110 (10:50) to minute 131 (11:11), full plot height (y=70 to y=185), topped by a 12px red `#e74c3c` label "main blocked 21 min".
- **Dot labels:** bold 12px red "#4 red 10:50" above the red dot; bold 12px green "fix green 11:11" above the dot at minute 123.
- **Annotation (bold 13px `#1a5276`, centered near y=245):** "11 green pushes merged the same day they were written".
- **Caption (12px `#444`, bottom right):** "push times illustrative".

## Delivery, Deployment, and the Rollback Path

**Tags:** `where it's used` (blue), `canary` (green), `rollback` (orange)

- **Two meanings** — continuous DELIVERY: every green build is deployable, a human clicks; continuous DEPLOYMENT: green builds ship themselves
- **The toolkit** — staging environment, a canary slice of traffic, health checks gating each promotion
- **The canary** — the new version gets 5% of traffic first; healthy metrics promote it to 25%, then 100%
- **Rollback** — reverting must be a rehearsed one-command path; an unrehearsed rollback does not exist
- **The yardstick** — DORA metrics: deploy frequency, lead time, time to restore, change-failure rate
- **Speed** — a 45-minute pipeline means 45-minute-minimum fixes when fixing forward; speed is a feature

*Example (italic):* The 2:00pm release runs at 5% traffic for 15 minutes; health checks pass, so it steps to 25% and reaches 100% by 2:30 — a failed check would have rolled it back in 2 minutes.

**Key point:** CI/CD is risk management — make every change small, frequent, and reversible, and no single deploy can hurt you for long.

### Visualization (canvas `c3`, 720×300)

Step chart of new-version traffic share during a canary rollout, with a red dashed alternate path showing the rehearsed rollback.

- **Title (bold 15px, `#1a5276`, top center):** "Canary Rollout: 5% → 25% → 100%, with Rollback as a Designed Path".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = clock time 2:00pm to 3:00pm, 12px `#444` tick labels every 15 minutes; y = traffic on new version 0 to 100%, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` labels.
- **Rollout step line:** green `#008300` 3px through points (time, %) `(2:00, 0) → (2:00, 5) → (2:15, 5) → (2:15, 25) → (2:30, 25) → (2:30, 100) → (3:00, 100)` (vertical risers at each promotion).
- **Health-check markers:** bold 12px green `#008300` "✓ health check" just above the line at 2:15 and 2:30.
- **Rollback path:** red `#e74c3c` 3px dashed (5/4) line from (2:15, 5) down to (2:17, 0), with 12px red label "failed check → back to 0% in 2 min" beside it.
- **Annotation (bold 13px violet `#4a3aa7`, near 2:40, y=95):** "each step is small enough to reverse".
- **Caption (12px `#444`, bottom right):** "traffic shares and timings illustrative".

## When Red Stops Meaning Stop

**Tags:** `common mistake` (red), `flaky tests` (orange)

- **The drift** — one flaky test fails 1 run in 10, and the team learns to click re-run instead of investigating
- **Broken windows** — once some red is "known noise", every red is presumed noise; real breaks slip through
- **Eroded trust** — a gate nobody believes stops nothing; certification quietly reverts to human guesswork
- **The rule** — red is stop-the-line: fix it or revert it within minutes; flaky tests are quarantined same day
- **The tell** — "merged anyway on red" per month is the health metric of the whole system

*Example (italic):* After 12 weeks of tolerating one flaky test, a team ships a genuine breakage because "the suite is always a bit red".

**Common mistake:** Treating a red pipeline as background noise. The gatekeeper only works if the team treats every red as stop-the-line and evicts flaky tests immediately — otherwise the pipeline still runs, but it no longer guards anything.

### Visualization (canvas `c4`, 720×300)

Two-line chart over 12 weeks: share of red runs dismissed as noise rising while share of real breaks caught before merge falls.

- **Title (bold 15px, `#1a5276`, top center):** "Broken Windows: Ignoring One Flaky Test Erodes the Whole Gate".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = weeks 1 to 12, 12px `#444` tick labels at 1, 4, 8, 12; y = percent 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Dismissed line:** red `#e74c3c` 3px through weeks `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]`, "red runs dismissed as noise" percent `[0, 2, 5, 10, 18, 28, 40, 52, 63, 72, 80, 85]`, with bold 12px red label "dismissed as noise" near week 10 above the line.
- **Caught line:** blue `#2a78d6` 3px through the same weeks, "real breaks caught before merge" percent `[100, 98, 96, 92, 86, 80, 72, 64, 55, 48, 42, 38]`, with bold 12px blue label "real breaks caught" near week 4 below the line.
- **Crossover marker:** vertical dashed `#6b7280` (dash 4/3) line at week 8.6 (where the lines cross), 12px `#6b7280` label "trust crosses over" at its top.
- **Annotation (bold 13px orange `#d95926`, near week 6, y=70):** "once red means 'probably flaky', the gate is open".
- **Caption (12px `#444`, bottom right):** "percentages illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); push times, pipeline durations, canary traffic shares, and the week-by-week erosion percentages are invented and labeled illustrative; the worked example's clock times (10:42 push, 10:50 red, 11:03 fix, 11:11 green, 21-minute block) must match between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
