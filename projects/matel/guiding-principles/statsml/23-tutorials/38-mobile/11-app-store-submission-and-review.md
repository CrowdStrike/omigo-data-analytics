# App Store Submission & Review

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** App Store Submission & Review

**Subtitle:** Every mobile app update passes through a store review gate — human review on iOS, mostly automated on Android — so releases take hours to days, not the minutes a web deploy takes

## Code-Complete Monday, Live Friday

**Tags:** `core idea` (blue), `release gate` (orange), `days not minutes` (green)

- **The app** — a coffee chain's loyalty app has a one-line fix: the "redeem" button shows the wrong points
- **The web twin** — the same fix on the chain's website deploys in 15 minutes, live for every visitor
- **The gate** — the app version must be submitted to the store, where a reviewer (human on iOS) approves it
- **The timeline** — code-complete Monday, submitted Tuesday, in review Wednesday–Thursday, live Friday
- **The difference** — you push a web deploy yourself; a store release is a request someone else grants

*Example (italic):* The Monday fix is on the website by lunch, but app users see it Friday — four days later.

**Key point:** An app store release passes through a review gate someone else controls, so the unit of shipping time is days, not minutes — and the schedule is not fully yours.

### Visualization (canvas `c1`, 720×300)

Two-row Gantt timeline on a shared Monday-to-Friday axis: the web deploy (done in minutes) vs the app store release (build, queue, review, live).

- **Title (bold 15px, `#1a5276`, top center):** "The Same One-Line Fix: 15 Minutes on the Web, 4 Days in the App Store".
- **Axes:** plot from x=110 to x=660 (550 wide), days 0–5 mapped linearly (110px per day); vertical gridlines `#e5e9ef` at each day boundary; 12px `#444` tick labels "Mon" "Tue" "Wed" "Thu" "Fri" centered under each day; row labels 12px `#444` at x=20: "web site" at y=115, "mobile app" at y=200.
- **Web row (bar y=100, 26px tall):** green `#008300` solid segment for day `[0, 0.12]` (the 15-minute deploy, drawn ≥12px wide), then a 3px green line from day 0.12 to day 5 at bar mid-height with 12px green label "live" above it near day 2.
- **Mobile row (bar y=186, 26px tall), four segments with 11px `#2c3e50` labels inside or above:** blue `#2a78d6` fill `rgba(42,120,214,0.30)` for days `[0, 1]` "build + QA (Mon)"; yellow `#c98500` fill for days `[1, 2]` "submitted, in queue (Tue)"; violet `#4a3aa7` fill for days `[2, 4]` "in review (Wed–Thu)"; green `#008300` solid for days `[4, 4.5]` "live (Fri)".
- **Annotation (bold 13px green `#008300`, near day 2.5, y=70):** "web users get the fix 4 days earlier".
- **Caption (12px `#444`, bottom right):** "durations illustrative — review times vary".

## The Submission Checklist, Hour by Hour

**Tags:** `worked example` (blue), `checklist` (green), `review queue` (orange)

- **Metadata** — release notes, version number, and description updates take about 30 minutes
- **Screenshots** — fresh screenshots for two phone sizes take about 2 hours
- **Privacy labels** — declaring what data the app collects and why takes about 1 hour
- **The upload** — building, signing, and uploading the binary takes about 30 minutes
- **The review** — a human reviewer checks the app against store guidelines: about 36 hours of waiting
- **The release** — after approval, one click publishes; the store propagates it in about 1 hour

*Example (italic):* The team spends 4 hands-on hours on the checklist, then waits 36 hours for a stranger's approval — 41 hours end to end.

**Key point:** The team controls only 4 of the 41 total hours; the rest is the review queue — so prepare the checklist carefully and never burn a review cycle on a typo.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of hours per submission step; the review bar dwarfs every step the team controls.

- **Title (bold 15px, `#1a5276`, top center):** "Where the 41 Hours Go: 4 Hours of Work, 36 Hours of Waiting".
- **Axis:** vertical 2px `#999` baseline at x=190; bars extend right, scale 12px per hour (36h → 432px); 12px `#444` step labels right-aligned at x=180.
- **Rows (top to bottom at y = 62, 96, 130, 164, 198, 232), bars 20px tall, hours `[0.5, 2, 1, 0.5, 36, 1]`:**
  - "metadata — 0.5h": blue `#2a78d6` fill `rgba(42,120,214,0.35)`, width 6 (drawn ≥6px)
  - "screenshots — 2h": blue, width 24
  - "privacy labels — 1h": blue, width 12
  - "upload build — 0.5h": blue, width 6
  - "human review — 36h": yellow `#c98500` fill `rgba(201,133,0,0.35)` with 2px `#c98500` border, width 432
  - "release — 1h": green `#008300` fill `rgba(0,131,0,0.30)`, width 12
- **Value labels:** 11px `#444` hour values at each bar end.
- **Annotation (bold 13px magenta `#d55181`, inside the review bar near x=400, y=205):** "36 of 41 hours are the review wait".
- **Caption (12px `#444`, bottom right):** "hours illustrative — queues vary by week".

## Why a One-Line Hotfix Takes Two Weeks to Reach Everyone

**Tags:** `why it matters` (blue), `hotfix latency` (orange), `release trains` (green)

- **Hotfix latency** — a crash found today cannot reach users today; the review gate sits in between
- **The ramp** — even after the fix is live in the store, users update slowly over days
- **The numbers** — fix live on day 2; by day 7 about 68% have updated; by day 14 about 90%
- **Batching** — because each release costs days, teams bundle features into weekly or biweekly trains
- **Planning** — launches, marketing, and A/B tests schedule around the review, not the code merge

*Example (italic):* A crash fix coded Monday is live Wednesday, yet on day 14 roughly 1 in 10 users still runs the crashing build.

**Key point:** When shipping costs days, behavior changes — mobile teams batch work into release trains and treat every hotfix as an expensive, planned event.

### Visualization (canvas `c3`, 720×300)

Line chart of the share of users running the fixed version, by day since the bug was found; the review gate delays day zero and the update ramp stretches the tail.

- **Title (bold 15px, `#1a5276`, top center):** "A Fix Is Live on Day 2 — But Users Arrive Over Two Weeks".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days since bug found, 0 to 14, 12px `#444` tick labels at 0/2/4/7/10/14; y = % of users on fixed version, 0 to 100, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` labels.
- **Adoption line:** blue `#2a78d6` 3px line with 4px dots through days `[0, 2, 3, 4, 5, 7, 10, 14]`, percent `[0, 0, 22, 40, 52, 68, 82, 90]` — flat at 0 until the fix clears review, then a decelerating climb.
- **Review marker:** vertical dashed `#6b7280` (dash 4/3) line at day 2, 12px `#6b7280` label "fix live in store" at its top.
- **Annotation (bold 13px red `#e74c3c`, near day 10, y=85):** "day 14: 1 in 10 users still runs the bug".
- **Caption (12px `#444`, bottom right):** "adoption curve illustrative".

## The Friday Launch That Slipped a Week

**Tags:** `common mistake` (red), `rejection loop` (orange)

- **The plan** — the team schedules the launch email for Friday, assuming approval is a formality
- **The rejection** — on Thursday the reviewer flags a mismatch between screenshots and the actual app
- **The loop** — fix, resubmit Friday, wait through a second full review; live the next Wednesday
- **The cost** — one rejection adds a full review cycle; the launch slips from Friday to Wednesday
- **Not instant** — even approval is no light switch: staged rollouts and update lag stretch it further
- **The habit** — treat approval day as an estimate, never a promise; announce after approval, not before

*Example (italic):* The Friday launch email goes out on schedule — pointing at an app version still sitting in the review queue.

**Common mistake:** Treating the review as a rubber stamp. A rejection restarts the clock, so never anchor a public date to a build that has not been approved yet.

### Visualization (canvas `c4`, 720×300)

Two-row timeline on a shared 10-day axis: the planned release (submit Tuesday, live Friday) vs reality with one rejection (live the next Wednesday).

- **Title (bold 15px, `#1a5276`, top center):** "One Rejection: the Friday Launch Lands the Next Wednesday".
- **Axes:** plot from x=110 to x=660 (550 wide), days 0–10 mapped linearly (55px per day); vertical gridlines `#e5e9ef` at each day; 11px `#444` tick labels "Mon" "Tue" "Wed" "Thu" "Fri" "Sat" "Sun" "Mon" "Tue" "Wed" under days 0–9; row labels 12px `#444` at x=20: "the plan" at y=115, "reality" at y=205.
- **Plan row (bar y=100, 24px tall):** blue `#2a78d6` fill `rgba(42,120,214,0.30)` for days `[1, 3]` "in review", green `#008300` solid for days `[4, 4.5]` "live Fri", 11px labels above segments.
- **Reality row (bar y=190, 24px tall):** blue fill for days `[1, 3]` "review #1"; red `#e74c3c` 14px bold "✗ rejected" marker at day 3 with a red 2px tick; orange `#d95926` fill `rgba(217,89,38,0.30)` for days `[3, 4]` "fix + resubmit"; violet `#4a3aa7` fill `rgba(74,58,167,0.25)` for days `[4, 8]` "review #2"; green solid for days `[8, 8.5]` "live Wed".
- **Email marker:** vertical dashed `#c98500` (dash 4/3) line at day 4 spanning both rows, bold 12px `#c98500` label "launch email already sent" at its top.
- **Annotation (bold 13px red `#e74c3c`, near day 6.5, y=70):** "one rejection ≈ 5 extra days".
- **Caption (12px `#444`, bottom right):** "days illustrative — rejection reasons vary".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); every number is invented and labeled illustrative — checklist hours `[0.5, 2, 1, 0.5, 36, 1]` (total 41, review 36), adoption days `[0, 2, 3, 4, 5, 7, 10, 14]` with percent `[0, 0, 22, 40, 52, 68, 82, 90]`, timeline day spans as listed per segment; text numbers (4 of 41 hours, 68% by day 7, 90% by day 14, 5-day slip) must match the charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
