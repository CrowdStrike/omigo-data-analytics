# Feature Flags

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Feature Flags

**Subtitle:** A feature flag is a runtime if-statement that separates deploying code from releasing it — new code ships dark, turns on without a deploy, and turns off in seconds when it misbehaves

## The Checkout Rewrite That Ships Dark

**Tags:** `core idea` (blue), `deploy vs release` (green), `kill switch` (red)

- **The rewrite** — a shop rebuilds its checkout page; the new code merges and deploys on Monday
- **The flag** — every request hits `if flag("new_checkout") ...`; the flag is off, so users see old checkout
- **Dark code** — Tuesday and Wednesday bring two more deploys; still zero users see the new page
- **The release** — Thursday someone flips the flag in a config panel: users switch with no deploy at all
- **The kill switch** — if the new page misbehaves, flipping the flag off takes seconds, not a rollback deploy

*Example (italic):* Three deploys land Monday to Wednesday with the new checkout invisible; Thursday's release is one config flip — no build, no deploy, no restart.

**Key point:** Deploy means code reached production; release means users experience it. A flag is the runtime conditional that lets those two events happen days apart — in either direction.

### Visualization (canvas `c1`, 720×300)

Timeline chart of one week: percentage of users seeing the new checkout stays at 0 through three deploy markers, then jumps to 100 at the flag flip.

- **Title (bold 15px, `#1a5276`, top center):** "Three Deploys, Zero Exposure — Release Is the Flag Flip".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days "Mon" to "Fri" with 12px `#444` tick labels per day; y = % of users on new checkout 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Exposure line:** blue `#2a78d6` 3px step line through days `[1, 2, 3, 4, 4, 5]`, exposure `[0, 0, 0, 0, 100, 100]` — flat at 0, vertical step to 100 at Thursday.
- **Deploy markers:** vertical dashed `#6b7280` (dash 4/3) lines at days 1, 2, 3 with 12px `#6b7280` labels "deploy #1", "deploy #2", "deploy #3" near the top.
- **Release marker:** vertical solid green `#008300` 2px line at day 4, bold 12px green label "flag on = release".
- **Annotation (bold 13px blue `#2a78d6`, near day 2, y=120):** "code is live but dark — users see nothing new".
- **Caption (12px `#444`, bottom right):** "timeline illustrative".

## Turning the Dial: 1% → 10% → 100%

**Tags:** `worked example` (blue), `progressive rollout` (green)

- **The dial** — the flag takes a percentage, not just on/off: it hashes each user id into or out of the rollout
- **Day 1** — 1% of 200,000 daily users, so 2,000 people hit new checkout; error rate 0.22% vs 0.21% baseline
- **Day 2** — dial to 10%: 20,000 users exposed; error rate reads 0.24%, so the team holds at 10% a day
- **Day 3** — the blip subsides to 0.22%; day 4 goes to 50% (100,000 users), day 5 to 100% (all 200,000)
- **Hand-check** — 10% of 200,000 = 20,000 exposed users; a bug at that step touches 20,000, not 200,000

*Example (italic):* The day-2 error blip (0.24% vs 0.21% baseline) is seen by 20,000 users instead of all 200,000 — the dial capped the blast radius at one tenth.

**Key point:** A percentage rollout makes blast radius a dial: expose 1%, watch the metrics, turn it up only when the numbers hold — and turn it back down the moment they don't.

### Visualization (canvas `c2`, 720×300)

Step chart of rollout percentage over five days with the error-rate readings annotated at each step against the baseline.

- **Title (bold 15px, `#1a5276`, top center):** "Blast Radius as a Dial: Hold at 10% Until the Metrics Say Go".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = "Day 1" to "Day 5" with 12px `#444` tick labels; y = rollout % 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Rollout steps:** blue `#2a78d6` 3px step line through days `[1, 2, 3, 4, 5]`, rollout `[1, 10, 10, 50, 100]`, with `rgba(42,120,214,0.25)` fill under the steps.
- **Step labels (bold 12px `#1a5276`, above each step):** "2,000 users", "20,000", "20,000", "100,000", "200,000".
- **Error readings (12px `#444`, below each step label):** "err 0.22%", "err 0.24%", "err 0.22%", "err 0.22%", "err 0.21%"; the day-2 reading in bold red `#e74c3c`.
- **Hold marker:** orange `#d95926` bracket over days 2–3 with bold 12px orange label "held: error blip 0.24% vs 0.21% baseline".
- **Annotation (bold 13px green `#008300`, near day 4, y=70):** "dial up only when the numbers hold".
- **Caption (12px `#444`, bottom right):** "user counts and error rates illustrative".

## The Kill Switch and Everything Else Flags Buy

**Tags:** `where it's used` (blue), `incident mitigation` (red), `experimentation` (green)

- **Trunk-based work** — unfinished features merge to main behind an off flag instead of living on long branches
- **Targeting** — the flag turns on for employees first, then a beta cohort, before any external user sees it
- **A/B experiments** — an experiment is a flag with metrics attached: same plumbing, plus a comparison
- **Incidents** — when a released change misbehaves, flag off mitigates in ~0.5 min; a rollback deploy takes ~45
- **The contrast** — building and deploying a hotfix runs ~120 min while users keep hitting the broken path

*Example (italic):* At 2:14am the new checkout starts erroring; the on-call flips the flag off at 2:15am — the alternative was a 45-minute rollback deploy with errors the whole way.

**Key point:** One mechanism buys four things — safe merging of unfinished work, staged targeting, A/B experimentation, and a kill switch that mitigates incidents in seconds instead of a redeploy.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing minutes-to-mitigate a bad change across four responses, from flag flip to hotfix deploy.

- **Title (bold 15px, `#1a5276`, top center):** "Minutes Until Users Stop Seeing the Bad Change".
- **Axis:** left-aligned 12px `#444` row labels at x=20, bars start at x=230, 2px `#999` vertical baseline at x=230, max bar width 440 (scaled to 120 min, ≈3.7 px/min).
- **Rows (top to bottom at y = 70, 120, 170, 220):**
  - "flag off (kill switch) — 0.5 min": green `#008300` bar width 4, bold 12px green label "seconds"
  - "config revert + propagate — 10 min": aqua `#199e70` bar width 37
  - "rollback deploy — 45 min": orange `#d95926` bar width 165
  - "hotfix: code, build, deploy — 120 min": red `#e74c3c` bar width 440
- **Bar style:** 14px tall, solid fills, 11px `#444` minute labels at bar ends.
- **Annotation (bold 13px green `#008300`, right side near y=60):** "the kill switch is the whole point".
- **Caption (12px `#444`, bottom right):** "durations illustrative; ratios are the message".

## Flag Debt and the Doubling State Space

**Tags:** `common mistake` (red), `flag debt` (orange), `fail safe` (green)

- **Doubling paths** — every flag doubles the state space: 2 flags = 4 code paths, 5 flags = 32
- **Test what matters** — nobody tests all 32; pick the combinations users actually hit and test those
- **Flag debt** — "temporary" flags become permanent: a graveyard of stale conditionals nobody dares remove
- **The remedy** — every flag gets an owner and an expiry date at creation; removal is scheduled, not hoped for
- **Critical infra** — the flag service itself can take the site down; documented outages have started as flag misconfigurations
- **Fail safe** — when the flag service is unreachable, defaults must fall back to the safe, boring path

*Example (italic):* A team audits its flag system and finds 40 active flags — 29 past any plausible removal date, three guarding code paths whose authors have left the company.

**Common mistake:** Treating flags as free. Each one is a live conditional that multiplies paths, ages into debt, and rides on a config service that is now critical infrastructure — hygiene (owner, expiry, safe default) is part of creating the flag, not an afterthought.

### Visualization (canvas `c4`, 720×300)

Vertical bar chart of code paths versus active flag count, doubling from 2 to 32, with a debt annotation.

- **Title (bold 15px, `#1a5276`, top center):** "Every Flag Doubles the State Space".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = active flags 1 to 5, 12px `#444` tick labels centered under bars; y = code paths 0 to 32, gridlines `#e5e9ef` at 8/16/24.
- **Bars:** five bars at flags `[1, 2, 3, 4, 5]`, paths `[2, 4, 8, 16, 32]`, width 70px, centered at x = 120/240/360/480/600; first three bars blue `rgba(42,120,214,0.35)` with 2px `#2a78d6` border, last two orange `rgba(217,89,38,0.30)` with 2px `#d95926` border.
- **Value labels:** bold 12px `#1a5276` path counts ("2", "4", "8", "16", "32") above each bar.
- **Annotation (bold 13px orange `#d95926`, near x=430, y=70):** "32 combinations — test the ones users actually hit".
- **Annotation 2 (bold 12px red `#e74c3c`, near x=140, y=110):** "stale flags keep paying this cost".
- **Caption (12px `#444`, bottom right):** "paths = 2^flags, exact; flag counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); user counts, error rates, and mitigation minutes are invented and labeled illustrative; the path counts (2 / 4 / 8 / 16 / 32) are exact powers of two for the flag counts shown.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
