# Branching & Release Strategies

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Branching & Release Strategies

**Subtitle:** Trunk-based vs git-flow — two branching philosophies, and why the real choice is how often you can afford to integrate and ship

## One Company, Two Ways to Branch

**Tags:** `core idea` (blue), `git-flow` (orange), `trunk-based` (green)

- **The company** — one team ships an installed store register app; the other runs a web dashboard
- **The register app** — stores still run v4.2 while v5.0 is built, so v4.x needs its own patch line
- **Git-flow** — Vincent Driessen's famous 2010 model: develop, feature, release, and hotfix branches
- **Trunk-based** — dashboard devs merge to main within a day or two; branches live hours-to-days
- **Releases** — the register cuts a release branch and hardens it; the dashboard tags main and ships

*Example (italic):* The same week the register team hotfixes v4.2 on its own branch, the dashboard team deploys straight from main every afternoon.

**Key point:** A branching strategy is a policy about integration and releases — git-flow structures parallel released versions; trunk-based keeps everyone's code meeting on main continuously.

### Visualization (canvas `c1`, 720×300)

Two-half branch diagram: git-flow's five lanes on top, trunk-based's single trunk with short stubs below, drawn as horizontal lane lines with commit dots and merge arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Same Repo, Two Philosophies: git-flow Lanes vs One Trunk".
- **Git-flow half (top):** lane labels 12px `#444` left-aligned at x=20; lane lines 2px from x=110 to x=690.
  - "main" lane at y=55, ink `#1a5276`; commit dots (radius 4) at x=140, 420, 660 with 11px labels "v4.1", "v4.2", "v5.0" above.
  - "hotfix" lane at y=85, red `#e74c3c`: stub branching down from main at x=440, running to x=520, arrow merging back up to main at x=540; 11px red label "4.2.1" above the stub.
  - "release" lane at y=115, orange `#d95926`: segment x=520 to x=640 branching up from develop at x=520, arrow merging into main at x=660.
  - "develop" lane at y=145, blue `#2a78d6`, full width, commit dots at x=160, 250, 340, 430, 520, 610.
  - "feature" lane at y=175, green `#008300`: two stubs (x=160→280 and x=330→470), each branching down from develop and arrowing back up into it.
- **Divider:** dashed `#e5e9ef` (dash 4/4) horizontal line at y=200.
- **Trunk-based half (bottom):** "main" label at x=20, y=235; 3px `#1a5276` trunk line x=110 to x=690 with commit dots every 40px from x=140 to x=660; three short green `#008300` stubs dipping to y=260 at x=180, 330, 480, each ~50px long, arrowing back into the trunk.
- **Annotation (bold 12px green `#008300`, near x=500, y=282):** "branches live hours-to-days".
- **Caption (12px `#444`, bottom right):** "commit positions schematic".

## Counting the Drift: One 15-Day Branch vs Three 5-Day Branches

**Tags:** `worked example` (blue), `merge hell` (red)

- **The setup** — teammates change ~40 lines a day in code your branch also touches (illustrative)
- **The long branch** — open 15 working days, it meets 15 × 40 = 600 drifted lines in one merge
- **Hand-check** — on day 10 the drift is 10 × 40 = 400 lines, and it only grows until you merge
- **The short branches** — three 5-day branches meet 200 lines each, and every merge resets to zero
- **The compounding** — 600 lines at once conflict with each other; the big merge costs extra

*Example (italic):* The day-15 "merge hell" takes two days to untangle; none of the three 200-line merges took more than an hour.

**Key point:** Deferred integration compounds — merge pain grows faster than branch age, so many small merges beat one big-bang merge of the same total code.

### Visualization (canvas `c2`, 720×300)

Line chart of drifted lines to reconcile vs working day: the long branch climbs steadily to 600 while short branches sawtooth back to zero at every merge.

- **Title (bold 15px, `#1a5276`, top center):** "Drift Against Main: One Long Branch vs Merging Every 5 Days".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = working day 0 to 15 with 12px `#444` tick labels at 0/5/10/15; y = drifted lines 0 to 600, gridlines `#e5e9ef` at 200/400, 12px `#444` y labels.
- **Long-branch line:** red `#e74c3c` 3px line through days `[0, 5, 10, 15]`, drift `[0, 200, 400, 600]`.
- **Short-branch line:** green `#008300` 3px sawtooth through (day, drift) points `[(0,0), (5,200), (5,0), (10,200), (10,0), (15,200)]` — vertical drops at each merge.
- **Merge markers:** small green dots (radius 4) at days 5 and 10 on the zero line, 11px green label "merge" under each.
- **Annotation (bold 13px red `#e74c3c`, near day 12, y=85 px):** "big-bang merge: 600 lines".
- **Annotation (bold 12px green `#008300`, near day 7, y=200 px):** "each merge resets the drift".
- **Caption (12px `#444`, bottom right):** "40 lines/day drift, illustrative".

## Why the Deploy-Every-Day Crowd Picked Trunk

**Tags:** `where it's used` (blue), `DORA research` (green), `trade-offs` (orange)

- **Merge hell** — long-lived branches defer integration, and deferred integration is compounding risk
- **DORA research** — published DORA studies associate trunk-based work with higher delivery performance
- **The price of speed** — trunk-based demands strong CI, fast tests, and feature flags from day one
- **Git-flow's shelter** — its ceremony protects teams with weak automation or regulated release trains
- **On top of either** — release branches, tags, release trains, and hotfix paths ride on both models

*Example (italic):* The dashboard team ships a tagged trunk commit every day; the register team boards a quarterly release train with a hardening branch and a hotfix path per shipped version.

**Key point:** Neither model is universally right — trunk-based buys speed at the price of discipline; git-flow buys safety with ceremony and support for parallel released versions.

### Visualization (canvas `c3`, 720×300)

Two-column fit diagram: three context boxes under each model, showing what situation each philosophy is built for.

- **Title (bold 15px, `#1a5276`, top center):** "Pick by Context: What Each Model Optimizes For".
- **Column headers (y=55):** green `#008300` rounded box (260×34, 8px radius, fill `rgba(0,131,0,0.12)`) centered at x=210 labeled "trunk-based development"; blue `#2a78d6` rounded box (260×34, fill `rgba(42,120,214,0.15)`) centered at x=530 labeled "git-flow (Driessen, 2010)". Bold 13px `#2c3e50` text.
- **Rows (box tops at y=110, 155, 200), each a 260×34 rounded box, 12px `#2c3e50` text, fill `rgba(26,82,118,0.06)`, 1px `#e5e9ef` border:**
  - left column (centered x=210): "one live version, deployed continuously" / "strong CI and fast automated tests" / "unfinished work hidden by flags"
  - right column (centered x=530): "boxed releases: v4.x live while v5 is built" / "weak automation or regulated release trains" / "unfinished work parked on branches"
- **Divider:** dashed `#6b7280` (dash 4/3) vertical line at x=370 from y=90 to y=240.
- **Annotation (bold 13px magenta `#d55181`, centered, y=270):** "released versions in the field? that's the deciding question".

## Copying the Branches, Skipping the Discipline

**Tags:** `common mistake` (red), `feature flags` (green)

- **The cargo cult** — deleting develop and merging daily copies the branch diagram, not the discipline
- **What breaks** — without fast tests and flags, half-finished checkout code reaches users on deploy
- **Not branchless** — trunk-based still uses branches; they just live hours-to-days, not weeks
- **Flags, not branches** — unfinished work merges to main behind a flag that stays off in production
- **The takeaway** — a branching strategy is an integration frequency policy; pick by what you can afford

*Example (italic):* A team adopts trunk-based with 40-minute flaky tests and no flags; main breaks daily, and everyone quietly drifts back to long branches.

**Common mistake:** Treating the branch diagram as the strategy. The strategy is integration frequency — trunk-based without strong CI and feature flags is git-flow with the safety rails removed.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: merging unfinished checkout code without a flag (users hit it) vs behind a flag (deploy is safe, flag flips later).

- **Title (bold 15px, `#1a5276`, top center):** "Unfinished Work on Main: the Feature Flag Makes It Safe".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "no flag"; blue `#2a78d6` rounded box at x=150 labeled "half-built checkout merged", 3px arrow to a blue box at x=370 labeled "daily deploy", 3px arrow to a red `#e74c3c` box at x=560 labeled "users hit broken checkout" with bold 12px red "✗" beside it.
- **Row 2 (boxes centered on y=205), label:** "behind a flag"; blue box at x=150 "checkout merged, flag OFF", arrow to blue box at x=370 "daily deploy", arrow to green `#008300` box at x=560 "users see nothing new" with bold 12px green "✓"; 12px green label "flag ON when ready" under the last box at y=245.
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "the flag, not the branch, hides unfinished work".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 40-lines/day drift rate and every derived count (200 / 400 / 600) are invented and labeled illustrative; git-flow is credited to Vincent Driessen's 2010 published model, and the trunk-based performance link is attributed to published DORA research — cite both as published sources, no invented statistics from either.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
