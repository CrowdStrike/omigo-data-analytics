# Jenkins & GitHub Actions

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Jenkins & GitHub Actions

**Subtitle:** Continuous integration is a machine that runs your tests on every proposed change — Jenkins is a server you run yourself, GitHub Actions is a YAML file that lives next to your code

## The Test Suite That Guards Every Merge

**Tags:** `core idea` (blue), `continuous integration` (green), `pull requests` (orange)

- **The team** — six developers share one codebase; anyone's change can break someone else's feature
- **The rule** — every pull request runs the full test suite before a human even reviews it
- **The trigger** — pushing a branch or opening a PR fires an event; the CI machine picks it up
- **The catch** — PR #214 renames a field; 411 of 412 tests pass, one fails on the old name
- **The gate** — the failing check blocks the merge button; the bug never reaches the main branch

*Example (italic):* PR #214 shows a red ✗ five minutes after push; the author fixes the field name and merges green the same hour.

**Key point:** Continuous integration runs your checks automatically on every proposed change — problems surface before merge, while the change is still one small, easy-to-fix diff.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram of one pull request triggering one automatic test run that ends in a blocked merge.

- **Title (bold 15px, `#1a5276`, top center):** "One Pull Request, One Automatic Test Run".
- **Boxes (rounded 8px radius, 44px tall, at y=110, 12px `#2c3e50` text, 3px `#6b7280` arrows between):**
  - x=20, width 140, blue fill `rgba(42,120,214,0.15)`: "PR #214 pushed"
  - x=200, width 140, violet fill `rgba(74,58,167,0.12)`: "event fires workflow"
  - x=380, width 140, blue fill `rgba(42,120,214,0.15)`: "runner: 412 tests"
  - x=560, width 145, red fill `rgba(231,76,60,0.12)`: "1 failed — merge blocked"
- **Time ticks (11px `#6b7280`, centered under each box at y=175):** "t=0", "+5s", "+20s", "+5m10s".
- **Failure flag (bold 12px red `#e74c3c`, under the last box at y=200):** "✗ bug stopped before main".
- **Annotation (bold 13px green `#008300`, centered near y=255):** "no human ran anything — the push was the button".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Five Minutes Inside One CI Run

**Tags:** `worked example` (blue), `pipeline stages` (green)

- **The stages** — one run is a fixed recipe: check out code, install dependencies, run tests, report
- **The clock** — checkout 12s, install 94s, tests 176s, report 8s (all illustrative)
- **Hand-check** — 12 + 94 + 176 + 8 = 290s, so the whole run takes 4m 50s (exact sum)
- **The pace** — 412 tests in 176s is roughly 0.43 seconds per test on the runner
- **The verdict** — one failed assertion turns the whole run red; the PR page names the stage and test

*Example (italic):* The red run reports "test_billing_export: field 'amount' not found" at 4m 12s — test 342 of 412, deep in the test stage.

**Key point:** A CI run is just a scripted sequence anyone could type by hand — its value is that the machine repeats it identically, on a clean machine, for every single change.

### Visualization (canvas `c2`, 720×300)

Single horizontal stacked stage bar for the 290-second run, drawn at an exact scale of 2 pixels per second, with a red marker at the failing test.

- **Title (bold 15px, `#1a5276`, top center):** "Anatomy of One Run: 290 Seconds, Four Stages".
- **Bar (40px tall at y=130, left edge x=60, total width 580 = 290s × 2px/s):** segments in order:
  - checkout 12s → width 24, blue `#2a78d6`
  - install deps 94s → width 188, aqua `#199e70`
  - run 412 tests 176s → width 352, fill `rgba(0,131,0,0.30)` with 2px `#008300` border
  - report 8s → width 16, violet `#4a3aa7`
- **Stage labels (12px `#444`, above the bar at y=115, small segments with leader lines):** "checkout 12s", "install deps 94s", "run 412 tests 176s", "report 8s".
- **Failure marker:** vertical 2px red `#e74c3c` line at x=564 (252s = 4m12s into the run), bold 12px red label "test 342 of 412 fails here" above at y=95.
- **Axis:** 2px `#999` baseline at y=190 from x=60 to x=640; 12px `#444` tick labels "0s", "60s", "120s", "180s", "240s" at x = 60 + 2×t, plus "290s" at x=640.
- **Annotation (bold 13px `#1a5276`, centered near y=235):** "installing dependencies eats a third of the run".
- **Caption (12px `#444`, bottom right):** "durations illustrative; scale exact at 2px per second".

## A Server You Run vs YAML Next to the Code

**Tags:** `where it's used` (blue), `Jenkins` (orange), `GitHub Actions` (green)

- **Jenkins** — a self-hosted CI server (renamed from Hudson in 2011); you install and patch the machine
- **Plugins** — Jenkins does nearly everything via community plugins: source control, notifications, reports
- **Jenkinsfile** — Jenkins 2 (2016) moved the pipeline definition into a file checked into the repo
- **GitHub Actions** — CI as YAML workflows in `.github/workflows/`, triggered by push and PR events (2019)
- **Hosted runners** — Actions provisions a fresh machine per job; there is no server to keep alive
- **The trade** — Jenkins gives full control on your own hardware; Actions trades control for zero upkeep

*Example (italic):* The same 412-test suite is a Jenkinsfile running on the team's own build box, or a 30-line workflow YAML running on a hosted runner.

**Key point:** The concept is identical in both — an event triggers a scripted run that reports a verdict — the models differ in who owns the machine: you (Jenkins) or the host (Actions).

### Visualization (canvas `c3`, 720×300)

Two-row architecture diagram: the Jenkins self-hosted path on top, the GitHub Actions hosted path below, each as three boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Who Owns the Machine: Self-Hosted Server vs Hosted Workflow".
- **Row 1 (boxes 44px tall at y=85), row label 12px `#444` at x=20:** "Jenkins";
  - x=110, width 140, blue fill `rgba(42,120,214,0.15)`: "repo webhook fires"
  - x=290, width 210, orange fill `rgba(230,126,34,0.15)`: "your Jenkins server + plugins", 11px `#6b7280` sub-label below the box: "always on — you patch, upgrade, scale it"
  - x=540, width 160, blue fill `rgba(42,120,214,0.15)`: "Jenkinsfile stages run"
- **Row 2 (boxes 44px tall at y=195), row label:** "GitHub Actions";
  - x=110, width 140, blue fill `rgba(42,120,214,0.15)`: "push / PR event"
  - x=290, width 210, violet fill `rgba(74,58,167,0.12)`: "workflow YAML in the repo", 11px `#6b7280` sub-label: "versioned next to the code it tests"
  - x=540, width 160, green fill `rgba(0,131,0,0.12)`: "fresh hosted runner", 11px `#6b7280` sub-label: "created per job, thrown away after"
- **Arrows:** 3px `#6b7280` between boxes in each row.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=275):** "same idea, different landlord: event → script → verdict".

## A Green Check Is Not a Proof

**Tags:** `common mistake` (red), `flaky tests` (orange)

- **The habit** — a test fails randomly, so the team clicks "re-run" until the check turns green
- **The numbers** — the same commit run 10 times: 7 green, 3 red, with zero code changes between runs
- **The rot** — once red can mean "flaky", real failures get re-run away instead of investigated
- **The limit** — green only means the tests you wrote passed; untested behavior stays unchecked
- **The fix** — quarantine or repair flaky tests fast; a gate the team ignores protects nothing

*Example (italic):* Run 3 fails on a time-of-day-dependent assertion; run 4 of the identical commit passes — the "fix" was rerunning.

**Common mistake:** Treating the green check as truth and the red check as noise. CI is only as trustworthy as its least reliable test — flakiness quietly turns the merge gate into a decoration.

### Visualization (canvas `c4`, 720×300)

Row of ten pass/fail squares for ten CI runs of the identical commit, showing a flaky 70% pass rate.

- **Title (bold 15px, `#1a5276`, top center):** "Same Commit, Ten Runs: 7 Green, 3 Red, Zero Code Changes".
- **Squares (48×48, rounded 6px, at y=110, x = 60 + i×62 for i = 0..9):** results `['P','P','F','P','F','P','P','P','F','P']`; pass squares fill `rgba(0,131,0,0.30)` with a bold 16px `#008300` "✓" centered; fail squares fill `rgba(231,76,60,0.15)` with a bold 16px `#e74c3c` "✗" centered.
- **Run labels (11px `#6b7280`, centered under each square at y=180):** "run 1" through "run 10".
- **Annotation 1 (bold 13px red `#e74c3c`, centered near y=220):** "the code was identical every time".
- **Annotation 2 (bold 12px orange `#d95926`, centered near y=250):** "pass rate 70% — would you trust this gate?".
- **Caption (12px `#444`, bottom right):** "pass/fail pattern illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and pixel positions above (no randomness); PR number, test count (412), stage durations (12/94/176/8s), failing-test position (342 at 252s) and the ten-run pass/fail pattern are invented and labeled illustrative; the 290s total, the 2px-per-second scale, and the 70% pass rate (7 of 10) are exact arithmetic on those illustrative inputs. Jenkins facts (Hudson renamed Jenkins in 2011, plugin ecosystem, Jenkinsfile pipelines in Jenkins 2, 2016) and GitHub Actions facts (workflow YAML in `.github/workflows/`, hosted runners, push/PR triggers, CI launched 2019) are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
