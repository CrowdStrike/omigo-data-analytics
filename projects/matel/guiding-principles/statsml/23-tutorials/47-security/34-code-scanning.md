# Code Scanning

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Code Scanning

**Subtitle:** SAST reads your source code hunting dangerous data flows; DAST attacks the running app like a probing attacker — the same bug, found two very different ways

## Two Robots Hunt the Same Injection Bug

**Tags:** `core idea` (blue), `SAST vs DAST` (green), `defensive` (orange)

- **The app** — a small shop's web store has an order-lookup page that takes an order id from the URL
- **The flaw** — the code pastes that id straight into a SQL query string, so user input becomes SQL
- **SAST** — a static scanner reads the source without running it, tracing input from source to sink
- **DAST** — a dynamic scanner attacks the running app from outside, firing payloads like `' OR 1=1--`
- **IAST** — instrumented hybrids watch from inside the app while tests run; a middle ground (mention)

*Example (italic):* SAST flags the flaw by reading one file; DAST proves it by getting every order in the database back from one crafted URL.

**Key point:** SAST finds bugs by reading code — tracing data from untrusted sources to dangerous sinks; DAST finds them by behaving like an attacker against the live app. Same bug, two detectors.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the SAST path (read source, trace taint, flag the sink) above the DAST path (send payload at running app, read the response, confirm).

- **Title (bold 15px, `#1a5276`, top center):** "Same SQL Injection, Two Detectors: Read the Code vs Attack the App".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "SAST (static)"; blue `#2a78d6` rounded box at x=130 labeled "source: order id from URL" (12px), 3px arrow to a violet `#4a3aa7` box at x=330 labeled "taint trace through code", 3px arrow to a red `#e74c3c` box at x=530 labeled "sink: SQL string — flag".
- **Row 2 (boxes centered on y=215), label 12px `#444` at x=20:** "DAST (dynamic)"; green `#008300` box at x=130 labeled "payload: ' OR 1=1--", 3px arrow to a blue box at x=360 labeled "running app (staging)", dashed 2px `#6b7280` return arrow beneath it labeled 12px "response: all orders leak", bold 12px green `#008300` verdict "confirmed real" at x≈560.
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px ink `#1a5276`, centered near y=280):** "SAST never runs the app; DAST never reads the code".

## One Repo, Two Scans, Very Different Numbers

**Tags:** `worked example` (blue), `precision vs coverage` (green)

- **The repo** — 40,000 lines and 90 HTTP endpoints; both scanners target the same codebase
- **SAST output** — 120 findings; hand triage confirms 26 real, so 94 are false alarms (~22% precision)
- **Why the noise** — static analysis can't see runtime context, e.g. input already validated upstream
- **DAST output** — 9 findings, 8 confirmed real (~89% precision): what it flags, it usually proved live
- **DAST's blind spot** — it reached only 62 of 90 endpoints; 28 sat behind logins or unfound links

*Example (italic):* SAST saw 100% of the code but was right 22% of the time; DAST was right 89% of the time but saw only 69% of the endpoints.

**Key point:** SAST trades precision for coverage; DAST trades coverage for precision. That asymmetry is why mature teams run both, not one.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: findings vs confirmed-real counts for each scanner, on one shared scale so SAST's noise and DAST's small-but-solid output are visible at a glance.

- **Title (bold 15px, `#1a5276`, top center):** "120 SAST Findings, 26 Real — 9 DAST Findings, 8 Real".
- **Axis:** vertical 2px `#999` baseline at x=190, bars extend right, max width 460 = 120 findings (scale ~3.83 px per finding).
- **Rows (bars 16px tall at y = 70, 112, 178, 220), each with a right-aligned 12px `#444` label ending at x=182:**
  - "SAST findings: 120" — fill `rgba(42,120,214,0.30)`, width 460
  - "SAST confirmed: 26" — solid blue `#2a78d6`, width 100, bold 12px blue label "22% precision" at bar end
  - "DAST findings: 9" — fill `rgba(217,89,38,0.30)` orange, width 35
  - "DAST confirmed: 8" — solid green `#008300`, width 31, bold 12px green label "89% precision" at bar end
- **Value labels:** 11px `#444` count at each bar end (before the precision labels).
- **Annotation (bold 13px magenta `#d55181`, near y=262):** "SAST read all 90 endpoints' code; DAST reached only 62".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Where the Scanners Sit in the Pipeline

**Tags:** `where it's used` (blue), `shift left` (green)

- **SAST on every PR** — fast enough (minutes) to gate a pull request before the bug is even merged
- **DAST against staging** — needs a deployed, running app, so it runs nightly on the staging copy
- **Early beats late** — a flaw caught in the PR costs one review comment; in prod it costs an incident
- **Not a substitute** — scanners match known bug patterns; design flaws need threat modeling and review
- **Stay in bounds** — point DAST only at systems you own or are explicitly authorized to test

*Example (italic):* The injection above never reaches production — SAST blocks the pull request four minutes after push, at 10:14am.

**Key point:** SAST shifts left (every pull request), DAST checks the assembled running system (staging) — both complement code review and threat modeling rather than replace them.

### Visualization (canvas `c3`, 720×300)

Pipeline flow diagram: commit → pull request → merge → staging → production, with a SAST gate callout on the pull request and a DAST callout on staging.

- **Title (bold 15px, `#1a5276`, top center):** "SAST Gates Every Pull Request; DAST Attacks Staging Nightly".
- **Spine (boxes centered on y=160):** five rounded boxes 110px wide, 44px tall at x = 30, 168, 306, 444, 582, labeled 12px `#2c3e50` "commit", "pull request", "merge", "staging", "production"; 3px `#999` arrows between consecutive boxes; fills `rgba(42,120,214,0.15)`, production box fill `rgba(107,114,128,0.12)`.
- **SAST callout:** blue `#2a78d6` rounded box (200×46) at x=120, y=58 labeled 12px "SAST: 4-min scan on every PR, blocks merge on new finding", dashed 2px `#6b7280` connector down to the pull-request box.
- **DAST callout:** green `#008300` rounded box (200×46) at x=400, y=222 labeled 12px "DAST: nightly payloads against the deployed staging app", dashed connector up to the staging box.
- **Stop marker (bold 13px red `#e74c3c`, above the pull-request box near y=120):** "✗ injection stopped here, 10:14am".
- **Caption (12px `#444`, bottom right):** "IAST (instrumented) would sit inside the staging box".

## The Backlog That Teaches Everyone to Ignore Alarms

**Tags:** `common mistake` (red), `triage` (orange)

- **The pile** — a scanner switched on with default rules dumps hundreds of findings on day one
- **The reflex** — when 94 of 120 alerts are false, developers learn to close all 120 without reading
- **The fix: tune** — disable rules that only ever fire falsely on your codebase; noise is configurable
- **The fix: gate on new** — block merges only on findings this change introduces, not the old pile
- **Burn down separately** — schedule the historical backlog as ordinary tech-debt work, not as gates

*Example (italic):* After 12 months of "report everything", the untriaged pile hits 1,380 findings — and the one real injection inside it is as invisible as if no scanner ran.

**Common mistake:** Treating scanner installation as the finish line. An unmanaged findings backlog trains the team to ignore the tool; the real work is tuning rules and gating only on new findings.

### Visualization (canvas `c4`, 720×300)

Line chart over 12 months: open-findings backlog under "report everything, triage nothing" (grows to 1,380) vs "tune rules + gate on new findings" (stays around 31–55).

- **Title (bold 15px, `#1a5276`, top center):** "The Untriaged Pile: 1,380 Findings Nobody Reads vs a Gated 31".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 12 with 12px `#444` tick labels every 2 months; y = open findings 0 to 1500, gridlines `#e5e9ef` at 375/750/1125 with 11px `#6b7280` labels.
- **Untriaged line:** red `#e74c3c` 3px line through months `[0, 2, 4, 6, 8, 10, 12]`, open findings `[40, 210, 430, 660, 890, 1130, 1380]`; bold 12px red label "report everything" near month 9 above the line.
- **Gated line:** green `#008300` 3px line through the same month grid, open findings `[40, 55, 48, 42, 38, 35, 31]`; bold 12px green label "tuned + gated on new" near month 8 above the line.
- **Annotation (bold 13px red `#e74c3c`, near month 5, y=80):** "a pile this big is read by no one".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); finding counts, endpoint counts, precision percentages, and backlog trajectories are invented and labeled illustrative; text numbers (120/26, 9/8, 90/62, 1,380/31) must match the chart numbers exactly.
- **Framing:** defensive/educational only — the page teaches how teams find and fix their own bugs; the DAST section states scanning only systems you own or are authorized to test.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
