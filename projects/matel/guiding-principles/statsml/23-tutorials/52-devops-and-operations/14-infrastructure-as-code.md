# Infrastructure as Code

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Infrastructure as Code

**Subtitle:** Your data center declared in files — servers, networks, and permissions live in version control, and a tool makes reality match the files

## The Server Nobody Can Rebuild

**Tags:** `core idea` (blue), `snowflake servers` (red), `declarative` (green)

- **The snowflake** — a web server built in 2019 by clicking consoles and running remembered commands
- **The drift** — every emergency tweak since then lives only in that one box; no file records it
- **The exit** — the engineer who set it up leaves, and "how was this configured?" has no answer
- **The shift** — declare the DESIRED infrastructure in files: servers, networks, databases, permissions
- **Declarative** — the files say WHAT should exist; the tool computes the diff and figures out HOW
- **Imperative** — a script says HOW step by step, and you pray it is safe to run a second time

*Example (italic):* After a year, the hand-managed fleet carries 68 undocumented changes; the IaC fleet carries 3, and each was caught by the next plan.

**Key point:** Infrastructure as code replaces remembered commands with version-controlled files describing the desired state — a tool diffs declared against actual and applies the difference.

### Visualization (canvas `c1`, 720×300)

Line chart of configuration drift over one year: undocumented manual changes accumulating on a click-built fleet vs staying near zero on an IaC-managed fleet.

- **Title (bold 15px, `#1a5276`, top center):** "Drift: Changes Nobody Wrote Down, Over 12 Months".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 12 with 12px `#444` tick labels every 2 months; y = undocumented changes 0 to 80, gridlines `#e5e9ef` at 20/40/60.
- **Click-built line:** red `#e74c3c` 3px line through months `[0, 2, 4, 6, 8, 10, 12]`, changes `[0, 9, 19, 31, 42, 55, 68]` — steadily climbing.
- **IaC line:** green `#008300` 3px line through the same months, changes `[0, 1, 1, 2, 2, 3, 3]` — near flat.
- **Departure marker:** vertical dashed `#6b7280` (dash 4/3) line at month 6, 12px `#6b7280` label "the engineer who built it leaves" at its top.
- **Annotation (bold 13px red `#e74c3c`, near month 9, y=95):** "68 changes live only in someone's memory".
- **Annotation (bold 12px green `#008300`, near month 9, y=215):** "drift caught and reverted by the next plan".
- **Caption (12px `#444`, bottom right):** "change counts illustrative".

## Plan First: "Will Create 2, Modify 1, Destroy 0"

**Tags:** `worked example` (blue), `plan/apply` (green)

- **The repo** — the files declare 2 web servers and 1 database sized at 100 GB
- **The edit** — one commit changes the count to 4 web servers and the database size to 200 GB
- **The plan** — the tool diffs declared vs actual and prints: 2 to add, 1 to change, 0 to destroy
- **Hand-check** — 4 declared minus 2 running = 2 creates; size 100→200 = 1 modify; nothing removed = 0 destroys
- **The review** — the plan output rides the pull request: infrastructure change gets code review and CI
- **The apply** — only after approval does the tool touch anything; rollback = revert the commit, re-plan

*Example (italic):* The reviewer rejects the first draft because the plan shows a surprise "1 to destroy" — a typo would have deleted the database; the typo is fixed before anything runs.

**Key point:** Plan shows the exact create/modify/destroy diff BEFORE touching anything, so infrastructure changes get review, CI, and rollback like any other code.

### Visualization (canvas `c2`, 720×300)

Pipeline flow diagram of the plan/apply workflow: four boxes across the top, with the plan's diff output shown in a terminal-style panel below.

- **Title (bold 15px, `#1a5276`, top center):** "Change a File, See the Diff, Then Apply".
- **Pipeline row (boxes at y=70, each 140px wide, 44px tall, 8px radius, 12px `#2c3e50` text, 3px `#6b7280` arrows between):** blue `rgba(42,120,214,0.15)` box at x=30 "edit files: 4 web, 200 GB db"; blue box at x=205 "plan (dry run)"; orange `rgba(230,126,34,0.15)` box at x=380 "review + CI on the diff"; green `rgba(0,131,0,0.12)` box at x=555 "apply".
- **Plan output panel:** rectangle x=110, y=150, width 500, height 118, fill `#f8f9fa`, 1px `#e0e0e0` border, 4px radius; 12px monospace-style lines left-aligned at x=130, starting y=172, 22px line spacing: green `#008300` "+ create web-3 (server)"; green "+ create web-4 (server)"; orange `#d95926` "~ modify db-main: size 100 GB → 200 GB"; bold `#2c3e50` "Plan: 2 to add, 1 to change, 0 to destroy".
- **Annotation (bold 13px green `#008300`, right of the panel near x=620, y=200, wrapped two lines):** "nothing is touched until apply".
- **Caption (12px `#444`, bottom right):** "plan output schematic".

## Rebuild the Data Center from a Git Repo

**Tags:** `where it's used` (blue), `disaster recovery` (green), `immutable` (orange)

- **Disaster recovery** — the region burns down; recovery is re-applying the repo in a new region
- **Environment parity** — staging and prod are spun from the same modules, so staging really resembles prod
- **Audit history** — "who changed the firewall and why" is answered by `git log`, not by asking around
- **Immutable servers** — never patch a running box: build a new image, replace the old one (containers' habit)
- **Boring on purpose** — the tenth environment is as unexciting as the second, because it is the same code

*Example (italic):* In the disaster drill, the hand-built stack takes 21 days to reconstruct from memory and stale docs; the IaC stack is re-applied in 90 minutes.

**Key point:** The repo, not the running hardware, becomes the source of truth — so recovery is a re-run, environments match by construction, and the audit trail is the commit history.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: time to rebuild the full stack after a disaster, for three ways of remembering how it was built.

- **Title (bold 15px, `#1a5276`, top center):** "Disaster Drill: Time to Rebuild the Stack".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 430; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 85, 150, 215), each with a left-aligned 12px `#444` label at x=20, bars 18px tall, 12px value labels at bar ends:**
  - "hand-built, from memory — 21 days": red `#e74c3c` fill `rgba(231,76,60,0.35)` bar width 430
  - "stale runbook docs — 6 days": orange `#d95926` fill `rgba(230,126,34,0.30)` bar width 210
  - "IaC: re-apply the repo — 90 min": green `#008300` fill `rgba(0,131,0,0.30)` bar width 26
- **Annotation (bold 13px green `#008300`, near x=320, y=250):** "recovery is a re-run, not an archaeology dig".
- **Caption (12px `#444`, bottom right):** "durations illustrative; pixel widths schematic".

## Clicking Around the Tool's Back

**Tags:** `common mistake` (red), `state file` (orange)

- **The state file** — the tool's memory of what it manages; plans are diffs of files vs state vs reality
- **The quiet click** — an engineer resizes the database in the console; files and state still say 100 GB
- **The surprise** — the next plan proposes shrinking it back to 100 GB, "fixing" the undeclared change
- **State conflicts** — two people applying at once corrupt the shared state; lock it, store it remotely
- **Secrets caveat** — passwords and keys do not belong in the files; reference a secret store instead
- **Tech-debt caveat** — IaC codebases grow copy-pasted modules and dead resources like any other code

*Example (italic):* The console click set the database to 500 GB on Friday; Monday's routine apply shrinks it back to the declared 100 GB and pages the on-call.

**Common mistake:** Treating the console as still yours. Once a tool manages a resource, every manual change becomes drift that the next plan will expose — or silently undo.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a manual console change colliding with the state file (surprise shrink) vs the disciplined path of changing the file first.

- **Title (bold 15px, `#1a5276`, top center):** "Manual Console Changes Drift Away from the State File".
- **Row 1 (y=95), label 12px `#444` at x=20:** "console click"; blue `#2a78d6` rounded box at x=140 labeled "files + state: db 100 GB" (12px), 3px arrow to an orange `#d95926` box at x=340 labeled "console: resized to 500 GB", 3px arrow to a red `#e74c3c` box at x=545 labeled "next plan: shrink to 100 GB" with bold 12px red "✗ surprise shrink".
- **Row 2 (y=205), label:** "file first"; blue box at x=140 "edit file: db 500 GB", 3px arrow to a green `#008300` box at x=340 labeled "plan: ~ modify 100 → 500", arrow to a green box at x=545 labeled "apply: state = reality" with bold 12px green "✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(230,126,34,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "every console click is a debt the next plan collects".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); drift counts, plan diff (2 add / 1 change / 0 destroy), database sizes (100 → 200 GB, console 500 GB), and rebuild durations (21 days / 6 days / 90 min) are invented and labeled illustrative or schematic in captions; text numbers must match chart numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
