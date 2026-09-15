# Terraform

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Terraform

**Subtitle:** Terraform (HashiCorp, 2014) turns infrastructure into text files and a diff — `plan` shows exactly what would change, `apply` makes it so

## The Server You Describe Instead of Click

**Tags:** `core idea` (blue), `infrastructure as code` (green), `HashiCorp` (orange)

- **The task** — a team runs a small web app and needs one more server plus a DNS record for it
- **The old way** — click through the cloud console; six months later nobody remembers what was clicked
- **The Terraform way** — write the server and the DNS record as two blocks in an HCL text file
- **Declarative** — the file states what should exist, not the steps to create it
- **The diff** — `terraform plan` compares the file to a recorded state file and prints what would change

*Example (italic):* The whole change is about 15 lines of HCL — one server block, one DNS record block — checked in and reviewed like any other code.

**Key point:** Terraform is infrastructure as diffs: describe the desired end state in HCL, and `plan` works out the exact set of changes needed to get there.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the core loop: config and state feed into `terraform plan`, which emits a diff panel showing the two additions.

- **Title (bold 15px, `#1a5276`, top center):** "plan = diff( desired config, recorded state )".
- **Input boxes (left):** blue `#2a78d6` rounded box at (x=30, y=80), 190×46, labeled "config (.tf files)" with 12px `#444` sub-line "describes 7 resources"; second blue box at (x=30, y=180), 190×46, labeled "state file" with sub-line "records 5 resources".
- **Plan box (middle):** ink `#1a5276` rounded box at (x=290, y=130), 140×46, bold 13px white text "terraform plan"; 3px `#6b7280` arrows from both input boxes into its left edge.
- **Diff panel (right):** box at (x=490, y=70), 210×170, fill `rgba(42,120,214,0.08)`, 1px `#e5e9ef` border; monospace-style 12px lines: green `#008300` "+ aws_instance.web_2", green "+ dns_record.app", then bold 12px `#2c3e50` "Plan: 2 to add, 0 to change, 0 to destroy"; 3px arrow from the plan box to the panel.
- **Box style:** 8px radius, fills `rgba(42,120,214,0.15)` for blue boxes, 12px `#2c3e50` text unless noted.
- **Annotation (bold 13px green `#008300`, under the diff panel near y=262):** "a diff you read before anything changes".
- **Caption (12px `#444`, bottom left):** "resource counts illustrative".

## Reading the Plan: 2 to Add, 0 to Change

**Tags:** `worked example` (blue), `plan and apply` (green)

- **Before** — the state file records 5 resources: two servers, a load balancer, a database, one DNS record
- **The edit** — the config now describes 7: new server `web_2` plus the record `app.example.com`
- **Plan** — prints `+ aws_instance.web_2`, `+ dns_record.app`, then "2 to add, 0 to change, 0 to destroy"
- **Nothing moved** — after plan the cloud still holds exactly 5 resources; plan is read-only
- **Apply** — `terraform apply` executes the diff; the cloud and the state file both go from 5 to 7

*Example (italic):* Config describes 7, state records 5, so the diff is +2 — both additions are printed before a single change is made to the cloud.

**Key point:** `plan` computes the diff and shows it; `apply` executes it. Nothing in the real world changes until someone approves the diff.

### Visualization (canvas `c2`, 720×300)

Step chart of live resource count across the four moments of the change: edit, plan, apply, next plan — showing plan touches nothing.

- **Title (bold 15px, `#1a5276`, top center):** "Plan Changes Nothing: Live Resources Stay at 5 Until Apply".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = four evenly spaced steps with 12px `#444` labels "edit config", "terraform plan", "terraform apply", "next plan"; y = resources 0 to 8, gridlines `#e5e9ef` at 2/4/6.
- **Live-resources line:** blue `#2a78d6` 3px step line through step indices `[0, 1, 2, 3]`, counts `[5, 5, 7, 7]` — flat at 5 through plan, jumps to 7 at apply.
- **Config line:** dashed green `#008300` (dash 6/4) 2px line at a constant 7 across all four steps, 12px green label "described in config: 7" above its left end.
- **Gap marker:** vertical violet `#4a3aa7` double-headed arrow between the two lines above "terraform plan", bold 12px violet label "the gap is the diff: +2".
- **Annotation (bold 13px blue `#2a78d6`, above the apply step, y=70):** "apply closes the gap: 5 → 7".
- **Caption (12px `#444`, bottom right):** "resource counts illustrative".

## Why the State File Is the Boss

**Tags:** `where it's used` (blue), `state file` (green), `teams` (orange)

- **Source of record** — the state file stores the real IDs Terraform created; every future diff depends on it
- **Every cloud** — providers translate HCL into API calls: AWS, Google Cloud, Azure, Cloudflare, and more
- **Teams** — shared remote state plus state locking keeps two applies from colliding mid-change
- **Modules** — a reusable "web server" module gets stamped out 3 times instead of copy-pasted
- **Review** — the plan output goes into the pull request, so infrastructure changes get code review

*Example (italic):* Two engineers run apply a minute apart; the lock lets the first finish writing state, and holds the second until it can plan against the fresh copy.

**Key point:** The state file is the source of record — keep it remote and locked, and the diff stays trustworthy for everyone on the team.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: two engineers apply a minute apart; the state lock serializes them so the second plans against fresh state.

- **Title (bold 15px, `#1a5276`, top center):** "State Locking: Two Applies, One Lock".
- **Row 1 (boxes centered on y=110), label 12px `#444` at x=20:** "engineer A — 3:00pm"; blue `#2a78d6` rounded box at x=150 labeled "apply starts", 3px arrow to a green `#008300` box at x=320 labeled "acquires lock", arrow to a green box at x=500 labeled "writes state 5 → 7, releases" with bold 12px green "✓" at its right.
- **Row 2 (boxes centered on y=210), label:** "engineer B — 3:01pm"; blue box at x=150 labeled "apply starts", 3px arrow to an orange `#d95926` box at x=320 labeled "lock held — waits", dashed 2px `#6b7280` arrow to a green box at x=500 labeled "plans on fresh state".
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(217,89,38,0.14)`, 12px `#2c3e50` text.
- **Lock marker:** small 12px `#6b7280` padlock glyph "lock" label between the rows at x=320, y=160.
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "without the lock, B would overwrite A's state file".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative".

## The Console Edit Terraform Will Undo

**Tags:** `common mistake` (red), `drift` (orange)

- **The shortcut** — Friday 5pm, a teammate resizes `web_2` from 2 GB to 8 GB in the cloud console
- **Drift** — the cloud now disagrees with both the HCL config and the recorded state file
- **Detection** — Monday's plan refreshes state, sees 8 GB against the configured 2 GB: "1 to change"
- **The trap** — an unrelated Monday apply quietly shrinks the server back to 2 GB
- **The fix** — change the HCL and apply it, or accept the new value into config; never edit around Terraform

*Example (italic):* The Friday console fix survives the weekend, then Monday's routine apply reverts it to 2 GB — the original problem returns with no console click to blame.

**Common mistake:** Hand-editing infrastructure that Terraform manages. The config is the source of truth; anything changed behind its back is drift, and the next apply will revert it.

### Visualization (canvas `c4`, 720×300)

Timeline chart of the server's memory across five days: configured value flat at 2 GB, actual value jumping to 8 GB at the console edit and reverting at Monday's apply.

- **Title (bold 15px, `#1a5276`, top center):** "Console Drift: Monday's Apply Reverts the Friday Hand-Edit".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days "Fri" to "Tue" with 12px `#444` tick labels at Fri/Sat/Sun/Mon/Tue (day indices 0–4); y = memory GB 0 to 10, gridlines `#e5e9ef` at 2/4/6/8.
- **Configured line:** dashed blue `#2a78d6` (dash 6/4) 2px line, constant 2 GB across days `[0, 4]`, 12px blue label "configured (HCL): 2 GB" above its right end.
- **Actual line:** orange `#d95926` 3px step line through day points `[0, 0.7, 0.7, 3.1, 3.1, 4]`, GB `[2, 2, 8, 8, 2, 2]` — vertical jump to 8 at Friday 5pm, vertical drop back to 2 at Monday's apply.
- **Edit marker:** vertical dashed `#6b7280` (dash 4/3) line at day 0.7, 12px `#6b7280` label "console edit: 2 → 8 GB" at its top.
- **Apply marker:** vertical dashed `#6b7280` line at day 3.1, bold 12px red `#e74c3c` label "plan: 1 to change — apply reverts" at its top.
- **Annotation (bold 13px red `#e74c3c`, near day 3.6, y=180):** "drift undone: back to 2 GB".
- **Caption (12px `#444`, bottom right):** "sizes and timing illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); resource counts (5 → 7), timestamps, and memory sizes (2 GB / 8 GB) are invented and labeled illustrative; the plan/apply loop, HCL, providers, state file, state locking, modules, and the summary-line format "N to add, N to change, N to destroy" match Terraform's public documentation (exact behavior, HashiCorp, 2014).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
