# Out of Memory & the OOM Killer

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Out of Memory & the OOM Killer

**Subtitle:** Linux promises programs more memory than it has — when the bill comes due, the kernel picks one process and kills it, and it is rarely the guilty one

## The Database That Died at 3am

**Tags:** `core idea` (blue), `overcommit` (orange), `SIGKILL` (red)

- **The host** — a 16 GB server with no swap runs a 9.2 GB database and a small log-shipper sidecar
- **The promise** — Linux overcommits: `malloc` hands out address space freely and almost never fails
- **The bill** — memory is only really claimed when a page is first written, long after the malloc
- **The leak** — the log-shipper leaks, growing from 0.5 GB at 22:00 to 4.2 GB by 03:12
- **The death** — with the last free page gone, the kernel SIGKILLs a process — the 9.2 GB database

*Example (italic):* At 03:12 total demand hits the full 16 GB; the database, which never grew a byte all night, is killed without warning while the leaky shipper keeps running.

**Key point:** Because of overcommit, a full machine does not show up as a failed `malloc` — it shows up as the OOM killer terminating some process with an unblockable SIGKILL.

### Visualization (canvas `c1`, 720×300)

Stacked-feel timeline of host memory from 22:00 to 03:12: database flat, log-shipper leaking upward, total climbing into the 16 GB ceiling where the kill fires.

- **Title (bold 15px, `#1a5276`, top center):** "One Process Leaks All Night — a Different One Dies at 3am".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = clock time "22:00" to "03:12" with 12px `#444` tick labels at 22:00, 00:00, 02:00, 03:12; y = GB 0 to 16, gridlines `#e5e9ef` at 4/8/12, 2px `#999` ceiling line at 16 with 12px `#444` label "16 GB physical, no swap".
- **Hour grid (hours since 22:00):** `[0, 1, 2, 3, 4, 5, 5.2]`.
- **Database line:** blue `#2a78d6` 3px flat line, GB `[9.2, 9.2, 9.2, 9.2, 9.2, 9.2, 9.2]`, 12px blue label "database 9.2 GB (flat)" above it near hour 1.
- **Total line:** orange `#d95926` 3px line, GB `[12.3, 13.0, 13.7, 14.4, 15.1, 15.8, 16.0]` (database + shipper + 2.6 GB other/kernel), 12px orange label "total demand" near hour 3.5.
- **Shipper line:** magenta `#d55181` 3px line, GB `[0.5, 1.2, 1.9, 2.6, 3.3, 4.0, 4.2]`, 12px magenta label "log-shipper leaking" near hour 2.5.
- **Kill marker:** vertical dashed `#e74c3c` (dash 4/3) line at hour 5.2 where total meets 16; bold 13px red `#e74c3c` annotation at its top: "03:12 — OOM killer fires, database dies".
- **Caption (12px `#444`, bottom right):** "sizes illustrative; other + kernel ≈ 2.6 GB constant".

## How the Kernel Picks Its Victim

**Tags:** `worked example` (blue), `oom_score` (green)

- **The rule** — each process gets an `oom_score` roughly proportional to its share of total memory
- **The scale** — score ≈ resident memory ÷ machine memory × 1000, so 9.2 of 16 GB scores about 575
- **Hand-check** — shipper 4.2 GB → 262; page-cache helper 1.1 GB → 69; sshd 0.05 GB → 3
- **The pick** — highest score dies: 575 beats 262, so the innocent database is the "best" victim
- **The dial** — `oom_score_adj` (−1000 to +1000) is added: database at −500 scores 75, shipper now wins

*Example (italic):* Scores at 03:12 are database 575, shipper 262, cache helper 69, sshd 3 — the kernel kills the 575; with `oom_score_adj=-500` on the database, the 262 shipper dies instead.

**Key point:** The OOM killer optimizes for freeing the most memory with one kill, not for finding the leak — biggest wins unless you tilt the scores with `oom_score_adj`.

### Visualization (canvas `c2`, 720×300)

Grouped horizontal bar chart: `oom_score` for the four processes with default adj, and the same four after setting the database's `oom_score_adj` to −500.

- **Title (bold 15px, `#1a5276`, top center):** "oom_score Decides: Default Pick vs After oom_score_adj = −500".
- **Layout:** two panels side by side; left panel bars start at x=150 (max width 200), right panel bars start at x=470 (max width 200); panel headers bold 13px `#1a5276` at y=55: "default" (x=200) and "database adj −500" (x=520); scale 1000 score = 200px.
- **Rows (top to bottom at y = 90, 135, 180, 225), left-aligned 12px `#444` labels at x=20:** "database 9.2 GB", "log-shipper 4.2 GB", "cache helper 1.1 GB", "sshd 0.05 GB".
- **Left panel scores (widths px):** database 575 (115px) red `#e74c3c` solid with bold 11px red "575 — killed"; shipper 262 (52px) fill `rgba(42,120,214,0.30)` label "262"; cache 69 (14px) label "69"; sshd 3 (2px) label "3"; labels 11px `#444` at bar ends.
- **Right panel scores (widths px):** database 75 (15px) fill `rgba(42,120,214,0.30)` label "575 − 500 = 75"; shipper 262 (52px) red `#e74c3c` solid with bold 11px red "262 — killed"; cache 69 (14px); sshd 3 (2px).
- **Bar style:** 16px tall, 2px `#999` axis line at each panel's bar start.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "same leak, different victim — the adj dial moves the target".
- **Caption (12px `#444`, bottom right):** "score ≈ RSS/16 GB × 1000, illustrative".

## Container Limits Change Who Dies

**Tags:** `where it's used` (blue), `cgroups` (green), `containers` (orange)

- **Two walls** — a container's cgroup memory limit is a private wall; host RAM is the shared wall
- **Cgroup OOM** — a container that hits its own limit gets an OOM kill chosen from inside it only
- **The fix** — cap the shipper's container at 1 GB: its leak dies at 1 GB, the database never notices
- **No cap** — an uncapped shipper leaks into shared RAM until the host-level OOM killer picks globally
- **The habit** — memory limits on every sidecar turn "host roulette" into a contained, named failure

*Example (italic):* With `memory.max = 1 GB` on the shipper's cgroup, the leak is killed at 23:30 inside its own container — the 9.2 GB database runs untouched through the night.

**Key point:** A cgroup limit converts a machine-wide lottery into a local, predictable kill: the process that exceeds its own budget is the one that dies.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: uncapped shipper dragging down the host (database dies) vs a 1 GB cgroup cap containing the blast (shipper dies inside its box).

- **Title (bold 15px, `#1a5276`, top center):** "Host OOM vs cgroup OOM: Where the Wall Is Decides Who Dies".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no limit"; magenta `#d55181` rounded box at x=150 labeled "shipper leaks to 4.2 GB" (12px), 3px arrow to an orange `#d95926` box at x=360 labeled "host RAM exhausted", 3px arrow to a red `#e74c3c` box at x=555 labeled "host OOM kills database" with bold 12px red "✗ wrong process".
- **Row 2 (y=205), label:** "memory.max = 1 GB"; magenta box at x=150 "shipper leaks to 1 GB cap", 3px arrow to a green `#008300` box at x=360 labeled "cgroup OOM kills shipper", then arrow to a green box at x=555 labeled "database unaffected" with bold 12px green "✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(213,81,129,0.12)` / `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, centered near y=270):** "the limit doesn't stop the leak — it names who pays for it".
- **Caption (12px `#444`, bottom right):** "flow schematic, sizes illustrative".

## The Log Names the Victim, Not the Culprit

**Tags:** `common mistake` (red), `kernel log` (orange)

- **The line** — `dmesg` shows: "Out of memory: Killed process 4172 (database) anon-rss:9646080kB"
- **The trap** — reading that line as a verdict: "the database used 9.2 GB, so the database is the bug"
- **The table** — just above the kill line, the kernel dumps every process's RSS at the moment of death
- **The tell** — in that table the shipper sits at 4.2 GB — a sidecar that should never pass 0.5 GB
- **The habit** — after a 3am kill, diff the table against normal sizes; the outlier growth is the culprit

*Example (italic):* The 03:12 log kills PID 4172 (database, 9,646,080 kB RSS), but the table above shows the shipper at 4,404,000 kB — eight times its healthy size, and the real leak.

**Common mistake:** Restarting or "right-sizing" the killed process and closing the ticket. The kill line names the biggest payer, not the leaker — the process table above it holds the actual evidence.

### Visualization (canvas `c4`, 720×300)

Annotated mock of the kernel log after the kill: monospace log lines with two callouts — the kill line (the victim) and the process-table row that exposes the culprit.

- **Title (bold 15px, `#1a5276`, top center):** "Reading the 3am dmesg: Victim on the Last Line, Culprit in the Table".
- **Log block:** rounded rect x=40, y=55, width 640, height 165, fill `#f8f9fa`, 1px `#e0e0e0` border; lines in 12px monospace `#2c3e50`, left padding 16px, line height 22px starting y=80:
  - "[Tue 03:12:07] kernel: log-shipper invoked oom-killer: gfp_mask=0x140cca, order=0"
  - "[Tue 03:12:07] kernel: Tasks state (memory values in pages):"
  - "[Tue 03:12:07] kernel:  pid 3981 (log-shipper)  rss 1101000   oom_score_adj 0"
  - "[Tue 03:12:07] kernel:  pid 4172 (database)     rss 2411520   oom_score_adj 0"
  - "[Tue 03:12:07] kernel:  pid 2214 (cache-helper) rss  288000   oom_score_adj 0"
  - "[Tue 03:12:07] kernel: Out of memory: Killed process 4172 (database)"
  - "[Tue 03:12:07] kernel:  anon-rss:9646080kB, total-vm:12582912kB"
- **Culprit highlight:** the log-shipper table row gets a `rgba(217,89,38,0.15)` background band; bold 12px orange `#d95926` callout at (x=520, y=110) with a 2px orange arrow to that row: "4.2 GB in a sidecar — the leak".
- **Victim highlight:** the "Killed process" line gets a `rgba(231,76,60,0.12)` background band; bold 12px red `#e74c3c` callout at (x=520, y=215) with a 2px red arrow to it: "the victim, not the bug".
- **Annotation (bold 13px `#1a5276`, centered near y=270):** "rss is in 4 kB pages: 1,101,000 pages ≈ 4.2 GB".
- **Caption (12px `#444`, bottom right):** "log text illustrative, format abridged".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); process sizes, timestamps, and log text are invented and labeled illustrative; the score formula (score ≈ RSS/total × 1000, shifted by `oom_score_adj` in the range −1000..1000) and the 4 kB page-to-GB conversion are real kernel behavior, so text scores (575/262/69/3) must match the bar widths at 200px = 1000.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
