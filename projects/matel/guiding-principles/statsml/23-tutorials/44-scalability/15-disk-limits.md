# Disk Limits

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Disk Limits

**Subtitle:** A disk can stop you three different ways — full of bytes, out of I/O operations, or out of inodes — and "disk is fine" needs three different checks

## The Database With Plenty of Disk

**Tags:** `core idea` (blue), `three limits` (green), `storage` (orange)

- **The server** — an orders database sits on a 500 GB cloud volume; the dashboard shows 220 GB used (44%)
- **The stall** — at 9am queries that took 5 ms start taking 400 ms; on-call checks disk: "44% used, disk is fine"
- **Limit 1: bytes** — capacity fills with logs, temp files, old snapshots; at 100% databases crash or go read-only
- **Limit 2: operations** — every volume has an IOPS ceiling; hit it and reads queue even with space free
- **Limit 3: inodes** — every file costs one inode; millions of tiny files exhaust them while bytes sit free

*Example (italic):* The same volume that morning: bytes 44% used, IOPS demand 160% of its cap, inodes 97% used — one green light and two red ones.

**Key point:** "Disk" is three separate budgets — bytes, operations per second, and inodes — and any one of them alone can take the system down.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: the three disk budgets on the same volume at 9am, each as a percent-of-limit bar, showing one healthy and two exhausted.

- **Title (bold 15px, `#1a5276`, top center):** "One Volume, Three Budgets: Only One of Them Is Fine".
- **Axis:** vertical 2px `#999` baseline at x=200, bars extend right, 100% of limit = width 400; dashed `#6b7280` (dash 4/3) vertical line at x=600 labeled "limit" (12px `#6b7280`, top).
- **Rows (bar tops at y = 75, 140, 205), each with a left-aligned 12px `#444` label at x=20, bars 26px tall, 12px value labels at bar ends:**
  - "bytes — 220 / 500 GB": green `#008300` bar width 176 (44%), label "44% — fine"
  - "IOPS — 4,800 / 3,000": red `#e74c3c` bar drawn width 500 (160% would be 640), clipped at the plot edge with two white break marks near the right end, bold label "160% — saturated (bar clipped)"
  - "inodes — 31.8M / 32.7M": orange `#d95926` bar width 388 (97%), label "97% — nearly out"
- **Annotation (bold 13px red `#e74c3c`, near x=230, y=250):** "the dashboard only watched the green one".
- **Caption (12px `#444`, bottom right):** "numbers illustrative".

## Three Commands, Three Answers

**Tags:** `worked example` (blue), `df and iostat` (green)

- **Check bytes** — `df -h` on the volume: 500 GB size, 220 GB used, 44% — capacity is genuinely fine
- **Check operations** — `iostat -x` shows %util 100 and average wait 48 ms per read, up from 1.2 ms
- **The math** — the 9am report queries demand 4,800 random reads/s; the volume is provisioned for 3,000 IOPS
- **The queue** — 1,800 reads/s over budget pile into a queue, so every query waits behind it
- **Check inodes** — `df -i`: 31.8M of 32.7M inodes used (97%) — a third, separate problem waiting

*Example (italic):* Same minute, three commands: `df -h` says 44%, `iostat -x` says 100% busy with 48 ms waits, `df -i` says 97% of inodes gone.

**Key point:** Diagnosing "is the disk okay" takes all three checks — `df -h` for bytes, `iostat -x` for IOPS and wait time, `df -i` for inodes — because each can be red while the others are green.

### Visualization (canvas `c2`, 720×300)

Timeline of the 9am stall: IOPS demand line crossing the provisioned-IOPS cap, with read latency exploding the moment demand exceeds the cap.

- **Title (bold 15px, `#1a5276`, top center):** "9am: Demand Crosses the 3,000-IOPS Cap and Latency Explodes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "8:30" to "9:30" with 12px `#444` tick labels every 15 min; left y = IOPS 0 to 6,000, gridlines `#e5e9ef` at 1,500/3,000/4,500.
- **Cap line:** horizontal dashed `#6b7280` (dash 4/3) line at IOPS 3,000, 12px `#6b7280` label "provisioned 3,000 IOPS" above its left end.
- **Demand line:** blue `#2a78d6` 3px line through minutes-past-8:30 `[0, 10, 20, 30, 35, 40, 50, 60]`, IOPS `[900, 1100, 1600, 2800, 3600, 4800, 4800, 4700]`.
- **Latency line:** red `#e74c3c` 3px line, same x grid, read latency in ms `[1.2, 1.2, 1.5, 2.5, 18, 48, 47, 45]` drawn against a right-hand scale 0–60 ms (12px `#e74c3c` tick labels at 0/20/40/60 on the right edge).
- **Marker:** vertical dashed `#d95926` line at minute 33 where demand crosses the cap, bold 12px `#d95926` label "cap crossed" at its top.
- **Annotation (bold 13px red `#e74c3c`, near minute 45, y=80):** "48 ms waits with 280 GB free".
- **Caption (12px `#444`, bottom right):** "IOPS and latencies illustrative".

## How Many Operations a Disk Actually Gives

**Tags:** `where it's used` (blue), `rule of thumb` (green), `hardware` (orange)

- **Spinning disk** — a mechanical arm must seek per random read: roughly 100–200 random IOPS, full stop
- **SATA SSD** — no moving parts: tens of thousands of random IOPS from one device
- **NVMe SSD** — hundreds of thousands of IOPS; the bottleneck moves to CPU and software
- **Cloud volume** — IOPS are provisioned and billed; a general-purpose volume might cap at 3,000
- **The symptom** — IOPS-bound looks like high I/O wait and 100% disk util while `df -h` shows plenty free

*Example (italic):* The orders workload needs 4,800 random reads/s — impossible on a 150-IOPS spinning disk, easy on an SSD, and 160% of a 3,000-IOPS cloud volume.

**Key point:** Bytes and operations are priced and limited separately — a volume can hold your data ten times over and still be far too slow to serve it.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of random-read IOPS by device type on a schematic log-feel scale, with the workload's 4,800-IOPS need marked.

- **Title (bold 15px, `#1a5276`, top center):** "Random IOPS by Device — the Workload Needs 4,800".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (bar tops at y = 65, 115, 165, 215), each with a left-aligned 12px `#444` label at x=20, bars 24px tall, 12px value labels at bar ends:**
  - "spinning disk": red `#e74c3c` bar width 40, label "~150"
  - "cloud gp volume": orange `#d95926` bar width 150, label "3,000 (provisioned)"
  - "SATA SSD": green `#008300` bar width 300, label "~50,000"
  - "NVMe SSD": blue `#2a78d6` bar width 440, label "~500,000"
- **Need marker:** vertical dashed `#4a3aa7` (dash 4/3) line at x=395 spanning the plot, bold 12px violet `#4a3aa7` label "workload: 4,800" at its top.
- **Annotation (bold 13px orange `#d95926`, near x=250, y=260):** "the cloud volume falls short — pay for IOPS or change the disk".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic; IOPS figures typical, illustrative".

## df Says 44%, the Kernel Says Full

**Tags:** `common mistake` (red), `inodes` (orange)

- **The mistake** — trusting `df -h` alone: bytes are only one of the three budgets
- **The culprit** — a cache job writes one 2 KB session file per request; 31 million files use only 62 GB
- **The cost** — every file, however tiny, consumes one inode; the volume was built with 32.7M of them
- **The night it hits** — inodes reach 32.7M used; writes fail with "No space left on device" at 44% bytes used
- **The reveal** — `df -i` shows IUse% 100; the fix is deleting or packing the tiny files, not adding gigabytes

*Example (italic):* The volume dies with 280 GB free: 31 million 2 KB session files (62 GB of data) consumed all 32.7M inodes.

**Common mistake:** Reading "No space left on device" as a byte problem. When `df -h` shows free space, run `df -i` — inode exhaustion produces the exact same error, and adding storage without adding inodes fixes nothing.

### Visualization (canvas `c4`, 720×300)

Side-by-side pair of vertical gauge bars for the same volume at the moment of failure: bytes used vs inodes used, showing the contradiction between the two checks.

- **Title (bold 15px, `#1a5276`, top center):** "Same Volume, Same Moment: df -h vs df -i".
- **Layout:** two bar groups centered at x=220 and x=500; each an outlined `#999` container 120px wide from y=70 down to y=250 representing 100%, gridlines `#e5e9ef` at 25/50/75% with 11px `#6b7280` percent labels on the left of each container.
- **Left gauge ("df -h — bytes", bold 13px `#1a5276` label above):** green `#008300` fill `rgba(0,131,0,0.30)` from the bottom to 44% of container height, bold 13px green label "220 / 500 GB — 44%" centered inside.
- **Right gauge ("df -i — inodes", bold 13px `#1a5276` label above):** red `#e74c3c` fill `rgba(231,76,60,0.30)` to 100% of container height, bold 13px red label "32.7M / 32.7M — 100%" centered inside.
- **Failure tag:** bold 12px white on red `#e74c3c` rounded box (8px radius) attached to the top of the right gauge: "write() → No space left on device".
- **Annotation (bold 13px violet `#4a3aa7`, centered between the gauges near y=270):** "280 GB free, zero files creatable".
- **Caption (12px `#444`, bottom right):** "file counts illustrative; error string exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and widths above (no randomness); volume size, IOPS demand, latency, and file counts are invented and labeled illustrative; the device-class IOPS ranges (spinning ~100–200, SATA SSD tens of thousands, NVMe hundreds of thousands) are typical published magnitudes, and the "No space left on device" error string is exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
