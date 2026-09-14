# Real-Time Scheduling

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Real-Time Scheduling

**Subtitle:** In a real-time system the deadline is part of the answer — a correct result delivered late counts as a wrong result

## The Airbag That Must Answer in 10 Milliseconds

**Tags:** `core idea` (blue), `deadline as correctness` (green), `hard real-time` (orange)

- **The controller** — a car's crash sensor chip decides whether to fire the airbag after an impact
- **The deadline** — the fire/no-fire decision must be delivered within 10 ms of the impact signal
- **The twist** — a perfect "fire!" computed at 14 ms is as useless as no answer at all
- **The reframe** — correctness = right value AND on-time delivery; the OS scheduler owns the second half
- **The job** — a real-time scheduler orders work so every task provably finishes before its deadline

*Example (italic):* Two identical runs both conclude "fire the airbag" — the one finishing at 6 ms saves the passenger, the one finishing at 14 ms does not.

**Key point:** Real-time does not mean fast — it means the deadline is a correctness condition, so the scheduler must guarantee when an answer arrives, not just what it says.

### Visualization (canvas `c1`, 720×300)

Horizontal timeline chart: two runs of the same airbag decision on a 0–20 ms axis, with the 10 ms deadline as a vertical line; one run finishes before it, one after.

- **Title (bold 15px, `#1a5276`, top center):** "Same Answer, Different Arrival: Only One Counts as Correct".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time 0 to 20 ms (30 px per ms), 12px `#444` tick labels every 5 ms; no y axis, two labeled rows instead.
- **Deadline marker:** vertical dashed `#d55181` (dash 4/3) line at 10 ms from y=60 to y=245, bold 12px `#d55181` label "deadline 10 ms" at its top.
- **Row 1 (bar center y=110), 12px `#444` label at x=20:** "run 1"; green `#008300` fill `rgba(0,131,0,0.30)` bar from 0 to 6 ms, 26px tall, bold 12px green "fires at 6 ms ✓" just right of the bar.
- **Row 2 (bar center y=185), label:** "run 2"; blue `#2a78d6` fill `rgba(42,120,214,0.25)` bar from 0 to 10 ms, then red `#e74c3c` solid segment from 10 to 14 ms, bold 12px red "fires at 14 ms ✗ — too late" just right of the bar.
- **Annotation (bold 13px violet `#4a3aa7`, near x=15 ms, y=70):** "late = wrong, even with the right value".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Three Tasks, One CPU: Checking the Math by Hand

**Tags:** `worked example` (blue), `rate monotonic` (green), `fixed priorities` (orange)

- **The task set** — A: every 10 ms, needs 3 ms; B: every 20 ms, needs 4 ms; C: every 40 ms, needs 8 ms
- **The rule** — rate monotonic scheduling: the shorter the period, the higher the fixed priority (A > B > C)
- **The load** — utilization = 3/10 + 4/20 + 8/40 = 30% + 20% + 20% = 70% of the CPU
- **The bound** — for 3 tasks the rate-monotonic test guarantees deadlines up to ~78% utilization
- **Hand-check** — 70% ≤ 78%, so every A, B, and C instance provably meets its deadline, forever

*Example (italic):* At t=0 all three arrive: A runs 0–3, B runs 3–7, C starts, gets preempted by A at 10, and still finishes at 18 ms — well inside its 40 ms deadline.

**Key point:** Real-time scheduling replaces "it seemed fast when we tried it" with a paper-and-pencil proof: if utilization stays under the bound, no deadline is ever missed.

### Visualization (canvas `c2`, 720×300)

Gantt chart of the first 40 ms under rate monotonic scheduling: three task rows plus arrival ticks, showing preemption of C and idle gaps.

- **Title (bold 15px, `#1a5276`, top center):** "One 40 ms Cycle: A Preempts B Preempts C, Nobody Misses".
- **Axes:** origin x=60, baseline y=245, plot width 600 (15 px per ms), plot height 180; x = time 0 to 40 ms, 12px `#444` tick labels every 10 ms; gridlines `#e5e9ef` at 10/20/30 ms.
- **Rows (bar center y = 90 for A, 150 for B, 210 for C), 12px `#444` labels at x=20:** "A (10ms)", "B (20ms)", "C (40ms)"; bars 26px tall.
- **A bars (blue `#2a78d6`, fill `rgba(42,120,214,0.35)`):** ms intervals `[0,3]`, `[10,13]`, `[20,23]`, `[30,33]`.
- **B bars (green `#008300`, fill `rgba(0,131,0,0.30)`):** intervals `[3,7]`, `[23,27]`.
- **C bars (aqua `#199e70`, fill `rgba(25,158,112,0.30)`):** intervals `[7,10]` and `[13,18]` — split by A's preemption at 10 ms; 11px `#6b7280` label "preempted" between the two pieces.
- **Arrival ticks:** small 2px `#6b7280` down-arrows on each row at that task's arrivals: A at 0/10/20/30, B at 0/20, C at 0.
- **Annotation (bold 13px violet `#4a3aa7`, near x=30 ms, y=60):** "70% load ≤ 78% bound — deadlines proven, not hoped".
- **Caption (12px `#444`, bottom right):** "schedule exact for the stated periods; task set invented".

## When a Late Answer Loses Its Value

**Tags:** `where it's used` (blue), `soft vs hard` (green)

- **Hard** — airbags, pacemakers, anti-lock brakes: a miss is a system failure, value drops to zero
- **Firm** — a trading quote or a discarded video frame: a late result is worthless but not fatal
- **Soft** — a dashboard refresh: a late answer still helps, it just helps less the later it gets
- **Data work** — sensor loggers, streaming pipelines, and edge inference all carry frame deadlines
- **The symptom** — a scheduler hiccup shows up in your data as timestamp gaps and jitter, not errors

*Example (italic):* A video player must deliver a frame every 33 ms — one late frame is dropped (firm), but a heart pacemaker's late pulse is a failure (hard).

**Key point:** Classify the deadline before designing the system: the shape of the value-versus-lateness curve — cliff, step, or slope — decides how much engineering the guarantee is worth.

### Visualization (canvas `c3`, 720×300)

Line chart: value of a result (y) versus its completion time (x) for hard, firm, and soft deadlines sharing one 10 ms deadline.

- **Title (bold 15px, `#1a5276`, top center):** "Three Deadline Types: How Fast Value Dies After 10 ms".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = completion time 0 to 20 ms, 12px `#444` tick labels every 5 ms; y = value of the result 0 to 100%, gridlines `#e5e9ef` at 25/50/75.
- **Deadline marker:** vertical dashed `#6b7280` (dash 4/3) line at 10 ms, 12px `#6b7280` label "deadline" at its top.
- **Hard line (violet `#4a3aa7`, 3px):** points (ms, value) `[0,100]`, `[10,100]`, `[10,0]`, `[20,0]` — full value, then a cliff to zero; 12px violet label "hard" near (4 ms, 100%).
- **Firm line (yellow `#c98500`, 3px, drawn 4px below hard where they overlap):** `[0,96]`, `[10,96]`, `[10,0]`, `[20,0]` — same cliff shape; 12px yellow label "firm (result discarded)" near (12 ms, 40%).
- **Soft line (green `#008300`, 3px):** `[0,100]`, `[10,100]`, `[15,50]`, `[20,0]` — a slope, not a cliff; 12px green label "soft" near (16 ms, 55%).
- **Annotation (bold 13px magenta `#d55181`, near x=13 ms, y=75):** "hard deadlines have no partial credit".
- **Caption (12px `#444`, bottom right):** "value curves schematic, illustrative".

## Fast on Average Is Not Real-Time

**Tags:** `common mistake` (red), `worst case` (orange)

- **The trap** — "our control loop averages 4.6 ms, the deadline is 10 ms, we have tons of margin"
- **The tail** — averages hide the worst case; one 12 ms outlier per thousand runs breaks a hard system
- **The culprits** — garbage-collector pauses, cache misses, and a busy general-purpose OS scheduler
- **The fix** — real-time analysis budgets the worst-case execution time (WCET), never the mean
- **The trade** — real-time OSes give up average throughput to make the worst case predictable

*Example (italic):* In 1,000 loop runs averaging 4.6 ms, runs at 11 and 12 ms blow the 10 ms deadline twice — a 99.8% pass rate that a hard real-time system calls broken.

**Common mistake:** Judging a real-time system by its mean latency. The guarantee lives entirely in the worst case — a distribution with a beautiful average and one bad tail is a failed design.

### Visualization (canvas `c4`, 720×300)

Histogram of 1,000 control-loop completion times in 1 ms bins, with the 10 ms deadline line and the two late runs highlighted in red.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Runs, Average 4.6 ms — and Still 2 Missed Deadlines".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = completion time bins 2 to 12 ms, 12px `#444` bin labels under each bar; y = run count 0 to 320, gridlines `#e5e9ef` at 80/160/240.
- **Bars:** bin centers (ms) `[2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]`, counts `[40, 180, 310, 240, 130, 60, 25, 10, 3, 1, 1]` (sums to 1,000); bars ≤10 ms filled `rgba(42,120,214,0.35)` with 2px `#2a78d6` edge; bars at 11 and 12 ms solid red `#e74c3c` with 11px red count labels "1" above each.
- **Mean marker:** vertical dashed `#008300` (dash 4/3) line at 4.6 ms, 12px green label "mean 4.6 ms" at its top.
- **Deadline marker:** vertical dashed `#d55181` (dash 4/3) line at 10 ms (right edge of the 10 ms bin), bold 12px `#d55181` label "deadline 10 ms" at its top.
- **Annotation (bold 13px red `#e74c3c`, near the 11–12 ms bars, y=120):** "the average passes; the tail crashes the system".
- **Caption (12px `#444`, bottom right):** "run counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); airbag timings, the task set (A 3/10 ms, B 4/20 ms, C 8/40 ms), value curves, and the histogram counts `[40,180,310,240,130,60,25,10,3,1,1]` are invented and labeled illustrative; the 70% utilization, the ~78% three-task rate-monotonic bound (3·(2^(1/3)−1)), the Gantt intervals, and the 4.6 ms mean are exact consequences of those stated numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
