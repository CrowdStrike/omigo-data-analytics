# The JavaScript Event Loop

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The JavaScript Event Loop

**Subtitle:** JavaScript runs on one thread and never stands around waiting — slow work runs in the background, and finished results line up in queues until the thread is free

## One Barista, No Waiting Around

**Tags:** `core idea` (blue), `one thread` (green), `background work` (orange)

- **The barista** — one barista is the only worker: one thread, one thing happening at a time
- **The machine** — the espresso machine brews on its own, like a setTimeout or fetch in the background
- **No waiting** — she starts a 25-second brew, then rings up the next customers instead of watching it
- **The ding** — when the machine dings at 35s, "serve drink 1" joins the back of her to-do queue
- **The loop** — finish the current job, take the next item off the queue, repeat: that is the event loop

*Example (italic):* In 40 seconds she rings up orders 1, 2 and 3 while drink 1 brews from 10s to 35s — the 25-second brew never once makes her stand idle.

**Key point:** The event loop is one thread working a queue: slow jobs run elsewhere, and only their finished callbacks come back as new items in the queue.

### Visualization (canvas `c1`, 720×300)

Two-lane Gantt timeline: the barista (the single thread) stays busy taking orders while the espresso machine (background work) brews in parallel; the ding queues a callback.

- **Title (bold 15px, `#1a5276`, top center):** "One Thread, 40 Seconds: the Barista Never Waits on the Machine".
- **Axes:** time axis 0–40s mapped to x=140..680 (plot width 540); baseline 2px `#999` at y=245; tick labels "0s", "10s", "20s", "30s", "40s" (12px `#444`); lane labels 12px `#444` at x=20: "barista (one thread)" at y=115, "espresso machine (background)" at y=195.
- **Barista lane (bars 30px tall, centered y=115):** blue `#2a78d6` fill `rgba(42,120,214,0.30)` bars with 12px `#2c3e50` labels — "take order 1" seconds `[0,10]`, "take order 2" `[10,20]`, "take order 3" `[20,30]`; green `#008300` fill `rgba(0,131,0,0.20)` bar "serve drink 1" `[35,40]`.
- **Machine lane (bar 30px tall, centered y=195):** aqua `#199e70` fill `rgba(25,158,112,0.20)` bar "brew drink 1 (25s)" seconds `[10,35]`.
- **Ding marker:** vertical dashed `#6b7280` (dash 4/3) line at 35s from y=70 to y=245, 12px `#6b7280` label "ding → callback queued" at its top.
- **Annotation (bold 13px green `#008300`, near x=300, y=70):** "3 orders taken while one drink brews — the thread never blocks".
- **Caption (12px `#444`, bottom right):** "seconds illustrative".

## Four Logs, One Surprising Order

**Tags:** `worked example` (blue), `task vs microtask` (green), `ordering` (orange)

- **The code** — four lines: log "A"; setTimeout(log "B", 0); a promise .then(log "C"); log "D"
- **The stack** — the script runs top to bottom first without interruption: "A" prints, then "D"
- **Two queues** — the timer callback B waits in the task queue; the promise callback C in the microtask queue
- **Drain rule** — when the stack empties, ALL microtasks run before the next task: C prints before B
- **Final order** — A, D, C, B — and no line of your own code is ever interrupted mid-run

*Example (italic):* Run the four lines in any browser console and the output is always A, D, C, B — a fixed rule, not a race.

**Key point:** Finish the stack, drain the whole microtask queue, then take one task — that single rule predicts every ordering puzzle in JavaScript.

### Visualization (canvas `c2`, 720×300)

Three-box queue diagram (call stack, microtask queue, task queue) with numbered chips showing where each log lands, and an output strip reading A → D → C → B.

- **Title (bold 15px, `#1a5276`, top center):** "log A · setTimeout(B, 0) · promise.then(C) · log D — Who Prints When?".
- **Boxes:** three rounded rectangles 170px wide, y=80 to y=195, 1px `#e5e9ef` border, at x=55 ("call stack"), x=280 ("microtask queue"), x=505 ("task queue"); headers bold 13px `#1a5276` centered above each box.
- **Chips (rounded 8px, 12px `#2c3e50` text):** in call stack, blue `rgba(42,120,214,0.15)` chips "log A — runs 1st" (y=105) and "log D — runs 2nd" (y=150); in microtask queue, green `rgba(0,131,0,0.12)` chip "then → log C — 3rd" (y=125); in task queue, orange `rgba(217,89,38,0.12)` chip "timer → log B — 4th" (y=125).
- **Output strip (y=250):** label 12px `#444` "console output:" at x=55, then bold 14px chips in order "A" (blue `#2a78d6`), "D" (blue), "C" (green `#008300`), "B" (orange `#d95926`) separated by `#6b7280` arrows.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=225):** "stack first, then ALL microtasks, then one task".
- **Caption (12px `#444`, bottom right):** "same order every run — deterministic".

## Why a Busy Loop Freezes the Whole Page

**Tags:** `where it's used` (blue), `page freeze` (red), `async/await` (green)

- **One thread for everything** — the same thread runs your code, paints the page, and answers clicks
- **The freeze** — a 3-second synchronous loop blocks it all: no paint, no scroll, no click for 3 seconds
- **The math** — at 60 frames per second, 3 blocked seconds means 180 skipped paint frames
- **async/await** — await pauses one function and frees the thread; it is promise queues in nicer clothes
- **Node.js** — servers run the same loop: one thread juggles thousands of requests between I/O waits

*Example (italic):* Five clicks that arrive during the 3-second loop all fire in one burst at the end — to the user the page simply looks broken.

**Key point:** Never block the one thread: keep synchronous work short and hand all waiting — network, timers, disk — to the background.

### Visualization (canvas `c3`, 720×300)

Two-lane timeline comparing when 5 user clicks get handled: under a 3-second blocking loop (all handled in a burst at 3.0s) vs the same work chunked into 50ms slices (each handled within 50ms).

- **Title (bold 15px, `#1a5276`, top center):** "5 Clicks During 3 Seconds of Work: Blocking vs Chunked".
- **Axes:** time axis 0–3.5s mapped to x=150..680 (plot width 530); baseline 2px `#999` at y=245; tick labels "0s"–"3.5s" every 0.5s (12px `#444`); lane labels 12px `#444` at x=20: "one 3s blocking loop" at y=110, "chunked, 50ms slices" at y=190.
- **Blocking lane (y=110):** orange `#d95926` fill `rgba(217,89,38,0.25)` bar seconds `[0, 3.0]` 26px tall labeled "loop runs — page frozen" (12px); click arrivals as `#6b7280` down-arrows at seconds `[0.5, 1.0, 1.5, 2.0, 2.5]`; handled marks as five red `#e74c3c` dots clustered at second 3.0 with bold 12px red label "all 5 fire at 3.0s".
- **Chunked lane (y=190):** aqua `#199e70` fill `rgba(25,158,112,0.20)` slice bars 26px tall at seconds `[0, 0.45]`, `[0.55, 0.95]`, `[1.05, 1.45]`, `[1.55, 1.95]`, `[2.05, 2.45]`, `[2.55, 3.1]`; same click arrows at `[0.5, 1.0, 1.5, 2.0, 2.5]`; handled marks as green `#008300` dots at seconds `[0.55, 1.05, 1.55, 2.05, 2.55]` — each 0.05s after its click.
- **Annotation (bold 13px red `#e74c3c`, near x=430, y=70):** "3 blocked seconds = 180 skipped paint frames".
- **Caption (12px `#444`, bottom right):** "click times illustrative; slice widths exaggerated; 60fps assumed".

## setTimeout(0) Does Not Mean Now

**Tags:** `common mistake` (red), `setTimeout(0)` (orange), `microtask priority` (blue)

- **The trap** — setTimeout(fn, 0) does not run fn now; it queues fn behind everything already pending
- **Behind sync code** — a timer cannot fire while the stack is busy: 40ms of remaining code is 40ms of delay
- **Behind microtasks** — every waiting promise callback also runs first; microtasks always cut the line
- **Hand-check** — 40ms of sync code plus 3 microtasks at 1ms each turns "0ms" into a 43ms wait
- **Starvation** — a microtask that queues another microtask can starve tasks and rendering forever

*Example (italic):* A "0ms" timer queued at the top of a script that still has 40ms of sync code and 3 one-millisecond promise callbacks ahead of it fires 43ms later.

**Common mistake:** Reading setTimeout(0) as "immediately". It means "at the earliest opportunity" — after the current stack finishes and after every waiting microtask has run.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: actual firing time of a setTimeout(fn, 0) under three situations, against the requested 0ms.

- **Title (bold 15px, `#1a5276`, top center):** "When Does a 0ms Timer Actually Fire?".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right; ms scale 0–50 mapped to max width 420; light `#e5e9ef` gridlines at 10/20/30/40ms with 11px `#6b7280` labels on the bottom edge.
- **Rows (bars 18px tall, left-aligned 12px `#444` labels at x=20, 11px value labels at bar ends):**
  - y=90 "queue empty, stack idle": blue `#2a78d6` bar to 1ms — "1ms"
  - y=150 "40ms of sync code ahead": yellow `#c98500` bar to 40ms — "40ms"
  - y=210 "sync code + 3 microtasks (1ms each)": orange `#d95926` bar to 43ms — "43ms"
- **Requested marker:** dashed `#6b7280` (dash 4/3) vertical line at 0ms on the baseline, 12px `#6b7280` label "requested: 0ms" above it.
- **Annotation (bold 13px magenta `#d55181`, right side near y=60):** "0ms means 'as soon as you're free', never 'right now'".
- **Caption (12px `#444`, bottom right):** "milliseconds illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); barista/brew timings (orders at 0–10/10–20/20–30s, brew 10–35s, serve 35–40s), click times (0.5–2.5s), slice bounds, and timer delays (1 / 40 / 43ms) are invented and labeled illustrative; the console output order A, D, C, B is the true spec-defined ordering, and 180 skipped frames is exactly 3s × 60fps.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
