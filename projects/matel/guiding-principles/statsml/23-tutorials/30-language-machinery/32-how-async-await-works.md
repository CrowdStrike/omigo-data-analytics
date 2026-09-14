# How Async/Await Works

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** How Async/Await Works

**Subtitle:** `await` means "start the slow thing, save my place, and go do other work" — the compiler makes that possible by rewriting your function into a checklist with a bookmark: a state machine

## A Barista Who Never Stands Still

**Tags:** `core idea` (blue), `pausing a function` (green), `state machine` (orange)

- **The latte recipe** — grind the beans, start the espresso machine, wait for the shot, pour and serve
- **The wait** — the machine takes 30 seconds; a bad barista stares at it, a good one serves someone else
- **`await` is the walk-away** — "start the machine, note where I am in the recipe, come back at the beep"
- **The bookmark** — the recipe card records the current step and the details in hand (whose cup, what milk)
- **The rewrite** — the compiler cuts your function at every `await` into numbered steps: a state machine
- **Resuming** — when the machine beeps, the barista reads the card and continues from the saved step

*Example (italic):* The barista starts latte A's espresso shot, writes "order A: at step 2, oat milk" on the card, and takes latte B's order during the 30-second brew.

**Key point:** `await` does not make the wait shorter — it frees the worker during the wait, and the saved step number plus saved locals are exactly what a state machine is.

### Visualization (canvas `c1`, 720×300)

Single-panel diagram: one recipe-card function cut at its `await` into two state boxes, with a suspended zone between them and a bookmark arrow showing where execution pauses and resumes.

- **Title (bold 15px, `#1a5276`, top center):** "One `await` Cuts the Recipe into Two States".
- **State 0 box:** rounded rect x=40, y=70, width 250, height 130, 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.10)`; bold 13px blue header "STATE 0 — before the await" at its top; inside, 12px `#2c3e50` lines: "grind the beans", "start the espresso machine", then bold 12px orange `#d95926` line "save bookmark: step=1, cup=A, milk=oat" and 12px `#6b7280` line "return to the counter".
- **State 1 box:** rounded rect x=430, y=70, width 250, height 130, 2px green `#008300` border, fill `rgba(0,131,0,0.10)`; bold 13px green header "STATE 1 — after the beep" at its top; inside, 12px `#2c3e50` lines: "read bookmark: cup=A, milk=oat", "pour the shot", "serve latte A — done".
- **Suspended zone:** between the boxes, dashed `#6b7280` (dash 4/3) rounded rect x=305, y=95, width 110, height 80; 12px `#6b7280` centered text on two lines: "machine brews" / "30 s — no one waits"; 3px `#6b7280` arrow from state 0's right edge into the zone and a 3px green arrow from the zone into state 1's left edge, each with an arrowhead.
- **Annotation (bold 13px orange `#d95926`, centered near y=250):** "the function is not running while it waits — it is a saved card, resumed at the beep".
- **Caption (12px `#444`, bottom right):** "illustrative — one await, two states; every extra await adds one more state".

## Two Lattes: 90 Seconds Becomes 55

**Tags:** `worked example` (blue), `timeline` (green)

- **One latte** — grind 10 s (hands busy), brew 30 s (machine runs alone), pour and serve 5 s (hands busy)
- **Blocking barista** — stares at each brew: (10 + 30 + 5) × 2 lattes = 90 s for two orders
- **Async barista** — grinds A (0–10 s), starts A's brew, grinds B (10–20 s), starts B's brew
- **The beeps** — A's brew ends at 40 s, pour A 40–45 s; B's brew ends at 50 s, pour B 50–55 s
- **The saving** — 55 s instead of 90 s, and every saved second came out of stare-at-the-machine time
- **Check by hand** — hands-on work is still 2 × 15 = 30 s; only the overlapped waiting got cheaper

*Example (italic):* At the 15-second mark the blocking barista is watching latte A brew; the async barista is already grinding latte B's beans.

**Key point:** Two lattes take 90 s blocking and 55 s with await — the 35 s saved is exactly the waiting that now overlaps with useful work.

### Visualization (canvas `c2`, 720×300)

Two-lane Gantt timeline on a shared seconds axis: the blocking barista's 90-second lane above the async barista's 55-second lane, with each latte's grind/brew/pour segments as colored bars.

- **Title (bold 15px, `#1a5276`, top center):** "Same Two Lattes: Blocking 90 s vs Async 55 s".
- **Axis:** horizontal 2px `#999` line at y=250 from x=140 to x=680 (width 540), seconds 0 to 90 (6 px per second); tick labels "0 s", "15 s", "30 s", "45 s", "60 s", "75 s", "90 s" (12px `#444`) below; light `#e5e9ef` vertical gridlines at each tick.
- **Bar style:** 22px-tall bars; grind = blue `#2a78d6` solid fill, brew = `rgba(107,114,128,0.30)` with 1px dashed `#6b7280` border (waiting), pour = green `#008300` solid fill; 11px white bold labels "grind"/"pour" inside solid bars, 11px `#6b7280` label "brew (wait)" inside brew bars.
- **Lane 1 (blocking, bar center y=105), 12px `#444` label "blocking barista" at x=20:** latte A grind 0–10, brew 10–40, pour 40–45; latte B grind 45–55, brew 55–85, pour 85–90; thin `#2c3e50` end bracket at 90 s with bold 13px `#2c3e50` label "90 s" above.
- **Lane 2 (async, bar center y=185), 12px `#444` label "async barista" at x=20:** grind A 0–10, grind B 10–20 (both blue), brew A 10–40 and brew B 20–50 drawn as gray wait bars in a slim sub-row just below (bar center y=205, 12px tall), pour A 40–45, pour B 50–55 (green); thin green end bracket at 55 s with bold 13px green `#008300` label "55 s" above.
- **Annotation (bold 12px orange `#d95926`, near x=430, y=160):** "brews overlap with work — 35 s of staring reclaimed".
- **Caption (12px `#444`, bottom right):** "illustrative timings — grind 10 s, brew 30 s, pour 5 s per latte".

## One Worker, Many Slow Calls

**Tags:** `where it's used` (blue), `network waits` (green), `throughput` (orange)

- **The daily reality** — most program "work" is waiting: web requests, database queries, file reads
- **Six downloads** — a script fetches 6 files, 2 s each: one-at-a-time `await`s take 6 × 2 = 12 s
- **Start first, await later** — kick off all 6 requests, then await them together: about 2 s total
- **Still one worker** — no extra threads; one worker holds 6 bookmarks and resumes each reply as it lands
- **Why servers care** — a waiter thread per customer runs out fast; bookmarks are nearly free to keep
- **The habit** — put `await` where the wait is real, and start independent waits before awaiting any

*Example (italic):* The same 6 downloads finish in 12 s when each one is awaited before the next starts, and in about 2 s when all six are started first — the network does all six waits at once.

**Key point:** Async/await turns dead waiting time into throughput: start the independent slow calls first, and 12 s of downloads collapses to about 2 s.

### Visualization (canvas `c3`, 720×300)

Two stacked Gantt panels on one shared seconds axis: six sequential 2-second download bars forming a 12-second staircase on top, and the same six bars overlapping in a 2-second block below.

- **Title (bold 15px, `#1a5276`, top center):** "Six 2-second Downloads: One at a Time vs Started Together".
- **Axis:** horizontal 2px `#999` line at y=255 from x=130 to x=670 (width 540), seconds 0 to 12 (45 px per second); tick labels "0 s", "2 s", "4 s", "6 s", "8 s", "10 s", "12 s" (12px `#444`); light `#e5e9ef` vertical gridlines at each tick.
- **Top panel (sequential), 12px `#444` label "await each in turn" at x=20, y=95:** six 14px-tall bars, one per row at y = 60, 76, 92, 108, 124, 140, starts at seconds `[0, 2, 4, 6, 8, 10]`, each 2 s long; fill `rgba(42,120,214,0.35)` with 2px blue `#2a78d6` border; bold 13px blue label "12 s" just right of the last bar's end.
- **Bottom panel (concurrent), 12px `#444` label "start all, then await" at x=20, y=205:** six 8px-tall bars stacked at y = 178, 188, 198, 208, 218, 228, all starting at 0 s, each 2 s long; fill `rgba(0,131,0,0.35)` with 1px green `#008300` border; bold 13px green label "≈ 2 s" just right of their shared end.
- **Divider:** light 1px `#e5e9ef` horizontal line at y=160 across the plot area.
- **Annotation (bold 13px orange `#d95926`, near x=420, y=195):** "one worker, six bookmarks — the network waits in parallel".
- **Caption (12px `#444`, bottom right):** "illustrative — equal 2 s responses; real replies land in any order".

## Await Is Not a Second Pair of Hands

**Tags:** `common mistake` (red), `cpu vs waiting` (orange)

- **The confusion** — people hear "async" and picture threads; `await` adds bookmarks, not workers
- **Grinding still blocks** — the 10 s of grinding needs the barista's hands; no beep can free them
- **A mixed job** — a task with 4 s of number crunching and 20 s of database waiting takes 24 s alone
- **What await reclaims** — the 20 s wait can host other orders; the 4 s crunch hogs the worker either way
- **The symptom** — an "async" server that freezes during a big in-memory sort: CPU work between awaits
- **The fix** — awaits help waits; heavy computation needs a real second worker (a thread or a process)

*Example (italic):* Sorting a 10-million-row table between two awaits stalls every other bookmark for the whole sort — async never promised a second pair of hands.

**Common mistake:** Using `await` to speed up computation. Await reclaims the 20 s of waiting, never the 4 s of crunching — if the CPU is the busy part, async/await alone saves nothing.

### Visualization (canvas `c4`, 720×300)

Single mixed job shown as one horizontal timeline bar split into a crunch segment and a wait segment, with a bracket marking the part `await` can give back to other work and a bracket marking the part it cannot.

- **Title (bold 15px, `#1a5276`, top center):** "One Job = 4 s Crunch + 20 s Wait: Await Reclaims Only the Wait".
- **Axis:** horizontal 2px `#999` line at y=230 from x=110 to x=670 (width 560), seconds 0 to 24; tick labels "0 s", "4 s", "8 s", "12 s", "16 s", "20 s", "24 s" (12px `#444`); light `#e5e9ef` gridlines at each tick.
- **Job bar (28px tall, centered at y=140):** crunch segment 0–4 s, fill orange `#d95926`, bold 12px white centered label "CPU crunch 4 s"; wait segment 4–24 s, fill `rgba(107,114,128,0.25)` with 1px dashed `#6b7280` border, 12px `#6b7280` centered label "database wait 20 s".
- **Reclaimed bracket:** green `#008300` 2px square bracket above the wait segment (from 4 s to 24 s, at y=95), bold 13px green centered label above it: "await frees this — 20 s hosts other orders".
- **Blocked bracket:** red `#e74c3c` 2px square bracket below the crunch segment (from 0 s to 4 s, at y=185), bold 12px red centered label below it on two lines: "worker is hostage here —" / "await cannot help".
- **Annotation (bold 13px violet `#4a3aa7`, near x=420, y=270):** "async reclaims waiting, never computing — 20 s back, 4 s untouchable".
- **Caption (12px `#444`, bottom right):** "illustrative — a 4 s crunch + 20 s wait job".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar positions, segment start/end seconds, and box coordinates are the hardcoded literal values above (no randomness); every invented timing carries an "illustrative" caption; the timings in the text (10/30/5 s latte steps, 90 s vs 55 s, 6 × 2 s = 12 s vs ≈ 2 s, 4 s + 20 s = 24 s) must match the chart values exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
