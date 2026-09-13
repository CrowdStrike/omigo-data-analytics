# Stacks & Queues

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Stacks & Queues

**Subtitle:** Two ways to store things in a row and take them back out — a stack returns the newest thing first (last in, first out), a queue returns the oldest thing first (first in, first out)

## Trays and the Lunch Line

**Tags:** `core idea` (blue), `LIFO vs FIFO` (green), `two disciplines` (orange)

- **The tray stack** — clean trays pile up by the counter; whoever grabs one takes the tray washed last
- **The lunch line** — customers join at the back and are served from the front, first come first served
- **LIFO** — the stack's rule: Last In, First Out — the newest item, sitting on top, leaves first
- **FIFO** — the line's rule: First In, First Out — the oldest item, at the front, leaves first
- **Same storage** — both just hold things in order; the only design choice is which end gives them back

*Example (italic):* Tray E was washed last and sits on top, so it is grabbed first; Ana joined the line first, so she is served first.

**Key point:** A stack and a queue store items the same way — they differ only in which end you take from: the newest end (stack) or the oldest end (queue).

### Visualization (canvas `c1`, 720×300)

Two-panel diagram of the same five items A–E held under the two disciplines: a vertical tray stack on the left (in and out at the top) and a horizontal lunch line on the right (in at the back, out at the front).

- **Title (bold 15px, `#1a5276`, top center):** "One Cafeteria, Two Disciplines: the Tray Stack and the Lunch Line".
- **Left panel — stack:** bold 13px blue `#2a78d6` label "tray stack — LIFO" centered at x=180, y=68; five rectangles 120×26 (fill `rgba(42,120,214,0.15)`, 2px blue border) stacked at x=120, tops at y = 216, 188, 160, 132, 104 (bottom to top), 12px `#2c3e50` centered labels "A — first in", "B", "C", "D", "E — last in" (A bottom, E top).
- **Stack arrows:** two short 2px blue arrows above the top tray — down arrow at x=150 labeled "in" (11px blue), up arrow at x=210 labeled "out first" (bold 11px blue) — both acting on the SAME end.
- **Right panel — queue:** bold 13px green `#008300` label "lunch line — FIFO" centered at x=530, y=68; five rectangles 52×36 (fill `rgba(0,131,0,0.12)`, 2px green border) in a row at y=160, left edges x = 400, 458, 516, 574, 632, 12px centered labels "A", "B", "C", "D", "E" (A at the front/left, E at the back/right).
- **Queue arrows:** 2px green arrow entering the right end at x=700→688, y=178, labeled "in (back)" (11px green, above); 2px green arrow leaving the left end at x=396→372, y=178, labeled "out first (front)" (bold 11px green, above).
- **Annotation (bold 12px violet `#4a3aa7`, centered near x=360, y=282):** "same five items — the only difference is which end lets go first".
- **Caption (11px `#444`, bottom right):** "illustrative — five items added in order A to E".

## Six Moves, Two Different Answers

**Tags:** `worked example` (blue), `trace by hand` (green)

- **Six moves** — add A, add B, take, add C, take, take — run them through a stack, then through a queue
- **Stack run** — each take grabs the top: first B (newest), then C, and the last take finally reaches A
- **Queue run** — each take serves the front: first A (oldest), then B, then C, in arrival order
- **Check it** — the stack hands back B, C, A; the queue hands back A, B, C — trace it on paper in a minute
- **Reversal trick** — push items onto a stack and pop them all off, and their order comes out flipped

*Example (italic):* After "add A, add B", the stack's top is B so "take" returns B; the queue's front is still A, so its "take" returns A.

**Key point:** The same six moves give different outputs — stack: B, C, A; queue: A, B, C — and that output order is the entire difference between them.

### Visualization (canvas `c2`, 720×300)

Two-lane operation timeline: the identical six-move script across the top, one lane showing what the stack returns and one lane showing what the queue returns at each "take".

- **Title (bold 15px, `#1a5276`, top center):** "Same Script: add A, add B, take, add C, take, take".
- **Step headers:** "step 1" ... "step 6" in 11px `#6b7280`, centered at x = 165, 255, 345, 435, 525, 615, y=62.
- **Operation boxes (shared row, y=78):** six rounded rectangles 74×24 centered at the same six x positions, fill `#f8f9fa`, 1px `#e5e9ef` border, 12px `#2c3e50` labels: "add A", "add B", "take", "add C", "take", "take".
- **Lane labels (12px `#444`, left-aligned at x=16):** "stack (take from top)" at y=160 and "queue (take from front)" at y=235.
- **Stack lane (y=160):** horizontal 1px `#e5e9ef` guide line from x=130 to x=655; under each "take" step (x=345, 525, 615) a blue `#2a78d6` filled circle radius 14 with bold 13px white letter — "B", "C", "A"; under each "add" step a small 11px `#6b7280` note "in".
- **Queue lane (y=235):** same layout; green `#008300` circles at x=345, 525, 615 with bold 13px white letters "A", "B", "C"; 11px `#6b7280` "in" notes under the adds.
- **Annotation (bold 12px orange `#d95926`, centered near x=360, y=284):** "stack returns B, C, A — queue returns A, B, C".
- **Caption (11px `#444`, bottom right):** "outputs are exact — trace the six moves by hand".

## Undo Buttons and Waiting Rooms

**Tags:** `where it's used` (blue), `fairness` (green), `rule of thumb` (orange)

- **Undo button** — editors keep your changes on a stack; undo pops the most recent change first
- **Back button** — browsers stack visited pages; back returns you to the page you left last
- **Waiting jobs** — printers, ticket counters, and task runners queue work so the earliest is served first
- **Fairness** — five people A–E line up before opening; served one per minute, the queue's waits are 0–4
- **Starvation** — the stack flips that to 4–0: A, first in line, waits longest of all — 4 minutes
- **Rule of thumb** — stack when the newest matters most (undo); queue when arrival order is a promise

*Example (italic):* Ana (A) is first in line; the queue serves her after 0 minutes, but the stack serves her dead last after 4 minutes.

**Key point:** Both disciplines here average a 2-minute wait — but the queue rewards arriving early while the stack punishes it, so pick by fairness, not averages.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: for each arrival A–E, two bars showing minutes waited before service under the queue (FIFO) and under the stack (LIFO), from the five-arrivals, one-served-per-minute story in the text.

- **Title (bold 15px, `#1a5276`, top center):** "Minutes Waited by Arrival Order: Queue vs Stack".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 165; y = minutes waited 0 to 5, 12px `#444` tick labels "0"–"5", light `#e5e9ef` gridlines at 1, 2, 3, 4; x = five groups centered at x = 130, 240, 350, 460, 570 with 13px `#444` labels "A (1st)", "B (2nd)", "C (3rd)", "D (4th)", "E (5th)".
- **Bars (two per group, each 34px wide, 6px gap):** queue/FIFO bars green `#008300` fill `rgba(0,131,0,0.30)` with 2px green border, heights from `[0, 1, 2, 3, 4]`; stack/LIFO bars orange `#d95926` fill `rgba(217,89,38,0.25)` with 2px orange border, heights from `[4, 3, 2, 1, 0]`; bold 12px value label above each bar ("0 min" shown as "0" sitting on the baseline).
- **Legend (top right, 12px):** green swatch "queue (FIFO)", orange swatch "stack (LIFO)".
- **Annotation (bold 12px orange `#d95926`, near x=120, y=95, two lines):** "A is first in line —" / "the stack serves A last (4 min)".
- **Caption (11px `#444`, bottom right):** "illustrative — five in line before opening, one served per minute".

## Same Boxes, Different End

**Tags:** `common mistake` (red), `wrong-end bug` (orange)

- **Not different boxes** — a stack and a queue can hold the exact same items in the exact same slots
- **The one choice** — a stack adds and takes at the same end; a queue takes from the opposite end
- **Wrong-end bug** — take from the back of your "queue" and you have silently built a stack
- **The symptom** — work is handled newest-first: fresh requests fly, the earliest ones sit forever
- **Name test** — before coding, say out loud which end items enter and which end they leave

*Example (italic):* A ticket tool grabbed the latest ticket "because it was on top" — old tickets sat for weeks; the fix was one line: take from the front.

**Common mistake:** Treating stack vs queue as different containers. They are the same row of boxes — the entire design decision is which end you remove from.

### Visualization (canvas `c4`, 720×300)

Single row of the five shared items with two labeled removal arrows: the stack's take-arrow at the same end items enter, the queue's take-arrow at the opposite end — one picture, one difference.

- **Title (bold 15px, `#1a5276`, top center):** "One Row of Boxes — Where Does 'take' Point?".
- **Item row:** five rectangles 56×40 (fill `#f8f9fa`, 2px `#1a5276` border) at y=130, left edges x = 220, 282, 344, 406, 468, bold 14px `#1a5276` centered letters "A", "B", "C", "D", "E"; 11px `#6b7280` labels "oldest" under A and "newest" under E.
- **Enter arrow (shared):** 2px `#6b7280` arrow into the right end, x=580→528, y=150, 12px `#6b7280` label "items enter here" above it.
- **Stack take-arrow:** 3px blue `#2a78d6` arrow out of the right end, x=528→600, y=110, bold 12px blue label "stack: take from the SAME end (E first)" near x=430, y=88.
- **Queue take-arrow:** 3px green `#008300` arrow out of the left end, x=216→144, y=150, bold 12px green label "queue: take from the OTHER end (A first)" near x=150, y=200.
- **Annotation (bold 13px magenta `#d55181`, centered near x=360, y=262):** "the boxes never change — only the end you take from does".
- **Caption (11px `#444`, bottom right):** "illustrative — same five items as the cafeteria example".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all item letters, output orders, and bar heights are the hardcoded values above (no randomness); the stack/queue outputs B, C, A and A, B, C are exact consequences of the six-move script; the c3 wait times `[0,1,2,3,4]` vs `[4,3,2,1,0]` are the invented cafeteria timings and stay labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
