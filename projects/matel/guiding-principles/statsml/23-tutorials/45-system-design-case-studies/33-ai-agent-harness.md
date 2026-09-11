# AI Agent Harness

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** AI Agent Harness

**Subtitle:** The model only produces text — everything that makes it an agent is the system around it: assemble the context, run the tool it asked for, append the result, ask again, and know when to stop

## The Turn Cycle: Model Asks, Harness Runs

**Tags:** `core idea` (blue), `the loop is the system` (green), `agents` (orange)

- **The task** — a developer types "fix the failing test" and nothing else happens yet
- **What the model returns** — text naming a tool and its arguments, never the action itself
- **Who acts** — the harness runs the tool in a sandbox; the model never touches the repo
- **A turn** — one model call, the tool it asked for, and that result appended to the context
- **Four parts inside** — a context assembler, a permission gate, a tool router, and a turn log
- **The stop rule** — the loop ends when the model replies with no tool call, or a guard fires
- **Stateless model** — it recalls nothing between calls, so the harness re-sends the whole conversation

*Example (italic):* The model asks to read `test_billing.py`, then to run the test, then to edit one line — three turns, three tool results, all executed by the harness.

**Key point:** The model is a stateless function from text to text — the harness is the actual system: it owns the context, the tool execution, the sandbox, the stop condition, and the bill.

### Visualization (canvas `c1`, 720×300)

Component diagram: task enters the harness box, which contains four labeled internal parts; the harness talks up to the model API and right to the sandboxed tool executor.

- **Title (bold 15px, `#1a5276`, top center):** "The Turn Cycle: the Harness Owns Everything Except the Text".
- **Task box** (rounded 120×40, x=16, y=150, radius 8, fill `rgba(42,120,214,0.15)`, stroke `#2a78d6`, 12px `#2c3e50` two lines): "task: fix the / failing test".
- **Model box** (rounded 200×40, x=250, y=44, fill `rgba(74,58,167,0.15)`, stroke `#4a3aa7`, bold 12px `#4a3aa7`): "model API — stateless".
- **Harness outer box** (rounded 200×120, x=250, y=118, fill `rgba(26,82,118,0.07)`, stroke `#1a5276` 2px); label bold 12px `#1a5276` centered at y=134: "harness".
- **Four internal parts** (rounded 176×20, x=262, radius 4, fill `#fff`, stroke `#6b7280` 1px, 11px `#2c3e50` centered) at y = 142, 166, 190, 214: "context assembler", "permission gate", "tool router", "turn log".
- **Tools box** (rounded 180×56, x=520, y=150, fill `rgba(25,158,112,0.15)`, stroke `#199e70`, 12px `#2c3e50` two lines): "tool executor / sandbox: read, edit, bash".
- **Arrows** (2px `#6b7280`): task→harness at y=170 (x 138→246); harness up at x=310 (y=118→y=86) with 11px `#6b7280` right-aligned label "full context" at (304, 106); model down at x=390 (y=84→y=116) with left-aligned label "tool call" at (396, 106); harness→tools at y=168 (x 452→516); tools→harness at y=192 (x 516→452) with centered 11px label "result appended" at (484, 216).
- **Annotation (bold 13px blue `#2a78d6`, centered y=262):** "the model asks; only the harness ever acts".
- **Caption (12px `#444`, bottom left):** "illustrative".

## Why the Context Is Append-Only

**Tags:** `core idea` (blue), `prefix and tail` (green), `caching` (orange)

- **The fixed head** — the system prompt plus tool definitions, byte-identical on every turn
- **The growing tail** — each finished turn appends the model's request and the tool's output
- **Never rewritten** — earlier turns are not edited, reordered, or summarized away mid-task
- **Why it matters** — a prefix the provider has already seen costs a small fraction as much
- **The catch** — the match is byte for byte, so a single changed character ends the discount
- **The trap** — a clock or request id in the system prompt voids the hit on every single turn
- **The cost shape** — the conversation is re-sent each turn, so early turns are paid for repeatedly

*Example (italic):* A timestamp injected into the system prompt changes the first few bytes, so every turn of a long session is billed as brand-new text.

**Key point:** Treat the context as append-only with a frozen head — that single discipline is what makes an agent affordable, and any harness that rewrites earlier turns throws the saving away.

### Visualization (canvas `c2`, 720×300)

Payload-structure diagram: the request drawn as stacked labeled blocks at an early, middle, and late turn, showing the same head with a lengthening tail and a marked cache boundary.

- **Title (bold 15px, `#1a5276`, top center):** "Every Request: the Same Frozen Head, a Longer Tail".
- **Rows** (bars 34px tall, x from 110 to at most 670, top edges y = 70, 130, 190), each with a right-aligned 12px `#444` label ending at x=100: "early turn", "middle turn", "late turn".
- **Segments per row, left to right:** "system prompt" block (`rgba(26,82,118,0.55)`, 62px), "tool defs" block (`rgba(74,58,167,0.5)`, 92px), then equal 38px turn blocks (`rgba(42,120,214,0.35)`) — 1 on the early row, 6 on the middle row, 13 on the late row — separated by 1px white lines. Widths are schematic, not a token scale.
- **Head labels (bold 10px white, centered in their blocks, first row only):** "system", "tool defs"; a bold 11px `#1a5276` bracket label above the late row's tail at y=182: "one block per completed turn".
- **Cache boundary:** on the late row, a dashed 1.5px `#d95926` vertical line (dash 4/3) before the final turn block, with bold 11px `#d95926` labels "already seen" left-aligned at x=120, y=236 and "new" left-aligned just right of the line at y=236.
- **Annotation (bold 13px magenta `#d55181`, centered y=266):** "the head never changes — that is the whole trick".
- **Caption (12px `#444`, bottom right):** "block widths schematic".

## One Sandbox per Live Session

**Tags:** `where it's used` (blue), `deployment` (green), `isolation` (orange)

- **Sessions are long** — a routine task runs for a minute or two, nearly all of it waiting
- **The sandbox is held** — the tools' files and shell state must survive into the next turn
- **Idle but occupied** — a worker waiting on the model still owns an entire filesystem
- **The scaling unit** — capacity is counted in live sandboxes, not in requests per second
- **Admission control** — a full pool queues new tasks rather than slowing the running ones
- **Why isolate** — the model, not the author, picks the shell command that actually runs
- **Untrusted input** — a file saying "ignore prior instructions" enters the context as text
- **Resumability** — the turn log outlives the worker, so a crash replays instead of restarting

*Example (italic):* Because sessions are mostly waiting, one host holds many sandboxes at once — memory and open file handles run out long before CPU does.

**Key point:** An agent session is a long-lived, mostly-idle hold on isolated state, so the thing you provision is a concurrent sandbox and the knob you control is admission.

### Visualization (canvas `c3`, 720×300)

Deployment diagram: task arrivals into an admission queue, a pool of session workers each wrapping its own sandbox, the shared model API above, and an external turn-log store below.

- **Title (bold 15px, `#1a5276`, top center):** "Deployment: the Scaling Unit Is a Held Sandbox, Not a Request".
- **Arrivals label (12px `#2c3e50`, left, x=16, y=155):** "incoming tasks".
- **Admission queue** (rect 34×130, x=120, y=90, fill `rgba(201,133,0,0.18)`, stroke `#c98500`); six 11px horizontal tick lines inside; rotated −90° bold 11px `#c98500` label "admission queue" centered on the box.
- **Worker pool box** (rounded 300×150, x=210, y=80, radius 8, fill `rgba(26,82,118,0.05)`, dashed 1.5px `#1a5276` stroke, dash 5/4); bold 12px `#1a5276` label at (360, 96): "session worker pool".
- **Three worker cards** (rounded 84×56, radius 6, y=110, x = 222, 318, 414, fill `#fff`, stroke `#1a5276`): each holds bold 10px `#1a5276` "harness loop" at y=126 and an inner rounded 68×20 box (x+8, y=134, fill `rgba(25,158,112,0.18)`, stroke `#199e70`, 10px `#2c3e50`) labeled "sandbox".
- **Ellipsis (bold 13px `#6b7280`)** at (480, 142): "…"; below the pool a bold 12px `#1a5276` centered label at (360, 202): "one worker, one sandbox, held for the whole session"; under it 11px `#6b7280` at y=220: "mostly idle — waiting on the model".
- **Model API box** (rounded 180×34, x=540, y=70, fill `rgba(74,58,167,0.15)`, stroke `#4a3aa7`, bold 11px `#4a3aa7`): "model API (shared)"; 2px `#6b7280` bidirectional arrows between the pool's right edge and this box.
- **Turn-log store** (rounded 180×34, x=540, y=170, fill `rgba(25,158,112,0.12)`, stroke `#199e70`, 11px `#2c3e50`): "turn log (durable)"; 2px `#6b7280` arrow from the pool to it, with 10px `#6b7280` label at (630, 216): "replay, don't restart".
- **Arrows:** arrivals→queue at y=155; queue→pool at y=155.
- **Annotation (bold 13px green `#008300`, centered y=272):** "shed load at the queue — an accepted session that loses its sandbox loses its work".
- **Caption (12px `#444`, bottom left):** "illustrative".

## The Loop With No Stop Condition

**Tags:** `common mistake` (red), `guards` (orange)

- **The failure** — the model keeps proposing one more check, and nothing in the loop says "enough"
- **Why it compounds** — each turn re-sends every earlier turn, so the latest turns cost the most
- **Not caught by tests** — every individual turn looks reasonable; only the total is absurd
- **The turn cap** — a hard ceiling on the turn count: crude, but it always terminates
- **The budget** — a ceiling on tokens or wall-clock time, for turns that balloon in size not count
- **The duplicate detector** — the same tool, arguments, and result twice running means no progress
- **Cheapest guard first** — it ends a read-test-read-test spin in a few turns rather than at the cap
- **Say it out loud** — report which guard fired; a silent stop is indistinguishable from a finished task

*Example (italic):* An agent that alternates reading one file and re-running one test can spin indefinitely — two identical calls in a row is enough evidence to stop.

**Common mistake:** Trusting the model to decide when it is finished. The guards are not tuning knobs — they are the difference between a bounded task and an open-ended bill, and the harness must say which guard fired instead of presenting truncated work as done.

### Visualization (canvas `c4`, 720×300)

Two-row cycle diagram: the same model↔tool cycle drawn without guards (an unbroken ring) and with three guards inserted on the return path as gates that can break it.

- **Title (bold 15px, `#1a5276`, top center):** "Same Cycle, With and Without Guards on the Return Path".
- **Row labels (12px `#444`, left-aligned x=16):** "no guards" at y=100, "guarded" at y=225.
- **Row 1 cycle:** two rounded boxes 120×38 (radius 8) at x=120 and x=340, y=80 — "model" (fill `rgba(74,58,167,0.15)`, stroke `#4a3aa7`) and "tool result" (fill `rgba(25,158,112,0.15)`, stroke `#199e70`), 12px `#2c3e50`; a 2px `#6b7280` arrow forward at y=99 (x 242→336) and a curved 2px `#e74c3c` return arc from the tool box's bottom (400, 118) dipping to y=142 and back up to the model box's bottom (180, 118), arrowhead at the model end.
- **Row 1 terminus (bold 12px `#e74c3c`, right-aligned at (700, 100), two lines at y=94 and y=110):** "nothing closes the ring —" / "it stops when someone notices".
- **Row 2 cycle:** the same two boxes at y=205, same forward arrow at y=224; the return path is drawn as three inline gate boxes (rounded 92×22, radius 4, y=252, x = 150, 254, 358, fill `rgba(230,126,34,0.15)`, stroke `#d95926`, 10px `#2c3e50` centered): "turn cap", "budget", "duplicate call", chained left-to-right by 1.5px `#6b7280` arrows, with a 2px `#6b7280` arrow from the tool box down into the rightmost gate and from the leftmost gate up into the model box.
- **Row 2 terminus (bold 12px `#008300`, right-aligned at (700, 225), two lines at y=219 and y=235):** "any gate fires → stop," / "report the partial answer".
- **Annotation (bold 13px orange `#d95926`, centered y=292):** "the guard, not the model, is what ends the task".
- **Caption (12px `#444`, bottom left, y = h−8):** "illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`), matching `32-multi-tenancy.html`. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Visual character:** all four canvases are architecture diagrams — component, payload-structure, deployment, and cycle-with-gates. No bar or line charts on this page.
- **Numeric restraint (deliberate):** this page teaches structure, not arithmetic. No token counts, prices, latencies, percentages, or capacity math anywhere in the text or the diagrams — quantities are described in words ("a minute or two", "a dozen or so"). An earlier draft carried token/cost/Little's-law arithmetic in every section and was unreadable; do not reintroduce it. If a future edit needs the cost model, it belongs on its own page.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Bullet length:** aim for bullets that render on one line at normal page width — bold label plus a short phrase, roughly 80 characters of plain text. Prefer splitting a long bullet into another labeled bullet over deleting the fact, which is why several sections carry a dozen or more short bullets rather than eight long ones.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared `rr` (rounded rect), `arrow`, and `box` helpers. Draw functions registered in a `__charts` array and re-run on debounced (150ms) window resize.
- **Palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data:** no randomness, no measured or asserted statistics; block counts in c2 (1 / 6 / 13 turn blocks) are schematic illustrations of a lengthening tail, and widths are fixed pixels rather than a token scale.
- **Grid entry:** card 33 on `45-system-design-cases`, category label "AI SYSTEMS" colored `#16a085`.
