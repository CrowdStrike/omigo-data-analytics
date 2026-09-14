# Vector Clocks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Vector Clocks

**Subtitle:** Each computer keeps one counter per participant; comparing two vectors tells you whether one event knew about the other — or whether neither did

## Three Coffee Branches, One Price List

**Tags:** `core idea` (blue), `one counter each` (green), `no wall clock` (orange)

- **The chain** — three coffee branches (Downtown, Airport, Mall) share one replicated price list
- **The vector** — each branch keeps three counters [D, A, M], one slot per branch, all starting at 0
- **Own slot** — when a branch edits the list, it adds 1 to its own slot and touches nothing else
- **Piggyback** — every sync message carries the sender's whole vector along with the edit
- **Merge** — the receiver takes the element-wise max of the two vectors, then bumps its own slot
- **The payoff** — comparing two stamps later reveals whether one edit knew about the other

*Example (italic):* Downtown edits once and syncs; Airport's vector jumps from [0,0,0] to [1,1,0] — one Downtown edit it now knows about, plus its own receive.

**Key point:** A vector clock is one counter per participant; a stamp records exactly which of everyone's events that moment already knew about — real time of day never enters it.

### Visualization (canvas `c1`, 720×300)

Space-time sync diagram: three horizontal branch timelines with stamped event dots and violet sync arrows between them; every stamp follows the two rules.

- **Title (bold 15px, `#1a5276`, top center):** "Three Branches, Five Events: Every Stamp Follows the Two Rules".
- **Timelines:** three horizontal 2px `#6b7280` lines from x=110 to x=690 at y=90 (Downtown), y=160 (Airport), y=230 (Mall); 13px `#1a5276` labels "Downtown", "Airport", "Mall" left-aligned at x=15; Mall's line dashed (dash 5/4) with a 12px `#6b7280` label "no edits — third slot stays 0 everywhere" centered on it.
- **Events (7px radius dots, bold 12px stamp labels 16px above the line):** Downtown: blue `#2a78d6` dot at x=180 "[1,0,0] edit", green `#008300` dot at x=420 "[2,1,0] receive", blue dot at x=560 "[3,1,0] edit"; Airport: green dot at x=300 "[1,1,0] receive", blue dot at x=520 "[1,2,0] edit" (Airport labels 20px below the line).
- **Sync arrows (violet `#4a3aa7`, 2px, filled arrowheads):** from (180, 90) to (300, 160) with 11px violet label "carries [1,0,0]" at the midpoint; from (300, 160) to (420, 90) with 11px violet label "carries [1,1,0]".
- **Annotation (bold 13px magenta `#d55181`, centered near x=470, y=272):** "[2,1,0] and [1,2,0] never heard of each other".
- **Caption (12px `#444`, bottom right):** "all stamps exact — rule 1: bump own slot; rule 2: max, then bump".

## Reading Two Stamps Slot by Slot

**Tags:** `worked example` (blue), `element-wise compare` (green)

- **Rule 1** — on a local edit, add 1 to your own slot only
- **Rule 2** — on receive, take the element-wise max of both vectors, then add 1 to your own slot
- **Ordered** — V comes before W when every slot of V is ≤ W's and at least one is strictly smaller
- **Check one** — [2,1,0] vs [3,1,0]: 2≤3, 1≤1, 0≤0 with one strict — so [2,1,0] came first
- **Check two** — [2,1,0] vs [1,2,0]: 2>1 in slot D but 1<2 in slot A — neither before the other
- **The verdict** — a split like check two is called concurrent: each edit was blind to the other

*Example (italic):* Airport's solo edit stamps [1,2,0] while Downtown sits at [2,1,0]; each vector wins one slot, so the two edits are concurrent.

**Key point:** Element-wise comparison yields three verdicts — before, after, or concurrent — and "concurrent" is the verdict no single timestamp can ever produce.

### Visualization (canvas `c2`, 720×300)

Two side-by-side comparison panels: each shows two 3-cell vector boxes with per-slot comparison marks between them and a verdict badge underneath.

- **Title (bold 15px, `#1a5276`, top center):** "Compare Slot by Slot: All One Way Means Ordered, a Split Means Concurrent".
- **Cell style:** each vector is three adjacent boxes 46px wide, 34px tall, 1px `#1a5276` border, fill `rgba(42,120,214,0.12)`, bold 13px `#2c3e50` digit centered; 11px `#6b7280` slot headers "D  A  M" above the top vector.
- **Left panel (vectors left-aligned at x=90):** top vector [2,1,0] at y=95, bottom vector [3,1,0] at y=175; between them, per-slot marks in bold 13px green `#008300`: "≤", "≤", "≤"; verdict badge below at y=235 (bold 13px green): "ordered: [2,1,0] happened before [3,1,0]".
- **Right panel (vectors left-aligned at x=430):** top vector [2,1,0] at y=95, bottom vector [1,2,0] at y=175; per-slot marks in bold 13px magenta `#d55181`: ">", "<", "="; verdict badge below at y=235 (bold 13px magenta): "concurrent: neither saw the other".
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=270):** "one comparison, three possible answers: before, after, concurrent".
- **Caption (12px `#444`, bottom right):** "vectors exact — taken from the sync diagram above".

## Why the App Says "Your Edit Conflicts"

**Tags:** `where it's used` (blue), `replicated stores` (green), `conflict detection` (orange)

- **The clash** — Downtown prices the latte at $5.25 stamped [3,1,0]; Airport says $4.95 stamped [1,2,0]
- **The store** — the replicated database compares the stamps, sees concurrent, and keeps both versions
- **Last-write-wins** — with wall-clock timestamps, whichever synced later silently erases the other
- **The prompt** — every "your edit conflicts with another change" message is this comparison surfacing
- **Real systems** — Dynamo-style stores, sync engines, and version-control tools lean on exactly this

*Example (italic):* A customer app reads the latte price and gets two siblings, $5.25 and $4.95, with proof that neither writer saw the other.

**Key point:** Vector clocks let a replicated store tell "stale overwrite, discard safely" apart from "true conflict, keep both" — a plain timestamp cannot make that distinction.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: the same two offline price edits resolved by wall-clock last-write-wins (one edit lost) vs by vector clocks (conflict kept).

- **Title (bold 15px, `#1a5276`, top center):** "Two Offline Price Edits: Timestamps Overwrite, Vector Clocks Flag".
- **Row 1 (y=95), 12px `#444` label at x=20:** "wall-clock LWW"; blue `#2a78d6` rounded box at x=150 labeled "$5.25 @ 2:04pm", blue box at x=320 labeled "$4.95 @ 2:05pm", 3px arrows from both into a red `#e74c3c` box at x=510 labeled "keep $4.95 only" with bold 12px red "✗ $5.25 silently lost" beneath it.
- **Row 2 (y=205), label:** "vector clocks"; blue box at x=150 labeled "$5.25 [3,1,0]", blue box at x=320 labeled "$4.95 [1,2,0]", 3px arrows into a green `#008300` box at x=510 labeled "concurrent → keep both" with bold 12px green "✓ app shows the conflict" beneath it.
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=278):** "'your edit conflicts' is this comparison talking".
- **Caption (12px `#444`, bottom right):** "prices and times illustrative; vector stamps exact".

## Detecting Is Not Resolving

**Tags:** `common mistake` (red), `merge still needed` (orange), `growth` (blue)

- **Detects only** — the vector says "concurrent"; it never says which price the latte should be
- **Resolution** — someone must still merge: a user prompt, a business rule, or an automatic merge type
- **The growth** — one slot per participant: 3 branches is tiny, 200 phone clients means 200 slots
- **The cost** — at 8 bytes per slot, a version's clock weighs 24 B at 3 nodes but 1.6 KB at 200
- **The fixes** — real stores prune long-dead slots or keep one slot per server instead of per client

*Example (italic):* A cart store stamping versions per phone client ends up with a 200-slot vector — 1.6 KB of clock attached to a 40-byte cart row.

**Common mistake:** Expecting vector clocks to resolve conflicts. They only detect them — and since the vector grows one slot per participant, even the detection has a price.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: clock size per stored version as the participant roster grows from 3 branches to 200 phone clients.

- **Title (bold 15px, `#1a5276`, top center):** "One Slot per Participant: the Clock Grows with the Roster".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 430; widths proportional to bytes.
- **Rows (bars 14px tall at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20, 11px byte label at the bar end:**
  - "3 branches — 3 slots": blue `#2a78d6` bar width 6, label "24 B"
  - "10 clients — 10 slots": aqua `#199e70` bar width 22, label "80 B"
  - "50 clients — 50 slots": orange `#d95926` bar width 108, label "400 B"
  - "200 clients — 200 slots": red `#e74c3c` bar width 430, label "1.6 KB per version"
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "and the verdict is still only 'conflict' — merging is your job".
- **Caption (12px `#444`, bottom right):** "exact: one 8-byte slot per participant".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all vector stamps are the hardcoded values above ([1,0,0], [1,1,0], [2,1,0], [1,2,0], [3,1,0]) and follow rules 1 and 2 exactly (no randomness); latte prices and wall-clock times are invented and labeled illustrative; clock sizes (24 B / 80 B / 400 B / 1.6 KB) are exact at one 8-byte slot per participant for 3 / 10 / 50 / 200 participants, bar pixel widths 6 / 22 / 108 / 430 proportional to bytes.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
