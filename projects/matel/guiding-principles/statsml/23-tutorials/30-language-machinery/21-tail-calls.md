# Tail Calls

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Tail Calls

**Subtitle:** A tail call is a recursive call made as the very last act of a function — nothing is left waiting for its answer, so the computer can reuse one stack frame and run the recursion like a loop

## Adding a Pile of Receipts Two Ways

**Tags:** `core idea` (blue), `running total` (green), `one reused frame` (orange)

- **The pile** — a cashier totals 5 receipts: 12, 7, 5, 9, 4 — and does it by peeling one off the top
- **The come-back way** — "total = my receipt + total of the rest": each receipt waits for the rest's answer
- **Waiting costs memory** — every waiting receipt keeps its own stack frame open; 5 receipts, 5 frames
- **The carry-forward way** — pass a running total down with the pile; the top receipt is done the instant it hands off
- **The tail call** — when the hand-off is the very last act, nobody needs to come back, so one frame is reused
- **Loop in disguise** — the compiler can replace "call myself" with "jump back to the top": recursion becomes a loop

*Example (italic):* Handing the pile plus "running total so far: 12" to the next person and walking away is a tail call; staying to add your 12 after they finish is not.

**Key point:** If the recursive call is the last thing the function does, no one waits — the same stack frame can be reused, and the recursion runs as a loop.

### Visualization (canvas `c1`, 720×300)

Two side-by-side stack diagrams for the same 5-receipt pile: the come-back way stacking 5 frames on the left, the carry-forward way reusing 1 frame on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Same 5 Receipts: One Way Stacks Frames, One Way Reuses a Frame".
- **Left panel (x=40 to x=340):** 13px bold `#2c3e50` header "come back and add" at top; 5 stacked boxes (each 250×30, 6px vertical gap, bottom box at y=250) filled `rgba(42,120,214,0.15)` with 2px `#2a78d6` border; 12px `#2c3e50` labels bottom to top: "sum(12,7,5,9,4) — waiting", "sum(7,5,9,4) — waiting", "sum(5,9,4) — waiting", "sum(9,4) — waiting", "sum(4) — working"; a 12px `#d95926` note "+12", "+7", "+5", "+9" to the right of each waiting box.
- **Right panel (x=400 to x=700):** 13px bold `#2c3e50` header "carry the total forward" at top; ONE box (250×30 at y=250) filled `rgba(0,131,0,0.15)` with 2px `#008300` border, 12px label "sum(pile, total) — reused"; above it a green `#008300` circular arrow (two arced 2px strokes with an arrowhead) and 12px green labels listing the totals it cycles through: "total: 0 → 12 → 19 → 24 → 33 → 37".
- **Annotation (bold 13px orange `#d95926`, centered near y=120 between panels):** two lines: "left: 5 frames held open" / "right: 1 frame, reused 5 times".
- **Caption (12px `#444`, bottom right):** "same pile, same answer 37 — different memory shape".

## Hand-Checking the Total: 12 + 7 + 5 + 9 + 4 = 37

**Tags:** `worked example` (blue), `accumulator` (green)

- **The rule** — sum(pile, total): if the pile is empty, answer total; else sum(rest, total + top)
- **Start** — sum([12, 7, 5, 9, 4], 0): peel the 12, hand off sum([7, 5, 9, 4], 12)
- **Each step is final** — nothing is written down to do later; the total travels with the pile
- **The chain** — totals go 0 → 12 → 19 → 24 → 33 → 37 as the pile shrinks to empty
- **The answer** — the empty pile returns 37 straight to the original caller; no unwinding, no adding on the way back

*Example (italic):* Redo it on paper: 0+12=12, 12+7=19, 19+5=24, 24+9=33, 33+4=37 — that pencil trace IS the loop the compiler runs.

**Key point:** The accumulator does the arithmetic on the way down (0, 12, 19, 24, 33, 37), so the way back up has nothing to do — which is exactly why the frames can be thrown away.

### Visualization (canvas `c2`, 720×300)

Horizontal hand-off chain: six boxes left to right, each showing the shrinking pile and the growing total, joined by arrows, ending in the answer 37.

- **Title (bold 15px, `#1a5276`, top center):** "The Running Total Travels With the Pile".
- **Chain:** six rounded boxes (95×64 each) evenly spaced from x=25 to x=695, centered vertically at y=150, filled `rgba(42,120,214,0.12)` with 2px `#2a78d6` border; the last box filled `rgba(0,131,0,0.15)` with 2px `#008300` border.
- **Box contents (two lines, 12px `#2c3e50`, pile on top, bold total below):** "[12,7,5,9,4]" / "total 0"; "[7,5,9,4]" / "total 12"; "[5,9,4]" / "total 19"; "[9,4]" / "total 24"; "[4]" / "total 33"; "[ ] empty" / bold 13px green "answer 37".
- **Arrows:** 2px `#6b7280` arrows with arrowheads between consecutive boxes; bold 11px `#d95926` label "+12", "+7", "+5", "+9", "+4" above each arrow.
- **Trace line (below the chain):** 12px `#444` text at y=245 centered: "totals: 0 → 12 → 19 → 24 → 33 → 37"; beside it 12px `#008300` "stack depth the whole time: 1 frame".
- **Annotation (bold 13px green `#008300`, near x=360, y=70):** "each box replaces the last — nothing waits behind it".

## Why a Loop in Disguise Matters

**Tags:** `where it's used` (blue), `stack overflow` (red), `constant memory` (green)

- **Small piles hide it** — 5 receipts either way is fine; the difference appears when the pile gets deep
- **The crash** — come-back recursion needs one frame per receipt, and the stack allows about 10,000 frames
- **Day-of-sales pile** — a 20,000-receipt pile overflows the come-back version at receipt 10,000
- **The tail version** — reuses 1 frame no matter the pile size; 20,000 receipts, still 1 frame
- **Who optimizes** — Scheme and some functional languages guarantee it; C compilers often do; Python never does
- **Same trick elsewhere** — walking long linked lists, retry loops, state machines written as mutual calls

*Example (italic):* The come-back version dies with "maximum recursion depth exceeded" on a 20,000-receipt day; the tail version totals it in one reused frame.

**Key point:** Tail-call optimization turns "one stack frame per item" into "one stack frame, period" — deep recursion stops being a crash risk and becomes an ordinary loop.

### Visualization (canvas `c3`, 720×300)

Line chart of stack frames used versus pile size: the come-back line climbs 1-for-1 and hits the stack limit, the tail-call line stays flat at 1.

- **Title (bold 15px, `#1a5276`, top center):** "Frames Needed vs Receipts in the Pile".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; x = receipts 0 to 20,000 with 12px `#444` tick labels "0", "5,000", "10,000", "15,000", "20,000"; y = stack frames 0 to 12,000 with 12px `#444` tick labels "0", "4,000", "8,000", "12,000" and light `#e5e9ef` gridlines.
- **Stack limit:** horizontal dashed red `#e74c3c` (dash 6/4) 2px line at 10,000 frames; 12px red label at its left end: "stack limit ~10,000 frames".
- **Come-back line:** blue `#2a78d6` 3px line through hardcoded points (receipts, frames) = `[[0, 0], [2500, 2500], [5000, 5000], [7500, 7500], [10000, 10000]]`, stopping where it meets the limit; bold 14px red `#e74c3c` "✗ crash" marker just past the meeting point at (10,000, 10,000); 12px blue label "one frame per receipt" along the slope near (5,500, 6,300 frames).
- **Tail-call line:** green `#008300` 3px line, flat at 1 frame from receipts 0 to 20,000 (points `[[0, 1], [20000, 1]]`, drawn a few pixels above the baseline so it stays visible); 12px green label "tail call: 1 frame, always" above its right end.
- **Annotation (bold 13px green `#008300`, near x=545, y=115):** two lines: "20,000 receipts:" / "crash vs. still 1 frame".
- **Caption (12px `#444`, bottom right):** "frame counts and 10,000 limit illustrative — real limits vary by language".

## What Counts as "Last"

**Tags:** `common mistake` (red), `pending work` (orange)

- **The test** — after the recursive call returns, is there anything left to do? If yes, it is not a tail call
- **Looks last, isn't** — `return 12 + sum(rest)`: the call sits at the end of the line, but the "+ 12" runs after it
- **Truly last** — `return sum(rest, total + 12)`: the adding happens before the call; the call's answer is THE answer
- **The rewrite** — moving the pending "+" into an accumulator argument is what converts one form into the other
- **Language fine print** — writing a perfect tail call in Python still overflows; the language must do the optimization

*Example (italic):* `return 12 + sum(rest)` must come back for the "+", so its frame stays open — one plus sign is the whole difference from the tail form.

**Common mistake:** Judging by where the call appears on the line instead of by pending work. "Last thing executed" is the test, not "last thing written" — and even a true tail call only saves memory if the language optimizes it.

### Visualization (canvas `c4`, 720×300)

Two annotated code boxes side by side: the not-a-tail-call form with its pending "+" circled, and the tail-call form with nothing pending.

- **Title (bold 15px, `#1a5276`, top center):** "One Plus Sign Decides It".
- **Left box (x=40 to x=345, y=70 to y=210):** rounded box, fill `rgba(231,76,60,0.06)`, 2px `#e74c3c` border; 13px bold `#e74c3c` header "not a tail call" above; monospace 14px `#2c3e50` code line centered: "return 12 + sum(rest)"; a 2.5px `#e74c3c` circle around the "+" glyph; 12px `#e74c3c` note below the code, two lines: "the + runs AFTER the call returns" / "frame must stay open and wait".
- **Right box (x=375 to x=680, y=70 to y=210):** rounded box, fill `rgba(0,131,0,0.06)`, 2px `#008300` border; 13px bold `#008300` header "tail call" above; monospace 14px `#2c3e50` code line centered: "return sum(rest, total + 12)"; 12px `#008300` note below the code, two lines: "the + runs BEFORE the call" / "nothing pending — frame can be reused".
- **Arrow:** 2px `#6b7280` arrow from the left box to the right box at y=140 with 11px `#6b7280` label "move the + into an argument" above it.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=260):** "tail = the call is the very last act — its answer is your answer".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all receipt values (12, 7, 5, 9, 4), the running-total sequence (0, 12, 19, 24, 33, 37), and the frames-vs-receipts points are the hardcoded literals above (no randomness); the stack-limit figure 10,000 is illustrative and captioned as such. Code snippets in c4 are drawn as canvas text, not real `<code>` blocks.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
