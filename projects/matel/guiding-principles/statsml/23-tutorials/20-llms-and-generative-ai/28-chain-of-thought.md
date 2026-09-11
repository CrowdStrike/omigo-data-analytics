# Chain-of-Thought

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Chain-of-Thought

**Subtitle:** Asking a model to write out its steps is not asking it to explain itself — every extra token it writes buys one more pass of computation, so the steps are extra thinking time, not a report

## One Coffee Order, Two Ways to Answer

**Tags:** `core idea` (blue), `intermediate tokens` (green), `one pass per token` (orange)

- **The order** — a model is asked: 3 lattes at $4.50 and 2 muffins at $3.25, plus a 10% tip — what's the total?
- **The direct way** — the model must produce the total in its very next few tokens: one shot, and it blurts $21.50
- **The step way** — told to "work it out step by step", it writes the lattes, the muffins, the tip, then $22.00
- **The trick** — a model spends one fixed-size pass of compute per token; the direct answer got ~3 passes, the steps got ~30
- **Chain-of-thought** — that written run of intermediate tokens is the chain; the extra tokens ARE the extra thinking

*Example (italic):* Same model, same question — the only difference is that the step-by-step version was allowed to write 30 tokens before committing to a total.

**Key point:** A model can't think longer on a hard token — it can only think MORE TIMES by writing more tokens first; chain-of-thought is buying passes, not narrating them.

### Visualization (canvas `c1`, 720×300)

Two-lane token diagram: the same prompt box feeding a short direct-answer lane (wrong total) and a long chain-of-thought lane (right total), each token drawn as a box so the compute difference is visible as sheer length.

- **Title (bold 15px, `#1a5276`, top center):** "Same Question, Same Model — the Long Lane Bought More Compute".
- **Prompt box:** rounded rect at x=20, y=115, 120×70, fill `rgba(26,82,118,0.12)`, 1.5px `#1a5276` border; centered 12px `#1a5276` text on three lines: "3 lattes $4.50" / "2 muffins $3.25" / "+ 10% tip?".
- **Lane 1 (direct), boxes at y=85, 26px tall:** label bold 12px `#444` "direct answer — 3 tokens" at x=170, y=75; three 34px-wide token boxes starting x=170 (6px gaps), fill `rgba(217,89,38,0.15)`, 1px `#d95926` border; final answer box at x=300, 90×26, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, bold 13px `#e74c3c` centered text "$21.50 ✗".
- **Lane 2 (chain-of-thought), boxes at y=185, 26px tall:** label bold 12px `#444` "step by step — ~30 tokens" at x=170, y=175; twelve 26px-wide token boxes starting x=170 (5px gaps), fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border, with 11px `#2a78d6` labels under boxes 2, 6, 10: "$13.50", "$20.00", "tip $2.00"; final answer box at x=550, 100×26, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 13px `#008300` centered text "$22.00 ✓".
- **Connectors:** 1.5px `#6b7280` arrows from the prompt box to the first token of each lane.
- **Annotation (bold 13px violet `#4a3aa7`, near x=380, y=255):** "every token = one more fixed-size pass of compute".
- **Caption (12px `#444`, bottom right):** "token counts illustrative".

## The Bill, One Written Step at a Time

**Tags:** `worked example` (blue), `redo it by hand` (green)

- **Step 1** — lattes: 3 × $4.50 = $13.50; the model writes this down, and the number is now in its context
- **Step 2** — muffins: 2 × $3.25 = $6.50, so the running subtotal is $13.50 + $6.50 = $20.00
- **Step 3** — tip: 10% of $20.00 = $2.00, so the final total is $20.00 + $2.00 = $22.00
- **Why it helps** — each later step reads the earlier written numbers instead of re-deriving them from scratch
- **The direct miss** — $21.50 = a $13.00 latte slip + $6.50 + the $2.00 tip, with no written step to catch it

*Example (italic):* Cover the answer and redo it on paper — $13.50, then $20.00, then $22.00 — you just ran the chain-of-thought yourself.

**Key point:** Each written step turns a hard one-shot question into three easy lookups — the chain works because later tokens can read the earlier ones.

### Visualization (canvas `c2`, 720×300)

Waterfall chart of the bill building up in three steps on a dollar axis, each written step adding one colored block to the running total.

- **Title (bold 15px, `#1a5276`, top center):** "The Chain in Dollars: $13.50 → $20.00 → $22.00".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y axis = dollars 0 to 24 with 12px `#444` tick labels "$0", "$6", "$12", "$18", "$24" and light `#e5e9ef` gridlines; x axis = three bar slots plus a total slot.
- **Bar slots (width 90, centered at x = 150, 290, 430, 590), 12px `#444` labels below the baseline:** "3 lattes × $4.50", "2 muffins × $3.25", "10% tip", "total".
- **Waterfall blocks:** slot 1: blue `rgba(42,120,214,0.35)` block from $0 to $13.50, 2px `#2a78d6` border; slot 2: green `rgba(0,131,0,0.30)` block from $13.50 to $20.00, 2px `#008300` border; slot 3: orange `rgba(217,89,38,0.30)` block from $20.00 to $22.00, 2px `#d95926` border; slot 4: solid-border ink block from $0 to $22.00, fill `rgba(26,82,118,0.15)`, 2px `#1a5276` border.
- **Step connectors:** dashed `#6b7280` (dash 4/3) horizontal guides at $13.50, $20.00, $22.00 linking each block's top to the next block's base.
- **Value labels (bold 12px, matching each block's border color, just above each block top):** "+$13.50", "+$6.50 → $20.00", "+$2.00", "$22.00".
- **Annotation (bold 13px `#e74c3c`, near x=560, y=90):** two lines: "direct answer said $21.50 —" / "no step ever wrote it down".
- **Caption (12px `#444`, bottom right):** "exact arithmetic — redo it by hand".

## Tokens Are the Compute Budget

**Tags:** `where it's used` (blue), `cost & latency` (orange), `compute budget` (green)

- **Fixed per token** — the network is the same size every pass; a token about tips costs the same as one about cats
- **Serial depth** — 30 intermediate tokens means 30 passes IN A ROW, each able to build on the last one's output
- **The knob** — "think step by step", reasoning modes, and token budgets all turn the same dial: more passes
- **Diminishing returns** — on our illustrative math set, accuracy climbs 35% → 76% by 20 tokens, then flattens near 88%
- **The bill** — cost and latency grow with every token whether or not it helped, so budget to the flat part

*Example (italic):* A team caps reasoning at 40 intermediate tokens because their eval showed 85% there and only 88% at double the tokens — and double the cost.

**Key point:** Chain-of-thought is a compute dial, not a magic phrase — you are trading tokens (money, seconds) for serial passes, and the trade flattens out.

### Visualization (canvas `c3`, 720×300)

Line chart of accuracy versus allowed intermediate tokens with a straight dashed cost line, showing accuracy flattening while cost keeps climbing.

- **Title (bold 15px, `#1a5276`, top center):** "Accuracy Flattens, Cost Doesn't (illustrative eval)".
- **Axes:** origin x=70, baseline y=240, plot width 570, plot height 175; x axis = intermediate tokens allowed, ticks at 0, 10, 20, 40, 60, 80 (12px `#444`, linear scale 0–80); left y axis = accuracy 0–100% with 12px `#444` labels "0%", "25%", "50%", "75%", "100%" and light `#e5e9ef` gridlines.
- **Accuracy curve:** blue `#2a78d6` 3px line with 5px dots through hardcoded points: tokens `[0, 5, 10, 20, 40, 80]`, accuracy `[35, 48, 62, 76, 85, 88]`; bold 12px blue value labels "35%", "62%", "76%", "88%" beside the 0, 10, 20 and 80 dots.
- **Cost line:** orange `#d95926` 2px dashed (dash 6/4) straight line from (0 tokens, y=235) to (80 tokens, y=95); 12px orange label "cost & latency (linear in tokens)" along its upper end.
- **Budget marker:** vertical dashed `#6b7280` (dash 4/3) line at 40 tokens from baseline to y=80; 11px `#6b7280` label "a sane budget" at its top.
- **Annotation (bold 13px `#008300`, near x=430, y=130):** two lines: "35% → 76% by 20 tokens," / "then the curve goes flat".
- **Caption (12px `#444`, bottom right):** "illustrative accuracy numbers, not a benchmark".

## The Steps Are Scratch Paper, Not a Confession

**Tags:** `common mistake` (red), `faithfulness` (orange)

- **The temptation** — the steps read like a diary of the model's reasoning, so people audit them as if they were one
- **Scratch paper** — the steps are workspace the model writes FOR ITSELF; nothing forces them to match its internals
- **Right for wrong reasons** — on our illustrative audit, 9% of answers were correct while a written step was flawed
- **Wrong with clean steps** — another 12% showed tidy, convincing steps and still landed on the wrong total
- **The mistake** — trusting an answer because its steps sound good; grade the answer, treat steps as a hint at best

*Example (italic):* A reviewer approves a refund because the model's five steps "all check out" — but step 3 quietly used last month's price, and the tidy prose hid it.

**Common mistake:** Reading chain-of-thought as a faithful explanation. It is compute the model spent, written where you happen to see it — verify the answer itself, not the story.

### Visualization (canvas `c4`, 720×300)

Four horizontal bars splitting an illustrative audit of 100 answers by whether the steps looked sound and whether the final answer was right, showing the two mismatch bars that break the "good steps = good answer" assumption.

- **Title (bold 15px, `#1a5276`, top center):** "Steps vs Answers: an Audit of 100 Responses (illustrative)".
- **Axis:** horizontal 2px `#999` line at y=250 from x=250 to x=680 (width 430), scale 0–80%; tick labels "0%", "20%", "40%", "60%", "80%" (12px `#444`) below.
- **Rows (18px-tall bars at y = 85, 125, 165, 205), each with a right-aligned 12px `#444` label ending at x=240:**
  - "steps sound, answer right": green `rgba(0,131,0,0.35)` bar to 71%, 2px `#008300` border
  - "steps flawed, answer right": aqua `rgba(25,158,112,0.35)` bar to 9%, 2px `#199e70` border
  - "steps sound, answer WRONG": red `rgba(231,76,60,0.25)` bar to 12%, 2px `#e74c3c` border
  - "steps flawed, answer wrong": mute `rgba(107,114,128,0.25)` bar to 8%, 2px `#6b7280` border
- **Value labels:** bold 12px, matching each bar's border color, at each bar's right end: "71%", "9%", "12%", "8%".
- **Highlight bracket:** 2px `#e74c3c` bracket spanning the middle two rows on the right side near x=400, with bold 13px `#e74c3c` two-line annotation: "21% mismatch —" / "the steps are not the reasoning".
- **Caption (12px `#444`, bottom right):** "illustrative audit — proportions invented for teaching".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all token counts, dollar amounts, accuracy points and audit percentages are the hardcoded literals above (no randomness); the bill arithmetic in c2 is exact ($13.50 + $6.50 + $2.00 = $22.00); c1 token counts, the c3 accuracy curve and the c4 audit split are invented and must keep their "illustrative" captions; text numbers and chart numbers must stay identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
