# LoRA & Parameter-Efficient Fine-Tuning

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** LoRA & Parameter-Efficient Fine-Tuning

**Subtitle:** A giant pre-trained model has billions of knobs already set — LoRA freezes all of them and trains only a thin add-on sliver, so the model learns your task without anyone touching the original

## A Bakery Bot on a Borrowed Brain

**Tags:** `core idea` (blue), `freeze and add` (green), `running example` (orange)

- **The bakery** — a small bakery wants its chatbot to talk like its menu, not like a generic robot
- **The giant** — the bot runs on a huge ready-made model whose billions of knobs someone else already set
- **Full fine-tune** — retraining every knob for one bakery is like reprinting an encyclopedia to add a page
- **The sliver** — LoRA freezes the whole model and bolts a small trainable patch beside each weight block
- **Same path** — the answer is the frozen block's output plus the patch's small correction, added together

*Example (italic):* The bakery's bot learns to say "our sourdough proofs for 18 hours" by training a patch under 2% of one layer's size — the base model itself is never edited.

**Key point:** LoRA = freeze the pre-trained model and train tiny add-on matrices beside it; the base stays exactly as downloaded, the sliver carries the specialization.

### Visualization (canvas `c1`, 720×300)

Block diagram of one layer: a large frozen weight block plus the two skinny trainable LoRA strips, with the count annotation making the size gap concrete.

- **Title (bold 15px, `#1a5276`, top center):** "One Layer: a Frozen Million Plus a Trainable Sliver".
- **Frozen block:** rectangle at x=90, y=85, size 190×175, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; three centered 13px `#2a78d6` lines inside: "frozen weights W" / "1000 × 1000" / "1,000,000 numbers"; bold 11px `#2a78d6` tag "FROZEN" in the block's top-left corner.
- **Plus sign:** bold 26px `#2c3e50` "+" at (322, 180).
- **Strip A:** tall thin rectangle at x=368, y=85, size 18×175, fill `rgba(0,131,0,0.25)`, 2px `#008300` border; 12px `#008300` label below at y=278: "A — 1000×8".
- **Times sign:** bold 20px `#2c3e50` "×" at (412, 180).
- **Strip B:** wide short rectangle at x=440, y=164, size 190×18, fill `rgba(0,131,0,0.25)`, 2px `#008300` border; 12px `#008300` label below at y=200: "B — 8×1000".
- **Trainable tag:** bold 11px `#008300` "TRAINABLE" centered above the two strips at y=75.
- **Annotation (bold 12px orange `#d95926`, near x=440, y=235, two lines):** "sliver = 8,000 + 8,000 = 16,000 numbers" / "1.6% of the frozen block".
- **Caption (12px `#444`, bottom right):** "illustrative layer — the same freeze-plus-sliver pattern repeats in every layer".

## Counting the Knobs: 1,000,000 Frozen, 16,000 Trained

**Tags:** `worked example` (blue), `rank` (green)

- **One block** — take one weight block of the model: 1000 wide × 1000 tall = 1,000,000 numbers
- **Two strips** — LoRA adds A (1000×8) and B (8×1000): 8,000 + 8,000 = 16,000 trainable numbers
- **The share** — 16,000 / 1,000,000 = 1.6%; that sliver is everything training is allowed to change
- **The rank** — the strip width 8 is called the rank: rank 2 gives 4,000 numbers, rank 16 gives 32,000
- **By hand** — trainable count = rank × (width + height); for a square block that is 2 × width × rank

*Example (italic):* For a 1000×1000 block at rank 8: 1000×8 + 8×1000 = 16,000 trainable numbers — 1.6% of the million sitting frozen in the block.

**Key point:** Trainable count = rank × (width + height) — for rank 8 on a square 1000-wide block that is 16,000 numbers, and the other 984,000 never move.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart on a shared count axis: full fine-tuning's bar spans the plot while the LoRA rank bars are near-invisible slivers — the widths themselves are the lesson.

- **Title (bold 15px, `#1a5276`, top center):** "Trainable Numbers in One 1000×1000 Block: Full vs LoRA".
- **Axis:** horizontal 2px `#999` line at y=250 from x=210 to x=690 (width 480), scale 0 to 1,000,000; 12px `#444` tick labels "0", "250k", "500k", "750k", "1,000k" every 250,000.
- **Rows (bars 22px tall, centered at y = 85, 130, 175, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "full fine-tune": bar width 480px (1,000,000), fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; bold 12px white label "1,000,000" inside near the right end.
  - "LoRA rank 16": bar width 15px (32,000), fill `rgba(0,131,0,0.35)`, 1px `#008300` border; bold 12px `#008300` label "32,000" to the right.
  - "LoRA rank 8": bar width 8px (16,000), same green style; bold 12px `#008300` label "16,000" to the right.
  - "LoRA rank 2": bar width 2px (4,000), same green style; bold 12px `#008300` label "4,000" to the right.
- **Annotation (bold 13px green `#008300`, near x=420, y=155):** "rank 8 trains 16,000 numbers — 1.6% of the block".
- **Caption (12px `#444`, bottom right):** "counts follow directly from the matrix sizes — arithmetic, not estimates".

## One Base Model, a Shelf of Tiny Adapters

**Tags:** `where it's used` (blue), `adapters` (green), `serving cost` (orange)

- **Sharing** — a bakery, a clinic, and a law firm can all run on the same frozen base model
- **Full copies** — full fine-tuning gives each client its own 14 GB model, so 3 clients cost 42 GB
- **Adapter files** — LoRA saves only the sliver: about 20 MB per client, 700× smaller than a copy
- **Hot swap** — load the 14 GB base once, then swap the 20 MB adapter per request on one machine
- **Cheaper training** — with 60× fewer numbers to update, training fits on far smaller hardware

*Example (italic):* Three custom bots the full way need 42 GB of copies; with LoRA it is one 14 GB base plus three 20 MB adapters — about 14.1 GB in total.

**Key point:** Because the base is frozen and shared, each new client costs a 20 MB adapter file instead of a 14 GB model copy — that is what makes many custom bots affordable.

### Visualization (canvas `c3`, 720×300)

Fan-out diagram: one shared base-model box on the left feeding three small adapter boxes on the right, with the storage comparison as the annotation.

- **Title (bold 15px, `#1a5276`, top center):** "One Shared 14 GB Base, Three Swappable 20 MB Adapters".
- **Base box:** rectangle at x=70, y=105, size 200×110, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; three centered 13px `#2a78d6` lines: "base model" / "14 GB" / "loaded once, frozen".
- **Adapter boxes (each 190×42, at x=440, y = 70 / 130 / 190):** fill `rgba(0,131,0,0.15)`, 2px `#008300` border; centered 12px `#008300` text: "bakery adapter — 20 MB", "clinic adapter — 20 MB", "law-firm adapter — 20 MB".
- **Arrows:** 2px `#6b7280` lines with small arrowheads from the base box's right edge (270, 160) to each adapter box's left edge midpoint (440, 91 / 151 / 211).
- **Annotation (bold 13px orange `#d95926`, centered near x=360, y=268):** "3 full copies: 42 GB — one base + 3 adapters: about 14.1 GB".
- **Caption (12px `#444`, bottom right):** "illustrative sizes for a mid-size model".

## Cranking the Rank Doesn't Keep Paying

**Tags:** `common mistake` (red), `diminishing returns` (orange)

- **The hope** — "if rank 8 works, rank 64 must work eight times better" — so teams crank the dial
- **The plateau** — on a tone-matching test (illustrative), rank 1 scores 78, rank 8 scores 90, rank 64 only 91
- **Full fine-tune** — retraining everything scores 92 here: about 60× the trainable numbers for 2 points
- **Style, not facts** — adapters excel at tone, format, and task habits; fresh facts belong in the prompt
- **Right dial** — start small (rank 4–16) and raise the rank only while the score is still climbing

*Example (italic):* The bakery's bot at rank 8 already matches the shop's voice at 90; quadrupling the sliver to rank 32 buys a single extra point.

**Common mistake:** Treating rank as a quality dial that always pays. Past a small rank the score flattens, so extra rank buys cost, not quality — and no rank makes an adapter a substitute for putting new facts in the prompt.

### Visualization (canvas `c4`, 720×300)

Single-panel line chart: tone-matching score against LoRA rank, flattening after rank 8, with the full fine-tune score as a dashed ceiling just above the plateau.

- **Title (bold 15px, `#1a5276`, top center):** "Tone-Matching Score vs LoRA Rank — the Plateau (illustrative)".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 175; x = seven evenly spaced category ticks for ranks `[1, 2, 4, 8, 16, 32, 64]` with 12px `#444` labels; y = score 70 to 95 with light `#e5e9ef` gridlines and 12px `#444` labels at 75, 80, 85, 90.
- **LoRA curve:** green `#008300` 3px line with 5px dots at scores `[78, 84, 88, 90, 91, 91, 91]` over the seven rank ticks.
- **Full fine-tune line:** horizontal dashed `#6b7280` (dash 4/3) line at score 92 across the plot; 12px `#6b7280` label at its right end: "full fine-tune: 92".
- **Rank-8 marker:** vertical dashed blue `#2a78d6` (dash 4/3) line at the rank-8 tick from baseline up to its dot; bold 13px blue label beside the dot: "rank 8: 90".
- **Annotation (bold 12px magenta `#d55181`, near x=430, y=110, two lines):** "within 2 points of full fine-tuning" / "at roughly 1/60th the trainable numbers".
- **Caption (12px `#444`, bottom right):** "illustrative scores — the plateau shape is the lesson".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar widths, box positions, scores, and counts are the hardcoded values above (no randomness); parameter counts are exact arithmetic from the stated matrix sizes; storage sizes and quality scores are invented and carry "illustrative" captions; every number in the text matches its chart.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
