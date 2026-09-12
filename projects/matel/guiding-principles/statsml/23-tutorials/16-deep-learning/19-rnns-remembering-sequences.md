# RNNs: Remembering Sequences

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** RNNs: Remembering Sequences

**Subtitle:** A network that reads one step at a time and carries a running memory forward, so earlier items shape what it predicts later

## Your Keyboard Reads the Sentence with a Notepad

**Tags:** `core idea` (blue), `running example` (green), `sequences` (orange)

- **The task** — phone autocomplete: you typed "I'll meet you at the" — suggest the next word
- **One at a time** — the network reads "I'll", then "meet", then "you"… never all at once
- **The notepad** — after each word it updates a small memory, called the hidden state
- **Carry forward** — that memory, plus the next word, produces the next memory
- **The payoff** — by the last word, the memory summarizes the whole sentence so far

*Example (italic):* Reading a novel, you don't reread page one for every sentence — you carry a running summary in your head.

**Key point:** An RNN is one small network applied over and over, passing its memory from each step to the next.

### Visualization (canvas `c1`, 720×300)

Unrolled-RNN diagram over the sentence "I'll meet you at the ___".

- **Title (bold 15px `#1a5276`, top center):** "One Word In per Step, One Memory Passed Along".
- **Memory cells:** five 74×46 boxes at y=120 starting at x=55 with 52px gaps, fill `rgba(42,120,214,0.15)`, stroke `#2a78d6` 2px; each labeled (bold 12px `#1a5276`, two lines): "memory" / "m1" … "m5".
- **Word inputs:** words `["I'll", 'meet', 'you', 'at', 'the']` below each box (bold 13px `#2c3e50`), with an upward aqua `#199e70` arrow from each word into its box.
- **Memory handoffs:** blue `#2a78d6` horizontal arrows between consecutive boxes.
- **Prediction:** orange `#d95926` arrow rising from the last box, labeled (bold 13px `#d95926`): "predict: "office"? "airport"?".
- **Bottom annotations (left-aligned at x=55):** bold 12px `#2a78d6`: "blue arrows: the notepad handed to the next step"; bold 13px magenta `#d55181`: "the same small network runs 5 times — unrolled here for viewing".

## A One-Number Memory, Updated by Hand

**Tags:** `worked example` (green), `by hand` (blue)

- **Toy rule** — keep one memory number m; each step: new m = 0.5 × old m + 0.5 × input
- **The inputs** — each word carries a "sports-topic" score: watch=4, the=2, cup=6, final=8
- **Step 1** — m = 0.5×0 + 0.5×4 = 2
- **Step 2** — m = 0.5×2 + 0.5×2 = 2, then step 3: 0.5×2 + 0.5×6 = 4
- **Step 4** — m = 0.5×4 + 0.5×8 = 6: the memory now says "strongly about sports"
- **Real RNNs** — same recipe, but the memory is hundreds of numbers and the rule is learned

*Example (italic):* After "watch the cup final", memory 6 makes the keyboard suggest "match" — not "invoice".

**Key point:** Every step is the same tiny formula — old memory blended with new input — repeated down the sequence.

### Visualization (canvas `c2`, 720×300)

Worked memory-update chain with the arithmetic shown above each cell.

- **Title (bold 15px `#1a5276`, top center):** "Toy Memory: new m = 0.5 × old m + 0.5 × input".
- **Start state:** "m = 0" (bold 12px `#6b7280`) at far left with a blue arrow into the first cell.
- **Memory cells:** four 96×50 boxes at y=104 starting at x=60 with 60px gaps, fill `rgba(42,120,214,0.15)`, stroke `#2a78d6` 2px; each holds a value (bold 16px `#1a5276`): "m = 2", "m = 2", "m = 4", "m = 6".
- **Calculations above cells (bold 12px magenta `#d55181`):** "0.5·0 + 0.5·4", "0.5·2 + 0.5·2", "0.5·2 + 0.5·6", "0.5·4 + 0.5·8".
- **Word inputs below:** aqua `#199e70` upward arrows; words (bold 13px `#2c3e50`): ""watch"", ""the"", ""cup"", ""final""; scores below each (12px `#199e70`): "score 4", "score 2", "score 6", "score 8".
- **Handoffs:** blue `#2a78d6` arrows between consecutive cells.
- **Bottom annotations (centered):** bold 13px orange `#d95926`: "memory climbs 2 → 2 → 4 → 6: the sentence turned sporty"; 12px `#6b7280`: "word scores are illustrative — real inputs are learned vectors, not single numbers".

## Order Is the Signal Autocomplete Lives On

**Tags:** `where it's used` (blue), `time series` (orange)

- **Bag of words fails** — "the dog bit the man" and "the man bit the dog" use identical words
- **Context shifts bets** — earlier words change the next-word odds; the memory carries them in
- **Our numbers** — after "meet you at the": airport 22%; after "flight lands, meet me at the": 61%
- **Same shape everywhere** — sensor readings, daily sales, heartbeats: sequences with memory
- **Without it** — a model that shuffles time away predicts tomorrow from an averaged blur

*Example (italic):* One earlier word — "flight" — nearly triples the model's bet on "airport" (illustrative numbers).

**Key point:** A data scientist reaches for recurrent models whenever the order of the data carries the information.

### Visualization (canvas `c3`, 720×300)

Two side-by-side horizontal bar panels comparing next-word probabilities under two contexts, separated by a dashed vertical divider at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px `#1a5276`, top center):** "Same Slot, Different History, Different Bets (illustrative)".
- **Candidates (both panels, right-aligned row labels):** office, airport, station, gym.
- **Left panel** (x0=30), title (bold 12px `#2c3e50`): ""I'll meet you at the ___"". Probabilities: office 31%, airport 22%, station 17%, gym 9%. Bars scaled to max 70% over 190px width, 38px row height; highlighted bar: office in violet `#4a3aa7`, others `rgba(42,120,214,0.30)`; percentage labels after each bar (bold 12px, highlight color or `#6b7280`).
- **Right panel** (x0=390), title: ""The flight lands, meet me at the ___"". Probabilities: office 8%, airport 61%, station 14%, gym 3%; highlighted bar: airport in orange `#d95926`.
- **Bottom annotations (centered):** bold 13px magenta `#d55181`: ""flight", five words back, lifts airport 22% → 61%"; 12px `#6b7280`: "the hidden state is how that earlier word reaches the prediction".

## The Catch: the Notepad Fades over Long Gaps

**Tags:** `common mistake` (red), `limitation` (orange)

- **The mistake** — assuming the memory holds the whole sequence; it is a blend, not a recording
- **Halving rule** — in our toy, each step keeps 0.5 of the past: word 1's share is 0.5ⁿ after n steps
- **Ten steps later** — 0.5¹⁰ ≈ 0.001: the opening word is nearly erased
- **Gated fixes** — LSTM and GRU cells learn what to keep and what to drop, fading slower
- **Why transformers won** — attention skips the relay entirely for very long texts

*Example (italic):* By the 40th word of a paragraph, a plain RNN has all but forgotten the name that opened it.

**Key point:** A blended memory fades geometrically — know this limit before trusting an RNN with long-range context.

### Visualization (canvas `c4`, 720×300)

Line chart of geometric memory decay 0.5ⁿ over 10 steps.

- **Title (bold 15px `#1a5276`, top center):** "Word 1's Share of the Memory, n Steps Later (0.5ⁿ)".
- **Data:** share at steps 1–10: `[0.5, 0.25, 0.125, 0.0625, 0.031, 0.016, 0.008, 0.004, 0.002, 0.001]`.
- **Axes:** padding top 56, bottom 60, left 70, right 45; y scaled to max 0.55 with gridlines (`#e5e9ef`) and right-aligned labels (12px `#6b7280`) at 0.1–0.5 in 0.1 steps; L-shaped axis lines `#999`; x labels "1"…"10" under each point (12px `#2c3e50`).
- **Series:** orange `#d95926` line, 3px, with 4px-radius orange dots at each point.
- **Annotations (bold 13px, left-aligned):** orange near step 1: "step 1: half the memory"; red `#e74c3c` near the flat tail: "step 10: ≈ 0.001 — effectively forgotten".
- **X-axis label (12px `#444`, bottom center):** "steps since word 1 was read".
- **Callout above it (bold 12px aqua `#199e70`, centered):** "LSTM gates slow this fade; attention removes the relay altogether".

## Regeneration instructions

- **Template/layout:** tutorials topic-page skeleton. `<h1>` (no index number) + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%), both 12px padding, top-aligned.
- **Left column structure:** `.tags` row of colored pill spans (`.tag` — 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) whose `<strong>` prefix is "Key point:".
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; a shared `arrow(ctx,x1,y1,x2,y2,col)` helper draws lines with filled triangular heads. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions (this page has none).
