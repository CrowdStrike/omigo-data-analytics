# Transformers & Attention

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Transformers & Attention

**Subtitle:** Instead of reading in order, every word looks at every other word and decides which ones matter most for its own meaning

## Which "Bank"? Let the Word Ask the Sentence

**Tags:** `core idea` (blue), `running example` (green), `context` (orange)

- **The sentence** — "She sat on the bank of the river and watched the water"
- **The puzzle** — "bank" alone could mean money or riverside; context must decide
- **The move** — "bank" scores every other word: how relevant are you to me?
- **The winners** — "river" and "water" score high; "sat" and "the" score low
- **The result** — "bank" rebuilds its meaning as a mix weighted by those scores: riverside
- **All at once** — every word does this simultaneously, not just "bank"

*Example (italic):* In "she checked her bank balance", the same word would attend to "balance" and mean money.

**Key point:** Attention lets each word pull in exactly the context it needs, no matter where in the sentence that context sits.

### Visualization (canvas `c1`, 720×300)

Sentence with attention arcs from "bank" to other words; arc thickness encodes attention weight.

- **Title (bold 15px `#1a5276`, top center):** ""bank" Looks at Every Word — Line Thickness = How Much It Matters".
- **Sentence (at y=210, evenly spaced x from 55 to 665):** She sat on the bank of the river and watched the water. "bank" (index 4) drawn bold 14px magenta `#d55181`; "river" (index 7) and "water" (index 11) bold 14px orange `#d95926`; all other words 13px `#2c3e50`.
- **Arcs (quadratic curves from "bank", height proportional to span):** to She weight 0.10, sat 0.10, the(idx 3) 0.05, river 0.45, water 0.30. Arcs with weight ≥ 0.3 stroked orange `#d95926`, others `rgba(107,114,128,0.55)`; line width = 1 + weight × 14. Each arc labeled with its weight ("0.45", "0.30", "0.10", "0.10", "0.05") at the arc apex — bold 12px orange for big arcs, 11px `#6b7280` otherwise.
- **Bottom annotations (centered):** bold 13px orange `#d95926`: ""river" (0.45) and "water" (0.30) decide: riverside, not money"; 12px `#6b7280`: "every other word runs the same look-around at the same time".

## Scoring the Sentence, Worked to the Last Decimal

**Tags:** `worked example` (green), `by hand` (blue)

- **Raw scores** — "bank" rates five words: river 9, water 6, she 2, sat 2, the 1
- **The total** — 9 + 6 + 2 + 2 + 1 = 20
- **Normalize** — divide each by 20: 0.45, 0.30, 0.10, 0.10, 0.05
- **Sanity check** — 0.45 + 0.30 + 0.10 + 0.10 + 0.05 = 1.00, a proper share-out
- **The blend** — new "bank" = 45% river-meaning + 30% water-meaning + 25% the rest
- **Real models** — same idea with a softmax instead of plain division, and learned scores

*Example (italic):* Redo it on paper: five ratings, one division each — that is one attention computation.

**Key point:** Attention weights are just relevance scores rescaled to sum to 1 — then used to mix the other words' meanings.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of the five attention weights with the division written beside each bar.

- **Title (bold 15px `#1a5276`, top center):** "Raw Scores ÷ Total = Attention Weights (sum = 1.00)".
- **Rows (bars start at x=120, scaled weight/0.5 × 340px, 24px tall, 36px row pitch from y=62):**
  - river — raw 9, weight 0.45, orange `#d95926`
  - water — raw 6, weight 0.30, orange `#d95926`
  - she — raw 2, weight 0.10, blue `#2a78d6`
  - sat — raw 2, weight 0.10, blue `#2a78d6`
  - the — raw 1, weight 0.05, blue `#2a78d6`
- **Bar style:** fill at 0.6 alpha of the row color, 1.5px solid stroke; word right-aligned before the bar (bold 13px `#2c3e50`); after each bar (bold 12px, row color): "9 / 20 = 0.45", "6 / 20 = 0.30", "2 / 20 = 0.10", "2 / 20 = 0.10", "1 / 20 = 0.05".
- **Bracket:** orange bracket to the right spanning the river+water rows, labeled (bold 13px orange, two lines): "0.75 of the" / "meaning mix".
- **Bottom annotations (centered):** bold 13px magenta `#d55181`: "new "bank" = 0.45·river + 0.30·water + 0.10·she + 0.10·sat + 0.05·the"; 12px `#6b7280`: "scores are illustrative; real models learn them and use softmax to rescale".

## Why It Replaced Reading in Order

**Tags:** `where it's used` (blue), `language models` (green)

- **The relay problem** — an RNN passes a memory word by word; "bank" to "water" takes 7 handoffs
- **The direct line** — attention connects "bank" to "water" in exactly 1 hop, any distance
- **No fading** — page-one context reaches page ten as easily as the previous word
- **Parallel** — all words are processed at once, so GPUs can chew huge texts fast
- **The payoff** — translation, chatbots, code assistants, search are all transformers now

*Example (italic):* Every modern large language model is a stack of these attention layers — that is the "transformer".

**Key point:** One hop between any two words, computed in parallel — that pair of properties is why transformers took over language.

### Visualization (canvas `c3`, 720×300)

Two rows of the same sentence contrasting the RNN relay with a single attention hop.

- **Title (bold 15px `#1a5276`, top center):** "From "bank" to "water": 7 Handoffs vs 1 Hop".
- **Sentence (both rows, same 12-word sentence, evenly spaced x from 55 to 665):** "bank" (index 4) and "water" (index 11) are the key words, drawn bold 13px in the row color; other words 12px `#6b7280`.
- **RNN row (y=105, key color blue `#2a78d6`):** chained short arrows (line + filled head) hopping word-to-word from "bank" through to "water" (7 segments); label above (bold 13px blue, left-aligned at x=55): "RNN relay: 7 handoffs, memory fades at each one".
- **Attention row (y=220, key color orange `#d95926`):** a single 3px orange quadratic arc from "bank" directly to "water" with an arrowhead; label above (bold 13px orange, left-aligned at x=55): "attention: 1 hop, same cost at any distance".
- **Bottom annotation (bold 13px magenta `#d55181`, centered):** "1 hop between any pair + all pairs computed in parallel = the transformer advantage".

## The Confusion: It's a Weighted Average, Not Understanding

**Tags:** `common mistake` (red), `multiple heads` (orange)

- **The mistake** — reading "attention" as the model consciously focusing, like a person
- **Reality** — it is arithmetic: learned scores, a rescale, a weighted average of vectors
- **Many heads** — each layer runs several attention patterns side by side, called heads
- **Different jobs** — one head may track meaning words, another nearby grammar words
- **Read with care** — attention maps hint at what the model uses, but they are not explanations

*Example (italic):* For the same word "bank", one head leans on "river" and "water" while another watches "sat" and "she".

**Key point:** Nothing mystical happens — several weighted averages run in parallel, and training decides what each one weighs.

### Visualization (canvas `c4`, 720×300)

Two side-by-side horizontal bar panels showing two attention heads weighing the same five words differently; dashed vertical divider at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px `#1a5276`, top center):** "Two Heads Weigh the Same Word Differently (illustrative)".
- **Words (row labels, both panels):** she, sat, the, river, water.
- **Left panel** (x0=20, orange `#d95926`), title (bold 13px): "head 1: what kind of "bank"?". Weights: she 0.10, sat 0.10, the 0.05, river 0.45, water 0.30.
- **Right panel** (x0=380, violet `#4a3aa7`), title: "head 2: who is doing what?". Weights: she 0.35, sat 0.40, the 0.15, river 0.05, water 0.05.
- **Bar style:** weight/0.5 × 170px wide, 20px tall, 32px row pitch from y=74; fill at 0.6 alpha of the panel color; weight value after each bar (bold 12px, panel color); word right-aligned before the bar (12px `#2c3e50`).
- **Bottom annotation (bold 13px magenta `#d55181`, centered):** "same word, same sentence — each head is its own weighted average".

## Regeneration instructions

- **Template/layout:** tutorials topic-page skeleton. `<h1>` (no index number) + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%), both 12px padding, top-aligned.
- **Left column structure:** `.tags` row of colored pill spans (`.tag` — 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) whose `<strong>` prefix is "Key point:".
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. A shared `sentence` array `['She','sat','on','the','bank','of','the','river','and','watched','the','water']` feeds c1 and c3. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions (this page has none).
