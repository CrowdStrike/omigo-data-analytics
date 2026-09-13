# From HMMs to Transformers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** From HMMs to Transformers

**Subtitle:** Fifty years of next-word guessers, told through one text message — each new model exists to fix the exact way the previous one forgot

## A Phone That Remembers One Word

**Tags:** `core idea` (blue), `hidden states` (green), `one-step memory` (orange)

- **The text** — you type "grab your swimsuit and meet me at the" and the keyboard must guess the next word
- **The old guesser** — an HMM walks the sentence one word at a time, carrying a single hidden note
- **The hidden note** — a rough label like "a place noun is coming", rewritten after every word
- **One-step memory** — each guess uses only the last word plus that note; nothing older survives
- **The crack** — "swimsuit", six words back, is the whole clue, and the note lost it long ago

*Example (italic):* Any human reading "grab your swimsuit and meet me at the" says "pool" instantly — the HMM sees only "the" and one worn-out note.

**Key point:** An HMM predicts each word from a single hidden state that only remembers one step back — a chain of local guesses that forgets how the sentence began.

### Visualization (canvas `c1`, 720×300)

Single-panel chain diagram: the nine tokens of the message as boxes along the bottom, hidden-state circles above them linked left-to-right, with a one-step memory bracket and the lost "swimsuit" clue highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "The HMM's View: One Hidden Note, One Step Back".
- **Word boxes:** nine tokens `["grab", "your", "swimsuit", "and", "meet", "me", "at", "the", "?"]` as 68×30 rounded boxes centered at x = `[58, 133, 208, 283, 358, 433, 508, 583, 658]`, y=215; fill `#f8f9fa`, 1px `#e5e9ef` border, 12px `#2c3e50` labels; the "swimsuit" box gets orange `#d95926` 2px border and bold orange label; the "?" box gets blue `#2a78d6` 2px border and bold 14px blue "?".
- **Hidden-state circles:** eight 14px-radius circles at the same x positions (58–583), y=120; fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` stroke, 11px `#2a78d6` label "s1"…"s8" inside; 2px `#2a78d6` arrows circle-to-circle left to right, and thin 1px `#6b7280` arrows from each circle down to its word box.
- **Memory bracket:** bold 2px `#1a5276` bracket under the last transition (x 508–583, y=155), bold 12px `#1a5276` label below: "memory window: 1 step".
- **Lost clue:** dashed orange `#d95926` (dash 4/3) arc from the "swimsuit" box up and over to the "?" box (arcing clear above the hidden-state circles), crossed by a bold 13px orange annotation near x=400, y=62: "the clue is 6 words back — the chain already forgot it".
- **Caption (12px `#444`, bottom right):** "illustrative — a next-word HMM on one text message".

## Counting What Follows "at the"

**Tags:** `worked example` (blue), `n-gram counts` (green), `attention` (orange)

- **Old texts** — across past messages, the phrase "at the" appeared 100 times
- **The counts** — it was followed by "station" 40 times, "movie" 33, "pool" 15, "gym" 12
- **N-gram counter's pick** — 40/100 = 40% for "station"; "pool" gets only 15/100 = 15%, so it loses
- **Attention's trick** — before guessing, weigh every earlier word; "swimsuit" gets weight 0.62
- **New pick** — with "swimsuit" in view the guess flips: "pool" 78%, "station" drops to 9%

*Example (italic):* Same sentence, same history — counting alone says "station" at 40%; letting the model stare back at "swimsuit" says "pool" at 78%.

**Key point:** 40/100 = 40% is the right guess for the average sentence; attention fixes it for this sentence by looking back and reweighing "swimsuit".

### Visualization (canvas `c2`, 720×300)

Two side-by-side bar panels on one canvas: left panel shows the raw next-word counts an HMM uses, right panel shows the probabilities after attention has weighed "swimsuit" — same four candidate words, flipped winner.

- **Title (bold 15px, `#1a5276`, top center):** "Same History, Two Guessers: Counts vs Attention".
- **Left panel ("counting only — n-gram", bold 13px `#2c3e50` header at x=185 centered):** baseline y=245, four vertical bars 52px wide at x = `[70, 140, 210, 280]` for `["station", "movie", "pool", "gym"]` with heights scaled to values `[40, 33, 15, 12]` (percent, y-scale 0–80 over 170px); fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` top edge; bold 12px `#2a78d6` value labels "40%", "33%", "15%", "12%" above each bar; 12px `#444` word labels below.
- **Right panel ("attention sees 'swimsuit'", bold 13px `#2c3e50` header at x=535 centered):** same baseline and y-scale, bars 52px wide at x = `[420, 490, 560, 630]` for values `[9, 8, 78, 5]` in the same word order; "pool" bar fill `rgba(0,131,0,0.35)` with 2px `#008300` top edge and bold 13px green "78%" label, the other three fill `rgba(107,114,128,0.25)` with 12px `#6b7280` labels "9%", "8%", "5%".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=55 to y=260.
- **Annotation (bold 12px orange `#d95926`, near x=535, y=70):** "weight 0.62 on 'swimsuit' flips the answer".
- **Caption (12px `#444`, bottom right):** "counts hand-checkable (40/100 = 40%); attention side illustrative".

## Fifty Years, Four Fixes

**Tags:** `where it's used` (blue), `lineage` (green), `what each step fixed` (orange)

- **HMM (1970s)** — hand-sized hidden states, one-step memory; won speech recognition, forgot sentences
- **RNN (1990s)** — the note becomes a learned vector but gradients fade; reach ~8 words (illustrative)
- **LSTM (1997)** — gates choose what to keep and drop, stretching reach to ~60 words (illustrative)
- **Attention (2015)** — stop squeezing everything into one vector; look back at any word directly
- **Transformer (2017)** — drop the chain entirely: attention only, so the whole message is in reach
- **Where you meet it** — autocomplete, translation, and search ranking all run on this last step today

*Example (italic):* On the swimsuit text, an HMM sees 1 word back, an RNN about 8, an LSTM about 60, and a transformer reaches "swimsuit" at any distance.

**Key point:** Each step attacked one bottleneck — hand-built states, fading memory, the single-vector squeeze, and finally the chain itself.

### Visualization (canvas `c3`, 720×300)

Horizontal bar ladder: four rows (HMM, RNN, LSTM, Transformer) whose bar lengths show how far back each model can usefully look, drawn on a compressed log-style width scale so all four fit, each row carrying its one-line fix.

- **Title (bold 15px, `#1a5276`, top center):** "How Far Back Each Guesser Can Usefully Look".
- **Rows (top to bottom at y = 80, 130, 180, 230), each with a left-aligned bold 12px `#1a5276` model label at x=20 and the bar starting at x=150:**
  - "HMM (1970s)": bar width 24px, fill `rgba(107,114,128,0.35)`, 12px `#444` label at bar end "1 word — one-step chain"
  - "RNN (1990s)": bar width 90px, fill `rgba(42,120,214,0.35)`, label "~8 words — memory fades"
  - "LSTM (1997)": bar width 220px, fill `rgba(42,120,214,0.35)`, label "~60 words — gates keep the clue"
  - "Transformer (2017)": bar width 520px, fill `rgba(0,131,0,0.35)` with 2px `#008300` edge, bold 12px `#008300` label "the whole message (1,000+ words)"
- **Bar style:** 18px-tall rounded bars; widths are on a compressed log-style scale, not linear.
- **Fix ticks:** between consecutive rows, an 11px `#6b7280` italic note at x=150: "fix: learn the state" (between rows 1–2), "fix: gate the memory" (2–3), "fix: drop the chain, attend to everything" (3–4).
- **Annotation (bold 13px violet `#4a3aa7`, near x=430, y=62):** "each step fixed how the last one forgot".
- **Caption (12px `#444`, bottom right):** "reach numbers illustrative — widths log-style, not to linear scale".

## Transformers Do Not Read Left to Right

**Tags:** `common mistake` (red), `position tags` (orange)

- **The habit** — we read "grab your swimsuit…" left to right, so we assume the model does too
- **All at once** — a transformer takes in every word simultaneously; word 3 and word 8 arrive together
- **Position tags** — each word carries its position number (1st, 2nd, 3rd…) so order is not lost
- **Attention is the reading** — for the blank, the weights decide: "swimsuit" 0.62, "the" 0.10, "at" 0.08
- **The payoff** — no chain means training runs on all words in parallel; that is why scaling got cheap

*Example (italic):* Feed the words in shuffled order but keep their position tags and the transformer answers the same — order lives in the tags, not in arrival time.

**Common mistake:** Thinking attention means the model reads like a person. It sees the whole message at once; the weights, not the word order, decide what counts.

### Visualization (canvas `c4`, 720×300)

Attention fan diagram: the eight typed words in a row with position tags, the "?" slot at the right, and one arc from "?" to every word whose thickness and label show its attention weight — "swimsuit" visibly dominant.

- **Title (bold 15px, `#1a5276`, top center):** "One Guess, Eight Arcs: Attention Weights for the Blank".
- **Word boxes:** eight tokens `["grab", "your", "swimsuit", "and", "meet", "me", "at", "the"]` as 62×28 rounded boxes centered at x = `[55, 128, 201, 274, 347, 420, 493, 566]`, y=230; fill `#f8f9fa`, 1px `#e5e9ef` border, 12px `#2c3e50` labels; an 11px `#6b7280` position tag "1"…"8" under each box; the "?" slot at x=655, y=230, 2px `#2a78d6` border, bold 14px blue "?" with tag "9".
- **Arcs:** quadratic arcs from the top of the "?" slot to the top of each word box, weights `[0.05, 0.03, 0.62, 0.02, 0.06, 0.04, 0.08, 0.10]` (sum 1.00); line width = 1 + weight×10 px; the "swimsuit" arc green `#008300`, all others `#6b7280` at 60% alpha; 11px weight labels ("0.05", "0.03", …) on each arc apex, the "swimsuit" label bold 13px `#008300` "0.62".
- **Arc heights:** apexes staggered between y=70 and y=150 (farthest word highest) so labels never overlap.
- **Annotation (bold 13px magenta `#d55181`, near x=200, y=45):** "0.62 of the attention goes 6 words back — no chain to fade".
- **Caption (12px `#444`, bottom right):** "weights illustrative — they sum to 1.00".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all tokens, counts, bar values, reach widths, and attention weights are the hardcoded arrays above (no randomness); the worked-example counts 40/33/15/12 out of 100 and the weight list summing to 1.00 must match between text and charts; invented numbers keep their "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
