# The KV Cache

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The KV Cache

**Subtitle:** A model writing word by word files two index cards for every word it has already seen — the KV cache — so each new word checks the card box instead of rereading the whole conversation from the start

## A Card Box Beside the Scribe

**Tags:** `core idea` (blue), `memoization` (green), `attention` (orange)

- **The scribe** — a scribe writes a story one word at a time, and every new word must fit all the words before it
- **The slow way** — with no notes, choosing each new word means rereading the entire story from word one
- **Two cards per word** — instead, each written word gets two index cards: a "look me up by" card (K) and a "what I carry" card (V)
- **The card box** — to pick the next word, the scribe scans the box of cards once — no rereading, ever
- **That's the cache** — attention computes a key and a value for every token; the KV cache just refuses to throw them away

*Example (italic):* After writing "Once upon a time there was a dog", the scribe holds 8 words and 16 cards — the ninth word is chosen by checking those cards, not by rereading the sentence.

**Key point:** The KV cache is memoization for attention — each token's key and value are computed once, filed, and reused for every later word.

### Visualization (canvas `c1`, 720×300)

Two-lane schematic contrasting the same step — adding one new word to an 8-word story — without a cache (arrows sweep back over every earlier word) and with a cache (one lookup into a card box).

- **Title (bold 15px, `#1a5276`, top center):** "One New Word: Reread Everything vs Check the Card Box".
- **Lanes:** two horizontal lanes centered at y=110 and y=230; left-aligned 12px `#444` lane labels at x=20: "no cache" and "KV cache".
- **Token squares (both lanes):** 8 squares 24×24, fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border, left edges at x = `[130, 162, 194, 226, 258, 290, 322, 354]`, vertically centered on the lane; a 9th "new word" square at x=400, fill `rgba(217,89,38,0.18)`, 1.5px `#d95926` border, 11px `#d95926` label "new" beneath it.
- **No-cache lane (top):** 8 thin 1.5px `#d95926` arcs from the new square's top back to the top of each of the 8 earlier squares (arc peaks stepping up from y=88 to y=60); bold 12px `#d95926` two-line annotation at x=480, y=90: "8 rereads" / "for 1 new word".
- **Cache lane (bottom):** card box drawn at x=470–580 as a 3px `#199e70` rounded rectangle (height 56) holding 5 overlapping card shapes (14×20, fill `rgba(25,158,112,0.25)`, offset 8px apart), 11px `#199e70` label "card box (K, V pairs)" beneath; one 2.5px `#008300` arrow from the new square to the box with arrowhead; bold 12px `#008300` two-line annotation at x=600, y=215: "1 lookup +" / "file 2 new cards".
- **Caption (12px `#444`, bottom right):** "illustrative — an 8-word story, one word being added".

## Ten New Words After a 100-Word Story

**Tags:** `worked example` (blue), `counting the work` (green)

- **The setup** — the scribe gets a 100-word prompt, then must write 10 more words one at a time
- **Prefill** — the first read of the 100-word prompt files 100 pairs of cards; that cost is paid once
- **No cache** — word 1 rereads 101 words, word 2 rereads 102, ... word 10 rereads 110: 1,055 in total
- **With cache** — each new word costs 1 pass plus the one-time 100: just 110 token-passes in total
- **The ratio** — 1,055 vs 110 is nearly 10× less work, and the gap widens as the story grows

*Example (italic):* Adding the numbers by hand: 101+102+...+110 = 1,055 without a cache, against 100+10 = 110 with one.

**Key point:** Without a cache the work per word grows with everything written so far; with the cache each new word costs one pass — 1,055 vs 110 token-passes here.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart over the 10 generation steps: for each step, an orange "no cache" bar (rereads that step) next to a green "with cache" bar (always 1), with the two grand totals called out.

- **Title (bold 15px, `#1a5276`, top center):** "Work per New Word: 100-Word Prompt, Then 10 Words".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; y axis = token-passes 0 to 120 with light `#e5e9ef` gridlines at 30, 60, 90, 120 and 12px `#444` tick labels; x axis = 10 groups labeled "1"–"10" (12px `#444`) under group centers, 12px `#444` axis label "new word #" centered below.
- **Groups:** 10 groups of width 60 starting at x=60; in each group an orange bar (width 20, fill `rgba(217,89,38,0.55)`, 1px `#d95926` border) and a green bar (width 20, fill `#008300`) with a 4px gap.
- **No-cache values (orange):** `[101, 102, 103, 104, 105, 106, 107, 108, 109, 110]`.
- **With-cache values (green):** `[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]` — at this scale each green bar is a ~2px sliver, which is the point.
- **Legend (12px, top left inside plot at x=75, y=75):** orange swatch "no cache", green swatch "with KV cache".
- **Annotation (bold 13px `#008300`, centered near x=360, y=55):** "totals: 1,055 vs 110 token-passes — nearly 10× less work".
- **Caption (12px `#444`, bottom right):** "illustrative — cache total counts the one-time 100-word prefill".

## Every Token Rents Memory

**Tags:** `where it's used` (blue), `memory cost` (orange), `long context` (green)

- **Cards aren't free** — every filed card is numbers sitting in GPU memory for the whole conversation
- **Per-token bill** — 24 layers × 16 heads × 64 numbers, twice (K and V), at 2 bytes each = 96 KB per token
- **It only grows** — 1,000 tokens hold 96 MB of cards; 4,000 hold 384 MB; 32,000 hold 3,072 MB (~3 GB)
- **Context as a line item** — long-context pricing and chat length limits are largely this card box's rent
- **The trade** — the cache spends memory to save compute; serving systems budget it per conversation

*Example (italic):* A 32,000-token chat on this illustrative model carries a 3 GB card box before generating a single new word — that memory is reserved as long as the chat is open.

**Key point:** KV memory grows linearly with context — 96 KB per token here — so doubling the conversation doubles the rent, which is why long context costs real money.

### Visualization (canvas `c3`, 720×300)

Bar chart of KV-cache size against context length: six blue bars doubling from 1,000 to 32,000 tokens, each labeled with its megabytes, showing the linear climb to gigabytes.

- **Title (bold 15px, `#1a5276`, top center):** "The Card Box's Rent: KV Memory vs Context Length".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y axis = MB 0 to 3,200 with light `#e5e9ef` gridlines at 800, 1,600, 2,400, 3,200 and 12px `#444` tick labels ("0", "800", "1600", "2400", "3200 MB"); x axis = six bars labeled "1k", "2k", "4k", "8k", "16k", "32k" tokens (12px `#444`), 12px `#444` axis label "context length (tokens)" centered below.
- **Bars:** width 58, evenly spaced (centers at x = 120, 218, 316, 414, 512, 610), fill `rgba(42,120,214,0.55)`, 1.5px `#2a78d6` border.
- **Values (MB):** `[96, 192, 384, 768, 1536, 3072]` — bold 12px `#2a78d6` value label above each bar ("96", "192", "384", "768", "1536", "3072").
- **Annotation (bold 13px `#d95926`, two lines near x=200, y=85):** "double the context," / "double the memory — 3 GB at 32k".
- **Caption (12px `#444`, bottom right):** "illustrative — 24 layers, 16 heads, 64 dims, fp16: 96 KB per token".

## Scratch Paper, Not Learning

**Tags:** `common mistake` (red), `cache vs weights` (orange)

- **Two memories** — the weights are what the model learned in training; the cache is scratch paper for one chat
- **It vanishes** — close the conversation and the card box is discarded; the weights don't change at all
- **Not saving memory** — a cache usually means "smaller"; here it means "extra memory spent to save compute"
- **It can outgrow the model** — at 128k tokens this cache is 12.3 GB, close to the 14 GB of weights themselves
- **Same words, same cards** — the cache holds nothing new; only exactly what recomputing would rebuild

*Example (italic):* A user says "the model remembered my name from yesterday" — it didn't; yesterday's card box was thrown away, and only text pasted back into today's context is seen again.

**Common mistake:** Treating the KV cache as the model's memory of facts. It is per-conversation scratch paper that dies with the session — persistence has to come from re-sending text, not from the cache.

### Visualization (canvas `c4`, 720×300)

Bar chart comparing the growing KV cache (three aqua bars at 4k, 32k, 128k tokens) against a fixed dashed line for the model's weights, showing the scratch paper nearly outweighing the model.

- **Title (bold 15px, `#1a5276`, top center):** "Scratch Paper vs the Model: KV Cache Can Rival the Weights".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y axis = GB 0 to 16 with light `#e5e9ef` gridlines at 4, 8, 12, 16 and 12px `#444` tick labels ("0", "4", "8", "12", "16 GB"); x axis = three bars labeled "4k tokens", "32k tokens", "128k tokens" (12px `#444`).
- **Bars:** width 90, centers at x = 190, 365, 540, fill `rgba(25,158,112,0.5)`, 1.5px `#199e70` border.
- **Values (GB):** `[0.4, 3.1, 12.3]` — bold 12px `#199e70` value label above each bar ("0.4 GB", "3.1 GB", "12.3 GB").
- **Weights line:** horizontal dashed `#4a3aa7` (dash 6/4) 2px line at 14 GB across the plot; bold 12px `#4a3aa7` label above its left end: "model weights: 14 GB (fixed)".
- **Annotation (bold 13px `#d95926`, two lines near x=430, y=95):** "at 128k tokens the card box" / "nearly outweighs the model".
- **Caption (12px `#444`, bottom right):** "illustrative — same 96 KB-per-token model, weights of a 7B fp16 model".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, arc endpoints, and box coordinates are the hardcoded arrays and pixel values above (no randomness); c2 totals 1,055 and 110 are the exact sums 101+...+110 and 100+10; c3/c4 memory figures follow from 96 KB per token (24 × 16 × 64 × 2 values × 2 bytes) and are labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
