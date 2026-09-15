# Tokens & Tokenization

**Page type:** detail page (tutorial layout: one `.card-section` per concept, each with h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Tokens &amp; Tokenization

**Subtitle:** Before a language model reads anything, your text is chopped into pieces called tokens — and every price tag and limit you'll ever hit is counted in them

## Cutting "unbelievable pricing!" Into Pieces

**Tags:** core idea (blue), running example (green)

- **The model never sees words** — text is first chopped into pieces called tokens
- **Our sentence** — "unbelievable pricing!" becomes 6 tokens: un / believ / able / pric / ing / !
- **A token is a chunk** — a run of characters that shows up often in everyday text
- **Each token gets an ID number** — the model works on those numbers, nothing else
- **The chop is fixed** — the token vocabulary (~50k–200k pieces) is set before training

*Example:* Two words and an exclamation mark walk into the tokenizer — six tokens come out.

**Key point:** A token is the unit an LLM actually reads: a common chunk of characters — not a word, and not a letter.

### Visualization (canvas `c1`, 720×300)

Three-row flow diagram: raw sentence → colored token blocks → token IDs.

- **Title (bold 15px `#1a5276`, top center):** "One Sentence, Three Views: Characters → Tokens → IDs".
- **Row 1:** the sentence "unbelievable pricing!" in 20px monospace `#2c3e50`, with 12px muted caption "what you typed — 21 characters"; a small muted downward arrow beneath.
- **Row 2 — token blocks (centered row at y=140, 40px tall, 10px gaps; each block outlined 2px in its color with 18%-alpha fill of the same color, bold 15px monospace label):**
  - "un" (52px wide) blue `#2a78d6`, ID 517
  - "believ" (92px) green `#008300`, ID 2842
  - "able" (72px) violet `#4a3aa7`, ID 481
  - "·pric" (84px) orange `#d95926`, ID 1050
  - "ing" (58px) aqua `#199e70`, ID 278
  - "!" (36px) magenta `#d55181`, ID 0
- **Row 3:** the ID numbers in 13px monospace `#2c3e50` below each block; caption 12px muted: "token IDs — the only thing the model receives (splits and IDs illustrative)".
- **Takeaway (bold 14px orange `#d95926`, bottom center):** '2 words + 1 mark → 6 tokens — the "word" was never the unit'.

## Counting Tokens by Hand

**Tags:** worked example (green), rule of thumb (blue)

- **the → 1 token** — very common words ride through whole
- **run → 1, running → 2** — add an ending, gain a token: run + ning
- **pricing → 2** (pric + ing) and **unbelievable → 3** (un + believ + able)
- **kombucha → 3** (kom + buch + a) — rare words shatter into small pieces
- **Rule of thumb** — English averages about 4 characters, or 3/4 of a word, per token
- **Check it** — "unbelievable pricing!" is 21 characters; 21 ÷ 4 ≈ 5, close to the real 6

*Example:* A 3,000-word report is roughly 4,000 tokens — multiply words by 4/3 for a quick estimate.

**Key point:** Common = one token, rare = several. Exact splits differ between models — the ones here are illustrative.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: token count per word, colored by count.

- **Title (bold 15px `#1a5276`, top center):** "Tokens per Word: Common Words Ride Whole, Rare Words Shatter".
- **Data (word, token count, split shown below in 11px muted):**
  - the — 1 — "the"
  - run — 1 — "run"
  - running — 2 — "run+ning"
  - pricing — 2 — "pric+ing"
  - unbelievable — 3 — "un+believ+able"
  - kombucha — 3 — "kom+buch+a"
- **Bar colors by count:** 1 token green `#008300`, 2 tokens blue `#2a78d6`, 3 tokens orange `#d95926`; bars 74px wide.
- **Axes:** y from 0 to 4 with integer ticks and light `#e5e9ef` gridlines; L-shaped `#999` axis; padding top 50, bottom 46, left 60, right 25; rotated y-axis label "tokens"; count bold 13px above each bar, word 12px below baseline.
- **Annotation (bold 13px orange, inside plot top-left):** "rule of thumb: ~4 characters per token (splits illustrative)".

## Why the Bill and the Limit Count Tokens, Not Words

**Tags:** where it's used (blue), worked example (green)

- **Pricing is per token** — say $3 per million input tokens, $15 per million output
- **Our report** — 4,000 input tokens × $3/M = $0.012, about one cent
- **The answer** — 500 output tokens × $15/M = $0.0075: fewer tokens, higher rate
- **Context window** — the model can only hold so many tokens at once, prompt + answer
- **Budget check** — 8,000-token window: 500 instructions + 4,000 report + 1,000 answer fits

*Example:* "Summarize this 300-page book" fails not because it's hard — its tokens don't fit the window.

**Key point:** Every LLM constraint a data scientist hits — price, rate limit, context limit — is denominated in tokens.

This section's viz cell holds both canvases side by side in a `.viz-pair` flex row.

### Visualization (canvas `c3a`, 310×340)

Two-bar chart: cost of input vs output tokens for one report.

- **Title (bold 15px `#1a5276`, top center):** "The Bill for One Report"; subtitle 12px muted on two lines: "$3 / M input tokens, $15 / M output" / "(illustrative)".
- **Bars (80px wide):** "input: report" — $0.012, 4,000 tokens, blue `#2a78d6`; "output: answer" — $0.0075, 500 tokens, orange `#d95926`. Dollar value bold 13px above each bar; label 12px and token count 12px muted below.
- **Axes:** L-shaped `#999` axis, y-scale max $0.015; padding top 74, bottom 72, left 46, right 12.
- **Takeaway (bold 12px orange, bottom center, two lines):** "8x fewer tokens, 5x the rate:" / "output tokens are the expensive ones".

### Visualization (canvas `c3b`, 310×340)

Stacked single-column budget bar: fitting an 8,000-token context window.

- **Title (bold 15px `#1a5276`, top center):** "Fitting the 8,000-Token Window".
- **Stack (62px wide bar at x=100, top y=48, total height 240px, segments proportional to tokens out of 8,000, white 2px separators, ink `#1a5276` outer border):**
  - instructions — 500 tok, violet `#4a3aa7`
  - report — 4,000 tok, blue `#2a78d6`
  - room for answer — 1,000 tok, green `#008300`
  - unused — 2,500 tok, gray `#c9d2dc`
- **Segment labels:** bold 12px in the segment's color (muted for "unused") to the right of the bar, with "N tok" in 12px `#2c3e50` beneath each label.
- **Rotated left label (12px muted):** "8,000-token window".
- **Takeaway (bold 12px orange, bottom center, two lines):** "prompt + answer share one budget —" / "a longer prompt leaves less room to reply".

## Why It Can't Count the R's in "strawberry"

**Tags:** common mistake (red), limitation (orange)

- **strawberry → 3 tokens** — str + aw + berry, delivered to the model as 3 ID numbers
- **Letters are invisible** — the model receives the IDs, never the 10 characters inside
- **Counting letters** — means recalling spelling facts about IDs, not reading text
- **Same trap** — reversing strings, counting words, rhyming on exact spelling
- **Fix** — ask it to spell the word out letter by letter first, or just use one line of code

*Example:* Asking an LLM to count r's is like asking someone to count letters in a word they only ever heard.

**Common mistake:** Treating letter-counting failures as stupidity. The model reads token IDs, not characters — the task fights the tokenizer, not the intelligence.

### Visualization (canvas `c4`, 720×300)

Two-row comparison: the 10 letters you see vs the 3 token IDs the model sees.

- **Title (bold 15px `#1a5276`, top center):** 'What You See vs What the Model Sees: "strawberry"'.
- **Row 1 — letter boxes:** 10 boxes (40×40, 8px gaps, centered at y=58) spelling s-t-r-a-w-b-e-r-r-y in 17px monospace; the three "r" boxes highlighted: fill `rgba(231,76,60,0.15)`, 2px red `#e74c3c` border, bold red letter; other boxes fill `#f4f6f8` with `#c9d2dc` border. Captions: 12px muted "what you see: 10 characters"; bold 12px red "you can count 3 r's because the letters are right there". Small muted downward arrow beneath.
- **Row 2 — token blocks (centered, 42px tall, 14px gaps; outlined 2px in color with 18%-alpha fill; bold 15px monospace label and 13px monospace ID):**
  - "str" (90px) blue `#2a78d6`, #496
  - "aw" (70px) green `#008300`, #675
  - "berry" (130px) violet `#4a3aa7`, #19772
- **Caption (12px muted):** "what the model sees: 3 token IDs (illustrative)".
- **Takeaway (bold 14px orange, bottom center):** "the 10 letters never arrive — counting r's means guessing spelling from 3 numbers".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, 2px `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks (40px bottom margin). Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row. Every section uses two columns — `td.text-col` (50%) / `td.viz-col` (50%). Section 3 places canvases `c3a`/`c3b` (310×340 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Text column structure:** `.tags` row of pill spans (0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); `<ul>` bullets (0.92rem) each opening with `<b>` term in `#1a5276`; italic `.example` paragraph (`#555`, 0.9rem); `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) with `<strong>` lead.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal `box-sizing: border-box` reset; canvases have `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; shared `setup(id)` helper reads each canvas's intrinsic `width`/`height` attributes and scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Token/letter blocks use ui-monospace/Menlo. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
