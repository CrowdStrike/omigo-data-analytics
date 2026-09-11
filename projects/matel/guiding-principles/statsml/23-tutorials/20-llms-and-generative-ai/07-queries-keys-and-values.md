# Queries, Keys & Values

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Queries, Keys & Values

**Subtitle:** Attention is a library lookup: your question is the query, each book's index card is a key, the book's content is the value — and the answer is a blend of contents, weighted by how well each card matches the question

## One Question Walks Into a Library

**Tags:** `core idea` (blue), `library lookup` (green), `three roles` (orange)

- **The reader** — someone walks into a small library and asks: "how do I bake bread?"
- **The query** — that question, written down, is the query: what the reader is looking for
- **The keys** — every book has an index card saying what it is about; each card is a key
- **The values** — the pages inside each book are the value: the content you get if the book is picked
- **The match** — the librarian compares the question to every card and scores how well each fits
- **The answer** — instead of one book, the reader gets a blend: mostly the best match, a little of the rest

*Example (italic):* "How do I bake bread?" matches the card for "Bread at Home" strongly, "Kitchen Basics" fairly, and "Car Repair" not at all — so the answer draws mostly from the first two.

**Key point:** Query = what you're asking, key = each item's label for being matched, value = the content handed back — matching happens on keys, but what you receive is values.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one query card on the left, four book cards (key label on top, value line below) stacked on the right, connected by arrows whose thickness shows match strength.

- **Title (bold 15px, `#1a5276`, top center):** "The Library Lookup: One Query, Four Keys, Four Values".
- **Query card:** rounded rect x=30–225, y=115–185, 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.10)`; bold 13px blue label "QUERY" at the top, 12px `#2c3e50` text below: "how do I / bake bread?" (two lines, centered).
- **Book cards (rounded rects x=460–690, heights 48px, tops at y = 40, 105, 170, 235... use y = 38, 103, 168, 233 to fit):** each has a bold 12px title line (the KEY) and an 11px `#6b7280` value line:
  - "KEY: baking bread at home" / "value: knead, rise 60 min, bake" — 2px green `#008300` border
  - "KEY: everyday kitchen basics" / "value: doughs need a 90 min rise" — 2px blue `#2a78d6` border
  - "KEY: history of ancient Rome" / "value: Roman loaves rose 30 min" — 1px mute `#6b7280` border
  - "KEY: fixing your own car" / "value: torque specs, oil changes" — 1px `#e5e9ef` border, text `#6b7280`
- **Arrows (from query card's right edge x=225, y=150 to each book's left edge x=460):** green 5px to book 1, blue 3px to book 2, mute `#6b7280` 1.5px to book 3, grid `#e5e9ef` 1px to book 4; each carries a bold 12px label near its midpoint: "strong", "good", "weak", "none" in the arrow's color, except "none" which is drawn in mute `#6b7280` for legibility.
- **Annotation (bold 12px orange `#d95926`, near x=250, y=60, two lines):** "match on the card (key)," / "read from the pages (value)".
- **Caption (12px `#444`, bottom left):** "illustrative — a four-book library".

## Scoring the Match by Hand

**Tags:** `worked example` (blue), `dot product` (green), `weights` (orange)

- **Tiny vectors** — write the question as two numbers, topic scores for (cooking, history): q = [2, 1]
- **Keys as numbers** — the four index cards: bread [3, 0], kitchen [2, 1], Rome [0, 1], car [0, 0]
- **Score = multiply and add** — q·bread = 2×3 + 1×0 = 6; kitchen = 5; Rome = 1; car = 0
- **Scores to weights** — divide by the total (12): weights 0.50, 0.42, 0.08, 0.00 — they sum to 1
- **Blend the values** — each book's rise time is its value: 0.50×60 + 0.42×90 + 0.08×30 ≈ 70 min
- **Real models** — LLMs turn scores into weights with softmax instead of plain division; same idea

*Example (italic):* The answer "let the dough rise about 70 minutes" belongs to no single book — it is half "Bread at Home" (60), a good chunk "Kitchen Basics" (90), a dash of Rome (30).

**Key point:** Attention output = sum of (weight × value), with weights from query·key scores — here 0.50×60 + 0.42×90 + 0.08×30 ≈ 70, computable by hand.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: four rows, one per book, bar length = attention weight, with the score arithmetic on each label and the blended answer called out.

- **Title (bold 15px, `#1a5276`, top center):** "Scores Become Weights, Weights Blend the Values".
- **Axis:** horizontal 2px `#999` line at y=245 from x=250 to x=680 (width 430), weight scale 0 to 0.6; tick labels "0", "0.1", ..., "0.6" (12px `#444`) below.
- **Rows (bars 26px tall, centered at y = 75, 120, 165, 210), each with a right-aligned 12px `#444` label ending at x=240:**
  - "bread book — score 6": bar 0 to 0.50, fill green `rgba(0,131,0,0.55)`
  - "kitchen book — score 5": bar 0 to 0.42, fill blue `rgba(42,120,214,0.55)`
  - "Rome book — score 1": bar 0 to 0.08, fill mute `rgba(107,114,128,0.45)`
  - "car book — score 0": no bar; 11px `#6b7280` text "0.00" at the axis start
- **Bar-end labels (bold 12px, bar's solid color, just right of each bar):** "0.50 × 60 min", "0.42 × 90 min", "0.08 × 30 min".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=52):** "blend: 30 + 37.8 + 2.4 ≈ 70 min rise".
- **Caption (12px `#444`, bottom right):** "illustrative — weights are scores ÷ total (real models use softmax)".

## The Same Trick Inside an LLM

**Tags:** `where it's used` (blue), `attention` (green), `context` (orange)

- **Words, not books** — in an LLM every word in the sentence is a book: it offers a key and a value
- **Every word asks** — each word also issues its own query, searching the sentence for what it needs
- **Resolving "it"** — in "The cat drank the milk because it was thirsty", what does "it" mean?
- **The lookup** — "it" sends a query; "cat" and "milk" answer with keys; "cat" scores highest
- **The payoff** — "it" absorbs mostly cat's value (weight 0.62 vs 0.24), so the model reads "it" as the cat
- **Why it matters** — this lookup, run for every word against every word, is how LLMs use context at all

*Example (italic):* Swap "thirsty" for "sweet" and the query from "it" changes, the weights flip toward "milk" — same words, different lookup, different meaning.

**Key point:** Attention runs the library lookup for every word at once: each word's new representation is a weighted blend of the values of the words it attended to.

### Visualization (canvas `c3`, 720×300)

Sentence strip with attention arcs: the nine words in boxes along the bottom, curved arrows rising from "it" to the other words, arc thickness and labels showing the attention weights.

- **Title (bold 15px, `#1a5276`, top center):** "Where Does 'it' Look? Attention Weights from One Word".
- **Word boxes (rounded rects along y=225–260, 12px `#2c3e50` bold text, left edges from x=35 with ~8px gaps, sized to fit):** "The", "cat", "drank", "the", "milk", "because", "it", "was", "thirsty"; "it" gets a 2px orange `#d95926` border and fill `rgba(217,89,38,0.12)`; "cat" a 2px green `#008300` border; "milk" a 2px blue `#2a78d6` border; the rest 1px `#e5e9ef`.
- **Arcs (quadratic curves from the top of "it" to the top of each target, drawn above the boxes):** to "cat" green 5px peaking near y=70; to "milk" blue 3px peaking near y=110; to "drank" mute `#6b7280` 1.5px peaking near y=150; to "thirsty" mute 1.5px peaking near y=185.
- **Arc labels (bold 12px in each arc's color, at the arc peak):** "cat 0.62", "milk 0.24", "drank 0.06", "thirsty 0.05".
- **Annotation (bold 12px green `#008300`, near x=60, y=45, two lines):** "'it' blends mostly cat's value —" / "the model reads 'it' as the cat".
- **Caption (11px `#444`, bottom right):** "weights illustrative; remaining 0.03 spread over other words".

## Same Word, Three Different Hats

**Tags:** `common mistake` (red), `three projections` (orange)

- **The confusion** — people assume query, key, and value are three different words or three inputs
- **One source** — every word produces all three: its own query, its own key, and its own value
- **Three translations** — the model learns three separate recipes turning one word into q, k, and v
- **Different jobs** — the key advertises "what I can offer"; the query asks "what I need"; the value delivers
- **Why separate** — with one shared vector, "what I offer" and "what I ask for" would be forced to match
- **Library again** — a book has a card in the catalog AND can send its own reader to the shelves

*Example (italic):* "cat" might carry query [1, 3], key [2, 1], value [4, 0] — three different number lists made from the same word, each for a different job.

**Common mistake:** Thinking Q, K, V come from different places. They are three learned transformations of the same word — drop the separation and a word could only find others that look exactly like itself.

### Visualization (canvas `c4`, 720×300)

Fan-out diagram: one word box in the center-left, three arrows to three role cards (query, key, value), each card showing its illustrative vector and one-line job description.

- **Title (bold 15px, `#1a5276`, top center):** "One Word 'cat' → Its Own Query, Key, and Value".
- **Word box:** rounded rect x=60–200, y=125–180, 2px ink `#1a5276` border, fill `rgba(26,82,118,0.08)`; bold 15px `#1a5276` centered text: "cat".
- **Role cards (rounded rects x=420–690, heights 56px, tops at y = 48, 122, 196):**
  - "QUERY  [1, 3]" bold 13px blue `#2a78d6`, 2px blue border; 11px `#6b7280` line below: "what does 'cat' need from other words?"
  - "KEY  [2, 1]" bold 13px green `#008300`, 2px green border; 11px line: "how 'cat' advertises itself to queries"
  - "VALUE  [4, 0]" bold 13px orange `#d95926`, 2px orange border; 11px line: "what 'cat' hands over when matched"
- **Arrows:** 3px lines from the word box's right edge (x=200, y=152) to each card's left edge (x=420), colored to match each card (blue, green, orange), with small arrowheads.
- **Arrow labels (12px `#6b7280`, above each arrow's midpoint):** "recipe 1", "recipe 2", "recipe 3".
- **Annotation (bold 12px magenta `#d55181`, near x=230, y=270, one line):** "three learned recipes, one input word — not three inputs".
- **Caption (12px `#444`, bottom right):** "vectors illustrative — real ones have hundreds of numbers".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all vectors, scores, weights, and value numbers are the hardcoded literals above (no randomness); weights in c2 are the exact scores 6, 5, 1, 0 divided by 12 and rounded to 2 decimals; the blended 70 uses the rounded weights (30 + 37.8 + 2.4 = 70.2); c3 and c4 numbers are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
