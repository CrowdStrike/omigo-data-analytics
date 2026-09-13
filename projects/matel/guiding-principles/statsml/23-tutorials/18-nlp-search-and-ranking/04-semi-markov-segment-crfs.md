# Semi-Markov / Segment CRFs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Semi-Markov / Segment CRFs

**Subtitle:** A semi-Markov (segment) CRF labels whole chunks of words instead of one word at a time — so a four-word dish name can be scored, and recognized, as one single thing

## One Query, Two Ways to Read It

**Tags:** `core idea` (blue), `spans not tokens` (green), `search queries` (orange)

- **The query** — someone types "new york style cheesecake" into a food-delivery search box
- **Word-at-a-time** — a token tagger labels each word alone and confidently tags "new york" as a city
- **Span-at-a-time** — a segment CRF first cuts the query into chunks, then labels each whole chunk
- **The right chunk** — here the best chunk is all four words together, one span labeled "dish"
- **The name** — "semi-Markov" just means the model steps span by span instead of word by word

*Example (italic):* The customer wants cheesecake delivered, not a trip to Manhattan — reading the four words as one span is what gets the order right.

**Key point:** A semi-Markov (segment) CRF labels chunks of words as units, so "new york style cheesecake" can be one dish instead of a city plus leftovers.

### Visualization (canvas `c1`, 720×300)

Two-row diagram: the same four query words read by a word-at-a-time tagger (chopped, wrong) on top, and by a segment model (one long span, right) below.

- **Title (bold 15px, `#1a5276`, top center):** "One Query, Read Word-by-Word vs Span-by-Span".
- **Word boxes (shared, y=58, height 32):** four rounded rects 130px wide at x = 90, 240, 390, 540; fill `#f8f9fa`, 1px `#e5e9ef` border; bold 13px `#2c3e50` centered words "new", "york", "style", "cheesecake".
- **Row 1 — token tagger (y=112, height 26):** left label 12px `#444` at x=20: "word-at-a-time"; under "new" and "york" two orange chips (fill `rgba(217,89,38,0.15)`, 1px `#d95926`, bold 12px `#d95926` text "city"); under "style" and "cheesecake" two blue chips (fill `rgba(42,120,214,0.12)`, 1px `#2a78d6`, bold 12px `#2a78d6` text "dish").
- **Row 2 — segment model (y=205, height 34):** left label 12px `#444` at x=20: "segment CRF"; one green rounded bar from x=90 to x=670 (width 580), fill `rgba(0,131,0,0.15)`, 2px `#008300` border, bold 13px `#008300` centered label "dish — one span of 4 words".
- **Annotation (bold 12px orange `#d95926`, near x=400, y=170):** "word-by-word chops the dish into a city + leftovers".
- **Caption (12px `#444`, bottom right):** "illustrative — one search query read two ways".

## Scoring Three Splits by Hand

**Tags:** `worked example` (blue), `segment scores` (green)

- **Segment scores** — give each possible chunk-plus-label some points, e.g. "new york" as city earns 4
- **Split A** — "new york" city (4) + "style cheesecake" dish (2) adds up to 6 points
- **Split B** — the whole line "new york style cheesecake" as one dish earns 7 points
- **Split C** — "new york style" dish (2) + "cheesecake" dish (3) adds up to 5 points
- **The winner** — the model returns the split with the highest total, so split B wins with 7
- **Why 7** — a whole-span check ("this exact 4-word chunk appears on menus") fires only in split B

*Example (italic):* You can redo the whole prediction by hand: 4 + 2 = 6, 7 alone, and 2 + 3 = 5 — the single big span beats both chopped splits.

**Key point:** The prediction is just "the segmentation whose segment scores add to the biggest total" — here 7 beats 6 and 5.

### Visualization (canvas `c2`, 720×300)

Horizontal stacked-bar chart: the three candidate splits as rows, each bar built from its segment scores, with the winning total highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Three Splits of the Same Query — Highest Total Wins".
- **Axis:** horizontal 2px `#999` line at y=255 from x=230 to x=680 (width 450), points scale 0 to 8; tick labels "0", "2", "4", "6", "8" (12px `#444`) below; light `#e5e9ef` vertical gridlines at 2, 4, 6.
- **Rows (bars 26px tall, tops at y = 85, 148, 211), each with a left-aligned 12px `#444` label at x=20:**
  - "A: [new york]=city + [style cheesecake]=dish": stacked bar — segment 0→4 fill `rgba(42,120,214,0.35)` 1px `#2a78d6` with white bold 12px "4" inside, segment 4→6 fill `rgba(25,158,112,0.35)` 1px `#199e70` with "2" inside; bold 13px `#444` total "6" right of the bar.
  - "B: [new york style cheesecake]=dish": one segment 0→7 fill `rgba(0,131,0,0.30)` 2px `#008300` with bold 12px "7" inside; bold 13px `#008300` total "7  ← winner" right of the bar.
  - "C: [new york style]=dish + [cheesecake]=dish": segment 0→2 fill `rgba(74,58,167,0.30)` 1px `#4a3aa7` with "2" inside, segment 2→5 fill `rgba(213,81,129,0.30)` 1px `#d55181` with "3" inside; bold 13px `#444` total "5".
- **Annotation (bold 12px green `#008300`, near x=470, y=68):** "the whole-span menu-match feature adds the winning points".
- **Caption (12px `#444`, bottom right):** "segment points are illustrative".

## Where Whole-Span Features Win

**Tags:** `where it's used` (blue), `whole-span features` (green), `search ranking` (orange)

- **Search ranking** — whether a query span means "dish" or "location" decides which restaurants come back
- **Multi-word names** — dishes, addresses, song titles, and people's names usually spread over several words
- **Whole-span features** — the span's length, an exact dictionary match, the look of the chunk as a unit
- **Token taggers fragment** — word-at-a-time models often split one name into two half-labeled pieces
- **The gap** — exact-span accuracy on multi-word names: 62% word tagger vs 81% segment model (illustrative)

*Example (italic):* A pizza app that reads "buffalo chicken pizza" as the city Buffalo plus a dish returns restaurants in the wrong state.

**Key point:** Segment models earn their keep exactly where names span many words — the whole-span features a token model can never ask about.

### Visualization (canvas `c3`, 720×300)

Grouped vertical bar chart: exact-span match accuracy for the two models, on single-word names vs multi-word names, showing where the segment model pulls ahead.

- **Title (bold 15px, `#1a5276`, top center):** "Exact-Match Accuracy: Word Tagger vs Segment Model".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; y = accuracy 0 to 100% with 12px `#444` tick labels "0%", "25%", "50%", "75%", "100%" and light `#e5e9ef` gridlines at 25/50/75.
- **Groups:** "single-word names" centered near x=240, "multi-word names" centered near x=500 (12px `#444` labels below the baseline); within each group two bars 70px wide with a 16px gap.
- **Bars (hardcoded values):** word tagger fill `rgba(42,120,214,0.35)` 1px `#2a78d6` at heights `[91, 62]`; segment model fill `rgba(0,131,0,0.30)` 1px `#008300` at heights `[92, 81]`; bold 13px value labels above each bar: "91%", "92%", "62%", "81%".
- **Legend (12px, top right near x=560, y=48):** blue square + "word tagger", green square + "segment model".
- **Annotation (bold 13px green `#008300`, near x=470, y=90):** "+19 points where names span many words".
- **Caption (12px `#444`, bottom right):** "illustrative numbers — invented for the example".

## But BIO Tags Can Mark Spans Too — So What's New?

**Tags:** `common mistake` (red), `BIO vs segments` (orange)

- **The BIO trick** — token taggers mark spans with B-dish / I-dish tags, so span outputs are not the new part
- **What each sees** — a BIO score reads any word but scores one tag at a time; a segment score sees the whole chunk
- **Whole-span questions** — "is this exact chunk a known dish?" or "is it 4 words long?" needs the full span
- **The price** — the model must consider chunks up to some max length L, so decoding is roughly L times slower
- **Same family** — both are CRFs; semi-Markov just moves the scoring unit from the word to the span

*Example (italic):* Both models can output "dish covering four words" — only the segment model could score "this exact chunk appears on thousands of menus" as one feature.

**Common mistake:** Thinking BIO tags already give you a span model. They give span-shaped outputs, but not span-level features — and span-level features are the whole reason semi-CRFs exist.

### Visualization (canvas `c4`, 720×300)

Two-row diagram: the BIO tagger's four per-word tag chips (each seeing one word) above, and the segment model's single span with its whole-span feature list below — same output, different evidence.

- **Title (bold 15px, `#1a5276`, top center):** "BIO Gives Span Outputs — a Semi-CRF Gives Span Features".
- **Row 1 — BIO tagger (chips y=70, height 30):** left label 12px `#444` at x=20: "BIO tagger"; four rounded chips 120px wide at x = 140, 275, 410, 545, fill `rgba(42,120,214,0.12)`, 1px `#2a78d6`, bold 12px `#2a78d6` centered text "B-dish", "I-dish", "I-dish", "I-dish"; under each chip an 11px `#6b7280` note at y=120: "scores 1 tag".
- **Row 2 — segment model (bar y=185, height 34):** left label 12px `#444` at x=20: "semi-CRF"; one green rounded bar from x=140 to x=665 (width 525), fill `rgba(0,131,0,0.15)`, 2px `#008300` border, bold 13px `#008300` centered label "dish (words 1–4)"; below at y=242 a 12px `#444` line: "features: span length = 4 · exact menu match · starts the query".
- **Annotation (bold 12px violet `#4a3aa7`, near x=400, y=155):** "same answer on paper — different evidence to earn it".
- **Caption (12px `#444`, bottom right):** "illustrative — how each model scores the same span".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** every segment score, bar height, chip label, and coordinate is a hardcoded literal from the specs above (no randomness); `c1` and `c4` are drawn diagrams (rounded rects and chips via `ctx.roundRect` or a small helper), `c2` and `c3` are bar charts from the literal arrays; all invented numbers carry an "illustrative" caption.
- The worked-example numbers in the text (4+2=6, 7, 2+3=5; 62% vs 81%) must match the chart values exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
