# Text Generation Metrics

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Text Generation Metrics

**Subtitle:** BLEU, ROUGE, and perplexity all grade machine-written text with counting — overlap with a human reference, or how surprised a model is word by word

## One Bot Reply, One Human Reference

**Tags:** `core idea` (blue), `n-gram overlap` (green), `reference text` (orange)

- **The bakery bot** — a bakery's bot texts order updates; a human once wrote the ideal message
- **Reference** — the human message is "your order will arrive on friday morning" (7 words)
- **Candidate** — the bot instead wrote "your order arrives friday morning" (5 words)
- **Word overlap** — 4 of the bot's 5 words appear in the reference; only "arrives" does not
- **Pair overlap** — word pairs count too: "your order" and "friday morning" match, 2 pairs do not

*Example (italic):* Grading the bot comes down to counting shared words and word pairs between two fixed sentences.

**Key point:** Text metrics reduce "is this good text?" to "how much does it overlap a trusted reference?" — everything BLEU and ROUGE compute builds on that count.

### Visualization (canvas `c1`, 720×300)

Word-alignment diagram: the 7 reference words in a top row of boxes, the 5 bot words in a bottom row, with green connector lines joining the exact matches.

- **Title (bold 15px, `#1a5276`, top center):** "Reference vs Bot Reply: Which Words Match?".
- **Data:** reference tokens `["your","order","will","arrive","on","friday","morning"]`; candidate tokens `["your","order","arrives","friday","morning"]`; matched pairs (reference index → candidate index): 0→0, 1→1, 5→3, 6→4.
- **Reference row:** heading bold 12px `#1a5276` "human reference (7 words)" at x=55, y=66; 7 boxes starting x=55, y=76, each 86 wide × 30 tall, 8px gap; matched words ("your", "order", "friday", "morning") fill `rgba(0,131,0,0.12)` with 2px green `#008300` border and green bold 12px text; unmatched ("will", "arrive", "on") fill `#f2f4f7`, 1px `#999` border, 12px `#6b7280` text.
- **Candidate row:** heading bold 12px `#1a5276` "bot candidate (5 words)" at x=55, y=192; 5 boxes starting x=55, y=202, same size/gap; "your", "order", "friday", "morning" styled like reference matches; "arrives" fill `rgba(213,81,129,0.10)` with 2px magenta `#d55181` border and magenta bold 12px text.
- **Connectors:** green `#008300` 2px lines from the bottom center of each matched reference box to the top center of its candidate box.
- **Annotations:** green bold 13px "4 of 5 bot words match" at top right (x≈540, y=66); magenta bold 12px two-line "\"arrives\" ≠ \"arrive\"" / "— no credit" to the right of the candidate row (x≈540, y=212/228).
- **Caption (12px `#444`, bottom center y=285):** "exact word matching — stems and synonyms do not count".

## BLEU: Precision from the Bot's Side

**Tags:** `worked example` (blue), `precision` (green), `brevity penalty` (orange)

- **Unigram precision** — 4 of the bot's 5 words match the reference, so p1 = 4/5 = 0.80
- **Bigram precision** — 2 of the bot's 4 word pairs match, so p2 = 2/4 = 0.50
- **Brevity penalty** — 5 bot words vs 7 reference words gives BP = e^(1−7/5) ≈ 0.67
- **Geometric mean** — BLEU-2 = 0.67 × √(0.80 × 0.50) ≈ 0.42; both n-gram sizes must do well
- **Clipping** — each reference word can be matched once, so repeating "friday" earns nothing extra

*Example (italic):* Without the brevity penalty, the one-word reply "friday" would score a perfect p1 = 1/1.

**Key point:** BLEU looks from the bot's side — of what it wrote, how much appears in the reference — and the brevity penalty stops short answers from gaming that precision.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart assembling BLEU-2 from its three ingredients, ending with the combined score.

- **Title (bold 15px, `#1a5276`, top center):** "Building BLEU-2 for the Bot's Reply".
- **Data:** rows `["1-gram precision p1", "2-gram precision p2", "brevity penalty BP", "BLEU-2 score"]` with values `[0.80, 0.50, 0.67, 0.42]` and end labels `["4/5 = 0.80", "2/4 = 0.50", "e^(1−7/5) ≈ 0.67", "0.42"]`.
- **Layout:** row labels 12px `#2c3e50` right-aligned at x=170; bars start x=180, full scale width 440 (value 1.0), bar height 26, row tops at y=62, 112, 162, 212.
- **Scale:** vertical gridlines `#e5e9ef` 1px at values 0, 0.25, 0.50, 0.75, 1.0 from y=55 to y=245, each with an 11px `#6b7280` label below at y=258.
- **Bar fills:** p1 `rgba(42,120,214,0.55)`, p2 `rgba(25,158,112,0.5)`, BP `rgba(217,89,38,0.5)`, BLEU-2 `rgba(74,58,167,0.55)`; end labels bold 13px in the matching solid colors (`#2a78d6`, `#199e70`, `#d95926`, `#4a3aa7`) just right of each bar.
- **Annotation (bold 13px violet `#4a3aa7`, bottom center y=288):** "BLEU-2 = 0.67 × √(0.80 × 0.50) ≈ 0.42".

## ROUGE: Recall from the Reference's Side

**Tags:** `worked example` (blue), `recall` (green), `paraphrase trap` (red)

- **Flip the view** — ROUGE asks how much of the human reference the bot managed to cover
- **ROUGE-1** — 4 of the reference's 7 words appear in the bot reply: recall = 4/7 ≈ 0.57
- **ROUGE-2** — 2 of the reference's 6 word pairs are covered: recall = 2/6 ≈ 0.33
- **ROUGE-L** — the longest common subsequence "your order friday morning" gives F1 ≈ 0.67
- **Paraphrase trap** — "the package should reach you friday before noon" scores only 1/7 ≈ 0.14

*Example (italic):* A perfect paraphrase of the reference shares only the word "friday" with it, so ROUGE calls it bad.

**Key point:** ROUGE is recall-first (summarization), BLEU precision-first (translation) — and both give near-zero credit to text that says the same thing in different words.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of the three ROUGE scores for the bot reply, plus a fourth bar showing the paraphrase collapsing to near zero.

- **Title (bold 15px, `#1a5276`, top center):** "ROUGE Scores for the Same Reply — and a Paraphrase".
- **Data:** bars `["ROUGE-1", "ROUGE-2", "ROUGE-L F1", "paraphrase ROUGE-1"]` with values `[0.57, 0.33, 0.67, 0.14]` and value labels `["4/7 ≈ 0.57", "2/6 ≈ 0.33", "0.67", "1/7 ≈ 0.14"]`.
- **Axes:** origin x=70, baseline y=235, chart height 175, y scale 0–1.0 with gridlines `#e5e9ef` and 11px `#6b7280` labels at 0.25, 0.50, 0.75, 1.00.
- **Bars:** width 90, centered at x=140, 290, 440, 600; fills `rgba(0,131,0,0.5)` (green), `rgba(25,158,112,0.5)` (aqua), `rgba(42,120,214,0.55)` (blue), `rgba(213,81,129,0.5)` (magenta); bold 13px value labels above each bar in the matching solid colors (`#008300`, `#199e70`, `#2a78d6`, `#d55181`).
- **Bar labels:** 12px `#444` below baseline; "paraphrase ROUGE-1" on two lines ("paraphrase" / "ROUGE-1").
- **Annotation (bold 12px magenta `#d55181`, two lines above the last bar):** "same meaning," / "almost no overlap".
- **Caption (12px `#444`, bottom center y=290):** "paraphrase = \"the package should reach you friday before noon\"".

## Perplexity: Grading Without a Reference

**Tags:** `core idea` (blue), `language models` (orange), `common mistake` (red)

- **No reference** — perplexity skips the human answer; it asks how surprised a model is by text
- **Per-word bets** — model A gives the 7 reference words probabilities from 0.10 up to 0.80
- **The score** — perplexity is the inverse geometric mean of those bets: model A scores 3.1
- **Reading it** — perplexity 3.1 means the model is as unsure as picking among ~3 equal options
- **Lower is better** — model B's timid bets (0.02–0.20) give perplexity 12.7, about 4× worse

*Example (italic):* On "friday" model A bets 0.10 and model B bets 0.02 — one bad guess can dominate the whole average.

**Common mistake:** Reading low perplexity as "good output" — it measures how predictable text is to the model, not whether the text is true or useful, and scores from models with different vocabularies are not comparable.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: for each of the 7 reference words, the probability model A and model B assigned to it, with the resulting perplexities called out.

- **Title (bold 15px, `#1a5276`, top center):** "How Surprised Is the Model? Word-by-Word Probabilities".
- **Data:** words `["your","order","will","arrive","on","friday","morning"]`; model A probabilities `[0.20, 0.50, 0.25, 0.40, 0.50, 0.10, 0.80]`; model B probabilities `[0.05, 0.10, 0.08, 0.10, 0.12, 0.02, 0.20]` (both illustrative).
- **Axes:** origin x=55, baseline y=235, chart height 165, y scale 0–1.0 with gridlines `#e5e9ef` and 11px `#6b7280` labels at 0.25, 0.50, 0.75, 1.00.
- **Groups:** 7 groups of width 88 starting at x=70; within each group, model A bar (fill `rgba(0,131,0,0.5)`, width 26) then model B bar (fill `rgba(217,89,38,0.5)`, width 26) with a 6px gap; 11px value labels above each bar in `#008300` / `#d95926`.
- **Word labels:** 12px `#444` below the baseline, centered under each group.
- **Callouts (top right, x≈470):** green bold 13px "model A: perplexity 3.1" at y=52; orange bold 13px "model B: perplexity 12.7" at y=72.
- **Caption (12px `#444`, bottom center y=290):** "perplexity = inverse geometric mean of the probabilities (illustrative) — lower means less surprised".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
