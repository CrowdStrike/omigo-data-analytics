# Machine Translation

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left ~40%, canvas right ~60%)
**HTML title tag:** Machine Translation

**Subtitle:** Neural translation pitfalls: metrics that don't measure quality, data deserts, and meaning that can't be verified at scale.

## BLEU Score ≠ Quality

**High N-Gram Overlap Can Mean Bad Translation; Low Overlap Can Mean Great One**

- **What BLEU measures:** N-gram overlap against a reference translation — a string-similarity count.
- **What it does not measure:** Not meaning, not fluency, not correctness — only surface word matching.
- **Zero for perfection:** A perfectly natural translation phrased in different words scores near 0.
- **High for garbage:** Unnatural, barely readable output that matches reference n-grams scores high.
- **Rankings disagree:** Systems ranked by BLEU and by human raters come out in different orders.
- **What you optimize:** Tuning for BLEU optimizes reference-matching, not the quality of the translation.

### Visualization (canvas `c1`, 720×300)

Scatter plot of BLEU score vs human quality showing weak correlation.

- **Title (bold 17px `#1a5276`, top center):** "BLEU Score vs. Human Quality: Weak Correlation".
- **Plot area:** padding top 35, right 30, bottom 45, left 60. Axes as L-shape (left + bottom) in `#2980b9`, width 1.5.
- **Data (10 systems, {bleu, human} as fractions of axis):** (0.12, 0.65), (0.28, 0.55), (0.41, 0.72), (0.55, 0.48), (0.62, 0.78), (0.71, 0.60), (0.78, 0.82), (0.85, 0.70), (0.45, 0.80), (0.68, 0.45).
- **Diagonal reference:** dashed (4/4) line from bottom-left to top-right in `rgba(149,165,166,0.6)`, width 1 — represents perfect correlation.
- **Points:** blue `#2980b9` filled circles radius 5 at (bleu, human). For each point whose human-quality y differs from the perfect-correlation y (y = bleu) by more than 10px, draw a vertical divergence line in `rgba(231,76,60,0.4)` width 1 from the actual point down/up to the expected position, with a small red `#e74c3c` dot radius 3 at the expected position.
- **Axis labels (17px `#1a5276`):** "BLEU Score →" centered below x-axis; "Human Quality →" rotated -90° at left (x=20, vertical center).
- **Annotation (17px `#e74c3c`, right-aligned at top-right of plot):** "Red = where the metric lies".

## Low-Resource Language Data Desert

**"Universal" Translation = Works for 20 Languages, Fails for 6,980**

- **The skew:** ~80% of parallel training data covers English, Chinese, and major European languages.
- **The tail:** About 7,000 languages exist; most have near-zero parallel corpus to train on.
- **False universality:** Benchmarks average over high-resource pairs, hiding the long tail's failure.
- **No shortcut:** Transfer learning and pivoting through English help only marginally on those pairs.
- **Viability threshold:** Below a minimum data volume, the language pair simply does not work at all.

### Visualization (canvas `c2`, 720×300)

Bar chart of parallel corpus size by language (long-tail decay).

- **Title (bold 17px `#1a5276`, top center):** "Parallel Corpus by Language (log scale): The Long Tail Starves".
- **Plot area:** padding top 35, right 20, bottom 55, left 60. Axes as L-shape in `#2980b9`, width 1.5.
- **Bars (19 total, name: height fraction of chart):** EN 0.98, ZH 0.92, DE 0.85, FR 0.83, ES 0.80, RU 0.72, JA 0.70, PT 0.62, AR 0.55, KO 0.50, HI 0.35, TH 0.28, VI 0.25, SW 0.12, YO 0.07, AM 0.06, ZU 0.05, QU 0.04, "~6980 more" 0.02. Bar width = chartWidth/19 − 4, 4px gap.
- **Bar colors:** `#2980b9` if value ≥ 0.30 (above threshold), otherwise `#e74c3c`.
- **Threshold line:** horizontal dashed (6/3) red `#e74c3c` line width 1.5 at y = 0.30 of chart height, labeled "Min viable data" (17px red, left-aligned just above the line).
- **X labels:** language names in `#555` 17px, rotated -36° (−π/5) below each bar.
- **Annotation (bold 17px `#e74c3c`, right-aligned near top of plot):** "~6,980 languages below viable threshold".

## Domain Terminology Drift

**One Word, Different Meaning Per Domain — General Models Mistranslate All of Them**

- **Medical and legal senses:** Medical "culture" = a lab culture; legal "party" = a litigant.
- **Financial sense:** Financial "interest" = a rate on money, not curiosity about something.
- **General model default:** Trained mostly on general text, it picks the everyday sense every time.
- **Catastrophic output:** Domain terms come out mistranslated, in confident and fluent-sounding prose.
- **Legal stakes:** One wrong word in a legal contract translation is a liability, not a typo.
- **Medical stakes:** In a translated medical instruction, the same wrong word can cause real harm.
- **The fix isn't free:** Domain-adapted models need domain parallel data — rarely published in these fields.

### Visualization (canvas `c3`, 720×300)

Diagram: three word rows, each branching to a correct domain sense (green) and the model's wrong everyday pick (red).

- **Title (bold 17px `#1a5276`, top center):** "Same Word, Different Meaning Per Domain".
- **Rows (word at x=40 bold `#1a5276`, domain at x=150 in `#555`, at y=60/110/160):**
  - "culture" — Medical — domain: lab culture / model picks: arts & society
  - "party" — Legal — domain: litigant / model picks: celebration
  - "interest" — Finance — domain: rate on money / model picks: curiosity
- **Per row:** green `#27ae60` arrow line (width 2) from x=250 up-right to x=310, then text "domain: <right sense>" at x=320 (17px green); red `#e74c3c` arrow line down-right, then text "model picks: <wrong sense>" at x=320 (17px red).
- **Bottom annotations (centered):** bold 17px `#e74c3c` at y=215: "General model defaults to the everyday sense — every domain term drifts."; 17px `#555` at y=238: "One wrong word in a legal or medical document = liability, not a typo."

## Gender/Formality Ambiguity — Guessing Missing Information

**The Source Doesn't Contain the Answer, but the Target Language Requires One**

- **Formality:** English "you" must become formal or informal in 50+ target languages.
- **No signal:** The English source carries no marker telling the model which register to pick.
- **Gender:** English "they" or "the doctor" must map to a gendered pronoun in many targets.
- **Forced guessing:** The model must emit information not present in the input at all.
- **Cannot be correct:** With the answer absent from the source, output is lucky rather than right.
- **Systematic bias:** Skewed corpora push defaults to masculine forms: "doctor" → he, "nurse" → she.

### Visualization (canvas `c4`, 720×300)

Branching diagram: one English source box fanning out to three target-language options.

- **Title (bold 17px `#1a5276`, top center):** "Source Has One Form. Target Requires a Choice."
- **Source box:** rect at (50, 80) size 180×50, stroke `#2980b9` width 2, fill `rgba(41,128,185,0.3)`; centered bold text 'English: "you"' (`#1a5276`) and below it '(no formality info)' (17px `#555`).
- **Branches (lines width 2 from box right edge (230,105) to x=360 at target y; label at x=370, 17px in branch color):**
  - formal "usted" — y=60 — `#27ae60`
  - informal "tú" — y=105 — `#f39c12`
  - plural "ustedes" — y=150 — `#e67e22`
- **Right-side annotation:** bold 17px `#e74c3c` "Model must GUESS" at (530, 100); 17px `#555` "info not in source" at (530, 122).
- **Bottom annotations (centered):** bold 17px `#e74c3c` at y=205: 'Skewed corpora make the guess systematic: "doctor" → he, "nurse" → she.'; 17px `#555` at y=228: "A forced guess plus biased training data = bias shipped as translation."

## Context Window Insufficient for Document Coherence

**Sentence-by-Sentence Translation Loses Cross-Sentence References**

- **The scenario:** A pronoun in paragraph 3 refers to a noun first introduced in paragraph 1.
- **Outside the window:** That antecedent sits beyond what the model sees for this sentence.
- **The failure:** "It" gets translated with the wrong gender or number, the antecedent being invisible.
- **Beyond pronouns:** Terminology consistency, register, and discourse markers drift the same way.
- **Why they drift:** Each sentence is translated in isolation, with no memory of earlier choices.
- **Metric blindness:** Sentence-level BLEU scores every sentence fine while the document reads broken.

### Visualization (canvas `c5`, 720×300)

Diagram: three stacked document paragraphs with a red context-window box around only the last one, and a dashed green link back to paragraph 1 cut off by the window.

- **Title (bold 17px `#1a5276`, top center):** "Antecedent in Paragraph 1, Pronoun in Paragraph 3".
- **Paragraph bars (540×32 rects starting x=50, at y=55/100/145, text left-aligned at x=60 in 17px `#1a5276`):**
  - 'Para 1: "...the committee (fem. in target)..."' — fill `rgba(41,128,185,0.3)`
  - "Para 2: ..." — fill `rgba(41,128,185,0.15)` (dimmer)
  - 'Para 3: "It decided..." → needs gender from Para 1' — fill `rgba(41,128,185,0.3)`
- **Context window:** red `#e74c3c` stroke rect width 2.5 at (44, 139) size 552×44 around Para 3 only, with bold 17px red label "← model sees" at (602, 166).
- **Antecedent link:** dashed (5/4) green `#27ae60` bezier curve width 2 from left of Para 3 (40, 161) arcing left and up to left of Para 1 (40, 71).
- **Bottom annotations (centered):** bold 17px `#e74c3c` at y=215: 'Antecedent outside window → "it" translated with wrong gender.'; 17px `#555` at y=238: "Every sentence scores fine on BLEU. The document reads broken."

## Meaning Preservation Is Unmeasurable at Scale

**Only a Bilingual Human Can Verify Meaning — and No Human Can Check 1M/Day**

- **The core problem:** No automated test proves that a translation preserves the source meaning.
- **What proxies do:** They measure n-gram overlap or fluency, never semantics — a different quantity.
- **The only oracle:** A bilingual human reading both sides, which is expensive and slow.
- **Rater noise:** Even that oracle is inconsistent, with two raters scoring the same output differently.
- **The scale gap:** Production emits millions of translations a day; humans audit a vanishing fraction.
- **The consequence:** Quality at scale is unverifiable — silent meaning errors ship with no dashboard.

### Visualization (canvas `c6`, 720×300)

Two horizontal magnitude bars: machine output vs human audit capacity.

- **Title (bold 17px `#1a5276`, top center):** "Translations per Day vs. What Humans Can Verify".
- **Bar 1:** label "System output: 1,000,000 / day" (17px `#333`, left at x=50, y=55); bar rect (50, 62) size 610×30, fill `rgba(41,128,185,0.3)`, stroke `#2980b9` width 1.5.
- **Bar 2:** label "Bilingual human audit: ~500 / day" (17px `#333` at y=125); tiny red `#e74c3c` bar rect (50, 132) size 2×30 with red label "0.05%" at (60, 153).
- **Bottom annotations (centered):** bold 17px `#e74c3c` at y=200: "99.95% of translations ship with meaning never verified by anyone."; 17px `#555` at y=225: "Automated proxies check overlap and fluency — not whether the meaning survived."; 17px `#555` at y=247: "Silent semantic errors have no dashboard."

## Regeneration instructions

- **Layout:** detail page in the domains-page style: h1 + `.subtitle`, then one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px, margin 40px 0 15px), each followed by a full-width `.obj-table` with a single `<tr>`: left `<td>` (40%) holds `.obj-title` div + `<ul>` bullets, right `<td>` (60%, centered) holds the canvas. No thead, no nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`, margin 8px 0 8px 20px; `strong` in `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; even rows background `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) but unused on this page.
- **Palette:** primary blue `#1a5276`, axis/accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, amber `#f39c12`, gray text `#666`/`#555`/`#333`, bar fill `rgba(41,128,185,0.3)`.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes (720×300); a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart fonts are 17px -apple-system (bold for titles/emphasis). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
