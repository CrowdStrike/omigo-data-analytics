# Text to Features

**Page type:** detail page (tutorial layout: one `.card-section` per concept, each with h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Text to Features

**Subtitle:** A model can't read — so turn each text into a row of numbers, and each upgrade (counts → weights → embeddings) keeps more of the meaning

## Five Reviews a Model Can't Read

**Tags:** core idea (blue), running example (green)

- **The data** — 5 product reviews, e.g. "great coffee, fast shipping" and "mug arrived broken"
- **The problem** — a model needs numbers per review, and text is not numbers
- **Simplest fix** — pick the useful words, count them: one column per word
- **Bag of words** — each review becomes a row of counts, mostly 0s and 1s
- **Already usable** — rows sharing words ("broken", "mug") already look alike as numbers

*Example:* "Great coffee, fast shipping" becomes: great 1, coffee 1, fast 1, shipping 1, everything else 0.

**Key point:** Bag of words is the entry ticket — text becomes a table a model can use. It only knows WHICH words appeared, nothing more.

### Visualization (canvas `c1`, 720×300)

Count-matrix grid: bag-of-words table, 5 reviews × 7 words.

- **Title (bold 15px, `#1a5276`, top center):** "Bag of Words: One Column per Word, One Row per Review".
- **Columns (bold 12px monospace headers in `#1a5276`):** great, coffee, mug, broken, shipping, fast, slow.
- **Rows (right-aligned 11px labels in `#444`) and values:**
  - R1 "great coffee, fast shipping" → [1, 1, 0, 0, 1, 1, 0]
  - R2 "coffee tastes great" → [1, 1, 0, 0, 0, 0, 0]
  - R3 "broken mug, slow shipping" → [0, 0, 1, 1, 1, 0, 1]
  - R4 "mug arrived broken" → [0, 0, 1, 1, 0, 0, 0]
  - R5 "great mug" → [1, 0, 1, 0, 0, 0, 0]
- **Grid geometry:** matrix starts at x=285, y=62; cells 58×36px, borders `#e5e9ef` 1px.
- **Cell styling:** value 1 cells filled `rgba(42,120,214,0.30)` with bold 14px monospace digit in `#1a5276`; value 0 cells filled `#fafbfc` with digit in `#c2c9d0`.
- **Footnote (11px, muted `#6b7280`, below matrix):** 'filler words ("tastes", "arrived") trimmed to keep 7 columns'.
- **Takeaway (bold 13px orange `#d95926`, bottom center):** 'R3 and R4 already look alike as rows — they share "mug" and "broken"'.

## TF-IDF by Hand: Rare Words Weigh More

**Tags:** worked example (green)

- **The flaw in counts** — "great" is in 3 of 5 reviews; it barely separates them
- **The recipe** — weight = count × ln(reviews ÷ reviews containing the word)
- **"great"** — in 3 of 5: ln(5/3) = 0.51 — common, so downweighted
- **"fast"** — in 1 of 5: ln(5/1) = 1.61 — rare, so it stands out
- **Review 1 rescored** — great 0.51, coffee 0.92, shipping 0.92, fast 1.61

*Example:* All four words appeared once, but after weighting, "fast" carries three times the weight of "great".

**Key point:** TF-IDF keeps the counts but adds importance — a word that shows up everywhere tells you little, a word few reviews use tells you a lot.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: raw count vs TF-IDF weight for Review 1's four words.

- **Title (bold 15px `#1a5276`, top center):** "Review 1 Rescored: Count vs TF-IDF Weight".
- **Groups (word, document frequency, idf value, bar color):**
  - great — in 3 of 5 → ln(5/3), idf 0.51, muted gray `#6b7280`
  - coffee — in 2 of 5 → ln(5/2), idf 0.92, blue `#2a78d6`
  - shipping — in 2 of 5 → ln(5/2), idf 0.92, aqua `#199e70`
  - fast — in 1 of 5 → ln(5/1), idf 1.61, green `#008300`
- **Bars:** per group, a flat gray raw-count bar (`#c9d1d8`, height for value 1) and a colored TF-IDF bar at 0.75 alpha; bars 46px wide, groups 140px apart starting x=95, baseline y=225, chart height 150px, y-scale max 1.8.
- **Reference line:** dashed light-gray (`#bbb`, dash 5/4) horizontal line at value 1 labeled "raw count = 1 for all four words" (11px muted).
- **Labels:** TF-IDF value (e.g. "0.51") bold 13px in the bar's color above each colored bar; bold 13px word label below the baseline; 11px muted line "in 3 of 5 → ln(5/3)" etc. under each word.
- **Legend (top right):** gray swatch "raw count"; four-color striped swatch (mute/blue/aqua/green) "TF-IDF weight (one color per word)" (12px `#333`).
- **Takeaway (bold 13px orange `#d95926`, bottom center):** 'same counts, new story: rare "fast" (1.61) now outweighs common "great" (0.51) 3-to-1'.

## Embeddings: Meaning as a Position on a Map

**Tags:** core idea (blue), where it's used (blue)

- **The flaw in weights** — "broken" and "damaged" are separate columns sharing nothing
- **Embeddings** — a pretrained model maps each review to a point (a vector of numbers)
- **The rule of the map** — similar meaning lands nearby, regardless of exact words
- **Our reviews** — happy-coffee reviews cluster together; broken-mug reviews cluster apart
- **The payoff** — a new review "damaged cup" lands next to "mug arrived broken": zero shared words

*Example:* Search, spam filters, and duplicate-ticket detection all run on "nearby points mean similar text".

**Key point:** Counts and weights match words; embeddings match meaning. That's the step where synonyms finally count as similar.

### Visualization (canvas `c3`, 720×300)

Scatter plot: reviews as 2-D embedding points, two clusters plus an arriving new review.

- **Title (bold 15px `#1a5276`, top center):** "Reviews as Points: Similar Meaning Lands Nearby (illustrative 2-D view)".
- **Axes:** L-shaped gray (`#999`) axis frame; padding top 46, bottom 40, left 55, right 30; data space x 0–10, y 0–5.
- **Cluster halos:** ellipse `rgba(0,131,0,0.08)` centered at data (2.7, 3.1), radii 105×62 — happy cluster; ellipse `rgba(213,81,129,0.10)` centered at (7.8, 1.8), radii 100×62 — broken cluster.
- **Points (7px dots, bold 12px labels in point color):**
  - R1 great coffee, fast shipping — (2.1, 3.4), green `#008300`, label above
  - R2 coffee tastes great — (2.7, 2.7), green `#008300`, label below
  - R5 great mug — (3.9, 3.3), aqua `#199e70`, label above
  - R3 broken mug, slow shipping — (7.3, 2.1), magenta `#d55181`, label above
  - R4 mug arrived broken — (7.9, 1.4), magenta `#d55181`, label below
- **New review:** orange (`#d95926`) 8px dot with white ring at (8.6, 2.4); dashed orange arrow line from (6.0, 4.5) to the dot; bold orange label 'new: "damaged cup"' above the arrow origin.
- **Cluster labels (bold 12px):** "happy coffee cluster" in green under the left halo; "broken mug cluster" in magenta under the right halo.
- **Takeaway (bold 13px orange, bottom center):** '"damaged cup" lands beside "mug arrived broken" — zero words in common'.

## What Each Step Keeps — and What It Still Loses

**Tags:** common mistake (red), rule of thumb (blue)

- **Bag of words** — keeps which words; loses order: "mug arrived broken" = "broken mug arrived"
- **TF-IDF** — adds which words matter; still blind to order and synonyms
- **Embeddings** — add meaning and context; cost: numbers you can't read off
- **The classic trap** — counts score "not broken" as containing "broken"; embeddings handle it
- **Start simple** — TF-IDF is a strong, cheap, explainable baseline; upgrade when it fails

*Example:* A returns model built on word counts flagged every review saying "not broken at all" as a complaint.

**Rule of thumb:** Each step keeps more meaning and hides more of the mechanics — pick the simplest representation that solves your problem, not the fanciest one available.

### Visualization (canvas `c4`, 720×300)

Staircase ladder diagram: three representation steps rising left to right, connected by arrows.

- **Title (bold 15px `#1a5276`, top center):** "Each Step Keeps More Meaning".
- **Boxes (200×78px, background `#f8f9fa`, 2px colored border, bold 14px colored title, 12px keeps line in `#2c3e50`, 12px loses line in `#6b7280`):**
  - "1. bag of words" at (40, 170), blue `#2a78d6` — "keeps: which words" / "loses: order, importance, synonyms"
  - "2. TF-IDF" at (260, 135), violet `#4a3aa7` — "keeps: which words matter" / "loses: order, synonyms"
  - "3. embeddings" at (480, 70), green `#008300` — "keeps: meaning in context" / "loses: easy interpretability"
- **Arrows:** light gray-blue (`#b9c2cc`, 2px) lines with filled arrowheads connecting box 1 → 2 and 2 → 3 at mid-height.
- **Trap note (one line under first box):** bold 12px red `#e74c3c`: 'trap: "not broken" still counts "broken"' followed on the same line by 12px muted: " — counts and weights can't see negation or order".
- **Takeaway (bold 13px orange `#d95926`, bottom center):** "climb only as high as your problem needs — TF-IDF is often enough".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, 2px `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks (40px bottom margin). Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row — left `td.text-col` (50%) holds tags, bullets, `.example`, `.key-point`; right `td.viz-col` (50%) holds the canvas.
- **Text column structure:** `.tags` row of pill spans (0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); `<ul>` bullets (0.92rem) each opening with `<b>` term in `#1a5276`; italic `.example` paragraph (`#555`, 0.9rem); `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) with `<strong>` lead.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal `box-sizing: border-box` reset; canvases have `width:100%`, 1px `#e0e0e0` border, 4px radius; `li code` in ui-monospace on `#f4f6f8`.
- **Canvas JS:** shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; shared `setup(id)` helper scales each 720×300 canvas by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
