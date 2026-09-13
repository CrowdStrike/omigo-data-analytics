# Suffix Arrays

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Suffix Arrays

**Subtitle:** Sort every tail of a text once, and any substring — word or not — can be found by binary search, because all its occurrences sit together in that sorted list

## An Index Built from Every Tail

**Tags:** `core idea` (blue), `every position` (green), `substring search` (orange)

- **The task** — find every place "ana" appears inside the text "banana"
- **A suffix** — the tail of the text starting at position k: "banana" has 6 of them, one per letter
- **All six** — banana (0), anana (1), nana (2), ana (3), na (4), a (5)
- **The move** — sort those tails alphabetically and remember only their starting positions
- **The payoff** — every tail that begins with "ana" now sits in one contiguous run of the sorted list
- **The name** — that list of starting positions in sorted-tail order IS the suffix array

*Example (italic):* Any occurrence of "ana" is the start of some tail that begins with "ana" — so finding a substring becomes finding a run in a sorted list.

**Key point:** A suffix array is the text's tails sorted alphabetically, stored as starting positions — it turns "search anywhere inside the text" into "binary-search a sorted list".

### Visualization (canvas `c1`, 720×300)

Table-style diagram of the six unsorted suffixes of "banana" with their starting positions.

- **Title (bold 15px, `#1a5276`, top center):** 'The Six Tails of "banana"'.
- **Text strip:** the word "banana" as 6 letter boxes (34×34, 2px `#2a78d6` stroke, bold 16px letters) starting at x=95, y=52; 11px `#6b7280` position labels "0"–"5" under each box.
- **Suffix rows:** six rows listed at x=380 (left-aligned), y = `[120, 148, 176, 204, 232, 260]`; each row: bold 12px `#1a5276` position tag "start 0:" … "start 5:" at x=380, then the suffix in 14px monospace `#2c3e50` at x=450: "banana", "anana", "nana", "ana", "na", "a".
- **Tail arrows:** thin 1.5px `#6b7280` curved lines from letter-box k to row k's position tag, drawn for positions 0, 3, and 5 only (avoid clutter), each with a small arrowhead.
- **Highlight:** rows "anana" (start 1) and "ana" (start 3) get a 12px-tall underline in `rgba(0,131,0,0.25)` beneath the first three characters, previewing the "ana"-prefixed tails.
- **Annotation (bold 12px `#008300`, near x=520, y=95):** 'two tails start with "ana" — positions 1 and 3'.
- **Caption (12px `#444`, bottom right):** "a text of n letters has exactly n tails".

## Sorting the Tails, Then Cutting to the Run

**Tags:** `worked example` (blue), `binary search` (green)

- **Sort them** — alphabetically the tails read: a, ana, anana, banana, na, nana
- **Keep positions** — their starting positions in that order form the suffix array: 5, 3, 1, 0, 4, 2
- **Search "ana"** — binary search for the first tail ≥ "ana": row 1 ("ana") — and the run begins
- **The run** — rows 1 and 2 ("ana", "anana") both start with "ana"; row 3 ("banana") does not
- **Read answers** — the run's positions are 3 and 1: "ana" occurs at positions 1 and 3 of "banana"
- **Check by eye** — b-ANA-na and ban-ANA both read "ana"; the array found both without scanning

*Example (italic):* The whole search touched a handful of rows in a 6-row sorted list — never the text itself — and returned every occurrence at once.

**Key point:** Sorted order clusters every "ana"-prefixed tail into one run — binary search finds the run's edges, and the run's positions are all the matches.

### Visualization (canvas `c2`, 720×300)

The sorted suffix column with the suffix-array values, the "ana" run highlighted, and binary-search probes drawn.

- **Title (bold 15px, `#1a5276`, top center):** 'Sorted Tails: the "ana" Run Sits at Rows 1–2'.
- **Sorted rows:** six rows at y = `[80, 112, 144, 176, 208, 240]`; each row: 12px `#6b7280` row index "row 0"–"row 5" at x=150 (right-aligned), the tail in 14px monospace at x=175 (left-aligned): "a", "ana", "anana", "banana", "na", "nana"; then bold 13px `#1a5276` array value in a small box (36×24, 1.5px `#6b7280` stroke) at x=330: values 5, 3, 1, 0, 4, 2.
- **Array label:** bold 12px `#1a5276` header "suffix array" centered above the value boxes at (348, 62).
- **Run highlight:** rounded rectangle behind rows 1–2 (x=140 to x=378, y=96 to y=158), fill `rgba(0,131,0,0.10)`, 2px `#008300` stroke; bold 12px `#008300` label 'the "ana" run' at (258, 90) — drawn behind row text.
- **Binary-search probes (right side):** three probe arrows in 2px `#d95926` from a vertical "probe" track at x=470 to rows 3, 1, and 2 in that order, labeled 12px orange "1: banana — too far", "2: ana — first hit", "3: anana — still ana"; labels left-aligned at x=485, at the arrow root heights.
- **Result strip:** bold 13px `#008300` at (360, 283): "matches at positions 3 and 1".
- **Caption (12px `#444`, bottom right at y=283 — omit if it collides; place at x=690 right-aligned only if space):** none (result strip carries it).

## Three Billion Letters, Thirty-Two Probes

**Tags:** `where it's used` (blue), `full-text search` (green), `genomes` (orange)

- **Documents** — "find this phrase anywhere" over a big text: the array answers without rescanning
- **Genomes** — DNA is one 3-billion-letter text with no words; substring search is the ONLY search
- **Autocomplete** — "…ana…" matches mid-word because every position, not every word, is indexed
- **The cost** — a lookup is ~pattern length × log₂(text length): about 32 probe rounds at 3 billion
- **The scan** — checking every position instead means billions of comparisons per query
- **The price** — one integer per letter of text, paid once when the array is built

*Example (italic):* Finding a 20-letter DNA fragment in a 3-billion-letter genome takes ~32 binary-search rounds instead of a 3-billion-position scan.

**Key point:** For texts without word boundaries — genomes, logs, code — the suffix array is the standard way to make "find this fragment" fast.

### Visualization (canvas `c3`, 720×300)

Bar chart comparing positions examined per query: full scan versus suffix-array binary search, at growing text sizes.

- **Title (bold 15px, `#1a5276`, top center):** "Positions Examined per Query: Scan vs Suffix Array".
- **Axes:** baseline 2px `#999` at y=240 from x=90 to x=660; y is log-scaled by category (no numeric axis) — bar heights hand-set; three x groups centered at x = `[210, 400, 590]` labeled "1 million letters", "100 million", "3 billion (genome)" in 12px `#444` at y=258.
- **Scan bars:** heights `[120, 145, 170]` px (tops at y = 120/95/70), fill `rgba(217,89,38,0.35)`, 2px `#d95926` stroke, bold 12px `#d95926` labels above: "~1,000,000", "~100,000,000", "~3,000,000,000".
- **Array bars:** fixed height 26px (top y=214), fill `rgba(0,131,0,0.25)`, 2px `#008300` stroke, bold 12px `#008300` labels above: "~20 rounds", "~27 rounds", "~32 rounds"; bars 62px wide, scan bar left / array bar right of each group center, 14px apart.
- **Annotation (bold 13px `#008300`, near x=375, y=190):** "the green bars barely notice the size".
- **Caption (12px `#444`, bottom right):** "heights not to scale — labels carry the numbers; illustrative".

## It Indexes Positions, Not Words

**Tags:** `common mistake` (red), `not a dictionary` (orange)

- **The mix-up** — people picture a sorted list of the text's WORDS; that finds only whole words
- **The truth** — the array indexes every POSITION, so "ana" is found though it is no word at all
- **Storage worry** — "sorting all tails must copy tons of text" — no: it stores one integer per letter
- **No copies** — the tails are never written out; comparing two tails just reads the original text
- **The limit** — it serves one fixed text; heavy edits to the text mean rebuilding the array

*Example (italic):* The array for "banana" is just the six integers 5, 3, 1, 0, 4, 2 — the tails in c1 were only ever drawn for the reader, never stored.

**Common mistake:** Confusing a suffix array with a word index — and overestimating its size. It finds any substring, word or not, and costs one integer per character, not one copy per tail.

### Visualization (canvas `c4`, 720×300)

Two-panel contrast: a word index missing "ana" versus the position index finding it, with the storage note.

- **Title (bold 15px, `#1a5276`, top center):** 'Why a Word Index Cannot Find "ana"'.
- **Panels:** rounded rectangles 300×175 at (35, 55) and (385, 55); left stroked 2px `#d95926` titled "word index" (bold 13px `#d95926` at top center y=77), right stroked 2px `#008300` titled "position index (suffix array)" (bold 13px `#008300`).
- **Left panel content:** 13px monospace `#2c3e50` entries centered at (185, 115): "banana"; 12px `#6b7280` note at (185, 140): "one entry — one word"; bold 13px `#d95926` verdict at (185, 185): '"ana"? not found'; small red cross (two 2.5px `#e74c3c` strokes) at (185, 205).
- **Right panel content:** 13px monospace `#2c3e50` entry centered at (535, 115): "5, 3, 1, 0, 4, 2"; 12px `#6b7280` note at (535, 140): "six integers — six positions"; bold 13px `#008300` verdict at (535, 185): '"ana"? positions 1 and 3'; small green check (2.5px `#008300` strokes) at (535, 205).
- **Storage strip:** bold 12px `#4a3aa7` centered at (360, 255): "storage: one integer per letter — the tails themselves are never copied".
- **Caption (12px `#444`, bottom right):** "same text, two very different indexes".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the displayed CSS width × `devicePixelRatio` (sharp-rendering pattern) and scales the context; chart functions are pushed into a `__charts` array, run once, and re-run on window resize debounced 150 ms.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** the text "banana", its six suffixes, the sorted order (a, ana, anana, banana, na, nana), the suffix array `[5, 3, 1, 0, 4, 2]`, and the matches at positions 1 and 3 are exact and MUST agree between text and charts; `c3` bar heights are explicitly not to scale (labels carry the numbers) and say so in the caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
