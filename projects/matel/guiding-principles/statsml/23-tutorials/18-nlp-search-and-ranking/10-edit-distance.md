# Edit Distance

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Edit Distance

**Subtitle:** Edit distance counts the fewest single-letter fixes — insert a letter, delete a letter, or swap one for another — needed to turn one word into another, and a small grid of numbers finds that count for you

## Fixing "kofee" One Keystroke at a Time

**Tags:** `core idea` (blue), `three moves` (green), `fewest fixes` (orange)

- **The typo** — you text a friend "kofee?" and your phone quietly wonders how far that is from "coffee"
- **Fix one** — swap the k for a c: "kofee" becomes "cofee", one edit spent
- **Fix two** — insert a second f: "cofee" becomes "coffee", two edits total
- **The three moves** — every fix is an insert, a delete, or a swap; nothing else is allowed
- **The distance** — no path of fixes is shorter than 2, so the edit distance is exactly 2
- **Why fewest** — a clumsy path could take 5 edits; the distance only counts the best one

*Example (italic):* "kofee" is 2 edits from "coffee" but 5 edits from "muffin" — that is why the phone suggests coffee and not muffin.

**Key point:** Edit distance is the smallest number of insert/delete/swap moves that turns one string into the other — a score of how far apart two spellings are.

### Visualization (canvas `c1`, 720×300)

Step-ladder diagram: the three word stages drawn as rows of letter tiles, with labeled arrows showing each single edit and a running edit counter on the right.

- **Title (bold 15px, `#1a5276`, top center):** "From 'kofee' to 'coffee' in 2 Moves".
- **Letter tiles:** each letter a 36×36 rounded box (1px `#e5e9ef` border, white fill) with a bold 18px `#2c3e50` centered letter; rows centered on x=330.
- **Row 1 (tile tops y=60):** tiles `["k","o","f","e","e"]`; the "k" tile gets an orange `#d95926` 2px border and orange letter; 12px `#6b7280` label "what you typed" at x=60, vertically centered on the row.
- **Row 2 (tile tops y=135):** tiles `["c","o","f","e","e"]`; the "c" tile gets a blue `#2a78d6` 2px border and blue letter; label "after edit 1" at x=60.
- **Row 3 (tile tops y=210):** tiles `["c","o","f","f","e","e"]`; the second "f" tile gets a green `#008300` 2px border and green letter; label "after edit 2" at x=60.
- **Arrows:** 2px `#1a5276` vertical arrows with small arrowheads at x=200, from row 1 to row 2 (y 96→135) and row 2 to row 3 (y 171→210); beside each arrow a bold 12px label — blue `#2a78d6` "swap k → c" and green `#008300` "insert f".
- **Edit counter (right side, x=590):** 13px `#444` lines "edits: 1" at y=120 and "edits: 2" at y=195.
- **Annotation (bold 13px violet `#4a3aa7`, near x=560, y=255):** "no shorter path exists — edit distance = 2".

## The Grid That Does the Counting: "cat" → "hats"

**Tags:** `worked example` (blue), `dynamic programming` (green)

- **The setup** — write "hats" across the top and "cat" down the side; each cell asks a smaller question
- **The meaning** — a cell holds the edit distance between the word prefixes ending at that row and column
- **The rule** — each cell = the cheapest of: cell above +1 (delete), cell left +1 (insert), diagonal +swap cost
- **Free diagonal** — when the row letter equals the column letter, the diagonal step costs 0
- **Fill it** — first row is 0,1,2,3,4; the rest fills left-to-right until the corner reads 2
- **Read it back** — the corner path spells the fixes: swap c→h, keep a, keep t, insert s

*Example (italic):* The cell for prefix "ca" vs "ha" holds 1, because one swap (c→h) already turns "ca" into "ha".

**Key point:** The bottom-right cell of the grid IS the edit distance — here 2 — and each cell only ever looks at its three neighbors, so a person can fill it by hand.

### Visualization (canvas `c2`, 720×300)

Single DP grid drawn as a table of cells: header letters for "hats" and "cat", all sixteen distance values from the hardcoded matrix, the cheapest path shaded, and a legend for the three moves.

- **Title (bold 15px, `#1a5276`, top center):** "The Edit-Distance Grid for 'cat' → 'hats'".
- **Grid geometry:** 5 columns × 4 rows of 48×48 cells, top-left cell corner at x=250, y=65; 1px `#e5e9ef` cell borders, white fill.
- **Headers:** bold 14px `#1a5276` column letters `["", "h", "a", "t", "s"]` centered 16px above each column; bold 14px `#1a5276` row letters `["", "c", "a", "t"]` 16px left of each row.
- **Cell values (13px `#2c3e50`, centered), rows top to bottom:** `[0,1,2,3,4]`, `[1,1,2,3,4]`, `[2,2,1,2,3]`, `[3,3,2,1,2]`.
- **Path shading:** fill `rgba(0,131,0,0.15)` on the path cells (row,col) = `[[0,0],[1,1],[2,2],[3,3],[3,4]]`; the final cell (3,4) also gets a green `#008300` 2px border and its "2" drawn bold 15px green.
- **Legend (left side, x=25, y=90, one line per move, 12px):** blue `#2a78d6` "↖ diagonal = swap (free if letters match)", orange `#d95926` "← from left = insert", magenta `#d55181` "↑ from above = delete"; each line prefixed by its arrow glyph in the same color.
- **Annotation (bold 13px green `#008300`, near x=90, y=245):** two lines: "bottom-right corner" / "= edit distance 2".
- **Caption (12px `#444`, bottom right):** "every cell = distance between two prefixes".

## From Spell-Check to DNA

**Tags:** `where it's used` (blue), `ranking candidates` (green), `same grid` (orange)

- **Spell-check** — the phone computes the distance from your typo to dictionary words and ranks by it
- **Closest wins** — for the typo "grap", the distance-1 words are the suggestions; distance-2 waits behind
- **Fuzzy search** — "did you mean" in search boxes is the same trick: tolerate a small edit distance
- **DNA alignment** — biologists compare gene strings like ACGT-runs; a mutation is literally one edit
- **Deduping records** — "Jon Smiht" vs "John Smith" match because their distance is small, not zero
- **The cost** — the grid does length × length work, so long strings need the smarter variants

*Example (italic):* For the typo "grap", the words grape, graph, and grab all sit at distance 1, so all three appear in the suggestion bar.

**Key point:** One grid, many jobs — anywhere two strings should count as "almost equal", edit distance is the number that says how almost.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: dictionary candidates for the typo "grap" ranked by edit distance, distance-1 bars highlighted as the suggestions.

- **Title (bold 15px, `#1a5276`, top center):** "Spell-Check Ranks Candidates for 'grap' by Edit Distance".
- **Axes:** word labels on the left at x=130 (right-aligned, 13px `#2c3e50`); x axis from x=140 to x=660, edit distance 0 to 5, tick labels "0"–"5" (12px `#444`) below the baseline at y=255; light `#e5e9ef` vertical gridlines at each tick.
- **Bars (18px tall, rounded, one row each at y = 68, 98, 128, 158, 188, 218):** words `["grape", "graph", "grab", "great", "group", "giraffe"]`, distances `[1, 1, 1, 2, 2, 4]`; distance-1 bars fill green `#008300` at 0.75 alpha, distance-2 bars fill blue `rgba(42,120,214,0.55)`, the distance-4 bar fill `#6b7280` at 0.4 alpha.
- **Value labels:** bold 12px, same hue as the bar, 8px right of each bar end: "1", "1", "1", "2", "2", "4".
- **Suggestion bracket:** dashed green `#008300` (dash 4/3) rounded rectangle enclosing the three distance-1 rows (x 20–690, y 58–150); bold 12px green label at its top-right corner: "the suggestion bar".
- **Annotation (bold 13px orange `#d95926`, near x=420, y=228):** "distance 4: never suggested".
- **Caption (12px `#444`, bottom right):** "illustrative candidate list — ties at distance 1 are broken by word frequency".

## Not the Same as Counting Mismatched Letters

**Tags:** `common mistake` (red), `hamming vs edit` (orange)

- **The shortcut** — people compare words letter-by-letter in place and count mismatches (Hamming style)
- **Same length trap** — "stop" vs "tops": position-by-position, all 4 letters disagree, score 4
- **Edit view** — delete the s from the front, insert an s at the end: only 2 edits, distance 2
- **Why it differs** — inserts and deletes let letters slide over; in-place comparison cannot slide
- **Length limit** — letter-by-letter counting cannot even score words of different lengths
- **Rule of thumb** — one early insertion wrecks a positional count but costs edit distance just 1

*Example (italic):* "stop" and "tops" share almost everything, yet the positional count says 4 while the edit distance says 2 — the slide is the whole story.

**Common mistake:** Treating "number of positions where the letters differ" as edit distance. That is Hamming distance; it panics at one shifted letter, while edit distance calmly charges a single insert or delete.

### Visualization (canvas `c4`, 720×300)

Two-panel comparison of the same word pair: left panel scores "stop" vs "tops" position-by-position (4 mismatches), right panel shows the 2-edit slide (delete the leading s, insert a trailing s).

- **Title (bold 15px, `#1a5276`, top center):** "Same Words, Two Scores: Positional Count 4 vs Edit Distance 2".
- **Letter tiles:** 34×34 rounded boxes, 1px `#e5e9ef` border, bold 17px centered letters.
- **Left panel (centered on x=190):** bold 13px `#2c3e50` header "compare in place" at y=60; row "s t o p" (tile tops y=80), row "t o p s" (tile tops y=130); between the rows a red `#e74c3c` bold 14px "×" under each of the 4 mismatched columns; bold 13px red result label at y=205: "4 mismatches".
- **Right panel (centered on x=530):** bold 13px `#2c3e50` header "let letters slide" at y=60; row "s t o p" (tile tops y=80) with the "s" tile struck through by a red `#e74c3c` 2px diagonal line and 11px red label "delete" above it; row "t o p s" (tile tops y=130) with the final "s" tile in a green `#008300` 2px border and 11px green label "insert" below it; the shared letters t, o, p joined by 1px `#6b7280` connector lines sliding one tile left; bold 13px green result label at y=205: "2 edits".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=55 to y=230.
- **Annotation (bold 13px magenta `#d55181`, centered on x=360, y=265):** "insert + delete let letters slide — that is what the positional count misses".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all tiles, matrix values, path cells, candidate words, and distances are the hardcoded arrays above (no randomness); the c2 matrix is the true Levenshtein table for "cat" → "hats" (distance 2), and c3's candidate distances are true Levenshtein distances from "grap" with an "illustrative" caption for the invented candidate list.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
