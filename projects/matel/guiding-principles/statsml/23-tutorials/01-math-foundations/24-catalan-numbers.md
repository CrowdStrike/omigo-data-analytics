# Catalan Numbers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Catalan Numbers

**Subtitle:** One sequence — 1, 1, 2, 5, 14, 42, ... — counts balanced parentheses, binary tree shapes, and parse trees, all at the same time

## Three Cups, Five Legal Shifts

**Tags:** `core idea` (blue), `balanced sequences` (green)

- **The barista** — logs every action as P (put a cup on the pile) or T (take one off the top)
- **The rule** — she can never take from an empty pile, and the pile must be empty at closing
- **Three of each** — 3 puts and 3 takes can be shuffled 20 ways, but only 5 obey the rule
- **The five logs** — PPPTTT, PPTPTT, PPTTPT, PTPPTT, PTPTPT are the only legal shifts
- **Same as brackets** — write P as "(" and T as ")": the 5 legal logs are the 5 balanced strings

*Example (italic):* PTTPPT is illegal — its second take reaches for a cup when the pile is already empty.

**Key point:** The counts of legal logs for 0, 1, 2, 3, 4... pairs are 1, 1, 2, 5, 14... — the Catalan numbers. Everything on this page is C<sub>3</sub> = 5 in a different costume. (The "C3" subscripts use `<sub>` tags in the HTML.)

### Visualization (canvas `c1`, 720×300)

Small-multiples grid (3 columns × 2 rows) of pile-height "mountain paths": the 5 legal logs plus 1 illegal one, each drawn as an up/down polyline over a dashed empty-pile baseline.

- **Title (bold 15px, `#1a5276`, top center):** "3 Puts + 3 Takes: the 5 Legal Pile Paths (and 1 Illegal)".
- **Data (6 panels, label + height sequence after each action + legality):** PPPTTT `[1,2,3,2,1,0]` ok; PPTPTT `[1,2,1,2,1,0]` ok; PPTTPT `[1,2,1,0,1,0]` ok; PTPPTT `[1,0,1,2,1,0]` ok; PTPTPT `[1,0,1,0,1,0]` ok; PTTPPT `[1,0,-1,0,1,0]` illegal.
- **Layout:** panel x-origins `[45, 280, 515]`, row baselines y `[128, 232]`, step dx=30, unit height dy=18, panel width 180.
- **Baseline:** dashed `#bdc3c7` (dash 3/3) 1px horizontal line per panel from x0 to x0+180 at the baseline y (empty pile).
- **Paths:** 3px polyline starting at (x0, baseY), each of the 6 steps moving dx right to height `baseY − hs[i]·dy`; green `#008300` for legal, magenta `#d55181` for the illegal one.
- **Panel labels:** bold 12px centered under each panel at baseY+18 (legal) or baseY+34 (illegal), in the path's color; the illegal label reads "PTTPPT  ✗ dips below empty".
- **Caption (12px mute `#6b7280`, left at x=45, y=292):** "up = put a cup, down = take one; dashed line = empty pile".
- **Takeaway (bold 13px green, right-aligned at x=695, y=292):** "5 legal paths = C3".

## Counting by Splitting at the First Empty Pile

**Tags:** `worked example` (blue), `recurrence` (orange)

- **First empty** — every legal log is P + inner log + T + rest, cut where the pile first empties
- **Smaller pieces** — the inner part and the rest are both legal logs with fewer cup pairs
- **Multiply and add** — C<sub>3</sub> = C<sub>0</sub>·C<sub>2</sub> + C<sub>1</sub>·C<sub>1</sub> + C<sub>2</sub>·C<sub>0</sub> = 2 + 1 + 2 = 5
- **Base case** — C<sub>0</sub> = 1 counts the empty log, and it makes every product work out
- **One more step** — C<sub>4</sub> = 1·5 + 1·2 + 2·1 + 5·1 = 14, reusing every earlier answer

*Example (italic):* In PPTTPT the pile first empties after 4 actions, so it splits as P(PT)T followed by PT.

**Key point:** A closed formula skips the recurrence: C<sub>n</sub> = (2n choose n) / (n+1). For 3 pairs that is 20 / 4 = 5 — the same answer the split gives.

### Visualization (canvas `c2`, 720×300)

Three horizontal rows of labeled boxes, each decomposing a legal log as P + inner + T + rest, with the product equation to the right and the sum below.

- **Title (bold 15px, `#1a5276`, top center):** "Splitting Every Legal Log as P + inner + T + rest".
- **Data (3 rows):** row 1 — inner "inner: 0 pairs" (width 64), rest "rest: 2 pairs" (width 140), equation "C0 · C2 = 1 · 2 = 2 logs"; row 2 — "inner: 1 pair" (102), "rest: 1 pair" (102), "C1 · C1 = 1 · 1 = 1 log"; row 3 — "inner: 2 pairs" (140), "rest: 0 pairs" (64), "C2 · C0 = 2 · 1 = 2 logs".
- **Layout:** rows at y = 58, 118, 178; boxes start at x=45, each 30px tall with 6px horizontal gap; equations left-aligned bold 13px `#2c3e50` at x=480, y+20.
- **Boxes (fill / 1.5px stroke, bold 12px centered label in the stroke color):** "P" and "T" boxes 26px wide, fill `rgba(42,120,214,0.18)`, stroke blue `#2a78d6`; inner box fill `rgba(0,131,0,0.14)`, stroke green `#008300`; rest box fill `rgba(217,89,38,0.14)`, stroke orange `#d95926`.
- **Caption (12px mute, left at x=45, y=232):** "the T closes the very first return to an empty pile".
- **Result (bold 16px green, centered, y=268):** "C3 = 2 + 1 + 2 = 5".
- **Sub-caption (12px mute, centered, y=288):** "check with the formula: (6 choose 3) / 4 = 20 / 4 = 5".

## The Same Five as Trees and Parse Trees

**Tags:** `where it's used` (blue), `binary trees` (green), `parse trees` (orange)

- **Bracketings** — fully bracketing a·b·c·d can be done in exactly 5 ways, no more
- **Parse trees** — each bracketing draws one parse tree, so 4 leaves give 5 tree shapes
- **The bridge** — each legal cup log encodes one tree shape, so both families count to C<sub>n</sub>
- **Compilers** — an ambiguous grammar can hand one expression Catalan-many parse trees
- **Search spaces** — matrix-chain multiplication explores Catalan-many bracketings of a product

*Example (italic):* (a·(b·c))·d and a·((b·c)·d) use the same letters but are different parse trees.

**Key point:** Parentheses, cup logs, binary trees, parse trees — one sequence counts them all. Seeing 1, 2, 5, 14, 42 in a count is a Catalan fingerprint.

### Visualization (canvas `c3`, 720×300)

Five binary parse trees drawn side by side, one per full bracketing of a·b·c·d, each with its bracketing formula below.

- **Title (bold 15px, `#1a5276`, top center):** "The 5 Parse Trees of a·b·c·d — One per Bracketing".
- **Data (nested arrays, internal node = [left, right], leaf = letter):** `[[['a','b'],'c'],'d']` labeled "((a·b)·c)·d"; `[['a',['b','c']],'d']` labeled "(a·(b·c))·d"; `[['a','b'],['c','d']]` labeled "(a·b)·(c·d)"; `['a',[['b','c'],'d']]` labeled "a·((b·c)·d)"; `['a',['b',['c','d']]]` labeled "a·(b·(c·d))".
- **Layout:** tree root centers at x = 90, 225, 360, 495, 630, roots at y=64; recursive draw with initial horizontal spread 32, each level down adds 36 to y and multiplies spread by 0.55; edges drawn to cy−8.
- **Nodes:** internal nodes are filled blue `#2a78d6` circles radius 5 with 1.5px ink `#1a5276` edges; leaves are bold 13px green `#008300` letters.
- **Bracketing labels:** bold 12px `#2c3e50`, centered under each tree at y=222.
- **Caption (12px mute, left at x=45, y=258):** "blue dot = a multiply, green letter = a leaf".
- **Takeaway (bold 13px green, centered, y=284):** "5 trees, 5 bracketings, 5 cup logs — the same C3 = 5 every time".

## The Orderings That Don't Count

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Wrong guess #1** — 2^6 = 64 strings of 6 letters ignores that a shift has exactly 3 of each
- **Wrong guess #2** — 20 shuffles of 3 puts and 3 takes still overcounts: 15 break the rule
- **Exact share** — exactly 1 in n+1 shuffles is legal: 5 of 20 for 3 pairs, 14 of 70 for 4
- **Shrinking odds** — the legal share falls as pairs grow: 50%, 33%, 25%, 20% for 1 to 4 pairs
- **Sizing trap** — estimating a tree search space with (2n choose n) overstates it by (n+1)×

*Example (italic):* A random shuffle of 3 puts and 3 takes has only a 25% chance of being a legal shift.

**Common mistake:** Catalan numbers count constrained orderings, not all orderings. Take the central binomial (2n choose n) and divide by n+1 — forgetting the division inflates every estimate.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart comparing all shuffles (central binomial) vs legal logs (Catalan) for 1–4 pairs, with the legal share fraction annotated above each group.

- **Title (bold 15px, `#1a5276`, top center):** "All Shuffles vs Legal Logs: Only 1 in n+1 Survives".
- **Data:** totals `[2, 6, 20, 70]`; legal `[1, 2, 5, 14]`; share labels `['1/2 = 50%', '1/3 ≈ 33%', '1/4 = 25%', '1/5 = 20%']`; x labels `['1 pair', '2 pairs', '3 pairs', '4 pairs']`.
- **Axes:** L-shaped axis in `#999` 1px; origin x=60, plot width 620, baseline y=240, chart height 165, value scale max 75.
- **Bars:** per group centered at `rx + (i+0.5)·(rw/4)`, bar width 34, gap 8; left bar (totals) fill `rgba(42,120,214,0.45)`, right bar (legal) fill `rgba(0,131,0,0.5)`; value labels bold 12px above each bar top (blue `#2a78d6` for totals, green `#008300` for legal); x labels 12px `#444` at baseY+18.
- **Share annotations:** magenta `#d55181` bold 13px, centered above each group at `baseY − max(totalBarHeight, 30) − 24`.
- **Legend (top left):** 14×14 swatches at (70,44) and (70,64) with 12px `#2c3e50` labels "all shuffles: (2n choose n)" and "legal logs: Cn".
- **Takeaway (bold 13px magenta, centered, y=282):** "legal share = 1 / (n+1) — the gap widens as n grows".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Catalan subscripts (C<sub>3</sub>, C<sub>0</sub>, etc.) use `<sub>` tags; middle dots use `&middot;`.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
