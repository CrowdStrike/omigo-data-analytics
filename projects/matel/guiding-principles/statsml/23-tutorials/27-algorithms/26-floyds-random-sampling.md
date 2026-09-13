# Floyd's Random Sampling

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Floyd's Random Sampling

**Subtitle:** Robert Floyd's trick draws k distinct items from 1..n in exactly k random draws — no retries, no shuffle, and memory for only the k picks

## Three Raffle Winners Without a Shuffle

**Tags:** `core idea` (blue), `running example` (green), `exactly k draws` (orange)

- **The raffle** — a company raffle must pick 3 distinct winners from tickets numbered 1 to 8
- **The redraw way** — draw a random ticket; if it already won, toss it back and draw again
- **Retries pile up** — the more winners you hold, the more often a draw hits one already picked
- **The shuffle way** — shuffle all 8 tickets and take the top 3: fair, but it touches every ticket
- **Floyd's trick** — make exactly 3 draws, no retries, no shuffle: each draw adds one new winner
- **The memory** — remember only the winners picked so far, in a small hash set

*Example (italic):* Eight tickets in a hat: the redraw way can fish out ticket 4 twice and waste a turn; Floyd's never wastes a draw.

**Key point:** Floyd's algorithm picks k distinct items from 1..n, uniformly, in exactly k random draws — storing only the k picks, never touching the rest.

### Visualization (canvas `c1`, 720×300)

Three-way comparison strip: one row of ticket squares per method (naive redraw with a retry, full shuffle touching all 8, Floyd's exactly 3 draws).

- **Title (bold 15px, `#1a5276`, top center):** "One Raffle, Three Ways to Pick 3 Winners of 8".
- **Shared square helper `cell`:** squares 34×34, spacing 44px, first square at x=170; centered bold 12px number inside; row tops at y = 70, 140, 210.
- **Row labels (bold 12px `#2c3e50`, left-aligned at x=12, vertically centered on each row):** "naive redraw", "full shuffle", "Floyd's".
- **Row 1 (naive redraw, y=70):** draw sequence `[4, 7, 4, 2]`; squares 1, 2, 4 (values 4, 7, 2) filled `rgba(0,131,0,0.12)` with 2px `#008300` border and green numbers; square 3 (the repeated 4) filled `rgba(217,89,38,0.15)` with 2px `#d95926` border, orange number, and a bold 11px orange "retry" label centered 14px below it; note text 12px `#2c3e50` left-aligned at x=370, row-centered: "4 draws for 3 winners — one wasted".
- **Row 2 (full shuffle, y=140):** shuffled order `[4, 7, 2, 5, 1, 8, 3, 6]` as 8 squares; the first 3 filled `rgba(0,131,0,0.12)` with 2px `#008300` border and green numbers (the kept winners); the other 5 filled `rgba(42,120,214,0.12)` with 1.5px `#2a78d6` border and blue numbers; note text 12px `#2c3e50` left-aligned at x=546, row-centered: "all 8 moved, keep 3".
- **Row 3 (Floyd's, y=210):** winners `[4, 7, 2]` as 3 green squares (same green style as above).
- **Annotation (bold 13px green `#008300`, left-aligned at x=320, centered on row 3):** "exactly 3 draws — never a retry, never a shuffle".
- **Caption (12px `#444`, bottom right):** "draw sequences illustrative".

## Three Rounds: j = 6, 7, 8

**Tags:** `worked example` (green), `fixed draws` (orange)

- **The loop** — run j = 6, 7, 8 — that is j = n−k+1 up to n, one round per winner (rendered with `&minus;` in HTML)
- **Each round** — draw a random ticket t between 1 and j, then check the winner set
- **Round j=6** — draw t=4; ticket 4 is not a winner yet, so ticket 4 wins
- **Round j=7** — draw t=4 again; 4 already won, so ticket j=7 itself wins instead
- **Round j=8** — draw t=2; ticket 2 is free, so it wins — final winners {4, 7, 2}
- **The count** — exactly 3 draws, zero retries; every step is hand-checkable

*Example (italic):* The winner bitmap fills {4} → {4, 7} → {4, 7, 2} in three draws — the collision at j=7 costs nothing. (arrows rendered with `&rarr;` in HTML)

**Key point:** One round per pick: if the drawn ticket t is free it wins; if t is already taken, the round's own ticket j wins — either way the set grows by exactly one.

### Visualization (canvas `c2`, 720×300)

Three rows, one per round j = 6, 7, 8: each shows the winner bitmap of tickets 1..8 with the 1..j range active, the drawn t outlined, the collision redirected, and the winners filling in.

- **Title (bold 15px, `#1a5276`, top center):** "Three Rounds Fill the Winner Bitmap: Draws 4, 4, 2".
- **Bitmap cells:** 32×32 squares, spacing 40px, first cell at x=150 (cell for ticket i at x = 150 + (i−1)·40); row tops at y = 64, 140, 216; each cell shows its ticket number.
- **Round data (hardcoded array):** `{j:6, t:4, before:[], win:4, collide:false}`, `{j:7, t:4, before:[4], win:7, collide:true}`, `{j:8, t:2, before:[4,7], win:2, collide:false}`.
- **Left labels per row (x=14):** bold 13px `#1a5276` "j = 6" / "j = 7" / "j = 8" on the first line; 12px `#2c3e50` "draw t = 4" / "draw t = 4" / "draw t = 2" on the second.
- **Cell states:** tickets > j: fill `#f2f3f5`, 1px dashed (dash 3/3) `#c9ced6` border, `#aab0b8` number (out of draw range); winners (prior + this round's `win`): fill `rgba(0,131,0,0.18)`, 2px `#008300` border, green number; all other tickets ≤ j: white fill, 1.5px `#2a78d6` border, `#6b7280` number.
- **Drawn-ticket outline:** 3px rectangle 3px outside cell t — green `#008300` in rounds 1 and 3 (fresh draw), orange `#d95926` in round 2 (collision).
- **Collision redirect (row 2 only):** 2px orange quadratic curve from the top of cell 4 (center x=286) to the top of cell 7 (center x=406), control point at the midpoint, y = rowTop−30, ending in an orange arrowhead; bold 11px orange label centered above at (346, rowTop−34): "already a winner → j wins".
- **Outcome text per row (bold 12px, left-aligned at x=486, row-centered):** green "4 is free → winner 4"; orange "4 taken → j = 7 wins"; green "2 is free → winner 2".
- **Annotation (bold 13px green `#008300`, left-aligned at x=150, y=286):** "3 draws, 3 winners, 0 retries".
- **Caption (12px `#444`, bottom right):** "draws fixed at 4, 4, 2 for illustration — grey cells are outside 1..j".

## Sampling 1,000 Ids From 10 Million

**Tags:** `where it's used` (blue), `speed` (green), `vs reservoir` (orange)

- **The pilot** — pick 1,000 user ids from 10 million: 1,000 draws and a 1,000-entry set
- **No table shuffle** — a full shuffle touches all 10 million rows just to keep the first 1,000
- **Redraw degrades** — as k nears n, almost every draw hits a taken id and becomes a retry
- **Floyd's stays flat** — exactly k draws no matter how close k gets to n
- **Known n needed** — Floyd's needs the total count and random access to ids 1..n
- **Streams differ** — reservoir sampling is for streams with unknown n; different problem

*Example (italic):* Sampling 900 of 1,000 ids: the redraw way expects about 2,300 draws; Floyd's needs exactly 900.

**Key point:** When n is known and ids are addressable, Floyd's gives a fair k-sample for the cost of k draws — reach for reservoir sampling only when n is unknown.

### Visualization (canvas `c3`, 720×300)

Line chart of expected draws as the sample size k grows toward n = 1,000: the redraw curve explodes near k = n while Floyd's is the straight line draws = k.

- **Title (bold 15px, `#1a5276`, top center):** "Draws Needed to Sample k of 1,000: Redraw vs Floyd’s" (curly apostrophe).
- **Axes:** origin x=80, baseline y=245, plot top y=60, plot width 580; y = draws, 0 at baseline to 8,000 at y=60, linear; light `#e5e9ef` gridlines at 2,000 / 4,000 / 6,000 / 8,000 with right-aligned 12px `#444` labels "2,000", "4,000", "6,000", "8,000"; gray `#999` L-shaped axis frame.
- **X mapping:** x(k) = 100 + (k−100)/900 × 560; ticks at k = `[100, 250, 500, 750, 900, 1000]` labeled "100", "250", "500", "750", "900", "1,000" (12px `#444`, centered, y=263); axis caption "sample size k" centered at (370, 281).
- **Redraw line (orange `#d95926`, 3px, 4.5px dots):** expected draws `[105, 288, 693, 1386, 2303, 7485]` at those k values; 12px orange value labels only at k=900 ("2,303", right-aligned 8px left of the dot, 6px above) and k=1,000 ("7,485", right-aligned 8px left of the dot).
- **Floyd's line (green `#008300`, 3px, 4.5px dots):** draws `[100, 250, 500, 750, 900, 1000]` (a straight line = k); 12px green label "1,000" just right of the final dot.
- **Annotation (bold 13px orange, left-aligned at x=340):** two lines at y=88 and y=106: "redraw explodes near k = n:" / "the last id alone averages 1,000 draws".
- **Annotation (bold 13px green, left-aligned at x=130, y=208):** "Floyd’s: draws = k, a straight line" (curly apostrophe).
- **Caption (12px `#444`, bottom right):** "expected draws, n = 1,000 — illustrative".

## Doesn't Ticket j Get an Unfair Boost?

**Tags:** `common mistake` (red), `fairness` (orange)

- **The worry** — "if t is taken, j wins instead" looks like it hands ticket j extra chances
- **The late start** — j is only drawable from its own round on; the redirect pays that back
- **Count it** — at j=7, ticket 7 wins if t=7 or t hits the one taken ticket: chance 2/7
- **Same for all** — after that round every ticket 1..7 holds a win with exactly 2/7
- **All subsets** — every one of the 56 possible 3-of-8 winner sets is equally likely
- **A set, not an order** — the output is unordered; a variant also randomizes the order

*Example (italic):* Ticket 7 never appears in round j=6, so the collision case at j=7 gives it exactly the probability it missed.

**Common mistake:** Reading the redirect as a bias toward j. It is the exact correction for j's late start — the reason all C(n,k) subsets come out equally likely.

### Visualization (canvas `c4`, 720×300)

Single-round diagram of the collision at j = 7: the draw arrow lands on taken ticket 4, a curved arrow redirects the win to ticket 7, and an equal-chance strip shows every in-range ticket at 2/7.

- **Title (bold 15px, `#1a5276`, top center):** "The Collision Round j = 7: a Redirect, Not a Bias".
- **Bitmap cells:** 40×40 squares, spacing 52px, first cell at x=140, all tops at y=140 (ticket i center x = 140 + (i−1)·52 + 20; cell 4 center x=316, cell 7 center x=472); each cell shows its ticket number.
- **Cell states:** ticket 8 out of range (fill `#f2f3f5`, 1px dashed `#c9ced6`, `#aab0b8` number); ticket 4 a prior winner and ticket 7 the new winner (fill `rgba(0,131,0,0.18)`, 2px / 2.5px `#008300` border, green number); tickets 1, 2, 3, 5, 6 free (white fill, 1.5px `#2a78d6` border, `#6b7280` number).
- **Draw arrow:** bold 13px `#1a5276` label "draw t = 4" left-aligned at (150, 74); 2.5px orange `#d95926` vertical arrow from (316, 80) down to the top of cell 4, with an orange arrowhead.
- **Collision ring:** 3px orange rectangle 4px outside cell 4; bold 11px orange "taken" centered 16px below the cell.
- **Redirect curve:** 2.5px green `#008300` quadratic curve from the top-right of cell 4 to the top of cell 7, control point at the x-midpoint, y = 96, ending in a green arrowhead; bold 12px green label centered at (midpoint+30, 92): "the win goes to j = 7".
- **Equal-chance strip (11px, 34px below the cells):** "2/7" in `#6b7280` centered under each of tickets 1..7; "—" in `#aab0b8` under ticket 8; caption 11px `#6b7280` centered at (360, 230): "chance of holding a win after this round".
- **Annotation (bold 13px violet `#4a3aa7`, centered):** two lines at y=258 and y=276: "every ticket 1..7 now holds a win with the same chance 2/7" / "this redirect is exactly what keeps all 56 subsets equally likely".
- **Caption (12px `#444`, bottom right):** "one round shown — draw illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" in sections 1–3, "Common mistake:" in section 4).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to displayed width × `window.devicePixelRatio` and calls `ctx.scale`; all charts live in a `__charts` array of functions run once on load and re-run on window resize via a 150ms debounced handler. Shared helpers: `arrowHead(ctx, x, y, angle, color)` filled triangular head (size 7, spread 0.45 rad) and `cell(ctx, x, y, size, num, fill, stroke, lw, txtColor, dashed)` numbered square.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all draw sequences, round states, cell coordinates, and the c3 arrays are the hardcoded literals above (no `Math.random()` — the illustrative draws 4, 4, 2 are fixed). The worked-example numbers must agree everywhere: winners {4, 7, 2} in the text, in c1's Floyd row, and in c2; the collision t=4 → winner 7 in c2 and c4; the 2,303-draw figure in the section-3 example and in c3; the 56-subsets count in the section-4 bullets and c4's annotation.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
