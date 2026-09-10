# Mathematical Induction

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Mathematical Induction

**Subtitle:** The domino argument — prove the first case, prove each case knocks over the next, and a claim is true for every number at once

## A Row of Eight Dominoes

**Tags:** `core idea` (blue), `domino argument` (green), `base case + step` (orange)

- **The setup** — 8 dominoes stand in a row, each placed close enough to knock over the next
- **The push** — you tip only domino 1; you never touch dominoes 2 through 8 yourself
- **The chain** — domino 1 hits 2, 2 hits 3, and so on until domino 8 lands flat
- **Two facts, all fall** — "1 falls" plus "every faller knocks its neighbor" topples all 8
- **The definition** — induction proves a claim for all n from one base case and one step rule
- **What you never do** — you never check n = 500 by hand; the step rule reaches it for you

*Example (italic):* You touch one tile, and all 8 end up flat — the other 7 fell by spacing alone, not by hand.

**Key point:** Prove P(1) — the push. Prove P(k) implies P(k+1) — the spacing. Together they prove P(n) for every n, with only two proofs.

### Visualization (canvas `c1`, 720×300)

Side view of 8 dominoes in a row: the first three already fallen, the fourth mid-tip, the last four standing, with a push arrow and a spacing annotation.

- **Title (bold 15px, `#1a5276`, top center):** "One Push + Even Spacing = All 8 Fall".
- **Data:** 8 dominoes at base-x positions `[80, 160, 240, 320, 400, 480, 560, 640]`, floor line y=230 from x=50 to x=690 (2px `#999`); each domino 18px wide, 80px tall.
- **Fallen (1–3):** drawn as 80×18 rectangles lying flat rightward from their base-x, fill `rgba(42,120,214,0.45)`, 2px blue `#2a78d6` border; numbers "1", "2", "3" bold 12px `#2a78d6` centered on each.
- **Tipping (4):** rectangle rotated 40° clockwise about its base point, fill `rgba(217,89,38,0.5)`, 2px orange `#d95926` border, bold 12px "4" label.
- **Standing (5–8):** upright 18×80 rectangles, fill `rgba(0,131,0,0.35)`, 2px green `#008300` border, numbers "5"–"8" bold 12px `#008300` centered.
- **Push arrow:** orange `#d95926` 3px arrow from (40, 130) to (74, 155), bold 13px orange label "base case: tip #1" above it at y=118.
- **Spacing annotation:** ink `#1a5276` bold 13px text "step: each faller reaches the next" centered above dominoes 5–7 at y=110, with a thin dashed `#bdc3c7` bracket (dash 4/3) from x=480 to x=640 at y=125.
- **Caption (12px `#444`, bottom center y=280):** "you only prove two things — the push and the spacing".

## Building Squares from Odd Numbers

**Tags:** `worked example` (blue), `base case + step` (orange)

- **The claim** — the first n odd numbers always sum to n²: 1+3+5+7+9 = 25 = 5²
- **Base case** — for n=1 the sum is just 1, and 1² = 1, so the first domino falls
- **The hypothesis** — assume some k works: k² tiles arranged as a full k×k square
- **The knock** — glue on an L of 2k+1 tiles: k across the top, k down the right, 1 corner
- **The algebra** — k² + (2k+1) = (k+1)², which is exactly the claim at k+1
- **The result** — the square grows to (k+1)×(k+1), so k working forces k+1 to work

*Example (italic):* A 3×3 block of 9 tiles plus an L of 7 tiles makes 16 = 4²; add 9 more and you get 25 = 5².

**Key point:** The proof is two lines: 1 = 1², and k² + (2k+1) = (k+1)². Every n from 1 to a million is certified by those two lines.

### Visualization (canvas `c2`, 720×300)

Five tile squares of growing size (1×1 up to 5×5) drawn left to right, each new L-shaped layer in its own color, showing 1, 1+3, +5, +7, +9 building perfect squares.

- **Title (bold 15px, `#1a5276`, top center):** "1 + 3 + 5 + 7 + 9 = 5×5: Each Odd Number Is One L-Layer".
- **Data:** squares of side `[1, 2, 3, 4, 5]` cells; cell size 22px with 2px gaps; squares bottom-aligned at y=235, left edges at x `[60, 155, 275, 420, 585]` (illustrative layout, exact data).
- **Layer colors:** layer 1 (the 1×1 corner cell in every square) blue `#2a78d6`; layer 2 (the 3-cell L) green `#008300`; layer 3 (5-cell L) orange `#d95926`; layer 4 (7-cell L) violet `#4a3aa7`; layer 5 (9-cell L) magenta `#d55181`. Each cell filled at 0.55 alpha of its layer color with a 1px solid border of the same color; layer c of square s covers cells where max(row, col) = c−1 (0-indexed from the bottom-left corner).
- **Labels below each square (bold 12px `#1a5276`, centered):** built at render time as the running total of the odd numbers `[1,3,5,7,9]` — "1 = 1²", "1+3 = 4", "+5 = 9", "+7 = 16", "+9 = 25", with each total taken from the accumulator, not typed in.
- **Arrows:** thin 2px `#6b7280` arrows between consecutive squares at y=160 with 11px `#6b7280` labels "+3", "+5", "+7", "+9" generated from the odd-number array.
- **Annotation (bold 13px magenta `#d55181`, top right, x=560, y=55):** "the new L is always 2k+1 tiles".
- **Caption (12px `#444`, bottom center y=285):** "k×k square + one L-layer = (k+1)×(k+1) square — the domino knock, drawn".

## Towers of Hanoi Needs 2ⁿ − 1 Moves

**Tags:** `worked example` (blue), `recurrence` (green), `base case + step` (orange)

- **The puzzle** — 3 pegs, n stacked disks, one move at a time, never a big disk onto a smaller one
- **Base case** — 1 disk takes 1 move, and 2¹ − 1 = 1, so the claim is true at n = 1
- **The recurrence** — clear the top k disks, move the biggest once, rebuild: M(k+1) = 2·M(k) + 1
- **The step** — put M(k) = 2ᵏ − 1 in: 2(2ᵏ − 1) + 1 = 2ᵏ⁺¹ − 1, the claim at k+1
- **The check** — the recurrence gives 1, 3, 7, 15, 31, 63, 127 and 2ⁿ − 1 gives the same values
- **The scale** — 64 disks would need 2⁶⁴ − 1 moves, which is why nobody finishes that version

*Example (italic):* 3 disks take 7 moves: 3 to park the top two on the spare peg, 1 for the big disk, 3 to stack them back.

**Key point:** The recurrence is the domino spacing written in algebra. Once M(k+1) = 2·M(k) + 1 is established, one base case fixes the whole sequence forever.

### Visualization (canvas `c3`, 720×320)

Bar chart of the move count for n = 1..7 computed by the recurrence, with hollow dots plotted from the independent closed form 2ⁿ − 1 landing on every bar top.

- **Title (bold 15px, `#1a5276`, top center):** "Recurrence M(k+1) = 2M(k) + 1 Lands Exactly on 2ⁿ − 1".
- **Data (computed in JS, no literals):** `M[1] = 1`, then `M[i] = 2*M[i-1] + 1` for i = 2..7, giving 1, 3, 7, 15, 31, 63, 127. Closed form array computed separately as `Math.pow(2, n) - 1`.
- **Axes:** baseline y=250 from x=80 to x=690 (2px `#999`); bars for n = 1..7 at 80px pitch starting x=100, bar width 46px, height = `M[n] / M[7] * 180` so the tallest bar is 180px.
- **Bars:** fill `rgba(42,120,214,0.45)`, 2px blue `#2a78d6` border; value printed bold 12px `#1a5276` above each bar, text taken from `M[n]`.
- **Closed-form dots:** hollow 6px circles, 2px magenta `#d55181` stroke, white fill, centred on each bar top at the height computed from `Math.pow(2,n)-1` (same scale) — visibly coincident with the bar tops.
- **x labels (12px `#444`, below baseline y=270):** "n=1" … "n=7", generated in the loop.
- **Legend (12px, x=110, y=60):** blue swatch + "bars: recurrence 2M+1", magenta hollow dot + "dots: 2ⁿ − 1".
- **Annotation (bold 13px `#008300`, x=360, y=88, centered):** string built at render time as `"M(7) = " + M[7] + " = 2⁷ − 1 = " + (Math.pow(2,7)-1)`.
- **Caption (12px `#444`, bottom center y=305):** "seven values agree — but agreement is evidence; the step is the proof".

## Strong Induction: Why 12 Cents Needs Four Base Cases

**Tags:** `strong induction` (blue), `multiple base cases` (orange), `worked example` (green)

- **The claim** — every postage of 12 cents or more is payable with only 4c and 5c stamps
- **Why one base case fails** — the step builds n out of n−4, so it cannot reach a single start
- **Four base cases** — 12 = 4+4+4, 13 = 4+4+5, 14 = 4+5+5, 15 = 5+5+5, each checked by hand
- **The step** — for n ≥ 16, take the stamps for n−4 and add one more 4c stamp
- **Strong induction** — the step may assume every case below n, not only the one directly before
- **The floor is real** — 6, 7 and 11 cents are impossible, so no proof could start below 12

*Example (italic):* 23 cents comes from 19 plus a 4c stamp, and 19 came from 15 = 5+5+5, so 23 = 5+5+5+4+4.

**Key point:** The number of base cases is set by how far back the step reaches. A step that looks back 4 needs 4 base cases; a Fibonacci-style step using n−1 and n−2 needs 2.

### Visualization (canvas `c4`, 720×300)

A single row of boxes for n = 4..23, colored by whether n is payable with 4c and 5c stamps (computed by dynamic programming in JS), with the four base cases marked and green +4 arcs from each base case to the case it unlocks.

- **Title (bold 15px, `#1a5276`, top center):** "Payable with 4c and 5c Stamps: Four Base Cases, Then +4 Forever".
- **Data (computed in JS):** `ok[0] = true`; for n = 1..23, `ok[n] = (n>=4 && ok[n-4]) || (n>=5 && ok[n-5])`. This makes 4, 5, 8, 9, 10 payable, 6, 7, 11 not, and everything from 12 up payable. Combos for the four base cases are found by a small search over counts of 4s and 5s and rendered as "4+4+4"-style strings.
- **Boxes:** 20 boxes for n = 4..23 at 31px pitch starting x=62, box 26px wide and 34px tall, top y=150. Payable: fill `rgba(0,131,0,0.18)`, 2px green `#008300` border. Not payable: fill `rgba(231,76,60,0.14)`, 2px `#e74c3c` border. Base cases 12–15 get a 2px violet `#4a3aa7` border and `rgba(74,58,167,0.18)` fill.
- **Box labels:** n printed bold 12px inside each box in its border color; a bold 13px `#e74c3c` "✗" sits above the three unpayable boxes at y=144.
- **Step arcs:** four green `#008300` 2px quadratic arcs above the row, from the center of 12→16, 13→17, 14→18, 15→19, apex at y=104, each with an arrowhead and an 11px green "+4" label at the apex.
- **Annotations (bold 13px, y=60):** `#e74c3c` "11 is unpayable — false below 12" at x=44 (left-aligned); `#008300` "every n ≥ 16 reuses n − 4" at x=440 (left-aligned).
- **Base-case line (bold 12px `#4a3aa7`, centered, y=218):** one line joining the four computed combo strings with three-space gaps: "12 = 4+4+4   13 = 4+4+5   14 = 4+5+5   15 = 5+5+5".
- **Caption (12px `#444`, bottom center y=275):** "the step reaches back 4, so exactly 4 base cases are needed".

## Recursion and Loop Invariants Are Induction in Code

**Tags:** `where it's used` (blue), `recursion` (green), `loop invariant` (orange)

- **Same two parts** — a recursive function's exit branch is the base case, its call is the step
- **Worked recursion** — sum(0) = 0 and sum(n) = n + sum(n−1) gives sum(4) = 10 = 4·5/2
- **Missing exit** — a recursion with no base case is an induction with no base case: it never lands
- **Loop invariants** — "the first k+1 items are sorted after pass k" is a claim indexed by k
- **Invariant base** — before the first pass a one-item prefix is trivially sorted
- **Invariant step** — one insertion turns a sorted prefix of length k into one of length k+1

*Example (italic):* Insertion sort on [5, 2, 4, 6, 1, 3] takes 5 passes, and the invariant certifies the result without tracing all 5.

**Key point:** Proving a loop correct means proving an invariant by induction on passes, then reading the answer off the invariant at the final pass.

### Visualization (canvas `c5`, 720×360)

Two panels split by a dashed divider at x=352: left, the recursion `sum(n)` unwinding to its base case and returning values; right, the insertion-sort trace with the sorted prefix boxed after each pass.

- **Title (bold 15px, `#1a5276`, top center):** "The Base Case Is the Exit Branch; the Step Is One Pass".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=352 from y=40 to h−14.
- **Left panel heading (bold 13px `#444`, x=48, y=62):** "recursion: sum(n) = n + sum(n−1)".
- **Left data (computed in JS):** call chain for n = 4 down to 0; returns computed as `r[0]=0`, `r[i]=i+r[i-1]` giving 0, 1, 3, 6, 10.
- **Left rows:** five rows at y = 100, 132, 164, 196, 228 for sum(4), sum(3), sum(2), sum(1), sum(0), each indented 14px further right (x = 60 + row*14); label 12px `#2c3e50` "sum(4)" etc. Downward grey `#6b7280` 1.5px arrows connect consecutive rows.
- **Base row:** sum(0) drawn in a 178×22 `rgba(217,89,38,0.18)` box with 2px orange `#d95926` border and bold 12px orange label "sum(0) = 0 — base case".
- **Returns:** green `#008300` bold 12px values printed at x=300 next to each row as "→ 10", "→ 6", "→ 3", "→ 1", "→ 0", each taken from the computed `r` array.
- **Left annotation (bold 12px `#008300`, x=48, y=272):** built at render time as `"sum(4) = " + r[4] + " = 4·5/2 = " + (4*5/2)`.
- **Left caption (12px `#444`, x=48, y=300):** "remove the exit branch and nothing ever returns".
- **Right panel heading (bold 13px `#444`, x=380, y=62):** "insertion sort: prefix sorted after pass k".
- **Right data (computed in JS):** start array `[5,2,4,6,1,3]` (literal, seeded); the page runs insertion sort and stores a snapshot of the array plus the sorted-prefix length after each of the 5 passes, producing rows [5,2,4,6,1,3] / [2,5,4,6,1,3] / [2,4,5,6,1,3] / [2,4,5,6,1,3] / [1,2,4,5,6,3] / [1,2,3,4,5,6].
- **Right rows:** 6 rows (start plus 5 passes) at y = 96 + row*36; each row draws 6 value cells at 34px pitch starting x=396, cell 28×26, 12px centered values.
- **Cell colors:** cells inside the sorted prefix fill `rgba(42,120,214,0.35)` with 2px blue `#2a78d6` border; the rest fill `#f4f6f8` with 1px `#d5dae0` border.
- **Prefix bracket:** a 2px blue rectangle drawn around the prefix cells of each row, with an 11px blue label at x=618 reading "sorted " + prefixLength (computed: 1, 2, 3, 4, 5, 6).
- **Row labels (11px `#6b7280`, right-aligned at x=392):** "start", "pass 1" … "pass 5".
- **Right annotation (bold 12px `#2a78d6`, x=380, y=330):** built as `"invariant holds through pass " + passes + " → all " + n + " sorted"` using the computed pass count and array length.

## The Proof That All Horses Are One Color

**Tags:** `common mistake` (red), `check the smallest k` (orange), `rule of thumb` (green)

- **The false claim** — "any n horses have the same color", supposedly proved for every n
- **Base case is fine** — 1 horse matches itself, so P(1) really is true; the flaw is elsewhere
- **The step** — from k+1 horses drop one, then drop another; both groups of k match, so all match
- **Where it breaks** — the two groups overlap in k−1 horses, and that is 0 horses when k = 1
- **No overlap, no link** — P(1) never reaches P(2), so the chain stops before its first knock
- **The mirror error** — a flawless step with no base case proves nothing, like dominoes never pushed

*Example (italic):* With 4 horses the two groups of 3 share 2 horses and the argument works; with 2 horses they share none and it collapses.

**Common mistake:** Checking the step only at a comfortable large k. The step must hold at every k including the smallest, and the induction hypothesis is a loan repaid by a real base case.

### Visualization (canvas `c6`, 720×320)

Two panels split by a dashed divider at x=300: left, dominoes with perfect spacing and no push; right, the horse-overlap diagram at n=4 (works) and n=2 (fails), with the overlap size computed.

- **Title (bold 15px, `#1a5276`, top center):** "Two Broken Proofs: No Push, and a Step That Fails at k = 1".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=300 from y=40 to h−14.
- **Left panel:** heading bold 13px `#e74c3c` "no base case" at (44, 64); floor line 2px `#999` at y=210 from x=40 to x=286; 8 upright dominoes 12px wide, 52px tall at base-x `[52, 82, 112, 142, 172, 202, 232, 262]`, fill `rgba(107,114,128,0.3)`, 2px `#6b7280` border, numbers 1–8 in 10px `#6b7280` centered.
- **Left marks:** bold 15px `#e74c3c` "✗" at (40, 150) where the push arrow would be; bold 12px `#e74c3c` two-line label at (44, 90)/(44, 106): "spacing is perfect," / "nobody tips #1 — all 8 stand".
- **Left caption (12px `#444`, x=44, y=250):** "0 of 8 proven".
- **Right panel heading (bold 13px `#444`, x=326, y=64):** "'any k+1 horses match' — drop one, drop another".
- **Right top case (n=4, works):** 4 horse circles radius 15 at y=130, centers x = 350, 410, 470, 530; fill `rgba(0,131,0,0.2)`, 2px green `#008300` border, bold 11px green labels "H1".."H4"; a blue `#2a78d6` 2px bracket under H1–H3 at y=152 labelled 11px "drop H4", a violet `#4a3aa7` 2px bracket above H2–H4 at y=104 labelled 11px "drop H1"; bold 12px `#008300` annotation at (556, 134) built as `"overlap = " + (k-1) + " horses"` with k = 3, so "overlap = 2 horses".
- **Right bottom case (n=2, fails):** 2 horse circles radius 15 at y=230, centers x = 380, 460; fill `rgba(231,76,60,0.16)`, 2px `#e74c3c` border, bold 11px red labels "H1", "H2"; brackets under H1 only (blue, y=252, "drop H2") and above H2 only (violet, y=204, "drop H1"); bold 12px `#e74c3c` annotation at (496, 234) built as `"overlap = " + (k-1) + " horses"` with k = 1, so "overlap = 0 horses".
- **Right caption (12px `#444`, x=326, y=292):** "the step is only valid for k ≥ 2, so it never leaves P(1)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then six `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** logical sizes 720×300 (`c1`, `c2`, `c4`), 720×320 (`c3`, `c6`), 720×360 (`c5`); shared `setup(id, W, H)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Computed-not-asserted rule:** every printed number in `c2`–`c6` is derived in JS at render time — running totals of the odd numbers, the Hanoi recurrence and its closed form, the stamp DP and combo search, the recursive `sum` returns, the insertion-sort trace and prefix lengths, and the horse-overlap size k−1.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
