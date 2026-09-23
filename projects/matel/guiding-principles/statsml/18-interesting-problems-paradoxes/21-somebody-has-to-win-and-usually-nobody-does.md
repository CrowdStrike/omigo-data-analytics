# "Somebody Has to Win" — And Usually Nobody Does

**Page type:** detail page (h2-sectioned two-column obj-table layout: text left 50%, canvas right 50%; philosophy callouts at top and bottom)
**HTML title tag:** Somebody Has to Win — And Usually Nobody Does — Case Study

**Subtitle:** Ask someone their lottery odds and they picture the queue at the counter. The draw does not run over the queue.

**What this page is about — keep it to this.** One psychological error: people use the crowd of
buyers as the denominator, because that is the pool they can see. The correction is one conditional
probability. Nothing else belongs here — not rollover ladders, not annuity-versus-cash, not tax,
not expected value, not prize-tier tables. Those are different pages. An earlier draft carried all
of them and the single idea disappeared underneath.

**Note on the data:** the town is a toy setup and every figure on it is exact arithmetic on stated
numbers. One line of real-lottery scale appears in section 2 for calibration; the ticket-sales
figure there is illustrative and labelled. No random or generated data, so no PRNG is needed.

**Number style:** "1 in N" for chances, percentages for shares. Chances are always stated as the
chance of winning, never of losing.

## Callout (philosophy box, top)

**The puzzle:** People judge their odds by the crowd they can see, not by the numbers they cannot.

**Your gut says:** A hundred people in this town bought a ticket and one of them will win — so I am one in a hundred.

**The arithmetic says:** Your odds come from how many numbers were *printed*, not how many people bought. Print 1,000 and you are `1 in 1,000` — ten times worse than the crowd suggests, and that figure is never on the poster.

## 1. The Denominator You Can See, and the One That Counts

**Tags:** `the mechanism` (blue) · `wrong denominator` (orange) · `toy example` (green)

**Obj-title:** The Gut Divides by the Crowd, Because the Crowd Is Visible

Math box:

**A town of 1,000. A hundred people buy a ticket. One prize. The operator prints 1,000 numbers.**

| You divide by | You get | Which answers |
|---|---|---|
| 100 buyers — visible | 1 in 100 | "who wins, *if* anyone does" |
| 1,000 numbers — printed | 1 in 1,000 | "do I win" |

The crowd is the pool you can stand in. The printed numbers are the pool the draw actually runs over.

Bullets:

- **What the gut reaches for:** One in a hundred, because a hundred bought in and one prize exists.
- **Why that pool is wrong:** The draw picks from the numbers printed, not from the people holding them.
- **The invisible variable:** How many numbers exist — the operator's choice, and it sets your odds alone.
- **Nobody advertises it:** Ticket counts and prize sizes are on the poster; the size of the number pool is not.
- **Always the same direction:** Every pool you can see is smaller than the real one, so the guess always flatters.

### Visualization (canvas `canvas1`, 720×360)

The two denominators side by side, drawn to the same scale so one visibly dwarfs the other.

- **Title (bold 14px `#1a5276`, centered):** "Two Denominators. The Gut Picks the Visible One."
- **Left block (x=70–330):** 100 small `rgba(26,82,118,0.35)` figures in a 10×10 grid, `#1a5276` outlines, one filled `#e67e22` labelled "you". Header (bold 12px `#21618c`): "What you can see". Sub-label (11px `#666`): "100 buyers in the queue".
- **Right block (x=390–670):** a 40×25 grid of 1,000 cells at ~7px, `#e8edf1` fill with `#e0e0e0` 0.5px outlines, one cell `#e67e22` for your number. Header (bold 12px `#c0392b`): "What decides". Sub-label (11px `#666`): "1,000 numbers printed".
- **The point is the area ratio:** both grids use the same cell pitch, so the right block is literally ten times the left. Do not normalise the two blocks to equal size — the size difference *is* the lesson.
- **Verdict labels below each block (bold 13px):** left "gut: 1 in 100" in `#e67e22`; right "truth: 1 in 1,000" in `#c0392b`.
- **Annotation (bold 11px `#e74c3c`, leader line to the right block):** "the operator picks this, and never prints it".
- **Bottom note (11px `#666`, centered, y≈344):** "Illustrative Example."

## 2. The Gut's Answer Is a Conditional Probability

**Tags:** `the correction` (red) · `conditioning` (purple) · `the missing factor` (orange)

**Obj-title:** 1 in 100 Is the Right Answer to "Who Wins, Given Somebody Does"

Math box:

**The gut is not sloppy — it is answering a different question.**

`P(you win | somebody wins)` = `1 in 95` — almost exactly the crowd of 100.

Run the town 1,000 times:

| Draws | What happens |
|---|---|
| 1,000 | the draw is held |
| 95 | somebody's number comes up |
| ~1 | that somebody is you |

The gut quietly assumes the prize gets claimed. It usually does not — **90% of draws end with nobody winning.**

Bullets:

- **The missing factor:** True odds = the gut's odds × the chance somebody wins — here, 1 in 100 × 10%.
- **Why the error feels safe:** Conditioned on a winner existing, "one in a hundred" is genuinely correct.
- **What gets assumed away:** That the prize is claimed at all, which is the rare outcome, not the default.
- **Not exactly 100:** It is 1 in 95, because two buyers can pick the same number and share the pool.
- **Real-lottery scale:** 292M numbers, ~10M tickets sold — gut says 1 in 10M, truth is 1 in 292M, 97% no winner.

### Visualization (canvas `canvas2`, 720×360)

A funnel of 1,000 draws narrowing twice, showing where the gut's fraction actually lives.

- **Title (bold 14px `#1a5276`, centered):** "Where 'One in a Hundred' Is Actually True".
- **Three stacked bands, left-aligned at x=90, width ∝ count** (1,000 → 95 → 1), heights 54, y centres 105/185/265, minimum 3px width so the last band stays visible.
- **Band 1** `rgba(26,82,118,0.35)` fill, `#1a5276` stroke — "1,000 draws held".
- **Band 2** `rgba(230,126,34,0.45)` fill, `#d35400` stroke — "95 draws somebody wins" (computed as `1000 × (1 − (1 − 1/1000)^100)`, rounded).
- **Band 3** `#e74c3c` fill — "1 draw you win".
- **Right-hand labels (12px `#333`, bold count in the band's accent):** the count, then an 11px `#666` gloss.
- **The gut's bracket:** a `#e67e22` brace spanning bands 2→3 with bold 11px `#d35400` label "the gut's 1 in 100 lives here"; a second `#c0392b` brace spanning bands 1→3 labelled "your real odds: 1 in 1,000".
- **Honesty rule:** label the bands as three separately computed facts. Do **not** caption the figure as though 95 × (1/100) multiplies out exactly to 1/1,000 — it gives 1 in 1,050, since shared picks make the conditional 1 in 95.2 rather than 1 in 100.
- **Bottom note (11px `#666`, centered, y≈344):** "Illustrative Example."

## 3. In a Raffle the Gut Is Exactly Right

**Tags:** `the boundary` (green) · `where it holds` (blue) · `transferred habit` (orange)

**Obj-title:** The Intuition Is Imported From Raffles, Where It Is Correct

Math box:

**One structural difference, and it flips the answer.**

| | Raffle | Lottery |
|---|---|---|
| Draw runs over | the 100 tickets sold | the 1,000 numbers printed |
| Somebody wins | always | 10% of draws |
| Your chance | 1 in 100 | 1 in 1,000 |
| Gut's answer | right | 10× too kind |

A raffle draws out of the drum, so the prize cannot leave the room — and the conditional and the real answer coincide.

Bullets:

- **Why the habit exists:** Office pools and school draws are the only version most people have stood in.
- **What carries over:** Equal terms for every buyer — genuinely true of both.
- **What does not:** The guaranteed winner, which is what made counting the crowd valid.
- **The tell:** Ask whether the draw could come up empty; if it cannot, count the crowd.
- **The honest boundary:** Nothing here argues against a $2 ticket — only against believing the crowd is the denominator.

### Visualization (canvas `canvas3`, 720×360)

Two pools, the same 100 buyers in each, drawn from differently.

- **Title (bold 14px `#1a5276`, centered):** "Drawn From the Tickets, or Drawn From the Numbers".
- **Left panel (x=70–340):** `#f4fbf7` fill, `#27ae60` 2px border, a 10×10 grid of 100 tickets; one `#27ae60` with a bold 11px `#1e8449` label "the winner is in here". Caption (bold 12px `#1e8449`): "Raffle — somebody always wins."
- **Right panel (x=386–680):** `#fdf8f7` fill, `#e74c3c` 2px border, a 40×25 grid of all 1,000 printed numbers; the drawn cell ringed `#e74c3c` 2.5px on an **empty** cell, labelled "drawn — nobody bought it". Caption (bold 12px `#c0392b`): "Lottery — 90% of draws end like this."
- **Filled-cell count is a statistic, not decoration.** The number of shaded cells is the expected count of *distinct* numbers the 100 buyers hold: `1000 × (1 − (1 − 1/1000)^100)` = **95 of 1,000**, computed at render time. This is the same formula as `P(somebody wins)`, which is why 95 shaded cells and a 9.5% win chance are one fact, not two. A grid shaded to some other fraction would contradict its own caption — that defect existed in an earlier draft, which shaded 30% of cells beside a caption reading "30% of draws end like this" (30% shaded means somebody wins 30% of the time, i.e. 70% end empty).
- **Bought indices are fixed literals** spread evenly across the grid, and the drawn index is a fixed literal chosen to be absent from them.
- **Legend (11px `#666`):** a `rgba(26,82,118,0.35)` swatch and "somebody bought this number".
- **Bottom note (11px `#666`, centered, y≈344):** "Illustrative Example."

## Callout (philosophy box, bottom)

**One sentence:** The crowd at the counter is the pool you can see, but the draw runs over the numbers that were printed — so "one in a hundred" is the right answer to "who wins if anyone does," and the wrong answer to "do I win."

## Regeneration instructions

- **Layout:** h1, `.subtitle`, `.philosophy` callout, three numbered sections, closing `.philosophy`. Each section is an `<h2>` with a section accent class followed by an `.obj-table` (one `<tr>`; left `<td>` 50% with `.tags` + `.obj-title` + one `.math-box` + a 5-item `<ul>`, right `<td>` 50% centered with the canvas). No nav, no back/home links, no cross-page links.
- **Length discipline:** one math box and five bullets per section, three sections. This page is the folder's reference for that shape, so it must not exceed it. If a new fact needs a second math box it belongs on another page.
- **Scope discipline:** the page teaches one error and its correction. Resist adding jackpot mechanics, payout arithmetic, or expected value — each was present in an earlier draft and each buried the idea.
- **Section accents:** `<h2>` and `.obj-table` share a class — `s-blue` (1), `s-red` (2), `s-green` (3) — recoloring the h2 bottom border, the `.math-box` left edge, the `.obj-title`, and every bullet's bold label. Colors `#2980b9`/`#21618c`, `#e74c3c`/`#c0392b`, `#27ae60`/`#1e8449`. Bold text inside a math box stays `#1a5276`.
- **Tag pills:** `.topic-tag` — inline-block, 0.7em, bold, uppercase, letter-spacing 0.5px, padding 2px 8px, radius 10px; tinted variants `.tag-blue`, `.tag-red`, `.tag-green`, `.tag-orange`, `.tag-purple` pairing a pale background with matching dark text.
- **Math boxes:** background `#f8fafb`, border `1px solid #e0e0e0` plus a 3px accent left border, radius 6px, padding 16px 20px, 0.9em; inline `code` on `#eef2f7`. Tables inside: `width: 100%` with `table-layout: fixed`, 0.95em, header bold `#1a5276` with a `1px solid #d5dbdf` bottom border, cells padding 4px 8px, all cells left-aligned.
- **Page style:** body system sans-serif, white, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cells `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600; ul 0.9em `#333`.
- **Canvas:** intrinsic 720×360; scale by `window.devicePixelRatio` (cap display at logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back) through a shared `setupCanvas(id, w, h)`. Draw functions in a `__charts` array, re-run on debounced (150ms) resize.
- **Data rule:** three constants — `BUYERS = 100`, `NUMBERS = 1000`, and `N_REAL = comb(69,5) * 26` for the one calibration line (never the literal 292201338). Everything else derives: `pSomebody = 1 − (1 − 1/NUMBERS)^BUYERS`, `pCond = (1/NUMBERS) / pSomebody`, distinct numbers held = `NUMBERS × pSomebody`. Every printed figure computed at render time; no `Math.random()` and no PRNG, since nothing is generated.
- **Computed-not-typed:** the conditional "1 in 95" in section 2's math box and the "95" and "90%" in the prose are written into spans (`#cond-odds`, `#n-somebody`, `#pct-nobody`) at load from the derived values, so prose, table, and both canvases cannot drift apart.
- **Palette:** `#1a5276`, `#2980b9`, `#27ae60`, `#e74c3c`, `#e67e22`, `#8e44ad`, fill `rgba(26,82,118,0.35)`, pool `#e8edf1`, gray `#666`/`#333`.
