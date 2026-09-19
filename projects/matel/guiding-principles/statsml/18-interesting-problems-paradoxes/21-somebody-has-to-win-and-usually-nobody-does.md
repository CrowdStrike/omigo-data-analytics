# "Somebody Has to Win" — And Usually Nobody Does

**Page type:** detail page (h2-sectioned two-column obj-table layout: text left 50%, canvas right 50%; philosophy callouts at top and bottom)
**HTML title tag:** Somebody Has to Win — And Usually Nobody Does — Case Study

**Subtitle:** At a $1Bn jackpot about 70% of draws do have a winner. Your ticket is at 1 in 292M. This page is the distance between those two numbers.

**Note on the data:** the town example is exact arithmetic on a stated toy setup. Real figures use the published Powerball structure — 5 of 69 white balls plus 1 of 26 red, which is 292M combinations. Sales volumes are illustrative; the arithmetic on them is exact. No random or generated data.

**Number style:** percentages for shares, abbreviations in prose and labels (292M, $1Bn), exact digits only where the exact figure is the point. Chances always stated as the chance of winning, never of nobody winning.

**Three sections, one math box and five bullets each.** Section 3 is the boundary.

## Callout (philosophy box, top)

**The puzzle:** "Somebody will win, and it could be you" is true, and says almost nothing about your ticket.

**Your gut says:** The winner will be one of us — somebody in the queue, holding the ticket I would have held.

**The arithmetic says:** That needs every combination sold, and it never is. Grant the slogan in full and your ticket moves from `1 in 292M` to `1 in 204M`.

## 1. A Town of a Thousand, and One Number Nobody Tells You

**Tags:** `the mechanism` (blue) · `pool size` (orange) · `toy example` (green)

**Obj-title:** Your Odds Rest on a Figure That Is Never Advertised

Math box:

**A town of 1,000. A hundred buy a ticket. One prize.**

How many numbers exist is the operator's choice:

| Numbers issued | Your chance | Somebody wins |
|---|---|---|
| 100 | 1 in 100 | 63% |
| 1,000 | 1 in 1,000 | 10% |
| 10,000 | 1 in 10,000 | 1% |

Same town, same buyers, same prize — your odds move a hundredfold.

Bullets:

- **What the gut reaches for:** One in a hundred, because a hundred bought in and one wins.
- **Why that pool is wrong:** The draw picks from the numbers printed, not the people holding them.
- **The free variable:** How many numbers exist — set by the operator, not by the crowd.
- **Always the same direction:** Every pool you can see is smaller, so the guess always flatters.
- **Granting the slogan in full:** `1 in 292,201,338` becomes `1 in 203,998,526` — real, and worth nothing.

### Visualization (canvas `canvas1`, 720×360)

Three bands: the same 100 buyers against three pools of numbers.

- **Title (bold 14px `#1a5276`, centered):** "Same Hundred Buyers. Three Different Answers."
- **Layout:** three bands x=150 to x=660, y centres 100/180/260, height 42.
- **Per band:** a `#e8edf1` pool rectangle, width ∝ log10 of pool size, outlined in that band's accent (`#2980b9`, `#e67e22`, `#8e44ad`); a `rgba(26,82,118,0.35)` block of **constant 46px width** at the left for the 100 buyers — identical in all three, so the pool grows while the crowd does not.
- **Labels:** row name right of x=138 (12px `#333`); bold 12px accent "1 in 100" / "1 in 1,000" / "1 in 10,000" to the right of each pool, with 11px `#666` "somebody wins 63%" / "10%" / "1%" beneath — computed as `1 − (1 − 1/T)^100`.
- **Annotation (bold 11px `#e74c3c`, leader line to the buyer block):** "the part you can see never changes".
- **Bottom note (11px `#666`, centered, y≈338):** "Pool widths on a log scale. Illustrative Example."

## 2. The Billion Is Built Out of Draws Nobody Won

**Tags:** `the reversal` (red) · `rollover` (orange) · `the payout` (purple)

**Obj-title:** A Rising Jackpot Is a Tally of Not-Winning

Math box:

**Each rung up the jackpot is a draw that missed.**

Sales climb with the prize, so the odds climb too — 3% at `$20M`, 36% at `$450M`, 70% at `$1Bn`.

Chain the rungs: about **1 streak in 20** reaches a `$1Bn` headline. That billboard means roughly nine draws in a row with no winner.

And `$1Bn` advertised pays about **`$184M`** — half for cash, then tax, then the likely split.

Bullets:

- **The reversal:** The number that makes the game look winnable is a record of nobody winning.
- **What a small jackpot means:** Somebody won recently, and that draw had the thinnest crowd.
- **What a huge jackpot means:** A long miss streak, plus the biggest crowd you will ever share with.
- **Self-cancelling:** The prize that pulls the crowd in is the prize the crowd makes you divide.
- **Never on the billboard:** Tickets sold — the one figure that would put the printed odds in context.

### Visualization (canvas `canvas2`, 720×360)

Bars for the climbing jackpot, line for the chance the streak got that far.

- **Title (bold 14px `#1a5276`, centered):** "Every Step Up Is One More Draw Nobody Won".
- **Plot area:** origin x=80, baseline y=285, width 555, height 215; left axis `#1a5276`, right axis `#e74c3c`.
- **Data (nine rungs, sales in M / jackpot in $M):** (9, 20), (13, 40), (18, 70), (25, 110), (40, 170), (70, 280), (130, 450), (220, 700), (350, 1000). Survival = running product of `exp(−S/N)`, computed to 97%, 93%, 87%, 80%, 70%, 55%, 35%, 17%, 5%.
- **Bars (left axis, 0–1100):** `rgba(26,82,118,0.35)` with `#1a5276` stroke, warming to `rgba(230,126,34,0.45)` with `#d35400` stroke for the last three, and `#e74c3c` for the final one. Value labels only on the first and last.
- **Line (right axis, 0–100%):** `#e74c3c` 2.5px with dots; labels at the first and last points only.
- **Axes:** x "Draw number in the streak" ticks 1–9; left y "Jackpot" ticks "$0"…"$1Bn"; right y "Streak got this far" in `#e74c3c`.
- **Annotations:** bold 11px `#e74c3c` with leader to the last bar, "$1Bn = about nine draws, nobody won"; bold 11px `#8e44ad` upper-left, two lines, "That $1Bn pays about $184M" / "after cash option, tax, and sharing" — computed from the waterfall.
- **Bottom note (11px `#666`, left at x=80, y≈326):** "Illustrative sales per rung. Illustrative Example."

## 3. A Raffle Is the Other Thing

**Tags:** `the boundary` (green) · `where it holds` (blue) · `scratch cards` (orange)

**Obj-title:** In a Raffle the Prize Cannot Go Unclaimed

Math box:

**One structural difference:**

| | Raffle | This lottery |
|---|---|---|
| Draw runs over | the tickets sold | all 292M combinations |
| Somebody wins | always | 70% at a big draw |
| Sitting out forfeits | a share of something certain | often nothing |

A raffle draws out of the drum, so the prize leaves with somebody in the room.

Bullets:

- **Why the mix-up is easy:** Office pools and school draws are the only version most people have stood in.
- **What carries over:** Equal terms for every buyer, genuinely true of both.
- **What does not:** The guaranteed winner — the half that makes sitting out a real loss.
- **Not even cheaper:** A raffle over 350M tickets is 1 in 350M, worse than 1 in 292M.
- **The honest boundary:** Nothing here argues against a $2 ticket — only against sizing the spend as though the neighbour's chance were evidence about yours.

### Visualization (canvas `canvas3`, 720×360)

Two side-by-side pools.

- **Title (bold 14px `#1a5276`, centered):** "Drawn From the Tickets, or Drawn From the Numbers".
- **Left panel (x=70–340):** `#f4fbf7` fill with `#27ae60` 2px border, holding 60 tickets, one `#27ae60` with a bold 11px label "the winner is in here". Caption (bold 12px `#1e8449`): "Raffle — somebody always wins."
- **Right panel (x=386–680):** `#fdf8f7` fill with `#e74c3c` 2px border around a 20×12 grid of 240 cells (`#e0e0e0` 0.5px outlines); 72 cells filled `rgba(26,82,118,0.35)` from a fixed literal array; the drawn cell ringed `#e74c3c` 2.5px on an **empty** cell, labelled "drawn — nobody bought it". Caption (bold 12px `#c0392b`): "Lottery — 30% of big draws end like this."
- **Panel headers (bold 12px):** "Pool of tickets sold" in `#1e8449`, "Pool of numbers" in `#c0392b`.
- **Legend (11px):** a `rgba(26,82,118,0.35)` swatch and "somebody bought this number".
- **Bottom note (11px `#666`, centered, y≈344):** "Grid shown at 240 cells; the real one has 292M. Illustrative Example."
- **No random values:** the bought indices and the drawn index are fixed literals, and the drawn index is not in the bought array.

## Callout (philosophy box, bottom)

**One sentence:** Your chance depends on how many numbers were printed — a figure the seller chooses and never advertises — so "somebody will win" can be true at 70% while your ticket sits at 1 in 292M.

## Regeneration instructions

- **Layout:** h1, `.subtitle`, `.philosophy` callout, three numbered sections, closing `.philosophy`. Each section is an `<h2>` with a section accent class followed by an `.obj-table` (one `<tr>`; left `<td>` 50% with `.tags` + `.obj-title` + one `.math-box` + a 5-item `<ul>`, right `<td>` 50% centered with the canvas). No nav, no back/home links, no cross-page links.
- **Length discipline:** one math box and five bullets per section. Anything that needs a second math box belongs in a different section or gets cut.
- **Section accents:** `<h2>` and `.obj-table` share a class — `s-blue` (1), `s-red` (2), `s-green` (3) — recoloring the h2 bottom border, the `.math-box` left edge, the `.obj-title`, and every bullet's bold label. Colors `#2980b9`/`#21618c`, `#e74c3c`/`#c0392b`, `#27ae60`/`#1e8449`. Bold text inside a math box stays `#1a5276`.
- **Tag pills:** `.topic-tag` — inline-block, 0.7em, bold, uppercase, letter-spacing 0.5px, padding 2px 8px, radius 10px; tinted variants `.tag-blue`, `.tag-red`, `.tag-green`, `.tag-orange`, `.tag-purple` pairing a pale background with the matching dark text.
- **Math boxes:** background `#f8fafb`, border `1px solid #e0e0e0` plus a 3px accent left border, radius 6px, padding 16px 20px, 0.9em; inline `code` on `#eef2f7`. Tables inside: `width: 100%` with **`table-layout: fixed`** so every column gets an equal share regardless of content length, 0.95em, header bold `#1a5276` with a `1px solid #d5dbdf` bottom border, cells padding 4px 8px, all cells left-aligned (no right-aligned numeric column — with equal widths a uniform alignment reads better).
- **Page style:** body system sans-serif, white, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cells `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600; ul 0.9em `#333`.
- **Canvas:** intrinsic 720×360; scale by `window.devicePixelRatio` (cap display at logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back) through a shared `setupCanvas(id, w, h)`. Draw functions in a `__charts` array, re-run on debounced (150ms) resize.
- **Data rule:** one constant `N = comb(69,5) * 26` via an explicit `comb()` helper — never the literal 292201338. Win chances from `1 − Math.exp(-S/N)`; split from Poisson with `λ = S/N`; expected share as `Σ e^−λ λ^k / k! / (k+1)` over k = 0..79. Every printed figure computed at render time. No `Math.random()`; no seeded PRNG needed since nothing is generated.
- **Number formatting:** `abbr()` for "292M" / "$1Bn" / "$315M", `abbr1()` keeping one decimal below 100M where rounding would mislead, `money()` prefixing a dollar sign. The two exact digit strings in section 1 are written into `#exact-uncond` and `#exact-cond` at load time by `fmt()` from the computed values, never typed into the markup.
- **Palette:** `#1a5276`, `#2980b9`, `#27ae60`, `#e74c3c`, `#e67e22`, `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, pool fill `#e8edf1`, gridlines `#eee`, gray `#666`/`#333`/`#999`.
