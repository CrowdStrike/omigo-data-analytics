# Weighing Puzzles

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Weighing Puzzles

**Subtitle:** A balance scale answers a three-way question, so three weighings can tell 27 stories apart — just enough to find one bad jar among 12 and say whether it is heavy or light

## One Bad Jar and a Kitchen Balance

**Tags:** `core idea` (blue), `three-way question` (green), `counting outcomes` (orange)

- **The stall** — a market stall packs 12 identical jam jars; exactly one was filled wrong, heavier or lighter
- **The tool** — a two-pan kitchen balance with no numbers: it only tips left, tips right, or balances
- **Three answers** — every weighing is a question with 3 possible answers, not 2 like a yes/no question
- **The suspects** — 12 jars, each possibly heavy or possibly light: 12 × 2 = 24 stories to tell apart
- **The budget** — 3 weighings give 3 × 3 × 3 = 27 distinct answer patterns; 27 ≥ 24, so 3 can be enough
- **The contrast** — 3 yes/no questions give only 2 × 2 × 2 = 8 patterns; 8 < 24, hopeless

*Example (italic):* One weighing can't name the jar by itself — but "tips left / balances / tips right" three times in a row spells one of 27 codewords, and 24 suspects need only 24 codewords.

**Key point:** Before planning any weighing, count: possibilities to tell apart (24) versus answer patterns available (27) — the puzzle is solvable only because 27 ≥ 24.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: answer patterns after 1, 2, 3 questions for a yes/no question versus a balance weighing, with a dashed target line at the 24 possibilities that must be told apart.

- **Title (bold 15px, `#1a5276`, top center):** "How Many Stories Can Your Questions Tell Apart?".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 190; y = answer patterns 0 to 30, light `#e5e9ef` gridlines at 5, 10, 15, 20, 25 with 12px `#444` labels; x = three groups centered at x=170, 360, 550 labeled "1 question", "2 questions", "3 questions" (12px `#444`).
- **Bars (each 55px wide, 14px gap within a group):** yes/no bars blue `#2a78d6` at heights `[2, 4, 8]`; balance bars green `#008300` at heights `[3, 9, 27]`; bold 12px value label in matching color on top of every bar ("2", "4", "8", "3", "9", "27").
- **Legend (12px, top left inside plot at x=85, y=70):** blue swatch "yes/no — 2 answers", green swatch "balance — 3 answers".
- **Target line:** horizontal dashed orange `#d95926` (dash 6/4) line at 24 across the plot; 12px orange label just below its left end: "24 suspects (12 jars × heavy/light)".
- **Annotation (bold 13px green `#008300`, right-aligned at x=545, y=88/105):** two lines: "27 ≥ 24 — three weighings can win" / "8 < 24 — three yes/no questions cannot".
- **Caption (12px `#444`, bottom right):** "counts are exact — 2^n vs 3^n answer patterns".

## Weighing 4 Against 4: the Suspect List Shrinks

**Tags:** `worked example` (blue), `divide by three` (green)

- **Weighing 1** — put jars 1–4 on the left pan, 5–8 on the right, leave 9–12 on the table
- **Three branches** — tips left: 8 suspects; balances: 8 suspects; tips right: 8 suspects — 24 splits 8/8/8
- **Balanced case** — the odd jar is among 9–12, either heavy or light: 4 jars × 2 = 8 stories left
- **Weighing 2** — put 9, 10, 11 against three known-good jars (1, 2, 3); branches hold 3, 2, 3 suspects
- **Weighing 3** — if 9–11 came down heavy, weigh 9 vs 10: the heavier one is guilty; balanced means 11
- **The rhythm** — worst branch runs 24 → 8 → 3 → 1: each weighing cuts suspects to a third

*Example (italic):* Balanced, then left pan down, then balanced again means: odd jar is in 9–12, it is one of 9/10/11 and heavy, and it is not 9 or 10 — so jar 11 is heavy, no doubt left.

**Key point:** The winning first move splits 24 suspects into 8/8/8 — every branch fits inside the 9 answer patterns the two remaining weighings still offer.

### Visualization (canvas `c2`, 720×300)

Descending bar chart of suspects remaining after each weighing on the worst branch (24, 8, 3, 1), with the move written under each step and divide-by-three arrows between bars.

- **Title (bold 15px, `#1a5276`, top center):** "Each Weighing Cuts the Suspect List to a Third".
- **Axes:** origin x=70, baseline y=225, plot width 580, plot height 165; y = suspects remaining 0 to 24, light `#e5e9ef` gridlines at 6, 12, 18, 24 with 12px `#444` labels.
- **Bars (80px wide, centered at x=160, 310, 460, 610):** heights `[24, 8, 3, 1]`; first bar blue `#2a78d6`, middle two aqua `#199e70`, last green `#008300`; bold 13px value label in matching color above each bar ("24", "8", "3", "1").
- **Step labels (12px `#444`, two lines, below baseline under each bar):** "start" / "12 jars × 2"; "weighing 1" / "4 vs 4"; "weighing 2" / "9,10,11 vs good"; "weighing 3" / "9 vs 10".
- **Arrows:** 2px `#6b7280` arrow between consecutive bar tops, each with a bold 12px `#6b7280` "÷3" label above it (24→8 exact, 8→3 and 3→1 are the worst branch, so the last two are "÷3 (worst branch)" in 11px).
- **Annotation (bold 13px orange `#d95926`, near x=430, y=70):** two lines: "balanced / left down / balanced" / "spells: jar 11, heavy".
- **Caption (12px `#444`, bottom right):** "worst-case branch shown — every other branch is the same size or smaller".

## Why Data People Care: Every Test Has a Capacity

**Tags:** `where it's used` (blue), `information` (green), `test design` (orange)

- **The capacity rule** — a test with k outcomes, run n times, can separate at most k^n possibilities
- **Twenty questions** — 20 yes/no answers separate 2^20 ≈ 1 million things; that is why the game works
- **Richer answers pay** — 4 weighings separate 3^4 = 81 patterns; 4 yes/no questions only 2^4 = 16
- **Balanced splits win** — a weighing is most informative when all three outcomes stay live and even
- **Everywhere in data** — binary search, decision trees, and A/B tests all spend the same currency: outcomes
- **Wasted outcomes** — a test whose result you can predict in advance carries no information at all

*Example (italic):* A triage checklist with three-way answers (low/medium/high) needs far fewer questions than a yes/no one — the same reason the balance beats the coin-flip question 27 to 8.

**Key point:** Count outcomes before running tests: n tests with k outcomes each can never distinguish more than k^n cases — the balance puzzle is this law wearing a costume.

### Visualization (canvas `c3`, 720×300)

Two-line growth chart: separable possibilities after 0–4 rounds for a 2-outcome test versus a 3-outcome test, showing the gap explode from 1× to 5×.

- **Title (bold 15px, `#1a5276`, top center):** "Capacity of n Tests: 3 Outcomes Pull Away Fast".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 190; x = number of tests 0 to 4, tick labels "0"–"4" (12px `#444`) at x=70, 215, 360, 505, 650; y = separable possibilities 0 to 90, light `#e5e9ef` gridlines at 20, 40, 60, 80 with 12px `#444` labels.
- **Yes/no line:** blue `#2a78d6` 3px line through points `[1, 2, 4, 8, 16]` at the five x ticks, 5px blue dots, bold 12px blue value labels above each dot; 12px blue label "2 outcomes (yes/no)" near the line end.
- **Balance line:** green `#008300` 3px line through points `[1, 3, 9, 27, 81]`, 5px green dots, bold 12px green value labels above each dot; 12px green label "3 outcomes (balance)" near x=3.2.
- **Marker:** vertical dashed `#6b7280` (dash 4/3) line at n=3 from baseline to y=70; 12px `#6b7280` label at top: "the puzzle lives here: 27 vs 8".
- **Annotation (bold 13px violet `#4a3aa7`, near x=250, y=85):** two lines: "at 4 tests: 81 vs 16 —" / "one extra outcome per test, 5× the reach".
- **Caption (12px `#444`, bottom right):** "exact values of 2^n and 3^n".

## The Half-and-Half Trap

**Tags:** `common mistake` (red), `wasted outcome` (orange)

- **The instinct** — split in half: weigh 6 jars against 6, the way binary search halves a sorted list
- **The flaw** — with all 12 jars on the pans, the scale can never balance; one outcome is dead on arrival
- **Two live branches** — 6 vs 6 splits the 24 suspects into 12 / 0 / 12 instead of 8 / 8 / 8
- **The budget check** — two weighings remain, worth 3 × 3 = 9 patterns; 12 suspects do not fit in 9
- **The lesson** — leave 4 jars off the pans; a third outcome only helps if it can actually happen

*Example (italic):* After 6 vs 6 tips left, 12 stories remain (left six heavy or right six light) — and no clever pair of follow-up weighings can sort 12 suspects with only 9 answer patterns.

**Common mistake:** Halving instead of thirding. The balance is a three-answer instrument; a first move that makes "balanced" impossible throws away a third of every future answer.

### Visualization (canvas `c4`, 720×300)

Two grouped bar panels comparing the branch sizes of the 6-vs-6 first move against the 4-vs-4 first move, with a dashed line at 9 — the capacity of the two weighings left.

- **Title (bold 15px, `#1a5276`, top center):** "First Move Compared: 6 vs 6 Dead-Ends, 4 vs 4 Survives".
- **Axes:** origin x=70, baseline y=235, plot width 580, plot height 175; y = suspects in branch 0 to 14, light `#e5e9ef` gridlines at 3, 6, 9, 12 with 12px `#444` labels; two group labels (bold 13px `#2c3e50`, below baseline): "weigh 6 vs 6" centered at x=215, "weigh 4 vs 4" centered at x=505.
- **6-vs-6 bars (45px wide at x=140, 215, 290):** heights `[12, 0, 12]` for branches "tips left", "balances", "balances" tick labels — use 11px `#444` branch labels "left", "balanced", "right" under each bar; the two 12-bars red `#e74c3c` with bold 13px red "12" on top; the empty balanced slot gets a 11px `#6b7280` label "impossible (0)".
- **4-vs-4 bars (45px wide at x=430, 505, 580):** heights `[8, 8, 8]`, all green `#008300`, bold 13px green "8" on top, 11px `#444` branch labels "left", "balanced", "right".
- **Capacity line:** horizontal dashed `#6b7280` (dash 6/4) line at 9 across the plot; bold 12px `#6b7280` label above the line from x=336: "9 = what 2 weighings can still sort (3×3)".
- **Annotation (bold 13px red `#e74c3c`, near x=140, y=70):** "12 > 9: stuck"; **second annotation (bold 13px green, near x=470, y=70):** "8 ≤ 9: every branch fits".
- **Caption (12px `#444`, bottom right):** "branch sizes are exact counts of the 24 heavy/light stories".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights and line points are the hardcoded arrays above (no randomness); every number is exact combinatorics (2^n, 3^n, and branch counts of the 24 heavy/light stories), so captions state "exact", not "illustrative".
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
