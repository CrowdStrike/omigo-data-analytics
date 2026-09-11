# Gale-Shapley by Hand

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Gale-Shapley by Hand

**Subtitle:** Propose, hold tentatively, reject — repeat until the market goes quiet, and the result is always stable

**Running data (illustrative, shared across this tutorial series), used identically in every section — 4 candidates × 4 companies, one opening each:**

Candidate preference lists: Alice A>B>C>D; Bob A>D>C>B; Carol B>A>C>D; Dan A>B>C>D.
Company preference lists: A Dan>Alice>Bob>Carol; B Carol>Alice>Dan>Bob; C Alice>Bob>Carol>Dan; D Bob>Alice>Carol>Dan.

Candidates-propose run:
Round 1 — Alice→A, Bob→A, Carol→B, Dan→A; A holds Dan (its top pick), rejects Alice and Bob; B holds Carol (its top pick).
Round 2 — Alice→B, rejected (B keeps Carol); Bob→D, held.
Round 3 — Alice→C, held; no one left proposing.
Final matching: Dan–A, Carol–B, Alice–C, Bob–D. Proposals per round: 4, 2, 1 (7 total; worst case n² = 16).
First-come-first-served contrast run (arrival order Alice, Bob, Carol, Dan; instant acceptance): Alice–A, Bob–D, Carol–B, Dan–C — blocking pair (Dan, A).

## Propose, Hold Tentatively, Reject

**Tags:** `core idea` (blue), `deferred acceptance` (green)

- **The market** — Alice, Bob, Carol, Dan each want one job; Companies A–D each have one opening
- **Everyone ranks** — each candidate ranks all four companies; each company ranks all four candidates
- **Propose** — each round, every unmatched candidate asks their best not-yet-refused company
- **Hold, don't accept** — a company keeps only the best proposal so far and rejects the rest
- **Trade up** — a held candidate is dropped if someone the company ranks higher proposes later
- **Stop** — when nobody is left proposing, the market is quiet and every hold becomes final

*Example (italic):* Three of the four candidates put Company A first — A will collect proposals and pick at leisure.

**Key point:** Propose, hold tentatively, reject — a held offer is a dance partner mid-song, swappable until the music stops.

### Visualization (canvas `c1`, 720×300)

Two side-by-side panels showing the full preference lists of both sides, first choices in bold color.

- **Title (bold 15px, `#1a5276`, top center):** "The Job Market — Everyone's Ranked List (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=44 to y=252.
- **Left panel header (bold 13px blue `#2a78d6`, centered at (185, 56)):** "CANDIDATES rank companies".
- **Left rows (y = 92, 130, 168, 206):** names "Alice Bob Carol Dan" bold 12px `#2c3e50` right-aligned at x=115; each list drawn at x=132 left-aligned — first choice bold 13px blue `#2a78d6`, remainder 13px `#2c3e50`: "A > B > C > D", "A > D > C > B", "B > A > C > D", "A > B > C > D".
- **Left annotation (bold 12px orange `#d95926`, centered at (185, 238)):** "three of the four put A first".
- **Right panel header (bold 13px green `#008300`, centered at (545, 56)):** "COMPANIES rank candidates".
- **Right rows (same y):** names "A B C D" bold 12px `#2c3e50` right-aligned at x=425; lists at x=440 — first choice bold 13px green `#008300`, remainder 13px `#2c3e50`: "Dan > Alice > Bob > Carol", "Carol > Alice > Dan > Bob", "Alice > Bob > Carol > Dan", "Bob > Alice > Carol > Dan".
- **Caption (12px `#6b7280`, centered at y=284):** "the shared instance for this series — bold names are first choices; one opening per company".

## Three Rounds and the Market Goes Quiet

**Tags:** `worked example` (blue), `candidates propose` (orange)

- **Round 1** — Alice→A, Bob→A, Carol→B, Dan→A: three of the four proposals land on Company A
- **A's call** — A ranks Dan first: hold Dan, reject Alice and Bob; B holds its top pick Carol
- **Round 2** — Alice tries B; B keeps Carol and rejects her; Bob tries D and is held
- **Round 3** — Alice tries C and C holds her; nobody is left proposing, so the rounds stop
- **Final** — Dan–A, Carol–B, Alice–C, Bob–D: every tentative hold now becomes permanent

*Example (italic):* Redo Alice's path by hand: her list is A > B > C > D, and each rejection just moves her one step down it.

**Key point:** Seven proposals across three rounds and the market goes quiet: Dan–A, Carol–B, Alice–C, Bob–D.

### Visualization (canvas `c2`, 720×300)

Three side-by-side round panels; in each, candidates on the left propose to companies on the right — green solid arrow = held, red solid arrow = rejected, green dashed line = hold carried from an earlier round.

- **Title (bold 15px, `#1a5276`, top center):** "Round by Round — Propose, Hold, Reject (illustrative)".
- **Legend (y=35 line):** green `#008300` 10×10 swatch at (150, 30) + "held" 11px `#6b7280` at (165, 39); red `#e74c3c` swatch at (230, 30) + "rejected" at (245, 39); dashed `rgba(0,131,0,0.5)` line from (340, 35) to (370, 35) + "held from earlier round" at (378, 39).
- **Dividers:** 1px `#e5e9ef` vertical lines at x=245 and x=480 from y=48 to y=246.
- **Panels:** origins x0 = 15, 250, 485 (width 220). Header "Round 1 / Round 2 / Round 3" bold 13px `#1a5276` centered at (x0+110, 62). Candidate names "Alice Bob Carol Dan" bold 12px `#2c3e50` right-aligned at x0+75, rows y = 92, 132, 172, 212. Company letters "A B C D" bold 12px `#1a5276` centered at x0+165, same rows.
- **Arrows:** solid 2px lines with filled rotated arrowheads from (x0+82, rowY−4) to (x0+150, targetY−4); green `#008300` = held, red `#e74c3c` = rejected. Carried holds: dashed `rgba(0,131,0,0.5)` 1.5px (dash 5,4), no arrowhead, same endpoints.
- **Panel 1 arrows:** Alice→A red, Bob→A red, Carol→B green, Dan→A green.
- **Panel 2:** dashed holds Dan–A and Carol–B; arrows Alice→B red, Bob→D green.
- **Panel 3:** dashed holds Dan–A, Carol–B, Bob–D; arrow Alice→C green.
- **Status lines (11px `#6b7280`, centered at (x0+110, 236)):** "A holds Dan; B holds Carol" / "B keeps Carol; D holds Bob" / "C holds Alice — none waiting".
- **Final line (bold 12px green `#008300`, centered at (360, 260)):** "FINAL MATCHING: Dan–A, Carol–B, Alice–C, Bob–D".
- **Caption (12px `#6b7280`, centered at y=284):** "red = rejected, tries again next round; when no one is left proposing, holds become permanent".

## Why No Pair Ever Defects

**Tags:** `where it's used` (blue), `stability guarantee` (green)

- **No blocking pair** — any company a candidate likes better already rejected them for someone better
- **Fast** — every proposal crosses one name off someone's list, so at most n² = 16 proposals ever
- **Here** — this run needed only 7: four in round 1, two in round 2, one in round 3
- **At scale** — with capacities it assigns tens of thousands of doctors and students every year
- **Stability ≠ happiness** — Alice lands her 3rd choice, yet no company she prefers would take her

*Example (italic):* If Alice preferred some company over C, that company refused her earlier — for a candidate it likes more.

**Key point:** The output is guaranteed stable in at most n² proposals — stable meaning no pair wants to defect, not that everyone is happy.

### Visualization (canvas `c3`, 720×300)

Bar chart of proposals per round showing quick convergence.

- **Title (bold 15px, `#1a5276`, top center):** "Proposals per Round — Quick Convergence".
- **Annotation (bold 12px violet `#4a3aa7`, centered at (400, 48)):** "7 proposals in total — the worst case allows n² = 16".
- **Axes:** 1px `#999`, origin (90, 226), y-axis up to y=64, x-axis to (660, 226); y-axis caption "proposals" 11px `#6b7280` left-aligned at (90, 58).
- **Bars:** rounds at x = 150, 330, 510 (width 120), values `[4, 2, 1]` scaled max 4 over 140px (heights 140, 70, 35); fill `rgba(42,120,214,0.5)`, 1px blue `#2a78d6` stroke; value labels bold 14px blue centered above each bar (top − 6); labels "round 1 / round 2 / round 3" 12px `#444` at y=244.
- **Annotation (bold 12px aqua `#199e70`, centered at (480, 150)):** "each rejection crosses one name off a list".
- **Caption (12px `#6b7280`, centered at y=284):** "every proposal is a new candidate–company pair, so the process must stop — here after just 7".

## The Tentative Hold Is the Whole Trick

**Tags:** `common mistake` (red), `tentative vs final` (orange)

- **The trick** — "tentative" is the entire mechanism; everything else is bookkeeping
- **The other rule** — accept on the spot, first come first served: the first proposal is locked in
- **Run it here** — Alice reaches A first and is locked in; Dan arrives late and settles for C
- **The crack** — A prefers Dan and Dan prefers A: a blocking pair, and the matching falls apart
- **Deferring wins** — holding until all proposals settle lets A trade up to Dan before anything is final

*Example (italic):* One early yes to Alice costs A its favorite candidate — Dan was only two proposals away.

**Common mistake:** Believing a held proposal is an accepted one. Immediate acceptance is a different, unstable mechanism — deferring the final yes until the market goes quiet is exactly what buys stability.

### Visualization (canvas `c4`, 720×300)

Two side-by-side matchings: instant acceptance on the left with a red blocking pair, deferred acceptance on the right with no blocking pair.

- **Title (bold 15px, `#1a5276`, top center):** "Accept Instantly vs Hold Tentatively (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=44 to y=262.
- **Left panel header (bold 13px orange `#d95926`, centered at (182, 52)):** "ACCEPT ON THE SPOT".
- **Left panel (rows y = 92, 132, 172, 212):** names "Alice Bob Carol Dan" bold 12px `#2c3e50` right-aligned at x=105; companies "A B C D" bold 12px `#1a5276` centered at x=258; match lines solid 2px orange `#d95926` from (112, rowY−4) to (245, targetY−4): Alice–A, Bob–D, Carol–B, Dan–C (arrival order Alice, Bob, Carol, Dan; each first available choice locked instantly).
- **Blocking pair:** dashed 2px red `#e74c3c` line (dash 6,4) from (112, 208) to (245, 88) with filled red arrowheads at BOTH ends; labels bold 12px red centered at (182, 240) "Dan & A prefer each other" and at (182, 256) "a blocking pair — unstable".
- **Right panel header (bold 13px green `#008300`, centered at (540, 52)):** "HOLD TENTATIVELY (GALE-SHAPLEY)".
- **Right panel (same rows):** names right-aligned at x=460; companies centered at x=612; match lines solid 2px green `#008300` from (467, rowY−4) to (600, targetY−4): Alice–C, Bob–D, Carol–B, Dan–A.
- **Right labels (bold 12px green, centered at (540, 240) and (540, 256)):** "no pair prefers each other" / "over their match — stable".
- **Caption (12px `#6b7280`, centered at y=284):** "same market — locking in the first yes creates the pair that runs off together".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize; shared `arrow(ctx, x1, y1, x2, y2, color, opts)` helper drawing a line (optionally dashed via `opts.dash`) with a filled rotated arrowhead at the end (`opts.noHead` skips it, `opts.bothHeads` adds one at the start); shared `rankList(ctx, x, y, first, rest, color)` helper drawing the first choice bold colored and the remainder plain.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for rejected proposals (c2) and the blocking pair (c4).
- **Data integrity:** hardcoded arrays only — candidate lists `{Alice:[A,B,C,D], Bob:[A,D,C,B], Carol:[B,A,C,D], Dan:[A,B,C,D]}`, company lists `{A:[Dan,Alice,Bob,Carol], B:[Carol,Alice,Dan,Bob], C:[Alice,Bob,Carol,Dan], D:[Bob,Alice,Carol,Dan]}`, proposals per round `[4, 2, 1]`, final matching Dan–A / Carol–B / Alice–C / Bob–D, first-come matching Alice–A / Bob–D / Carol–B / Dan–C with blocking pair (Dan, A); invented preferences carry "(illustrative)" in chart titles.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
