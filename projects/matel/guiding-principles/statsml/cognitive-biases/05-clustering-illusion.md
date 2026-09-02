# Clustering Illusion: Chance Arrives in Lumps

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Clustering Illusion — Cognitive Biases

**Subtitle:** Scatter a hundred dots at random and some corner of the page ends up crowded. Nothing crowded it.

---

## Section 1 — Random Dots Land in Clumps

**Tags:** `core idea` (blue), `lumps and voids` (violet), `nothing caused it` (magenta)

**Bullets:**
- **What you see** — a few tight knots of dots, and a stretch of page with nothing on it
- **What put them there** — nothing; each dot was placed without reference to any other
- **Divide the page** — twenty-five equal squares, four dots apiece if they spread out evenly
- **The crowded square** — holds ten, two and a half times its share, and it catches the eye first
- **The bare square** — one square drew no dots at all, and nobody ever asks why
- **Evenly spread is the odd one** — that needs a force arranging it, and would be the real finding

**Key point:** Clumps and voids are not departures from randomness — they are what randomness produces. A lump means something only once it beats the lumps chance already delivers.

### Visualization — canvas `c1`, 720×330

A hundred seeded dots over a faint five-by-five grid, the crowded square and the empty one called out, with the counts printed from the tally.

- **Data:** seeded Park–Miller LCG, seed 42; 100 points, `x = rng()`, `y = rng()`. Binned into a 5×5 grid: the fullest square holds 10, one square holds 0, average 4.0. All read off the tally in the draw function.
- **Title (bold 15px `P.ink`, centered, y=22):** "A Hundred Dots, Dropped at Random"
- **Plot box:** square, `PY=44`, side `= min(h − PY − 58, 0.62w)`, left-aligned at `PX=44`.
- **Grid:** 5×5 `P.grid` lines, 1px. The fullest square filled `rgba(74,58,167,0.10)` and stroked 2px `P.violet`; the empty square stroked 2px dashed `P.magenta` (dash 4/3).
- **Dots:** radius 4, `rgba(42,120,214,0.55)` stroked `P.blue` 1px. Dots inside the fullest square get `rgba(74,58,167,0.65)` stroked `P.violet` so the knot reads as one group.
- **Callouts:** bold 12px `P.violet` "10 dots here" with a leader line to the fullest square; bold 12px `P.magenta` "none at all" with a leader to the empty one. Both positioned from the tally, both nudged outside the plot box.
- **Side panel** at `PX + side + 26`: bold 13px `P.ink` "IF THEY SPREAD OUT EVENLY", then bold 19px `P.mute` "4.0" + 12px "dots per square"; bold 13px `P.ink` "WHAT CHANCE ACTUALLY DID", then bold 19px `P.violet` "10" + 12px "in the fullest square" and bold 19px `P.magenta` "0" + 12px "in the emptiest". The multiple ("2.5× its share") printed beneath, computed as fullest ÷ average.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "The crowded corner is the ordinary outcome, not the story."

---

## Section 2 — Six Shots in a Row

**Tags:** `streaks` (blue), `hot hand` (violet), `already expected` (magenta)

**Bullets:**
- **What the room decides** — six baskets running, and the player is officially on fire
- **The stand-in** — fifty tosses of a fair coin, where nothing can possibly be on fire
- **What came out** — the longest run of heads in this particular fifty was six
- **How ordinary** — roughly three fifty-toss stretches in ten contain a run that long
- **Runs of four** — those turn up in more than eight stretches in ten, near enough to guaranteed
- **The question people ask** — "what are the odds of six in a row", which has a tiny answer
- **The question that settles it** — how often fifty tries contain a six-run anywhere

**Key point:** A streak is evidence of a hot hand only if it beats what a fair coin already does over the same number of tries. Six in a row does not.

**Source note (`.src`):** Illustrative Example — one seeded toss sequence; every printed chance is exact, computed in the draw function.

### Visualization — canvas `c1b`, 720×320

The fifty tosses as a strip with the longest run boxed, above exact chances for each run length.

- **Data:** seeded LCG, seed 42; 50 draws, heads if `rng() < 0.5`. Yields 29 heads and a longest head-run of 6 starting at index 13, found by a scan.
- **Exact chances:** `pRun(50, k)` — DP over the current run length with one absorbing state. 98% for k=3, 83% for 4, 55% for 5, 31% for 6, 17% for 7, 8% for 8.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Fair Coin, Fifty Tosses"
- **Toss strip:** 50 cells across `x = 42 … w−30` at `y=56`, height 22. Heads `rgba(74,58,167,0.50)` stroked `P.violet`; tails `rgba(107,114,128,0.18)` stroked `#dcdfe4`.
- **Run box:** 2.5px `P.magenta` rectangle inset 3px around the longest run, bold 12px `P.magenta` "longest run: 6 heads" centered above it. Both driven by the scan.
- **Bar panel:** header bold 13px `P.ink` "HOW OFTEN FIFTY TOSSES CONTAIN A RUN THAT LONG". Six horizontal bars on a 26px pitch, track `rgba(107,114,128,0.12)`. Shorter runs `rgba(25,158,112,0.45)`/`P.aqua`; the observed length `rgba(213,81,129,0.45)`/`P.magenta` with "← the run we just saw"; longer `rgba(107,114,128,0.30)`/`P.mute`. Percentages printed from the DP.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "The streak is what a fair coin does, not what a hot hand does."

---

## Section 3 — A Shuffle That Feels Broken Is Working

**Tags:** `where it bites` (aqua), `shuffle` (orange), `complaints` (blue)

**Bullets:**
- **The complaint** — listeners insist shuffle keeps serving up the same singer twice over
- **The playlist** — thirty songs, six singers, five each, ordered by a genuinely fair draw
- **What a fair draw does** — it puts two songs by one singer side by side about four times
- **The order people expect** — no back-to-back repeat at all, seen about twice in a hundred draws
- **So the request** — "make shuffle random" is really "make shuffle stop looking random"
- **What music apps ship** — a draw that deliberately spaces each singer out, and is not fair
- **Why it survives** — the unfair version is the one that gets described as random

**Key point:** People report a fair process as broken when it produces the lumps fairness requires. A process tuned until it feels random has stopped being random.

**Source note (`.src`):** Illustrative Example — figures computed from seeded shuffles of a constructed playlist.

### Visualization — canvas `c2`, 720×320

The seeded playlist as a colored song strip with its repeats bracketed, above the spread of how many repeats a fair draw produces.

- **Construction:** six singers × five songs = 30, Fisher–Yates with the seeded LCG. The strip is the **first** shuffle, containing 4 adjacent same-singer pairs at positions 1, 7, 14, 26 — found by a scan.
- **Spread:** 4,000 seeded shuffles tallied by repeat count; average 4.0 against the exact `29 × (6·5·4)/(30·29) = 4.00`, and 2% with no repeat. Printed from the tally.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Fair Shuffle of Thirty Songs"
- **Song strip:** 30 cells at `y=46`, height 24, each in its singer's colour at 0.5 alpha stroked in the solid hue. Singers cycle `P.blue`, `P.aqua`, `P.violet`, `P.yellow`, `P.magenta`, `P.green` — the strip is the page's one deliberately multi-hue element, since colour *is* the data here.
- **Repeat brackets:** 2.5px `P.aqua` under each adjacent same-singer pair, with bold 12px `P.aqua` "4 back-to-back repeats" beneath, count from the scan.
- **Spread bars:** repeat counts 0–9 as columns. The zero bin `rgba(107,114,128,0.30)`/`P.mute` labelled "expected"; the bin matching the strip `rgba(25,158,112,0.50)`/`P.aqua` labelled "this one"; the rest `rgba(201,133,0,0.40)`/`P.yellow`. Only those two bins get a printed percentage, so the chart does not become a table.
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "Fairness produces the repeats. Removing them is the unfair step."

---

## Section 4 — How Big Must a Lump Be Before It Counts

**Tags:** `the boundary` (green), `what clears the bar` (orange), `common mistake` (red)

**Bullets:**
- **The setting** — forty break-ins across a hundred streets in one month, spread by chance alone
- **Per street** — that averages under half a break-in, so most streets record nothing at all
- **The worst street** — took three, roughly eight times the average, and the residents notice
- **Three is nothing** — chance hands some street three or more in over half of all months
- **Four starts to count** — chance reaches four only about once in sixteen months
- **Five is worth acting on** — chance almost never gets there, so something real is likely at work
- **The mistake** — comparing the worst street to the average rather than to chance's worst street
- **The fix** — ask what the worst street would look like in a month where nothing was wrong

**Key point:** The comparison that decides it is your lump against the biggest lump a fair process throws up — never your lump against the average. Below that line the lump explains itself.

**Source note (`.src`):** Illustrative Example — the reference spread comes from 4,000 seeded months over the same hundred streets.

### Visualization — canvas `c3`, 720×340

A ten-by-ten street map for one seeded month, beside the computed chance that a quiet month produces a lump of each size.

- **Data:** seeded LCG, seed 42; 40 events into 100 cells. The month yields a worst street of 3, 66 streets with nothing, average 0.4. Read off the tally.
- **Spread:** 4,000 seeded months recording the worst street; chance reaches 2+ always, 3+ in 56% of months, 4+ in 6%, 5+ in 0.5%. Stable across 2,000 / 4,000 / 8,000 / 16,000 trials.
- **Title (bold 15px `P.ink`, centered, y=22):** "Forty Break-Ins, One Hundred Streets, One Month"
- **Map:** 10×10 cells at `PX=44`, `PY=48`, cell `= min(24, (0.52w)/10)`. Empty streets `rgba(107,114,128,0.10)` stroked `#e5e9ef`; one event `rgba(201,133,0,0.40)` stroked `P.yellow`; two `rgba(217,89,38,0.45)` stroked `P.orange`; the worst street `rgba(217,89,38,0.70)` stroked 2.5px `P.orange` with its count printed bold 12px white inside.
- **Map legend (12px `P.mute`, under the map):** "each square is one street — colour is how many break-ins"; then bold 12px `P.orange` "worst street: 3" and 12px `P.mute` "66 streets had none", both from the tally.
- **Chance panel** at `0.60w`: bold 13px `P.ink` "IN A MONTH WITH NOTHING WRONG, HOW OFTEN CHANCE GIVES SOME STREET —", then four rows on a 34px pitch. Each row: bold 19px figure, 12px `P.mute` label ("three or more", "four or more", "five or more"), and a plain-language frequency ("about every other month", "about once in 16 months", "about once in 200 months") computed as `1 / p`. Rows above the bar in `P.mute`, the row where chance runs out in `P.green`.
- **Verdict strip** under the chance panel: bold 12px `P.green` "this month's worst street clears nothing" — the comparison stated from the tally, not asserted.
- **Caption (bold 13px `P.green`, centered, `h−9`):** "Compare the lump to chance's biggest lump, not to the average."

---

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. The canvas is capped at 720px, so a wide cell leaves slack — centering puts the chart in the middle of the right half.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>term</b>` → one `.key-point` callout → `.src` note **only where the figures are constructed**. No paragraph blocks, no data tables.
- **Bullets: count follows the content, never a quota.** Six where six covers it, eight where the mechanism needs eight. Do not pad a section to reach a number, and do not add an `.example` line that restates a bullet — if the bullets already carry the fact, the section ends at the key point.
- **Bullet form:** each is ONE line that does not wrap at 50% column width (~≤95 characters), opening with a `<b>bold term</b>` then an em dash and the fact.
- **Section titles:** name the content. No role labels, no phrasing reused from other pages.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no back/home links, no cross-page links, no `.nav` CSS.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Beyond the four base classes this page adds `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d` — so a section's pills match its chart's dominant hue.
- **Colour variety across sections is a requirement, not a preference.** Each section owns a hue family and its pills, chart fills, and caption all sit in it: section 1 violet/blue with a magenta void, section 2 violet strip with magenta/aqua bars, section 3 the multi-hue song strip with yellow/aqua bars, section 4 yellow/orange map with a green verdict. Do not let blue-fill-plus-orange-highlight become every chart.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart `height`. `setup(id)` caches the logical size in `dataset` on the first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart header bold 12–13px; body/axis labels 12px floor; the big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42. Cell counts, run lengths, repeat counts, and every percentage are computed in the draw function and printed from those variables, so a label cannot drift from the plotted data.
- **The lead chart must show a visible clump.** The page is about clustering; if no chart contains a knot of points the reader can see, the page has not made its case. An earlier draft opened on a distribution of a maximum — second-order, and it showed no cluster at all.
- **Corrections applied to earlier versions of this page:** a histogram highlighted a bin 3.05 standard deviations above expected (72 against 51.3, reached by chance only ~1.2% of the time) while captioning it "within random variance" — it demonstrated the opposite of its lesson. A birthday-month example has been dropped. A "busiest of ten support areas" section was replaced by the street map, which shows the lump instead of describing it.
