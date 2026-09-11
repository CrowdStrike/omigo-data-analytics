# Floyd's Cycle Detection

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Floyd's Cycle Detection

**Subtitle:** Send a slow walker and a fast runner down the same path — if the path ever loops back on itself, the runner is guaranteed to catch the walker, and that meeting proves the loop exists

## Two Friends on a Looping Trail

**Tags:** `core idea` (blue), `tortoise & hare` (green), `two pointers` (orange)

- **The trail** — a jogging trail passes water stations 0, 1, 2, then station 3 starts a circle: 3→4→5→6→7→8 and back to 3
- **The question** — from the start you cannot see the whole trail; how do you find out it loops without a map?
- **Two friends** — a walker covers 1 station per minute, a runner covers 2; both leave station 0 together
- **Straight trail** — if the trail never loops, the runner just pulls away and reaches the end first, alone
- **Looping trail** — on a loop the runner comes around from behind and lands on the walker's station
- **The proof** — the moment they stand on the same station again, the trail must contain a loop

*Example (italic):* Six minutes after leaving station 0, the walker and the runner both stand at station 6 — no map needed, the loop is proven.

**Key point:** One slow and one fast pointer on the same path meet again if and only if the path has a cycle — that is the whole algorithm.

### Visualization (canvas `c1`, 720×300)

Node-and-arrow diagram of the trail: three tail stations feeding into a six-station circle, with the walker and runner drawn at their meeting station.

- **Title (bold 15px, `#1a5276`, top center):** "The Trail: 3 Straight Stations, Then a 6-Station Loop".
- **Tail nodes:** circles radius 18, 2px `#1a5276` stroke, white fill, centers at (80, 170), (170, 170), (260, 170); bold 13px `#2c3e50` labels "0", "1", "2" centered inside.
- **Loop nodes:** same style, six circles on a ring centered (500, 170) radius 85, station "3" at the leftmost point (415, 170) and "4", "5", "6", "7", "8" clockwise from it (every 60 degrees); bold 13px labels inside.
- **Arrows:** 2px `#6b7280` straight arrows 0→1→2→3 and curved arrows along the ring 3→4→5→6→7→8→3, each with a small filled arrowhead; the 8→3 arrow is the one that closes the loop.
- **Loop-back highlight:** the 8→3 arrow drawn 3px in magenta `#d55181`, 12px magenta label "back to 3!" beside it.
- **Meeting marker:** station 6's circle gets a 3px green `#008300` ring and two dots just above it — blue `#2a78d6` 7px dot labeled "walker" and orange `#d95926` 7px dot labeled "runner" (12px labels).
- **Annotation (bold 13px green `#008300`, near x=500, y=45):** "they meet at station 6 — the loop is real".
- **Caption (12px `#444`, bottom right):** "illustrative trail — tail of 3 stations, loop of 6".

## Minute by Minute: They Meet at Station 6

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **Setup** — walker moves 1 station per minute, runner 2; positions each minute can be checked by hand
- **Walker's path** — minutes 0–6 put the walker at stations 0, 1, 2, 3, 4, 5, 6
- **Runner's path** — the runner hits 0, 2, 4, 6, 8, then wraps past 3 to land on 4, then 6
- **The wrap** — at minute 5 the runner leaves 8, steps to 3 and then 4 — the loop pulls him back behind
- **The meeting** — at minute 6 both stand at station 6: walker walked 6 stations, runner ran 12
- **Loop length** — 12 − 6 = 6 extra stations the runner covered equals exactly one lap of the loop

*Example (italic):* Write the two rows yourself — walker 0,1,2,3,4,5,6 and runner 0,2,4,6,8,4,6 — and the columns match at minute 6.

**Key point:** Walker at 0,1,2,3,4,5,6 and runner at 0,2,4,6,8,4,6 collide at station 6 on minute 6 — seven numbers each, checkable by hand.

### Visualization (canvas `c2`, 720×300)

Two-line step chart of station number versus minute: the walker's line climbs steadily, the runner's line climbs twice as fast, wraps back down after station 8, and lands on the walker's line at minute 6.

- **Title (bold 15px, `#1a5276`, top center):** "Station Number Each Minute — the Lines Collide at Minute 6".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; x = minute 0 to 6 with 12px `#444` tick labels "0"–"6"; y = station 0 to 8 with 12px `#444` labels "0", "2", "4", "6", "8" and light `#e5e9ef` gridlines at 2, 4, 6, 8.
- **Walker line:** blue `#2a78d6` 3px line through points (minute, station) = `[[0,0],[1,1],[2,2],[3,3],[4,4],[5,5],[6,6]]`, 5px blue dots at each point; 12px blue label "walker (1/min)" near minute 4.3, station 3.4.
- **Runner line:** orange `#d95926` 3px line through `[[0,0],[1,2],[2,4],[3,6],[4,8],[5,4],[6,6]]`, 5px orange dots; the drop from (4,8) to (5,4) drawn dashed (dash 6/4) to show the wrap past station 3; 12px orange label "runner (2/min)" near minute 2.1, station 5.2.
- **Wrap note (12px `#6b7280`, near minute 4.5, station 6.6):** "wraps: 8 → 3 → 4".
- **Meeting marker:** green `#008300` 9px ring around the shared point (6, 6); vertical dashed green line (dash 4/3) from the baseline up to it.
- **Annotation (bold 13px green `#008300`, near minute 5, station 1.6):** two lines: "minute 6: both at station 6" / "walker went 6, runner went 12".

## Why a Data Scientist Cares About a Loop Check

**Tags:** `where it's used` (blue), `tiny memory` (green), `hidden loops` (orange)

- **Linked chains** — records that point to a "next" record (tickets, redirects, parent IDs) can secretly loop
- **Iterated functions** — feeding a function its own output (simulations, hash chains) eventually repeats
- **The naive fix** — remember every station visited in a notebook; the notebook grows with every step
- **Floyd's fix** — two pointers need memory for exactly 2 positions no matter how long the trail is
- **Bonus round** — restart one pointer at station 0, move both 1/min: they re-meet at 3, the loop's door
- **Guaranteed stop** — the check finishes within a few laps, so it never runs forever on a loop

*Example (italic):* A "manager of manager" lookup that secretly circles through 3 employees would spin forever — the walker-runner check flags it while remembering only 2 positions.

**Key point:** Floyd's trick detects a loop using memory for just 2 positions, while the keep-a-notebook approach must store every station it has ever seen.

### Visualization (canvas `c3`, 720×300)

Two-line comparison chart: positions remembered versus steps taken, the notebook approach climbing linearly while Floyd's two-pointer line stays flat at 2.

- **Title (bold 15px, `#1a5276`, top center):** "Memory Needed to Catch a Loop: Notebook vs Two Pointers".
- **Axes:** origin x=80, baseline y=245, plot width 570, plot height 185; x = steps taken, ticks at `[0, 200, 400, 600, 800, 1000]` (12px `#444`); y = positions remembered 0 to 1000, labels "0", "250", "500", "750", "1000" with light `#e5e9ef` gridlines.
- **Notebook line:** magenta `#d55181` 3px line through (steps, remembered) = `[[0,0],[200,200],[400,400],[600,600],[800,800],[1000,1000]]`; 12px magenta label "remember every visit" above its midpoint.
- **Floyd line:** green `#008300` 3px line through `[[0,2],[200,2],[400,2],[600,2],[800,2],[1000,2]]`; bold 12px green label "two pointers — always 2" just above the line near steps 620.
- **Gap arrow:** thin dashed `#6b7280` (dash 4/3) vertical arrow at steps 1000 from the Floyd line up to the notebook line, 12px `#6b7280` label "998 fewer" beside it.
- **Annotation (bold 13px green `#008300`, near steps 260, height 820):** "same answer, flat memory".
- **Caption (12px `#444`, bottom right):** "illustrative — a 1,000-step trail".

## But Won't the Runner Jump Over the Walker?

**Tags:** `common mistake` (red), `gap shrinks by 1` (orange)

- **The worry** — the runner moves 2 at a time, so surely he can hop over the walker without landing on him
- **Watch the gap** — once both are on the loop, measure how far the runner is behind the walker
- **Our trail** — at minutes 3, 4, 5, 6 that gap is 3, 2, 1, 0 stations: it shrinks by exactly 1 each minute
- **Why exactly 1** — each minute the walker gains 1 and the runner gains 2, so the runner closes 1 net
- **No hop** — a gap that falls 3, 2, 1, 0 must pass through 0; it cannot skip from 1 to −1
- **Second trap** — the meeting station (6) is NOT the loop entrance (3); a second phase finds the door

*Example (italic):* From minute 3 the runner trails the walker by 3, then 2, then 1, then 0 — landing exactly on him at station 6, never over him.

**Common mistake:** Assuming the fast pointer can leapfrog the slow one, or that the meeting point is the start of the loop. The gap closes by exactly 1 per step so it must hit 0, and the loop's entrance takes one more (equal-speed) pass to find.

### Visualization (canvas `c4`, 720×300)

Bar chart of the runner-to-walker gap on the loop at minutes 3 through 6, dropping one station per minute down to zero.

- **Title (bold 15px, `#1a5276`, top center):** "Gap Between Runner and Walker Once Both Are on the Loop".
- **Axes:** origin x=90, baseline y=240, plot width 540, plot height 170; x = minute, four slots labeled "min 3", "min 4", "min 5", "min 6" (12px `#444`); y = gap in stations 0 to 3, labels "0"–"3" with light `#e5e9ef` gridlines at 1, 2, 3.
- **Bars:** four bars 70px wide, gap values `[3, 2, 1, 0]`, fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; bold 13px `#1a5276` value label above each bar ("3", "2", "1", "0"); the minute-6 bar has height 0, so draw a green `#008300` 8px dot on the baseline with bold 13px green label "0 — caught!".
- **Step arrows:** thin `#6b7280` arrows between consecutive bar tops, each with an 11px `#6b7280` label "−1".
- **Annotation (bold 13px orange `#d95926`, near the minute-4 bar, y=70):** two lines: "closes by exactly 1 per minute —" / "it can never skip over 0".
- **Caption (12px `#444`, bottom right):** "gap measured along the 6-station loop".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** every position, line point, bar value, and gap value is a hardcoded literal array as specced above (no randomness); walker `[0,1,2,3,4,5,6]` and runner `[0,2,4,6,8,4,6]` come from the tail-3/loop-6 trail and must match the text; the memory-comparison numbers in c3 are illustrative and labeled so.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
