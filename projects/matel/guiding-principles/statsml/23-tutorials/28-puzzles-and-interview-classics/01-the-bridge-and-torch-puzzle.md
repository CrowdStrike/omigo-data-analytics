# The Bridge and Torch Puzzle

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Bridge and Torch Puzzle

**Subtitle:** Four hikers, one torch, a bridge that holds two — the obvious plan of having the fastest person ferry everyone across loses by two minutes, because sending the two slowest together makes one slow walk free

## Four Hikers, One Torch

**Tags:** `core idea` (blue), `puzzle setup` (green), `pair pace` (orange)

- **The scene** — night falls; Amy, Ben, Cara, and Dan must cross an old rope bridge with one torch
- **The times** — Amy crosses in 1 minute, Ben in 2, Cara in 5, Dan in 10; those speeds never change
- **The rules** — at most two on the bridge at once, and whoever crosses must carry the torch
- **Pair pace** — two people walking together move at the slower one's speed: Amy + Dan takes 10
- **The question** — someone must keep walking the torch back; what order gets all four over fastest?

*Example (italic):* If Amy and Dan cross together, Amy's 1-minute legs don't help — the pair arrives after Dan's full 10 minutes.

**Key point:** The torch must shuttle back and forth, and every pair moves at its slower member's pace — those two rules are the whole puzzle.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: one bar per hiker showing their solo crossing time, with a bracket annotation showing that a pair walks at the slower member's pace.

- **Title (bold 15px, `#1a5276`, top center):** "Four Hikers, One Torch — a Pair Walks at the Slower Pace".
- **Axis:** horizontal 2px `#999` line at y=255 from x=200 to x=680 (width 480), minutes 0 to 10 mapped 48px per minute; 12px `#444` tick labels "0", "2", "4", "6", "8", "10 min" every 2 minutes; light `#e5e9ef` vertical gridlines at each tick.
- **Rows (bar height 26, tops at y = 65, 115, 165, 215), left-aligned 12px `#444` name labels at x=20:** "Amy — 1 min", "Ben — 2 min", "Cara — 5 min", "Dan — 10 min".
- **Bars:** durations `[1, 2, 5, 10]` from x=200; Amy green `#008300`, Ben blue `#2a78d6`, Cara orange `#d95926`, Dan magenta `#d55181`; bold 12px value label ("1", "2", "5", "10") just right of each bar end in the bar's color.
- **Annotation (bold 13px ink `#1a5276`, near x=420, y=95):** two lines: "Amy + Dan together = 10 min" / "her speed is wasted next to him".
- **Caption (12px `#444`, bottom right):** "classic 1-2-5-10 version of the puzzle".

## The Obvious Plan Loses by Two Minutes

**Tags:** `worked example` (blue), `step by step` (green), `greedy vs clever` (orange)

- **The greedy plan** — Amy ferries each person over and jogs back with the torch every time
- **Greedy steps** — Amy+Dan cross (10), Amy back (1), Amy+Cara cross (5), Amy back (1), Amy+Ben (2)
- **Greedy total** — 10 + 1 + 5 + 1 + 2 = 19 minutes; the natural fastest-courier plan
- **The clever plan** — send the fast pair first, then let the two slowest cross together, once
- **Clever steps** — Amy+Ben cross (2), Amy back (1), Cara+Dan cross (10), Ben back (2), Amy+Ben (2)
- **Clever total** — 2 + 1 + 10 + 2 + 2 = 17 minutes; same people, same bridge, two minutes saved

*Example (italic):* Anyone can check both plans on paper: 10+1+5+1+2 = 19 for greedy, 2+1+10+2+2 = 17 for the clever order.

**Key point:** Greedy's fastest-courier plan costs 19 minutes; pairing Cara and Dan on one crossing gets everyone over in 17.

### Visualization (canvas `c2`, 720×300)

Two horizontal timeline rows on a shared minutes axis: the greedy plan's five trips stacked end to end (total 19) above the clever plan's five trips (total 17), with return trips visually distinct.

- **Title (bold 15px, `#1a5276`, top center):** "Two Plans, Trip by Trip: 19 Minutes vs 17".
- **Axis:** horizontal 2px `#999` line at y=255 from x=60 to x=680, minutes 0 to 20 mapped 31px per minute; 12px `#444` tick labels "0", "5", "10", "15", "20 min" every 5 minutes; light `#e5e9ef` vertical gridlines at each tick.
- **Row labels (bold 13px `#1a5276`):** "greedy — Amy ferries everyone" at (60, 85); "clever — slowest pair crosses together" at (60, 175).
- **Greedy row (bar tops y=95, height 24):** segment starts `[0, 10, 11, 16, 17]`, durations `[10, 1, 5, 1, 2]`, labels `["Amy+Dan 10", "back 1", "Amy+Cara 5", "back 1", "Amy+Ben 2"]`; crossings filled blue `#2a78d6`, returns filled orange `#d95926`; 11px white labels inside wide segments, 11px `#444` labels above segments narrower than 60px.
- **Clever row (bar tops y=185, height 24):** segment starts `[0, 2, 3, 13, 15]`, durations `[2, 1, 10, 2, 2]`, labels `["Amy+Ben 2", "back 1", "Cara+Dan 10", "back 2", "Amy+Ben 2"]`; same crossing/return colors.
- **Totals:** bold 14px labels just right of each row's last segment — magenta `#d55181` "19 min" for greedy, green `#008300` "17 min" for clever.
- **Annotation (bold 13px green `#008300`, near x=440, y=150):** "same four people — 2 minutes saved".
- **Caption (12px `#444`, bottom right):** "each block is one torch trip; returns in orange".

## Why the Ferry Instinct Fails

**Tags:** `why it matters` (blue), `greedy algorithms` (orange)

- **The instinct** — "use the fastest walker for every trip" picks the cheapest move at each step
- **The blind spot** — greedy pays for Dan's 10 and Cara's 5 as two separate slow crossings, 15 in all
- **The free ride** — clever puts Cara beside Dan, so her 5-minute walk hides inside his 10
- **The price** — clever's extra fast trips and Ben's 2-minute return cost a bit, but far less than 5
- **The lesson** — a locally cheapest move can lock in a globally expensive plan; greedy is a bet
- **Where it bites** — scheduling, routing, and batching jobs all have this pair-the-costs flavor

*Example (italic):* Greedy spends 17 minutes on crossings and 2 on returns; clever spends 14 on crossings and 3 on returns — 19 vs 17.

**Key point:** Greedy loses because it never considers overlapping the two big costs — Cara's crossing is free once she walks in Dan's shadow.

### Visualization (canvas `c3`, 720×300)

Two stacked vertical bars comparing where each plan's minutes go: crossing minutes on the bottom, return-trip minutes on top, showing clever's crossings shrink by 5 while its returns grow by only 1.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Minutes Go: Crossings vs Returns".
- **Axis:** baseline 2px `#999` at y=250 from x=140 to x=560; y scale 10px per minute (20 minutes = 200px tall); light `#e5e9ef` gridlines at 5, 10, 15 minutes with 12px `#444` labels "5", "10", "15" at x=115.
- **Bars (width 120):** greedy centered at x=240, clever centered at x=460; bottom segment = crossing minutes filled blue `#2a78d6` (greedy 17, clever 14), top segment = return minutes filled orange `#d95926` (greedy 2, clever 3); 12px white labels inside segments: "crossings 17", "returns 2", "crossings 14", "returns 3".
- **Bar labels:** bold 13px `#444` "greedy" and "clever" under each bar at y=272; bold 14px totals above bars — magenta `#d55181` "19 min", green `#008300` "17 min".
- **Annotation (bold 13px green `#008300`, near x=580, y=140):** two lines: "Cara's 5-minute walk" / "hides inside Dan's 10".
- **Caption (12px `#444`, bottom right):** "minutes from the two worked plans above".

## When the Ferry Plan Actually Wins

**Tags:** `common mistake` (red), `rule of thumb` (orange)

- **The over-correction** — after this puzzle, people pair the slowest pair every time, reflexively
- **New times** — make Ben slower: with times 1, 4, 5, 10, the clever pairing trick now backfires
- **Ferry wins** — Amy ferrying everyone: 10 + 1 + 5 + 1 + 4 = 21; pairing: 4 + 1 + 10 + 4 + 4 = 23
- **The reason** — pairing needs Ben to walk two extra trips, and a 4-minute Ben is too pricey a courier
- **The check** — pair the slowest two only when twice Ben's time beats Amy's plus Cara's time

*Example (italic):* With times 1-2-5-10 pairing wins (2×2 = 4 is less than 1+5 = 6); with 1-4-5-10 ferrying wins (2×4 = 8 is more than 6).

**Common mistake:** Swapping one reflex for another. The clever pairing is not a law — compare 2× the second-fastest against fastest + third-fastest before choosing.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: two scenarios (times 1-2-5-10 and 1-4-5-10), each with a ferry-plan bar and a pairing-plan bar, showing the winner flips when Ben slows down.

- **Title (bold 15px, `#1a5276`, top center):** "Change One Walker's Speed and the Winner Flips".
- **Axis:** baseline 2px `#999` at y=250 from x=100 to x=680; y scale 8px per minute (25 minutes = 200px); light `#e5e9ef` gridlines at 5, 10, 15, 20 minutes with 12px `#444` labels at x=75.
- **Scenario A (times 1, 2, 5, 10), bars width 90:** ferry bar at x=170 height 19 min filled `rgba(42,120,214,0.35)` with 2px blue `#2a78d6` border; pairing bar at x=280 height 17 min filled solid green `#008300`; bold 13px totals above: "19" (blue), "17" (green); winner tag bold 12px green "pairing wins" centered over the pairing bar, just above its "17" total.
- **Scenario B (times 1, 4, 5, 10), bars width 90:** ferry bar at x=460 height 21 min filled solid blue `#2a78d6`; pairing bar at x=570 height 23 min filled `rgba(0,131,0,0.35)` with 2px green border; bold 13px totals above: "21" (blue), "23" (green); winner tag bold 12px blue "ferry wins" above the pair.
- **Group labels (13px `#444`, y=272, centered under each pair):** "times 1, 2, 5, 10" and "times 1, 4, 5, 10".
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=370, y=70):** "rule: pair the slowest two only when 2 × second-fastest < fastest + third".
- **Caption (12px `#444`, bottom right):** "illustrative times; totals checkable by hand".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, segment starts, and durations are the hardcoded arrays above (no randomness); every total in a chart matches the arithmetic shown in that section's text (19 = 10+1+5+1+2, 17 = 2+1+10+2+2, 21 = 10+1+5+1+4, 23 = 4+1+10+4+4).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
