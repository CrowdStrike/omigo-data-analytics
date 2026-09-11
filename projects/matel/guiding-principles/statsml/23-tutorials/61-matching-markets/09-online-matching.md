# Online Matching

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Online Matching

**Subtitle:** When one side arrives over time, you must match now or lose the chance — without knowing who comes next

## Match Now or Lose the Chance

**Tags:** `core idea` (blue), `online algorithms` (green)

- **The scene** — a ride-hailing dispatcher: drivers sit parked, requests pop up one at a time
- **The clock** — each request needs a driver within seconds; riders cancel if the app hesitates
- **The blindfold** — the dispatcher cannot see the next hour's requests, or even the next minute's
- **The luxury lost** — offline assignment sees the whole list first; here decisions can't wait for it
- **The tension** — commit a driver now, or hold capacity for a future request that may never come

*Example (italic):* A request appears; you answer in five seconds with a driver — knowing nothing about who calls next.

**Key point:** Online matching is assignment with the future hidden — every arrival forces a commit-or-hold decision before the next arrival is known.

### Visualization (canvas `c1`, 720×300)

A timeline with requests appearing left to right, each pinned to an immediate driver commitment from above, and a shaded "unknown future" region on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Requests Arrive; Each Forces an Answer Now (illustrative)".
- **Time axis:** 1px `#999` line from (60, 230) to (690, 230) with a small filled right arrowhead at (690, 230); tick labels 12px mute `#6b7280` at y=250: "0s" at x=70, "60s" at x=655.
- **Requests:** magenta `#d55181` filled circles r=8 centered on the axis at x = 150, 300, 450; below each, bold 12px magenta labels at y=252: "R1 (t=9s)", "R2 (t=24s)", "R3 (t=39s)".
- **Driver commitments:** above each request, a box 110×28 centered at the request x (top edge y=118): fill `#fbfcfd`, 2px blue `#2a78d6` border, bold 12px blue centered text "→ driver A" / "→ driver B" / "→ driver C" at box mid; a 2px blue vertical arrow from (x, 146) down to (x, 216) with a small filled downward arrowhead at the bottom.
- **Unknown future:** rect (560, 70) to (690, 230) filled `rgba(107,114,128,0.10)`; bold 26px mute "?" centered at (625, 140); bold 12px mute "who calls next?" centered at (625, 170); 11px mute "unknown" centered at (625, 188).
- **Callout (bold 12px violet `#4a3aa7`, centered at y=284):** "each arrival is answered before the next is known — no do-overs".

## Two Drivers, Two Requests, One Regret

**Tags:** `worked example` (blue), `greedy vs batch` (orange)

- **The setup** — driver X waits downtown, driver Y waits uptown; both free (illustrative minutes)
- **Request 1** — downtown: X is 2 min away, Y is 4 min; greedy sends the closest, X
- **Request 2** — downtown, one minute later: only Y is left, and Y is 9 min away
- **Greedy total** — 2 + 9 = 11 minutes of rider waiting across the two pickups
- **Hindsight optimum** — Y to request 1 (4 min), X to request 2 (2 min): 6 minutes total
- **The batching fix** — wait 30 seconds, collect both, solve the tiny assignment; dispatchers do this

*Example (italic):* Greedy's first pick was locally perfect (2 < 4) and globally wrong — it spent the only downtown driver.

**Key point:** A locally best match can burn the one resource the next arrival needs; batching a short window turns two forced guesses into one small assignment problem.

### Visualization (canvas `c2`, 720×300)

Two map-style panels split by a divider: greedy assignment on the left (2 + 9 = 11 min), batched assignment on the right (4 + 2 = 6 min).

- **Title (bold 15px, `#1a5276`, top center):** "Greedy vs Batched: Same Two Requests (illustrative minutes)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=40 to y=240.
- **Panel headers (bold 13px, centered at y=52):** "GREEDY — closest driver each time" in orange `#d95926` at x=190; "BATCHED — wait 30s, assign together" in green `#008300` at x=545.
- **Region labels (11px mute):** "uptown" at (65, 88) and (405, 88); "downtown" at (65, 236) and (405, 236).
- **Nodes (both panels, mirrored 340px apart):** drivers are 12×12 filled blue `#2a78d6` squares — X at (95, 200) / (435, 200) with bold 12px blue "X" at 16px left of the square and 11px mute "(downtown)" 14px below; Y at (95, 95) / (435, 95) with "Y" and "(uptown)" likewise. Requests are magenta `#d55181` filled circles r=7 — R1 at (275, 172) / (615, 172) with bold 12px magenta "R1" 12px above; R2 at (295, 218) / (635, 218) with "R2" 14px to the right.
- **Left (greedy) arrows:** 2.5px blue line X(95,200)→R1(275,172) with filled arrowhead at R1 end, bold 13px blue "2 min" at the midpoint offset 12px above; 2.5px red `#e74c3c` line Y(95,95)→R2(295,218) with arrowhead, bold 13px red "9 min" at the midpoint offset 14px left/above. Total (bold 13px red, centered at (190, 262)): "total = 2 + 9 = 11 min".
- **Right (batched) arrows:** 2.5px green line Y(435,95)→R1(615,172) with arrowhead, bold 13px green "4 min" at the midpoint offset above; 2.5px green line X(435,200)→R2(635,218) with arrowhead, bold 13px green "2 min" at the midpoint offset above. Total (bold 13px green, centered at (545, 262)): "total = 4 + 2 = 6 min".
- **Caption (12px `#6b7280`, centered at y=288):** "greedy's first pick (2 < 4) spent the only downtown driver".

## The Guarantee Game

**Tags:** `rule of thumb` (green), `competitive ratio` (blue), `ad budgets` (orange)

- **The yardstick** — competitive ratio: the worst-case fraction of the hindsight optimum you keep
- **Greedy's floor** — for online bipartite matching, greedy always captures at least 1/2 of optimum
- **The ceiling** — a randomized ranking rule guarantees 1−1/e ≈ 63%; no algorithm can beat that
- **The ad twist** — advertisers carry daily budgets; every query must go to one bidder instantly
- **The starvation trap** — draining a flexible bidder early starves later queries only they serve
- **Water-filling** — prefer the bidder with the most budget left, so all budgets drain evenly

*Example (italic):* An adversary designs the worst possible arrival order — a 63% guarantee holds even then.

**Key point:** Since seeing the future is impossible, online algorithms are graded on worst-case guarantees: greedy keeps 1/2 of hindsight, and the best possible rule keeps 1−1/e ≈ 63%.

### Visualization (canvas `c3`, 720×300)

Left: three bars comparing worst-case guarantees (50% / 63% / 100%). Right: a two-advertiser budget-depletion sketch with the water-filling rule.

- **Title (bold 15px, `#1a5276`, top center):** "The Guarantee Game: Worst-Case Fraction of Hindsight".
- **Left bars:** base y=230, height = pct/100 × 150px; bars at x = 60/170/280, width 80 — greedy 50% (fill `rgba(217,89,38,0.45)`, 1px orange `#d95926` stroke), ranking 63% (fill `rgba(42,120,214,0.45)`, 1px blue stroke), hindsight 100% (fill `rgba(0,131,0,0.35)`, 1px green stroke); bold 14px value labels in the bar's stroke color 8px above each bar top: "50%", "63%", "100%"; 12px `#2c3e50` names centered at y=248: "greedy", "ranking", "hindsight"; 11px mute second line at y=262: "guaranteed", "1−1/e, best possible", "sees the future".
- **Divider:** 1px `#e5e9ef` vertical line at x=390 from y=44 to y=262.
- **Right header (bold 13px `#1a5276`, centered at (545, 56)):** "ad budgets: don't drain the flexible one".
- **Budget bars (illustrative):** row A — 12px `#2c3e50` left-aligned "Advertiser A — flexible" at (420, 78); outline rect (420, 84, 240, 24) stroke 1px mute; remaining-budget fill `rgba(42,120,214,0.45)` width 36 (15% left); bold 11px blue "15% left by noon" at (466, 100). Row B — "Advertiser B — niche only" at (420, 140); outline rect (420, 146, 240, 24); fill `rgba(0,131,0,0.35)` width 192 (80% left); bold 11px green "80% left" at (420+96, 162) centered on the fill.
- **Starvation line (bold 12px red `#e74c3c`, centered at (545, 200)):** "3pm: a query only A can serve — A is empty ✕".
- **Rule (bold 12px green, centered):** "water-filling: pick the bidder with" at (545, 226) and "the most budget left" at (545, 242).
- **Caption (11px `#6b7280`, centered at y=286):** "left: guarantees for online bipartite matching; right: illustrative budget pacing".

## Every Impatient Marketplace Plays This Game

**Tags:** `where it's used` (blue), `match-now vs wait` (green)

- **Ad exchanges** — every query is matched to a budgeted advertiser within milliseconds
- **Delivery apps** — couriers and orders collect in short batching windows, then match together
- **Cloud schedulers** — jobs arrive over time and must land on machines with finite capacity
- **The recurring dial** — match now for speed, or wait and batch for quality; every system tunes it
- **The trade** — a 30-second window costs a little latency and buys a much better assignment

*Example (italic):* A delivery app that batches orders for half a minute pairs couriers far better than one that fires instantly.

**Key point:** The match-now-vs-wait dial is the operational knob of every online marketplace: batching windows trade seconds of waiting for a permanently better set of matches.

### Visualization (canvas `c4`, 720×300)

A trade-off chart: average pickup time falls steeply then flattens as the batching window grows, while the rider's wait for an answer rises linearly; a dashed marker picks the ≈30s sweet spot.

- **Title (bold 15px, `#1a5276`, top center):** "The Match-Now-vs-Wait Dial (illustrative)".
- **Axes:** 1px `#999` — y axis from (70, 60) to (70, 235), x axis from (70, 235) to (670, 235).
- **X ticks (12px mute, y=252):** windows `[0, 10, 20, 30, 45, 60]` seconds at x = 70/170/270/370/520/670, labeled "0s 10s 20s 30s 45s 60s"; x caption 12px `#2c3e50` "batching window" centered at (370, 272).
- **Blue curve (avg pickup time):** values `[9.0, 7.4, 6.4, 5.8, 5.5, 5.4]` minutes at the six window x positions; y = 235 − (v − 5.0)/4.5 × 165 (so 9.0→88, 7.4→147, 6.4→184, 5.8→206, 5.5→217, 5.4→220); 2.5px blue `#2a78d6` polyline with 3.5px filled dots; end value labels bold 12px blue "9.0 min" at (86, 80) and "5.4 min" at (648, 208); line label bold 12px blue "avg pickup time" at (205, 122).
- **Orange line (latency cost):** straight 2px orange `#d95926` from (70, 235) to (670, 115) — y = 235 − w/60 × 120, i.e. the answer-delay equals the window itself; line label bold 12px orange "rider waits for the window" at (480, 132).
- **Sweet spot:** dashed (6,4) 1.5px green `#008300` vertical line at x=370 from y=70 to y=235; bold 12px green label centered at (370, 60): "≈30s: most of the gain, little wait".
- **Caption (12px `#6b7280`, centered at y=292):** "illustrative: longer windows buy better matches, with fast-diminishing returns".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for genuine failure states: the greedy 9-min haul and 11-min total in c2, the starved 3pm query in c3. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** hardcoded arrays only, no `Math.random()` — pickup minutes greedy `[2, 9]` (total 11), hindsight `[4, 2]` (total 6); guarantees `[50, 63, 100]`; budget-remaining fractions `[0.15, 0.80]`; batching curve windows `[0, 10, 20, 30, 45, 60]` vs pickup minutes `[9.0, 7.4, 6.4, 5.8, 5.5, 5.4]`. All invented numbers carry "(illustrative)" in chart titles or captions, and worked-example numbers in the text match the charts exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
