# The Two-Egg Problem

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Two-Egg Problem

**Subtitle:** With two phones and a hundred floors, the winning trick is shrinking jumps — start at floor 14 and every possible outcome costs at most 14 drops

## A Hundred Floors and Only Two Phones

**Tags:** `core idea` (blue), `worst case` (green), `search under limits` (orange)

- **The tower** — a case maker wants the highest floor of a 100-floor tower a cased phone survives
- **The budget** — only two test phones exist; once both are cracked, the testing is over for good
- **One phone** — with a single phone you must climb floor by floor: up to 100 drops in the worst case
- **Two phones** — the first phone can jump ahead, but its first crack ends all jumping
- **The catch** — after a crack, the second phone must climb one floor at a time from the last safe floor
- **Jump by 10s** — drop at 10, 20, ..., 100; a crack at 100 forces climbing 91–99: 10 + 9 = 19 drops

*Example (italic):* Jumping by tens, the unlucky case is a phone that survives floor 99: ten jumps to 100, a crack, then nine climbs from 91 — 19 drops in all.

**Key point:** The score is the worst case, not the average — a strategy is judged by its unluckiest possible run, and jumping by 10s costs 19.

### Visualization (canvas `c1`, 720×300)

Single-panel drop-by-drop timeline of the jump-by-10s strategy on its unluckiest run: phone 1 jumps in tens and cracks at 100, then phone 2 climbs 91–99 one floor at a time.

- **Title (bold 15px, `#1a5276`, top center):** "Jump by 10s, Worst Run: 10 Jumps + 9 Climbs = 19 Drops".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = drop number 1 to 19, 12px `#444` tick labels "1", "3", "5", ..., "19" every 2; y = floor 0 to 100 with light `#e5e9ef` gridlines and 12px `#444` labels at 20, 40, 60, 80, 100.
- **Phone 1 (blue `#2a78d6`):** 2px line through 6px dots at drops `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, floors `[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]`; 12px blue label "phone 1: jump by 10" near drop 4, above the line.
- **Crack marker:** bold 14px red `#e74c3c` "✕" on the drop-10 dot (floor 100), bold 12px red label "cracks at 100" just right of it.
- **Phone 2 (orange `#d95926`):** 2px line through 6px dots at drops `[11, 12, 13, 14, 15, 16, 17, 18, 19]`, floors `[91, 92, 93, 94, 95, 96, 97, 98, 99]`; 12px orange label "phone 2: climb 91–99" below the line near drop 15.
- **Annotation (bold 13px orange `#d95926`, near x=drop 12, y=90):** two lines: "worst case: 19 drops —" / "can we do better?".
- **Caption (12px `#444`, bottom right):** "illustrative — the safe floor happens to be 99".

## Shrinking Jumps: 14, 27, 39, ...

**Tags:** `worked example` (blue), `balanced risk` (green)

- **The fix** — make each jump one floor smaller, so a later crack leaves a shorter climb to pay for
- **The stops** — phone 1 drops at floors 14, 27, 39, 50, 60, 69, 77, 84, 90, 95, 99, 100
- **Why 14 first** — the jumps 14 + 13 + 12 + ... + 1 = 105 floors, enough to cover all 100
- **Crack at 14** — 1 drop spent, then climb 1–13 with phone 2: 1 + 13 = 14 drops total
- **Crack at 27** — 2 drops spent, then climb 15–26: 2 + 12 = 14 drops total, the same bill
- **Every path** — each stop trades one more jump for one less climb, so no outcome exceeds 14

*Example (italic):* A phone that cracks on the 27-floor drop has cost 2 drops so far; the second phone climbs the 12 floors 15–26, landing the total at exactly 14.

**Key point:** Shrinking jumps balance the bill — jumps + remaining climb = 14 wherever the first crack lands, so the worst case falls from 19 to 14.

### Visualization (canvas `c2`, 720×300)

Stacked bar chart, one bar per first-phone stop: the blue base counts phone-1 jumps spent, the orange top counts the phone-2 climbs still owed if the crack happens there — the stacks all level off at 14.

- **Title (bold 15px, `#1a5276`, top center):** "Jumps Spent + Climbs Owed = 14, Wherever the Crack Lands".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = total drops 0 to 15 with light `#e5e9ef` gridlines and 12px `#444` labels at 5, 10, 15; x = the 12 stops with 12px `#444` labels `["14", "27", "39", "50", "60", "69", "77", "84", "90", "95", "99", "100"]` under the bars, 11px `#6b7280` caption "floor where phone 1 cracks" centered below them.
- **Bars:** 12 bars, ~34px wide, 14px gaps; blue `rgba(42,120,214,0.55)` base heights (phone-1 drops) `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]`; orange `rgba(217,89,38,0.55)` top heights (phone-2 climbs) `[13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 0]`; 1px matching solid borders.
- **Ceiling line:** horizontal dashed ink `#1a5276` (dash 4/3) line at y = 14 drops across the plot, bold 12px ink label "worst case: 14 drops" above its left end.
- **Legend (12px, top right inside plot):** blue swatch "jumps by phone 1", orange swatch "climbs by phone 2".
- **Annotation (bold 13px green `#008300`, centered at x=300, above the ceiling line, y=44/60):** two lines: "every stack tops out at 14 —" / "the risk is perfectly balanced".
- **Caption (12px `#444`, bottom right):** "last bar is 12: a crack at 100 means floor 99 was already proven safe".

## Where a Fixed Failure Budget Shows Up

**Tags:** `where it's used` (blue), `rule of thumb` (green), `budgeted search` (orange)

- **The formula** — k shrinking jumps cover k + (k−1) + ... + 1 = k(k+1)/2 floors with 2 phones
- **Solving it** — k(k+1)/2 ≥ 100 first holds at k = 14, since 14×15/2 = 105 while 13×14/2 = 91
- **Load testing** — finding the traffic level that crashes a server, when each crash costs a real outage
- **Dose escalation** — stepping a dose upward when overshooting is far more costly than a slow climb
- **Stress testing** — rating hardware with only a couple of sacrificial units to destroy
- **The lesson** — when failures are scarce, front-load big steps and shrink them as the budget drains

*Example (italic):* An engineer with two crash-test servers ramps traffic in shrinking steps for the same reason the phone starts at floor 14 — 14 probes cover 105 levels.

**Key point:** With a 2-failure budget, k probes cover k(k+1)/2 levels — read the answer off the formula: 100 floors need k = 14, because 105 ≥ 100 and 91 is not.

### Visualization (canvas `c3`, 720×300)

Single-panel coverage curve: floors covered as a function of the drop budget k, with the 100-floor requirement drawn as a dashed line and the first budget that clears it marked.

- **Title (bold 15px, `#1a5276`, top center):** "Floors Covered by k Drops: k(k+1)/2 — First Past 100 at k = 14".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = drop budget k, 1 to 16, 12px `#444` tick labels "2", "4", ..., "16" every 2; y = floors covered 0 to 140 with light `#e5e9ef` gridlines and 12px `#444` labels at 25, 50, 75, 100, 125.
- **Coverage curve:** blue `#2a78d6` 3px line through 5px dots at k = `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]`, floors = `[1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 66, 78, 91, 105, 120, 136]`.
- **Requirement line:** horizontal dashed `#6b7280` (dash 4/3) line at 100 floors across the plot, 12px `#6b7280` label "100 floors needed" above its left end.
- **Shortfall marker:** orange `#d95926` 7px dot at k=13 (91 floors), 12px orange label "k = 13 → 91: not enough" below-left of it.
- **Success marker:** green `#008300` 8px dot at k=14 (105 floors), vertical dashed green (dash 4/3) drop line to the baseline.
- **Annotation (bold 13px green `#008300`, near k=11, y=75):** two lines: "k = 14 covers 105 floors —" / "the smallest budget that works".

## Why Starting in the Middle Backfires

**Tags:** `common mistake` (red), `binary search` (orange)

- **The instinct** — every programmer wants to drop at floor 50 first: halving is how search works, right?
- **The trap** — if that first phone cracks at 50, the second must climb 1–49: up to 50 drops in all
- **The assumption** — binary search halves safely only when failures cost nothing; here one crack is fatal
- **The lineup** — worst cases: floor-by-floor 100, start at 50 then climb 50, jump by 10s 19, shrinking jumps 14
- **The tell** — a strategy's boldest early move is only as good as the cleanup it forces after a failure

*Example (italic):* Two candidates got the same puzzle; one halved to floor 50 and owned a worst case of 50 drops, the other started at 14 and owned 14.

**Common mistake:** Reaching for binary search whenever the word "search" appears. Halving is optimal for cheap failures; with two phones the crack-then-climb cleanup makes it cost 50, not 7.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart ranking four strategies by worst-case drops on the same 100-floor tower, with the shrinking-jumps bar in green as the clear winner.

- **Title (bold 15px, `#1a5276`, top center):** "Worst-Case Drops on 100 Floors, by Strategy".
- **Axis:** vertical label column x=20 to 230; bars start at x=230, full scale to x=680 (450px = 100 drops); horizontal 2px `#999` baseline at y=255 with 12px `#444` tick labels "0", "25", "50", "75", "100" and light `#e5e9ef` vertical gridlines.
- **Rows (bar rows at y = 75, 120, 165, 210, each 26px tall, left-aligned 12px `#444` label at x=20):**
  - "floor by floor (1 phone)": bar to 100, fill `rgba(107,114,128,0.45)`, bold 12px `#6b7280` value "100" at bar end
  - "start at 50, then climb": bar to 50, fill `rgba(213,81,129,0.50)`, bold 12px magenta `#d55181` value "50"
  - "jump by 10s": bar to 19, fill `rgba(217,89,38,0.55)`, bold 12px orange `#d95926` value "19"
  - "shrinking jumps from 14": bar to 14, fill `rgba(0,131,0,0.55)`, bold 12px green `#008300` value "14"
- **Annotation (bold 13px green `#008300`, left-aligned at x=340, y=195/211):** two lines: "shrinking jumps: 14 — under a third of halving's 50," / "and 5 fewer than jump-by-10s' 19 — better than both".
- **Caption (12px `#444`, bottom right):** "same tower, same two phones — only the strategy differs".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, dot positions, and curve points are the hardcoded arrays above (no randomness); c2 stack heights satisfy jumps + climbs = 14 for the first 11 stops and 12 for the last; c3 values are exact triangular numbers k(k+1)/2; the "safe floor is 99" run in c1 is labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
