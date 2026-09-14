# Static vs Dynamic Typing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Static vs Dynamic Typing

**Subtitle:** Static typing checks that numbers are numbers before the program ever runs; dynamic typing waits until each line actually executes — the bug is the same either way, only the moment it surfaces changes

## One Price Written as Text

**Tags:** `core idea` (blue), `when errors surface` (green), `type check` (orange)

- **The script** — the day's 5 receipts are keyed into the script as a list and added into one total
- **The typo** — receipt 3 was keyed in as the text "2.25" (in quotes), not the number 2.25
- **Static typing** — a checker reads the code before it runs and refuses: "can't add text to a number"
- **Dynamic typing** — the program starts anyway; each value's type is checked only when its line runs
- **Same bug** — nothing about the mistake differs; what differs is when it surfaces: before the run, or mid-run

*Example (italic):* The static version never misbehaves in front of the owner — it refuses to start; the dynamic version runs fine right up to receipt 3.

**Key point:** Static vs dynamic typing doesn't change what a type error is — it changes when it surfaces: before the run starts, or at the moment the bad line executes.

### Visualization (canvas `c1`, 720×300)

Two-lane timeline: the same buggy script travels left to right through the stages "write → check → receipt 1 … receipt 5"; the static lane dies at the check stage, the dynamic lane dies at receipt 3.

- **Title (bold 15px, `#1a5276`, top center):** "Same Bug, Two Discovery Times".
- **Stage axis:** 7 stage columns at x = `[90, 185, 280, 375, 470, 565, 660]`, labeled below y=262 in 12px `#444`: "write code", "type check", "receipt 1", "receipt 2", "receipt 3", "receipt 4", "receipt 5".
- **Lane labels (12px `#444`, left at x=12):** "STATIC" at y=110, "DYNAMIC" at y=190; each lane is a 2px `#e5e9ef` horizontal line from x=80 to x=670.
- **Static lane (y=110):** green `#008300` 8px dot at "write code" (x=90); red `#e74c3c` bold X (14px stroke marks, 3px lines) at "type check" (x=185); the remaining five stages get hollow 8px circles (2px `#6b7280` stroke, no fill); bold 12px red label above the X: "error surfaces here — nothing ran".
- **Dynamic lane (y=190):** green 8px dots at "write code", "type check" (skipped through), "receipt 1", "receipt 2" (x=90, 185, 280, 375); red bold X at "receipt 3" (x=470); hollow gray circles at receipts 4 and 5 (x=565, 660) with 11px `#6b7280` label below "never reached".
- **Annotation (bold 13px violet `#4a3aa7`, near x=470, y=55, two lines):** "the dynamic run looks healthy" / "until the bad line executes".
- **Caption (12px `#444`, bottom right):** "illustrative — one script, run under two typing disciplines".

## Adding the Receipts by Hand

**Tags:** `worked example` (blue), `runtime crash` (green)

- **Five receipts** — 3.00, 4.50, then the text "2.25", then 5.75 and 1.50; the true total is 17.00
- **Static run** — never starts: the checker stops at the add-price line before receipt 1 is even read
- **Dynamic run** — the total climbs 3.00, then 7.50, then the program dies adding text at receipt 3
- **Never reached** — receipts 4 and 5 are never read; the day ends with a crash instead of a total
- **Sneakier still** — a loose language may glue the text on instead: 7.50 + "2.25" becomes "7.52.25"

*Example (italic):* Redo it by hand: 3.00, then 3.00 + 4.50 = 7.50, and at receipt 3 you must add the word "2.25" to 7.50 — that halt is the runtime type error.

**Key point:** The dynamic run's total goes 3.00 → 7.50 → crash at receipt 3; receipts 4 and 5 (5.75 and 1.50) are never counted, and the true 17.00 never appears.

### Visualization (canvas `c2`, 720×300)

Single-panel bar chart of the dynamic run: running total after each receipt, with real bars up to the crash, a red X at receipt 3, and dashed ghost bars for the totals that never happened.

- **Title (bold 15px, `#1a5276`, top center):** "Dynamic Run: the Total Climbs, Then Dies at Receipt 3".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y = running total 0 to 20 with 12px `#444` tick labels "0", "5", "10", "15", "20" and light `#e5e9ef` gridlines; x = five bar slots centered at x = `[135, 253, 371, 489, 607]`, labeled below in 12px `#444`: "receipt 1 (3.00)", "receipt 2 (4.50)", "receipt 3 (\"2.25\")", "receipt 4 (5.75)", "receipt 5 (1.50)".
- **Real bars:** width 70px, blue `#2a78d6` fill, heights for running totals `[3.00, 7.50]` at slots 1–2; bold 12px `#1a5276` value labels "3.00" and "7.50" above each bar.
- **Crash marker:** at slot 3, no bar — a red `#e74c3c` bold X (3px strokes, 22px across) at bar-top height for 9.75, with bold 13px red label above: "TypeError: can't add text".
- **Ghost bars:** slots 4 and 5 drawn as dashed (dash 5/4) 2px `#6b7280` outlines, no fill, at heights for `[15.50, 17.00]` (the would-have-been totals including receipt 3's 2.25); 11px `#6b7280` labels "15.50" and "17.00" above, and one 11px `#6b7280` label under the pair: "never reached".
- **Target line:** horizontal dashed green `#008300` (dash 4/3) line at total 17.00 across the plot, 12px green label at its left end: "true total 17.00".
- **Annotation (bold 12px orange `#d95926`, near x=420, y=70, two lines):** "5.75 and 1.50 never counted —" / "the day ends without a total".
- **Caption (12px `#444`, bottom right):** "illustrative receipts; totals computed from the five listed prices".

## The Bug That Sleeps for 29 Days

**Tags:** `where it's used` (blue), `rare code paths` (green), `trade-offs` (orange)

- **Runs-only rule** — dynamic checking tests only the lines that actually execute on that particular run
- **The rare branch** — a month-end summary branch runs once every 30 days; a type bug there sleeps 29 days
- **Static sweep** — a static checker reads every line up front, including branches that won't run for weeks
- **Cost of late** — a day-0 check failure costs minutes; a day-30 crash costs the month-end report night
- **The trade** — dynamic code starts faster with less ceremony; static code buys early warning with declarations

*Example (italic):* The owner adds a month-end branch carrying the same text-price bug; the dynamic script works fine for 29 straight days and crashes on day 30.

**Key point:** Dynamic typing only checks the lines that run — a bug in a once-a-month branch surfaces on day 30, while a static checker flags it on day 0 before any run.

### Visualization (canvas `c3`, 720×300)

Horizontal 30-day timeline: green ticks for 29 clean daily runs, a red X on day 30 when the month-end branch finally executes, and a blue day-0 marker where the static checker would have caught it.

- **Title (bold 15px, `#1a5276`, top center):** "One Bug, 29 Quiet Days: Dynamic Finds It on Day 30, Static on Day 0".
- **Axis:** horizontal 2px `#999` line at y=185 from x=60 to x=660 (width 600) = days 0 to 30; 12px `#444` tick labels below at days 0, 5, 10, 15, 20, 25, 30 (x = 60, 160, 260, 360, 460, 560, 660).
- **Daily runs:** 29 short green `#008300` 2px vertical ticks (12px tall, centered on the axis) at days 1–29 (x = 80, 100, ..., 640, step 20); 12px green label above the middle of the run (near x=300, y=150): "daily total runs clean, days 1–29".
- **Day-30 crash:** red `#e74c3c` bold X (3px strokes, 20px across) on the axis at x=660; bold 13px red label above (right-aligned near x=655, y=120, two lines): "month-end branch runs —" / "crashes on day 30".
- **Day-0 static marker:** vertical dashed blue `#2a78d6` (dash 4/3) line at x=60 from y=90 to the axis, blue 8px dot at its base, bold 13px blue label at its top: "static checker flags it on day 0".
- **Sleep band:** light `rgba(230,126,34,0.12)` rectangle from day 1 to day 29 between y=170 and y=200, 11px `#d95926` label centered below it (y=215): "the bug sleeps here — that code never executed".
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=360, y=255):** "dynamic checking can only catch what actually runs".
- **Caption (12px `#444`, bottom right):** "illustrative — one month-end branch, one hidden type bug".

## Dynamic Doesn't Mean Untyped

**Tags:** `common mistake` (red), `strong vs weak` (orange)

- **The mix-up** — "dynamic" is often heard as "no types"; dynamic values carry types, checked as lines run
- **Proof** — a dynamic-but-strict language stops 7.50 + "2.25" with a clear type error at that exact line
- **Coercion** — a dynamic-and-loose language glues them into the text "7.52.25" and keeps going silently
- **Two dials** — when types are checked (before vs during the run) and how strictly (stop vs convert)
- **Worst corner** — silent coercion beats no crash but loses the day: the wrong total can land in a report

*Example (italic):* Python halts at receipt 3 with "unsupported operand"; JavaScript turns the running total into "7.52.25" and keeps going — both dynamic, very different strictness.

**Common mistake:** Treating "dynamic" and "weakly typed" as the same dial. When checking happens and how strict it is are independent axes — the silent "7.52.25" comes from looseness, not from dynamism.

### Visualization (canvas `c4`, 720×300)

Two-by-two quadrant chart: horizontal axis = when types are checked (before the run vs during the run), vertical axis = strictness (stops vs converts), with one well-known language dot in each quadrant and the receipt bug's outcome noted per side.

- **Title (bold 15px, `#1a5276`, top center):** "Two Independent Dials: When It's Checked × How Strict It Is".
- **Quadrant frame:** rectangle from x=110 to x=650, y=55 to y=255; mid vertical line at x=380 and mid horizontal line at y=155, both 1.5px `#e5e9ef`; outer border 2px `#999`.
- **Axis labels:** 12px bold `#444` — "checked BEFORE the run (static)" centered below-left at x=245, y=278; "checked DURING the run (dynamic)" centered below-right at x=515, y=278; rotated or stacked left-side labels 12px bold `#444`: "STOPS on mismatch (strict)" beside the top half (x=14, y=105), "CONVERTS silently (loose)" beside the bottom half (x=14, y=205).
- **Language dots (9px, bold 13px labels beside each):** blue `#2a78d6` "Java" at (245, 105); green `#008300` "Python" at (515, 105); orange `#d95926` "C" at (245, 205); magenta `#d55181` "JavaScript" at (515, 205).
- **Outcome notes (11px `#6b7280`, under each right-side dot):** under Python: "7.50 + \"2.25\" → type error at that line"; under JavaScript: "7.50 + \"2.25\" → \"7.52.25\", no error".
- **Annotation (bold 13px green `#008300`, near x=515, y=70):** "dynamic AND strict — dynamic ≠ loose".
- **Caption (12px `#444`, bottom right):** "quadrant placements are the usual textbook characterizations".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red reserved for the genuine error states (the crash X marks and error labels).
- **Data:** all positions and values are the hardcoded literals above (no randomness); the receipt prices `[3.00, 4.50, "2.25", 5.75, 1.50]`, running totals `[3.00, 7.50]`, ghost totals `[15.50, 17.00]`, and the day-1..29 tick positions must match between text and charts exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
