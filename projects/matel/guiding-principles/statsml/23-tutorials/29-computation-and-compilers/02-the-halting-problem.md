# The Halting Problem

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Halting Problem

**Subtitle:** No program can look at any other program and always predict whether it will finish or run forever — and a seven-line prankster program is the whole proof

## The Checker That Would Save the Build Server

**Tags:** `core idea` (blue), `dream tool` (green), `infinite loops` (orange)

- **The stuck build** — last week one bad script looped forever and jammed a team's shared build server
- **The dream tool** — a checker `will_it_finish(program, input)` answering YES or NO before running anything
- **Not a timeout** — a timeout only says "still running after 60s"; it never says "this would run forever"
- **Easy by eye** — a human reads `while x > 0: x = x - 1` and sees it stops; the dream is doing that for any code
- **The catch** — Alan Turing proved in 1936 that no such checker can exist, on any computer, ever

*Example (italic):* A script printing the digits of pi never stops, but from the outside it looks exactly like a slow script that is about to finish — the dream checker would tell them apart without running either.

**Key point:** The halting problem asks for one program that decides, for every program and input, "will this ever stop?" — and that one program cannot exist.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three small program cards on the left feed into one central checker box, which routes each to a YES / NO / ? verdict on the right — the dream machine sketched before it is destroyed.

- **Title (bold 15px, `#1a5276`, top center):** "The Dream Tool: will_it_finish(program, input)".
- **Program cards (left column, x=30, width 200, height 46, rounded 6px, fill `#f8f9fa`, 1px `#e5e9ef` border), tops at y = 62, 128, 194; 12px monospace `#2c3e50` label centered in each:** `countdown from 10`, `print pi forever`, `hunt odd perfect number`.
- **Checker box:** rounded rect at x=290, y=105, width 170, height 90, fill `rgba(42,120,214,0.12)`, 2px blue `#2a78d6` border; bold 13px blue label in two lines centered: "will_it_finish" / "( program, input )".
- **Arrows in:** 2px `#6b7280` lines with small arrowheads from each card's right edge to the checker box's left edge.
- **Arrows out:** three 2px lines with arrowheads from the checker's right edge fanning to verdict labels at x=520, y = 85, 150, 215 — bold 13px green `#008300` "YES — finishes", bold 13px orange `#d95926` "NO — loops forever", bold 13px mute `#6b7280` "? — nobody knows today".
- **Annotation (bold 13px violet `#4a3aa7`, near x=490, y=262):** "one box, any program — that is the dream".
- **Caption (12px `#444`, bottom left):** "illustrative — the tool this page proves impossible".

## Ten Lines That Break the Checker

**Tags:** `worked example` (blue), `self-reference` (green), `contradiction` (red)

- **Assume it exists** — suppose someone actually ships a working `will_it_finish(program, input)` checker
- **The prankster** — write `contrary(p)`: ask the checker about `p` run on itself, then do the exact opposite
- **Seven lines** — contrary is genuinely tiny: one call to the checker, one endless loop, one plain stop
- **The trap** — now run `contrary(contrary)` and ask the checker about that very run
- **Case YES** — checker predicts "finishes", so contrary deliberately loops forever: the prediction fails
- **Case NO** — checker predicts "runs forever", so contrary stops immediately: wrong again

*Example (italic):* It is the barber who shaves exactly the people who don't shave themselves — asking "who shaves the barber?" breaks both possible answers the same way.

**Key point:** Both answers the checker could give about contrary(contrary) come out false, so the checker we assumed can't exist — not unbuilt yet, but impossible.

### Visualization (canvas `c2`, 720×300)

Two-panel layout: left panel shows the seven-line prankster as a code card; right panel is a two-branch decision tree for contrary(contrary) where both branches end in a red contradiction.

- **Title (bold 15px, `#1a5276`, top center):** "contrary(contrary): Both Answers Come Out Wrong".
- **Code card (left):** rounded rect x=25, y=52, width 280, height 210, fill `#f8f9fa`, 1px `#e5e9ef` border; 12px monospace `#2c3e50` lines, 20px line spacing, starting x=40, y=78: `def contrary(p):`, `  answer = will_it_finish(p, p)`, `  if answer == "YES":`, `      while True:`, `          pass        # loop`, `  else:`, `      return          # stop`; bold 12px ink `#1a5276` header above the box at y=46: "the seven-line prankster".
- **Root node (right panel):** rounded rect centered at x=515, y=70, width 300, height 40, fill `rgba(42,120,214,0.12)`, 2px blue `#2a78d6` border, bold 12px blue label: "checker's verdict on contrary(contrary)?".
- **Branches:** 2px `#6b7280` arrowed lines from the root's bottom to two outcome boxes — left box centered x=430, y=170 and right box centered x=610, y=170, each width 150, height 44, fill white, 1px `#e5e9ef` border; bold 12px `#2c3e50` branch labels "YES" (x=430, y=118) and "NO" (x=610, y=118); box texts 12px `#2c3e50`, two lines: "contrary loops" / "forever" and "contrary stops" / "at once".
- **Contradiction marks:** below each outcome box, bold 13px red `#e74c3c` label at y=232: "prediction wrong" (x=430) and "prediction wrong" (x=610), each with a 2px red X mark (12×12 px) drawn beside it.
- **Annotation (bold 13px red `#e74c3c`, centered at x=520, y=272):** "both branches contradict the checker — it cannot exist".

## Why Your Tools Ship With Timeouts

**Tags:** `where it's used` (blue), `timeouts` (orange), `approximation` (green)

- **Timeouts everywhere** — build servers, databases, and CI kill jobs by the clock because no oracle exists
- **Linters hedge** — static analyzers say "possible infinite loop"; certainty for all code is off the table
- **Scanners too** — "will this file ever do X?" is the halting problem in disguise, so antivirus approximates
- **Open math** — "loop until an odd perfect number appears" halts only if one exists; a checker would settle it
- **One month's toll** — a 60s timeout killed 12 of the team's 200 scripts; reruns showed 5 were slow but finite

*Example (italic):* A reviewer flags `while balance != target:` as risky, and no tool on earth can promise it terminates for every input — the reviewer's unease is a 1936 theorem.

**Key point:** Every "will this program ever...?" question inherits the same impossibility, so practical tools trade certainty for timeouts, budgets, and warnings.

### Visualization (canvas `c3`, 720×300)

Single horizontal segmented bar: 200 build-server scripts under a 60-second timeout, split into finished in time, slow-but-finite killed, and truly infinite killed — the timeout cannot tell the last two apart.

- **Title (bold 15px, `#1a5276`, top center):** "200 Scripts, One 60-Second Timeout (one month, illustrative)".
- **Bar geometry:** one horizontal bar at y=120, height 56, starting x=60, total width 600 (3px per script); rounded 4px ends.
- **Segments (left to right, hardcoded):** green `rgba(0,131,0,0.55)` width 564 = 188 scripts "finished in time"; orange `rgba(217,89,38,0.65)` width 15 = 5 scripts "slow but finite — killed anyway"; red `rgba(231,76,60,0.65)` width 21 = 7 scripts "truly infinite — killed".
- **Segment labels:** bold 13px green `#008300` "188 finished" centered inside the green segment; the orange and red segments get 2px leader lines down to 12px labels below the bar at y=215 and y=240 — orange `#d95926` "5 slow but finite (killed)" and red `#e74c3c` "7 truly infinite (killed)", left-aligned at x=430.
- **Bracket:** thin 2px `#6b7280` bracket over the orange+red segments (x=624 to x=660) at y=100 with 12px `#6b7280` label above: "12 killed by the clock".
- **Annotation (bold 13px violet `#4a3aa7`, x=60, y=272):** "the timeout cannot tell orange from red — that gap is the halting problem".
- **Caption (12px `#444`, bottom right):** "illustrative counts — reruns with a bigger budget exposed the 5".

## Impossible, Not Just Hard

**Tags:** `common mistake` (red), `undecidable` (orange)

- **Not a speed issue** — the proof assumes nothing about hardware; a computer a trillion times faster changes nothing
- **Not "never know"** — single programs are often easy calls: `for i in 1..10` halts, `while True` doesn't
- **The real claim** — no ONE checker handles every program; for each tool, some program fools it
- **AI included** — any future AI is itself a program, so its own contrary breaks it exactly the same way
- **Practical peace** — most everyday code is easy to settle; the impossibility bites only at "all programs"

*Example (italic):* A verifier proving 99.9% of real-world loops terminate is perfectly consistent with the theorem — it only guarantees some program escapes every such tool.

**Common mistake:** Hearing "undecidable" as "we can't tell for any program". It means no single method works for all programs — individual cases are decided every day.

### Visualization (canvas `c4`, 720×300)

Two-panel contrast: left panel lists four single programs with confident verdict pills; right panel shows the all-programs checker box crossed out — case-by-case is fine, universal is impossible.

- **Title (bold 15px, `#1a5276`, top center):** "Case by Case: Easy. One Checker for All: Impossible.".
- **Left panel header (bold 13px `#1a5276`, x=40, y=58):** "single programs — often easy".
- **Left rows (x=40, tops at y = 76, 122, 168, 214; each a rounded rect width 260, height 36, fill `#f8f9fa`, 1px `#e5e9ef` border):** 12px monospace `#2c3e50` program text at left inside; verdict pill (rounded 9px, bold 11px white text, 52×18 px) at the row's right edge — `for i in 1..10` / green `#008300` pill "halts"; `while True: pass` / red `#e74c3c` pill "loops"; `x=16; halve while even` / green pill "halts"; `print digits of pi` / red pill "loops".
- **Right panel:** rounded rect centered at x=530, y=140, width 240, height 100, fill `rgba(42,120,214,0.12)`, 2px blue `#2a78d6` border, bold 13px blue two-line label: "the all-programs" / "checker"; two 4px red `#e74c3c` diagonal lines crossing the whole box corner to corner.
- **Below the box (bold 13px red `#e74c3c`, centered at x=530, y=222):** "impossible — Turing, 1936".
- **Annotation (bold 12px mute `#6b7280`, centered at x=530, y=268):** two lines: "every tool has a blind spot:" / "its own contrary program".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red is used here for genuine error states only (contradictions, infinite loops, the crossed-out checker).
- **Data:** all boxes, arrows, segment widths, and counts are the hardcoded values above (no randomness); the 188 / 5 / 7 split of 200 scripts is invented and labeled illustrative, drawn at exactly 3px per script; code text is drawn as monospace canvas text, no `<pre>` blocks.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
