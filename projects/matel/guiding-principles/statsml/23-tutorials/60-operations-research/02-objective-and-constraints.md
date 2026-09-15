# Objective & Constraints

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Objective & Constraints

**Subtitle:** Every decision problem can be written as one sentence — the thing you want, subject to the rules you can't break

## The Cafeteria's One Sentence

**Tags:** `core idea` (blue), `diet problem` (green)

- **The scene** — a school cafeteria plans lunch: the cheapest menu that still feeds every student
- **The objective** — the thing you minimize or maximize: here, total cost of the menu
- **The constraints** — the lines you cannot cross: at least 20g protein and 600 calories each
- **The sentence** — "minimize cost, subject to protein ≥ 20g, calories ≥ 600, servings ≥ 0"
- **Why one sentence** — written this way, anyone (or any solver) knows exactly what "best" means

*Example (italic):* "Make lunch cheap but keep the kids fed" becomes precise the moment you split it into one objective and three rules.

**Key point:** Every decision problem compresses into one sentence: the objective you optimize, subject to the constraints you can't break.

### Visualization (canvas `c1`, 720×300)

The sentence rendered as a labeled diagram: one objective box on top, "subject to" connector, three constraint boxes below.

- **Title (bold 15px, `#1a5276`, top center):** "The One-Sentence Problem Statement".
- **Objective box:** x=210 y=54 w=300 h=52, fill `#fbfcfd`, 2px blue `#2a78d6` border; line 1 bold 13px blue centered at y+22: "MINIMIZE"; line 2 12px `#2c3e50` at y+42: "total cost of the menu".
- **Connector:** bold 13px violet `#4a3aa7` text "subject to" centered at (360, 136); 1.5px mute `#6b7280` vertical line from (360, 106) to (360, 146), then three 1.5px mute lines fanning from (360, 146) to the top-center of each constraint box, filled mute arrowheads pointing down at each box top.
- **Constraint boxes (y=176, h=52, w=200; x = 34 / 260 / 486):** fill `#fbfcfd`, 2px green `#008300` border; bold 13px green header centered at y+22, 12px `#2c3e50` sub-line at y+42:
  - "PROTEIN ≥ 20g" / "per student, every day"
  - "CALORIES ≥ 600" / "per student, every day"
  - "SERVINGS ≥ 0" / "can't serve negative food"
- **Caption (bold 13px magenta `#d55181`, centered at y=262):** "one thing to optimize, three lines that cannot be crossed".
- **Sub-caption (12px `#6b7280`, centered at y=284):** "objective = what \"best\" means · constraints = what \"allowed\" means".

## Trying Menus by Hand

**Tags:** `worked example` (blue), `feasible vs infeasible` (orange)

- **Two foods** — rice: $0.40, 4g protein, 200 cal per serving; beans: $0.60, 8g protein, 150 cal
- **Menu (a)** — 3 rice: $1.20, 12g protein, 600 cal — cheap, but breaks the protein rule
- **Menu (b)** — 2 rice + 2 beans: $2.00, 24g protein, 700 cal — every rule passes
- **Menu (c)** — 1 rice + 2 beans: $1.60, 20g protein, 500 cal — breaks the calorie rule
- **The word** — a plan that obeys every rule is feasible; a rule-breaker is infeasible

*Example (italic):* Check menu (b) yourself: 2×4 + 2×8 = 24g protein and 2×200 + 2×150 = 700 calories — both rules pass.

**Key point:** Feasibility is a yes/no check anyone can do by hand — add up each rule for the menu and see whether any line gets crossed.

### Visualization (canvas `c2`, 720×300)

A check-grid: three candidate menus as rows, protein and calorie rules as columns with pass/fail marks, cost at the right; the feasible row highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Three Menus Against the Rules (illustrative)".
- **Column headers (bold 12px `#2c3e50`, centered at y=64):** "menu" at x=150, "protein ≥ 20g" at x=340, "calories ≥ 600" at x=478, "cost" at x=590, "verdict" at x=666.
- **Rows (y = 78, 134, 190; height 48; row band x=36 to x=700):** menu (b)'s band filled `rgba(0,131,0,0.07)` with a 2px green `#008300` border; menus (a) and (c) bands filled `#fbfcfd` with 1px `#b8c4cf` border.
- **Row content (centered per column, y = band y + 29):**
  - (a) "3 rice" 12px `#2c3e50` · "12g ✗" bold 13px red `#e74c3c` · "600 ✓" bold 13px green · "$1.20" bold 13px ink `#1a5276` · "infeasible" bold 12px red
  - (b) "2 rice + 2 beans" · "24g ✓" bold 13px green · "700 ✓" bold 13px green · "$2.00" bold 13px ink · "feasible" bold 12px green
  - (c) "1 rice + 2 beans" · "20g ✓" bold 13px green · "500 ✗" bold 13px red · "$1.60" bold 13px ink · "infeasible" bold 12px red
- **Callout (bold 13px green `#008300`, centered at y=262):** "menu (b) crosses no line — it is feasible; one broken rule sinks the others".
- **Caption (12px `#6b7280`, centered at y=284):** "rice: $0.40, 4g protein, 200 cal per serving · beans: $0.60, 8g protein, 150 cal".

## Feasible Is Not Optimal

**Tags:** `core idea` (blue), `optimization` (green)

- **Menu (b) is legal** — $2.00 obeys every rule; but is it the cheapest legal menu?
- **A better one exists** — 3 rice + 1 beans: $1.80, 20g protein, 750 cal — legal and 20¢ cheaper
- **The search** — a solver checks all legal menus and returns the cheapest, not the first found
- **The gap** — "obeys the rules" and "best among rule-obeyers" are different claims
- **The word** — the cheapest feasible plan is called optimal; closing that gap is optimization

*Example (italic):* Verify the winner yourself: 3×4 + 8 = 20g protein and 3×200 + 150 = 750 calories, at 3×$0.40 + $0.60 = $1.80.

**Key point:** Feasible means legal; optimal means best among the legal — a solver's whole job is the gap between those two words.

### Visualization (canvas `c3`, 720×300)

A cost number-line scatter of many candidate menus: infeasible ones greyed on the upper band, feasible ones colored on the lower band, the cheapest feasible one flagged.

- **Title (bold 15px, `#1a5276`, top center):** "Every Candidate Menu by Cost (illustrative)".
- **Axis:** 1px `#999` horizontal line at y=210 from x=60 to x=690; ticks + 12px `#6b7280` labels at $1.00 / $1.50 / $2.00 / $2.50 mapped linearly (x = 60 + (cost − 0.90) / 1.80 × 630); axis caption "menu cost →" 12px mute at (690, 232) right-aligned.
- **Infeasible band (y=110):** grey `#b0b8c1` 7px dots with 11px grey labels above (label y=92): "3 rice $1.20", "2r+1b $1.40", "4 rice $1.60", "1r+2b $1.60" (offset below at y=126 to avoid overlap), "3 beans $1.80"; band label "infeasible — breaks a rule" 12px `#6b7280` left-aligned at (60, 74).
- **Feasible band (y=170):** aqua `#199e70` 7px dots with 11px `#2c3e50` labels below (label y=192): "5 rice $2.00", "2r+2b $2.00" (offset above at y=152), "1r+3b $2.20", "2r+3b $2.60"; band label "feasible — obeys every rule" 12px aqua left-aligned at (60, 148) — place at (60, 246) if crowded; final layout: band label at (60, 246).
- **The winner:** "3r+1b $1.80" as a 9px green `#008300` dot at (cost 1.80, y=170) with a bold 13px green annotation above-left at y=150: "cheapest feasible: 3 rice + 1 beans = $1.80" and a 1.5px green ring (radius 12) around the dot.
- **Insight (bold 13px magenta `#d55181`, centered at y=278):** "feasible menus exist at $2.00 — optimal is the $1.80 one nobody guessed".

## When Goals and Rules Swap Places

**Tags:** `common mistake` (red), `framing` (orange)

- **One word flips it** — "keep cost low" is an objective; "cost must stay under $2.00" is a constraint
- **Same words, new menu** — cost-as-objective picks 3 rice + 1 beans ($1.80, 20g protein)
- **The swap** — maximize protein with cost ≤ $2.00 picks 2 rice + 2 beans ($2.00, 24g protein)
- **Not math's call** — whether cost is the goal or a cap is a business conversation, not an equation
- **The skill** — writing the sentence transfers to every solver; the solver itself is replaceable

*Example (italic):* Same cafeteria, same two foods — move cost from objective to constraint and lunch itself changes.

**Common mistake:** Assuming the problem statement is fixed. Learning to state a problem as objective + constraints is the transferable skill — the solver is replaceable, the sentence is not.

### Visualization (canvas `c4`, 720×300)

Two side-by-side sentence cards showing the swap: cost as objective on the left, cost as constraint on the right, each with its winning menu.

- **Title (bold 15px, `#1a5276`, top center):** "Move One Word, Change the Lunch (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=44 to y=250.
- **Left card (x=44, y=56, w=280, h=110):** fill `#fbfcfd`, 2px blue `#2a78d6` border; header bold 13px blue centered at y+24: "cost is the OBJECTIVE"; lines 12px `#2c3e50` centered at y+50 / y+70 / y+92: "minimize cost", "subject to protein ≥ 20g,", "calories ≥ 600".
- **Left result:** bold 13px blue centered at (184, 196): "→ 3 rice + 1 beans"; 12px `#2c3e50` at (184, 216): "$1.80 · 20g protein · 750 cal".
- **Right card (x=396, y=56, w=280, h=110):** fill `#fbfcfd`, 2px orange `#d95926` border; header bold 13px orange centered at y+24: "cost is a CONSTRAINT"; lines 12px `#2c3e50` at y+50 / y+70 / y+92: "maximize protein", "subject to cost ≤ $2.00,", "calories ≥ 600".
- **Right result:** bold 13px orange centered at (536, 196): "→ 2 rice + 2 beans"; 12px `#2c3e50` at (536, 216): "$2.00 · 24g protein · 700 cal".
- **Insight (bold 13px magenta `#d55181`, centered at y=278):** "which word cost lives under is a business decision — and it changes the menu".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) (and the CSS-width ratio) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for broken-rule marks and "infeasible" verdicts. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity (all hardcoded):** rice = $0.40 / 4g protein / 200 cal per serving; beans = $0.60 / 8g protein / 150 cal. Menus: (a) 3r = $1.20/12g/600cal infeasible; (b) 2r+2b = $2.00/24g/700cal feasible; (c) 1r+2b = $1.60/20g/500cal infeasible; optimum 3r+1b = $1.80/20g/750cal. Scatter extras: 2r+1b $1.40 (16g ✗), 4r $1.60 (16g ✗), 3b $1.80 (450 cal ✗), 5r $2.00 feasible, 1r+3b $2.20 feasible, 2r+3b $2.60 feasible. Protein-max swap winner: 2r+2b at 24g. Invented numbers carry "(illustrative)".
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
