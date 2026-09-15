# Garbage In, Optimal Garbage Out

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Garbage In, Optimal Garbage Out

**Subtitle:** An optimizer squeezes every drop out of the forecast it's given — which puts the plan exactly where a forecast error hurts most

## Optimal Means Zero Slack

**Tags:** `core idea` (blue), `optimization` (green)

- **The job** — a call center asks the optimizer for the cheapest schedule that covers its call forecast
- **The squeeze** — any plan with spare agents costs more, so the cheapest plan keeps none
- **On the edge** — the cheapest legal plan sits exactly against the constraint: capacity = demand
- **By design** — zero buffer is not a bug; it is precisely what "optimal" means
- **The price** — a plan with no slack has no room left to absorb even a small surprise

*Example (italic):* Ask an optimizer for the cheapest schedule that covers 300 calls an hour and it will never pay for one agent more than that.

**Key point:** "Optimal" means the plan sits flush against its constraints — the optimizer trades away every drop of slack, which is exactly why optimized systems feel brittle.

### Visualization (canvas `c1`, 720×300)

A feasible-region sketch for a two-shift staffing problem: shaded region of plans with enough capacity, the demand constraint as its edge, and the chosen plan dot sitting exactly ON that edge.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Optimizer Puts the Plan".
- **Axes:** 1px `#999` — vertical line at x=80 from y=52 to y=248; horizontal baseline at y=248 from x=80 to x=660. Axis labels 12px `#6b7280`: "morning agents →" centered at (370, 268); "evening agents ↑" left-aligned at (16, 46).
- **Constraint line:** 2px ink `#1a5276` from (100, 236) to (640, 78) — the "capacity = demand" edge.
- **Feasible region:** polygon (100,236) → (100,56) → (640,56) → (640,78) closed along the constraint line back to (100,236); fill `rgba(42,120,214,0.10)`. Label 12px `#2c3e50` centered at (330, 80): "feasible: enough agents to cover demand".
- **Infeasible label:** 12px `#6b7280` centered at (240, 215): "infeasible: calls go unanswered".
- **Cost direction:** mute 1.5px arrow from (510, 100) to (445, 140) with a filled arrowhead at the tip; label 12px `#6b7280` left-aligned at (518, 98): "cheaper plans lie this way".
- **Optimal dot:** filled circle r=7 at (400, 148) — a point exactly on the constraint line — fill orange `#d95926`, 2px ink `#1a5276` stroke.
- **Annotation (bold 13px orange `#d95926`, centered at (400, 182)):** "the optimal plan: ON the edge, zero slack".
- **Caption (12px `#6b7280`, centered at y=292):** "(illustrative two-shift staffing problem)".

## When 300 Calls Become 330

**Tags:** `worked example` (blue), `forecast error` (orange)

- **The plan** — forecast 300 calls/hour, 20 per agent: staff exactly 300 / 20 = 15 agents
- **The surprise** — 330 calls actually arrive, only 10% above the forecast
- **The math** — 330 calls need 330 / 20 = 16.5 agents of work, but only 15 are on the floor
- **The queue** — capacity is 15 × 20 = 300, so 30 unanswered calls pile up every single hour
- **The buffer** — 17 agents give capacity 17 × 20 = 340, and the same 330 calls are absorbed

*Example (italic):* A 10% input error became a blown-up output: after four hours the exact-fit plan has 120 callers stuck on hold, and the queue is still growing.

**Key point:** Optimization amplifies input error — because the plan holds zero slack, a small forecast miss doesn't degrade the outcome gently, it breaks it.

### Visualization (canvas `c2`, 720×300)

Two side-by-side panels split by a light divider at x=360: the queue over the day under the exact-fit 15-agent plan (left, exploding) vs the 17-agent buffered plan (right, flat).

- **Title (bold 15px, `#1a5276`, top center):** "Same 330 Calls per Hour, Two Plans (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=40 to y=258.
- **Left panel:** header bold 13px `#2c3e50` centered at (195, 46): "planned to the point: 15 agents".
  - Axes 1px `#999`: y-axis at x=64 from y=74 to y=222; baseline at y=222 from x=64 to x=336. Y-axis label 12px `#6b7280` left-aligned at (18, 66): "callers waiting".
  - Data (hardcoded): hours 0–4 → backlog `[0, 30, 60, 90, 120]` callers waiting. Points at x = 64 + hour·66; y = 222 − v/130·148.
  - Line 2.5px red `#e74c3c` (genuine failure) with r=3.5 filled dots; hour tick labels "0".."4" 12px `#6b7280` under the baseline at y=238.
  - End label bold 12px red right-aligned at (322, 78): "120 waiting".
  - Annotation (bold 12px red `#e74c3c`, centered at (195, 262)): "queue grows +30 every hour — waits explode".
- **Right panel:** header bold 13px `#2c3e50` centered at (532, 46): "planned with buffer: 17 agents".
  - Same plot geometry shifted right: y-axis at x=408, baseline from x=408 to x=680; hour ticks "0".."4" at x = 408 + hour·66, y=238.
  - Data (hardcoded): backlog `[0, 0, 0, 0, 0]` — flat green `#008300` 2.5px line drawn at y=220 with r=3.5 dots.
  - Label bold 12px green centered at (544, 200): "capacity 17 × 20 = 340 ≥ 330".
  - Annotation (bold 12px green `#008300`, centered at (544, 262)): "the same 330 calls are absorbed".
- **Caption (12px `#6b7280`, centered at (360, 288)):** "(arrivals steady at 330 calls/hour; each agent handles 20)".

## Error Bars Are the Safety Margin

**Tags:** `where it's used` (blue), `uncertainty` (green)

- **The fix** — not a smarter optimizer: an honest forecast of "300 ± 40" instead of a bare "300"
- **Percentile staffing** — plan for the 90th-percentile hour, about 340 calls, not the mean's 300
- **The buffer, priced** — 340 / 20 = 17 agents: the error bar turned into exactly 2 extra agents
- **Not decoration** — the error bar is the only number telling the optimizer how much slack to buy
- **The handoff** — ship the uncertainty with the forecast; downstream it becomes real staffing

*Example (italic):* Handing over "300 ± 40" lets the optimizer weigh 2 extra salaries against blown-up hold times — a bare "300" hides that choice entirely.

**Key point:** Uncertainty quantification is not decoration — the error bar is what lets the optimizer price a buffer instead of planning flush to a guess.

### Visualization (canvas `c3`, 720×300)

The forecast as a distribution: a bell curve over calls per hour, with the mean (300 → 15 agents) and the 90th percentile (340 → 17 agents) marked, and the gap between them labeled as the buffer the error bar buys.

- **Title (bold 15px, `#1a5276`, top center):** "Hand Over the Error Bar, Not Just the Number".
- **Curve:** bell curve, mean 300, sd 40 (deterministic formula, no randomness). Calls 170–430 map to x pixels 70–680: xOf(v) = 70 + (v − 170) / 260 × 610. Baseline y=232, peak height 150: yOf(v) = 232 − 150 · exp(−0.5·((v − 300)/40)²). Stroke blue `#2a78d6` 2.5px; baseline 1px `#999` from x=70 to x=680.
- **Buffer shading:** area under the curve between v=300 and v=340 filled `rgba(0,131,0,0.18)`.
- **Mean marker:** dashed 1.5px blue vertical line at xOf(300)=375 from y=66 to y=232; label bold 13px blue `#2a78d6` centered at (375, 58): "mean 300 → staff 15 agents".
- **90th-percentile marker:** dashed 1.5px green vertical line at xOf(340)≈469 from y=100 to y=232; label bold 13px green `#008300` centered at (505, 92): "90th pct 340 → staff 17 agents".
- **Buffer arrow:** green 1.5px horizontal double-headed arrow just below the baseline at y=246, from x=375 to x=469, arrowheads at both ends.
- **Annotation (bold 13px green `#008300`, centered at (422, 266)):** "the buffer: 2 extra agents".
- **Caption (12px `#6b7280`, centered at (360, 290)):** "calls per hour (illustrative forecast distribution)".

## Don't Blame the Solver

**Tags:** `common mistake` (red)

- **The complaint** — "the plan failed, so the optimizer is broken" — heard after every bad day
- **The alibi** — the optimizer did exactly what its input said: cover 300, spend nothing more
- **The real culprit** — a point estimate that pretended to be certain when it wasn't
- **The amplifier** — optimization magnifies input errors precisely because it removes all slack
- **The check** — before tuning the solver, ask what error bar the inputs actually deserved

*Example (italic):* The 15-agent plan was flawless for a 300-call hour; the "300" was the part that was wrong.

**Common mistake:** Blaming the optimizer for a bad plan. An optimal plan is only as good as its worst input — garbage in, optimal garbage out.

### Visualization (canvas `c4`, 720×300)

A four-box cause chain — point forecast → zero-slack plan → small error → big failure — with a magenta blame arrow looping from the failure back to the input, not the solver.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Blame Belongs".
- **Chain boxes:** four rectangles, width 150, height 58, y=78, x = 24 / 202 / 380 / 558; fill `#fbfcfd`, 2px colored border. Each has a bold 13px header (left-aligned at x+12, y+24) and a 12px `#2c3e50` sub-line (x+12, y+45):
  - "POINT FORECAST" (yellow `#c98500`) / ""300", no error bar"
  - "ZERO-SLACK PLAN" (blue `#2a78d6`) / "exactly 15 agents"
  - "SMALL ERROR" (orange `#d95926`) / "330 calls arrive (+10%)"
  - "BIG FAILURE" (red `#e74c3c`, genuine failure) / "queue grows every hour"
- **Chain arrows:** mute `#6b7280` 1.5px horizontal lines between consecutive boxes at y=107 (from x+154 to next x−10), each with a filled rightward arrowhead.
- **Solver note:** bold 12px green `#008300` centered at (277, 158) under the second box: "the solver did its job".
- **Blame arrow:** magenta `#d55181` 2px polyline from the failure box's bottom center (633, 140) down to (633, 200), left to (99, 200), up to (99, 146), ending in a filled upward arrowhead at (99, 142) pointing at the POINT FORECAST box.
- **Blame label (bold 13px magenta `#d55181`, centered at (366, 192)):** "the blame arrow points at the input, not the solver".
- **Takeaway (bold 13px ink `#1a5276`, centered at (360, 248)):** "an optimal plan is only as good as its worst input".
- **Caption (12px `#6b7280`, centered at (360, 272)):** "optimization amplifies input error because it removes all slack".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` appears only for the genuine failure states (exploding queue, BIG FAILURE box). Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** all chart numbers are hardcoded literals and match the text (300 forecast, 20 calls/agent, 15 vs 17 agents, 330 actual, backlog 0/30/60/90/120, 90th percentile 340); no `Math.random()`; invented numbers carry an "(illustrative)" label.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
