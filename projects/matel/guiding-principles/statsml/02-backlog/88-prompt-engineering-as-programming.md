# Prompt Engineering as Programming

**Page type:** detail page (backlog-style two-column layout: text left 50%, canvas right 50%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** Prompt Engineering as Programming

**Subtitle:** A prompt is a spec that executes, so it inherits programming's disciplines — with almost none of programming's tooling.

**Intro callout:** The analogy is productive up to the point where it breaks. Prompts need versioning, diffing, review, and regression tests like code does. But the callee is non-deterministic, there is no compile step, and the interface is prose — so each borrowed discipline needs a different implementation, not just a rename.

## 1. A Spec That Executes

A prompt is the rare artifact that is both the requirements document and the program.

- **Both roles at once** — the prose states the intent and is also the thing that runs.
- **So it inherits the disciplines** — version control, review, tests, and a stable contract.
- **Versioning transfers cleanly** — a prompt is text; commit it, tag it, pin it per release.
- **Review transfers partly** — a reader can spot ambiguity but cannot predict behavior.
- **Testing needs redesign** — the callee returns a distribution, not a value.
- **Static checking has no analogue** — no parser rejects a contradictory instruction.
- **What survives the analogy** — process discipline; what does not is the tooling under it.

**Key point:** Borrow programming's disciplines, not its tools — most of the tools assume a deterministic, checkable callee.

### Visualization (canvas `c1`, 720x340)

Presence grid: which software-engineering disciplines have a working analogue for prompts.

- **Title (bold 16px, `#1a5276`, top center):** "Which Disciplines Have a Prompt Analogue".
- **Columns:** header row at y=56 — "code" centered at x = canvas-200, "prompt" centered at x = canvas-96 (bold 13px `#1a5276`).
- **Rows (13px `#2c3e50`, left-aligned at x=18, first row y=88, spacing 34):** `version control`, `meaningful diff`, `peer review`, `static check before run`, `unit test`, `integration test`, `stable contract for callers`.
- **Row separators:** 1px `#ecf0f1` line under each row, spanning x=14 to canvas-14.
- **States (drawn as glyphs, 15px bold, centered on the column x):** `2` = "✓" in `#27ae60`, `1` = "~" in `#e67e22`, `0` = "✗" in `#e74c3c`.
  - code column: `[2, 2, 2, 2, 2, 2, 2]`
  - prompt column: `[2, 0, 2, 0, 1, 1, 0]`
- **Legend (12px, bottom, y = height-14, left-aligned at x=18):** green "✓ works as-is", orange "~ partial / needs redesign", red "✗ no analogue".

## 2. No Compile Step

A broken prompt does not fail loudly. It returns something fluent and wrong.

- **A crash is a gift** — it names the file, the line, and the moment the contract broke.
- **A prompt has no such boundary** — malformed instructions still produce confident prose.
- **Strictly worse than a crash** — nothing signals the failure, so nothing triggers a fix.
- **The detection point moves late** — from build time to output review, or to never.
- **Contradictions are silently resolved** — the model picks one branch and never mentions it.
- **The only substitute** — an eval suite run before release, acting as the missing compiler.

**Key point:** With no compile step the failure is silent, so detection cost, not fix cost, dominates.

### Visualization (canvas `c2`, 720x320)

Horizontal stacked bars: where a defect is caught, for a compiled call vs a prompt call. Illustrative Example.

- **Title (bold 16px, `#1a5276`, top center):** "Where the Defect Gets Caught".
- **Subtitle (12px italic `#7f8c8d`, centered under title):** "Illustrative Example — 200 injected defects per row".
- **Data (raw defect counts out of 200; percentages computed at render from the counts, never hardcoded):**
  - compiled function call: `[144, 40, 12, 4]`
  - prompt call: `[0, 50, 60, 90]`
- **Segment order and colors:** caught at compile/parse `#1a5276`, caught by a test `#27ae60`, caught reviewing the output `#e67e22`, never noticed `#e74c3c` (all at 0.55 alpha fill with the solid color as 1.2px stroke).
- **Bars:** x=150, width = canvas-190; bar height 54; row 1 at y=96, row 2 at y=184.
- **Row labels (13px `#2c3e50`, right-aligned at x=140, vertically centered on the bar):** "compiled call", "prompt call".
- **In-segment labels (bold 12px white, centered):** computed percent (`round(count/total*100) + "%"`), printed only when the segment is at least 8% of the row.
- **Legend (12px `#2c3e50`, y=272, four swatches left to right from x=150):** the four segment names.
- **Annotation (13px `#e74c3c`, left-aligned, y=300, at x=150):** computed — "never noticed: 2% vs 45%" built from both rows' last segment.

## 3. Diffing Prose

A textual diff shows which words changed. It does not show what changed.

- **Textual diff is the wrong unit** — a one-word edit can move behavior more than a rewrite.
- **No structure to anchor on** — prose has no call graph, no types, no signature to compare.
- **Size does not predict impact** — edit length and behavior change are nearly uncorrelated.
- **The only meaningful diff is behavioral** — run both versions over one fixed eval set.
- **Report the delta as rates** — per-case pass/fail, plus the cases that flipped either way.
- **Flip lists beat aggregate deltas** — two offsetting flips can leave the rate unchanged.
- **Cost is the catch** — every review needs an eval run, so the suite must be cheap.

**Key point:** Diff the behavior over a fixed suite, and report which cases flipped, not just the net rate.

### Visualization (canvas `c3`, 720x320)

Scatter: size of the textual edit vs magnitude of the behavior change, with Pearson r computed at render.

- **Title (bold 16px, `#1a5276`, top center):** "Edit Size Does Not Predict Behavior Change".
- **Subtitle (12px italic `#7f8c8d`, centered under title):** "Illustrative Example — 15 prompt edits".
- **Data (hardcoded literal pairs — the shape carries the lesson; x = words changed, y = absolute change in suite pass rate, percentage points):**
  `[1,22] [2,3] [3,14] [5,2] [6,9] [8,1] [11,18] [14,5] [18,2] [23,11] [27,4] [34,16] [41,3] [52,7] [63,1]`
- **Plot area:** x=76, y=72, width = canvas-150, height = canvas-132; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** x from 0 to 65 words (ticks every 13), y from 0 to 25 points (ticks every 5, 12px `#5a6875`, right-aligned).
- **Axis labels (13px `#4a5866`):** "Words changed in the prompt" centered below; "|Δ pass rate| (pts)" rotated -90° left of the y ticks.
- **Points:** radius 5, fill `rgba(26,82,118,0.35)`, stroke `#1a5276` 1.4px.
- **Computed stats box (13px, top-right inside the plot):** Pearson `r` over the 15 plotted points and `r²`, both computed in JS and printed to two decimals — `r = -0.28`, `r² = 0.08` for the array above. Color `#e74c3c`.
- **Annotations (12px `#e67e22`):** "one word, 22 pts" beside the `[1,22]` point; "63 words, 1 pt" beside the `[63,1]` point.

## 4. A Clarifying Reword Is a Breaking Change

Two prompts a human calls equivalent can produce measurably different output distributions.

- **Human equivalence is not behavioral equivalence** — the reader's test is not the model's.
- **Adding a clarification reweights everything** — emphasis moves, so does the output mix.
- **Downstream parsers are the victims** — a format that was implicit becomes optional.
- **Semantic versioning applies** — any distribution shift is a major bump, not a patch.
- **Measure the shift, don't argue it** — total variation distance over labeled output categories.
- **Even a tightening is breaking** — fewer refusals is a contract change for a retry path.

**Key point:** Treat any reword as a breaking change until an eval shows the output distribution is unmoved.

### Visualization (canvas `c4`, 720x320)

Paired bars: output-category mix under the original prompt vs the reworded one. Illustrative Example.

- **Title (bold 16px, `#1a5276`, top center):** "Same Meaning to a Reader, Different Output Mix".
- **Subtitle (12px italic `#7f8c8d`, centered under title):** "Illustrative Example — 500 responses per prompt version".
- **Data (raw counts out of 500 each; shares computed at render):**
  - categories: `concise answer`, `bulleted list`, `clarifying question`, `hedged / caveats`, `declined`
  - prompt A: `[310, 120, 30, 30, 10]`
  - prompt A′ (reworded): `[180, 160, 95, 50, 15]`
- **Plot area:** x=66, y=88, width = canvas-120, height = canvas-160; scale max 65%; L-shaped axes `#95a5a6` (1.4px).
- **Bars:** 5 slots; per slot two bars each 0.34·slot-width — left (A) fill `rgba(41,128,185,0.50)` stroke `#2980b9` 1.4px; right (A′) fill `rgba(230,126,34,0.50)` stroke `#e67e22`.
- **Y ticks:** every 15% up to 60% (12px `#5a6875`, right-aligned), computed as `count/500`.
- **X labels:** category names (12px `#4a5866`) under each slot.
- **Legend (12px, top-left inside plot):** blue swatch + "prompt A", orange swatch + "prompt A′ (clarified)".
- **Computed stat (bold 13px `#e74c3c`, top-right inside plot):** total variation distance `0.5·Σ|p−q|` over the ten plotted counts — `0.26` for the arrays above, computed in JS.

## 5. Regression Tests for a Non-Deterministic Callee

You cannot assert equality against a sampler. You assert properties and track rates.

- **Equality assertions are unusable** — the same prompt returns different valid strings.
- **Assert invariants instead** — schema validity, required fields, bounds, no contradiction.
- **Track pass rate, not pass/fail** — the suite's output is a proportion with a standard error.
- **Small suites cannot see small shifts** — 40 cases only resolve very large drops.
- **The arithmetic, at 80% power and α=0.05** — 40 cases detect 90% → 65%, no finer.
- **Detecting 90% → 80% needs ~197 cases**, from n = (z₀.₉₇₅+z₀.₈)²(p₁q₁+p₂q₂)/(p₁−p₂)².
- **Noise floor at n=40** — the 95% interval around a 90% pass rate is ±9.3 points.
- **Assumes independent cases** — correlated or near-duplicate cases inflate the real n needed.

**Key point:** A regression suite's resolution is set by its size: state the drop you can detect before trusting a green run.

### Visualization (canvas `c5`, 720x320)

Nightly pass rates for one unchanged prompt on a 40-case suite, against the computed 95% band.

- **Title (bold 16px, `#1a5276`, top center):** "Run-to-Run Noise on a 40-Case Suite".
- **Subtitle (12px italic `#7f8c8d`, centered under title):** "Illustrative Example — same prompt, true pass rate 90%, seeded draws".
- **Data generation:** seeded Park–Miller LCG (`lcg(10)`), 14 runs × 40 independent Bernoulli(0.90) trials; each run's plotted value is `successes/40`. With this seed the successes are `[38,37,36,36,39,38,37,32,39,36,38,37,36,39]`, i.e. 80.0%–97.5%. No `Math.random()`.
- **Plot area:** x=76, y=88, width = canvas-150, height = canvas-150; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** y from 70% to 100% (ticks every 5%, 12px `#5a6875`); x spans runs 1–14, axis label "Nightly run" centered below.
- **Band (derived, not drawn by eye):** half-width `1.96·√(p(1−p)/n)` with p=0.90, n=40 → ±9.3 points; filled `rgba(26,82,118,0.12)` between 90±half-width, dashed `#1a5276` (dash 4/4) edges.
- **Center line:** dashed green `#27ae60` (dash 5/4) at 90%, label "true rate 90%" (12px `#27ae60`, right end of the line).
- **Points:** radius 5, fill `#2980b9`, joined by a 1.5px `rgba(41,128,185,0.45)` line.
- **Computed labels (13px):** `#1a5276` "95% band ±9.3 pts (n=40)" printed from the derived half-width; `#e74c3c` observed min–max of the 14 plotted points, computed at render.

## 6. Multi-Hop Prose Contracts

When one agent instructs another, each hop reinterprets the prose it was handed.

- **Every hop is a re-parse** — the receiver reconstructs intent from words, not from a struct.
- **Loss is not recoverable downstream** — hop 3 cannot restore what hop 2 dropped.
- **A simple model** — independent per-hop fidelity f gives end-to-end fidelity f^k over k hops.
- **The model is illustrative** — real hops are correlated and f is task-specific, not measured.
- **Even so, the shape is the point** — 95% per hop is 77% after 5 hops; 80% per hop is 33%.
- **Mitigation is structural** — pass a schema between agents and keep prose for humans.
- **Cap the chain** — fewer hops beats better wording at every level of f.

**Key point:** Prose is a lossy interface, so route agent-to-agent handoffs through a schema and keep the chain short.

### Visualization (canvas `c6`, 720x320)

Line chart: end-to-end fidelity vs number of hops under the independent-fidelity model f^k.

- **Title (bold 16px, `#1a5276`, top center):** "End-to-End Fidelity Under f^k".
- **Subtitle (12px italic `#7f8c8d`, centered under title):** "Illustrative model — independent per-hop fidelity, not a measurement".
- **Plot area:** x=76, y=88, width = canvas-150, height = canvas-150; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** y from 0% to 100% (ticks every 20%, 12px `#5a6875`); x spans hops 1–6, integer ticks, axis label "Hops" centered below.
- **Curves (values computed as `Math.pow(f, k)` at render, never hardcoded), 3px:** f=0.95 in `#27ae60`, f=0.90 in `#e67e22`, f=0.80 in `#e74c3c`; filled circles (radius 4) at each integer hop.
- **Curve labels (12px, matching curve color, right of the k=6 point):** "f = 0.95", "f = 0.90", "f = 0.80".
- **Computed annotation (12px `#2c3e50`, left-aligned inside the plot near hop 5):** "at 5 hops: 77% / 59% / 33%" with each value computed from `Math.pow(f, 5)` and rounded.

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 50% and `td.viz-col` 50%, both `vertical-align: top`, 12px padding. No index number in the h1 or the title tag.
- **Text blocks:** intro `<p>`, `<ul>` bullets (0.92rem) with `<strong>` lead-ins, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `height: auto`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `rgba(26,82,118,0.35)` bar/point fill; secondary `#2980b9`; gray labels `#5a6875`/`#4a5866`, axes `#95a5a6`.
- **Canvas:** intrinsic 720 wide (340 for `c1`, 320 for the rest); a shared `setupCanvas(id, w, h)` sizes the backing store to the rendered width × `window.devicePixelRatio`, caps display width via `style.maxWidth`, and resets the transform; all charts are registered and re-rendered on `window.resize`.
- **Numbers:** no `Math.random()` anywhere. Every statistic printed beside generated data (`r`, `r²`, total variation distance, percentages, the ±9.3-point band, the f^k values, observed min–max) is computed in JS from the plotted values at render time.
- **Cross-references:** none — no back/home/nav links, no links to sibling pages.
