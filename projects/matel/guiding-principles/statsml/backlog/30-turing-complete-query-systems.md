# Turing Complete Languages & Query Systems

**Page type:** detail page (backlog-style 2-column layout: `.card-section` per topic, each with a text column ~45% left and a canvas column ~55% right)
**HTML title tag:** Turing Complete Languages & Query Systems

**Subtitle:** Pure relational SQL is deliberately not Turing complete — it terminates, it is statically analyzable, and the optimizer can rewrite it freely. Recursive CTEs without bounds, procedural extensions and UDFs are what push a query system past that line, buying expressiveness by giving up guarantees.

## 1. The Power / Safety Tradeoff

Every language sits somewhere on a ladder between constrained-and-analyzable and general-and-opaque.

**Declarative (constrained)** — relational algebra, pure SQL, Datalog, regular expressions, CSS selectors:

- **You get:** guaranteed termination, bounded resource usage, global optimization, provable correctness properties
- **You lose:** cannot express all computations, limited control flow, sometimes awkward to write

**Turing complete (unconstrained)** — Python, Java, PL/pgSQL with loops, recursive CTEs without a bound:

- **You get:** any computable function, familiar imperative patterns, full generality
- **You lose:** termination is undecidable (halting problem), resource usage is unpredictable, the optimizer cannot reason about opaque code, caching and parallelism are no longer safe

Key-point callout (red accent): **The symmetry:** constraints enable reasoning, freedom prevents it. If the optimizer knows a query terminates, it can rewrite it freely; if it cannot see inside your code, it cannot help you.

### Visualization (canvas `c1`, 720×300)

Ascending staircase/ladder diagram of five expressiveness rungs, rising left-to-right.

- **Title (bold 14px `#1a5276`, top center):** "Expressiveness Ladder: Each Rung Buys Power, Spends a Guarantee".
- **Rungs:** five boxes 126×28 stepping up (x pitch 137.5 from x=22; y from 232 descending 38px per step), connected by dashed `#ccc` (3/3) diagonal connectors. Each rung shows a bold 10px `#1a5276` centered name inside, a green 9px "+ gain" label above, and a 9px note below (gray `#888` for safe rungs, red `#e74c3c` for bad rungs). Safe rungs: fill `rgba(39,174,96,0.08)` stroke `#27ae60`; bad rungs: fill `rgba(231,76,60,0.07)` stroke `#e67e22`, except the last rung stroked `#e74c3c`.
  1. "Relational algebra" — "+ select, project, join" — note "terminates, cost-bounded" (safe).
  2. "SQL + aggregates" — "+ GROUP BY, windows" — note "still statically analyzable" (safe).
  3. "Recursive CTE" — "+ transitive closure" — note "loses fixed cost estimate" (bad, orange).
  4. "Procedural (loops)" — "+ iteration, branching" — note "loses termination proof" (bad, orange).
  5. "UDF / host language" — "+ any computation" — note "loses plan visibility" (bad, red).
- **Caption (11px gray `#888`, bottom center):** "Power rises to the upper right; the guarantees below the rung do not come back".

## 2. Where Query Systems Cross the Line

The crossing is rarely announced. Some escapes are explicit, some arrive through the tooling around SQL.

**Explicit escapes** — arbitrary code invited into execution:

- **UDFs** in Python/Java embedded in query execution
- **Stored procedures** — PL/pgSQL, T-SQL with `WHILE` loops and conditionals
- **External / remote functions** — Snowflake external functions, BigQuery remote functions
- **Custom SerDes and Spark UDFs** — arbitrary Java, or Python/Scala closures shipped to executors

**Subtle escapes** — the SQL still looks like SQL:

- **Recursive CTEs with no bound** — unbounded recursion is the classic route to Turing completeness
- **Templating** — dbt is SQL-only by design, but Jinja itself has loops, conditionals and recursion
- **Dynamic SQL** — `EXECUTE IMMEDIATE` on a computed string; the text is only known at runtime
- **Trigger cascades** — triggers firing triggers, potentially non-terminating

Key-point callout (red accent): **The test:** can an unbounded loop be constructed? If yes, "will this query finish?" no longer has a general answer, no matter how declarative the surrounding syntax looks.

### Visualization (canvas `c2`, 720×300)

Two-region diagram divided by a horizontal dashed decidability boundary, with labeled pills in each region.

- **Title (bold 14px `#1a5276`, top center):** "Crossing the Decidability Boundary".
- **Regions:** upper band (y 40 to ~146) tinted `rgba(231,76,60,0.06)`; lower band (y 158 to 264) tinted `rgba(39,174,96,0.07)`.
- **Boundary:** horizontal dashed `#1a5276` (dash 6/4, width 2) line at y=152. Region labels in 10px, left-aligned at x=24: above the line in red — "ABOVE — termination and cost undecidable in general"; below in green — "BELOW — terminates, cost estimable, plan rewritable".
- **Pills:** white 200×26 boxes with 1.5px colored outline and 10px `#2c3e50` centered text, in a 3-column × 2-row layout per region (columns at x=30, 265, 490).
  - Above (red `#e74c3c` outline): "Python / Java UDF", "PL/pgSQL WHILE loop", "Jinja macro loops", "EXECUTE IMMEDIATE", "Trigger cascades", "Recursive CTE, no bound".
  - Below (green `#27ae60` outline): "Pure SELECT / JOIN", "GROUP BY + windows", "Recursive CTE + LIMIT", "Built-in scalar fns", "Stratified Datalog", "Regular expressions".
- **Caption (11px gray `#888`, bottom center):** "One construct above the line puts the whole query above the line".

## 3. What the Optimizer Loses

An opaque function is not just slower to run — it disables the reasoning the whole engine is built on.

- **Optimizer blindness** — predicates cannot be pushed through the function, operators cannot be reordered around it, unused input columns cannot be pruned inside it
- **Parallelization risk** — hidden state or side effects make concurrent execution unsafe
- **Resource unpredictability** — the body may allocate without bound, loop forever, or call an external service
- **Termination uncertainty** — no general procedure can prove an arbitrary program halts
- **Caching invalidation** — results cannot be memoized if behaviour depends on state the engine cannot observe

Key-point callout (red accent): **Impact:** the barrier is positional, not local. Everything downstream of the opaque box inherits its opacity, so one UDF in the middle of a plan can cost the optimizations of every stage after it.

Example line (italic): Example: a UDF might read its input columns reflectively, so the planner must assume all of them are live and keep the widest possible row.

### Visualization (canvas `c3`, 720×300)

Two side-by-side vertical query-plan panels comparing predicate pushdown with and without a UDF node.

- **Title (bold 14px `#1a5276`, top center):** "What the Optimizer Can See Inside the Plan".
- **Left panel (x=20, width 330), heading bold 11px green `#27ae60`:** "Declarative plan — fully described". Vertical stack of plan nodes (24px tall boxes, 8px gap, connected by short gray `#999` lines), top to bottom: "Aggregate", "Filter  region = \"APAC\"", "Hash join", "Scan  fact_table". All nodes fill `rgba(26,82,118,0.35)`, stroke `#1a5276`, 10px labels in `#1a5276`. Solid green `#27ae60` bracket arrow on the left from the Filter node down to the Scan node with rotated 9px green label "pushdown allowed".
- **Right panel (x=370, width 330), heading bold 11px red `#e74c3c`:** "Same plan with a UDF node". Nodes: "Aggregate", "Filter  region = \"APAC\"", "udf_score(row)" (opaque — fill `rgba(231,76,60,0.20)`, stroke `#e74c3c`, red label), "Scan  fact_table". Dashed (4/3) red bracket arrow on the left with rotated 9px red label "pushdown blocked" and a red X mark drawn on the blocked path.
- **Divider:** vertical `#e0e0e0` line at x=355 between panels.
- **Caption (11px gray `#888`, bottom center):** "The planner can only rewrite what it can describe — the opaque node pins its neighbours".

## 4. Concrete Examples

Three familiar escape hatches, all with the same shape: the analyzable layer stops where the arbitrary code starts.

- **SQL → SQL + Python UDF.** Pure SQL: the optimizer sees the full plan, pushes filters, prunes columns, chooses join order globally. With the UDF it treats that node as a black box — no pushdown past it, no pruning inside it, no time or memory estimate. Queries can end up an order of magnitude slower purely because the optimizer gave up.
- **Spark DataFrame → `.mapPartitions`.** The DataFrame API lets Catalyst see a logical plan, fuse operations, estimate shuffle sizes and generate code. Handing it an arbitrary closure stops Catalyst at that point: no predicate pushdown, no column pruning, no codegen for anything downstream.
- **dbt: SQL-only → complex Jinja.** The SQL-only philosophy is what makes models analyzable, testable and deterministic. Macros that loop to generate SQL, branch on runtime variables, or call themselves recursively rebuild a weak programming language on top of SQL and forfeit that simplicity.

Key-point callout (red accent): **Pattern:** the cost is not paid at the escape hatch, it is paid across the whole plan that surrounds it — and it shows up as unpredictability, not just as latency.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of planner cost estimates: bounded operators with uncertainty whiskers vs unbounded constructs.

- **Title (bold 14px `#1a5276`, top center):** "Cost Predictability Before Execution".
- **Rows (label, estimate ± error in arbitrary units):** Seq scan 70 ± 12; Hash join 155 ± 35; Sort + GROUP BY 120 ± 28; Window function 100 ± 22; Python UDF — no estimate; WHILE loop — no estimate.
- **Layout:** bars from x=175 to x=665, scale max 320 units, row height 34px starting y=56; row labels 11px `#2c3e50` right-aligned; vertical `#f0f0f0` gridlines at 0/80/160/240/320 with 9px `#999` value labels above.
- **Bounded bars:** fill `rgba(26,82,118,0.35)`, 16px tall, with a `#1a5276` uncertainty whisker (horizontal line with end caps spanning est ± err) and 9px `#1a5276` label to the right: "estimate ± N".
- **Unbounded rows:** full-width bar fill `rgba(231,76,60,0.18)` with dashed (4/3) red `#e74c3c` outline, an open red arrowhead at the right edge, and 9px red label inside: "no upper bound".
- **Axis label (10px `#666`, centered below rows):** "planner cost estimate (arbitrary units) →".
- **Caption (11px gray `#888`, bottom center):** "A cost model interpolates over bounded operators; a loop offers nothing to interpolate".

### Comparison table (full-width `.compare` table below the 2-col row)

| Escape hatch | Analyzable layer it disables | Guarantee given up |
|--------------|------------------------------|--------------------|
| Python UDF in SQL | Predicate pushdown, column pruning, join reorder | Cost estimate, determinism |
| `.mapPartitions` in Spark | Catalyst logical plan, whole-stage codegen | Shuffle-size prediction, operator fusion |
| Recursive Jinja macro | Static model analysis, diffable SQL | Reproducibility of generated text |
| Unbounded recursive CTE | Fixed-point cardinality estimate | Termination |

## 5. The "Just Expressive Enough" Design Space

**Sub-Turing languages that work in practice** show the space is not empty:

- **Pure SQL** — relational algebra plus aggregation, no general recursion
- **Stratified Datalog** — recursive, but stratified negation still guarantees termination
- **Regular expressions** — finite automata, always terminate, linear-time matching
- **Total functional languages** — Agda, Idris: every program provably terminates
- **Linear types** — resources provably used exactly once

**A transform language for features** would need: column references, arithmetic, aggregations and window functions; pattern matching on types instead of general conditionals; piped composition of transforms; no loops, no recursion, no external calls, no mutable state — plus an explicitly marked custom transform that opts out of the guarantees.

Questions callout (orange accent): **Question:** is that just SQL with different syntax, or is there a genuine sweet spot between SQL and Python — more expressive power at the same analyzability?

### Visualization (canvas `c5`, 720×300)

Scatter plot of languages on expressive power (x) vs static analyzability (y), with a frontier curve and a target band.

- **Title (bold 14px `#1a5276`, top center):** "Design Space: Expressive Power vs Static Analyzability".
- **Axes:** plot area x=70, y=44, width = canvas − 130, height 200; gray `#ccc` L-axes; 11px `#666` axis labels: rotated "static analyzability →" on the left, "expressive power →" below center. Coordinates in 0–100 units on both axes.
- **Sweet-spot band:** rectangle from x 38–72, y 68–94 (in plot units), fill `rgba(230,126,34,0.08)`, dashed (4/3) orange `#e67e22` outline, 9px orange label "target band" at its upper left.
- **Frontier curve:** dashed (5/4) gray `#999` bezier from (8, 97) curving through roughly (40, 92) and (70, 70) to (98, 8), 9px gray label "known frontier" near (92, 28).
- **Points (radius 4.5 dots, 10px `#2c3e50` labels):**
  - Green `#27ae60`: "Regular expressions" (12, 96); "Pure SQL" (40, 90); "Stratified Datalog" (52, 84); "Total functional (Agda)" (66, 72).
  - Orange `#e67e22`: "Proposed transform DSL" (58, 88).
  - Red `#e74c3c`: "SQL + Jinja templating" (82, 26); "Python / UDF" (95, 10).
- **Caption (11px gray `#888`, bottom center):** "The open question: does a point exist above the frontier — more power at equal analyzability?".

## 6. Containing the Inevitable Escape Hatch

Imperative code will be needed. The design goal is to isolate it behind interfaces that restore, externally, the guarantees the language no longer provides.

- **Contract** — input schema to output schema, typed and validated at the boundary
- **Guarantee** — terminates within a timeout, enforced by the runtime rather than proved from the code
- **Constraint** — no side effects, or declared side effects verified by a sandbox
- **Testing** — property-based tests checking the contract holds on generated inputs
- **Monitoring** — runtime resource tracking with alerts on anomalies

**Design implication for statsml:** prefer declarative specifications of transforms (what to compute) over imperative implementations (how to compute it). That is what enables reordering, concurrent execution of independent steps, verification of spec properties, and deterministic reruns.

Key-point callout (red accent): **Rule:** imperative code at the edges, declarative specifications in the core. A timeout is not a termination proof — it is a bound imposed from outside, and it only holds if every gate is actually enforced.

Questions callout (orange accent), full-width below the 2-col row: **Open discussion points**

- Where should the pipeline use declarative DSLs versus general-purpose code?
- Can a "just expressive enough" transform language be designed that is not Turing complete?
- How should custom transforms be handled without losing system-level properties?
- Is the Spark DataFrame API the right model — declarative until you reach for `.map`, then an explicit opt-out?

### Visualization (canvas `c6`, 720×300)

Horizontal five-box pipeline diagram: imperative edges, contract gates, declarative core.

- **Title (bold 14px `#1a5276`, top center):** "Imperative Edges, Declarative Core, Enforced Gates".
- **Boxes (left to right, 120px tall at y=60, connected by gray `#999` arrows, 13px gaps, starting x=10):**
  1. "Imperative edge" (130px wide) — lines "custom transform", "external call" — red `#e74c3c`, fill `rgba(231,76,60,0.07)`, solid border.
  2. "Contract gate" (120px) — lines "typed schema in", "timeout", "sandbox" — orange `#e67e22`, fill `rgba(230,126,34,0.07)`, dashed (5/4) border.
  3. "Declarative core" (150px) — lines "reorderable", "parallelizable", "provable", "reproducible" — blue `#1a5276`, fill `rgba(26,82,118,0.10)`, solid 2px border.
  4. "Contract gate" (120px) — lines "typed schema out", "property tests", "resource monitor" — orange, dashed border.
  5. "Imperative edge" (130px) — lines "serving code", "side effects" — red, solid border.
- Box headings bold 11px in box color; inner lines 10px `#2c3e50` centered.
- **Guarantee span:** solid green `#27ae60` 2px underline beneath the core box with 10px green centered label "guarantees hold here".
- **Gate note (10px orange `#e67e22`, left-aligned at x=20 below):** "gate = guarantee imposed from outside, not proved from the code".
- **Caption (11px gray `#888`, bottom center):** "A timeout bounds runtime; it never decides termination".

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col text/viz layout). Page: h1, `.subtitle` paragraph, then one `.card-section` per numbered topic. Each `.card-section` has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one `<tr>`: left `td.text-col` (45%) with paragraphs, `<ul>` bullets, `.key-point`/`.questions` callouts and `.example` lines; right `td.viz-col` (55%) with the canvas. Section 4 additionally has a full-width `table.compare` below its layout table; section 6 has a full-width `.questions` callout (with a `<ul>`) below its layout table. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `strong` in `#1a5276`; lists 0.92rem.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.questions` — same but left border `3px solid #e67e22`. `.example` — italic, `#555`, 0.9rem.
- **Inline code:** background `#f8f9fa`, border `1px solid #e0e0e0`, padding 1px 5px, radius 3px, 0.85em, color `#1a5276`.
- **Compare table:** `table.compare` full-width, 0.9rem; `th` and `td` border `1px solid #e0e0e0`, padding 8px 10px, left-aligned, top-aligned; `th` background `#f8f9fa`, color `#1a5276`.
- **Canvas:** intrinsic 720×300 per chart, CSS `width: 100%`, border `1px solid #e0e0e0` radius 4px; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, grays `#666`/`#888`/`#999`/`#ccc`.
- Detail pages have no nav bar and no back/home links; any card links in regenerated HTML grids use `.html` extensions.
