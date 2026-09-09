# Monads: Composable Effect Tracking for Pipelines

**Page type:** detail page (backlog-style 2-column layout: `.card-section` per topic, each with a text column ~45% left and a canvas column ~55% right)
**HTML title tag:** Monads: Composable Effect Tracking for Pipelines

**Subtitle:** Discussion / design pattern — a data pipeline is a chain of transforms where each step may fail, warn, log, or fan out. A monad makes that "extra" part of the type and supplies the plumbing so composition stays flat.

## 1. The Pipeline Problem

**Without monads — concerns are smeared.** Every transform step can succeed, fail with an error, succeed with warnings, hit missing data, need to log what it did, or produce several candidate results. Handled ad hoc, each of those becomes a try/catch and a null check inside every step. Error shapes differ per step, logging goes through a global logger, and nothing composes.

**With monads — one wrapper, one composition rule.** Each concern gets a type that carries it, and each step becomes a pure function returning a wrapped value:

- **Missing data** → Maybe/Option propagates absence
- **Failure** → Either/Result carries error context
- **Logging** → Writer accumulates metadata
- **Many outcomes** → List explores all paths
- **Side effects** → IO isolates impurity at the edges

Key-point callout (red accent): **The shift:** error handling stops being a statement scattered through every step and becomes a property of the composition operator. Steps describe only the happy path.

### Visualization (canvas `c1`, 720×300)

Two-column stage-stack diagram comparing imperative per-stage error handling with a single monadic failure rail.

- **Title (bold 14px `#1a5276`, top center):** "Where Error Handling Lives".
- **Stages (both columns, five 140×26 boxes at 40px pitch starting y=52):** parse, validate, join, encode, score.
- **Left column (x=24), heading bold 11px red `#e74c3c`:** "Imperative: a check per stage". Boxes stroked `#1a5276`, fill `rgba(26,82,118,0.06)`; next to each box a 9px red note "try/except + null check"; 9px gray `#888` centered footnote below: "error shape differs per stage".
- **Right column (x=396), heading bold 11px green `#27ae60`:** "Monadic: one failure rail". Boxes stroked `#27ae60`, fill `rgba(39,174,96,0.08)`; a 10px `#1a5276` ">>=" label centered between consecutive boxes; a vertical dashed red (4/3) failure rail at x=614 spanning the stack, with dashed gray `#ccc` arrows from each stage to the rail, ending in bold 10px red label "Left(error)".
- **Caption (11px gray `#888`, bottom center):** "Steps describe the happy path; the composition operator owns the failure path".

## 2. What Actually Makes It a Monad

A monad is a type constructor `M` plus two operations that satisfy the three laws:

- `unit :: a -> M a` — also called `return` or `pure`. Puts a plain value into the context.
- `bind :: M a -> (a -> M b) -> M b` — written `>>=`, `flatMap`, or `and_then`. Feeds the inner value to a function that itself returns a wrapped value.

**Not the same as a functor.** A functor only has `map :: M a -> (a -> b) -> M b`. Mapping a function that already returns a wrapped value gives `M (M b)` — the nesting compounds with every step. An applicative adds `ap`, which combines independent wrapped values but cannot let step N+1 depend on step N's result.

Key-point callout (red accent): **bind = map then join.** The flattening (`join :: M (M a) -> M a`) is the whole difference. It is what keeps a chain of fallible steps one level deep instead of a tower.

Example line (italic): Example: mapping `safe_div` over `Just 12` yields `Just (Just 4)`; binding it yields `Just 4`, which the next step can consume directly.

### Visualization (canvas `c2`, 720×300)

Four-row value-flow diagram: plain vs wrapped values, map nests, bind flattens.

- **Title (bold 14px `#1a5276`, top center):** "Plain Value vs Value in a Context".
- **Rows (54px pitch starting y=54); each row shows a 10px `#555` tag label at x=14, a source box (84×26, stroke `#1a5276`, fill `rgba(26,82,118,0.06)`), an arrow in the row color with a 9px op label above, a destination box, and a 10px note in the row color at x=440:**
  1. tag "plain value" — source "3" — op "f" — destination "f 3 = 6" (green `#27ae60` box) — note "composes".
  2. tag "wrapped + raw f" — source "Just 3" — op "f" — destination "type error" (dashed (4/3) red `#e74c3c` outlined box, red text) — note "f expects Int, not Maybe Int".
  3. tag "map (functor)" — source "Just 12" — op "map safe_div" — destination "Just (Just 4)" (orange `#e67e22` box) — note "nested — next step cannot read it".
  4. tag "bind (monad)" — source "Just 12" — op "bind safe_div" — destination "Just 4" (green box) — note "flat — chainable".
- **Caption (11px gray `#888`, bottom center):** "unit wraps; bind = map then join — the join is what a functor lacks".

## 3. The Three Laws

A type with `unit` and `bind` is only a monad if all three hold. They are not decoration — they are the guarantee that lets you refactor a pipeline without changing its meaning.

- **Left identity:** `unit(x) >>= f  ≡  f(x)`. Wrapping then binding adds nothing.
- **Right identity:** `m >>= unit  ≡  m`. A no-op step is really a no-op.
- **Associativity:** `(m >>= f) >>= g  ≡  m >>= (\x -> f x >>= g)`. Grouping is free.

Key-point callout (red accent): **Why it matters here:** associativity is what makes it safe to extract stages 3–5 of a pipeline into a named sub-pipeline and drop it back in. If a "monad" in your codebase breaks it — a wrapper that mutates shared state, or one that swallows a second error — that refactor silently changes results.

Example line (italic): Counterexample: pandas `NaN` looks like a Maybe, but `NaN != NaN` and some aggregations skip it while others propagate — a broken monad, so chain rewrites are not safe by construction.

### Visualization (canvas `c3`, 720×300)

Three side-by-side commuting-path diagrams, one per monad law.

- **Title (bold 14px `#1a5276`, top center):** "The Three Laws as Commuting Paths".
- **Panels:** three 226px-wide panels at 234px pitch starting x=14, each with a bold 11px `#1a5276` centered name at y=52, three node labels (11px `#2c3e50`: two top corners at y=88 and one bottom-center at y=176), a direct green `#27ae60` top arrow with 9px green label, two blue `#1a5276` via-node arrows with 9px blue labels, a bold gray "=" equivalence marker between paths, and the equation in 9px `#555` below (y≈226):
  1. "Left identity" — nodes "x", "M b", "unit x" — top arrow "f", down arrow "unit", up arrow ">>= f" — equation "unit x >>= f  =  f x".
  2. "Right identity" — nodes "m : M a", "M a", "M (M a)" — top arrow "id", down arrow "map unit", up arrow "join" — equation "m >>= unit  =  m".
  3. "Associativity" — nodes "m", "M c", "M b" — top arrow ">>= (f >=> g)", down arrow ">>= f", up arrow ">>= g" — equation lines "(m >>= f) >>= g  =" / "m >>= (x -> f x >>= g)".
- **Caption (11px gray `#888`, bottom center):** "Both paths must agree — that is what makes regrouping a chain into sub-pipelines safe".

## 4. Short-Circuiting and the Useful Monads

**Maybe / Option** handles missing data without null checks. If stage 3 of a chain returns `Nothing`, the remaining stages are never run and no null reaches them — the absence is the return value, not an exception raised somewhere downstream.

**Either / Result** does the same but carries *why*. `Right(value)` continues; `Left(error)` short-circuits with a structured error naming the failing rule, the offending input, and the stage. That reaches the caller intact instead of surfacing as a cryptic trace from a later stage that received bad data.

Key-point callout (red accent): **Short-circuit is a property of bind, not of the steps.** Once a value is on the failure side, every subsequent `bind` is a pass-through. The imperative equivalent needs an explicit "if error, return early" after each call — and one forgotten check is a silent corruption.

### Visualization (canvas `c4`, 720×300)

Two-track railway diagram: five validation stages with a failure at stage 2 diverting to the failure track.

- **Title (bold 14px `#1a5276`, top center):** "Failure at Stage 2 Arrives Intact at the End".
- **Stages (106×34 boxes, 16px gaps, starting x=26, success row at y=74):** not_empty, numeric, in_range, normality, fit.
- **Track labels (10px, left):** "success track" in green `#27ae60` above the success row; "failure track" in red `#e74c3c` above the failure rail at y=190.
- **Stage styling:** stage 1 green (stroke `#27ae60`, fill `rgba(39,174,96,0.08)`); stage 2 failed — red (stroke `#e74c3c`, fill `rgba(231,76,60,0.08)`); stages 3–5 skipped — dashed (4/3) `#ccc` outline, 11px `#999` labels with 9px "not run" beneath.
- **Failure rail:** solid 2px red horizontal line at y≈207 across the full stage span; a red arrow drops from the failed stage down to the rail, labeled 9px red "Left(Error(step, rule, input))"; at the rail's right end, bold 10px red right-aligned: "error returned unchanged".
- **Inter-stage arrows:** green before failure, gray `#ccc` after.
- **Caption (11px gray `#888`, bottom center):** "The imperative form needs an explicit early return after every call; one omission corrupts silently".

### Comparison table (full-width `.compare` table below the 2-col row)

| Monad | Wraps | Pipeline use | What bind does |
|-------|-------|--------------|----------------|
| **Maybe / Option** | value or absence | nullable feature values, lookups that may miss | skips the rest of the chain on `Nothing` |
| **Either / Result** | value or error | validation, schema and range checks, parsing | short-circuits, carrying the error untouched |
| **Writer** | value + accumulated log | row counts, timings, quality flags, transform lineage | runs the step, then merges its log into the running one |
| **List** | many values | multi-candidate parameter search, ambiguous parses | applies the step to each element and flattens the result |
| **IO** | a deferred effect | database reads, file writes, API and clock access | sequences effects, keeping the pure core testable |

(Column widths: 14% / 20% / 33% / 33%.)

## 5. In Practice

**Imperative validation** loses the context of which check failed, then falls back silently:

```
try:
    result = validate(data)
except ValueError as e:
    log("failed: %s" % e)
    return default_value   # silent
```

**Monadic validation** keeps the failure as a value:

```
result = (check_not_empty(data)
          .bind(check_numeric)
          .bind(check_range(0, 100))
          .bind(check_normality))
# Right(clean) | Left(Error(step, rule, input))
```

**Writer** lets a cleaning step return its value and its log together, and composition merges the logs: `Writer(df_clean, {"rows_removed": n, "reason": "negative_age"})`.

**List** replaces nested candidate loops with a flat chain — bind over bin counts, then over estimator methods, and filter the flattened scores.

Key-point callout (red accent): **Common thread:** each step writes only its own logic and returns a wrapped value. No step opens the wrapper of the step before it.

### Visualization (canvas `c5`, 720×300)

Two horizontal chains comparing monadic bind-threading with manual unwrap/check/re-wrap.

- **Title (bold 14px `#1a5276`, top center):** "bind Threads the Value Without Opening the Wrapper".
- **Steps (both chains, 96×28 boxes, 60px gaps, starting x=60):** raw, clean, scaled, scored; connecting ops: clean, scale, score.
- **Monadic band (top, y=74, height 60):** background band fill `rgba(26,82,118,0.08)` with 1px `#1a5276` outline spanning the whole chain, labeled bold 10px `#1a5276` above: "Result[...] context — held for the whole chain". Step boxes green (stroke `#27ae60`, fill `rgba(39,174,96,0.10)`) connected by blue `#1a5276` arrows with 9px blue labels ">>= clean", ">>= scale", ">>= score".
- **Manual chain (bottom, y=198):** labeled bold 10px red `#e74c3c` above: "Manual: unwrap, check, re-wrap between every step". Step boxes orange (stroke `#e67e22`, fill `rgba(230,126,34,0.08)`); between each pair, two red arrows form an up-and-over detour peak labeled 9px red "if err: return".
- **Caption (11px gray `#888`, bottom center):** "Same computation — the plumbing moves out of the steps and into bind".

## 6. Not Just Haskell — Railway Oriented Programming

Scott Wlaschin's framing is the same structure with an accessible metaphor: two parallel tracks, a success track and a failure track. Each function is a switch that either stays on the success track or diverts to the failure track, and composition connects the switches end to end. Errors are values riding the failure track to the end — no exception unwinding.

**The pattern is mainstream, under other names:**

- **Rust** — `Result<T, E>` and `Option<T>`, with `?` as bind sugar
- **Scala** — `Option`, `Either`, `Try`; for-comprehensions are bind syntax
- **Python** — the `returns` library, or a small hand-rolled `Result` dataclass
- **Java** — `Optional.flatMap`, `Stream.flatMap`
- **TypeScript** — `fp-ts`, `Effect`

Key-point callout (red accent): **Practical read:** you can adopt the mechanics with Result types and railway vocabulary and skip the category theory. The laws still have to hold for the refactoring guarantees to survive.

### Visualization (canvas `c6`, 720×300)

Railway diagram: two parallel tracks with four switch boxes and divert curves.

- **Title (bold 14px `#1a5276`, top center):** "Two Tracks, One Composition — Errors Are Values".
- **Tracks:** solid 2px horizontal lines from x=40 to x=canvas−40 — green `#27ae60` success track at y=106, red `#e74c3c` failure track at y=208. 10px labels: "success track" (green) above, "failure track" (red) below.
- **Switches:** four 88×32 boxes centered on the success track at x=150, 290, 430, 570 labeled "switch 1"–"switch 4" (stroke `#1a5276`, fill `rgba(26,82,118,0.06)`). From each switch a dashed (4/3) red bezier curve drops to the failure track, labeled 9px red "divert".
- **Outputs (bold 10px, right-aligned at the track ends):** "Ok(value)" in green above the success track; "Err(context)" in red above the failure track.
- **Footnote (10px gray `#888`, centered):** "Rust ?   ·   Scala for-comprehension   ·   Python returns.bind   ·   Java Optional.flatMap   ·   TS Effect".
- **Caption (11px gray `#888`, bottom center):** "Once on the failure track a value passes straight through — no exception unwinding".

## 7. Connection to statsml

The statsml pipeline already uses these shapes implicitly — naming them is what buys composition:

- **Multi-candidate validation** (several normality tests, several bin sizes) is the List pattern
- **A feature that cannot be classified** is Maybe/Option
- **Precondition checks that carry the reason for failure** are Either
- **Profiling metadata accumulated alongside the computation** is Writer
- **Data loading and artifact writes** are the IO edge

Key-point callout (red accent): **What making it explicit gives:** composition for free, one error shape across all precondition checks, and a pure core that can be tested without mocking a reader.

Questions callout (orange accent), full-width below the 2-col row: **Open discussion points:**

- Is explicit monadic composition worth it in Python, or does the ceremony cost more than the error-handling spaghetti it removes?
- How does it coexist with pandas/numpy, where `NaN` propagation is a Maybe that violates the laws?
- Can we take the mechanics (Result types, railway vocabulary) and leave the Haskell terminology out of the API?
- Overhead: wrapping every value in a container versus letting `NaN` propagate natively through vectorized code.
- Where should the IO boundary sit — at the reader, or at the whole profiling run?

### Visualization (canvas `c7`, 720×300)

Two-column mapping diagram: statsml concerns paired with the monad that names them.

- **Title (bold 14px `#1a5276`, top center):** "statsml Concerns → The Monad That Names Them".
- **Column headings (bold 10px `#1a5276`, centered at y=48):** "implicit today" over the left column, "explicit name" over the right.
- **Pairs (five rows, 42px pitch starting y=60; left boxes 216×28 at x=34, orange stroke `#e67e22`, fill `rgba(230,126,34,0.07)`; right boxes 130×28 at x=470, green stroke `#27ae60`, fill `rgba(39,174,96,0.10)`; dashed gray `#999` arrows between):**
  1. "multi-candidate search" → "List"
  2. "feature not classifiable" → "Maybe / Option"
  3. "precondition + reason" → "Either / Result"
  4. "profiling metadata" → "Writer"
  5. "reads, writes, clock" → "IO"
- **Caption (11px gray `#888`, bottom center):** "Gain: free composition, one error shape, a testable pure core. Cost: wrapper overhead vs native NaN propagation".

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col text/viz layout). Page: h1, `.subtitle` paragraph, then one `.card-section` per numbered topic. Each `.card-section` has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one `<tr>`: left `td.text-col` (45%) with paragraphs, `<ul>` bullets, `<pre><code>` blocks, `.key-point`/`.questions` callouts and `.example` lines; right `td.viz-col` (55%) with the canvas. Section 4 additionally has a full-width `table.compare` below its layout table; section 7 has a full-width `.questions` callout (with a `<ul>`) below its layout table. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `strong` in `#1a5276`; lists 0.92rem.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.questions` — same but left border `3px solid #e67e22`, padding 10px 14px. `.example` — italic, `#555`, 0.9rem.
- **Code:** inline `code` — background `#f8f9fa`, border `1px solid #e0e0e0`, padding 1px 4px, radius 3px, 0.82rem, color `#1a5276`. `pre` blocks — background `#f8f9fa`, border `1px solid #e0e0e0` with left border `3px solid #1a5276`, padding 8px 10px, 0.78rem, line-height 1.45.
- **Compare table:** `table.compare` full-width, 0.88rem; `th` background `#f8f9fa`, color `#1a5276`, left-aligned, `2px solid #2980b9` bottom border; `td` `1px solid #e0e0e0` bottom border, padding 8px 10px, top-aligned.
- **Canvas:** intrinsic 720×300 per chart, CSS `width: 100%`, border `1px solid #e0e0e0` radius 4px; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; shared `arrow` and `box` drawing helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, grays `#666`/`#888`/`#999`/`#ccc`.
- Detail pages have no nav bar and no back/home links; any card links in regenerated HTML grids use `.html` extensions.
