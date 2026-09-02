# Functional Programming & Immutability in Data Systems

**Page type:** detail page (backlog-style 2-column layout: `.card-section` per topic, each with a text column ~45% left and a canvas column ~55% right)
**HTML title tag:** Functional Programming & Immutability in Data Systems

**Subtitle:** Design discussion — pure functions, immutable data, and composition prevent whole classes of pipeline bugs. They also cost memory and allocation churn, so the question is where to spend them, not whether to adopt them everywhere.

## 1. Why FP Fits Data Pipelines Naturally

**A pipeline IS function composition.** `raw → clean → transform → model → output` — each stage takes a DataFrame and returns a DataFrame with no side effects.

- Composition means stages can be reordered, parallelized, or skipped without hunting for hidden state.
- Referential transparency means any intermediate result is safe to cache.
- This is not FP imposed on data — pipelines are already functional in shape; imperative style fights that shape.

**What imperative style gets wrong:**

- **Mutation** — "did step 3 modify what step 2 produced?" is invisible coupling between stages.
- **Shared state** — global config, connections, caches: change one and everything downstream shifts.
- **Ordering dependence** — swapping two steps changes results for reasons nothing in the code shows.
- **Testing** — you must mock the world to exercise one function.
- **Debugging** — "what was the state at step 5?" requires reproducing every prior step.

Key-point callout (red accent): **The core difference:** in the functional shape, the arrows in the diagram are the only channels between stages. In the imperative shape, there are extra arrows you cannot see in the code.

### Visualization (canvas `c1`, 720×300)

Two stage-chain rows contrasting functional composition with imperative hidden-state coupling.

- **Title (bold 14px `#1a5276`, top center):** "Composition vs Hidden State Coupling".
- **Stages (both rows, five 100×34 boxes, 40px gaps, starting x=30):** raw, clean, transform, model, output.
- **Functional row (y=54), header 11px green `#27ae60`:** "Functional: the only channel between stages is the arrow". Boxes fill `rgba(39,174,96,0.10)`, stroke `#27ae60`, connected by green arrows; 10px gray `#888` note below: "any intermediate result is safe to cache / reorder / retry".
- **Imperative row (y=150), header 11px red `#e74c3c`:** "Imperative: extra channels the code does not show". Boxes fill `rgba(231,76,60,0.08)`, stroke `#e74c3c`, connected by gray `#999` arrows.
- **Shared state box:** centered 200×30 box at y=232, fill `rgba(231,76,60,0.15)`, stroke `#e74c3c`, bold 11px red label "shared mutable state"; dashed (3/3) thin red lines from every imperative stage down to it.
- **Caption (11px gray `#888`, bottom center):** "Same five stages — the invisible edges are what make order and caching unsafe".

## 2. Immutability in Practice: Code and Storage

**Code level.** Don't mutate DataFrames — use `.assign()` instead of direct column assignment. Don't update rows in place; filter and build a new frame. Don't share mutable state between stages; pass snapshots. Avoid module-level variables that change between calls.

**Storage level.**

- **Write-once, read-many** — a Parquet file is immutable once written.
- **Append-only tables** — Delta Lake, Iceberg: a new version is an append; old versions remain queryable.
- **Event sourcing** — every change is an appended event; current state is the replay.
- **Slowly changing dimensions** — don't UPDATE a row, add a new timestamped version.
- **Time travel** — Delta Lake, LakeFS: query any historical version directly.

Key-point callout (red accent): **The pandas trap:** `df['new_col'] = ...` mutates in place, and any other reference to that frame sees the change. `df = df.assign(new_col=...)` makes the new state explicit and leaves the old one intact.

Example line (italic): Storage anti-pattern: "overwrite yesterday's table with today's results" — the same aliasing bug one layer down. No audit, no regression debugging, no replay.

### Visualization (canvas `c2`, 720×300)

Split diagram: aliased in-place mutation on the left vs copy-on-write versions on the right, divided by a vertical `#ccc` line at center.

- **Title (bold 14px `#1a5276`, top center):** "In-Place Mutation vs Copy-on-Write".
- **Left half, heading bold 11px red `#e74c3c`:** "df['col'] = ...   (mutate in place)". Two white reference boxes "ref A" and "ref B" (90×26, red outline) with red arrows pointing to one shared 3-cell buffer at y=150 holding values `3, 99, 2` (cells 46px wide, fill `rgba(26,82,118,0.35)`, mutated cell `99` highlighted `rgba(231,76,60,0.25)`, white 11px value text). Notes below (centered 10px): red "A writes 99 → B silently reads 99"; gray `#888` "one buffer, two owners, no record of 7" and "old value 7 is unrecoverable".
- **Right half, heading bold 11px green `#27ae60`:** "df = df.assign(...)   (copy-on-write)". Reference boxes "ref A" and "ref B" (green outline); ref A points (blue arrow) to buffer v1 `3, 7, 2` (blue `#1a5276` outline) labeled 10px blue "v1 (unchanged)"; ref B points (green arrow) to buffer v2 `3, 99, 2` (green outline) labeled 10px green "v2 (new)".
- **Caption (11px gray `#888`, bottom center):** "Two versions cost more memory than one — that is the price of losing the aliasing bug".

## 3. What Purity Buys You

**A function is pure when** it depends only on its arguments (not globals, clocks, or remote calls), produces only its return value, is deterministic, and has no observable side effects.

**What you get:**

- **Testability** — known input, asserted output, no mocking.
- **Memoization** — the result is a function of the input, so caching is always sound.
- **Parallelization** — no shared state to corrupt, so no locks to get wrong.
- **Retry safety** — a failed step can be re-run without half-applied effects.
- **Refactoring** — any expression can be replaced by its value.

Key-point callout (red accent): **These properties are not independent.** Cacheability, retry safety, and lock-free parallelism all follow from the same fact: the stage reads nothing and writes nothing outside its arguments. Lose purity at one stage and you lose all three there.

Example line (italic): Note the asymmetry: an IO read is usually retry-safe but never cacheable; a non-atomic write is neither.

### Visualization (canvas `c3`, 720×300)

Checkmark matrix: five pipeline calls scored against four properties.

- **Title (bold 14px `#1a5276`, top center):** "Purity per Stage → What Is Safe to Do".
- **Column headers (10px `#666`, centered at y=52, at x=300/400/510/630):** Pure, Cacheable, Retryable, Parallel-safe; header rule `#ccc` at y=60, light `#f0f0f0` rules under each row.
- **Rows (11px, 38px pitch starting y=84; row label green `#27ae60` if pure else red `#e74c3c`; cells bold 13px ✓ green / ✗ red):**
  1. `read_parquet(path)` — ✗ ✗ ✓ ✓
  2. `remove_nulls(df)` — ✓ ✓ ✓ ✓
  3. `engineer_features(df, cfg)` — ✓ ✓ ✓ ✓
  4. `append_to_metrics_db(m)` — ✗ ✗ ✗ ✗
  5. `write_parquet(df, path)` — ✗ ✗ ✗ ✗
- **Caption (11px gray `#888`, bottom center):** "The three right-hand columns follow from the first — an impure stage forfeits all of them".

## 4. Functional Core, Imperative Shell

**Push impurity to the edges** and keep the middle pure. If a transform reads thresholds from a database, it is not pure — lift that read to the boundary and pass the values in.

```
# Pure: depends only on its argument
def age_bucket(age, cuts):
    if age < cuts[0]: return "young"
    if age < cuts[1]: return "middle"
    return "senior"

# Impure: hidden dependency on external state
def age_bucket_v2(age):
    cuts = db.get("age_thresholds")   # side effect
    ...
```

```
# Impure shell: IO in
config = load_config("pipeline.yaml")
raw    = read_parquet("input.parquet")

# Pure core: all transforms are functions
clean    = remove_nulls(raw)
features = engineer_features(clean, config)
results  = validate(features, config)

# Impure shell: IO out
write_parquet(results, "output.parquet")
```

Key-point callout (red accent): **Why the shape matters:** the pure core is where tests, caches, and parallelism live. The thinner the shell, the more of the pipeline gets those properties for free.

### Visualization (canvas `c4`, 720×300)

Nested-box diagram: pure core inside an imperative shell, with inbound/outbound IO arrows.

- **Title (bold 14px `#1a5276`, top center):** "Functional Core, Imperative Shell".
- **Shell:** large box (x=60, y=46, width = canvas − 120, height 200), fill `rgba(230,126,34,0.07)`, 2px stroke `#e67e22`, bold 11px orange label top-left: "IMPERATIVE SHELL — IO, clocks, connections".
- **Core:** inner box (inset 110px left/right, 40px top, height = shell − 74), fill `rgba(39,174,96,0.10)`, 2px stroke `#27ae60`, bold 11px green centered label "PURE CORE"; inside, 11px `#2c3e50` centered lines "remove_nulls(...)", "engineer_features(...)", "validate(...)"; 10px green footer "testable · cacheable · parallel".
- **Inbound IO (10px orange labels left inside the shell):** "load_config()", "read_parquet()" with orange arrows into the core.
- **Outbound IO (right):** "write_parquet()", "log_metrics()" with orange arrows from the core outward.
- **Caption (11px gray `#888`, bottom center):** "Thin shell, wide core — impurity is contained rather than eliminated".

## 5. Composition Patterns

**Pipe / chain.** Each step is `DataFrame → DataFrame`, so steps can be reordered, dropped, or tested alone.

```
result = (df
    .pipe(remove_outliers)
    .pipe(normalize_numeric)
    .pipe(encode_categorical)
    .pipe(validate_schema))
```

**Higher-order functions.** Pass the strategy in as a value instead of branching on a string — one skeleton, many behaviours.

```
def apply_normalization(df, strategy):
    return df.assign(**{
        col: strategy(df[col])
        for col in numeric_columns(df)})

result_z  = apply_normalization(df, z_score)
result_mm = apply_normalization(df, min_max)
result_r  = apply_normalization(df, robust_scale)
```

Key-point callout (red accent): **Functions as values:** pass them, return them, compose them. The alternative — an if/else tree over strategy names — grows quadratically and cannot be extended without editing the skeleton.

Below the 2-col row (full width): **Map / filter / reduce over collections** — replace loops that carry a mutable accumulator:

```
# Imperative: mutable accumulator, leaking loop variable
results = []
for feature in features:
    if is_numeric(feature):
        results.append(profile(feature))

# Functional: intent is visible in the structure
results = [profile(f) for f in features if is_numeric(f)]
results = list(map(profile, filter(is_numeric, features)))
```

### Visualization (canvas `c5`, 720×300)

Fan-in/fan-out diagram: three strategy chips feeding one skeleton function producing three outputs.

- **Title (bold 14px `#1a5276`, top center):** "One Skeleton, Swappable Strategies".
- **Strategy chips (left, 130×32 at x=30, y=76/140/204, fill `rgba(26,82,118,0.35)`, stroke `#1a5276`, white 11px labels):** z_score, min_max, robust_scale; 10px `#666` caption above: "strategy = a function value".
- **Skeleton box (center, 220×160 at x=240, y=76, fill `rgba(39,174,96,0.10)`, 2px stroke `#27ae60`):** bold 11px green lines "apply_normalization(" / "df, strategy)"; 10px `#555` lines "pure · no branching" / "written once".
- **Output chips (right, 150×32 at x=540, same y rows, fill `rgba(39,174,96,0.12)`, stroke `#27ae60`, 11px `#2c3e50` labels):** result_z, result_mm, result_r; 10px `#666` caption above: "new frames, input untouched".
- **Arrows:** blue `#1a5276` from each strategy chip into the skeleton; green `#27ae60` from the skeleton to each output chip.
- **Caption (11px gray `#888`, bottom center):** "Each call allocates a fresh frame — convenient, and not free at scale".

## 6. Where Pure FP Breaks Down — The Honest Cost

**Immutability is not free.** Copying costs memory, allocator pressure, and cache locality. On a hundred-million-row frame, a naive copy per stage is the difference between fitting in RAM and not.

- **Performance** — full copies per stage multiply peak memory; you need copy-on-write, structural sharing, or laziness to make it viable.
- **Integration** — databases, APIs, and file systems are stateful by nature and cannot be made pure.
- **Debugging** — long composition chains are hard to inspect mid-flow.
- **Ecosystem** — pandas, numpy, and scikit-learn assume mutation; fighting them is expensive.
- **Team familiarity** — most data engineers think imperatively; FP has a real learning curve.

Philosophy callout (blue accent): **The pragmatic middle:** pure core transforms, IO at the boundaries, state passed explicitly, mutation isolated and documented. Use immutability where it is cheap — configs, metadata, small frames. Accept controlled mutation where the numbers demand it. The requirement is knowing which is which, not purity for its own sake.

### Visualization (canvas `c6`, 720×300)

Grouped bar chart of peak memory and allocation churn relative to in-place mutation across five approaches.

- **Title (bold 14px `#1a5276`, top center):** "The Cost Side: Memory and Allocation vs In-Place".
- **Data (approach: peak memory ×, allocation churn ×):** Mutate in place 1.0 / 1.0; Full copy per stage 5.0 / 5.0; Copy-on-write 1.4 / 1.3; Structural sharing 1.7 / 2.2; Lazy graph 1.1 / 0.8.
- **Axes:** plot area x=70, y=56, width = canvas − 130, height 180; gray `#ccc` L-axes; `#f0f0f0` gridlines with 9px `#999` labels 0x–5x (max 5.0); rotated 11px `#666` y label "relative to in-place (1x)"; two-line 9px `#2c3e50` category labels beneath each group.
- **Bars:** paired per group (bar width 28% of group): peak memory fill `rgba(26,82,118,0.35)` stroke `#1a5276`; allocation churn fill `rgba(230,126,34,0.45)` stroke `#e67e22`.
- **Legend (top right):** blue swatch "peak memory"; orange swatch "allocation churn".
- **Caption (11px gray `#888`, bottom center):** "Illustrative magnitudes — naive copying is the expensive option, not immutability itself".

## 7. Immutability at Scale: Structural Sharing

**Large systems already chose immutability** — they just avoid the naive copy. A new version reuses every untouched node and copies only the path to the change.

- **Arrow / Polars** — copy-on-write semantics: looks immutable, shares buffers until a write forces a copy.
- **Spark RDDs** — immutable by design; transformations produce new RDDs and lineage records how to rebuild them.
- **Dask** — lazy graph: nothing materializes until `.compute()`, and then only what is needed.
- **DuckDB** — columnar and vectorized, operating on immutable batches internally.

Key-point callout (red accent): **Still not free:** sharing turns bulk copying into pointer chasing. You trade memory-bandwidth cost for indirection and allocator traffic, and retained old versions keep their nodes alive until nothing references them.

Example line (italic): The small-scale Python script is what fights immutability; the distributed engines settled the argument years ago.

### Visualization (canvas `c7`, 720×300)

Persistent-tree diagram: version v2 sharing untouched nodes of v1 and copying only the path to the change.

- **Title (bold 14px `#1a5276`, top center):** "Structural Sharing: New Version, Copied Path Only".
- **Nodes:** circles radius 17, 10px `#2c3e50` labels. Blue nodes (stroke `#1a5276`, fill `rgba(26,82,118,0.35)`): v1 root at (165, 62); internal A (95, 150), B (265, 150); leaves a1 (55, 238), a2 (135, 238), b1 (225, 238), b2 (305, 238). Orange nodes (stroke `#e67e22`, fill `rgba(230,126,34,0.35)`): v2 root at (400, 62), B' (420, 150), b2' (430, 238).
- **Links:** solid blue: v1→A, v1→B, A→a1, A→a2, B→b1, B→b2. Orange: v2→A (dashed — pointer into old structure), v2→B' (solid), B'→b1 (dashed), B'→b2' (solid). Dash pattern 4/3.
- **Right-hand legend (x=490):** 11px orange "copied: root + path to change"; 11px blue "shared: every untouched node"; 10px `#555` "dashed = pointer into the" / "old structure, no copy"; 10px green `#27ae60` "v1 still valid → time travel"; 10px red `#e74c3c` "but v1 keeps its nodes alive" / "and reads chase pointers".
- **Caption (11px gray `#888`, bottom center):** "Copy cost scales with the depth of the change, not the size of the data".

## 8. Event Sourcing for Data

**Every mutation becomes an append.** Current state is a fold over the log, so state at any past point is a shorter fold. Nothing is lost; the audit trail is the data model.

- **Overkill for** exploratory analysis, one-off scripts, prototype pipelines.
- **Essential for** production ML under regulatory audit, financial data, and any dataset that receives corrections (medical records, restatements).
- **Middle ground** — snapshot meaningful checkpoints rather than every micro-operation.
- **Delta Lake / Iceberg** — event sourcing for tables, already productionized.

Key-point callout (red accent): **The trade:** destructive updates keep storage flat and lose history permanently. An append-only log grows without bound and needs compaction plus periodic snapshots, or replay cost creeps up with age.

Example line (italic): Debugging value: replay events up to the point where the number went wrong, then inspect — instead of guessing what the row used to hold.

### Visualization (canvas `c8`, 720×300)

Two-band diagram: an append-only event log folding into state (top) vs destructive updates (bottom), divided by a `#ccc` line at y=148.

- **Title (bold 14px `#1a5276`, top center):** "Append-Only Log vs Destructive Update".
- **Top band, header 11px green `#27ae60`:** "Append-only: state = fold(apply, events)". Six event boxes (66×32 starting x=34 at y=58, fill `rgba(39,174,96,0.10)`, stroke `#27ae60`, 11px labels): "+100", "−30", "+25", "−15", "+40", "−5", each tagged 9px `#999` "e1"…"e6" below; blue arrow to a result box (90×32, fill `rgba(26,82,118,0.35)`, stroke `#1a5276`) with bold white label "bal = 115". 10px green note below: "fold e1..e3 → bal = 95 : any past state is a shorter fold".
- **Bottom band, header 11px red `#e74c3c`:** "Destructive: UPDATE row SET bal = ...". Three cells (110×34 starting x=34 at y=186, 50px gaps): "bal = 100" and "bal = 70" — dead: fill `rgba(231,76,60,0.08)`, red stroke, red X drawn across, red arrow to the next, 9px red caption "overwritten, gone"; "bal = 115" — alive: fill `rgba(26,82,118,0.35)`, blue stroke, white text, 9px blue caption "only surviving state".
- **Caption (11px gray `#888`, bottom center):** "The log answers \"what did it look like then?\" — and grows until you compact and snapshot".

## 9. Open Questions: Where to Spend Immutability

**The decision is per-dataset, not per-codebase.** Weigh the cost of copying against the value of retained history.

- How much immutability is practical at scale, given that Arrow/Polars offer copy-on-write and Spark RDDs are immutable outright?
- Should the statsml pipeline enforce pure transforms — and how would purity actually be verified rather than asserted?
- Where exactly does the functional-core / imperative-shell boundary fall in a profiling pipeline?
- Is event sourcing overkill or essential for our datasets, and at what granularity?
- How do we get the benefits in Python without alienating engineers who think in pandas?

Questions callout (orange accent): **Working rule:** cheap to copy → immutable by default, no debate. Expensive to copy but historically valuable → version the storage and share structure. Expensive and disposable → mutate deliberately, in one isolated place, with the reason written down.

### Visualization (canvas `c9`, 720×300)

Quadrant scatter plot: copy cost (x) vs value of history (y) with per-dataset placement.

- **Title (bold 14px `#1a5276`, top center):** "Where to Spend Immutability: Copy Cost vs History Value".
- **Plot:** bordered `#ccc` rectangle (x=70, y=46, width = canvas − 110, height 200) with dashed (4/4) `#ccc` mid-line dividers into quadrants. Axis labels 11px `#666`: "copy cost (data size) →" below center; rotated "value of history →" on the left.
- **Quadrant guidance labels (10px, centered):** top-left green `#27ae60` "immutable by default"; top-right blue `#1a5276` "version the storage + share structure"; bottom-left orange `#e67e22` "immutable anyway — it is free"; bottom-right red `#e74c3c` "controlled, documented mutation".
- **Items (dots radius 5, 10px `#2c3e50` labels; coordinates as fractions of plot width/height, y from bottom):**
  - "configs / metadata" (0.12, 0.86) green.
  - "model artifacts" (0.30, 0.70) green.
  - "feature tables" (0.68, 0.80) blue.
  - "fact / event tables" (0.86, 0.62) blue.
  - "intermediate caches" (0.55, 0.22) red.
  - "100M-row arrays" (0.88, 0.14) red.
  - "small lookup frames" (0.14, 0.28) orange.
- **Caption (11px gray `#888`, bottom center):** "Decide per dataset — the goal is knowing which quadrant you are in, not purity everywhere".

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col text/viz layout). Page: h1, `.subtitle` paragraph, then one `.card-section` per numbered topic. Each `.card-section` has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one `<tr>`: left `td.text-col` (45%) with paragraphs, `<ul>` bullets, `<pre><code>` blocks, `.key-point`/`.questions`/`.philosophy` callouts and `.example` lines; right `td.viz-col` (55%) with the canvas. Section 5 additionally has a full-width paragraph plus `pre` code block below its layout table. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `strong` in `#1a5276`; lists 0.92rem.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.questions` — same but left border `3px solid #e67e22`. `.philosophy` — same but left border `3px solid #1a5276`. `.example` — italic, `#555`, 0.9rem.
- **Code:** inline `code` — background `#f8f9fa`, radius 3px, padding 1px 5px, 0.85em, color `#1a5276`. `pre` blocks — background `#f8f9fa`, left border `3px solid #1a5276`, padding 8px 10px, 0.78rem, line-height 1.45, inner code color `#2c3e50`.
- **Canvas:** intrinsic 720×300 per chart, CSS `width: 100%`, border `1px solid #e0e0e0` radius 4px; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; shared `arrow` drawing helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, grays `#666`/`#888`/`#999`/`#ccc`.
- Detail pages have no nav bar and no back/home links; any card links in regenerated HTML grids use `.html` extensions.
