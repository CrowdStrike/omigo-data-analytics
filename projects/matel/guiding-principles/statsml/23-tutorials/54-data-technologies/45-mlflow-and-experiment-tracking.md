# MLflow & Experiment Tracking

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** MLflow & Experiment Tracking

**Subtitle:** MLflow records what data, code, and parameters produced each model — the lab notebook ML forgot, so "model_final_v3.pkl" stops being a mystery

## Which Data Made model_final_v3.pkl?

**Tags:** `core idea` (blue), `reproducibility` (green), `MLflow` (orange)

- **The folder** — a churn-model team's shared drive holds `model_final.pkl`, `_v2`, `_v3`, `_v3_REAL`
- **The question** — nobody can say which data snapshot, code, or parameters made any of them
- **The notebook** — chemists log every experiment; ML teams often log nothing but a filename
- **The tool** — MLflow (open-sourced by Databricks) logs params, metrics, and artifacts per run
- **The run** — one training execution = one run, with an ID that ties inputs to the output model

*Example (italic):* Asked "why does v3 beat v2?", the team can only shrug — the runs that built them were never recorded anywhere.

**Key point:** An experiment tracker turns each training run into a durable record — parameters in, metrics out, model artifact attached — so any model file can be traced back to exactly what produced it.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: filename versioning (dead end) vs a tracked run (traceable), shown as boxes flowing left to right.

- **Title (bold 15px, `#1a5276`, top center):** "A Filename Answers Nothing; a Run Record Answers Everything".
- **Row 1 (y=95), label 12px `#444` at x=20:** "filename versioning"; blue `#2a78d6` rounded box at x=170 labeled "model_final_v3.pkl" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "which data? which params?" with bold 12px red "✗ untraceable".
- **Row 2 (y=205), label:** "tracked run"; blue box at x=170 labeled "run 27 (churn-tuning)", 3px arrow to a green `#008300` box at x=360 labeled "params + AUC + artifact", then arrow to a green box at x=565 labeled "reproducible" with bold 12px green "✓".
- **Box style:** 150–175px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the model file is the last step of a run, not the record of it".
- **Caption (12px `#444`, bottom right):** "run contents illustrative".

## Forty Churn Runs, One Table

**Tags:** `worked example` (blue), `params and metrics` (green)

- **The sweep** — the team tunes a churn model over 40 runs, each logging its params and test AUC
- **The params** — every run records learning rate, max depth, and tree count before training starts
- **The baseline** — run 1 (lr=0.10, depth=3, 100 trees) scores AUC 0.781
- **The winner** — run 27 (lr=0.05, depth=6, 400 trees) scores AUC 0.843, the best of the sweep
- **The compare** — MLflow's UI sorts all 40 runs by AUC in one table; no spreadsheet copy-paste
- **Hand-check** — the gain from run 1 to run 27 is 0.843 − 0.781 = 0.062 AUC (exact arithmetic)

*Example (italic):* Sorting the runs table by AUC puts run 27 on top; clicking it shows lr=0.05, depth=6, 400 trees, and the exact model artifact it saved.

**Key point:** Logging one line of params and one metric per run costs seconds; it turns a 40-run sweep from a pile of files into a sortable table where the best configuration is one click away.

### Visualization (canvas `c2`, 720×300)

Scatter plot of the 40-run sweep: x = run number, y = test AUC, best run highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "40 Logged Runs: the Best One Is Findable in Seconds".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = run number 1 to 40, 12px `#444` tick labels at 1/10/20/30/40; y = AUC 0.75 to 0.86, gridlines `#e5e9ef` at 0.78/0.81/0.84 with 12px `#444` labels.
- **Points:** 4px-radius blue `#2a78d6` filled circles at runs 1–40 with hardcoded AUC values `[0.781, 0.769, 0.774, 0.790, 0.785, 0.772, 0.796, 0.788, 0.801, 0.779, 0.804, 0.793, 0.810, 0.798, 0.806, 0.815, 0.802, 0.809, 0.818, 0.795, 0.821, 0.812, 0.808, 0.825, 0.817, 0.829, 0.843, 0.826, 0.820, 0.831, 0.824, 0.833, 0.819, 0.828, 0.836, 0.822, 0.830, 0.838, 0.827, 0.834]`.
- **Best run:** run 27 (AUC 0.843) drawn as a 7px green `#008300` circle with 2px ring.
- **Baseline marker:** dashed `#6b7280` (dash 4/3) horizontal line at AUC 0.781, 12px `#6b7280` label "run 1 baseline 0.781" at its left end.
- **Annotation (bold 13px green `#008300`, near run 27, above the point):** "run 27: AUC 0.843 — lr=0.05, depth=6, 400 trees".
- **Caption (12px `#444`, bottom right):** "AUC values illustrative".

## Six Months Later, Someone Asks Why

**Tags:** `where it's used` (blue), `model registry` (green), `audit` (orange)

- **The ask** — six months on, a stakeholder asks why production predicts the way it does
- **The registry** — MLflow's Model Registry holds named, versioned models with lifecycle stages
- **The stages** — a version moves through Staging to Production; superseded ones are Archived
- **The trace** — the Production version links back to run 27: its params, AUC 0.843, data path, code
- **The rerun** — with the run's logged inputs, the team retrains and gets the same model back
- **The field** — Weights & Biases and similar tools track the same things; the space is crowded

*Example (italic):* The audit takes ten minutes: registry shows churn-model v3 in Production, v3 links to run 27, run 27 lists every input — no archaeology through old laptops.

**Key point:** Tracking pays off long after training day — regulated reviews, debugging drift, and handovers all reduce to "open the run that built the production model".

### Visualization (canvas `c3`, 720×300)

Registry lifeline diagram: four versions of the churn model as boxes on a horizontal line, each labeled with its stage and origin run.

- **Title (bold 15px, `#1a5276`, top center):** "Model Registry: churn-model, Four Versions, One in Production".
- **Lifeline:** horizontal 2px `#999` line at y=160 from x=60 to x=680, small arrowhead at the right end, 12px `#6b7280` label "time →" under the right end.
- **Version boxes (130px wide, 52px tall, 8px radius, centered on the line at x = 125, 285, 445, 605):**
  - "v1 — Archived": fill `rgba(107,114,128,0.15)`, 2px `#6b7280` border, 11px `#444` sub-label "run 4 · AUC 0.790"
  - "v2 — Archived": same gray style, sub-label "run 13 · AUC 0.810"
  - "v3 — Production": fill `rgba(0,131,0,0.15)`, 2px `#008300` border, bold text, sub-label "run 27 · AUC 0.843"
  - "v4 — Staging": fill `rgba(201,133,0,0.15)`, 2px `#c98500` border, sub-label "run 38 · AUC 0.838"
- **Text:** 12px `#2c3e50` version+stage on the first line, 11px sub-label on the second.
- **Annotation (bold 13px green `#008300`, above v3 near y=80):** "Production traces to run 27 — params, data, code".
- **Caption (12px `#444`, bottom right):** "versions and AUCs illustrative".

## Installing MLflow Is Not the Discipline

**Tags:** `common mistake` (red), `half-logged runs` (orange)

- **The trap** — teams install the tracker, log AUC, and still can't reproduce anything
- **What's missing** — the data snapshot and code commit are the inputs people forget to log
- **The test** — a run is reproducible only if code, data, params, and environment are all pinned
- **The habit** — logging must run inside the training script, not by hand after "the good runs"
- **The tool-swap** — moving to a fancier tracker fixes nothing if the same fields stay empty

*Example (italic):* Run 27's page shows AUC 0.843 and lr=0.05 but no data version — the table was overwritten in July, so the "fully tracked" best run is unreproducible anyway.

**Common mistake:** Treating the tool as the practice. A tracker only stores what you send it — a run missing its data version or code commit is a prettier filename, not an experiment record.

### Visualization (canvas `c4`, 720×300)

Checklist grid: five reproducibility ingredients as rows, two columns ("metrics-only habit" vs "full logging"), filled with check/cross cells.

- **Title (bold 15px, `#1a5276`, top center):** "Reproducible = All Five Logged, Every Run".
- **Layout:** row labels 12px `#444` left-aligned at x=30, rows at y = 85, 120, 155, 190, 225: "params", "metrics (AUC)", "code commit", "data version", "environment"; column headers bold 12px at y=60 over x=390 ("metrics-only habit", `#e74c3c`) and x=580 ("full logging", `#008300`).
- **Cells:** 26px squares centered on the column x positions, 6px radius; logged = fill `rgba(0,131,0,0.15)` with bold 14px green `#008300` "✓"; missing = fill `rgba(231,76,60,0.12)` with bold 14px red `#e74c3c` "✗".
- **Cell values:** metrics-only column ✓ ✓ ✗ ✗ ✗ (top to bottom); full-logging column ✓ ✓ ✓ ✓ ✓.
- **Verdict row (y=262):** bold 12px red "✗ not reproducible" under the first column, bold 12px green "✓ rerunnable months later" under the second.
- **Annotation (bold 13px magenta `#d55181`, left side near y=262):** "the discipline matters more than the tool".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 40 AUC values, run numbers, params, and registry versions are invented and labeled illustrative; the run-1-to-run-27 gain 0.843 − 0.781 = 0.062 is exact arithmetic on those illustrative values. MLflow facts (Databricks origin; params/metrics/artifacts per run; runs-comparison UI; Model Registry stages; Weights & Biases as an alternative) are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
