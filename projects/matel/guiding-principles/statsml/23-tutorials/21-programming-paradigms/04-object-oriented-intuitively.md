# Object-Oriented, Intuitively

**Page type:** detail page (tutorial card-sections: h2 + two-column table.layout, text left 50%, canvas/code right 50%)
**HTML title tag:** Object-Oriented, Intuitively

**Subtitle:** Bundle the data and the functions that act on it into one thing — an object is a noun that carries its own skills

## A DataPipeline Is a Noun With Skills

**Tags:** `core idea` (blue pill), `running example` (green pill)

- **The bundle** — a `DataPipeline` holds its data (source file, rows) AND its methods
- **Nouns with skills** — the object is the noun; `load()`, `clean()`, `total()` are its skills
- **Self-contained** — you never pass the rows around; the object carries them inside
- **Dot means "ask it"** — `pipeline.clean()` reads as "pipeline, clean yourself"
- **Many at once** — two pipelines for two files never mix up whose rows are whose

*Example:* A rice cooker bundles the rice (data) with the cook button (method) — you don't hand rice to a separate cook() function.

**Key point:** Without objects, functions and data travel separately and you pass the right data to the right function every time; an object glues them so they can't drift apart.

Code payload (`.payload` monospace block below the canvas, verbatim):

```
class DataPipeline:
    def __init__(self, source):
        self.source = source     # data lives inside
        self.rows = None

    def load(self):              # skills live beside it
        self.rows = read_csv(self.source)

    def clean(self):
        self.rows = [r for r in self.rows
                     if r is not None and r > 0]

    def total(self):
        return sum(self.rows)
```

### Visualization (canvas `c1`, 720×300)

Split panel: loose data and functions with tangled arrows vs one object bundle with data and skills compartments.

- **Title (bold 15px, `#1a5276`, top center):** "Loose Pieces vs One Bundle".
- **Divider:** vertical dashed `#bdc3c7` (4/3) at x=340.
- **Left panel (center x=172), header bold orange `#d95926`:** "Without objects: you carry the data". Two data boxes 120×34 (fill `#f6f8fa`, stroke mute `#6b7280`): "rows (file A)", "rows (file B)". Three function boxes 110×34 (fill `#fdf0e6`, stroke orange, monospace orange text): "load(rows?)", "clean(rows?)", "total(rows?)". Tangled dashed mute lines crisscross between data and function boxes. Bold red `#e74c3c` caption: "which rows go to which call?"; mute caption: "every call is a chance to pass the wrong data".
- **Right panel (center x=528), header bold green `#008300`:** "With an object: the data rides inside". Outer bundle box 270×156 (fill `rgba(0,131,0,0.05)`, stroke green) titled in bold green monospace: `p = DataPipeline("sales.csv")`. Inside, two compartments: "data" box 115×106 (fill `#eef4fb`, stroke blue `#2a78d6`) listing monospace `source`, `rows`; "skills" box 110×106 (fill `#fdf0e6`, stroke orange) listing `load()`, `clean()`, `total()`. Bold green caption: "p.clean() — no arguments: it cleans its own rows"; bold violet `#4a3aa7` caption: "a second file? make a second object — nothing mixes".

## Three Method Calls, Traced by Hand

**Tags:** `worked example` (green pill)

- **Create** — `p = DataPipeline("sales.csv")`: rows is still empty
- **p.load()** — reads 8 amounts: [120, 80, -10, 150, None, 200, 90, 90]
- **p.clean()** — drops the -10 and the None, leaving 6 rows inside the object
- **p.total()** — 120 + 80 + 150 + 200 + 90 + 90 = 730
- **State persisted** — clean() changed what total() later saw, with no variable passed

*Example:* Between the calls, nothing was handed back and forth — the rows lived inside `p` the whole time.

**Key point:** Each method call changes or reads the object's internal state — that memory between calls is exactly what plain functions don't have.

### Visualization (canvas `c2`, 720×300)

State timeline: four boxes showing the object's contents after each call, connected by arrows.

- **Title (bold 15px, `#1a5276`):** "What Lives Inside p After Each Call".
- **Four boxes** (158×130, starting x=22, y=70, 18px gaps, fill `#f8f9fa`, last box `rgba(0,131,0,0.06)`), each with a bold monospace call label above and state text inside:
  1. `p = DataPipeline(...)` (mute `#6b7280`) — "rows = None"
  2. `p.load()` (blue `#2a78d6`) — "8 rows:" / monospace `[120, 80, -10, 150,` / `None, 200, 90, 90]`
  3. `p.clean()` (aqua `#199e70`) — "6 rows:" / `[120, 80, 150,` / `200, 90, 90]`
  4. `p.total()` (green `#008300`) — "returns 730" / "rows unchanged"
- **Arrows** between boxes, colored like the next stage.
- **Annotations below (centered):** bold red `#e74c3c` "clean() dropped -10 and None"; bold green 13px "120 + 80 + 150 + 200 + 90 + 90 = 730"; bold violet `#4a3aa7` "no rows were passed between calls — the object remembered them".

## Where You Already Use This: sklearn's fit and predict

**Tags:** `where it's used` (blue pill), `worked example` (green pill)

- **The object** — `model = LinearRegression()`: a noun with fit and predict skills
- **Tiny data** — X = [1, 2, 4], y = [7, 9, 13]: three points on the line y = 2x + 5
- **model.fit(X, y)** — learns slope 2.0 and intercept 5.0, stores them INSIDE the object
- **model.predict(3)** — reads the stored numbers: 2.0 × 3 + 5.0 = 11
- **The pattern** — every sklearn model, scaler, and encoder is this same fit/use bundle

*Example:* predict never asks for the training data — the coefficients it needs live in `model`, put there by fit.

**Key point:** A fitted model IS state between calls — fit writes the learned numbers into the object, predict reads them back out. That is object-orientation doing real work.

Code payload (`.payload` monospace block below the canvas, verbatim):

```
model = LinearRegression()      # empty object
model.fit([[1], [2], [4]], [7, 9, 13])
model.coef_        # 2.0  — stored inside
model.intercept_   # 5.0  — stored inside
model.predict([[3]])            # 2.0*3 + 5.0 = 11
```

### Visualization (canvas `c3`, 720×300)

Split panel: object state before/after fit (left) and a scatter plot with the fitted line and prediction (right).

- **Title (bold 15px, `#1a5276`):** "fit() Writes Into the Object, predict() Reads Out".
- **Divider:** vertical dashed `#bdc3c7` (4/3) at x=345.
- **Left panel:** "model before fit" (bold mute header) box 140×62 (fill `#f6f8fa`, stroke mute) with monospace lines `coef_      = ?` / `intercept_ = ?`; orange `#d95926` downward arrow labeled monospace `.fit(X, y)`; "model after fit" (bold green header) box (fill `rgba(0,131,0,0.07)`, stroke green `#008300`) with `coef_      = 2.0` / `intercept_ = 5.0`. To the right: bold orange monospace `.predict(3) → 11`, mute 11px "reads the stored 2.0, 5.0".
- **Right panel (plot area x=395, width 290, baseline y=240, height 170; x scale 0–5, y scale 0–16):** gray `#999` L-axes; green fitted line width 2.5 for y = 2x + 5 from x=0 to x=5; blue `#2a78d6` training dots (radius 6) at (1, 7), (2, 9), (4, 13), each labeled "(x, y)"; magenta `#d55181` predicted dot (radius 7) at (3, 11) with a magenta dashed drop line to the x-axis and bold label "predict(3) = 11".
- **Labels:** mute "x" under the axis; bold green "stored line: y = 2x + 5" at the top of the plot; bold blue "training points X=[1,2,4], y=[7,9,13]" below.

## Where Objects Are Overkill

**Tags:** `common mistake` (red pill), `trade-off` (orange pill)

- **The test** — does state need to persist between calls? No state, no class needed
- **Overkill** — a `Discounter` class wrapping one `apply()` method is just a function in a costume
- **One-off scripts** — a notebook that loads, plots, and exits gains nothing from classes
- **Good fits** — fitted models, database connections, a pipeline configured once, run often
- **Extra ceremony** — classes add init, self, and naming decisions; charge that cost only when state pays for it

*Example:* `def discount(p): return p * 0.9` beats a class holding no data — there is nothing to bundle.

**Key point:** Reach for an object when data and behavior genuinely belong together across many calls — not because "real programmers write classes".

### Visualization (canvas `c4`, 720×300)

Decision flow: one question box branching NO/YES into "plain function" and "object (class)" with three examples each.

- **Title (bold 15px, `#1a5276`):** "Function or Object? One Question Decides".
- **Decision box** (250×44 at center, fill `#eef4fb`, stroke blue `#2a78d6`): bold blue "does state persist between calls?".
- **NO branch (aqua `#199e70`):** elbow arrow left-down labeled bold "NO" to a box 210×40 (fill `rgba(25,158,112,0.08)`) "plain function"; three example lines below in dark text: "discount(price)", "parse one date string", "a one-off notebook step".
- **YES branch (violet `#4a3aa7`):** elbow arrow right-down labeled bold "YES" to a box 210×40 (fill `rgba(74,58,167,0.08)`) "object (class)"; examples: "fitted model: coef_ inside", "open database connection", "pipeline configured once".
- **Bottom captions (centered):** bold red `#e74c3c` "a class with one method and no data is a function in a costume"; mute "sklearn sits on the YES side: fit() writes state, predict() reads it".

## Regeneration instructions

- **Layout:** tutorial detail page — h1 + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%). Text column: `.tags` pill row, `<ul>` of one-line bullets each opening with `<b>` (bold term in `#1a5276`), an italic `.example` line, and a `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`). Viz column: one canvas per section; sections 1 and 3 also have a `.payload` `<pre>` under the canvas (background `#f8f9fa`, left border 3px solid `#1a5276`, monospace 0.78em).
- **Tag pills:** 0.72rem bold, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`. Inline `code` in monospace on `#f4f6f8`.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` underline; `.subtitle` `#666` 0.95rem. Canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
