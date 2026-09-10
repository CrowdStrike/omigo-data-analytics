# Genetic Algorithms

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Genetic Algorithms

**Subtitle:** Breed better answers instead of calculating them — keep the best candidates, mix pairs, tweak at random, and repeat until the scores stop climbing

## Breeding a Better Cookie Recipe

**Tags:** `core idea` (blue), `selection` (green), `evolution loop` (orange)

- **The bakery** — a shop tunes a cookie recipe with two knobs: grams of sugar and minutes in the oven
- **A recipe = a candidate** — each recipe is one guess; taste-testers score it from 0 to 100
- **The loop** — keep the tastiest recipes, mix pairs into new ones, randomly tweak an amount, re-score
- **Generations** — the best score climbs from 52 at generation 0 to 84 by generation 10
- **No formula needed** — nobody wrote an equation for taste; being able to score candidates is enough

*Example (italic):* Generation 0's best cookie scored 52; ten rounds of keep-mix-tweak later, the best scored 84 — no math beyond comparing scores.

**Key point:** A genetic algorithm optimizes by evolution: selection keeps good candidates, crossover mixes them, mutation adds random tweaks — repeat, and the population's scores climb.

### Visualization (canvas `c1`, 720×300)

Line chart of best and average taste score across generations 0–10, showing the population improving round by round.

- **Title (bold 15px, `#1a5276`, top center):** "Cookie Taste Score Over 10 Generations".
- **Data:** generations `[0,1,2,3,4,5,6,7,8,9,10]`; best score `[52, 58, 61, 67, 71, 74, 78, 80, 82, 83, 84]`; average score `[40, 46, 51, 55, 60, 63, 67, 70, 72, 74, 75]`.
- **Axes:** origin x=60, plot width 600, baseline y=245, chart height 185; y scale 30–90 with gridlines `#e5e9ef` and 12px `#444` labels at 30, 50, 70, 90; generation numbers 12px `#444` below baseline; x-axis caption 12px `#444` "generation".
- **Best line:** blue `#2a78d6` 3px with 4px dots; bold 12px blue end label "best = 84" right of the last point.
- **Average line:** green `#008300` 2.5px with 4px dots; bold 12px green end label "avg = 75".
- **Annotation:** orange `#d95926` bold 13px near generation 3–4, "keep + mix + tweak, every round"; small legend (12px) top-left inside plot: blue swatch "best of population", green swatch "population average".
- **Caption (12px `#444`, bottom right):** "taste score 0–100, illustrative".

## One Generation by Hand

**Tags:** `worked example` (blue), `crossover` (orange), `mutation` (green)

- **The population** — four recipes scored by tasters: A = 71, B = 64, C = 55, D = 48
- **Selection** — keep the top two, A (90g sugar, 12 min) and B (60g sugar, 10 min); drop C and D
- **Crossover** — the child takes A's sugar (90g) and B's bake time (10 min), one knob from each parent
- **Mutation** — a rare random nudge moves the child's sugar from 90g to 95g, a value no parent had
- **Re-score** — the new recipe (95g, 10 min) scores 76, beating both of its parents

*Example (italic):* Mixing A's sugar with B's bake time, then nudging sugar to 95g, produced a 76-point cookie from 71- and 64-point parents.

**Key point:** One generation = score, select, cross, mutate. You can run it by hand with four index cards — the algorithm is bookkeeping, not calculus.

### Visualization (canvas `c2`, 720×300)

Left-to-right flow diagram: four scored recipes, selection keeps two, crossover builds a child, mutation tweaks it, the child gets its new score.

- **Title (bold 15px, `#1a5276`, top center):** "One Generation: Select, Cross, Mutate, Re-score".
- **Stage labels (bold 13px `#1a5276`, y=52):** "population" at x=110, "selection" at x=300, "crossover" at x=455, "mutation" at x=615 (centered).
- **Recipe boxes (left column, x=35, width 160, height 32, 12px text, 1.5px borders, 4px radius):** at y=65 "A: 90g, 12 min — 71"; y=105 "B: 60g, 10 min — 64"; y=145 "C: 120g, 14 min — 55"; y=185 "D: 75g, 9 min — 48". A and B get green `#008300` borders and bold text; C and D get `#9aa2ad` borders, `#6b7280` text, and a thin `#9aa2ad` strike-through line.
- **Selection arrows:** two green 2px arrows from A and B boxes converging to x=250, y=125; small green bold 12px note "keep top 2" beside them.
- **Crossover box (x=385, y=95, width 150, height 60, orange `#d95926` 1.5px border):** bold 12px orange heading "child", 12px lines "sugar 90g (from A)" and "bake 10 min (from B)"; orange 2px arrow into it from the selection point.
- **Mutation box (x=560, y=95, width 140, height 60, violet `#4a3aa7` 1.5px border):** bold 12px violet heading "tweak", 12px line "sugar 90g → 95g"; violet 2px arrow into it from the crossover box.
- **Result (bold 13px green `#008300`, centered at y=255):** "new recipe (95g sugar, 10 min) scores 76 — better than both parents".
- **Caption (12px `#444`, bottom right):** "scores illustrative".

## When There Is No Gradient to Follow

**Tags:** `where it's used` (blue), `rugged landscape` (orange), `rule of thumb` (green)

- **Bumpy problems** — taste, schedules, and routes have many local peaks; tiny-step search gets stuck
- **The hill climber** — a tweak-one-knob-at-a-time search parks on a 70-point local peak and stops
- **The population** — six spread-out recipes explore several hills at once; one lands near the 88 peak
- **Where you meet it** — feature selection, hyperparameter search, scheduling, routing, circuit design
- **The trade** — a GA needs many score evaluations, so use it when scoring is cheap or parallelizable

*Example (italic):* A one-knob-at-a-time tuner stopped at a 70-point recipe; the six-recipe population found the 88-point recipe two hills away.

**Key point:** Reach for a GA when the score is easy to compute but has no derivative and many local peaks — a population searches several hills in parallel instead of climbing one.

### Visualization (canvas `c3`, 720×300)

A rugged 1-D fitness landscape (taste vs sugar amount) with a hill climber stuck on a local peak and six population members spread across the hills.

- **Title (bold 15px, `#1a5276`, top center):** "Rugged Landscape: One Climber Gets Stuck, a Population Explores".
- **Landscape data:** 15 points, x evenly spaced from x=60 to x=680; heights (taste 0–100) `[30, 55, 42, 70, 50, 35, 62, 48, 88, 66, 44, 58, 72, 52, 38]`; map score s to y = 250 − s×1.9.
- **Curve:** ink `#1a5276` 2px polyline through the 15 points with a light fill `rgba(42,120,214,0.08)` down to the baseline y=250; baseline 1px `#999` with x-axis caption 12px `#444` "sugar amount →".
- **Hill climber:** orange `#d95926` 8px dot on point index 3 (the 70 peak); orange bold 13px annotation above it, two lines: "hill climber" / "stuck at 70".
- **Population:** green `#008300` 6px dots on point indices 1, 4, 6, 10, 12 and a bold magenta `#d55181` 8px dot on index 8 (the 88 peak); green bold 13px annotation near the right, "population explores many hills"; magenta bold 13px label above index 8, "global best 88".
- **Caption (12px `#444`, bottom right):** "taste score vs sugar, illustrative".

## The Mutation-Rate Dial

**Tags:** `common mistake` (red), `mutation rate` (orange), `rule of thumb` (green)

- **Too little mutation** — the population turns identical fast and flatlines at 70; no new ideas arrive
- **Too much mutation** — every child is nearly random; scores jitter in the 50s and never build
- **The balance** — small, rare tweaks keep progress steady, reaching 84 by generation 10
- **Diversity check** — if all candidates look alike early on, raise mutation or the population size
- **Rule of thumb** — mutate a little and rarely; let selection and crossover do the actual climbing

*Example (italic):* Doubling mutation to "search faster" turned a steady climb into a jittery walk in the 50s with no trend after ten generations.

**Common mistake:** Treating mutation as the engine of progress. Selection and crossover drive the climb; mutation only supplies fresh material — crank it up and the GA becomes random guessing.

### Visualization (canvas `c4`, 720×300)

Three best-score curves over generations 0–10 comparing mutation set too low, balanced, and too high.

- **Title (bold 15px, `#1a5276`, top center):** "Same Problem, Three Mutation Rates".
- **Data:** generations `[0,1,2,3,4,5,6,7,8,9,10]`; too low `[52, 60, 66, 69, 70, 70, 70, 70, 70, 70, 70]`; balanced `[52, 58, 61, 67, 71, 74, 78, 80, 82, 83, 84]`; too high `[52, 55, 50, 57, 53, 58, 54, 59, 56, 60, 57]`.
- **Axes:** origin x=60, plot width 580, baseline y=245, chart height 185; y scale 40–90 with gridlines `#e5e9ef` and 12px `#444` labels at 40, 60, 80; generation numbers 12px `#444` below baseline; x-axis caption 12px `#444` "generation".
- **Too-low line:** orange `#d95926` 2.5px with 4px dots; orange bold 12px end label, two lines: "too low: stuck at 70" / "(premature convergence)".
- **Balanced line:** blue `#2a78d6` 3px with 4px dots; blue bold 12px end label "balanced: 84".
- **Too-high line:** magenta `#d55181` 2.5px with 4px dots; magenta bold 12px end label "too high: jitters in the 50s".
- **Annotation (green `#008300` bold 13px, top left inside plot):** "small + rare tweaks win".
- **Caption (12px `#444`, bottom right):** "best score per generation, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
