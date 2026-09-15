# Operations Research

**Page type:** grid page (tutorials category grid: two h2 sections ("101 Intro", "Related Topics"), 4-column nav-grid of cards with topic tags)
**HTML title tag:** Operations Research

**Subtitle:** How the world gets scheduled — the decision layer that turns forecasts into gate assignments, truck routes, and staffing plans.

## Cards

Each card links to a topic page under `operations-research/`. The card shows a colored uppercase category label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. Cards are split into two h2 sections, each with its own `.nav-grid`: a "101 Intro" spine followed by "Related Topics"; the colored labels carry the finer grouping.
### 101 Intro

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | 101 INTRO | What Is Operations Research | [60-operations-research/01-what-is-operations-research.md](60-operations-research/01-what-is-operations-research.md) | The decision layer on top of data science — prediction answers "what will happen", operations research answers "what should we do about it". | predict-then-optimize, forecasts, decisions |
| 2 | 101 INTRO | Objective & Constraints | [60-operations-research/02-objective-and-constraints.md](60-operations-research/02-objective-and-constraints.md) | Every decision problem can be written as one sentence — the thing you want, subject to the rules you can't break — and stating it well is the transferable skill. | objective, constraints, feasible vs optimal |
| 3 | 101 INTRO | Too Many Plans to Try | [60-operations-research/03-too-many-plans-to-try.md](60-operations-research/03-too-many-plans-to-try.md) | Real schedules have more possible plans than the universe has time for — computers rule out whole families at once instead of trying them all. | combinatorial explosion, greedy, solvers |

### Related Topics

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 4 | IN PRACTICE | A Day at the Airport | [60-operations-research/04-a-day-at-the-airport.md](60-operations-research/04-a-day-at-the-airport.md) | Gates, runways, and crews — three invisible optimizations that decide where your plane parks, when it lands, and who flies it. | gate assignment, runway order, crew schedules |
| 5 | IN PRACTICE | Running a City | [60-operations-research/05-running-a-city.md](60-operations-research/05-running-a-city.md) | Fire stations placed to cover every block, traffic lights timed into green waves, bus routes bent toward ridership — city planning as data-fed optimization. | covering, green waves, bus routes |
| 6 | IN PRACTICE | The Delivery Truck's Route | [60-operations-research/06-the-delivery-trucks-route.md](60-operations-research/06-the-delivery-trucks-route.md) | One driver, a hundred stops, billions of possible orders — how the route gets picked in seconds and why it avoids left turns. | routing, good enough beats perfect, left turns |
| 7 | DATA & DECISIONS | Decisions Change the Data | [60-operations-research/07-decisions-change-the-data.md](60-operations-research/07-decisions-change-the-data.md) | The plan you run decides what gets logged — so tomorrow's forecast learns from a world your own decisions filtered. | feedback loop, unseen demand, exploration |
| 8 | DATA & DECISIONS | Garbage In, Optimal Garbage Out | [60-operations-research/08-garbage-in-optimal-garbage-out.md](60-operations-research/08-garbage-in-optimal-garbage-out.md) | An optimizer squeezes every drop out of the forecast it's given — putting the plan exactly where a forecast error hurts most. | no slack, error bars, buffers |
| 9 | DATA & DECISIONS | Waiting Lines & Little's Law | [60-operations-research/09-waiting-lines-and-littles-law.md](60-operations-research/09-waiting-lines-and-littles-law.md) | People in the system equal arrival rate times time inside — one line of algebra that sizes checkouts, call centers, and emergency rooms. | little's law, utilization, queues |
| 10 | METHODS | Linear Programming & Simplex | [60-operations-research/10-linear-programming-and-simplex.md](60-operations-research/10-linear-programming-and-simplex.md) | When limits are straight lines, the allowed plans form a polygon — and the best plan always sits at a corner, so simplex just walks corner to corner uphill. | linear limits, corner solutions, simplex walk |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then two `<h2>` sections — "101 Intro" and "Related Topics" — each followed by its own `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "101 INTRO" `#8e44ad`, "IN PRACTICE" `#d35400`, "DATA & DECISIONS" `#27ae60`, "METHODS" `#2980b9`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (label accent also uses `#8e44ad`, `#d35400`, `#2980b9`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
