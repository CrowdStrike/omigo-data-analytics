# Premature Optimization & YAGNI

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Premature Optimization & YAGNI

**Subtitle:** Two disciplines of not building it yet — measure before you optimize, and wait for the requirement before you generalize

## The Cache That Saved 2 Milliseconds

**Tags:** `core idea` (blue), `premature optimization` (orange), `Knuth` (green)

- **The week** — an engineer spends five days building a memoization cache for a string formatter
- **The page** — the checkout page it lives on takes 3,000ms to load; nobody has profiled it
- **The win** — the cache works perfectly: the formatter drops from 8ms to 6ms, saving 2ms
- **The bill** — the page is 0.07% faster, and the codebase now carries cache invalidation forever
- **The quote** — Knuth (1974): "premature optimization is the root of all evil" — everyone stops there
- **The dropped half** — the sentence continues: "yet we should not pass up our opportunities in that critical 3%"

*Example (italic):* Five days of work make a 3,000ms page load in 2,998ms — complexity was bought, and nothing was received in return.

**Key point:** Donald Knuth's full 1974 sentence: "We should forget about small efficiencies, say about 97% of the time: premature optimization is the root of all evil. Yet we should not pass up our opportunities in that critical 3%." Optimizing before measuring almost always lands in the cold 97%.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart breaking the 3,000ms checkout page into its four time components, with the optimized formatter's sliver highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Where the 3,000ms Page Actually Spends Its Time".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, pixel scale 0.22 px/ms; left-aligned 12px `#444` row labels at x=20.
- **Rows (top to bottom at y = 80, 125, 170, 215), bars 16px tall:**
  - "database query — 1,740ms": blue `#2a78d6` bar width 383
  - "payment API call — 990ms": blue bar width 218
  - "template render — 262ms": blue bar width 58
  - "string formatter — 8ms": orange `#d95926` bar width 2 (minimum visible), bold 12px orange label "← the week went here" to its right
- **Bar style:** blue fills `rgba(42,120,214,0.30)` with 2px solid edges, orange bar solid; 11px `#444` ms labels at bar ends.
- **Annotation (bold 13px red `#e74c3c`, right side near y=250):** "the cache saved 2ms of 3,000 — 0.07%".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Profile First: Where the Time Concentrates

**Tags:** `worked example` (blue), `profiling` (green)

- **The profile** — the team finally runs a profiler over all 40 functions on the checkout path
- **Rank 1** — the database query alone owns 1,740ms of the 3,000ms total (58%)
- **Rank 2** — the payment API call owns another 990ms (33%)
- **Hand-check** — 1,740 + 990 = 2,730ms, so 2 of 40 functions carry 91% of the time
- **The cold tail** — the other 38 functions share 270ms, about 7ms each — the formatter lives here
- **The lesson** — intuition picked from the 38; the profiler pointed at the 2 in one afternoon

*Example (italic):* Batching the rank-1 query cuts 1,740ms to 600ms — one measured fix beats the cache by a factor of 570.

**Key point:** Runtime concentrates in a tiny fraction of code, so intuition-guided optimization mostly hits the cold 97%. Profile first: the critical 3% is found by measurement, not by guessing.

### Visualization (canvas `c2`, 720×300)

Pareto curve: cumulative share of runtime versus functions ranked by cost, showing 2 of 40 functions carrying 91% of the time.

- **Title (bold 15px, `#1a5276`, top center):** "2 of 40 Functions Carry 91% of the Runtime".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = function rank 1 to 40, 12px `#444` tick labels at 1, 10, 20, 30, 40; y = cumulative % of runtime 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Curve:** blue `#2a78d6` 3px line through ranks `[1, 2, 3, 5, 10, 20, 30, 40]`, cumulative % `[58, 91, 94, 96, 98, 99, 99.6, 100]`, with 4px blue dots at the first three points.
- **Hot-zone marker:** vertical dashed `#d95926` (dash 4/3) line at rank 2, bold 12px orange label "the critical 3%" at its top.
- **Cold-zone label:** bold 12px mute `#6b7280` "the cold 97%: 38 functions share 270ms" centered near rank 22, y=210.
- **Annotation (bold 13px green `#008300`, near rank 8, y=95):** "rank 1 + rank 2 = 2,730ms of 3,000".
- **Caption (12px `#444`, bottom right):** "profile illustrative".

## The Second Database That Never Came

**Tags:** `YAGNI` (blue), `Extreme Programming` (orange), `speculation` (green)

- **The prediction** — "we might switch databases someday," so the team builds a pluggable backend layer
- **The cost** — 40 hours to build in Q1, then every schema change touches the interface plus the impl
- **The tally** — by Q6 the abstraction has absorbed 130 cumulative hours of build and upkeep
- **The payoff** — the second backend has shipped zero times; the predicted future never arrived
- **The name** — YAGNI, "You Aren't Gonna Need It", from Extreme Programming (Kent Beck, late 1990s)
- **The rule** — build for today's requirement; add flexibility when a real second use case exists

*Example (italic):* Eighteen months in, the app still runs on one database — but every migration pays the two-layer toll.

**Key point:** YAGNI: speculative flexibility costs maintenance forever while its predicted future rarely arrives. Wait for the requirement — a second concrete case teaches you what the abstraction should actually look like.

### Visualization (canvas `c3`, 720×300)

Line chart over six quarters: cumulative hours spent on the unused backend abstraction versus its cumulative benefit, which stays at zero.

- **Title (bold 15px, `#1a5276`, top center):** "The Abstraction's Running Bill vs the Benefit That Never Arrived".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = quarters "Q1" to "Q6" with 12px `#444` tick labels; y = cumulative hours 0 to 150, gridlines `#e5e9ef` at 50/100.
- **Cost line:** orange `#d95926` 3px line through quarters `[1, 2, 3, 4, 5, 6]`, hours `[40, 55, 75, 90, 110, 130]`, 4px dots at each point, 11px `#444` value label "130h" at the last point.
- **Benefit line:** green `#008300` 3px line, flat at 0 across all six quarters, bold 12px green label "backends actually swapped: 0" above it near Q4.
- **Build marker:** vertical dashed `#6b7280` (dash 4/3) line at Q1, 12px `#6b7280` label "abstraction built (40h)" at its top.
- **Annotation (bold 13px red `#e74c3c`, near Q4, y=85):** "the future never arrived; the bill did".
- **Caption (12px `#444`, bottom right):** "hours illustrative".

## One-Way Doors and Two-Way Doors

**Tags:** `common mistake` (red), `irreversibility` (orange)

- **The overcorrection** — reading YAGNI as "never think ahead" and shipping every decision naively
- **Two-way doors** — most choices reverse cheaply: rename a helper, swap a private implementation
- **One-way doors** — some don't: schemas holding live data, published APIs, security models
- **The asymmetry** — reversing a helper rename costs 1 hour; retrofitting auth costs 300 (illustrative)
- **The skill** — spend design effort in proportion to how expensive the decision is to undo
- **Both disciplines agree** — earn complexity with evidence, but check which way the door swings first

*Example (italic):* Skipping a cache is a two-way door; skipping tenant isolation in the schema is a one-way door with 200 hours behind it.

**Common mistake:** Applying "don't build it yet" to decisions that are expensive to change later. YAGNI defers cheap-to-reverse work; schemas, public APIs, and security deserve upfront thought precisely because reversing them is brutal.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: illustrative cost in hours to reverse five decisions after launch, two-way doors in green, one-way doors in orange and red.

- **Title (bold 15px, `#1a5276`, top center):** "Cost to Reverse the Decision Later (hours, illustrative)".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440, pixel scale ~1.47 px/hour; left-aligned 12px `#444` row labels at x=20.
- **Rows (top to bottom at y = 70, 110, 150, 190, 230), bars 14px tall, 11px hour labels at bar ends:**
  - "rename internal helper — 1h": green `#008300` bar width 2 (minimum visible)
  - "swap private implementation — 4h": green bar width 6
  - "change published API — 120h": orange `#d95926` bar width 176
  - "migrate schema with live data — 200h": orange bar width 293
  - "retrofit auth into the design — 300h": red `#e74c3c` bar width 440
- **Bar style:** green bars solid, orange/red bars fill `rgba(217,89,38,0.30)` and `rgba(231,76,60,0.30)` with 2px solid edges.
- **Door labels:** bold 12px green "two-way doors" right of the top pair near y=90; bold 12px red "one-way doors" right of the bottom bars near y=190.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "defer the reversible; design the irreversible".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); page timings, profile shares, maintenance hours, and reversal costs are invented and labeled illustrative; text numbers and chart numbers must stay in sync (1,740 + 990 + 262 + 8 = 3,000ms; 1,740 + 990 = 2,730ms = 91%). The Knuth quote (1974, "Structured Programming with go to Statements") and the YAGNI attribution (Extreme Programming, Kent Beck) are real and must be kept verbatim.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
