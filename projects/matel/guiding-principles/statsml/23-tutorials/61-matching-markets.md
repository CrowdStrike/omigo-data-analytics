# Matching Markets

**Page type:** grid page (tutorials category grid: two h2 sections ("101 Intro", "Related Topics"), 4-column nav-grid of cards with topic tags)
**HTML title tag:** Matching Markets

**Subtitle:** Who gets what when both sides have preferences — from stable marriages and kidney chains to ride-hailing dispatch and why dating apps ration likes.

## Cards

Each card links to a topic page under `matching-markets/`. The card shows a colored uppercase category label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. Cards are split into two h2 sections, each with its own `.nav-grid`: a "101 Intro" spine followed by "Related Topics"; the colored labels carry the finer grouping.
### 101 Intro

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | 101 INTRO | What Is a Matching Problem | [61-matching-markets/01-what-is-a-matching-problem.md](61-matching-markets/01-what-is-a-matching-problem.md) | Two sides that each rank the other — jobs and candidates, students and schools — and no prices to clear the market, so an algorithm decides who ends up with whom. | two sides, preference lists, capacity |
| 2 | 101 INTRO | Stability & Blocking Pairs | [61-matching-markets/02-stability-and-blocking-pairs.md](61-matching-markets/02-stability-and-blocking-pairs.md) | A matching fails not when someone is unmatched but when two participants would rather ditch their assigned partners for each other. | blocking pair, stability, unraveling |
| 3 | 101 INTRO | Gale-Shapley by Hand | [61-matching-markets/03-gale-shapley-by-hand.md](61-matching-markets/03-gale-shapley-by-hand.md) | Run deferred acceptance on a 4×4 preference table — propose, hold tentatively, reject — and a stable matching appears in a handful of rounds. | deferred acceptance, proposals, tentative hold |
| 4 | 101 INTRO | The Assignment Problem | [61-matching-markets/04-the-assignment-problem.md](61-matching-markets/04-the-assignment-problem.md) | When preferences become numbers — costs or scores — matching turns into picking one cell per row and per column with the best total. | cost matrix, Hungarian algorithm, best total |
| 5 | 101 INTRO | Matching as a Graph Problem | [61-matching-markets/05-matching-as-a-graph-problem.md](61-matching-markets/05-matching-as-a-graph-problem.md) | Draw both sides as dots and options as edges, and matching becomes graph theory — augmenting paths grow a matching one swap chain at a time. | bipartite graph, augmenting path, max-flow |

### Related Topics

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 6 | MARKET DESIGN | Honesty & Who Proposes | [61-matching-markets/06-honesty-and-who-proposes.md](61-matching-markets/06-honesty-and-who-proposes.md) | Whichever side proposes gets its best stable outcome — and the receiving side can sometimes do better by lying about its preferences. | proposer advantage, strategy-proof, misreporting |
| 7 | MATCHING IN THE WILD | Residents & School Choice | [61-matching-markets/07-residents-and-school-choice.md](61-matching-markets/07-residents-and-school-choice.md) | The doctor-hospital match and big-city school assignment run on deferred acceptance — the same algorithm, at national scale, for decades. | hospital match, school choice, capacity |
| 8 | MATCHING IN THE WILD | Kidney Exchange | [61-matching-markets/08-kidney-exchange.md](61-matching-markets/08-kidney-exchange.md) | Paying for organs is illegal, so kidneys trade through swap cycles and donor chains — one altruistic donor can set off dozens of transplants. | swap cycles, donor chains, no prices |
| 9 | ONLINE & PLATFORMS | Online Matching | [61-matching-markets/09-online-matching.md](61-matching-markets/09-online-matching.md) | When one side arrives over time — ad impressions, ride requests — you must match now or lose the chance, without knowing who comes next. | arrivals over time, greedy, match now vs wait |
| 10 | ONLINE & PLATFORMS | Dating Apps & Platform Markets | [61-matching-markets/10-dating-apps-and-platform-markets.md](61-matching-markets/10-dating-apps-and-platform-markets.md) | Two-sided platforms live or die by thickness and congestion — why apps ration likes, curate both sides, and fight the popularity pile-up. | thickness, congestion, rationed likes |
| 11 | ML CONNECTION | Recommenders as Matching | [61-matching-markets/11-recommenders-as-matching.md](61-matching-markets/11-recommenders-as-matching.md) | A feed is a matching problem in disguise — users on one side, items with limited exposure on the other, and pure ranking ignores the capacity. | exposure budget, capacity, ranking vs matching |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then two `<h2>` sections — "101 Intro" and "Related Topics" — each followed by its own `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "101 INTRO" `#8e44ad`, "MARKET DESIGN" `#2980b9`, "MATCHING IN THE WILD" `#c0392b`, "ONLINE & PLATFORMS" `#d35400`, "ML CONNECTION" `#27ae60`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (label accents also use `#8e44ad`, `#d35400`, `#c0392b`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
