# Matching as a Graph Problem

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Matching as a Graph Problem

**Subtitle:** Draw both sides as dots and the options as edges — matching becomes graph theory with a mechanical solution

**Running data (illustrative), used identically in every section — who qualifies for what:**

| Candidate | Qualified jobs |
|-----------|----------------|
| Alice     | J1, J2         |
| Bob       | J1 only        |
| Carol     | J2, J3         |
| Dan       | J4 only        |

Greedy matching of size 3: Alice–J1, Carol–J2, Dan–J4 (Bob stuck, J1 taken).
Augmenting path from Bob: Bob→J1 (new), J1—Alice (drop), Alice→J2 (new), J2—Carol (drop), Carol→J3 (new).
After the flip, size 4: Alice–J2, Bob–J1, Carol–J3, Dan–J4.

## Dots, Edges, and Matchings

**Tags:** `core idea` (blue), `bipartite graph` (green)

- **The setup** — no rankings this time, only qualifications: each candidate can or can't do a job
- **Who fits what** — Alice: J1 or J2; Bob: only J1; Carol: J2 or J3; Dan: only J4
- **The drawing** — candidates as dots on the left, jobs on the right, one edge per allowed pair
- **A matching** — a set of edges where no dot is touched twice: nobody holds two jobs
- **The goal** — a maximum matching: as many edges as the graph can possibly support

*Example (italic):* Alice–J1, Carol–J2, Dan–J4 is a matching of size 3 — no dot used twice, but Bob is left out.

**Key point:** Once options are drawn as edges, "match as many as possible" becomes a precise graph question — find a maximum matching.

### Visualization (canvas `c1`, 720×300)

The bipartite graph with all six qualification edges, a size-3 matching bolded, and a warning that it is maximal but not maximum.

- **Title (bold 15px, `#1a5276`, top center):** "Candidates, Jobs, and a Matching (illustrative)".
- **Nodes:** candidates at x=200, jobs at x=520, y = 80/135/190/245 for Alice/Bob/Carol/Dan and J1/J2/J3/J4; filled circles radius 7, candidates blue `#2a78d6`, jobs green `#008300`.
- **Node labels (bold 12px `#2c3e50`):** names right-aligned at x=185 (+4 vertical offset), job names left-aligned at x=535.
- **All edges (1.5px `#6b7280`):** Alice–J1, Alice–J2, Bob–J1, Carol–J2, Carol–J3, Dan–J4.
- **Matched edges redrawn bold (3.5px blue `#2a78d6`):** Alice–J1, Carol–J2, Dan–J4.
- **Bob callout (bold 12px orange `#d95926`, right-aligned at x=185, y=155, under Bob's name):** "unmatched".
- **Annotation (bold 12px orange `#d95926`, centered at (360, 262)):** "size 3 — maximal (no edge can be added) but NOT maximum".
- **Caption (12px `#6b7280`, centered at y=284):** "bold edges form the matching; Bob's only edge leads to an already-matched dot".

## The Augmenting Path Trick

**Tags:** `worked example` (blue), `augmenting path` (orange)

- **Greedy stalls** — Alice–J1, Carol–J2, Dan–J4 are taken; Bob only knows J1, and J1 is gone
- **Don't restart** — start at unmatched Bob and walk edges, alternating unmatched and matched
- **The path** — Bob→J1 (new), J1—Alice (drop), Alice→J2 (new), J2—Carol (drop), Carol→J3 (new)
- **The flip** — swap every edge on the path: the matching grows 3 → 4, and nobody loses a job
- **The guarantee** — a matching is maximum exactly when no augmenting path exists at all

*Example (italic):* The path starts and ends on an unmatched edge, so it always has one more "add" than "drop" — flipping it must grow the matching by one.

**Key point:** "Keep finding augmenting paths until none remain" is a complete algorithm, not a heuristic — no remaining augmenting path is exactly what maximum means.

### Visualization (canvas `c2`, 720×300)

Two panels: before (Bob unmatched, the augmenting path dashed) and after (path flipped, all four matched in green).

- **Title (bold 15px, `#1a5276`, top center):** "Flip the Augmenting Path: 3 Matched → 4 Matched (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=44 to y=262.
- **Panel headers (bold 13px, centered at y=56):** "BEFORE — Bob is stuck" in orange `#d95926` at x=185; "AFTER — path flipped" in green `#008300` at x=545.
- **Node layout per panel:** candidates and jobs at y = 84/136/188/240; left panel candidates x=95, jobs x=275; right panel candidates x=455, jobs x=635; circles radius 6, candidates blue, jobs green; labels 11px `#2c3e50` (names right-aligned at candidate x − 12, jobs left-aligned at job x + 12).
- **Left panel edges:** matched Alice–J1, Carol–J2, Dan–J4 solid 3px blue `#2a78d6`; augmenting-path "add" edges Bob–J1, Alice–J2, Carol–J3 dashed (6,4) 2.5px orange `#d95926`.
- **Right panel edges:** unused edges Alice–J1, Carol–J2 thin 1.5px `#6b7280`; matched Alice–J2, Bob–J1, Carol–J3, Dan–J4 solid 3.5px green `#008300`.
- **Under-panel notes (bold 12px, centered at y=262):** left in orange at x=185 "dashed = edges the path adds"; right in green at x=545 "matching grew 3 → 4".
- **Caption (12px `#6b7280`, centered at y=284):** "the flip adds three edges and drops two — everyone matched before is still matched after".

## The Max-Flow View

**Tags:** `core idea` (blue), `max flow` (green), `Hall's condition` (orange)

- **Add plumbing** — a source wired into every candidate, a sink wired out of every job
- **Capacity 1** — every pipe carries at most one unit: one job per person, one person per job
- **The equivalence** — the maximum flow through this network equals the maximum matching
- **The payoff** — one reframing inherits every algorithm ever built for network flow
- **Hall's condition** — if k candidates together qualify for fewer than k jobs, no full matching
- **The certificate** — the bottleneck group itself is the proof that no full matching exists

*Example (italic):* If Bob and a fifth candidate Erin both qualified only for J1, the group {Bob, Erin} needs two jobs but reaches one — nothing can fix that.

**Key point:** Max flow makes matching mechanical, and Hall's condition names the only way a full matching can fail: some group with too few options — the group is the certificate of impossibility.

### Visualization (canvas `c3`, 720×300)

The same bipartite graph with a source and a sink added, every capacity 1, and the flow along the final size-4 matching highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Same Graph as a Flow Network (illustrative)".
- **Capacity note (bold 12px orange `#d95926`, centered at (360, 44)):** "every edge has capacity 1".
- **Nodes:** source S at (70, 162) and sink T at (650, 162), radius 11 circles filled ink `#1a5276` with bold 12px white "S"/"T" centered inside; candidates at x=250, jobs at x=470, y = 72/132/192/252, radius 6 circles (candidates blue, jobs green); labels 11px `#2c3e50` — names above-left of candidate dots (right-aligned at x=238), job names above-right of job dots (left-aligned at x=482).
- **Idle edges (1.5px `#6b7280`):** the two qualification edges without flow, Alice–J1 and Carol–J2.
- **Flow edges (3px green `#008300`):** S→each candidate, Bob–J1, Alice–J2, Carol–J3, Dan–J4, each job→T (the size-4 matching: every unit flows source → candidate → job → sink).
- **Annotation (bold 13px green `#008300`, centered at (360, 282)):** "max flow = 4 = maximum matching".
- **Caption:** folded into the annotation line above (single bottom line, no second caption).

## The Engine Under the Markets

**Tags:** `where it's used` (blue), `polynomial time` (green)

- **Preference matching** — stable-matching markets stand on this dots-and-edges machinery
- **Assignment solvers** — the cost-matrix problem is this same graph with numbers on the edges
- **Kidney exchanges** — cycle-and-chain finders search a compatibility graph for matchings
- **Scale** — bipartite maximum matching runs in polynomial time, even with millions of nodes
- **The pattern** — market language on the surface, dots and edges underneath, every time

*Example (italic):* An exchange with thousands of patient–donor pairs is searched exactly, not approximately — the graph algorithms are that fast.

**Key point:** Preference matching, assignment solvers, and kidney-cycle finders all reduce to graph matching — the market flavor is the surface; the graph is the engine.

### Visualization (canvas `c4`, 720×300)

Three market-flavored problem boxes funneling into one bipartite-matching engine box, which feeds a scale box.

- **Title (bold 15px, `#1a5276`, top center):** "The Engine Under the Market Problems".
- **Left boxes (x=48, width 210, height 44; y = 64, 124, 184):** "preference matching", "assignment solvers", "kidney-cycle finders"; fill `#fbfcfd`, 2px blue `#2a78d6` border, centered 12px `#2c3e50` text at box middle + 4.
- **Funnel arrows:** 1.5px mute `#6b7280` from (258, box mid) to (294, 136) per left box, filled arrowhead at (300, 136) drawn once.
- **Engine box (x=300, y=104, width 190, height 64):** fill `#fbfcfd`, 2.5px ink `#1a5276` border; "BIPARTITE MATCHING" bold 13px ink at y=130; "dots, edges, augmenting paths" 11px `#6b7280` at y=152.
- **Output arrow:** 2px mute from (490, 136) to (524, 136), arrowhead at (530, 136).
- **Scale box (x=530, y=104, width 160, height 64):** fill `#fbfcfd`, 2px green `#008300` border; "polynomial time" bold 12px green at y=130; "even at millions of nodes" 11px `#2c3e50` at y=152.
- **Caption (bold 13px magenta `#d55181`, centered at y=262):** "market language on top, graph theory underneath".
- **Sub-caption (12px `#6b7280`, centered at y=284):** "solve the graph problem once, and every market flavor of it comes for free".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize; shared `arrowHead(ctx, x, y, dir, color)` helper for filled right-pointing arrowheads; shared `edge` (line between two points, optional dash) and `node` (filled labeled circle) helpers for the graph charts.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data integrity:** hardcoded edge lists only — qualifications `Alice:[J1,J2], Bob:[J1], Carol:[J2,J3], Dan:[J4]`; size-3 matching `Alice–J1, Carol–J2, Dan–J4`; augmenting path adds `Bob–J1, Alice–J2, Carol–J3` and drops `Alice–J1, Carol–J2`; size-4 matching `Alice–J2, Bob–J1, Carol–J3, Dan–J4`; invented setup carries "(illustrative)" in chart titles.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
