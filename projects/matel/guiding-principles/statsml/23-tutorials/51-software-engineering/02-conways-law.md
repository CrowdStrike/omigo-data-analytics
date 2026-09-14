# Conway's Law

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Conway's Law

**Subtitle:** Organizations ship systems that copy their own communication structure — the seams in your architecture sit exactly where the seams in your org chart do

## One Product, Three Search Boxes

**Tags:** `core idea` (blue), `org chart` (green), `1968` (orange)

- **The product** — one storefront site, yet its catalog, help pages, and forum each get their own search box
- **The teams** — Catalog, Help, and Community are three separate teams that rarely talk between quarterly reviews
- **The symptom** — three ranking rules, three typo behaviors, three "no results" pages on what users see as one site
- **The cause** — a shared search service needs constant cross-team communication; a private one per team needs none
- **The pattern** — every visible seam in the product sits directly on a boundary in the org chart

*Example (italic):* Searching "shiping fees" finds products (typo-tolerant), fails in help (exact match only), and sorts the forum by date — one query, three team cultures.

**Key point:** This is Conway's law (Melvin Conway, 1968): organizations design systems that mirror their own communication structures — interfaces appear exactly where communication is thinnest.

### Visualization (canvas `c1`, 720×300)

Two-row mapping diagram: three team boxes on top, three search boxes below, each team wired only to its own search box, with dashed org-boundary lines running through both rows.

- **Title (bold 15px, `#1a5276`, top center):** "Three Teams In, Three Search Boxes Out".
- **Top row (team boxes, y=60, 160×44, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border):** at x=80 "Catalog team", x=300 "Help team", x=520 "Community team", 13px `#2c3e50` centered text.
- **Arrows:** one 3px `#6b7280` vertical arrow from each team box straight down to its search box — no arrows cross a boundary.
- **Bottom row (search boxes, y=170, 160×58, fill `rgba(0,131,0,0.10)`, 2px `#008300` border):** at the same x positions, two-line 12px labels: "product search / typo-tolerant, ranks by sales", "help search / exact match only", "forum search / fuzzy, ranks by date".
- **Boundary lines:** two vertical dashed `#d95926` (dash 5/4) lines at x=265 and x=485 running y=45 to y=245, 11px `#d95926` label "team boundary" beside each at y=48.
- **Annotation (bold 13px red `#e74c3c`, centered near y=262):** "the seams in the product are the seams in the org chart".
- **Caption (12px `#444`, bottom right):** "search behaviors illustrative".

## Four Teams Ship a Four-Pass Compiler

**Tags:** `worked example` (blue), `the classic` (green)

- **The setup** — a Conway-style illustration: assign one compiler to four teams and watch the design emerge
- **The result** — the compiler comes out as four passes, one pass per team, no more and no fewer
- **Hand-check** — a chain of 4 teams has 3 boundaries between them, so the design grows exactly 3 file handoffs
- **The reason** — a clean interface across a team boundary is easy; shared internals across one are painful
- **The general rule** — split work across N teams in a chain and you get N modules with N−1 seams

*Example (italic):* Lexer, parser, optimizer, and code generator each write an intermediate file for the next team — the 3 files are the org chart, serialized.

**Key point:** The pass structure was never a technical decision — count the passes and you have counted the teams; the architecture is the org chart, redrawn.

### Visualization (canvas `c2`, 720×300)

Left-to-right pipeline diagram: four compiler passes as boxes, each owned by one team, with the three intermediate files sitting exactly on the three team boundaries.

- **Title (bold 15px, `#1a5276`, top center):** "4 Teams → 4 Passes → 3 Handoff Files".
- **Team labels (bold 12px `#2a78d6`, y=95, centered over each box):** "Team A", "Team B", "Team C", "Team D".
- **Pass boxes (y=120, 140×54, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border):** at x=40 "lexer", x=215 "parser", x=390 "optimizer", x=565 "code gen", 13px `#2c3e50` centered text.
- **Handoff arrows:** three 3px `#008300` horizontal arrows between consecutive boxes at y=147, each with a 12px `#008300` label above: "tokens file", "tree file", "IR file".
- **Boundary lines:** three vertical dashed `#d95926` (dash 5/4) lines at x=197, x=372, x=547 running y=85 to y=210 — each passes exactly through one arrow.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=240):** "every interface lands on a team boundary — none anywhere else".
- **Caption (12px `#444`, bottom right):** "Conway-style compiler illustration, schematic".

## Choose the Architecture, Then the Org Chart

**Tags:** `where it's used` (blue), `inverse Conway` (green), `Team Topologies` (orange)

- **Embracing it** — microservices lean into the law: one service per team, interfaces exactly at team boundaries
- **Fighting it** — decree a clean architecture across a mismatched org chart and the org chart wins every time
- **The maneuver** — the Inverse Conway Maneuver: pick the architecture you WANT, then reorganize teams to match it
- **The credit** — the maneuver was publicly popularized in the Team Topologies literature
- **The payoff** — a change that once crossed 3 teams now lands inside 1, so it ships without cross-team meetings

*Example (italic):* A checkout change that needed sign-off from the frontend, backend, and database teams (3 teams) touches only the checkout team (1) after the reorg.

**Key point:** Because the org structure wins any fight with the intended architecture, reorganize the teams first and let the architecture follow.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: number of teams that must coordinate per feature change, layer-shaped org (blue) vs product-shaped org after the Inverse Conway Maneuver (green).

- **Title (bold 15px, `#1a5276`, top center):** "Teams Touched per Change: Before vs After the Inverse Conway Maneuver".
- **Axes:** origin x=70, baseline y=240, plot width 590, plot height 170; y = teams touched 0 to 4, gridlines `#e5e9ef` at 1/2/3, 12px `#444` y-tick labels; x = three change labels centered under groups (12px `#444`): "checkout field", "search filter", "refund flow".
- **Groups (centered at x=170, 370, 570; bars 52px wide, 12px gap within a pair):** blue `rgba(42,120,214,0.55)` "layer org" bars with heights for values `[3, 3, 2]`; green `rgba(0,131,0,0.55)` "product org" bars with values `[1, 1, 1]`; 12px bold value labels on top of every bar.
- **Legend (12px, top right inside plot):** blue swatch "layer-shaped org", green swatch "product-shaped org (after reorg)".
- **Annotation (bold 13px green `#008300`, near x=330, y=85):** "3 sign-offs → 1: the change fits inside one team".
- **Caption (12px `#444`, bottom right):** "team counts illustrative".

## The Boundary You're Fighting Is a Team Boundary

**Tags:** `common mistake` (red), `data teams` (orange)

- **The pipeline** — separate ingestion, warehouse, and BI teams build one "unified" data platform
- **The seams** — the platform arrives with exactly two awkward handoff points, one per team boundary
- **The speed** — each team's own step takes 1 day; each cross-team handoff waits 5 days in a ticket queue
- **The mistake** — blaming the tools or redrawing the architecture diagram while the org chart stays put
- **The practical use** — when a system boundary feels wrong, find the team boundary that created it

*Example (italic):* A new field lands in ingestion in 1 day, waits 5 days for the warehouse queue and 5 more for the BI queue, and reaches the dashboard 13 days later.

**Common mistake:** Treating the awkward seam as a technical flaw. The two handoffs are not in the pipeline's code — they are in the org chart, and no refactor removes them while three teams stay three teams.

### Visualization (canvas `c4`, 720×300)

Horizontal timeline bar: the 13-day journey of one new field, work segments (blue) vs handoff waits (red), showing the two team boundaries eating 10 of 13 days.

- **Title (bold 15px, `#1a5276`, top center):** "One New Field, 13 Days: Where the Time Goes".
- **Bar (single row, y=120, 26px tall, starting x=70):** five segments at 44px per day, left to right — blue `rgba(42,120,214,0.55)` width 44 ("ingestion, 1d"), red `rgba(231,76,60,0.45)` width 220 ("handoff wait, 5d"), blue width 44 ("warehouse, 1d"), red width 220 ("handoff wait, 5d"), blue width 44 ("BI, 1d"); 11px `#444` segment labels alternating above (y=108) and below (y=160) the bar.
- **Team spans (bold 12px `#2a78d6`, y=85):** "ingestion team" over segment 1, "warehouse team" over segment 3, "BI team" over segment 5.
- **Boundary markers:** two vertical dashed `#d95926` (dash 5/4) lines at the segment-1/2 and segment-3/4 joins (x=114 and x=378), y=70 to y=190, 11px `#d95926` label "team boundary" at each top.
- **Axis:** 2px `#999` baseline at y=190 from x=70 to x=642, 12px `#444` tick labels "day 0", "day 6", "day 12" at x=70, x=334, x=598.
- **Annotation (bold 13px red `#e74c3c`, centered near y=235):** "10 of 13 days are spent at the two team boundaries".
- **Caption (12px `#444`, bottom right):** "days illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded arrays and pixel positions above (no randomness); search behaviors, team counts (`[3,3,2]` vs `[1,1,1]`), and pipeline days (1/5/1/5/1 = 13) are invented and labeled illustrative; the 4-teams/4-passes/3-handoffs count is a Conway-style illustration (his 1968 paper's real case was 5- and 3-person teams); the generic company, coffee-shop-free examples name no real firms.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
