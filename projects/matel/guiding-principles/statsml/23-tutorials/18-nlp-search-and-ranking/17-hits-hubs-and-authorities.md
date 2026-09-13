# HITS: Hubs & Authorities

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** HITS: Hubs &amp; Authorities

**Subtitle:** For any topic the web splits into two kinds of good pages — lists that point to the best answers, and answers the best lists point to — so every page earns two scores, not one

## Good Lists and Good Answers

**Tags:** `core idea` (blue), `two scores` (green), `topic search` (orange)

- **The query** — "best running shoes" pulls up two very different kinds of useful page
- **Authorities** — shoe pages S1, S2, S3: the actual answers, each covering one shoe in depth
- **Hubs** — roundup pages R1, R2, R3: curated lists whose links point at the good shoe pages
- **The pairing** — a good hub points to good authorities; a good authority is pointed to by good hubs
- **Two scores** — HITS gives every page a hub score and an authority score, computed together

*Example (italic):* R2 links to all three shoe pages and becomes the top hub; S2 is linked by all three roundups and becomes the top authority — each side's winner is defined by the other side.

**Key point:** HITS splits "good page" into two roles — good at pointing (hub) and good at being pointed to (authority) — and lets each role's score feed the other.

### Visualization (canvas `c1`, 720×300)

Bipartite diagram: three roundup pages on the left, three shoe pages on the right, arrows for links, with the top hub and top authority visibly larger.

- **Title (bold 15px, `#1a5276`, top center):** "One Topic, Two Roles: Roundups Point, Shoe Pages Get Pointed At".
- **Left column (hubs, x=170):** rounded boxes 120×34 at y = 80, 150, 220 labeled "R1", "R2", "R3" with 11px `#6b7280` sublabel "roundup"; R2 drawn larger (140×40) with 2.5px border; fill `rgba(42,120,214,0.12)`, border blue `#2a78d6`.
- **Right column (authorities, x=470):** rounded boxes 120×34 at y = 80, 150, 220 labeled "S1", "S2", "S3" with sublabel "shoe page"; S2 drawn larger (140×40) with 2.5px border; fill `rgba(0,131,0,0.12)`, border green `#008300`.
- **Arrows (1.8px `#9aa4b0`, arrowheads at target):** R1→S1, R1→S2; R2→S1, R2→S2, R2→S3; R3→S2, R3→S3.
- **Column headers (bold 13px, x-centered over each column, y=52):** "hubs — good lists" in blue, "authorities — good answers" in green.
- **Annotation (bold 12px violet `#4a3aa7`, centered near x=360, y=272):** "R2 points at every good answer; S2 is on every good list".
- **Caption (11px `#444`, bottom right):** "links illustrative".

## Passing Scores Back and Forth

**Tags:** `worked example` (blue), `mutual update` (green)

- **Start simple** — give every roundup a hub score of 1 and every shoe page an authority score of 0
- **Authority step** — each shoe page adds up the hub scores pointing at it: S1 = 1+1 = 2, S2 = 3, S3 = 2
- **Hub step** — each roundup adds up the authorities it points at: R1 = 2+3 = 5, R2 = 2+3+2 = 7, R3 = 5
- **Rescale** — divide by the largest so scores stay tame: hubs 0.71, 1.00, 0.71; authorities 0.67, 1.00, 0.67
- **Repeat** — another round keeps the same order here; on big graphs a few rounds settle it

*Example (italic):* S2's authority of 3 is just "three hubs point at me, each currently worth 1" — every number in the round is an addition the reader can redo on paper.

**Key point:** One round is two sums — authorities collect from the hubs pointing in, hubs collect from the authorities they point out to — and repeating the pair of sums is the whole algorithm.

### Visualization (canvas `c2`, 720×300)

Stacked contribution bars for the first round: each shoe page's authority bar built from the hubs that vote for it, then each roundup's hub bar built from the authorities it points to.

- **Title (bold 15px, `#1a5276`, top center):** "Round 1: Two Sums, Six Scores".
- **Left half (authority step):** header bold 13px green `#008300` "authority = sum of hubs pointing in" at x=190 centered, y=58; three horizontal bars starting x=95, rows y = 90, 135, 180, height 26, scale 3 units = 210px; S1 bar of two 1-unit segments (R1, R2), S2 bar of three segments (R1, R2, R3), S3 bar of two segments (R2, R3); segments filled `rgba(42,120,214,0.30)` with 1.5px blue borders and 11px `#1a5276` centered segment labels "R1"/"R2"/"R3"; bold 13px green totals "2", "3", "2" right of each bar; 12px `#2c3e50` row labels "S1", "S2", "S3" left of the bars at x=88 right-aligned.
- **Right half (hub step):** header bold 13px blue `#2a78d6` "hub = sum of authorities pointed at" at x=545 centered, y=58; three horizontal bars starting x=455, rows y = 90, 135, 180, height 26, scale 7 units = 210px; R1 bar of segments 2 (S1) + 3 (S2), R2 bar of 2+3+2, R3 bar of 3+2; segments filled `rgba(0,131,0,0.30)` with 1.5px green borders, 11px `#008300` labels "S1 2"/"S2 3"/"S3 2"; bold 13px blue totals "5", "7", "5"; 12px row labels "R1", "R2", "R3" at x=448 right-aligned.
- **Annotation (bold 12px violet `#4a3aa7`, centered near x=360, y=248):** "each side's totals become the other side's next inputs".
- **Caption (11px `#444`, bottom right):** "start: every hub score 1 — rescaled after each round".

## The Best List Is Not the Best Answer

**Tags:** `common mistake` (red), `role separation` (orange)

- **Two scores, one page** — hub and authority are separate numbers; scoring high on one says nothing about the other
- **R2 in this web** — the best hub, yet nothing links to it: its authority score is 0
- **S2 in this web** — the best authority, yet it links out to nothing: its hub score is 0
- **Mixed pages exist** — a widely cited roundup would score on both; the roles mix, the scores don't
- **Reading results** — show authorities as answers; show hubs as "start here" guides

*Example (italic):* Ranking by one blended score would bury R2 — no one links to it — even though it is the single best place to start reading about running shoes.

**Common mistake:** Collapsing the two scores into one "goodness" number. A perfect hub with zero inbound links and a perfect authority with zero outbound links would both look mediocre on a blended score.

### Visualization (canvas `c3`, 720×300)

Paired-bar chart: for each of the six pages, its hub score next to its authority score after round 1, making the two-role split visible — roundups all-hub, shoe pages all-authority.

- **Title (bold 15px, `#1a5276`, top center):** "Every Page, Both Scores — High on One, Zero on the Other".
- **Layout:** six groups centered at x = 120, 210, 300, 420, 510, 600 for R1, R2, R3, S1, S2, S3; baseline y=235; two vertical bars per group, 30px wide, 6px gap; scale 1.0 = 140px.
- **Hub bars (left of pair):** fill `rgba(42,120,214,0.35)`, 2px blue `#2a78d6` border; values R1 0.71, R2 1.00, R3 0.71, S1 0, S2 0, S3 0 (zeros drawn as 2px stubs); bold 12px blue value labels above nonzero bars.
- **Authority bars (right of pair):** fill `rgba(0,131,0,0.35)`, 2px green `#008300` border; values R1 0, R2 0, R3 0, S1 0.67, S2 1.00, S3 0.67; bold 12px green labels above nonzero bars.
- **Group labels (bold 13px `#2c3e50`, below baseline):** "R1", "R2", "R3", "S1", "S2", "S3"; thin dashed `#9aa4b0` vertical divider at x=360 between the two families.
- **Legend (12px, top left):** blue swatch "hub score", green swatch "authority score".
- **Annotation (bold 12px `#e74c3c`, near x=210, y=70):** two lines: "best hub, authority 0 —" / "one blended score would bury it".
- **Caption (11px `#444`, bottom right):** "round-1 scores, rescaled — illustrative".

## HITS Next to PageRank

**Tags:** `where it's used` (blue), `vs PageRank` (green)

- **How many scores** — PageRank gives each page one global number; HITS gives two per topic
- **When it runs** — PageRank is precomputed over the whole web; HITS runs at query time on a small topic graph
- **The topic graph** — take pages matching the query plus their neighbors, run HITS just on that
- **Beyond the web** — citations: review articles are hubs, landmark papers are authorities; same split
- **The trade** — HITS answers "best for this topic" but costs work per query and is easier to spam locally

*Example (italic):* A survey paper that cites 200 papers is a superb hub and a weak authority; the classic paper it cites 200th may be the strongest authority in the field.

**Key point:** Reach for HITS-style thinking when "good" splits into pointing and being pointed at; reach for PageRank when you need one importance score computed once for everything.

### Visualization (canvas `c4`, 720×300)

Canvas-drawn comparison table: three contrast rows between PageRank and HITS, with a small topic-subgraph sketch showing HITS running on a query's neighborhood only.

- **Title (bold 15px, `#1a5276`, top center):** "One Global Number vs Two Topic Numbers".
- **Table (x=60–460):** two column headers at y=62 — bold 13px blue `#2a78d6` "PageRank" centered x=230, bold 13px green `#008300` "HITS" centered x=390; three rows at y = 100, 150, 200 with 12px `#6b7280` row labels at x=60 left-aligned ("scores per page", "computed", "question asked") and 12px `#2c3e50` cells: "one, global" / "two: hub + authority"; "once, whole web" / "per query, topic graph"; "important overall?" / "best for this topic?"; light `#e5e9ef` horizontal rules between rows.
- **Right sketch (x=490–700):** 12px `#6b7280` header "HITS runs here:" at y=70; a large light `#e5e9ef`-stroked cloud/ellipse labeled 11px "the whole web" (mute, top right); inside-bottom a small dashed green ellipse (2px, dash 5/3) around 6 small dots (3 blue, 3 green, 4px radius) with tiny connecting lines, labeled bold 12px green "query topic graph" below.
- **Annotation (bold 12px violet `#4a3aa7`, near x=490, y=262):** "small graph, fresh scores per query".
- **Caption (11px `#444`, bottom right):** "sketch illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; redraw all charts on debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data:** link graph is R1→{S1,S2}, R2→{S1,S2,S3}, R3→{S2,S3}; round-1 sums from all-hubs-at-1 give authorities 2, 3, 2 and hubs 5, 7, 5; rescaled by the max: hubs 0.71, 1.00, 0.71 and authorities 0.67, 1.00, 0.67; text and chart numbers identical; no randomness.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
