# PageRank

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** PageRank

**Subtitle:** A page is important when important pages link to it — a circular idea that a random reader clicking links forever turns into one number per page

## Links Are Votes — But Votes Are Not Equal

**Tags:** `core idea` (blue), `link graph` (green), `web search` (orange)

- **A tiny web** — four pages: A links to B and C, B links to C, C links back to A, D links to C
- **Counting votes** — a link is a vote: C collects 3 inbound links, A and B collect 1 each, D collects 0
- **The catch** — a vote from an important page should count more than a vote from a nobody
- **The circle** — C is important because A votes for it; A is important because C votes for it
- **The fix** — PageRank solves the circle by scoring every page at once, not one page at a time

*Example (italic):* A and B each hold exactly one inbound link, yet A ends up with score 0.372 and B with 0.196 — A's single vote comes from C, the most important page on the web.

**Key point:** PageRank turns "important pages link to important pages" from a circular sentence into a solvable system — every page's score is defined in terms of the others.

### Visualization (canvas `c1`, 720×300)

Node-link diagram of the four-page web: circles sized by final PageRank, arrows for links, so the reader sees C and A dominating while D shrinks to the floor.

- **Title (bold 15px, `#1a5276`, top center):** "Four Pages, Four Links Between Them — Size = PageRank".
- **Nodes (circles, 2px border in node color, fill = color at 0.18 alpha):** A at (200, 120) radius 38 blue `#2a78d6`; C at (450, 150) radius 40 green `#008300`; B at (250, 230) radius 24 orange `#d95926`; D at (600, 90) radius 13 mute `#6b7280`.
- **Node labels:** bold 14px node color, centered: "A" / "B" / "C" / "D" with 12px `#444` score line under each name inside or below the circle: "0.372", "0.196", "0.394", "0.038".
- **Edges (1.8px `#9aa4b0` lines with small filled arrowheads at the target end):** A→B, A→C, B→C, C→A (drawn as a slight curve above the direct A→C line so the two opposite arrows don't overlap), D→C.
- **Inlink note (11px `#6b7280`, under each node):** "3 links in" for C, "1 link in" for A and B, "0 links in" for D.
- **Annotation (bold 12px green `#008300`, near x=450, y=262):** "C wins on votes — and its single vote back makes A nearly as big".
- **Caption (11px `#444`, bottom right):** "scores computed with damping 0.85 — see next section".

## The Random Reader

**Tags:** `worked example` (blue), `random surfer` (green), `damping factor` (orange)

- **The reader** — imagine someone reading forever: on each page they follow a random outgoing link
- **The escape hatch** — 15% of the time they get bored and jump to a completely random page instead
- **The score** — PageRank is the share of time this eternal reader spends on each page
- **The rule** — score = 0.15/4 + 0.85 × (sum over linkers of their score ÷ their number of links)
- **Check B by hand** — B's only linker is A, which has 2 links: 0.0375 + 0.85 × (0.372 ÷ 2) = 0.196
- **Check A by hand** — A's only linker is C, which has 1 link: 0.0375 + 0.85 × 0.394 = 0.372

*Example (italic):* Start every page at 0.25 and re-apply the rule; by round 10 the scores settle near A 0.375, B 0.195, C 0.393, D 0.038 — the same numbers the equations give.

**Key point:** The steady scores are self-consistent — plug them into the update rule and they come back out unchanged, which is exactly what "the system is solved" means.

### Visualization (canvas `c2`, 720×300)

Convergence line chart: one line per page tracking its score across rounds 0–10 of the update rule, all four starting at 0.25 and settling onto their steady values.

- **Title (bold 15px, `#1a5276`, top center):** "Re-Apply the Rule Until Nothing Moves".
- **Axes:** origin x=70, baseline y=245, plot width 540, plot height 180; y from 0 to 0.6 with ticks at 0, 0.2, 0.4, 0.6 (12px `#444`, light `#e5e9ef` gridlines); x ticks 0–10 labeled every 2 rounds (12px `#444`), 12px `#444` axis label "round" centered below.
- **Lines (2.5px, small 3.5px dots at each round):** hardcoded score arrays —
  - A blue `#2a78d6`: 0.25, 0.25, 0.521, 0.291, 0.389, 0.389, 0.353, 0.383, 0.370, 0.370, 0.375
  - B orange `#d95926`: 0.25, 0.144, 0.144, 0.259, 0.161, 0.203, 0.203, 0.188, 0.200, 0.195, 0.195
  - C green `#008300`: 0.25, 0.569, 0.298, 0.413, 0.413, 0.371, 0.407, 0.392, 0.392, 0.397, 0.393
  - D mute `#6b7280`: 0.25, then 0.038 for rounds 1–10
- **End labels (bold 12px in line color, right of last point):** "A 0.375", "B 0.195", "C 0.393", "D 0.038".
- **Annotation (bold 12px violet `#4a3aa7`, near x=200, y=60):** "every page starts equal at 0.25 — the links do the sorting".
- **Caption (11px `#444`, bottom right):** "damping 0.85, jump share 0.15/4 = 0.0375 per page".

## One Strong Vote Beats a Weak One

**Tags:** `where it's used` (blue), `vote quality` (green)

- **Same vote count** — A and B both have exactly one inbound link, so raw counting calls them equal
- **Different sources** — A's vote comes from C (score 0.394, its only link); B's comes from half of A
- **Twice the score** — A lands at 0.372, B at 0.196: the source and its link count set a vote's worth
- **Split votes** — a page's score divides across its outgoing links, so a link from a link-farm page is tiny
- **Beyond the web** — the same score ranks papers by citations, accounts by followers, suspects in fraud rings

*Example (italic):* A citation from one landmark paper can outweigh five citations from obscure ones — PageRank on the citation graph makes that intuition a number.

**Key point:** PageRank rewards the quality of your linkers, not the quantity of your links — which is why it resists crude "get many links anywhere" manipulation.

### Visualization (canvas `c3`, 720×300)

Grouped comparison: for each page, a light bar for inbound-link count next to a solid bar for PageRank, showing that score is not proportional to vote count — A's one good vote nearly matches C's three.

- **Title (bold 15px, `#1a5276`, top center):** "Inbound Links vs PageRank — Counting Votes Misses the Point".
- **Layout:** four page groups centered at x = 155, 300, 445, 590; baseline y=240; two bars per group, 42px wide with a 10px gap.
- **Left bar per group (link count, scale 3 links = 150px tall):** fill `rgba(107,114,128,0.25)`, 1.5px `#6b7280` border; heights for counts A 1, B 1, C 3, D 0 (D drawn as a 2px stub); 12px `#6b7280` value labels "1", "1", "3", "0" above.
- **Right bar per group (PageRank, scale 0.4 = 150px tall):** fills at 0.35 alpha with 2px solid borders — A blue `#2a78d6` 0.372, B orange `#d95926` 0.196, C green `#008300` 0.394, D mute `#6b7280` 0.038; bold 13px value labels in the bar color above each bar.
- **Group labels (bold 13px `#2c3e50`, below baseline):** "A", "B", "C", "D"; 11px `#6b7280` second line "links in | score" under the first group only.
- **Legend (12px, top right):** gray swatch "inbound links", blue swatch "PageRank".
- **Annotation (bold 12px blue `#2a78d6`, near x=155, y=70):** two lines: "1 vote from C ≈ 3 votes" / "from smaller pages".
- **Caption (11px `#444`, bottom right):** "four-page web from above — illustrative".

## Important Is Not Relevant

**Tags:** `common mistake` (red), `query independence` (orange)

- **The trap** — reading PageRank as a search ranking: it never looks at the query, only at the link graph
- **One number, all queries** — a page's PageRank is the same for "sourdough starter" and "car insurance"
- **What search does** — combine a query-match score (does the text match?) with PageRank (is it trusted?)
- **Why both** — match alone surfaces keyword-stuffed junk; PageRank alone surfaces famous but off-topic pages
- **The modern view** — PageRank became one feature among hundreds inside learned ranking models

*Example (italic):* For the query "sourdough starter", a huge news homepage has enormous PageRank and zero relevance — it should lose to a modest baking page that actually answers the question.

**Common mistake:** Treating PageRank as the ranking. It is a query-independent importance prior — search engines multiply it with relevance signals; alone it would return the same famous pages for every query.

### Visualization (canvas `c4`, 720×300)

Scatter of three candidate pages for one query on two axes — query match across, PageRank up — with the winner being the balanced page, not the highest-PageRank one.

- **Title (bold 15px, `#1a5276`, top center):** "Query: sourdough starter — Two Signals Pick the Winner".
- **Axes:** origin (90, 240), width 400, height 170; x axis labeled "query match" (12px `#444`, arrowhead), y axis labeled "PageRank" (12px `#444`, rotated or stacked at top left); no numeric ticks — low/high 11px `#6b7280` labels at the ends of each axis.
- **Points (9px dots, bold 12px labels beside):** "news homepage" at (150, 95) green `#008300` — high PageRank, low match; "keyword-stuffed page" at (430, 215) orange `#d95926` — high match, low PageRank; "baking blog post" at (390, 120) blue `#2a78d6` — good match, decent PageRank.
- **Winner ring:** 2px blue dashed circle (radius 16) around the baking blog post with bold 12px blue "ranked first" beside it.
- **Side panel (x=540–700, 12px `#2c3e50`):** header bold "final score ="; lines "query match ×" / "importance (PageRank)"; below in 11px `#6b7280`: "both signals needed".
- **Annotation (bold 12px orange `#d95926`, near x=430, y=245):** "matches the words, trusted by no one".
- **Caption (11px `#444`, bottom right):** "positions illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; redraw all charts on debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data:** graph is A→B, A→C, B→C, C→A, D→C; damping 0.85 over 4 pages gives jump share 0.0375; steady scores A 0.372, B 0.196, C 0.394, D 0.038 satisfy the update rule and sum to 1.000; the round-by-round arrays in c2 are the hardcoded literals above (synchronous updates from 0.25 start); no randomness anywhere.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
