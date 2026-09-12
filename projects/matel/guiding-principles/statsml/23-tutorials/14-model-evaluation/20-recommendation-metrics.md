# Recommendation Metrics

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Recommendation Metrics

**Subtitle:** Accuracy tells you whether users liked the list — coverage, diversity, and serendipity tell you whether the list ever showed them anything new

## Four Users, the Same Five Blockbusters

**Tags:** `core idea` (blue), `coverage` (green), `diversity` (orange)

- **The app** — a streaming app with 50 movies shows each of 4 users a top-5 list, 20 slots in all
- **Recommender A** — gives all 4 users the identical top-5: M1–M5, four action films and a comedy
- **Recommender B** — mixes hits with lesser-known picks; its 20 slots hold 16 different movies
- **Coverage** — the share of the catalog that ever gets recommended: A touches 5 of 50, B 16 of 50
- **Diversity** — how varied one user's list is: A's list spans 2 genres, each B list spans 5

*Example (italic):* Maya, Ben, Ana, and Raj open the app and recommender A shows all four the exact same five blockbusters.

**Key point:** A recommender can look accurate while showing everyone the same few movies — coverage and diversity measure exactly what accuracy misses.

### Visualization (canvas `c1`, 720×300)

Two side-by-side 4×5 colored grids — each row is one user's top-5 list, each cell one movie colored by genre — recommender A (left) vs B (right), split by a vertical dashed divider at x=370.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "Top-5 Lists for Four Users: Recommender A vs Recommender B".
- **Genre colors:** action blue `#2a78d6`, comedy yellow `#c98500`, drama magenta `#d55181`, documentary aqua `#199e70`, sci-fi violet `#4a3aa7`.
- **Panel headers (bold 13px `#1a5276`, y=52):** "Recommender A" over left grid, "Recommender B" over right grid.
- **Left grid (A):** origin x=60, y=70; cells 52×34 with 4px gaps (row pitch 38); user labels "Maya", "Ben", "Ana", "Raj" 12px `#444` left of each row; every row identical: M1, M2, M3, M4 in action blue, M5 in comedy yellow; movie ids centered in white bold 11px.
- **Right grid (B):** origin x=400, same cell geometry; rows (genre order action, comedy, drama, documentary, sci-fi): Maya M1, M8, M14, M22, M31; Ben M2, M9, M15, M23, M32; Ana M1, M10, M16, M24, M31; Raj M3, M8, M17, M22, M34.
- **Annotations (y=240):** under A, orange `#d95926` bold 12px "same 5 movies for everyone → 5 distinct"; under B, green `#008300` bold 12px "16 distinct movies across 20 slots".
- **Legend (y=272):** five 12×12 swatches with genre names 11px `#444`, centered row: action, comedy, drama, documentary, sci-fi.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=370 from y=38 to h-12.

## Scoring Coverage and Diversity by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Coverage** — count distinct movies recommended and divide by catalog size: A scores 5/50 = 10%
- **B's coverage** — its 20 slots repeat only 4 movies, so 16 distinct of 50 gives 32%
- **Diversity** — take one list's 10 movie pairs and count the pairs from different genres
- **A's list** — four action films plus one comedy give 4 mixed pairs of 10, a diversity of 0.4
- **B's list** — five different genres make all 10 pairs mixed, a diversity of 1.0
- **Rule of thumb** — coverage is about the whole catalog; diversity is about one user's list

*Example (italic):* Maya's list from B — action, comedy, drama, documentary, sci-fi — has no two movies sharing a genre, so all 10 pairs count.

**Key point:** Both metrics are hand-countable: distinct items over catalog size for coverage, mixed-genre pairs over total pairs for diversity.

### Visualization (canvas `c2`, 720×300)

Dual-panel bar chart: catalog coverage (left) and intra-list diversity (right) for recommenders A and B, split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Coverage and Intra-List Diversity: Recommender A vs B".
- **Colors (used page-wide):** A = orange fill `rgba(217,89,38,0.55)` outline `#d95926`; B = green fill `rgba(0,131,0,0.45)` outline `#008300`.
- **Left panel (coverage):** axis origin x=55, width 280, baseline y=245, chart height 180, y scale 0–40%; two bars 70px wide: A = 10%, B = 32%; bold 13px value labels "10%" and "32%" above bars in each bar's outline color; bar names "A", "B" 12px `#444` below baseline; caption 12px `#444` "distinct movies ÷ 50-movie catalog".
- **Right panel (diversity):** axis origin x=400, width 280, same baseline/height, y scale 0–1.0; bars A = 0.4, B = 1.0 with bold 13px value labels "0.4" and "1.0"; green bold 12px annotation "B: every pair of movies differs in genre"; caption "mixed-genre pairs ÷ 10 pairs per top-5 list".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Happy Surprise: Serendipity

**Tags:** `core idea` (blue), `serendipity` (green), `worked example` (orange)

- **A hit** — a recommendation counts as a hit when the user watches it and rates it 4+ stars
- **The scores** — A lands 9 hits from its 20 slots and B lands 8, so plain accuracy favors A
- **Expected hits** — all 9 of A's hits are blockbusters the users had already seen trailers for
- **Serendipity** — a hit the user would not have found alone: unexpected AND liked, both at once
- **B's surprises** — 3 of B's 8 hits sit outside the user's usual genre yet still earn 4+ stars

*Example (italic):* Action-fan Maya taps the documentary M22 out of curiosity and rates it 5 stars — a serendipitous hit.

**Key point:** Serendipity credits only recommendations that are both unexpected and liked — an expected hit and a surprising flop both score zero.

### Visualization (canvas `c3`, 720×300)

Left panel: a 2×2 quadrant defining serendipity; right panel: stacked hit bars for A and B out of 20 slots; dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Serendipity = Unexpected AND Liked".
- **Left panel (quadrant):** 2×2 grid at x=70, y=70, cells 130×80 with 1px `#bdc3c7` borders; column headers 12px `#444` "expected" and "unexpected" above; row labels 12px `#444` "liked" and "not liked" at left; cell fills and bold 12px center labels: expected+liked `rgba(42,120,214,0.15)` "ordinary hit" in `#2a78d6`; unexpected+liked `rgba(0,131,0,0.2)` "SERENDIPITY" bold 13px in `#008300`; expected+not `#f0f2f5` "wasted slot" in `#6b7280`; unexpected+not `#f0f2f5` "noise" in `#6b7280`.
- **Right panel (stacked bars, out of 20 slots):** two horizontal bars at x=400, max width 280 (scale = width × count/20), 26px tall, at y=105 (A) and y=170 (B); A = 9 expected hits `rgba(42,120,214,0.55)` + 11 misses `#e5e9ef`; B = 5 expected hits `rgba(42,120,214,0.55)` + 3 serendipitous hits `rgba(0,131,0,0.55)` + 12 misses `#e5e9ef`; bold 12px labels above each bar in `#2c3e50`: "A: 9 hits, 0 surprises" and "B: 8 hits, 3 surprises".
- **Annotation (green `#008300` bold 12px, below B's bar):** "3 happy surprises that accuracy alone never credits".
- **Caption (12px `#444`, bottom right):** "hit = watched and rated 4+ stars (illustrative)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Popularity Bias and the Feedback Loop

**Tags:** `common mistake` (red), `popularity bias` (orange), `rule of thumb` (green)

- **Head, torso, tail** — the catalog splits into the 10 most-streamed movies, the next 15, and the last 25
- **A's slots** — all 20 of A's slots go to head movies, though the head is only 20% of the catalog
- **B's slots** — B spreads its slots 40% head, 40% torso, 20% tail, far closer to the catalog mix
- **The loop** — recommended movies get streamed more, which makes them look even more popular next week
- **Illustrative drift** — under A, the head's share of all streams climbs from 45% to 84% in 8 weeks

*Example (italic):* After two months on recommender A, the tail's 25 movies are effectively invisible in the app (illustrative).

**Common mistake:** Judging a recommender on accuracy alone — A "wins" 9 hits to 8 while burying 80% of the catalog and feeding a loop that makes popular titles ever more popular.

### Visualization (canvas `c4`, 720×300)

Left panel: grouped bars comparing catalog share vs slot share for head/torso/tail; right panel: 8-week head-share lines under A and B; dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Popularity Bias: Slot Shares and the Feedback Loop (illustrative)".
- **Left panel (grouped bars):** axis origin x=55, width 280, baseline y=240, chart height 170, y scale 0–100%; three groups "head", "torso", "tail" (12px `#444` below baseline), three 24px bars per group with 4px gaps: catalog share `#6b7280` fill `rgba(107,114,128,0.4)` = [20, 30, 50]; A `rgba(217,89,38,0.55)` = [100, 0, 0]; B `rgba(0,131,0,0.45)` = [40, 40, 20]; mini legend 11px top left: grey "catalog", orange "A slots", green "B slots"; orange `#d95926` bold 12px annotation "A: 100% of slots on the head".
- **Right panel (feedback loop lines):** axis origin x=400, width 280, baseline y=240, chart height 170, y range 40–90%; x = weeks 1–8 (12px `#444` labels); A line orange `#d95926` 3px with 4px dots = [45, 55, 63, 70, 75, 79, 82, 84]; B line green `#008300` 3px with 4px dots = [45, 46, 47, 47, 48, 48, 49, 49]; orange bold 12px annotation "popular gets more popular"; green bold 12px annotation "B stays in the mid-to-high 40s"; caption 12px `#444` "head's weekly share of all streams (illustrative)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
