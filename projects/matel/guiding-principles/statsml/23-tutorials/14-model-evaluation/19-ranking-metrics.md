# Ranking Metrics

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Ranking Metrics

**Subtitle:** When a model returns a ranked list, position is everything — precision@k, MRR, and NDCG all reward putting the right answers near the top

## One Search, Five Results

**Tags:** `core idea` (blue), `precision@k` (green), `position matters` (orange)

- **The search** — a recipe app returns 5 results for "chicken curry"; only ranks 1 and 3 are actual curries
- **The user** — nobody reads all 5 results; most people only ever look at the top 1 to 3
- **Precision@k** — the share of the top k results that are relevant, ignoring everything below k
- **Computed here** — p@1 = 1.00, p@2 = 0.50, p@3 = 0.67, p@4 = 0.50, p@5 = 0.40
- **Falling curve** — p@k usually drops as k grows, because deeper slots add mostly junk

*Example (italic):* The top result "Classic Chicken Curry" is relevant, so precision@1 = 1/1 = 1.00 — but by rank 5 only 2 of 5 hits are curries, so precision@5 = 0.40.

**Key point:** Precision@k grades only the window the user actually sees. Pick k to match real behavior — a phone screen showing 3 results means p@3 is the honest score.

### Visualization (canvas `c1`, 720×300)

Dual panel: the ranked result list with relevance marks (left) and a bar chart of precision@k for k = 1..5 (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One Search for 'chicken curry': the Ranked List and Its Precision@k".
- **Data:** ranked results `["Classic Chicken Curry", "Beef Stew", "Thai Chicken Curry", "Pancake Stack", "Fish Tacos"]` with relevance flags `[1, 0, 1, 0, 0]`; precision@k values `[1.00, 0.50, 0.67, 0.50, 0.40]`.
- **Left panel (list):** five 300×34 rounded rows from x=30, first row top y=55, 8px vertical gap; each row shows bold 12px ink rank number "1."–"5." then the recipe name 12px `#2c3e50`; relevant rows fill `rgba(0,131,0,0.12)` with a green `#008300` bold 13px check "✓ curry" at the right edge; irrelevant rows fill `#f4f6f8` with a mute `#6b7280` 12px "✗" mark; caption 12px `#444` below: "relevance is fixed: ranks 1 and 3 are curries".
- **Right panel (bars):** axis origin x=410, width 270, baseline y=245, chart height 175, y scale 0–1.1 with gridlines `#e5e9ef` at 0.25/0.50/0.75/1.00 (11px mute labels); five bars fill `rgba(42,120,214,0.45)` with blue `#2a78d6` 2px top edge, x labels "p@1".."p@5" 12px `#444`, exact values bold 12px blue above each bar; orange `#d95926` bold 12px annotation above the p@5 bar: "deeper k = more junk counted".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## How Fast Do Users Find the First Hit?

**Tags:** `worked example` (blue), `MRR` (green)

- **Three searches** — "chicken curry" hits at rank 1, "vegan lasagna" at rank 3, "gluten-free bread" at rank 4
- **Reciprocal rank** — each query scores 1/rank of its first relevant result: 1.00, 0.33, 0.25
- **MRR** — the mean of those reciprocal ranks: (1.00 + 0.33 + 0.25) / 3 ≈ 0.53
- **Steep penalty** — dropping from rank 1 to rank 2 costs half the score; rank 4 keeps only a quarter
- **One-hit metric** — MRR only cares about the FIRST relevant result; later hits add nothing

*Example (italic):* The lasagna query's first real lasagna sits at rank 3, so that query contributes 1/3 = 0.33 to the average, no matter how good ranks 4 and 5 are.

**Key point:** MRR answers "how far down must the user scan before the first good result?" — perfect is 1.00, and every extra scroll position cuts the score by a harsh 1/rank curve.

### Visualization (canvas `c2`, 720×300)

Three query rows of five result slots each with the first relevant slot highlighted (left), and reciprocal-rank bars with the MRR line (right), split by a dashed divider at x=390.

- **Title (bold 15px, `#1a5276`, top center):** "Three Searches: First Relevant Result and Reciprocal Rank".
- **Data:** queries `["chicken curry", "vegan lasagna", "gluten-free bread"]`; first-relevant ranks `[1, 3, 4]`; reciprocal ranks `[1.00, 0.33, 0.25]`; MRR `0.53`.
- **Left panel (slots):** three rows at y = 80, 150, 220; query name bold 12px ink above each row; five 46×30 slot squares per row starting x=40 with 10px gaps, each showing its rank number 11px mute; slots before the first hit fill `#f4f6f8` with a `#d0d5db` border; the first relevant slot fills `rgba(0,131,0,0.25)` with a 2px green `#008300` border and a bold green "✓"; slots after the hit fill `#fbfcfd` (MRR ignores them); aqua `#199e70` bold 12px label right of each row: "RR = 1.00", "RR = 0.33", "RR = 0.25".
- **Right panel (bars):** axis origin x=440, width 240, baseline y=245, chart height 175, y scale 0–1.1; three bars fill `rgba(25,158,112,0.45)` with aqua `#199e70` 2px top edge, heights 1.00 / 0.33 / 0.25, values bold 12px aqua above, x labels "curry", "lasagna", "bread" 12px `#444`; violet `#4a3aa7` dashed 2px horizontal line at y for 0.53 labeled bold 13px violet "MRR = 0.53"; caption 12px `#444` "mean of the three reciprocal ranks".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=390 from y=38 to h-12.

## When Results Aren't Just Yes/No

**Tags:** `worked example` (blue), `NDCG` (green), `graded relevance` (orange)

- **Grades, not flags** — an editor rates each result 0–3: perfect match 3, good 2, okay 1, off-topic 0
- **The ranking** — the app's order carries grades [2, 3, 0, 1, 0]; the perfect-3 recipe sits at rank 2
- **Discounting** — each grade is divided by log2(rank + 1), so rank 1 counts full and rank 5 counts ~0.39×
- **DCG** — 2/1.00 + 3/1.58 + 0 + 1/2.32 + 0 = 2.00 + 1.89 + 0.43 = 4.32
- **Normalizing** — the ideal order [3, 2, 1, 0, 0] gives IDCG = 4.76, so NDCG = 4.32 / 4.76 = 0.91

*Example (italic):* Swapping the top two results would move the grade-3 recipe to rank 1 and lift DCG from 4.32 toward the ideal 4.76 — NDCG measures exactly that gap.

**Key point:** NDCG rewards putting the best-graded items highest, discounts each slot by log2(rank+1), and divides by the ideal ordering so every query scores on the same 0–1 scale.

### Visualization (canvas `c3`, 720×300)

Dual panel of per-rank DCG contribution bars: the app's actual ranking (left) vs the ideal reordering of the same five grades (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "DCG per Rank: App's Order vs Ideal Order of the Same 5 Grades".
- **Data:** app grades `[2, 3, 0, 1, 0]` with contributions `[2.00, 1.89, 0, 0.43, 0]` (DCG 4.32); ideal grades `[3, 2, 1, 0, 0]` with contributions `[3.00, 1.26, 0.50, 0, 0]` (IDCG 4.76); discounts log2(rank+1) = `[1.00, 1.58, 2.00, 2.32, 2.58]`.
- **Left panel (app):** axis origin x=55, width 280, baseline y=240, chart height 170, y scale 0–3.3; five bars fill `rgba(42,120,214,0.45)` with blue `#2a78d6` 2px top edge; grade shown as bold 12px ink "g=2" style label inside/above each bar and the contribution value bold 12px blue on top; x labels "r1".."r5" 12px `#444`; caption 12px `#444` "DCG = 2.00 + 1.89 + 0.43 = 4.32"; magenta `#d55181` bold 12px annotation over rank 2: "grade-3 stuck at rank 2".
- **Right panel (ideal):** axis origin x=400, width 280, same baseline/height/scale; bars fill `rgba(0,131,0,0.4)` with green `#008300` 2px top edge, same label style; caption "IDCG = 3.00 + 1.26 + 0.50 = 4.76".
- **Takeaway (bold 13px green `#008300`, bottom center):** "NDCG = 4.32 / 4.76 = 0.91".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Set-Metric Trap

**Tags:** `common mistake` (red), `position matters` (orange)

- **Same items** — rankings A and B contain the exact same 5 results; only the order differs
- **Ranking A** — relevance [1, 0, 1, 0, 0]: hits at ranks 1 and 3, so p@1 = 1.00 and RR = 1.00
- **Ranking B** — relevance [0, 0, 1, 0, 1]: hits at ranks 3 and 5, so p@1 = 0.00 and RR = 0.33
- **The trap** — a set metric over the top 5 scores both identically: p@5 = 2/5 = 0.40 either way
- **The rule** — if your metric can't tell A from B, it is not a ranking metric at all

*Example (italic):* A team celebrated a stable 0.40 precision@5 for weeks while a bug had quietly pushed every relevant result out of the top spot.

**Common mistake:** Evaluating a ranker with order-blind metrics like accuracy or whole-list precision. Two rankings with identical items can differ hugely where it counts — the top.

### Visualization (canvas `c4`, 720×300)

Two ranked lists of the same five items side by side, with a three-metric scoreboard underneath showing where they agree and where they split.

- **Title (bold 15px, `#1a5276`, top center):** "Same 5 Results, Two Orders: Only Position-Aware Metrics See the Difference".
- **Data:** ranking A relevance `[1, 0, 1, 0, 0]`, ranking B relevance `[0, 0, 1, 0, 1]`; scoreboard rows `p@5: 0.40 vs 0.40 (tie)`, `p@1: 1.00 vs 0.00`, `RR: 1.00 vs 0.33`.
- **Ranking columns:** heading bold 13px blue `#2a78d6` "Ranking A" at x=140 and bold 13px orange `#d95926` "Ranking B" at x=470, y=50; each column shows five 200×26 rows (A from x=60, B from x=400, first row top y=62, 5px gaps) labeled "1."–"5." bold 12px ink; relevant rows fill `rgba(0,131,0,0.15)` with a green `#008300` "✓" and label "curry", irrelevant rows fill `#f4f6f8` with a mute `#6b7280` "✗".
- **Scoreboard (from y=225):** three text lines centered at x=360, 13px; "precision@5:  A 0.40  =  B 0.40" in mute `#6b7280` with a bold mute tag "order-blind: tie"; "precision@1:  A 1.00  vs  B 0.00" bold with A's value blue and B's orange; "reciprocal rank:  A 1.00  vs  B 0.33" bold, same coloring.
- **Takeaway (bold 13px magenta `#d55181`, bottom center):** "an order-blind metric calls these equal — the top-1 user disagrees".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
