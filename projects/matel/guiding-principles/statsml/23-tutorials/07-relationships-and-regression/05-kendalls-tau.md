# Kendall's Tau

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Kendall's Tau

**Subtitle:** Kendall's tau scores agreement between two rankings by treating every pair of items as a tiny vote — same order counts for, flipped order counts against

## Two Judges Rank Five Restaurants

**Tags:** `core idea` (blue), `rank agreement` (green), `pairs` (orange)

- **The setup** — a food critic and a rating app each rank the same five restaurants A through E
- **Critic's order** — the critic says A, B, C, D, E; the app says A, C, B, E, D — close but not identical
- **A pair vote** — pick any two restaurants and ask: do both rankings name the same one as better?
- **Concordant** — a pair kept in the same order by both rankings (A vs D: both put A ahead)
- **Discordant** — a pair the two rankings flip (B vs C: the critic prefers B, the app prefers C)

*Example (italic):* For the pair B and C, the critic ranks B ahead but the app's users rank C ahead — that one pair is discordant.

**Key point:** Kendall's tau never asks how far apart the ranks are — it only asks, pair by pair, "same order or flipped?"

### Visualization (canvas `c1`, 720×300)

Slope chart connecting the critic's ranking (left column) to the app's ranking (right column); each line crossing marks one discordant pair.

- **Title (bold 15px, `#1a5276`, top center):** "Critic vs App: Five Restaurants, Two Rankings".
- **Data:** critic order top-to-bottom `["A","B","C","D","E"]`; app order top-to-bottom `["A","C","B","E","D"]`; rank rows at y = 75, 115, 155, 195, 235.
- **Columns:** left endpoints at x=200, right endpoints at x=520; column headings bold 12px `#6b7280` — "critic's ranking" centered above x=200 at y=52, "app's ranking" above x=520.
- **Labels:** restaurant letters bold 13px `#1a5276`, drawn 14px outside each endpoint (left of x=200, right of x=520); small "1"–"5" rank numbers 11px `#6b7280` at the far left margin x=160.
- **Lines:** blue `#2a78d6` 3px with 5px dots at both ends — A: (200,75)→(520,75); B: (200,115)→(520,155); C: (200,155)→(520,115); D: (200,195)→(520,235); E: (200,235)→(520,195).
- **Crossings:** B/C lines cross at (360,135), D/E at (360,215); mark each with an 8px magenta `#d55181` circle (3px stroke, no fill).
- **Annotation (bold 13px magenta `#d55181`, centered at y=272):** "2 crossings = 2 discordant pairs".

## Counting All Ten Pairs by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **All pairs** — five restaurants make 10 pairs: AB, AC, AD, AE, BC, BD, BE, CD, CE, DE
- **Score each** — 8 pairs keep the same order in both rankings; only BC and DE flip
- **The formula** — tau = (concordant − discordant) / total pairs = (8 − 2) / 10 = 0.6
- **The range** — identical rankings score +1, exactly reversed score −1, unrelated ones sit near 0
- **Hand check** — n items make n(n−1)/2 pairs, so 5 restaurants give 10, easy to list on paper

*Example (italic):* Scoring the 10 pairs takes a minute by hand: 8 agree, 2 flip, so tau = (8 − 2) / 10 = 0.6.

**Key point:** Tau is just a pair tally: (agreeing pairs − flipped pairs) divided by all pairs. No squaring, no distances — counting is the whole method.

### Visualization (canvas `c2`, 720×300)

Grid of 10 pair tiles (5 columns × 2 rows), green for concordant and magenta for discordant, with the tau formula assembled underneath.

- **Title (bold 15px, `#1a5276`, top center):** "Scoring All 10 Pairs: 8 Concordant, 2 Discordant".
- **Data:** row 1 pairs `["A–B","A–C","A–D","A–E","B–C"]`, row 2 pairs `["B–D","B–E","C–D","C–E","D–E"]`; discordant pairs are "B–C" and "D–E", the other 8 are concordant.
- **Tiles:** 100×54 rounded rectangles (4px radius); grid starts at x=70, y=64; horizontal pitch 118, vertical pitch 70 (row 2 at y=134).
- **Concordant tile style:** fill `rgba(0,131,0,0.12)`, 2px `#008300` border; pair label bold 13px `#008300` centered at tile mid minus 8; word "agree" 11px `#008300` centered 14px below the label.
- **Discordant tile style:** fill `rgba(213,81,129,0.12)`, 2px `#d55181` border; pair label bold 13px `#d55181`; word "flip" 11px `#d55181` below.
- **Formula (bold 14px `#1a5276`, centered at y=248):** "tau = (8 − 2) / 10 = 0.6".
- **Caption (12px `#6b7280`, centered at y=278):** "concordant = same order in both rankings; discordant = flipped".

## When Tau and Spearman Split

**Tags:** `where it's used` (blue), `vs Spearman` (orange), `failure mode` (red)

- **Same critic list** — keep the critic's A–E order fixed and compare two different app rankings
- **Two neighbor swaps** — the app order B, A, D, C, E gives tau 0.6 but Spearman 0.8
- **One big fall** — the app buries A dead last (B, C, D, E, A): tau 0.2 while Spearman crashes to 0.0
- **Why they split** — Spearman squares each rank gap, so A's four-step fall costs 16 all by itself
- **Tau's view** — tau charges every flipped pair the same one unit, so no single item can dominate

*Example (italic):* The app burying the critic's favorite reads as "no relationship" to Spearman (0.0) but as mild agreement to tau (0.2).

**Key point:** Spearman weights disagreements by squared rank distance; tau counts flipped pairs equally. One badly misplaced item hits Spearman far harder than tau.

### Visualization (canvas `c3`, 720×300)

Dual-panel grouped bar chart comparing tau and Spearman for two app rankings against the same critic order, split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same Critic Order, Two App Rankings: Tau vs Spearman".
- **Legend (at y=48):** 10px aqua `#199e70` square + "Kendall tau" 12px `#444` starting at x=250; 10px violet `#4a3aa7` square + "Spearman rho" starting at x=390.
- **Shared axes:** baseline y=245, chart height 170, value scale 0–1; light gridlines `#e5e9ef` at 0.25/0.5/0.75/1.0 with 11px `#6b7280` labels on the far left (x=48).
- **Left panel (two neighbor swaps):** heading bold 12px `#444` centered at (200,72) — "app: B, A, D, C, E"; tau bar at x=110 width 60 height 0.6 of scale, fill `rgba(25,158,112,0.55)`, 2px `#199e70` border, bold 13px `#199e70` value label "0.6" above; Spearman bar at x=210 width 60 height 0.8, fill `rgba(74,58,167,0.45)`, 2px `#4a3aa7` border, label "0.8".
- **Right panel (critic's #1 buried last):** heading centered at (520,72) — "app: B, C, D, E, A"; tau bar at x=450 width 60 height 0.2, same aqua styling, label "0.2"; Spearman value 0.0 drawn as a 2px violet tick on the baseline at x=550 (width 60) with bold 13px `#4a3aa7` label "0.0" above it.
- **Annotation (bold 12px magenta `#d55181`, two lines, right-aligned near x=660, y=110):** "one big displacement:" / "Spearman 0.0, tau still 0.2".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=40 to h-12.
- **Caption (12px `#444`, centered at y=282):** "critic order fixed at A, B, C, D, E in both panels".

## Reading the Number Right

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Smaller by design** — tau 0.6 and Spearman 0.8 can describe the same data; tau is not "weaker"
- **Direct meaning** — (tau + 1) / 2 is the share of concordant pairs: tau 0.6 means 80% agree
- **Rule of thumb** — on typical data tau lands near two-thirds of Spearman for the same pairing
- **Ties** — when judges hand out tied ranks, use the tau-b variant, which adjusts the denominator
- **Wrong yardstick** — judging a tau of 0.6 against Spearman-style cutoffs undersells the agreement

*Example (italic):* An analyst flagged tau = 0.6 as "moderate at best" using a Spearman cutoff sheet — yet it means 8 of every 10 pairs agree.

**Common mistake:** Comparing a tau value against thresholds calibrated for Spearman or Pearson. Tau runs systematically smaller; translate it to "% of pairs concordant" before judging strength.

### Visualization (canvas `c4`, 720×300)

Stacked pair-share bar plus a tau-to-percent conversion scale, showing that tau 0.6 means 80% of pairs concordant.

- **Title (bold 15px, `#1a5276`, top center):** "What tau = 0.6 Really Says: 80% of Pairs Agree".
- **Stacked bar (y=72, height 34, from x=70, total width 580):** green segment `rgba(0,131,0,0.5)` width 464 (8/10) with bold 13px white centered label "8 concordant pairs"; magenta segment `rgba(213,81,129,0.55)` width 116 (2/10) with bold 12px white label "2 flip"; 1px `#999` outline around the whole bar.
- **Conversion line (bold 13px `#1a5276`, centered at y=142):** "(tau + 1) / 2 = (0.6 + 1) / 2 = 0.80 → 80% of pairs concordant".
- **Tau scale:** horizontal 2px `#999` line at y=205 from x=70 to x=650; ticks at tau = −1, 0, +1 mapped to x = 70, 360, 650 with two-row 12px `#444` labels ("tau −1" / "0% agree", "tau 0" / "50% agree", "tau +1" / "100% agree").
- **Marker:** blue `#2a78d6` 7px dot at x=534 (tau 0.6 → 70 + 580 × 1.6 / 2) with bold 13px `#2a78d6` label "tau 0.6" above at y=185.
- **Takeaway (bold 12px green `#008300`, centered at y=272):** "rule of thumb: tau ≈ two-thirds of Spearman — a tau of 0.6 typically pairs with Spearman near 0.8 (illustrative)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
