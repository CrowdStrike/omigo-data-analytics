# Linear Programming & Simplex

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Linear Programming & Simplex

**Subtitle:** When limits are straight lines, the allowed plans form a polygon — and the best plan always sits at a corner, so simplex just walks corner to corner uphill

## One Morning at the Bakery

**Tags:** `core idea` (blue), `constraints` (orange), `feasible region` (green)

- **The choice** — a bakery picks M muffin batches and C cookie batches to bake each morning
- **Profit** — each muffin batch earns $30 and each cookie batch earns $20
- **Oven limit** — a muffin batch takes 2 oven-hours, a cookie batch takes 1, and the oven has 12
- **Flour limit** — every batch of either kind uses 5 kg of flour, and only 40 kg is on hand
- **The polygon** — plans that fit both limits form a four-cornered region; outside it, plans fail
- **The name** — choosing the best plan inside straight-line limits is a linear program (LP)

*Example (italic):* Baking 5 muffin and 4 cookie batches sounds nice, but it needs 14 oven-hours and 45 kg of flour — outside the polygon on both counts.

**Key point:** Straight-line limits fence off a polygon of allowed plans — the feasible region. The whole optimization happens inside (really, on the edge of) this shape.

### Visualization (canvas `c1`, 720×300)

Feasible-region plot: the two constraint lines and the shaded four-corner polygon of allowed bakery plans, with a legend/annotation column on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Every Plan the Oven and Flour Allow (feasible region)".
- **Plot geometry:** x-axis M = muffin batches 0–8, origin x=70, plot width 340; y-axis C = cookie batches 0–9, baseline y=250, plot height 205; map px = 70 + M/8·340, py = 250 − C/9·205; axes 2px ink `#1a5276`; ticks 12px `#444` at M = 0, 2, 4, 6, 8 and C = 0, 3, 6, 9; axis titles 12px `#444` "muffin batches (M)" below and "cookie batches (C)" rotated left.
- **Flour line:** green `#008300` 2.5px from (0,8) to (8,0); bold 12px green label "flour: 5M + 5C ≤ 40" near (5.4, 3.4).
- **Oven line:** orange `#d95926` 2.5px from (1.5,9) to (6,0); bold 12px orange label "oven: 2M + 1C ≤ 12" near (2.6, 7.6).
- **Feasible polygon:** vertices (0,0), (6,0), (4,4), (0,8); fill `rgba(42,120,214,0.15)`, border blue `#2a78d6` 2px.
- **Corners:** 5px ink dots with bold 12px ink labels "(0,0)", "(6,0)", "(4,4)", "(0,8)" offset away from the region.
- **Right annotation column (from x=460):** bold 13px magenta `#d55181`, two lines: "only these 4 corners" / "can hold the best plan"; below it 12px `#6b7280` legend: "profit: $30 per muffin batch", "$20 per cookie batch".
- **Caption (12px `#444`, bottom left):** "M = muffin batches, C = cookie batches (illustrative bakery)".

## Scoring the Four Corners

**Tags:** `worked example` (blue), `corner rule` (green)

- **Corner list** — the region has exactly four corners: (0,0), (6,0), (4,4), and (0,8)
- **Score each** — profit is 30·M + 20·C, so the corners score $0, $180, $200, and $160
- **By hand** — for (4,4): 30·4 + 20·4 = 120 + 80 = $200; every corner checks the same way
- **The winner** — (4,4) earns $200 and uses all 12 oven-hours and all 40 kg of flour
- **Corner rule** — a linear profit always peaks at a corner, so four checks settle everything

*Example (italic):* The muffins-only plan (6,0) uses all 12 oven-hours but leaves 10 kg of flour idle and earns only $180.

**Key point:** You never have to test the infinitely many interior plans — with a linear objective and straight-line limits, only the corners can win.

### Visualization (canvas `c2`, 720×300)

Bar chart of the profit at each of the four corners, with the winning corner highlighted in green.

- **Title (bold 15px, `#1a5276`, top center):** "Profit at Each Corner: 30·M + 20·C".
- **Data:** corners `["(0,0)", "(6,0)", "(4,4)", "(0,8)"]`, sublabels `["bake nothing", "muffins only", "the mix", "cookies only"]`, profits `[0, 180, 200, 160]`.
- **Axes:** origin x=70, baseline y=235, chart height 175, y scale $0–$220; horizontal gridlines `#e5e9ef` every $50 with 12px `#444` labels "$0", "$50", "$100", "$150", "$200".
- **Bars:** 80px wide, centered at x = 150, 290, 430, 570; fill `rgba(42,120,214,0.45)` with 1.5px blue `#2a78d6` border; winner (4,4) fill `rgba(0,131,0,0.5)` with 2px green `#008300` border; the $0 bar drawn as a 2px stub so the corner still registers.
- **Value labels:** bold 13px above each bar — "$0", "$180", "$200", "$160"; winner label green, others blue.
- **Category labels:** corner names 12px `#444` below baseline; sublabels 11px `#6b7280` beneath them.
- **Annotation (bold 13px green, above the winner):** "best: 4 muffin + 4 cookie batches".
- **Caption (12px `#444`, bottom left):** "four hand checks replace infinitely many interior plans".

## Simplex: Walk the Edges, Skip the Interior

**Tags:** `where it's used` (blue), `algorithm` (orange), `rule of thumb` (green)

- **Start anywhere** — simplex begins at an easy corner, here (0,0) with profit $0
- **Hop uphill** — it moves along an edge to a better neighbor: (0,0) → (6,0) lifts profit to $180
- **Hop again** — from (6,0), the edge to (4,4) lifts profit to $200
- **Stop rule** — at (4,4) no neighboring corner scores higher, so simplex declares it optimal
- **Why it scales** — big LPs have astronomically many corners, yet simplex usually visits few
- **Where you meet it** — ad budget splits, staff schedules, shipping plans, and diet blends are LPs

*Example (italic):* The walk touched 3 of the 4 corners and never priced a single plan in the interior.

**Key point:** Simplex never searches the inside — it walks corner to corner along edges, always uphill in profit, and stops the moment no edge improves.

### Visualization (canvas `c3`, 720×300)

The same feasible polygon with the simplex walk drawn as arrows along its edges, profits labeled at each stop.

- **Title (bold 15px, `#1a5276`, top center):** "The Simplex Walk: Corner to Corner, Always Uphill".
- **Plot geometry:** identical to c1 (origin x=70, width 340, baseline y=250, height 205, px = 70 + M/8·340, py = 250 − C/9·205; same axes and ticks).
- **Polygon:** vertices (0,0), (6,0), (4,4), (0,8); fill `rgba(42,120,214,0.10)`, border `#6b7280` 1.5px.
- **Walk arrows:** violet `#4a3aa7` 3px lines with filled 8px arrowheads: (0,0) → (6,0), then (6,0) → (4,4).
- **Stop markers:** 6px violet dots at (0,0) and (6,0); final corner (4,4) a 7px green `#008300` dot with a 2px green ring.
- **Step labels:** bold 12px violet "start: $0" near (0,0) and "hop 1: $180" near (6,0); bold 13px green "hop 2: $200 — stop" near (4,4).
- **Skipped corner:** (0,8) as a 5px `#6b7280` dot with 11px mute label "never visited".
- **Right annotation column (from x=460):** bold 13px violet, two lines: "2 hops, 3 corners —" / "interior never entered"; below it 12px `#6b7280`, three lines: "rule: take any edge", "that raises profit; stop", "when none does".

## Why the Middle Never Wins

**Tags:** `common mistake` (red), `iso-profit lines` (green)

- **The hunch** — a "balanced" middle plan like (3,3) feels safest, but it earns only $150
- **Iso-profit lines** — every plan on the line 30·M + 20·C = 150 earns exactly the same $150
- **Slide up** — pushing the line to $180, then $200 keeps touching the region until the last contact
- **Last touch** — the $200 line meets the region at exactly one point: the corner (4,4)
- **Whole batches** — needing integer batches is a harder problem; rounding an LP can break a limit

*Example (italic):* A manager who split resources "evenly" at (3,3) left $50 on the table every single morning.

**Common mistake:** Assuming the best trade-off sits somewhere in the middle. Straight profit lines slide until they exit the region, and the exit is always through a corner (or a whole edge) — never the interior.

### Visualization (canvas `c4`, 720×300)

The feasible polygon with dashed iso-profit lines sliding up to the solid $200 line, whose last touch is the winning corner; a beaten interior point is marked.

- **Title (bold 15px, `#1a5276`, top center):** "Iso-Profit Lines Slide Until the Last Touch — a Corner".
- **Plot geometry:** identical to c1 (origin x=70, width 340, baseline y=250, height 205, px = 70 + M/8·340, py = 250 − C/9·205; same axes and ticks).
- **Polygon:** vertices (0,0), (6,0), (4,4), (0,8); fill `rgba(42,120,214,0.12)`, border blue `#2a78d6` 1.5px.
- **Dashed iso-profit lines:** `#6b7280` 1.5px, dash 5/4: $60 from (2,0) to (0,3); $120 from (4,0) to (0,6); $180 from (6,0) to (0,9); each labeled 11px `#6b7280` "$60", "$120", "$180" at its upper-left end.
- **Winning line:** solid magenta `#d55181` 2.5px for $200 from (6.67,0) to (0.667,9); bold 13px magenta label "$200 — last feasible touch" near its upper end.
- **Slide arrow:** short `#6b7280` 2px arrow perpendicular to the lines (from near (2.2,2.2) toward (3.0,3.0)) with 11px mute label "profit rises".
- **Winner:** green `#008300` 7px dot at (4,4), bold 13px green label "(4,4) = $200".
- **Interior point:** orange `#d95926` 6px dot at (3,3), bold 12px orange label "'balanced' (3,3) = $150 — beaten".
- **Caption (12px `#444`, bottom left):** "every point on one dashed line earns the same profit; sliding up exits at the corner".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Charts c1, c3, and c4 share one plot-mapping helper (px = 70 + M/8·340, py = 250 − C/9·205) — define it once and reuse. All data is hardcoded; no randomness.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
