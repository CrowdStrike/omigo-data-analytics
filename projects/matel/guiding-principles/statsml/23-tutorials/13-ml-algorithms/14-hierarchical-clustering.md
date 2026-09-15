# Hierarchical Clustering

**Page type:** detail page (tutorial card-sections: one h2 per section, two-column `table.layout` text 50% / canvas 50%; section 3 places two canvases side by side in its viz cell)
**HTML title tag:** Hierarchical Clustering

**Subtitle:** Merge the two closest groups, again and again — the merge history is a tree you can cut at any height to get any number of clusters

## Everyone Starts Alone, the Closest Pair Merges First

Tags: `core idea` (blue), `running example` (green)

- **The setup** — the same customer data, but start with every customer as a group of one
- **One rule** — merge the two closest groups, and write down how far apart they were
- **Repeat** — keep merging until only one giant group remains; the history is a tree
- **Five customers by spend** — Ana $10, Ben $12, Cai $30, Dee $34, Eli $80
- **First merge** — Ana and Ben, only $2 apart; no other pair comes close

*Example (italic):* A family tree built bottom-up: siblings join first, distant cousins last.

**Key point:** **Hierarchical clustering never asks for k:** it records every merge and the distance it happened at. How many groups to read off is a decision you make afterwards.

### Visualization (canvas `c1`, 720×300)

Number line of the five customers by spend, with numbered merge-order arcs above.

- **Title (bold 15px, `#1a5276`, top center):** "Five Customers on the Spend Line, and the Merge Order".
- **Number line:** at y=210, spanning $0–$90 with tick labels every $15 ("$0"…"$90"), axis label "monthly spend" below center; gray `#999` line, muted `#6b7280` 12px labels.
- **Points (8px dots with bold labels above, offset so Ana/Ben and Cai/Dee do not overlap):** Ana $10 and Ben $12 in aqua `#199e70`; Cai $30 and Dee $34 in violet `#4a3aa7`; Eli $80 in yellow `#c98500`. Labels "Ana $10", "Ben $12", "Cai $30", "Dee $34", "Eli $80".
- **Merge arcs (quadratic arcs above the line, 2.5px, bold 13px labels at apex):**
  - "1st: gap $2" — aqua arc from $10 to $12, rise 22
  - "2nd: gap $4" — violet arc from $30 to $34, rise 22
  - "3rd: gap $21" — green `#008300` arc from $11 to $32, rise 62
  - "4th: gap $58.50" — magenta `#d55181` arc from $21.5 to $80, rise 105
- **Caption (bold magenta 12px, bottom center):** "always the closest pair of groups next — Eli waits until the very end".

## Four Merges, Then the Dendrogram

Tags: `worked example` (green), `arithmetic` (blue)

- **Merge 1** — Ana + Ben, gap $2; the pair now acts as one group at its average, $11
- **Merge 2** — Cai + Dee, gap $4; that group's average is $32
- **Merge 3** — {Ana, Ben} + {Cai, Dee}: averages $11 vs $32, so the gap is $21
- **Merge 4** — Eli joins last; $80 sits $58.50 above the others' average of $21.50
- **The tree** — every merge draws a bridge at its gap height; a low bridge means very similar

*Example (italic):* Eli's bridge is 29× higher than Ana–Ben's — he barely belongs to the family.

**Key point:** **The dendrogram is data, not decoration:** bridge heights are exactly the recorded merge distances — $2, $4, $21, $58.50 — so you can check the whole tree by hand.

### Visualization (canvas `c2`, 720×300)

Full dendrogram with a merge-distance axis and every bridge height labeled.

- **Title (bold 15px, `#1a5276`, top center):** "The Dendrogram: Every Bridge Sits at Its Merge Distance".
- **Geometry:** leaves at x = 130 (Ana), 230 (Ben), 350 (Cai), 450 (Dee), 610 (Eli); baseline y=250, tree height 195px, height scale $0–65.
- **Height axis (left, x=62):** tick labels "$0", "$20", "$40", "$60" with light `#e5e9ef` gridlines; rotated axis title "merge distance".
- **Tree structure (2.5px lines):** leaf stems rise from the baseline; bridges at merge heights — Ana–Ben bridge at $2 (aqua `#199e70`), Cai–Dee bridge at $4 (violet `#4a3aa7`), {Ana,Ben}+{Cai,Dee} bridge at $21 (green `#008300`, with risers from the two pair-midpoints), final bridge to Eli at $58.50 (magenta `#d55181`); Eli's stem is yellow `#c98500` and runs all the way up to $58.50.
- **Leaf labels (bold dark 12px below baseline):** "Ana $10", "Ben $12", "Cai $30", "Dee $34", "Eli $80".
- **Bridge height labels (bold 12px in bridge color):** "$2", "$4", "$21", "$58.50" just above each bridge.
- **Annotation (bold magenta 13px, left-aligned at (480, 104), two lines):** "low bridges = tight pairs;" / "Eli joins 29× higher up".

## Cut the Tree Anywhere, Get Any Number of Groups

Tags: `payoff` (blue), `rule of thumb` (green). The viz cell holds both canvases side by side in a `.viz-pair` flex row.

- **One run, every answer** — cutting at a height reads off the groups hanging below the cut
- **Cut at $10** — three groups: {Ana, Ben}, {Cai, Dee}, {Eli}
- **Cut at $40** — two groups: {Ana, Ben, Cai, Dee} and {Eli}
- **No k up front** — unlike k-means, you pick the group count after seeing the structure
- **Rule of thumb** — cut across the widest empty stretch of heights; $21–$58.50 says 2 groups, $4–$21 says 3

*Example (italic):* The same tree answers this month's "give me 2 tiers" and next month's "give me 3".

**Key point:** **k-means must rerun for every k;** hierarchical clustering pays once and every k is a horizontal line away.

### Visualization (canvas `c3a`, 310×340)

Same dendrogram (leaves at x = 56, 106, 166, 216, 278; baseline y=272, height 200px) with a cut line at $10.

- **Title (bold 15px, `#1a5276`, top center):** "Cut at $10 → 3 Groups".
- **Tree:** leaf colors aqua/aqua/violet/violet/yellow; the two upper bridges ($21 and $58.50) are grayed out `#b9c2cc` since they sit above the cut.
- **Cut line:** dashed magenta `#d55181` horizontal line (dash 7/5, width 2.5) at height $10, labeled bold magenta 12px "cut height $10" at the left.
- **Leaf labels:** "Ana", "Ben", "Cai", "Dee", "Eli" below the baseline; group labels below them — "group 1" (aqua, under Ana/Ben), "group 2" (violet, under Cai/Dee), "group 3" (yellow, under Eli).
- **Caption (muted 12px near top):** "3 branches cross the line → 3 groups".

### Visualization (canvas `c3b`, 310×340)

Same dendrogram geometry with a cut line at $40.

- **Title (bold 15px, `#1a5276`, top center):** "Cut at $40 → 2 Groups".
- **Tree:** the four merged leaves (Ana–Dee) and the $21 bridge all in green `#008300`; Eli's stem yellow; the $58.50 bridge grayed out `#b9c2cc`.
- **Cut line:** dashed magenta horizontal line at height $40, labeled "cut height $40".
- **Leaf labels:** "Ana"…"Eli"; group labels — "group 1" (green, centered under Ana–Dee), "group 2" (yellow, under Eli).
- **Caption (muted 12px near top):** "same tree, higher cut → 2 groups".

## Reading It Right, and Where It Creaks

Tags: `common mistake` (red), `caution` (orange)

- **Stability read** — a group count that survives a wide range of cut heights is trustworthy
- **Here** — "2 groups" holds for any cut from $21 to $58.50; "4 groups" only from $2 to $4
- **Greedy** — every merge is final; an early awkward merge can never be undone later
- **Cost** — comparing all pairs grows like n²: fine for 200 customers, painful for millions
- **"Closest" needs a definition** — nearest, farthest, or average members each grow a different tree

*Example (italic):* Scaling spend from dollars to cents rescales every height — the ruler matters, as in kNN.

**Key point:** **The confusion:** the tree always exists, even for structureless data. Evidence of real clusters is a wide flat step in the staircase — not the mere fact that merges happened.

### Visualization (canvas `c4`, 720×300)

Staircase step chart: number of groups vs cut height.

- **Title (bold 15px, `#1a5276`, top center):** "Groups vs Cut Height: Wide Steps Are the Stable Answers".
- **Data (steps: cut-height start, end, group count):** `[0, 2, 5]`, `[2, 4, 4]`, `[4, 21, 3]`, `[21, 58.5, 2]`, `[58.5, 65, 1]`; step colors gray `#6b7280`, yellow `#c98500`, green `#008300`, violet `#4a3aa7`, gray.
- **Axes:** x = cut height $0–65 with labels every $10 ("$0"…"$60"), title "cut height (merge distance)"; y = number of groups 1–5 with integer labels, rotated title "number of groups"; padding top 48 / bottom 52 / left 70 / right 30; gray `#999` L axes.
- **Steps:** 4px horizontal segments at each group level, connected by light dashed `#c8ced6` vertical risers (dash 3/3).
- **Annotations:** bold violet 13px "\"2 groups\" survives any cut from $21 to $58.50 — widest step" above the 2-group step; bold green 13px "\"3 groups\": $4 to $21" above the 3-group step; bold yellow 12px "\"4 groups\": only $2 to $4 — fragile" beside the 4-group step.

## Regeneration instructions

- **Template/layout:** tutorials topic page. h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` with one row: every section uses `td.text-col` 50% + `td.viz-col` 50%; sections 1, 2 and 4 hold one 720×300 canvas. One section places canvases `c3a`/`c3b` (310×340 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Text column structure:** `.tags` row of pill spans (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem, weight 600, radius 10px); `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`; italic `.example` paragraph (`#555`, 0.9rem); `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>` lead-in.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; shared `setup(id)` helper reading the canvas width/height attributes, scaling the backing store by `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates; shared customer data `CUST = [['Ana',10],['Ben',12],['Cai',30],['Dee',34],['Eli',80]]` and a shared `drawDendro(ctx, geom, opts)` helper (max height 65; merge heights 2, 4, 21, 58.5; leaf colors and upper-bridge colors passed per chart) used by c2, c3a and c3b; all data hardcoded (no `Math.random()`).
- **Palette:** primary blue/ink `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; chart series use the P palette above.
- **Links:** none on this page (no cross-page links, no nav); in regenerated HTML any card links elsewhere use `.html` extensions.
