# The Ecological Fallacy

**Page type:** detail page (tutorial page: h2 card-sections, each a two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** The Ecological Fallacy

**Subtitle:** A pattern between group averages can point one way while the pattern between the people inside each group points the other — group facts are not individual facts

## Richer Streets, Bigger Grocery Bills

**Tags:** `core idea` (blue), `running example` (green)

- **Three areas** — Oakwood, Maplehill, Riverview: average incomes $40k, $70k, $100k
- **Average spend** — monthly grocery spend averages $120, $150, $180 in the same order
- **Clean line** — across neighborhoods, spend rises $1 for every extra $1k of income
- **The leap** — the marketer concludes richer people spend more, so target the richest
- **The name** — reading a group pattern as an individual pattern is the ecological fallacy

*Example:* The three neighborhood averages sit on a perfect upward line — the temptation to extrapolate is strong.

**Key point:** A pattern between group averages says nothing, by itself, about the people inside the groups.

### Visualization (canvas `c1`, 720×300)

Scatter plot of the three neighborhood averages on a rising trend line.

- **Title (bold 15px, `#1a5276`, top center):** "Neighborhood Averages: Spend Rises with Income"
- **Axes:** L-shaped axis in `#999`; padding left 80, right 40, top 45, bottom 50; x range 20–120 ($k income), y range 80–220 ($ spend). X tick labels "$40k", "$70k", "$100k" at those incomes; x-axis title "average income". Y tick labels "$120", "$150", "$180"; rotated y-axis title "avg monthly spend". Labels 12px muted gray `#6b7280`.
- **Trend line:** blue `#2a78d6`, width 2.5, from (25,105) to (115,195) in data coordinates (spend = income + 80).
- **Points (radius 9):** Oakwood (40,120) orange `#d95926`; Maplehill (70,150) aqua `#199e70`; Riverview (100,180) violet `#4a3aa7`. Bold 12px name label above each point in its color; muted "$120"/"$150"/"$180" value below each point.
- **Annotation (bold 13px blue `#2a78d6`, left-aligned near bottom-left of plot):** "+$1 of spend per $1k of income — for neighborhoods"

## Meet the Twelve Shoppers

**Tags:** `worked example` (green)

- **Four per area** — inside Oakwood the incomes are $30k, $35k, $45k, $50k
- **Their spend** — those four spend $140, $130, $110, $100: the richest spends the least
- **Same inside all** — Maplehill and Riverview each slope downward within the cluster too
- **Down inside** — within a neighborhood, spend drops $2 for every extra $1k of income
- **Both are true** — averages rise across areas while spend falls within every one of them

*Example:* The $80k Maplehill shopper spends $130 a month while the $60k neighbor spends $170.

**Key point:** The between-group line and the within-group line can point in opposite directions in the same data.

### Visualization (canvas `c2`, 720×300)

Scatter plot of all twelve individual shoppers: three clusters each sloping down, with the rising between-group line dashed through the cluster averages.

- **Title (bold 15px, `#1a5276`, top center):** "Same Data, Zoomed In: Every Neighborhood Slopes Down"
- **Axes:** same frame as c1 (x 20–120, y 80–220, padding 80/40/45/50). X tick labels "$40k", "$70k", "$100k"; x-axis title "shopper income". Y tick labels "$100", "$150", "$200"; rotated y-axis title "monthly spend".
- **Between-group line:** dashed blue `#2a78d6` (dash 6/4, width 2) from (30,110) to (110,190) in data coordinates.
- **Individual points (radius 6) and within-group trend lines (solid, width 2.5, cluster color, extended slightly past the first/last point):**
  - Oakwood, orange `#d95926`: (30,140), (35,130), (45,110), (50,100)
  - Maplehill, aqua `#199e70`: (60,170), (65,160), (75,140), (80,130)
  - Riverview, violet `#4a3aa7`: (90,200), (95,190), (105,170), (110,160)
  - Bold 12px cluster name label in cluster color above each cluster (at the cluster's average income, above its first point).
- **Annotations (bold 13px):** blue "between groups: up" (lower right area, at x≈75k, y≈203 data coords); magenta `#d55181` "within each group: down" (upper left, x≈24k, y≈96... rendered near top of plot).

## The Campaign Targets the Wrong People

**Tags:** `where it's used` (blue), `common mistake` (red)

- **The plan** — a mailer goes to the richest shopper in each area, expecting the biggest baskets
- **The line says** — plug $80k into the neighborhood line and it predicts $160 a month
- **Reality says** — that shopper actually spends $130, the least in all of Maplehill
- **Backwards** — the group line ranks neighbors in exactly the wrong order
- **Classic cases** — voting by state vs by voter, disease by country vs by person

*Example:* A famous 1950 study: states with more immigrants had higher literacy, yet immigrants themselves had lower.

**Key point:** Use group data to compare groups; to target or score individuals, you need individual-level data.

### Visualization (canvas `c3`, 720×300)

Paired bar chart: predicted (from the group line) vs actual spend for two Maplehill shoppers.

- **Title (bold 15px, `#1a5276`, top center):** "Two Maplehill Shoppers: the Group Line Ranks Them Backwards"
- **Axes:** L-shaped axis in `#999`; baseline y=240, chart height 165px, value max $220; y labels "$0" and "$200" in muted gray.
- **Groups (paired bars 74px wide, 8px from center):**
  - "$60k shopper" (cx=220): line predicts $140, actually spends $170
  - "$80k shopper" (cx=510): line predicts $160, actually spends $130
- **Colors:** predicted bars translucent blue `rgba(42,120,214,0.35)`; actual bars aqua `#199e70`.
- **Labels:** bold 13px dollar values above bars in `#2c3e50`; muted 12px "line predicts" / "actually spends" under each bar; bold shopper name centered below at baseline+34.
- **Annotation (bold 13px red `#e74c3c`, top center):** "the mailer goes to the $80k shopper — the smallest basket in Maplehill"

## One Dataset, Three Correlations

**Tags:** `rule of thumb` (blue), `common mistake` (red)

- **Between groups** — the three neighborhood averages line up perfectly: correlation +1.0
- **All twelve pooled** — the individuals together still tilt upward: correlation about +0.6
- **Within groups** — inside each neighborhood the four points line up downward: correlation −1.0
- **Aggregation inflates** — averaging smooths noise away, so group correlations run stronger
- **Reverse trap** — assuming one person's pattern holds for whole groups fails the same way

*Example:* The same twelve shoppers give +1.0, +0.6, or −1.0 depending on the level you compute at.

**Key point:** Always ask "correlation at which level?" — group, pooled, and within-group answers can all differ.

### Visualization (canvas `c4`, 720×300)

Diverging bar chart: the three correlations computed from the same data.

- **Title (bold 15px, `#1a5276`, top center):** "Income-Spend Correlation Depends on the Level You Compute At"
- **Axes:** vertical axis at x=75 in `#999` spanning r=+1.15 to −1.15; horizontal zero line at y=155 (muted gray, 1.5px) from x=75 to x=690; 95px per r=1; y labels "0", "+1", "−1" in muted gray.
- **Bars (95px wide):**
  - x=130: +1.0, blue `#2a78d6`, two-line label "between the 3" / "neighborhood averages"
  - x=330: +0.6, yellow `#c98500`, label "all 12 shoppers" / "pooled together"
  - x=530: −1.0, magenta `#d55181`, label "within each" / "neighborhood"
  - Bold 14px signed value labels ("+1.0", "+0.6", "−1.0") in bar color at the bar tip; bold 12px two-line labels on the opposite side of the zero line.
- **Annotation (bold 13px violet `#4a3aa7`, centered at (400,285)):** "same twelve people — three different answers"

## Regeneration instructions

- **Template:** tutorial detail page (tutorials style, see `tutorials/CLAUDE.md`). h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `.text-col` (50%) and `.viz-col` (50%) holding one 720×300 canvas.
- **Text cell structure:** `.tags` row of colored pills, `<ul>` of one-line bullets each opening with `<b>bold term</b>` (colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, `<strong>` prefix).
- **Tag pill styles:** 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; bullets 0.92rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Project palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Shared chart data (JS `hoods` array used by c1 and c2):** Oakwood (orange, avg 40k/$120, points [[30,140],[35,130],[45,110],[50,100]]), Maplehill (aqua, avg 70k/$150, points [[60,170],[65,160],[75,140],[80,130]]), Riverview (violet, avg 100k/$180, points [[90,200],[95,190],[105,170],[110,160]]).
- **Canvas:** intrinsic 720×300; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) via a shared `setup(id)` helper (`ctx.scale` back to logical coordinates); shared `axes()` helper draws the L-frame and returns X/Y mapping closures. All data hardcoded — no `Math.random()`.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
