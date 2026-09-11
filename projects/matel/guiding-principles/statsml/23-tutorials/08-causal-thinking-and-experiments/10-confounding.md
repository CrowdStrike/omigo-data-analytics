# Confounding

**Page type:** detail page (tutorial page: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%, tag pills above bullets)
**HTML title tag:** Confounding

**Subtitle:** A hidden third thing drives both the "cause" and the "effect" — so the two move together without one causing the other.

## Premium users churn less — or do older accounts churn less?

Tags: `core idea` (blue), `hidden driver` (orange)

- **The finding** — premium users churn at 5.4%, free users at 17.2%; premium looks protective
- **The catch** — premium users are mostly long-time accounts; free users are mostly new
- **Arrow one** — account age → plan: people who stayed years eventually upgrade
- **Arrow two** — account age → churn: people who stayed years rarely leave anyway
- **The confounder** — account age has an arrow into both sides of the comparison

*Example:* Loyal five-year customers upgrade AND stay — the plan gets credit for the loyalty that came first.

**Key point:** A confounder sits upstream of both the treatment and the outcome. It manufactures a correlation between them that no arrow from plan to churn needs to explain.

### Visualization (canvas `c1`, 720×300)

DAG diagram: the confounding triangle with three node boxes and arrows.

- **Title (bold 16px, ink `#1a5276`, top center):** "One Arrow Into Each Side: the Confounding Triangle".
- **Nodes (white-filled rectangles, 2px colored stroke, bold 13px colored centered label):** "Account age" at (360, 85), 170×40, violet `#4a3aa7`; "Premium plan" at (175, 220), 165×40, blue `#2a78d6`; "Low churn" at (545, 220), 175×40, green `#008300`.
- **Arrows (2.5px lines with filled triangular heads):** two solid violet arrows from "Account age" down-left to "Premium plan" and down-right to "Low churn"; one dashed (7/5) mute `#6b7280` arrow from "Premium plan" to "Low churn".
- **Edge labels:** bold violet 12px: "loyal users upgrade" (right-aligned, left of the top node) and "loyal users stay anyway" (left-aligned, right of the top node), both at y=155. Bold mute 13px centered above the dashed arrow: "does the plan do anything?".
- **Caption (bold magenta `#d55181` 13px, bottom center):** "the two solid arrows alone make plan and churn move together".

## Splitting 4,500 users by account age

Tags: `worked example` (green), `by hand` (blue)

- **New accounts** — free: 2,000 users, 400 churn (20%); premium: 200 users, 36 churn (18%)
- **Old accounts** — free: 500 users, 30 churn (6%); premium: 1,800 users, 72 churn (4%)
- **Overall free** — (400 + 30) / 2,500 = 17.2% churn
- **Overall premium** — (36 + 72) / 2,000 = 5.4% churn
- **The reveal** — the 11.8-point overall gap shrinks to 2 points inside each age group

*Example:* Compare like with like and the "premium effect" drops from 11.8 points to 2 — age was doing the work.

**Key point:** You can redo every number here with one division. Splitting by the confounder (stratifying) is the simplest honest fix.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: churn rate by plan within age groups and overall.

- **Title (bold 16px, ink, top center):** "Churn Rate by Plan — Before and After Splitting by Age".
- **Axes:** horizontal `#999` baseline; padding top 55, bottom 60, left 65, right 160; y scale max 24%.
- **Groups (three, each with a free bar in orange `#d95926` and a premium bar in blue `#2a78d6`, 0.7 alpha, bar width 52, bold 12px value labels above):**
  - "new accounts": free 20%, premium 18%; green `#008300` bold 12px gap label below: "gap: 2 pts".
  - "old accounts": free 6%, premium 4%; green gap label: "gap: 2 pts".
  - "ALL (mixed)": free 17.2%, premium 5.4%; group label and gap label in bold magenta `#d55181`: "gap: 11.8 pts".
- **Legend (right side, 13px swatches at 0.7 alpha):** orange "free plan", blue "premium plan".
- **Annotation (bold magenta 13px, right side, three lines):** "11.8-pt gap appears" / "only when ages" / "are mixed together".

## Why the raw comparison misleads: the groups were never alike

Tags: `where it's used` (blue), `costly mistake` (red)

- **The mix** — premium is 90% old accounts (1,800 of 2,000); free is 80% new (2,000 of 2,500)
- **Apples to oranges** — comparing plans mostly compares veterans to newcomers
- **The bad decision** — "push everyone to premium to cut churn 12 points" would buy ~2 points
- **Everywhere** — users of feature X retain better; buyers with the app spend more — same trap
- **The clean fix** — randomize who gets the treatment, and the arrows into it are cut

*Example:* A free upgrade experiment on new users would have shown ~2 points, before the pricing team spent the year's budget.

**Key point:** Before comparing two groups, ask how people ended up in them. If something drove the sorting and also drives the outcome, the raw gap is not the effect.

### Visualization (canvas `c3`, 720×300)

Two horizontal stacked composition bars: age mix inside each plan.

- **Title (bold 16px, ink, top center):** "Who Is in Each Plan? Mostly Different Kinds of People".
- **Bars (x=200, width 400 total scaled to 2,500 users, height 46):**
  - "free (2,500 users)" at y=80: 2,000 new + 500 old.
  - "premium (2,000 users)" at y=165: 200 new + 1,800 old.
  - New-segment fill `rgba(201,133,0,0.45)` with yellow `#c98500` stroke; old-segment fill `rgba(74,58,167,0.40)` with violet `#4a3aa7` stroke. Row labels right-aligned bold 13px. In-segment bold 12px counts: "2,000 new", "500 old", "1,800 old"; the narrow premium new-segment gets its "200 new" label placed above the bar (dark yellow `#7a5200` for new counts, violet for old counts).
- **Legend (12px, y≈232):** swatch `rgba(201,133,0,0.45)`/yellow stroke "new account (< 1 yr)"; swatch `rgba(74,58,167,0.40)`/violet stroke "old account (3+ yrs)".
- **Caption (bold magenta 13px, bottom center):** "comparing plans mostly compares 80% newcomers to 90% veterans".

## The confusion: not every third variable is a confounder

Tags: `common mistake` (red), `arrows matter` (orange)

- **Confounder** — causes both sides (age → plan, age → churn): adjust for it
- **Mediator** — sits on the causal path (premium → more features used → less churn)
- **Adjusting a mediator** — removes the part of the effect that flows through it
- **Direction test** — could this variable have influenced which group a user landed in?
- **No shortcut** — the data alone cannot tell you the arrows; domain knowledge draws them

*Example:* "Controlling for feature usage" made premium look useless — it had controlled away how premium works.

**Key point:** Adjust for things that come BEFORE the treatment, not things the treatment causes. Same statistical move, opposite consequences.

### Visualization (canvas `c4`, 720×300)

Side-by-side mini-DAGs: confounder triangle vs mediator chain, separated by a dashed light-gray (`#bdc3c7`, dash 4/3) vertical divider at x=360.

- **Title (bold 16px, ink, top center):** "Confounder vs Mediator: Same Third Variable, Opposite Rule".
- **Left panel — confounder triangle:** node boxes (white fill, 2px stroke, bold 13px label): "account age" at (180, 90) 130×36 violet `#4a3aa7`; "plan" at (90, 195) 100×36 blue `#2a78d6`; "churn" at (275, 195) 88×36 green `#008300`. Solid violet arrows from "account age" to both lower nodes; dashed mute arrow from "plan" to "churn". Bold green 13px caption centered below: "CONFOUNDER — upstream of both:" / "split or adjust for it".
- **Right panel — mediator chain:** nodes: "plan" at (445, 145) 100×36 blue; "features used" at (565, 90) 118×36 aqua `#199e70`; "churn" at (660, 145) 88×36 green. Solid blue arrows plan → features used → churn. Bold aqua 12px caption: "the effect flows THROUGH here". Bold red `#e74c3c` 13px caption below: "MEDIATOR — on the path:" / "adjusting it erases the effect".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` in `#666` 0.95rem, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%) holding the canvas.
- **Left column structure:** `.tags` row of colored pill spans (0.72rem, 600 weight, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem) beginning with `<strong>Key point:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal reset; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`; shared `setup(id)` helper with fixed 720×300 logical size that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; shared helpers `arrow(ctx, x1, y1, x2, y2, color, dashed)` (2.5px line, optional 7/5 dash, filled triangular head) and `nodeBox(ctx, x, y, bw, bh, label, color)` (white fill, 2px colored stroke, bold 13px centered label). All data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
