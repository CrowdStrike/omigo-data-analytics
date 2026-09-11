# Observational vs Experimental Data

**Page type:** detail page (tutorial page: `.card-section` blocks, each h2 + two-column layout table, text left 45% / canvas right 55%; one section uses a 3-column 38/31/31 layout with two canvases)
**HTML title tag:** Observational vs Experimental Data

**Subtitle:** Watching what users chose to do is not the same as assigning it — the choice itself carries hidden information

## The Notification Puzzle: 62% vs 38%

**Tags:** `core idea` (blue), `running example` (orange)

- **The observation** — users who enabled notifications retain far better than those who didn't
- **Observational data** — you only watched; each user picked their own group
- **Experimental data** — you assign the groups; users do not choose
- **The catch** — keen users both enable notifications and stick around anyway
- **The question** — did notifications cause retention, or did keen users cause both?

*Example:* Of 200 app users, the 100 who enabled notifications retained at 62%; the 100 who didn't, at 38%.

**Key point:** The gap you observed is a fact; the reason for the gap is a guess until someone assigns the groups.

### Visualization (canvas `c1`, 720×300)

Bar chart: the observed retention gap between the two self-selected groups.

- **Title (bold 16px, ink `#1a5276`, top center):** "30-Day Retention, Split by Who Enabled Notifications".
- **Bars:** width 120, baseline at y=245, chart height 165, y scale max 70%. Baseline is a thin gray `#999` line from x=80 to x=560.
  - "enabled notifications" at x=140: 62%, fill blue `#2a78d6`; value label "62%" bold 14px above bar; label "enabled notifications" (12px) and muted sub-label "62 of 100 users" (`#6b7280`) below baseline.
  - "did not enable" at x=400: 38%, fill yellow `#c98500`; value label "38%"; label "did not enable", sub-label "38 of 100 users".
- **Gap bracket:** magenta `#d55181`, width 2, at right (x 590–605) spanning from the 62% bar top height to the 38% bar top height, labeled "+24" / "points" in bold 13px magenta to its right.
- **Annotation (bottom center, bold 13px magenta):** "a big gap — but every user chose their own group".

## Redo the Math: Split the 200 Users Into Keen and Casual

**Tags:** `worked example` (green), `core idea` (blue)

- **200 users** — 100 keen (love the app) and 100 casual (barely use it)
- **Keen** — 80 of 100 enable notifications; both keen halves retain at 70%
- **Casual** — only 20 of 100 enable; both casual halves retain at 30%
- **Add it up** — enabled: 56 + 6 = 62 of 100 retain; not enabled: 14 + 24 = 38
- **Zero effect** — inside each group notifications change nothing, yet totals show +24

*Example:* The enabled group is mostly keen users (80 of 100); the not-enabled group is mostly casual (80 of 100).

**Key point:** The 24-point gap is entirely *who* enabled notifications, not what notifications did.

### Visualization (canvas `c2a`, 420×300)

Bar chart: the aggregate view of the same 200 users.

- **Title (bold 15px, ink `#1a5276`, top center):** "What the Totals Say".
- **Bars:** width 110, baseline at y=240, chart height 155, y scale max 70%; gray `#999` baseline from x=45 to x=390.
  - "enabled" at x=70: 62%, blue `#2a78d6`, value label "62%" bold 14px above.
  - "not enabled" at x=240: 38%, yellow `#c98500`, value label "38%".
- **Annotations (bottom center):** bold 13px magenta `#d55181`: "looks like notifications add +24"; below it, muted 12px `#6b7280`: "62 of 100  vs  38 of 100 retained".

### Visualization (canvas `c2b`, 420×300)

Grouped bar chart: the same users split by user type — the gap vanishes.

- **Title (bold 15px, ink `#1a5276`, top center):** "Same Users, Split by Type".
- **Clusters:** two clusters of two bars each (bar width 58, in-cluster gap 10), baseline at y=240, chart height 155, y scale max 80%; gray `#999` baseline from x=40 to x=395. Bar colors: enabled = blue `#2a78d6`, not enabled = yellow `#c98500`.
  - "keen users" cluster at x=60: values 70% and 70%; white 12px sub-labels inside bars near the baseline: "56/80" and "14/20".
  - "casual users" cluster at x=235: values 30% and 30%; white sub-labels "6/20" and "24/80".
  - Value labels ("70%", "30%", etc.) bold 13px above each bar; cluster labels below baseline.
- **Legend (top left):** 11px blue swatch + "enabled"; yellow swatch + "not enabled" (12px text).
- **Annotation (bottom center, bold 13px green `#008300`):** "inside each group: no gap at all".

## Where a Data Scientist Meets This Every Week

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Feature adoption** — "users of feature X churn less" has the same keen-user problem
- **Confounder** — the hidden trait (keenness) that drives both the choice and the outcome
- **Wasted roadmaps** — teams build for what keen users touch, expecting casual users to change
- **The fix** — run an experiment: assign notifications by coin flip and compare
- **Rule of thumb** — if users placed themselves in the groups, you compared users, not the feature

*Example:* "Enabled users retain 24 points better" became a quarter of notification work that moved nothing.

**Key point:** Before investing in a feature because its users look better, ask who chose to use it.

### Visualization (canvas `c3`, 720×300)

Diagram: confounder triangle (three labeled boxes with causal arrows).

- **Title (bold 16px, ink `#1a5276`, top center):** "The Hidden Third Player: a Confounder".
- **Boxes** (white fill, 2px colored border, bold 13px centered text in the border color):
  - Violet `#4a3aa7` box at (280, 55), 160×44: two lines "KEEN USER" / "(hidden trait)".
  - Blue `#2a78d6` box at (90, 190), 190×44: "enables" / "notifications".
  - Green `#008300` box at (440, 190), 190×44: "retains" / "at 30 days".
- **Arrows** (2px, filled triangular heads): solid violet from the top box down-left to the blue box and down-right to the green box; dashed (6/4) gray `#6b7280` arrow from the blue box to the green box.
- **Labels:** bold 14px gray "?" at (360, 205) on the dashed arrow; bold 12px violet "causes both" beside each solid arrow, at (205, 132) and (515, 132).
- **Annotation (bottom center, bold 13px magenta `#d55181`):** "keenness creates the 62% vs 38% gap even if the \"?\" arrow is zero".

## The Common Confusion: Two Questions, Two Answers

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Correlation** — "enabled users retain better" is true in the data, and stays true
- **Causation** — "turning notifications on raises retention" — only assigning can test this
- **Both are real** — the observed gap isn't wrong; it answers a different question
- **The tell** — watching answers "who retains?"; assigning answers "what if we switch it on?"
- **Same data, new label** — calling watched data an "experiment" doesn't make it one

*Example:* A coin-flip test on the same app showed roughly a 1-point lift — not the 24 points the watching suggested.

**Key point:** Observational data describes the world as it is; experimental data tests the world you'd create.

### Visualization (canvas `c4`, 720×300)

Two-panel bar chart: the same feature measured by watching vs by assigning, separated by a dashed vertical divider (`#bdc3c7`, dash 4/3) at x=360.

- **Title (bold 16px, ink `#1a5276`, top center):** "Same Feature, Two Ways of Measuring It".
- **Both panels:** two bars each (width 85), baseline at y=225, chart height 140, y scale max 70%; gray `#999` baseline; bar labels below baseline (12px): "with notifications" (blue `#2a78d6`) and "without" (yellow `#c98500`); value labels bold 13px above bars; panel title bold 13px ink centered at y=56; note bold 13px centered 38px below baseline.
  - Left panel (x0=40), title "WATCHING (users chose)": values 62% and 38%; note "gap: +24 points" in magenta `#d55181`.
  - Right panel (x0=400), title "ASSIGNING (coin flip)": values 46% and 45%; note "gap: +1 point" in green `#008300`.
- **Caption (bottom center, 12px muted `#6b7280`):** "coin-flip numbers illustrative — the point is how far the two answers can sit apart".

## Regeneration instructions

- **Layout:** tutorials topic-page template. h1 (no index number) with `border-bottom: 2px solid #2980b9`, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `td.text-col` (45%) and `td.viz-col` (55%). Section 2 uses `table.layout3` (text-col 38%, two viz-col 31% each) holding canvases `c2a` and `c2b` side by side.
- **Text column structure:** `.tags` row of `.tag` pills first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Tag pill styles:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. blue: `rgba(26,82,118,0.12)` bg / `#1a5276` text; green: `rgba(39,174,96,0.15)` / `#27ae60`; red: `rgba(231,76,60,0.12)` / `#e74c3c`; orange: `rgba(230,126,34,0.15)` / `#e67e22`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases have `width: 100%`, `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Hardcoded literal data arrays — no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS `P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
