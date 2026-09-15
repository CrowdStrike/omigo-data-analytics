# Ratio Metrics

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Ratio Metrics

**Subtitle:** A blended ratio like overall conversion rate can fall even when every segment improves — because the mix of traffic shifted, not the performance

## The Monday Dashboard That Fell While Every Segment Rose

**Tags:** `core idea` (blue), `Simpson's paradox` (orange), `blended ratio` (green)

- **The metric** — a site's Monday dashboard shows overall conversion rate: purchases divided by visits
- **The drop** — last week it read 3.80%; this week it reads 2.49%, and an alert fires
- **The segments** — desktop conversion rose 5.0% → 5.5%; mobile conversion rose 1.0% → 1.2%
- **The twist** — every segment got better, yet the blend got worse — both facts are true at once
- **The cause** — mobile's share of visits jumped from 30% to 70%, and mobile converts far lower
- **The name** — this is Simpson's paradox, showing up in an ordinary weekly dashboard

*Example (italic):* Marketing launched a mobile ad push; the extra mobile visits are real and welcome, but they dilute the blended rate from 3.80% to 2.49%.

**Key point:** A ratio of totals is a weighted average of segment ratios — change the weights (the mix) and the blend can move opposite to every segment.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: last week vs this week conversion rate for desktop, mobile, and the blend — the two segments rise while the blend falls.

- **Title (bold 15px, `#1a5276`, top center):** "Both Segments Up, Blend Down: 3.80% → 2.49%".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = conversion rate 0 to 6%, gridlines `#e5e9ef` at 2% and 4% with 12px `#444` tick labels; scale 30px per percentage point.
- **Groups (centered at x = 160, 340, 520; two bars each, 50px wide, 10px gap; group labels 13px `#444` below baseline):** "Desktop", "Mobile", "Blended".
- **Last-week bars (blue `#2a78d6`, fill `rgba(42,120,214,0.30)`, 2px solid edge):** heights `[150, 30, 114]` px for rates `[5.0, 1.0, 3.8]`.
- **This-week bars:** desktop and mobile green `#008300` fill `rgba(0,131,0,0.30)`, heights `[165, 36]` px for `[5.5, 1.2]`; blended bar red `#e74c3c` fill `rgba(231,76,60,0.20)`, height 75 px for `2.49`.
- **Value labels (bold 12px, matching bar color, above each bar):** "5.0%", "5.5%", "1.0%", "1.2%", "3.80%", "2.49%".
- **Annotation (bold 13px red `#e74c3c`, above the blended group, y=90):** "the only number on the dashboard is the one that fell".
- **Caption (12px `#444`, bottom right):** "rates illustrative; blends exact for these rates".

## Redoing the Arithmetic by Hand

**Tags:** `worked example` (blue), `mix shift` (orange)

- **Last week** — 10,000 visits: 7,000 desktop × 5.0% = 350 sales; 3,000 mobile × 1.0% = 30 sales
- **Last blend** — 380 sales / 10,000 visits = 3.80%, exactly 0.70×5.0 + 0.30×1.0
- **This week** — 10,000 visits: 3,000 desktop × 5.5% = 165 sales; 7,000 mobile × 1.2% = 84 sales
- **This blend** — 249 sales / 10,000 visits = 2.49%, exactly 0.30×5.5 + 0.70×1.2
- **Hand-check** — no rate fell; only the weights swapped, 70/30 desktop-heavy to 30/70 mobile-heavy

*Example (italic):* 165 + 84 = 249 sales this week vs 350 + 30 = 380 last week — 131 fewer sales on identical traffic volume, purely from where the traffic came from.

**Key point:** The blend is sales ÷ visits, which equals each segment's rate weighted by its visit share — swapping the 70/30 mix to 30/70 moves the blend even with better rates.

### Visualization (canvas `c2`, 720×300)

Two stacked vertical bars of visit mix (desktop vs mobile), one per week, each segment labeled with its visits, rate, and sales — total height constant at 10,000 visits.

- **Title (bold 15px, `#1a5276`, top center):** "Same 10,000 Visits, Opposite Mix: 70/30 Becomes 30/70".
- **Axes:** origin x=60, baseline y=245, plot height 180; y = visits 0 to 10,000, gridlines `#e5e9ef` at 2,500 / 5,000 / 7,500 with 12px `#444` tick labels; scale 18px per 1,000 visits.
- **Bars (140px wide, at x=180 "last week" and x=460 "this week", labels 13px `#444` below baseline):** stacked desktop on bottom, mobile on top.
- **Last week:** desktop blue `rgba(42,120,214,0.30)` 126px tall (7,000), mobile aqua `rgba(25,158,112,0.30)` 54px tall (3,000).
- **This week:** desktop blue 54px tall (3,000), mobile aqua 126px tall (7,000).
- **Segment labels (12px `#2c3e50`, centered in each block):** "desktop 7,000 × 5.0% = 350", "mobile 3,000 × 1.0% = 30", "desktop 3,000 × 5.5% = 165", "mobile 7,000 × 1.2% = 84".
- **Totals (bold 12px `#1a5276`, above each bar):** "380 sales — 3.80%" and "249 sales — 2.49%".
- **Annotation (bold 13px violet `#4a3aa7`, centered between bars at y=70):** "the mix flipped; the rates only rose".
- **Caption (12px `#444`, bottom right):** "visit counts illustrative; sales exact for these counts".

## Splitting the Drop: Rate Change vs Mix Change

**Tags:** `rule of thumb` (green), `decomposition` (blue), `where it's used` (orange)

- **The rule** — before reacting to any ratio move, split it into a rate effect and a mix effect
- **Counterfactual** — apply this week's mix to last week's rates: 0.30×5.0 + 0.70×1.0 = 2.20%
- **Mix effect** — 2.20 − 3.80 = −1.60pp: the mix shift alone explains a drop past this week's actual
- **Rate effect** — 2.49 − 2.20 = +0.29pp: the genuine rate improvements claw part of it back
- **The check** — −1.60 + 0.29 = −1.31pp, exactly the observed 3.80% → 2.49% move
- **The habit** — report per-segment rates next to every blend; blends alone hide this split

*Example (italic):* The decomposition turns "conversion crashed 1.31 points" into "mix cost 1.60 points, product gained 0.29 points" — opposite stories, same number.

**Key point:** Blend change = mix effect + rate effect; compute both with one counterfactual blend before deciding whether anything is actually broken.

### Visualization (canvas `c3`, 720×300)

Waterfall chart decomposing the blended drop: start 3.80%, mix effect −1.60pp down to 2.20%, rate effect +0.29pp up to 2.49%.

- **Title (bold 15px, `#1a5276`, top center):** "The −1.31pp Drop = −1.60pp Mix + 0.29pp Rate".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = blended rate 0 to 4%, gridlines `#e5e9ef` at 1% / 2% / 3% with 12px `#444` tick labels; scale 45px per percentage point.
- **Bars (110px wide at x = 90, 235, 380, 525; labels 12px `#444` below baseline):**
  - "last week 3.80%": blue `#2a78d6` fill `rgba(42,120,214,0.30)` from baseline up 171px (top y=74).
  - "mix effect −1.60pp": floating red `#e74c3c` fill `rgba(231,76,60,0.20)` from y=74 down to y=146 (72px), bold 12px red label "−1.60".
  - "rate effect +0.29pp": floating green `#008300` fill `rgba(0,131,0,0.30)` from y=146 up to y=133 (13px), bold 12px green label "+0.29".
  - "this week 2.49%": blue fill from baseline up 112px (top y=133).
- **Connectors:** dashed `#6b7280` (dash 4/3) horizontal lines linking each bar's edge to the next bar.
- **Annotation (bold 13px green `#008300`, near x=380, y=100):** "rates actually improved the blend".
- **Caption (12px `#444`, bottom right):** "decomposition exact: −1.60 + 0.29 = −1.31pp".

## Paging the Team Over a Mix Shift

**Tags:** `common mistake` (red), `weighted vs unweighted` (orange)

- **The page** — the 3.80% → 2.49% alert wakes an engineer; a "conversion incident" is declared
- **The hunt** — checkout, payments, and page speed are all inspected; every segment looks fine
- **The reveal** — the "incident" is the mobile ad campaign working; nothing on the site broke
- **Two averages** — the unweighted mean of segment rates rose 3.00% → 3.35%; the blend fell
- **Neither lies** — weighted answers "how does traffic convert"; unweighted "how do segments perform"
- **The fix** — alert on per-segment rates and on mix separately, not on the blend alone

*Example (italic):* A postmortem for a metric that measured marketing's channel mix, not the product — hours spent because one blended ratio stood in for two questions.

**Common mistake:** Treating a blended-ratio move as a performance change without decomposing it — a pure mix shift can trigger the same alert as a genuine site outage.

### Visualization (canvas `c4`, 720×300)

Paired-bar chart contrasting the two averages of the same two segment rates: unweighted mean (rose) vs visit-weighted blend (fell), week over week.

- **Title (bold 15px, `#1a5276`, top center):** "Same Segments, Opposite Verdicts: Unweighted Up, Weighted Down".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = rate 0 to 4%, gridlines `#e5e9ef` at 1% / 2% / 3% with 12px `#444` tick labels; scale 45px per percentage point.
- **Left group (centered at x=210, label 13px `#444` "unweighted mean of rates"):** last week blue `#2a78d6` bar 135px tall (3.00%), this week green `#008300` bar 151px tall (3.35%); bars 60px wide, 12px gap; bold 12px value labels "3.00%" and "3.35%" above.
- **Right group (centered at x=470, label "visit-weighted blend"):** last week blue bar 171px tall (3.80%), this week red `#e74c3c` bar 112px tall (2.49%); value labels "3.80%" and "2.49%".
- **Arrows:** bold green up-arrow "▲ +0.35pp" (12px) above the left group; bold red down-arrow "▼ −1.31pp" above the right group.
- **Annotation (bold 13px magenta `#d55181`, centered near y=70):** "pick the average that answers your question — and say which one it is".
- **Caption (12px `#444`, bottom right):** "both averages exact for the illustrative rates and mix".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); visit counts (7,000/3,000 flipping to 3,000/7,000) and segment rates (5.0→5.5, 1.0→1.2) are invented and labeled illustrative; every derived number is exact arithmetic on them — blends 3.80% and 2.49%, counterfactual 2.20%, mix effect −1.60pp, rate effect +0.29pp, total −1.31pp, unweighted means 3.00% and 3.35%, sales 350/30/165/84.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
