# Beta Channels

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Beta Channels

**Subtitle:** Before an app update reaches every phone, it flows through widening rings of testers — TestFlight on iOS, testing tracks on Google Play — so bugs hit dozens of people instead of millions

## One Build, Four Rings of Users

**Tags:** `core idea` (blue), `TestFlight & Play tracks` (green), `staged release` (orange)

- **The app** — a coffee shop's loyalty app ships update 2.4.0, which redesigns the checkout screen
- **Ring 1** — the build first goes to 25 internal staff phones via an internal testing track
- **Ring 2** — next, 500 invited regulars get it through a closed beta (invite-only list)
- **Ring 3** — then 8,000 volunteers who opted in get it through an open beta anyone can join
- **Ring 4** — only after that does 2.4.0 go to the production store listing and 400,000 customers
- **Same build** — every ring runs the identical binary; only the audience widens

*Example (italic):* Version 2.4.0 spends days 1–3 with 25 staff, days 4–10 with 500 regulars, days 11–24 with 8,000 volunteers, and reaches all 400,000 customers on day 25.

**Key point:** A beta channel is a separate distribution lane for the same app — each ring is a bigger, less forgiving audience, and a build must survive one ring before entering the next.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: four rounded boxes (one per ring) connected by arrows, box size and label growing with audience size.

- **Title (bold 15px, `#1a5276`, top center):** "Update 2.4.0: Internal → Closed Beta → Open Beta → Production".
- **Layout:** four rounded boxes centered vertically at y=150, left edges at x=[40, 210, 390, 570], widths [130, 140, 140, 130], heights [60, 80, 100, 120] (growing), 8px radius, connected by 3px `#6b7280` arrows.
- **Box fills and borders:** ring 1 blue `rgba(42,120,214,0.15)` border `#2a78d6`; ring 2 aqua `rgba(25,158,112,0.15)` border `#199e70`; ring 3 violet `rgba(74,58,167,0.12)` border `#4a3aa7`; ring 4 green `rgba(0,131,0,0.12)` border `#008300`.
- **Box text (12px `#2c3e50`, bold first line):** "internal / 25 staff / days 1–3", "closed beta / 500 invited / days 4–10", "open beta / 8,000 opt-in / days 11–24", "production / 400,000 users / day 25+".
- **Arrow labels (11px `#6b7280`, above each arrow):** "passes", "passes", "passes".
- **Annotation (bold 13px ink `#1a5276`, top left area near y=60):** "same binary, four audiences".
- **Caption (12px `#444`, bottom right):** "audience sizes and days illustrative".

## Counting Crashes Ring by Ring

**Tags:** `worked example` (blue), `crash rate` (green), `shrinking risk` (orange)

- **Ring 1** — 25 staff, 2 phones crash on checkout: 2 / 25 = 8% crash rate; the bug is fixed
- **Ring 2** — 500 regulars on the fixed build, 6 crashes: 6 / 500 = 1.2%; a rarer bug is fixed
- **Ring 3** — 8,000 volunteers, 24 crashes: 24 / 8,000 = 0.3%; one device-specific bug is fixed
- **Ring 4** — 400,000 customers, 200 crashes: 200 / 400,000 = 0.05% — the long tail that remains
- **Hand-check** — each rate is just crashes ÷ users in that ring: 8% → 1.2% → 0.3% → 0.05%
- **The pattern** — every ring catches the bugs common enough to show up at its size

*Example (italic):* The 8% checkout crash needed only 25 phones to appear twice; shipped straight to production it would have hit about 32,000 of the 400,000 customers.

**Key point:** Each ring drives the crash rate down before the audience grows — 8% falls to 1.2%, then 0.3%, then 0.05% — so the biggest audience meets the smallest risk.

### Visualization (canvas `c2`, 720×300)

Bar chart: crash rate measured at each ring, four bars stepping down, with crash counts labeled on each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Crash Rate Shrinks as the Audience Grows".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y is schematic (log-feel via hardcoded pixel heights, not a real log axis); light gridline `#e5e9ef` at y=155.
- **Bars (4, width 90, left edges at x=[100, 245, 390, 535]), heights hardcoded `[180, 118, 74, 30]`:** colors blue `#2a78d6`, aqua `#199e70`, violet `#4a3aa7`, green `#008300`, fills at 0.75 alpha with solid 2px top edge.
- **Value labels (bold 13px, bar color, above each bar):** "8%", "1.2%", "0.3%", "0.05%".
- **Count labels (11px `#6b7280`, inside/below value):** "2 of 25", "6 of 500", "24 of 8,000", "200 of 400,000".
- **X tick labels (12px `#444`, under baseline):** "internal", "closed beta", "open beta", "production".
- **Annotation (bold 13px green `#008300`, right side near y=90):** "the 400,000 see 0.05%, not 8%".
- **Caption (12px `#444`, bottom right):** "bar heights schematic (log-feel); rates illustrative".

## Why Bugs Should Die Before the Store

**Tags:** `where it's used` (blue), `feedback loop` (green), `store ratings` (orange)

- **Exposure math** — the 8% bug hit 2 staff phones in beta; straight to production it hits ~32,000 users
- **Private feedback** — beta testers file reports to the team; production users file 1-star public reviews
- **Fast loop** — TestFlight and Play testing tracks let a fixed build reach testers without a full store rollout
- **Real conditions** — betas surface what the office lab can't: odd devices, slow networks, real accounts
- **Halt switch** — a bad ring can be stopped at 500 users; a bad store release is already on every phone

*Example (italic):* The checkout crash cost the team 2 internal bug reports instead of ~32,000 crashing customers and a wave of public 1-star reviews.

**Key point:** Beta channels convert public, reputation-burning failures into private, cheap ones — the earlier the ring, the smaller and friendlier the audience that absorbs each bug.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: users exposed to the 8% checkout bug under two release strategies, plus where the feedback lands.

- **Title (bold 15px, `#1a5276`, top center):** "Who Meets the 8% Bug: Ringed Release vs Straight to Store".
- **Layout:** two labeled rows, bars extend right from x=250, max width 420; widths hardcoded (log-feel), 26px tall bars.
- **Row 1 (y=110), label 12px `#444` at x=20:** "ringed release — 2 staff phones"; green `#008300` bar width 14 with bold 12px green label "2 users, private bug reports" at bar end.
- **Row 2 (y=185), label:** "straight to store — ~32,000 users"; red `#e74c3c` bar width 420 with bold 12px red label "~32,000 users, public reviews" at bar end.
- **Divider:** dashed `#6b7280` (dash 4/3) vertical line at x=250 from y=80 to y=225.
- **Annotation (bold 13px magenta `#d55181`, centered near y=255):** "same bug, 16,000× fewer victims".
- **Caption (12px `#444`, bottom right):** "bar widths schematic; 32,000 = 8% of 400,000, illustrative".

## Beta Users Are Not Your Users

**Tags:** `common mistake` (red), `selection bias` (orange), `representativeness` (blue)

- **Who opts in** — beta joiners are enthusiasts: newer phones, faster networks, more patience
- **The skew** — in the coffee app's open beta, 85% run the latest OS; in production only 45% do
- **Heavy use** — 70% of beta testers open the app daily versus 20% of production users
- **Old devices** — low-memory phones are 5% of the beta but 30% of production — and crash most
- **The mistake** — reading a clean beta as proof the update is safe for phones the beta never saw
- **The fix** — treat beta pass as necessary, not sufficient; still roll production out gradually

*Example (italic):* The 0.3% open-beta crash rate became 0.05% overall in production — but on the low-memory phones the beta barely covered, the team still had to watch day-one crashes closely.

**Common mistake:** Assuming a quiet beta means a safe release. Beta users self-select for new devices and enthusiasm, so the riskiest phones — old, slow, full of other apps — are exactly the ones a beta undersamples.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: three traits compared between the open-beta audience and the production audience, showing the skew.

- **Title (bold 15px, `#1a5276`, top center):** "The Beta Audience vs the Real Audience".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = share of users 0–100%, gridlines `#e5e9ef` at 25/50/75 with 11px `#6b7280` labels.
- **Groups (3, centered at x=[170, 370, 570]), two bars each (width 55, 10px gap):** beta bar blue `#2a78d6`, production bar yellow `#c98500`, fills 0.75 alpha.
- **Data (percent, hardcoded):** latest OS `[85, 45]`; open app daily `[70, 20]`; low-memory phones `[5, 30]`.
- **Value labels (bold 12px, bar color, above each bar):** "85%", "45%", "70%", "20%", "5%", "30%".
- **X group labels (12px `#444`):** "latest OS", "open app daily", "low-memory phones".
- **Legend (12px, top right):** blue swatch "open beta", yellow swatch "production".
- **Annotation (bold 13px red `#e74c3c`, above the third group near y=95):** "the phones that crash most are the ones the beta barely sees".
- **Caption (12px `#444`, bottom right):** "shares illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); ring sizes (25 / 500 / 8,000 / 400,000), crash counts (2 / 6 / 24 / 200) and rates (8% / 1.2% / 0.3% / 0.05%), the 32,000 exposure figure (8% of 400,000), and the audience-trait shares (85/45, 70/20, 5/30) are invented and labeled illustrative; text numbers must match chart numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
