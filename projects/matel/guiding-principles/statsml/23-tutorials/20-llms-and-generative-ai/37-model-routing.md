# Model Routing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Model Routing

**Subtitle:** A small gatekeeper reads each request and sends the easy ones to a cheap model and the hard ones to a big one

## One Front Door, Two Models Behind It

**Tags:** `core idea` (blue), `gatekeeper` (green)

- **The fleet** — a cheap fast model and a big expensive model sit behind one front door
- **The router** — a small classifier reads each request and picks which model answers it
- **Easy lane** — "what's your refund policy?" needs no genius; the cheap model handles it
- **Hard lane** — "why does my proof fail at step 3?" goes to the big model
- **The win** — most traffic is easy, so most requests cost little and quality holds

*Example (italic):* A help desk sends password resets to the junior agent and legal disputes to the senior one — routing does that per request, in milliseconds.

**Key point:** Routing spends the big model only where it earns its cost — the router's job is knowing the difference.

### Visualization (canvas `c1`, 720×300)

Flow diagram: a stream of requests hits a router box that splits traffic into a thick cheap-model lane and a thin big-model lane.

- **Title (bold 15px, `#1a5276`, top center):** "The Router Splits the Traffic".
- **Requests:** three small rounded rects 120×26 stacked at x=30, tops y = `[76, 118, 160]`, fill `rgba(107,114,128,0.08)`, 1.5px `#6b7280` border; 11px `#2c3e50` centered labels: "refund policy?", "proof fails at step 3", "reset my password".
- **Router box:** rounded rect x=220, y=104, 130×56, fill `rgba(201,133,0,0.10)`, 2px `#c98500` border; bold 13px `#c98500` centered label "router" at (285, 128); 11px `#6b7280` centered "a small model itself" at (285, 146).
- **Cheap-model box:** rounded rect x=470, y=64, 210×64, fill `rgba(0,131,0,0.08)`, 2px `#008300` border; bold 13px `#008300` centered "small model" at (575, 88); 12px `#2c3e50` centered "fast & cheap" at (575, 108).
- **Big-model box:** rounded rect x=470, y=176, 210×64, fill `rgba(74,58,167,0.08)`, 2px `#4a3aa7` border; bold 13px `#4a3aa7` centered "big model" at (575, 200); 12px `#2c3e50` centered "slow & pricey" at (575, 220).
- **Arrows:** 1.5px `#6b7280` from each request box to the router; thick 5px `#008300` arrow from (350,120) to (464,96) labeled bold 12px `#008300` "~80% of traffic" above; thin 2px `#4a3aa7` arrow from (350,148) to (464,208) labeled bold 12px `#4a3aa7` "~20% of traffic" below.
- **Annotation (bold 12px orange `#d95926`, centered at y=270):** "the thick lane is cheap on purpose — most questions never needed the big model".
- **Caption (11px `#444`, bottom right, y=292):** "traffic split illustrative".

## Route 100 Questions, Count the Bill

**Tags:** `worked example` (blue), `cost control` (orange)

- **The prices** — big model: 10 units per answer; small model: 1 unit (illustrative)
- **All-big** — 100 questions × 10 = 1,000 units; quality is great, the bill is not
- **All-small** — 100 × 1 = 100 units; the 20 hard questions get weak answers
- **Routed** — 80 easy × 1 + 20 hard × 10 = 80 + 200 = 280 units
- **The claim to check** — 72% cheaper than all-big, with the hard slice still answered well

*Example (italic):* The whole business case is one line of arithmetic: 80 × 1 + 20 × 10 = 280 against 1,000 — redo it with your own traffic mix.

**Key point:** Routing beats all-big on cost and all-small on quality — but only as long as the 80/20 split is called correctly.

### Visualization (canvas `c2`, 720×300)

Bar chart: the daily bill under all-small, routed, and all-big strategies, with a quality note under each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Same 100 Questions, Three Strategies (illustrative)".
- **Axes:** baseline y=235, plot top y=70; y = cost in units 0–1000 with `#e5e9ef` gridlines at 250/500/750/1000 and 12px `#444` right-aligned tick labels at x=64; axis lines 1px `#999` from (70,70) to (70,235) to (660,235).
- **Bars (90px wide, centered x = `[200, 390, 580]`):** heights scaled 0.165px per unit:
  - "all small" 100 units, fill `#c98500`
  - "routed 80/20" 280 units, fill `#008300`
  - "all big" 1000 units, fill `#4a3aa7`
  - bold 13px value labels above each bar in the bar's color: "100", "280", "1,000".
- **X labels:** 12px `#444` main label centered at y=254 ("all small", "routed 80/20", "all big") and 11px labels at y=270: "hard questions suffer" in `#e74c3c`, "quality held" in `#008300`, "overpays on easy ones" in `#6b7280`.
- **Annotation (bold 12px orange `#d95926`, centered at (390, 92)):** "280 vs 1,000 — the big model now works only the 20 questions that need it".
- **Caption (11px `#444`, bottom right, y=292):** "unit prices illustrative".

## Routers Are Everywhere Once You Look

**Tags:** `where it's used` (blue), `cheap vs big` (green)

- **Inside products** — assistants with an "auto" model picker choose a model per message
- **Inside one model** — mixture of experts routes each token between sub-networks; same word, smaller scale
- **Cascades** — a variant: try the cheap model first, escalate to the big one when checks fail
- **Across providers** — API gateways route between vendors for cost, speed, or uptime
- **The signals** — request length, topic, user tier, and past failures feed the routing call

*Example (italic):* When a chat product's "auto" mode answers a greeting instantly but thinks hard about a math proof, you just watched a router decide.

**Key point:** The same routing idea repeats at three scales — inside a model, inside a product, and across providers.

### Visualization (canvas `c3`, 720×300)

Three stacked rows, one per scale of routing, each with a small diagram and a one-line description.

- **Title (bold 15px, `#1a5276`, top center):** "One Idea, Three Scales".
- **Rows:** three rounded rects 660×62 at x=30, tops y = `[48, 122, 196]`; borders 2px `#199e70` / `#008300` / `#2a78d6`, fills `rgba(25,158,112,0.05)` / `rgba(0,131,0,0.05)` / `rgba(42,120,214,0.05)`.
  - **Row 1 (aqua):** bold 12px `#199e70` label "inside one model" left-aligned at (46, 73); 12px `#2c3e50` "mixture of experts: each token routed to a couple of sub-networks" at (46, 93); right side: 3 small boxes 34×18 at x=560/600/640, y=62, 1px `#199e70` border, middle one filled `rgba(25,158,112,0.25)`; 10px `#199e70` "experts" centered at (617, 98).
  - **Row 2 (green):** bold 12px `#008300` label "inside a product" at (46, 147); 12px `#2c3e50` "an auto model picker sends each message to a cheap or big model" at (46, 167); right side: fork sketch — dot at (570,152), two 1.5px lines to boxes 44×18 at (600,132) and (600,158) with 1px `#008300` borders, 10px labels "small" / "big" centered inside.
  - **Row 3 (blue):** bold 12px `#2a78d6` label "across providers" at (46, 221); 12px `#2c3e50` "a gateway routes between vendors for cost, speed, or uptime" at (46, 241); right side: boxes "Vendor A" and "Vendor B" 60×18 at (585,206) and (585,232) with 1px `#2a78d6` borders and 10px centered labels, fork lines from a dot at (560,226).
- **Annotation (bold 12px orange `#d95926`, centered at y=278):** "spot the router: wherever traffic forks by difficulty, cost, or health".
- **Caption (11px `#444`, bottom right, y=294):** "diagrams simplified".

## The Router Is a Model Too

**Tags:** `common mistake` (red), `silent failures` (orange)

- **It can be wrong** — the router is a small classifier with its own error rate
- **The silent error** — a hard question sent to the small model returns a weak answer, no alarm
- **The loud-ish error** — an easy question sent to the big model just wastes money
- **The honest eval** — compare routed quality against all-big quality, not just the cost savings
- **Drift** — what counts as "easy" shifts as users learn what to ask; routers need re-tuning

*Example (italic):* A team celebrated 70% cost savings for a quarter before noticing the router had been sending a tenth of the hard questions to the small model.

**Common mistake:** Reporting the cost savings without reporting the quality change on the hard slice — the router's failures are exactly there, and they are silent.

### Visualization (canvas `c4`, 720×300)

2×2 outcome matrix: question difficulty × model chosen, showing the two good cells and the two failure cells.

- **Title (bold 15px, `#1a5276`, top center):** "The Four Routing Outcomes".
- **Axis labels:** bold 12px `#444` column headers "sent to small model" at (280, 64) and "sent to big model" at (530, 64); bold 12px `#444` row labels, right-aligned at x=150: "easy question" at y=120, "hard question" at y=210.
- **Cells:** four rounded rects 240×76 at (170, 82), (420, 82), (170, 172), (420, 172):
  - **easy→small (green `#008300`, fill `rgba(0,131,0,0.08)`):** bold 12px header "cheap and good" at cell center −14; 11px `#2c3e50` "the point of routing" at center +8.
  - **easy→big (yellow `#c98500`, fill `rgba(201,133,0,0.08)`):** bold 12px header "good but wasteful"; 11px "money burned, nobody hurt".
  - **hard→small (red `#e74c3c`, fill `rgba(231,76,60,0.10)`, border 2.5px):** bold 12px header "weak answer, no alarm"; 11px "the silent failure — watch this cell".
  - **hard→big (blue `#2a78d6`, fill `rgba(42,120,214,0.08)`):** bold 12px header "costly and good"; 11px "the big model earning its price".
- **Annotation (bold 12px orange `#d95926`, centered at y=276):** "cost dashboards light up the wasteful cell — the red cell needs a quality eval to see".
- **Caption (11px `#444`, bottom right, y=294):** "outcome matrix, simplified".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** the c2 bars must read exactly 100 / 280 / 1,000 to match the text's 80 × 1 + 20 × 10 arithmetic; the c1 lane labels must read ~80% / ~20%; all values hardcoded, no randomness; "Vendor A/B" naming for hypothetical providers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
