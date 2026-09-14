# Cloud Computing — IaaS, PaaS, SaaS

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cloud Computing — IaaS, PaaS, SaaS

**Subtitle:** Cloud service models form a rental ladder — each rung up rents one more layer of the stack, trading control for having less to run

## Four Ways to Get a Pizza

**Tags:** `core idea` (blue), `the rental ladder` (green), `pizza analogy` (orange)

- **The dinner problem** — a ten-person team needs pizza tonight; the only question is how much to outsource
- **Made at home** — buy flour, cheese, an oven, the dishes: you own every step; this is on-premises
- **Take and bake** — the shop assembles the pizza, your oven cooks it: IaaS rents machines, you run the rest
- **Delivered** — a hot pizza arrives, you supply the table and drinks: PaaS runs servers, you push code
- **Dining out** — you just show up and order: SaaS is finished software you configure but never operate

*Example (italic):* The same web app can run on a rack you bought (on-prem), rented VMs (IaaS), a push-to-deploy platform (PaaS), or a vendor's hosted product (SaaS).

**Key point:** IaaS, PaaS, and SaaS are rungs on a rental ladder — each step up rents one more layer of the stack, so there is less to operate and less you control.

### Visualization (canvas `c1`, 720×300)

Stair-step diagram of the four rungs, each a labeled box climbing left-to-right from on-premises up to SaaS, with the pizza analogy inside each box.

- **Title (bold 15px, `#1a5276`, top center):** "The Rental Ladder: Four Ways to Run the Same App".
- **Boxes:** four rounded boxes 165×58, 8px radius, stair-stepped up left to right at (x, top y): on-premises (25, 215), IaaS (195, 165), PaaS (365, 115), SaaS (535, 65). Fill `rgba(26,82,118,0.10)`; 2px borders: on-prem blue `#2a78d6`, IaaS aqua `#199e70`, PaaS orange `#d95926`, SaaS green `#008300`.
- **Box text:** line 1 bold 13px `#2c3e50` model name ("on-premises", "IaaS", "PaaS", "SaaS"); line 2 11px `#6b7280` pizza analogy ("made at home", "take and bake", "delivered", "dining out").
- **Captions under each box (11px `#444`, centered):** "you own everything" / "rent VMs, disks, networks" / "rent a runtime — push code" / "rent finished software".
- **Annotation (bold 12px violet `#4a3aa7`, at x≈40, y≈70, left-aligned):** "each rung up: you run less, you control less".
- **Caption (12px `#444`, bottom right):** "rung heights schematic".

## Who Patches the Operating System?

**Tags:** `worked example` (blue), `responsibility split` (green)

- **Nine layers** — the stack runs from networking and buildings up through OS, runtime, and your code
- **On-premises** — you handle all 9 layers: patching, scaling, backups, even the power bill
- **IaaS** — provider takes the bottom 4 (servers, storage, network, virtualization); you keep 5
- **PaaS** — provider also patches the OS, middleware, and runtime; you keep 2: code and data
- **SaaS** — you keep 1: your data and configuration; the vendor patches, scales, and backs up
- **The test questions** — who patches the OS? who scales it? who restores the backup at 3am?

*Example (italic):* When a critical OS vulnerability drops, the on-prem and IaaS teams patch servers that night; the PaaS and SaaS teams just read a status page.

**Key point:** The responsibility-split table is the real definition of each model — 9, 5, 2, then 1 layers left on your plate as you climb the ladder.

### Visualization (canvas `c2`, 720×300)

Responsibility matrix: 9 stack layers as rows, the 4 models as columns, each cell colored by who runs that layer — you (blue) or the provider (green).

- **Title (bold 15px, `#1a5276`, top center):** "Who Runs What: 9 Layers × 4 Models".
- **Legend (11px `#444`, top left at x=20, y=52):** blue swatch `rgba(42,120,214,0.55)` "you", green swatch `rgba(0,131,0,0.45)` "provider".
- **Column headers (bold 12px `#1a5276`, y=58, centered):** "on-prem" at x=320, "IaaS" at x=425, "PaaS" at x=530, "SaaS" at x=635.
- **Row labels (12px `#444`, right-aligned at x=255), rows at y=78 stepping 19px:** "application code", "data & configuration", "runtime", "middleware", "operating system", "virtualization", "physical servers", "storage", "networking".
- **Cells:** 96×15 rects centered on each column x. YOU cells (blue): on-prem rows 1–9; IaaS rows 1–5; PaaS rows 1–2; SaaS row 2 only. All other cells PROVIDER (green).
- **Totals row (bold 12px `#2a78d6`, y=262, centered per column):** "9", "5", "2", "1"; 11px `#6b7280` label "layers on you" right-aligned at x=255, y=262.
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=288):** "the 3am pager moves to the provider as you climb".
- **Caption (12px `#444`, bottom right):** "classic shared-responsibility split".

## Why Companies Climbed the Ladder

**Tags:** `where it's used` (blue), `economics` (green), `data stack` (orange)

- **Capex to opex** — buying servers is a big upfront purchase; renting is a monthly bill that can shrink
- **Elasticity** — demand peaks at 100 servers in December but sits near 20 in January
- **Renting the spike** — pay $50 per server-month only when used: $25,200 for the whole year
- **Owning the peak** — 100 owned servers at $30/month amortized cost $36,000, used or idle
- **The counter-current** — steady at 100 all year, renting costs $60,000; some firms have moved back
- **The data stack** — a hosted warehouse is SaaS, a managed Spark cluster is PaaS, a DB on raw VMs is IaaS

*Example (italic):* The spiky retailer saves $10,800 a year by renting; a steady service at the same peak would pay $24,000 extra to rent — the publicly discussed repatriation cases live here.

**Key point:** Cloud economics reward spiky and uncertain workloads — you pay for the peak only while it lasts; large steady workloads can be cheaper owned.

### Visualization (canvas `c3`, 720×300)

Line chart of monthly compute cost over one year: rented cost tracks the demand curve while owned cost is a flat line sized for the December peak.

- **Title (bold 15px, `#1a5276`, top center):** "Spiky Demand: Renting Follows the Curve, Owning Pays for the Peak".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 180; x = months Jan–Dec, 12px `#444` tick labels every other month ("Jan", "Mar", "May", "Jul", "Sep", "Nov"); y = monthly cost $0–$5,000, gridlines `#e5e9ef` at 1000/2000/3000/4000 with 11px `#6b7280` labels "$1k"–"$4k".
- **Rented line:** green `#008300` 3px line through the 12 monthly costs `[1000, 1100, 1250, 1200, 1400, 1500, 1750, 2000, 2250, 2750, 4000, 5000]` (server demand `[20, 22, 25, 24, 28, 30, 35, 40, 45, 55, 80, 100]` × $50).
- **Owned line:** blue `#2a78d6` 3px flat line at 3000 across the full plot; bold 12px blue label "owned: $3,000/mo flat" above the line near Feb.
- **Annotation (bold 13px green `#008300`, near Jun, y≈110):** "year total: rent $25,200 vs own $36,000".
- **Annotation (bold 12px orange `#d95926`, right-aligned near Dec, y≈52):** "steady at peak → rent $60,000: owning wins".
- **Caption (12px `#444`, bottom right):** "prices illustrative: $50/server-mo rented, $30/server-mo owned".

## Renting the Wrong Rungs

**Tags:** `common mistake` (red), `lock-in` (orange)

- **The tie** — each rung up binds you to provider-specific behavior: PaaS APIs, SaaS data formats
- **Exit cost** — leaving a SaaS warehouse means exporting the data and rewriting everything that touched it
- **Mistake one** — rebuilding commodity layers on IaaS: all of the burden, none of the differentiation
- **Mistake two** — renting the layer you compete on, so your edge behaves however the vendor decides
- **The heuristic** — rent the layers that don't differentiate you; keep control where you compete

*Example (italic):* A pricing startup runs email and payroll as SaaS but keeps its pricing engine on IaaS it controls — the engine is the business.

**Common mistake:** Treating the ladder as a ranking where higher is always better. Each rung up trades control and portability for convenience — lock-in is the price of that convenience, so climb deliberately, layer by layer.

### Visualization (canvas `c4`, 720×300)

Three-line chart across the four rungs: control and operational burden fall as you climb while lock-in rises, crossing between IaaS and PaaS.

- **Title (bold 15px, `#1a5276`, top center):** "The Trade-off Axis: Control Falls, Burden Falls, Lock-in Rises".
- **Axes:** baseline 2px `#999` at y=245; four rung positions at x = 130, 290, 450, 610 with bold 12px `#444` labels below the baseline: "on-prem", "IaaS", "PaaS", "SaaS"; y = relative score 0–100 mapped to plot height 180 (top y=65), gridlines `#e5e9ef` at 25/50/75.
- **Control line:** blue `#2a78d6` 3px through scores `[95, 70, 40, 15]`, 4px-radius dots at each point; bold 12px blue label "control & flexibility" near (300, above the 70 point).
- **Burden line:** magenta `#d55181` 3px through scores `[100, 50, 20, 5]`, dots; bold 12px magenta label "operational burden" near (295, below the 50 point).
- **Lock-in line:** orange `#d95926` 3px through scores `[10, 30, 60, 85]`, dots; bold 12px orange label "provider lock-in" near (300, below the 30 point).
- **Annotation (bold 13px violet `#4a3aa7`, near x=380, y=75):** "between IaaS and PaaS, convenience overtakes control".
- **Caption (12px `#444`, bottom right):** "scores illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); server counts, prices, and trade-off scores are invented and labeled illustrative; the layer counts (9 / 5 / 2 / 1) follow the classic shared-responsibility split, and every dollar figure in the text ($25,200 / $36,000 / $60,000 / $10,800 / $24,000) is derived from the chart's demand array at $50 rented and $30 owned per server-month.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
