# Microservices vs Monolith

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Microservices vs Monolith

**Subtitle:** A monolith's modules talk through function calls in one program; microservices talk through network hops between many — you pay the hops to buy team autonomy

## The Coffee Shop App: One Program or Four Services

**Tags:** `core idea` (blue), `architecture` (green), `one process vs many` (orange)

- **The app** — a coffee chain's ordering app does four jobs: menu, orders, payment, loyalty
- **Monolith** — all four jobs live in one program; a job asks another with a plain function call
- **Microservices** — each job becomes its own small program, talking to the others over the network
- **The trade** — every in-process call becomes a network hop; every module becomes a team's own service
- **Same features** — the customer sees the identical app either way; the split is behind the counter

*Example (italic):* When a customer taps "buy latte", the monolith calls four functions; the microservices version sends four network requests.

**Key point:** The choice is not about features — it is about whether the parts talk through function calls inside one process or network hops between independently run programs.

### Visualization (canvas `c1`, 720×300)

Side-by-side architecture diagram: one monolith box containing four modules with in-process arrows, vs four separate service boxes connected by dashed network arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Same Four Jobs: One Process vs Four Services on a Network".
- **Left half — monolith:** large rounded box x=40–330, y=60–265, 2px ink `#1a5276` border, fill `rgba(42,120,214,0.06)`, bold 13px ink label "monolith (one process)" at its top; inside, four small rounded boxes 120×34 (fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text) labeled "menu", "orders", "payment", "loyalty" at (x=60,y=100), (x=190,y=100), (x=60,y=180), (x=190,y=180); solid 2px blue `#2a78d6` arrows between them, one 11px mute `#6b7280` label "function call" beside the top arrow.
- **Right half — microservices:** four rounded boxes 120×34 at (x=400,y=80), (x=560,y=80), (x=400,y=200), (x=560,y=200), each with its own 2px border in blue `#2a78d6`, green `#008300`, aqua `#199e70`, violet `#4a3aa7`, fills at 0.12 alpha, labeled "menu svc", "orders svc", "payment svc", "loyalty svc"; dashed (dash 5/4) 2px mute `#6b7280` arrows connecting all four, one 11px mute label "HTTP over network" beside the top arrow.
- **Annotation (bold 13px ink `#1a5276`, bottom center y=285):** "inside: function calls — outside: network hops".
- **Caption (12px `#444`, bottom right):** "layout schematic".

## Counting the Hops in One Latte Order

**Tags:** `worked example` (blue), `latency` (orange), `reliability` (green)

- **The flow** — one tap hits menu, then orders, then payment, then loyalty: 4 calls per order
- **In process** — a function call is well under 0.001 ms; call it that: 4 × 0.001 = 0.004 ms
- **Over the network** — a hop costs about 2 ms round trip, so microservices pay 4 × 2 = 8 ms
- **The ratio** — 8 ms vs 0.004 ms: the same order pays at least 2,000× more call overhead
- **Reliability** — if each hop succeeds 99.9% of the time, 0.999⁴ ≈ 99.6%: ~40 of 10,000 orders hit a failed hop

*Example (italic):* The 8 ms is invisible to a human, but every hop can fail, retry, or time out — a function call cannot.

**Key point:** The arithmetic is hand-checkable: 4 hops × 2 ms = 8 ms per order, and 0.999 multiplied by itself 4 times ≈ 0.996 — latency and failure both compound per hop.

### Visualization (canvas `c2`, 720×300)

Step line chart of cumulative call overhead across the four calls of one order: monolith flat near zero, microservices climbing 2 ms per hop to 8 ms.

- **Title (bold 15px, `#1a5276`, top center):** "One Order, Four Calls: Cumulative Overhead per Hop".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = call number 0 to 4 with 12px `#444` tick labels "start", "menu", "orders", "payment", "loyalty"; y = milliseconds 0 to 10, gridlines `#e5e9ef` at 2.5/5/7.5, 12px `#444` labels "0", "2.5", "5", "7.5", "10 ms".
- **Microservices line:** orange `#d95926` 3px step line through calls `[0, 1, 2, 3, 4]`, cumulative ms `[0, 2, 4, 6, 8]`, filled circles radius 4 at each point, 12px orange value labels "2", "4", "6", "8" above the points.
- **Monolith line:** blue `#2a78d6` 3px line through the same calls, cumulative ms `[0, 0.001, 0.002, 0.003, 0.004]` — visually flat along the baseline, 12px blue label "monolith: 0.004 ms total" just above it near call 3.
- **Annotation (bold 13px orange `#d95926`, near call 2, y=80):** "same order, at least 2,000× more call overhead".
- **Caption (12px `#444`, bottom right):** "timings illustrative; 2 ms is a typical in-datacenter round trip".

## What the Hops Buy: Four Teams, Four Clocks

**Tags:** `where it's used` (blue), `team autonomy` (green), `data scientist angle` (orange)

- **One release train** — in the monolith all four teams share one deploy; one team's bug delays everyone
- **Independent deploys** — with services, the payment team ships 12 times a month without asking anyone
- **The tally** — teams ship 12, 10, 8, and 6 deploys a month on their own vs 4 shared trains for all
- **Independent scaling** — the payment service can run 10 copies at rush hour while loyalty runs 1
- **Data scientist angle** — one orders table becomes four databases; joining orders to loyalty means stitching events

*Example (italic):* A loyalty-points bug fix waits a week for the next monolith release train but ships in an hour as its own service.

**Key point:** The network hops are the price; independent deploys, scaling, and ownership are what they buy — judge the split by whether that autonomy is actually collected.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of deploys per month: one bar for the monolith's shared release train vs four per-team service bars.

- **Title (bold 15px, `#1a5276`, top center):** "Deploys per Month: One Shared Train vs Four Independent Teams".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = deploys per month 0 to 14, gridlines `#e5e9ef` at 4/8/12 with 12px `#444` labels.
- **Bars (70px wide, centered at x = 130, 270, 380, 490, 600), values `[4, 12, 10, 8, 6]`:**
  - "monolith — all 4 teams": ink fill `rgba(26,82,118,0.35)`, 2px `#1a5276` border
  - "payment svc": green `#008300` fill at 0.35 alpha, 2px border
  - "orders svc": aqua `#199e70` fill at 0.35 alpha, 2px border
  - "loyalty svc": violet `#4a3aa7` fill at 0.35 alpha, 2px border
  - "menu svc": yellow `#c98500` fill at 0.35 alpha, 2px border
- **Labels:** bold 13px value labels ("4", "12", "10", "8", "6") in each bar's border color above the bar; 12px `#444` category labels below the baseline (two lines where needed).
- **Divider:** vertical dashed (dash 4/3) `#6b7280` line at x=200 separating monolith from services.
- **Annotation (bold 13px green `#008300`, upper left near x=230, y=70):** "36 deploys vs 4 — each team ships on its own clock".
- **Caption (12px `#444`, bottom right):** "deploy counts illustrative".

## The Distributed Monolith: Paying Without Collecting

**Tags:** `common mistake` (red), `distributed monolith` (orange)

- **The trap** — services whose releases must still be coordinated: hops paid, autonomy not gained
- **The smell** — a one-line change needs three services deployed together in a fixed order
- **Shared database** — services writing to one shared database are one program wearing four costumes
- **Premature split** — a 3-person team running 12 services spends its week on plumbing, not features
- **The cheap alternative** — a modular monolith keeps clean module boundaries without the network hops

*Example (italic):* A team splits the app into services, but every release still needs all of them deployed together — now with 8 ms of hops and none of the autonomy.

**Common mistake:** Counting the services instead of the autonomy. If services cannot be deployed, scaled, and owned independently, you have bought the network hops and left the team autonomy on the shelf.

### Visualization (canvas `c4`, 720×300)

Quadrant scatter: overhead paid (x) vs autonomy gained (y), placing monolith, modular monolith, microservices, and the distributed monolith.

- **Title (bold 15px, `#1a5276`, top center):** "Pay the Hops Only If You Collect the Autonomy".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = call overhead per order with just two 12px `#444` labeled positions, "0.004 ms (in-process)" at x=210 and "8 ms (network hops)" at x=520; y = independent deploys per month 0 to 40, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels.
- **Quadrant split:** vertical and horizontal dashed (dash 4/3) `#e5e9ef` 1px lines at x=365 and the y=20 gridline.
- **Points (filled circles radius 9, bold 12px labels beside each in the point's color), overhead ms / deploys per month:**
  - "monolith" blue `#2a78d6` at `(0.004, 4)` → pixel (210, y for 4)
  - "modular monolith" aqua `#199e70` at `(0.004, 8)` → pixel (210, y for 8), label offset left to avoid overlap
  - "microservices" green `#008300` at `(8, 36)` → pixel (520, y for 36)
  - "distributed monolith" red `#e74c3c` at `(8, 4)` → pixel (520, y for 4)
- **Annotation (bold 13px red `#e74c3c`, near the distributed-monolith point, y=215):** "worst of both: hops paid, autonomy not collected".
- **Caption (12px `#444`, bottom right):** "positions illustrative; x positions schematic, not to scale".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the per-order hop counts, timings (`[0, 2, 4, 6, 8]` ms vs `[0, 0.001, 0.002, 0.003, 0.004]` ms), deploy counts (`[4, 12, 10, 8, 6]` per month), and quadrant positions are invented and labeled illustrative; the reliability figure 0.999⁴ ≈ 99.6% is exact arithmetic for the stated per-hop rate.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
