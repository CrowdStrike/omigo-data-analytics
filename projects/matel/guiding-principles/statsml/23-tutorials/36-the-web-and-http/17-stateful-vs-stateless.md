# Stateful vs Stateless

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Stateful vs Stateless

**Subtitle:** If a server remembers nothing between requests, any copy of it can answer the next one — that single property decides what scales by just adding machines

## One Coffee Order, Any Register Can Take It

**Tags:** `core idea` (blue), `no memory between requests` (green), `web tier` (orange)

- **The shop** — a coffee chain's online ordering site runs on 3 identical web servers behind a load balancer
- **Stateless** — each request carries everything needed: "add 1 latte to cart #4127", cart lives in a database
- **Any server** — request 1 lands on server A, request 2 on server C; both look up cart #4127 and it just works
- **Stateful** — the alternative keeps your cart in server B's RAM, so every request of yours must go back to B
- **The pin** — that "must go back to B" is called a sticky session, and it quietly chains you to one machine

*Example (italic):* Your "add a muffin" click hits server C while your latte sits in a database row — server C adds the muffin to the same cart #4127 without ever knowing server A took the first click.

**Key point:** A server is stateless when it keeps no per-user memory between requests — the request plus shared storage is the whole story, so every server is interchangeable.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: three requests from one customer fanning out to any of 3 servers (stateless, all sharing one cart store) vs the same three requests all pinned to server B (stateful, cart in B's RAM).

- **Title (bold 15px, `#1a5276`, top center):** "Same Customer, Three Clicks: Any Server vs Always Server B".
- **Row 1 (y≈95), label 12px `#444` at x=20:** "stateless"; a blue `#2a78d6` rounded box at x=120 labeled "3 clicks" (12px), three 2px blue arrows fanning to three aqua `#199e70` boxes at x=330 stacked at y=55/95/135 labeled "server A" / "server B" / "server C", then three arrows converging to a violet `#4a3aa7` box at x=560 labeled "cart #4127 (DB)".
- **Row 2 (y≈230), label:** "stateful"; the same blue "3 clicks" box at x=120, three 2px orange `#d95926` arrows all bending into ONE green `#008300` box at x=330 labeled "server B — cart in RAM", with two mute `#6b7280` dashed boxes at x=330, y=195/265 labeled "server A (idle)" / "server C (idle)".
- **Box style:** 130–170px wide, 34px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(25,158,112,0.12)` / `rgba(0,131,0,0.12)` / `rgba(74,58,167,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=160):** "stateless: the cart, not the server, holds the memory".
- **Caption (12px `#444`, bottom right):** "request routing schematic, illustrative".

## Adding a Fourth Server: 300 Each vs a Cold Start

**Tags:** `worked example` (blue), `hand-checkable` (green), `scale up` (orange)

- **The load** — Monday rush: 1,200 requests per minute spread across the 3 servers
- **Before** — stateless split is even: 1,200 ÷ 3 = 400 requests/min per server
- **Add one** — a 4th server joins; the balancer immediately sends it its share: 1,200 ÷ 4 = 300 each
- **Stateful version** — existing sessions stay pinned: A, B, C keep 380 each and the new server D gets only 60
- **The check** — both columns still sum to 1,200; only the stateless one actually relieved the hot servers

*Example (italic):* Ten minutes after adding server D, the stateless tier runs at 300/300/300/300 while the sticky-session tier still grinds at 380/380/380/60.

**Key point:** Adding a stateless server helps instantly (400 → 300 per server); adding a stateful one helps only new sessions, so the machines you bought it to relieve stay hot.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: requests/min per server after adding the 4th server — stateless group (300/300/300/300) vs sticky-session group (380/380/380/60), with the old 400 level as a dashed reference line.

- **Title (bold 15px, `#1a5276`, top center):** "After Adding Server D: 1,200 req/min Rebalanced vs Still Pinned".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = requests/min 0 to 400, gridlines `#e5e9ef` at 100/200/300/400 with 12px `#444` labels; x = two clusters of 4 bars labeled "A" "B" "C" "D" (12px `#444`), cluster titles bold 12px `#2c3e50` "stateless" (centered ≈x=200) and "sticky sessions" (centered ≈x=500).
- **Stateless bars:** four blue `#2a78d6` bars (fill `rgba(42,120,214,0.30)`, 2px solid top edge) at heights for `[300, 300, 300, 300]`, 40px wide, 16px gaps, 11px value labels on top.
- **Sticky bars:** three orange `#d95926` bars (fill `rgba(217,89,38,0.30)`) for `[380, 380, 380]` plus one mute `#6b7280` bar for `[60]` labeled "60 — new sessions only" in 11px.
- **Reference line:** dashed `#6b7280` (dash 4/3) horizontal line at the 400 level across the plot, 12px `#6b7280` label "before: 400 each" at its left end.
- **Annotation (bold 13px green `#008300`, above the stateless cluster, y≈70):** "every server drops 400 → 300 the moment D joins".
- **Caption (12px `#444`, bottom right):** "req/min illustrative; both groups sum to 1,200".

## Crashes, Deploys, and the 2am Autoscaler

**Tags:** `where it's used` (blue), `restarts & deploys` (green), `horizontal scale` (orange)

- **A crash** — stateless: server B dies, the balancer reroutes, zero carts lost; stateful: B's 400 in-RAM carts vanish
- **A deploy** — stateless servers restart one by one and traffic flows around them; stateful restarts log users out
- **Autoscaling** — a rule like "add a server above 350 req/min each" only works if the new server helps immediately
- **Load balancing** — stateless lets the balancer pick the least-busy server per request instead of per user
- **The pattern** — this is why web tiers are built stateless and the remembering is pushed to databases and caches

*Example (italic):* During Tuesday's deploy the stateless tier restarts all 4 servers in sequence and drops 0 carts; the same deploy on the sticky tier would wipe about 1,140 pinned sessions (380 × 3).

**Key point:** Statelessness is what makes the boring operations — restart, deploy, scale out, survive a crash — free; stateful tiers turn each of those into a customer-visible event.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: carts/sessions lost during three routine events (one server crash, rolling deploy, scale-in removing a server) — stateless vs sticky-session tier side by side per event.

- **Title (bold 15px, `#1a5276`, top center):** "Sessions Lost During Routine Operations: Stateless vs Sticky".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 420 representing 1,200 sessions; 12px `#444` scale labels "0" at x=230 and "1,200" at x=650 under a thin `#e5e9ef` rule.
- **Rows (three event groups, group labels left-aligned 12px `#2c3e50` at x=20, at y = 70, 140, 210):**
  - "server B crashes": blue `#2a78d6` bar width 0 labeled "0" (11px), below it orange `#d95926` bar width 140 for `400` labeled "400 carts gone"
  - "rolling deploy (all 4)": blue bar width 0 labeled "0", below it orange bar width 399 for `1,140` labeled "1,140 sessions wiped"
  - "scale-in removes D": blue bar width 0 labeled "0", below it mute `#6b7280` bar width 21 for `60` labeled "60"
- **Bar style:** 14px tall, 6px gap within a group, stateless bars drawn as a 2px blue tick at the baseline (width 0) so the "0" stays visible, sticky bars solid fills at 0.85 alpha.
- **Annotation (bold 13px magenta `#d55181`, right side near y=260):** "stateless makes restarts and rescues a non-event".
- **Caption (12px `#444`, bottom right):** "session counts illustrative — 400/server, 1,140 = 380 × 3".

## Stateless Doesn't Mean the State Disappears

**Tags:** `common mistake` (red), `state moves, not vanishes` (orange)

- **The confusion** — people hear "stateless" and think the app forgot the cart; the cart is fine, it just lives elsewhere
- **Where it goes** — into a database row, a shared cache, or a signed cookie the browser sends back each time
- **HTTP itself** — the protocol is stateless by design; cookies and sessions are layers that add memory on top
- **The new bottleneck** — push all carts into one database and the scaling problem moves there instead of vanishing
- **The mistake** — calling a tier stateless while one server still holds uploads, locks, or counters in local RAM

*Example (italic):* A team declares its tier stateless, yet in-progress file uploads buffer on local disk — the first rolling deploy kills every upload and the postmortem rediscovers the pinned state.

**Common mistake:** Treating stateless as "no state anywhere." The state still exists — the design question is only whether it sits inside one replaceable server or in shared storage every server can reach.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the cart's home in each design — stateful (cart inside server B's RAM, lost when B dies) vs stateless (cart in shared store, readable by any server, but the store must now scale).

- **Title (bold 15px, `#1a5276`, top center):** "The State Doesn't Vanish — It Moves to Shared Storage".
- **Row 1 (y=95), label 12px `#444` at x=20:** "stateful"; green `#008300` rounded box at x=180 labeled "server B RAM: cart #4127" (12px), 3px arrow to a red `#e74c3c` box at x=440 labeled "B restarts — cart lost" with bold 12px red "✗ 400 carts".
- **Row 2 (y=205), label:** "stateless"; three small aqua `#199e70` boxes stacked at x=170 (y=175/205/235) labeled "A" / "B" / "C", three 2px arrows converging on a violet `#4a3aa7` box at x=400 labeled "shared store: all carts", then a dashed `#6b7280` arrow to a yellow `#c98500` box at x=590 labeled "must scale too".
- **Box style:** 120–180px wide, 36px tall, 8px radius, fills `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)` / `rgba(25,158,112,0.12)` / `rgba(74,58,167,0.12)` / `rgba(201,133,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "you didn't delete the state problem — you relocated it to something built for it".
- **Caption (12px `#444`, bottom right):** "cart counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); traffic numbers are invented and labeled illustrative: 1,200 req/min total, stateless split `[400, 400, 400]` before and `[300, 300, 300, 300]` after adding server D, sticky split `[380, 380, 380, 60]` after; sessions lost per event `[0 vs 400]` (crash), `[0 vs 1,140]` (rolling deploy, 380 × 3), `[0 vs 60]` (scale-in); cart id #4127 is a label only.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
