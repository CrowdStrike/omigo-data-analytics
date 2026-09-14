# Load Balancing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Load Balancing

**Subtitle:** A load balancer is the host at the front of a coffee stand who decides which of several identical baristas takes your order — one front door, work spread so no line stalls while another sits idle

## The Host Who Points You to a Barista

**Tags:** `core idea` (blue), `round-robin` (green), `one front door` (orange)

- **The stand** — a food-court coffee stand has three identical baristas and one host at the front
- **The host** — every customer talks to the host first; the host points each one to a barista
- **Round-robin** — the host rotates: customer 1 to barista 1, 2 to barista 2, 3 to barista 3, 4 back to 1
- **Why bother** — with no host, everyone joins the first line they see and one barista drowns
- **The name** — in a server farm the host is the load balancer and the baristas are identical servers

*Example (italic):* Six customers walk in; the host sends customers 1 and 4 to barista 1, 2 and 5 to barista 2, and 3 and 6 to barista 3 — two orders each.

**Key point:** A load balancer is one front door that spreads incoming requests across identical workers — round-robin, the simplest policy, just deals them out in rotation like cards.

### Visualization (canvas `c1`, 720×300)

Flow diagram of round-robin: one host box on the left, three barista boxes stacked on the right, six numbered customer arrows dealt out in rotation.

- **Title (bold 15px, `#1a5276`, top center):** "Round-Robin: the Host Deals Six Customers Out in Rotation".
- **Left box:** ink `#1a5276` rounded box "host (load balancer)" at (x=70, y=130, 160×48).
- **Right boxes (stacked):** blue `#2a78d6` box "barista 1 — orders 1, 4" at (x=460, y=58, 200×42); green `#008300` box "barista 2 — orders 2, 5" at (x=460, y=133, 200×42); violet `#4a3aa7` box "barista 3 — orders 3, 6" at (x=460, y=208, 200×42).
- **Box style:** 8px radius, fills at 0.12 alpha of each stroke color, 2px stroke, 12px `#2c3e50` labels centered.
- **Arrows:** three 2px arrows from the host box's right edge to each barista box's left edge, each in its barista's color; bold 12px labels "1, 4" / "2, 5" / "3, 6" in the matching color at each arrow midpoint.
- **Incoming queue:** six small 12px `#6b7280` circled numbers 1–6 in a row at (x=20–60, y=40 down to y=90) feeding a 2px `#6b7280` arrow into the host box, 12px `#6b7280` label "customers arrive" above them.
- **Annotation (bold 13px `#199e70` aqua, bottom center near y=282):** "every barista gets exactly two orders — counts equal by construction".
- **Caption (12px `#444`, bottom right):** "customer numbers illustrative".

## Nine Orders and Three Slow Blenders

**Tags:** `worked example` (blue), `least-connections` (green)

- **The setup** — nine orders arrive: six 1-minute espressos and three 5-minute blended drinks
- **Prep minutes** — in arrival order: 1, 5, 1, 1, 5, 1, 1, 5, 1 — the blenders are orders 2, 5, 8
- **Round-robin** — barista 2 gets orders 2, 5, 8: all three blenders, 5+5+5 = 15 minutes of work
- **Hand-check** — baristas 1 and 3 each get 1+1+1 = 3 minutes and stand idle from minute 3 on
- **Least-busy** — sending each order to the least-loaded barista ends the day at 9, 6, 6 minutes
- **Real servers** — counting open connections per server is the cheap live proxy for pending work

*Example (italic):* Round-robin finishes the last drink at minute 15; least-busy finishes at minute 9 — same nine orders, same three baristas, different host policy.

**Key point:** Round-robin balances counts, not work; least-connections routes each request to whoever is least busy right now, which wins whenever request costs vary.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: total prep minutes per barista under round-robin vs least-busy assignment of the same nine orders.

- **Title (bold 15px, `#1a5276`, top center):** "Same Nine Orders: Round-Robin Ends at Minute 15, Least-Busy at 9".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = minutes of work 0 to 16, gridlines `#e5e9ef` at 4/8/12 with 12px `#444` labels; x = three groups centered at x=170, 360, 550 with 12px `#444` labels "barista 1" / "barista 2" / "barista 3".
- **Bars:** per group two 48px-wide bars 8px apart; round-robin bars blue `#2a78d6` with minutes `[3, 15, 3]`; least-busy bars green `#008300` with minutes `[9, 6, 6]`; scale 11.25 px per minute; bold 12px value labels in each bar's color above each bar.
- **Legend (12px, top left inside plot):** blue swatch "round-robin", green swatch "least-busy".
- **Annotation (bold 13px orange `#d95926`, near x=420, y=50):** "round-robin hands barista 2 every blender — 15 min vs 9".
- **Caption (12px `#444`, bottom right):** "prep minutes illustrative; totals match the text hand-check".

## Three Servers Sharing 300 Requests a Second

**Tags:** `where it's used` (blue), `capacity` (green), `p99 latency` (orange)

- **The farm** — a website runs three identical servers, each able to handle 150 requests per second
- **Peak load** — at lunchtime 300 requests per second arrive at the site's single public address
- **Unbalanced** — if clients pick a favorite, server A sees 220 req/s — 47% over its 150 capacity
- **Balanced** — the load balancer gives each server 100 req/s, comfortably under its limit
- **Data science** — a deployed model endpoint is replicas behind a balancer; p99 lives on the hottest one
- **Health checks** — the balancer pings each server and quietly stops routing to one that fails

*Example (italic):* At 220 req/s, requests queue and time out on server A while B and C coast at 40 — total capacity (450) was never the problem.

**Key point:** Overload is rarely "not enough total capacity" — it is capacity in the wrong place; the balancer's whole job is keeping the hottest worker under its limit.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart of requests per second per server, unbalanced vs balanced, with a dashed per-server capacity line at 150.

- **Title (bold 15px, `#1a5276`, top center):** "300 req/s Across Three Servers: Only Balance Keeps Everyone Under 150".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = requests per second 0 to 240, gridlines `#e5e9ef` at 60/120/180 with 12px `#444` labels; x = three groups centered at x=170, 360, 550 with 12px `#444` labels "server A" / "server B" / "server C".
- **Bars:** per group two 48px-wide bars 8px apart; unbalanced req/s `[220, 40, 40]` — server A's bar orange `#d95926` (over capacity), B and C blue `#2a78d6`; balanced bars green `#008300` with req/s `[100, 100, 100]`; scale 0.75 px per req/s; bold 12px value labels above each bar.
- **Capacity line:** horizontal dashed magenta `#d55181` (dash 6/4) 2px line at 150 req/s (y=132.5), bold 12px magenta label "capacity 150 req/s each" at its right end.
- **Legend (12px, top right inside plot):** orange/blue swatch "unbalanced", green swatch "balanced".
- **Annotation (bold 13px magenta `#d55181`, near x=140, y=75):** "server A at 220 is 47% over — B and C sit at 40".
- **Caption (12px `#444`, bottom right):** "request rates illustrative".

## The Stampede When Barista 2 Comes Back

**Tags:** `common mistake` (red), `thundering herd` (orange)

- **The outage** — barista 2 steps out for ten minutes; 24 customers pile up waiting to be seated
- **The stampede** — the moment she returns, the host sends all 24 at once and she is swamped again
- **Server version** — a server passes its health check and every queued client retries in the same second
- **The rhythm** — clients on a fixed 30-second retry timer stay synchronized and stampede in unison
- **The fix** — add random jitter to retry timers and ramp the recovered worker up slowly (slow start)

*Example (italic):* 240 queued clients hit the recovered server in its first second — it handles 150 req/s, so it fails the next health check and the cycle repeats every 30 seconds.

**Common mistake:** Believing recovery ends the incident. The recovered worker is the most fragile one in the pool — without jitter and slow start, the waiting crowd is the next outage.

### Visualization (canvas `c4`, 720×300)

Line chart of requests per second hitting the recovered server over its first 90 seconds: synchronized retries (repeating spikes) vs jittered retries (steady trickle), with the capacity line.

- **Title (bold 15px, `#1a5276`, top center):** "Thundering Herd: Synchronized Retries Re-Kill the Server Every 30s".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds since recovery 0 to 90, 12px `#444` tick labels every 30s; y = requests per second 0 to 250, gridlines `#e5e9ef` at 50/100/150/200.
- **Synchronized line:** magenta `#d55181` 3px line through seconds `[0, 10, 20, 30, 40, 50, 60, 70, 80, 90]`, req/s `[240, 8, 5, 235, 9, 6, 230, 10, 7, 225]` — spikes at 0/30/60/90 as the fixed 30-second timers fire together.
- **Jittered line:** green `#008300` 3px line through the same seconds, req/s `[40, 55, 50, 48, 52, 47, 51, 49, 50, 48]` — flat trickle well under capacity.
- **Capacity line:** horizontal dashed `#6b7280` (dash 4/3) 2px line at 150 req/s, 12px `#6b7280` label "capacity 150" at its left end above the line.
- **Legend (12px, top right inside plot):** magenta swatch "fixed 30s retries", green swatch "jittered retries".
- **Annotation (bold 13px orange `#d95926`, near x=45s, y=60):** "240 clients strike together — every spike is another outage".
- **Caption (12px `#444`, bottom right):** "retry traffic illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the nine prep minutes (1,5,1,1,5,1,1,5,1), per-barista totals (round-robin 3/15/3, least-busy 9/6/6), server rates (unbalanced 220/40/40, balanced 100/100/100, capacity 150), and retry traces (spikes 240/235/230/225 vs jitter ~50) are invented and labeled illustrative; the 47%-over figure is 220/150 rounded.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
