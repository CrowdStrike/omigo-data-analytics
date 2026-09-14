# Rate Limiting

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Rate Limiting

**Subtitle:** A token bucket lets a service absorb a short burst but hold a steady pace — extra requests get a polite "try again in a moment" instead of a crash

## The Pizza Shop That Says "Not Yet"

**Tags:** `core idea` (blue), `saying no gracefully` (green), `capacity` (orange)

- **The kitchen** — a pizza shop's online ordering page feeds ovens that can bake 6 pizzas per minute
- **The rush** — at 6:04pm a game ends nearby and orders spike to 12 per minute, double the ovens
- **The bad yes** — accepting everything means every pizza runs late and every customer gets it cold
- **The graceful no** — the site takes 6 orders and tells the rest "kitchen full — retry in 30 seconds"
- **The name** — rate limiting is exactly this: capping how fast requests are accepted, on purpose

*Example (italic):* At 6:04pm the site accepts 6 orders, asks 6 customers to retry in 30 seconds, and every accepted pizza still arrives hot.

**Key point:** Rate limiting protects the work you already accepted — a clear, early "not yet" beats a slow "yes" that fails everyone.

### Visualization (canvas `c1`, 720×300)

Line chart of incoming orders per minute across an evening rush, with a flat capacity line at 6/min; the spike above the line is the demand that must be refused gracefully.

- **Title (bold 15px, `#1a5276`, top center):** "6:04pm Rush: 12 Orders a Minute vs 6 the Ovens Can Bake".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "6:00" to "6:10" with 12px `#444` tick labels every 2 minutes; y = orders per minute 0 to 14, gridlines `#e5e9ef` at 4/8/12.
- **Orders line:** blue `#2a78d6` 3px line through minutes `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, orders `[3, 4, 5, 9, 12, 8, 5, 4, 3, 3, 2]`, small filled dots at each point.
- **Capacity line:** dashed orange `#d95926` (dash 6/4) 2px horizontal line at 6 orders/min, 12px orange label "oven capacity: 6/min" at its right end.
- **Excess shading:** red fill `rgba(231,76,60,0.15)` between the orders line and the capacity line wherever orders exceed 6 (minutes 3–5).
- **Annotation (bold 13px red `#e74c3c`, near minute 4, y=70):** "the shaded orders must hear a graceful no".
- **Caption (12px `#444`, bottom right):** "order counts illustrative".

## A Bucket of Ten Tokens, One Back Per Second

**Tags:** `worked example` (blue), `token bucket` (green)

- **The bucket** — the limiter holds at most 10 tokens; each accepted order spends exactly one
- **The refill** — 1 token drips back every second, and the bucket never fills past 10
- **The burst** — at second 0 six orders land at once: the full bucket pays 10 − 6 = 4 tokens left
- **The drain** — 2 orders/sec keep arriving against 1 token/sec refill: 4, 3, 2, 1, 0 by second 4
- **Empty bucket** — from second 5 on, each second's lone token serves 1 order and 1 is told to wait

*Example (italic):* At second 4 the last held token plus the refill serve both orders; from second 5 each second's lone refill token splits the pair — one accepted, one asked to retry.

**Key point:** Capacity sets the burst allowance and refill sets the sustained pace — a 10-token bucket refilling at 1/sec absorbs a 10-order burst but never sustains more than 60 orders a minute.

### Visualization (canvas `c2`, 720×300)

Step-line chart of the token count over the 12 seconds after the burst, with the capacity ceiling dashed at 10 and red ✗ marks on every second where an order is rejected.

- **Title (bold 15px, `#1a5276`, top center):** "Token Bucket: Burst of 6 at t=0, Then 2 Orders/sec vs 1 Token/sec".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 12 with 12px `#444` tick labels every 2 seconds; y = tokens 0 to 10, gridlines `#e5e9ef` at 2/4/6/8.
- **Capacity line:** dashed `#6b7280` (dash 4/3) 2px horizontal line at 10 tokens, 12px `#6b7280` label "capacity 10" at its left end above the line.
- **Token line:** blue `#2a78d6` 3px step line through seconds `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]`, tokens `[4, 3, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]`, filled dots at each point; 12px blue label "10 → 4 after the burst" beside the first point.
- **Rejection marks:** bold 13px red `#e74c3c` "✗" glyphs just above the baseline at seconds `[5, 6, 7, 8, 9, 10, 11, 12]`, one per second, with a single 12px red label "1 order rejected each second" above the row.
- **Annotation (bold 13px green `#008300`, near x=8, y=95):** "refill rate = the sustained pace: 60 orders/min".
- **Caption (12px `#444`, bottom right):** "counts exact for the stated rules; the scenario is illustrative".

## Why Servers Ration Their Own Door

**Tags:** `where it's used` (blue), `overload` (red), `fairness` (green)

- **Collapse** — a server pushed past capacity slows for everyone, queues balloon, then it falls over
- **The cap** — a limiter set at 150 requests/sec keeps serving 150 no matter how hard the flood hits
- **Fair share** — per-client buckets stop one greedy script from starving every other user
- **The polite no** — a rejected request gets an instant answer plus a wait hint, not a hung timeout
- **Everyday spots** — public APIs, login attempts, checkout buttons, scrapers hitting a data pipeline

*Example (italic):* At 300 incoming requests/sec the unprotected server completes 0, while the rate-limited one still completes its full 150.

**Key point:** Rate limiting turns overload from a cliff into a plateau — the service does less than was asked, but never less than it can.

### Visualization (canvas `c3`, 720×300)

Line chart of completed requests/sec as incoming load ramps up: the unprotected server climbs then collapses to zero, the rate-limited server plateaus at its cap.

- **Title (bold 15px, `#1a5276`, top center):** "Ramping Load: Collapse Without a Limiter, Plateau With One".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = incoming requests/sec 50 to 300 with 12px `#444` tick labels at 50/100/150/200/250/300; y = completed requests/sec 0 to 300, gridlines `#e5e9ef` at 75/150/225.
- **No-limiter line:** red `#e74c3c` 3px line through load `[50, 100, 150, 200, 250, 300]`, completed `[50, 100, 140, 90, 20, 0]` — climbs, then collapses as the queue drowns the server.
- **Limiter line:** green `#008300` 3px line through the same load points, completed `[50, 100, 150, 150, 150, 150]` — flat plateau at the 150/sec cap, with dashed `#6b7280` (dash 4/3) horizontal guide at 150 labeled "cap 150/s" in 12px `#6b7280`.
- **Labels:** bold 12px red "no limiter" near (250, 20); bold 12px green "with limiter" near (250, 150).
- **Annotation (bold 13px violet `#4a3aa7`, near x=300, y=80):** "at 300/s incoming: 0 vs 150 completed".
- **Caption (12px `#444`, bottom right):** "throughput curves illustrative".

## A 60-Per-Minute Limit Is Not One Per Second

**Tags:** `common mistake` (red), `fixed window` (orange), `bursts` (blue)

- **The label** — "60 requests per minute" reads like a smooth 1 per second, but limiters rarely mean that
- **Fixed window** — a counter that resets each minute lets all 60 land in the last second of a window
- **The double burst** — 60 at 0:59 plus 60 at 1:00 is 120 requests in two seconds, both windows "legal"
- **Token bucket** — capacity 10 refilling 1/sec bounds any instant burst at 10, sustained still 60/min
- **The silent no** — the other mistake: rejecting with no retry hint, so clients hammer back instantly

*Example (italic):* A client that saves its 60 calls for 0:59 and fires 60 more at 1:00 stays inside a fixed-window limit while hitting the server with 120 in two seconds.

**Common mistake:** Reading a rate limit as a smooth pace. A fixed window allows 2× bursts at its boundary; a token bucket makes the burst bound explicit — it is the bucket's capacity, nothing more.

### Visualization (canvas `c4`, 720×300)

Two-row bar diagram of per-second accepted requests around a minute boundary: fixed window admits two back-to-back bursts of 60, token bucket admits at most its 10-token capacity then a 1/sec trickle.

- **Title (bold 15px, `#1a5276`, top center):** "Same '60/min' Label, Very Different Bursts at the 1:00 Boundary".
- **Layout:** shared x axis of seconds `["0:58", "0:59", "1:00", "1:01", "1:02"]` with 12px `#444` labels at x = 150/260/370/480/590; dashed `#6b7280` (dash 4/3) vertical line at the 1:00 position labeled "window resets" in 12px `#6b7280` at its top.
- **Row 1 (bars sit on a 2px `#999` baseline at y=130), 12px `#444` label "fixed window" at x=20:** red `#e74c3c` bars 36px wide with per-second accepted counts `[0, 60, 60, 0, 0]`, heights scaled 1 count = 1.2px (60 → 72px tall), 11px count labels on top of each bar; bold 12px red bracket label "120 in 2 seconds — allowed" over the first tall bar, left of the reset line.
- **Row 2 (baseline at y=250), 12px `#444` label "token bucket (10, 1/sec)" at x=20:** green `#008300` bars 36px wide with per-second accepted counts `[0, 10, 1, 1, 1]`, same 1.2px scale, 11px count labels on top.
- **Annotation (bold 13px orange `#d95926`, right side near y=200):** "burst bound = bucket capacity: 10".
- **Caption (12px `#444`, bottom right):** "worst-case client behavior, counts exact for the stated rules".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); pizza-shop order counts (`[3,4,5,9,12,8,5,4,3,3,2]` vs capacity 6) and the throughput curves (`[50,100,140,90,20,0]` vs `[50,100,150,150,150,150]`) are invented and labeled illustrative; the token-bucket trace (`[4,3,2,1,0,...]` with rejections from second 5) and the boundary-burst counts (`[0,60,60,0,0]` vs `[0,10,1,1,1]`) follow exactly from the stated rules (capacity 10, refill 1/sec, burst 6, then 2 orders/sec; fixed window 60/min).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
