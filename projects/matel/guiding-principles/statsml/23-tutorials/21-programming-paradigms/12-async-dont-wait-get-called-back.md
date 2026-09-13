# Async: Don't Wait, Get Called Back

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table — text left 50%, canvas right 50%)
**HTML title tag:** Async: Don't Wait, Get Called Back

**Subtitle:** Instead of standing still while each slow answer comes back, send all the requests out and collect the answers as they arrive

## Ten API Pages, Two Ways to Fetch Them

**Tags:** `core idea` (blue), `running example` (green)

- **The job** — download 10 pages of results from an API; each page takes ~2 s to answer
- **Sequential** — ask for page 1, wait 2 s, ask for page 2, wait 2 s, … total 20 s
- **Async** — fire all 10 requests at once, then collect answers as they land: ~2.3 s total
- **Nothing got faster** — each page still takes ~2 s; the waits now overlap instead of stacking
- **The callback** — "tell me when page 3 arrives" replaces "stand here until page 3 arrives"

*Example:* A waiter takes all 10 tables' orders, then delivers plates as the kitchen finishes — instead of standing at one table until its food is done.

**Key point:** Async doesn't speed up any single request — it stops you from wasting time waiting on one while nine others could already be in flight.

### Visualization (canvas `c1`, 720×300)

Timeline strip chart comparing sequential (segments end to end) vs async (10 overlapped rows) execution of 10 requests.

- **Title (bold 15px, `#1a5276`, top center):** "10 Pages x ~2 s Each: Stack the Waits, or Overlap Them".
- **Shared duration data (seconds), used across all charts:** `DUR = [2.0, 1.9, 2.2, 2.1, 1.8, 2.0, 2.3, 1.9, 2.1, 1.7]` (sum = 20.0, max = 2.3).
- **Time scale:** x maps 0–21 s onto plot width; left padding 105px, right 30px.
- **Segment colors cycle:** `#2a78d6` (blue), `#199e70` (aqua), `#4a3aa7` (violet), `#d95926` (orange), `#c98500` (yellow), repeated twice for the 10 requests.
- **Sequential row (y=66, bars 20px tall):** right-aligned bold label "Sequential" at left; 10 colored segments laid end to end from t=0 to t=20; orange bold annotation "done at 20.0 s" above the end.
- **Async rows (starting y=116, 10 rows of 11px height, 9px bars):** right-aligned bold label "Async"; all 10 bars start at t=0 with lengths per `DUR`; vertical green (`#008300`) dashed line (dash 5/4, width 2) at t=2.3 spanning the rows; green bold annotation "done at 2.3 s — ~9x faster" to its right.
- **Time axis (y=246):** gray `#999` line with ticks and labels "0 s", "5 s", "10 s", "15 s", "20 s" in muted gray `#6b7280`.
- **Caption (magenta `#d55181`, bold 13px, bottom center):** "same 10 requests, same ~2 s each — only the waiting is arranged differently".

## The Same Ten Requests, Second by Second

**Tags:** `worked example` (green), `timeline` (blue)

- **t = 0.00 s** — fire requests 1 through 10; sending each takes about a millisecond
- **t = 0.01 s** — all 10 are in flight; your program has nothing left to send
- **t = 1.7 s** — first answer lands (page 10); the callback stores it
- **t = 1.7–2.3 s** — the other nine arrive: 1.8, 1.9, 1.9, 2.0, 2.0, 2.1, 2.1, 2.2 s …
- **t = 2.3 s** — last answer (page 7) lands; job done — vs 20.0 s one at a time
- **Speedup** — 20.0 s ÷ 2.3 s ≈ 9x, from changing zero lines of server code

*Example:* The total async time is the slowest single request (2.3 s), not the sum of all ten (20 s).

**Key point:** Sequential time adds the waits (sum); async time overlaps them (max) — that is the whole arithmetic of the speedup.

### Visualization (canvas `c2`, 720×300)

Event timeline (stopwatch view) of the async run: fire marker at t=0, arrival stems between 1.7 and 2.3 s.

- **Title (bold 15px, `#1a5276`):** "The Async Run on a Stopwatch".
- **Axis (y=210):** x maps 0–2.6 s, left padding 70px, right 40px; ticks/labels at 0.0, 0.5, 1.0, 1.5, 2.0, 2.5 s in muted gray.
- **Fire marker:** thick blue (`#2a78d6`, width 3) vertical line at t=0 from axis up to y=70; blue bold label "t = 0.00: fire all 10 requests"; muted note below it "by t = 0.01 every request is in flight".
- **Arrivals:** the 10 durations sorted ascending, each drawn as an aqua (`#199e70`, width 2) vertical stem from the axis with a 4px dot on top and bold centered label "p<page>" above; stem heights staggered through [36, 58, 80, 102, 124] twice to avoid label collisions. Arrival pairs (page, t): p10 at 1.7, p5 at 1.8, p2 at 1.9, p8 at 1.9, p1 at 2.0, p6 at 2.0, p4 at 2.1, p9 at 2.1, p3 at 2.2, p7 at 2.3.
- **Last-arrival annotation:** green (`#008300`) bold right-aligned text "last answer (page 7) at 2.3 s — job done" near the top, with a green dashed vertical line (dash 5/4) at t=2.3 down to the axis.
- **Captions (bottom center):** magenta bold "total = the slowest request (max 2.3 s), not the sum of all ten (20.0 s)"; below in muted gray "arrivals: 1.7, 1.8, 1.9, 1.9, 2.0, 2.0, 2.1, 2.1, 2.2, 2.3 s".

## Waiting Is the Bottleneck, Not Computing

**Tags:** `why it matters` (blue), `where it's used` (green)

- **Look inside one 2 s fetch** — your code runs ~10 ms; the other ~1,990 ms is network wait
- **99% idle** — sequential fetching leaves the CPU doing nothing almost the whole time
- **A data scientist's day** — API pulls, database queries, file downloads, model-service calls
- **Real win** — enriching 1,000 rows via an API: hours sequentially, minutes with async batches
- **Be polite** — real APIs rate-limit; fire 10–50 at a time, not all 100,000 at once

*Example:* A notebook that "runs for 3 hours" pulling an API is usually idle for 2 hours 58 minutes of it.

**Key point:** If a job is slow because it waits (I/O-bound), async recovers nearly all the lost time — the machine was never busy in the first place.

### Visualization (canvas `c3`, 720×300)

Anatomy bar of one 2-second fetch plus two CPU-utilization comparison bars.

- **Title (bold 15px, `#1a5276`):** "Inside One 2-Second Fetch: Where the Time Actually Goes".
- **Anatomy bar (y=80, 44px tall):** x maps 0–2000 ms (left padding 60px, right 40px); whole bar filled light grid gray `#e5e9ef` with muted outline; two thin blue (`#2a78d6`) slivers at the start and end (~5 ms each, min 3px wide). Labels: blue bold "send request (~5 ms)" above-left, blue bold "parse answer (~5 ms)" above-right, muted bold centered inside "waiting on the network: ~1,990 ms".
- **Axis (y=152):** ticks/labels at 0, 500, 1000, 1500, 2000 ms.
- **Utilization bars (starting y=208):** heading bold "CPU busy during the job:"; two rows, each a 220px gray track (`#e5e9ef`) with a colored fill proportional to percent (min 3px) and a bold label to the right:
  - "sequential (10 fetches)" — 0.5% fill in orange `#d95926`, label "~0.5% busy, 99.5% idle".
  - "async (10 fetches)" — 4.3% fill in green `#008300`, label "~4% busy — same work, 9x less idle time".
- **Caption (magenta bold 13px, bottom center):** "the machine was never the bottleneck — the waiting was".

## The Common Confusion: Async Doesn't Help CPU Work

**Tags:** `common mistake` (red), `trade-off` (orange)

- **CPU-bound job** — resize 10 images, each needing 2 s of actual computation
- **Async result** — still 20 s: one core can only compute one thing at a time
- **Why** — there is no waiting to overlap; the processor is already 100% busy
- **What does help** — more workers: 4 processes/cores finish the 10 images in ~6 s
- **The test** — ask "is it waiting or computing?" — async for waiting, parallelism for computing

*Example:* One cook can take 10 orders while dishes bake (async works), but chopping 10 salads takes 10 choppings no matter how the orders are arranged.

**Key point:** Async overlaps idle waiting; it cannot create computing power — for CPU-bound work you need more cores, not callbacks.

### Visualization (canvas `c4`, 720×300)

Two grouped bar charts (I/O-bound vs CPU-bound) separated by a dashed divider, sharing one baseline.

- **Title (bold 15px, `#1a5276`):** "Same Trick, Two Jobs: Async Only Fixes the Waiting Kind".
- **Scale:** baseline at y=232, chart height 156px, max value 22 s; bars 74px wide, 26px gap; baseline gray line from x=50 to x=670; vertical dashed divider (`#bdc3c7`, dash 4/3) at x=352.
- **Left group (x0=78), title "I/O-bound: fetch 10 pages":**
  - "sequential" bar: 20 s, orange `#d95926`, value label "20 s".
  - "async" bar: 2.3 s, green `#008300`, value label "2.3 s".
- **Right group (x0=400), title "CPU-bound: resize 10 images":**
  - "sequential" bar: 20 s, orange `#d95926`, "20 s".
  - "async" bar: 20 s, magenta `#d55181`, "20 s".
  - "4 cores" bar: 6 s, aqua `#199e70`, "~6 s".
- **Value labels** bold 13px in the bar's color above each bar; bar names 12px `#2c3e50` below the baseline.
- **Bottom annotations (bold 13px):** magenta "async gained nothing here — no waiting to overlap" centered at x=510; green "async wins: waits overlap" centered at x=190.

## Regeneration instructions

- **Template:** tutorials topic-page layout (simplest-form concept tutorial). Page: `<h1>` + `.subtitle`, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, bottom border 2px solid `#2980b9`) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%) holding one canvas.
- **Text column structure per section:** `.tags` row of colored pill spans (0.72rem, 600 weight, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (`li b` colored `#1a5276`); one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each declared 720×300 intrinsic; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Hardcode all data arrays (no `Math.random()`).
- **Chart palette (tutorials `P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette accents: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
