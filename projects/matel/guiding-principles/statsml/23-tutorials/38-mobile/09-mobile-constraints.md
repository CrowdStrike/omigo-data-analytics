# Mobile Constraints

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Mobile Constraints

**Subtitle:** A phone runs on a battery, a radio, and a network that comes and goes — so good mobile code does less, later, in batches

## The Loyalty App That Pings Every 10 Seconds

**Tags:** `core idea` (blue), `battery` (green), `do less, later` (orange)

- **The app** — a coffee shop's loyalty app keeps your points balance in sync with its server
- **The eager build** — version A pings the server every 10 seconds so points are always "fresh"
- **The patient build** — version B saves changes locally and syncs one batch every 5 minutes
- **Same job** — both show the right balance when you open the app; nobody sees a 10-second lag
- **The bill** — version A drains ~7% battery per hour; version B drains ~1.5% for the same job
- **Close of day** — after an 8-hour shift, phone A sits at 44%, phone B at 88%

*Example (italic):* Two baristas install the two builds at 9am; by 5pm one phone is at 44% and hunting for a charger, the other is at 88%.

**Key point:** On a phone the scarce resource is not CPU or bandwidth — it is battery, and after the screen the radio is the battery's biggest customer — and the one your sync code controls — so fewer, bigger, later requests win.

### Visualization (canvas `c1`, 720×300)

Line chart of battery percentage over an 8-hour workday: the every-10-seconds build vs the batch-every-5-minutes build.

- **Title (bold 15px, `#1a5276`, top center):** "Same App, Two Sync Policies: Battery Over an 8-Hour Shift".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hours since 9am, 0 to 8, 12px `#444` tick labels every 2 hours ("9am", "11am", "1pm", "3pm", "5pm"); y = battery % 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Ping line:** orange `#d95926` 3px line through hours `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, battery `[100, 93, 86, 79, 72, 65, 58, 51, 44]` — steady ~7%/hour drain.
- **Batch line:** green `#008300` 3px line through the same hour grid, battery `[100, 98, 97, 95, 94, 92, 91, 89, 88]` — steady ~1.5%/hour drain.
- **End labels (bold 12px, right of last point):** orange "ping every 10s — 44%"; green "batch every 5min — 88%".
- **Annotation (bold 13px green `#008300`, near hour 4, y=60):** "same points balance, double the battery at closing time".
- **Caption (12px `#444`, bottom right):** "drain rates illustrative".

## Counting Radio Seconds: 300 vs 12

**Tags:** `worked example` (blue), `radio tail` (orange)

- **The transmit** — sending one tiny points update takes about 2 seconds of radio time
- **The tail** — the cellular radio stays in its high-power state ~10 more seconds after each request
- **True cost** — one request = 2 s send + 10 s tail = 12 s of radio-on, even for a 1 KB payload
- **Version A** — a ping every 10 s lands inside the previous tail: the radio never sleeps, 300 s on per 5 min
- **Version B** — one batch per 5 min costs a single 12 s burst: 300 s vs 12 s, 25× less radio-on
- **Per hour** — 3,600 radio-seconds for A vs 144 for B, delivering exactly the same data

*Example (italic):* Version A's 30 pings in 5 minutes deliver the same 30 updates as version B's single batch — at 25× the radio-on time.

**Key point:** A mobile request's cost is dominated by the radio tail, not the payload — so the win comes from sending fewer times, not from sending less.

### Visualization (canvas `c2`, 720×300)

Two-row radio-state timeline over one 5-minute window: the ping build holds the radio high the whole time; the batch build wakes it once.

- **Title (bold 15px, `#1a5276`, top center):** "One 5-Minute Window: When Is the Radio Awake?".
- **Time axis:** x=60 to x=660 maps 0 to 300 seconds (2 px per second); 2px `#999` baseline at y=245 with 12px `#444` tick labels at 0s / 60s / 120s / 180s / 240s / 300s.
- **Row 1 (bar top y=95, height 26), 12px `#444` label "ping every 10s" at x=20 above the bar:** one solid orange `#d95926` bar from x=60 to x=660 (radio high for all 300 s); thin 1px `#1a5276` tick marks every 20 px along its top marking each of the 30 pings; bold 12px orange label "radio-on: 300 s" right of the bar end.
- **Row 2 (bar top y=185, height 26), label "batch every 5min":** one solid green `#008300` bar from x=60 width 24 px (the 12 s burst: 4 px send + 20 px tail, tail half drawn `rgba(0,131,0,0.35)`); a 2px `#e5e9ef` "radio asleep" line continuing to x=660; bold 12px green label "radio-on: 12 s" above x≈120.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "300 s vs 12 s of radio-on for the same 30 updates — 25× less".
- **Caption (12px `#444`, bottom right):** "2 s send + 10 s tail per request, timings illustrative".

## Why the OS Makes Your App Wait

**Tags:** `where it's used` (blue), `batching` (green), `coalescing` (orange)

- **Deferral** — mobile operating systems hold background work and release it in shared windows
- **The reason** — if every installed app pinged on its own clock, every phone would die by lunch
- **Analytics batching** — analytics libraries queue events on disk and flush dozens in one request
- **Piggybacking** — a pending low-priority sync fires when a user tap has the radio up anyway
- **The count** — the loyalty app's 42 separate wake-ups per hour become 4 shared windows

*Example (italic):* Per hour the app fires 20 analytics events, 12 point syncs, 6 log uploads, and 4 prefetches — coalesced, all 42 ride 4 shared radio windows.

**Key point:** The platform's background machinery exists to enforce one rule — do less, later, in batches — because deferred, coalesced work costs a fraction of the radio wake-ups.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: radio wake-ups per hour for each subsystem firing alone, their total, and the coalesced alternative.

- **Title (bold 15px, `#1a5276`, top center):** "Radio Wake-Ups per Hour: Solo Requests vs Coalesced Windows".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, scale 10 px per wake-up (max width 420); left-aligned 12px `#444` row labels at x=20.
- **Rows (bar height 14px, top to bottom at y = 70, 100, 130, 160, 200, 240):**
  - "analytics events — 20": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 200
  - "point syncs — 12": blue fill, width 120
  - "log uploads — 6": blue fill, width 60
  - "image prefetch — 4": blue fill, width 40
  - "total, each firing solo — 42": solid orange `#d95926`, width 420
  - "coalesced windows — 4": solid green `#008300`, width 40
- **Width labels:** 11px `#444` count at each bar's right end.
- **Annotation (bold 13px magenta `#d55181`, right side near y=265):** "same work delivered, one-tenth the wake-ups".
- **Caption (12px `#444`, bottom right):** "per-hour counts illustrative".

## A Phone Is Not a Small Server

**Tags:** `common mistake` (red), `flaky network` (orange)

- **The habit** — server code assumes the network is always there and a request is nearly free
- **Flaky reality** — a phone rides elevators and subway tunnels; connections drop mid-request routinely
- **Retry storms** — retrying a dead link immediately burns 12-second radio wakes on guaranteed failures
- **The queue** — mobile code writes the event to disk and syncs later, when the radio is up anyway
- **The flip** — the cheapest request is the one you defer into someone else's radio window

*Example (italic):* In a 3-minute subway ride, the eager build attempts 18 doomed pings with instant retries; the patient build writes to its queue and sends one batch at street level.

**Common mistake:** Treating a phone like a server with a small screen. A server pays for a request in milliseconds of CPU; a phone pays in 12 seconds of radio, a slice of battery, and a network that may not even be there.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: server-style thinking on a flaky network (retry storm) vs mobile-style thinking (queue, then one batch).

- **Title (bold 15px, `#1a5276`, top center):** "Signal Drops Mid-Sync: Two Ways to React".
- **Row 1 (boxes centered y=95), 12px `#444` label "server thinking" at x=20:** blue `#2a78d6` rounded box at x=150 labeled "send now" (12px), 3px arrow to an orange `#d95926` box at x=330 labeled "no signal — retry now", 3px arrow to a red `#e74c3c` box at x=520 labeled "18 doomed retries" with bold 12px red "✗ battery gone, nothing sent".
- **Row 2 (boxes centered y=205), label "mobile thinking":** blue box at x=150 labeled "write event to queue", 3px arrow to a green `#008300` box at x=330 labeled "wait for radio window", arrow to a green box at x=520 labeled "one batch on signal" with bold 12px green "✓ all events delivered".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "on mobile, patience is the optimization".
- **Caption (12px `#444`, bottom right):** "retry counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); battery curves (7%/h vs 1.5%/h, ending 44% vs 88%), wake-up counts (20/12/6/4 → 42 vs 4), and retry counts are invented and labeled illustrative; the radio math is internally exact given the stated 2 s send + 10 s tail: 12 s per request, 300 s vs 12 s per 5-minute window (25×), 3,600 s vs 144 s per hour.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
