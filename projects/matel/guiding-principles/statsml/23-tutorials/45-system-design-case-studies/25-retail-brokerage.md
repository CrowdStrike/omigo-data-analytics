# Retail Brokerage

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Retail Brokerage

**Subtitle:** The viral-stock stress test — when a stock goes viral, millions of users do the same thing in the same minute, and a retail trading mania shows what that does to a brokerage

## Everyone Buys the Same Stock in the Same Minute

**Tags:** `core idea` (blue), `thundering herd` (orange), `correlated traffic` (red)

- **The daily herd** — market open is a built-in thundering herd: the 9:30 bell is everyone's alarm clock
- **The viral multiplier** — in January 2021 a few meme stocks sent millions of retail buyers in at once
- **Correlated, not random** — same symbol, same minutes, same action: no average-load model predicts it
- **One hot key** — normally the busiest ticker is ~5% of order flow; on a meme morning one ticker is ~60%
- **The public record** — multiple retail brokers reported outages during the 2020–21 volatility spikes

*Example (italic):* On the illustrative meme morning, orders hit 15,000/s at 9:30 — 7.5× the normal open peak of 2,000/s and 50× the midday baseline of 300/s.

**Key point:** Retail trading load is correlated by design — the opening bell synchronizes everyone every day, and a viral stock multiplies that herd; capacity planned from average or even normal-peak load misses it entirely.

### Visualization (canvas `c1`, 720×300)

Line chart of order arrivals per second around market open: a normal day's open spike vs the meme morning's wall, on a shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "9:30 Bell: Normal Open 2,000/s, Meme Morning 15,000/s".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time 9:00 to 11:00 with 12px `#444` tick labels at "9:00", "9:30", "10:00", "10:30", "11:00"; y = orders/sec 0 to 15,000, gridlines `#e5e9ef` at 5,000 / 10,000 with 12px `#444` labels.
- **Normal-day line:** blue `#2a78d6` 3px line through clock times `[9:00, 9:15, 9:29, 9:30, 9:31, 9:33, 9:45, 10:15, 11:00]`, orders/sec `[250, 300, 350, 2000, 1800, 1200, 700, 450, 300]` — small step at the bell, quick decay; 12px blue label "normal day" near its peak.
- **Meme-day line:** red `#e74c3c` 3px line through clock times `[9:00, 9:15, 9:29, 9:30, 9:32, 9:33, 9:34, 9:45, 10:15, 11:00]`, orders/sec `[900, 1500, 2500, 15000, 15000, 15000, 3000, 3000, 1500, 800]` — vertical wall at the bell, 3-minute plateau, sharp fall; 12px red label "meme morning" near its plateau.
- **Bell marker:** vertical dashed `#6b7280` (dash 4/3) line at 9:30, 12px `#6b7280` label "open" at its top.
- **Annotation (bold 13px red `#e74c3c`, near 10:00, y=90):** "same symbol, same minutes: 7.5× the normal open peak".
- **Caption (12px `#444`, bottom right):** "order rates illustrative".

## The 9:30 Backlog: Queue Math at the Open

**Tags:** `worked example` (blue), `backpressure` (green), `queue math` (orange)

- **The pipeline** — order validation, risk checks, and clearing hand-off sustain 5,000 orders/s (illustrative)
- **The open** — meme-morning arrivals run at 15,000/s for the first 3 minutes of trading
- **The backlog** — the queue grows at 15,000 − 5,000 = 10,000/s; after 180s that is 1.8M queued orders
- **The drain** — arrivals fall to 3,000/s at 9:33; the net drain of 2,000/s clears 1.8M in 900s (15 min)
- **Hand-check** — the last order queued at 9:33 waits 1,800,000 / 5,000 = 360s before it is even processed

*Example (italic):* At 9:33 the backlog peaks at 1.8 million orders; a market order at the back of that queue executes six minutes late, at whatever the price is by then.

**Key point:** A queue doesn't solve overload — it converts it into delay, and a 6-minute-old market order in a fast market is worse than an honest rejection; bound the queue and push back at the front door instead.

### Visualization (canvas `c2`, 720×300)

Two-line chart on a shared time axis: order arrivals vs flat pipeline capacity, with the queue backlog drawn as an orange area between the growth and drain phases.

- **Title (bold 15px, `#1a5276`, top center):** "15,000/s In, 5,000/s Through: a 1.8M-Order Backlog by 9:33".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time 9:28 to 9:50 with 12px `#444` tick labels at "9:28", "9:30", "9:33", "9:40", "9:48"; y = orders/sec 0 to 15,000, gridlines `#e5e9ef` at 5,000 / 10,000 with 12px `#444` labels.
- **Arrivals line:** red `#e74c3c` 3px line through clock times `[9:28, 9:30, 9:31, 9:32, 9:33, 9:33, 9:35, 9:40, 9:48]`, orders/sec `[400, 15000, 15000, 15000, 15000, 3000, 3000, 3000, 2000]` (a vertical step down at 9:33); 12px red label "arrivals" near its plateau.
- **Capacity line:** dashed green `#008300` (dash 6/4) 2px horizontal line at 5,000 orders/sec across the plot, 12px green label "pipeline capacity 5,000/s" above its right end.
- **Backlog area:** orange fill `rgba(217,89,38,0.25)` between the arrivals line and the capacity line from 9:30 to 9:33 (growth), and between the capacity line and the arrivals line from 9:33 to 9:48 (drain), with bold 12px `#d95926` labels "backlog grows 10,000/s" at (≈9:31, y≈95) and "drains 2,000/s for 15 min" at (≈9:40, y≈200).
- **Peak marker:** vertical dashed `#6b7280` (dash 4/3) line at 9:33, 12px `#6b7280` label "peak backlog: 1.8M orders" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, near 9:41, y=60):** "an order queued at 9:33 waits 6 minutes".
- **Caption (12px `#444`, bottom right):** "rates and capacity illustrative".

## The Throttle That Came From the Clearinghouse

**Tags:** `where it's used` (blue), `regulatory constraint` (red), `graceful degradation` (green)

- **The surprise** — the restriction that made headlines came from capital requirements, not from servers
- **Clearing** — trades settled days after execution (T+2 then), so the clearinghouse holds a risk deposit
- **The deposit** — it scales with volatility and concentration; the meme week multiplied it ~10× overnight
- **Product throttle** — brokers publicly restricted buys on the hot symbols; server capacity was irrelevant
- **Degradation order** — quotes may go stale and pages may slow, but an order acknowledgment must never lie

*Example (italic):* A broker whose clearing deposit requirement jumps ~10× overnight (as publicly reported in the January 2021 episode) may have to restrict buying on specific symbols even with idle servers.

**Key point:** Some overload valves are regulatory and financial, not technical — a brokerage needs pre-built product-level throttles (per-symbol, buy-side-only, position-close-only) because the binding constraint can arrive from outside the datacenter.

### Visualization (canvas `c3`, 720×300)

Bar chart of the daily clearinghouse deposit requirement across the meme week, expressed as a multiple of the normal baseline, with the buy-restriction day marked.

- **Title (bold 15px, `#1a5276`, top center):** "The Clearing Deposit Jumps ~10× Overnight".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = five bars centered at x = 130, 240, 350, 460, 570 with 12px `#444` labels "Mon", "Tue", "Wed", "Thu", "Fri"; y = deposit as multiple of baseline 0 to 10×, gridlines `#e5e9ef` at 2.5× / 5× / 7.5× with 12px `#444` labels.
- **Bars:** 64px wide; Mon/Tue/Wed blue fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border at multiples `[1.0, 1.3, 2.4]`; Thu solid red `#e74c3c` at `10.0`; Fri orange `#d95926` at `4.0`; 12px `#444` value labels on top ("1.0×", "1.3×", "2.4×", "10×", "4.0×").
- **Restriction marker:** bold 12px red `#e74c3c` label "buys restricted on hot symbols" with a short 2px red pointer line to the top of the Thu bar.
- **Annotation (bold 13px `#1a5276`, near x=150, y=70):** "the throttle came from capital, not capacity".
- **Caption (12px `#444`, bottom right):** "multiples illustrative; ~10× overnight jump as publicly reported".

## Panicked Users Retry — Orders Must Not Multiply

**Tags:** `common mistake` (red), `idempotency` (green)

- **The retry storm** — a user whose buy times out does not wait; they tap the button again within seconds
- **The worst case** — the spinner timed out after the order reached the exchange, so the retry doubles it
- **The fix** — the client generates one order ID per intent; the gateway treats every repeat as the same order
- **Backpressure** — reject new orders at the front door with an honest "try again" before the pipeline chokes
- **Ack honesty** — "accepted", "queued", and "rejected" must each be true; never fake success or failure

*Example (italic):* A user taps buy three times during a 9:31 timeout; without an idempotency key that is three filled orders and 3× the intended position — bought at the top of a spike.

**Common mistake:** Treating each retry as a new order. Panic and timeouts peak at exactly the same moment, so duplicates multiply precisely when prices move fastest — order submission must be idempotent end to end.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: three retried taps without an idempotency key producing three fills, vs the same three taps with a client order ID producing one fill.

- **Title (bold 15px, `#1a5276`, top center):** "Three Taps, One Intent: the Idempotency Key".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no key"; blue `#2a78d6` rounded box at x=160 labeled "tap ×3 (timeouts)" (12px), three thin 2px arrows fanning to a red `#e74c3c` box at x=400 labeled "gateway: 3 new orders", 3px arrow to a red box at x=580 labeled "3 fills" with bold 12px red "✗ 3× the position".
- **Row 2 (y=205), label:** "client order ID"; blue box at x=160 labeled "tap ×3, same ID", three arrows converging on a green `#008300` box at x=400 labeled "gateway dedupes: 1 order", arrow to a green box at x=580 labeled "1 fill" with bold 12px green "✓ retries return the same ack".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "timeouts and panic peak together — retries must be free".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); order rates, pipeline capacity (5,000/s), backlog math (1.8M, 360s, 900s), and deposit multiples are invented and labeled illustrative; the only claims kept factual are publicly reported ones — retail-broker outages during 2020–21 volatility, buy restrictions on hot symbols, and the roughly 10× overnight clearing-deposit jump discussed publicly during the January 2021 episode — no claims about any broker's undisclosed internals.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
