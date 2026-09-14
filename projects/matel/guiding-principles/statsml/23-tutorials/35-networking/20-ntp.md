# NTP

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** NTP

**Subtitle:** Every computer clock quietly drifts — NTP is how machines agree what time it is, by repeatedly asking a reference clock how wrong they are and gently nudging themselves back

## The Coffee Shop Whose Registers Disagree

**Tags:** `core idea` (blue), `clock drift` (orange), `time server` (green)

- **Two registers** — a coffee shop's front register and drive-through register each keep their own clock
- **The drift** — cheap quartz clocks wander; the drive-through unit gains about 50 ms every hour
- **The referee** — both registers ask the same internet time server "what time is it right now?"
- **The nudge** — each answer tells a register how far off it is, and it gently corrects itself
- **The name** — this ask-compare-correct loop is the Network Time Protocol (NTP), running since 1985

*Example (italic):* Left alone for a 12-hour day, the drive-through clock ends 600 ms ahead of the front register; asking the time server once an hour keeps it within 50 ms.

**Key point:** No computer clock is trusted to stay right on its own — NTP keeps machines agreeing by repeatedly measuring each clock's error against a reference and steering it back to zero.

### Visualization (canvas `c1`, 720×300)

Line chart of one register's clock error over a 12-hour day: unchecked drift (climbing line) vs hourly NTP corrections (sawtooth hugging zero), on a shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "One Day of Drift: 600 ms Adrift Alone, Under 50 ms With NTP".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hours since opening 0 to 12 (50px/hour), 12px `#444` tick labels every 2 hours ("0h"…"12h"); y = clock error 0 to 600 ms, gridlines `#e5e9ef` at 150/300/450 with 12px `#444` labels.
- **No-NTP line:** red `#e74c3c` 3px line through hours `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]`, error `[0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600]` — a straight climb.
- **NTP sawtooth:** green `#008300` 2px line: for each hour h in 0..11, a segment from (h, 0) to (h+1, 50), then a vertical drop back to (h+1, 0) — twelve identical teeth.
- **Sync markers:** small green dots at each (h, 0) drop point, one 12px `#6b7280` label "hourly sync" near the third tooth.
- **Annotation (bold 13px green `#008300`, near x=6h, y=110):** "the sync caps the error at 50 ms, forever".
- **Caption (12px `#444`, bottom right):** "drift rate illustrative".

## Four Timestamps Find the Offset

**Tags:** `worked example` (blue), `four timestamps` (green), `offset & delay` (orange)

- **Four stamps** — the register sends at t1=100, server receives t2=160, replies t3=162, register receives t4=122
- **Offset** — ((t2−t1)+(t3−t4))/2 = (60+40)/2 = +50 ms: the register's clock is 50 ms behind
- **Delay** — (t4−t1)−(t3−t2) = 22−2 = 20 ms of round-trip network travel
- **The trick** — travel time cancels out of the offset as long as both directions take about the same time
- **The nudge** — the register slews its clock gently forward by 50 ms rather than jumping at once

*Example (italic):* All four numbers are milliseconds after 2:00:00 — each side just writes down when it sent and when it received, and two subtractions recover the 50 ms gap.

**Key point:** One request-reply carrying four timestamps is enough to estimate both the clock offset and the network delay — the whole protocol rests on this small piece of arithmetic.

### Visualization (canvas `c2`, 720×300)

Two-timeline exchange diagram: the register's clock line on top, the time server's below, with the request and reply arrows carrying t1–t4 and the offset arithmetic spelled out.

- **Title (bold 15px, `#1a5276`, top center):** "One NTP Exchange: Four Timestamps, One Offset".
- **Timelines:** register (client) line 2px `#2a78d6` at y=110 from x=80 to x=660 with 12px `#2a78d6` label "register clock" at (80, 90); server line 2px `#008300` at y=210 from x=80 to x=660 with 12px `#008300` label "time server clock" at (80, 232).
- **Points (6px dots on their lines, each with a bold 12px label):** t1=100 at (160, 110) label above; t2=160 at (300, 210) label below; t3=162 at (360, 210) label below; t4=122 at (500, 110) label above.
- **Arrows:** blue `#2a78d6` 2px arrow with arrowhead from (160, 110) to (300, 210) labeled "request" (12px `#6b7280` mid-arrow); green `#008300` 2px arrow from (360, 210) to (500, 110) labeled "reply".
- **Delay note (12px `#6b7280`, at x=360, y=270):** "delay = (122−100)−(162−160) = 20 ms round trip".
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=360, y=45):** "offset = ((160−100)+(162−122))/2 = +50 ms — the register is 50 ms behind".
- **Caption (12px `#444`, bottom right):** "timestamps in ms after 2:00:00, illustrative".

## When the Payment Beats the Order

**Tags:** `where it's used` (blue), `log ordering` (green), `distributed data` (orange)

- **Timestamp joins** — the shop matches orders to payments by time; drifting clocks scramble the pairing
- **Log ordering** — debugging across machines means merging logs by time; skew reorders cause and effect
- **Distributed systems** — leases, cache expiry, and certificate checks all quietly assume agreeing clocks
- **Stratum** — atomic/GPS clocks are stratum 0; stratum 1 servers read them; stratum 2 asks stratum 1
- **The scale** — a 200 ms error is invisible to a human and catastrophic to a millisecond-ordered log

*Example (italic):* The payment terminal runs 200 ms slow, so a payment made at 2:00:00.150 is logged at 1:59:59.950 — 50 ms before the 2:00:00.000 order it paid for.

**Key point:** Any analysis that joins or orders events across machines by timestamp — sessionization, funnels, log merges — inherits every clock error in the fleet; NTP is what makes those joins mostly trustworthy.

### Visualization (canvas `c3`, 720×300)

Before/after ordering diagram: the true sequence of two events on a top axis, their logged timestamps on a bottom axis, with the payment's mapping line crossing the order's.

- **Title (bold 15px, `#1a5276`, top center):** "A 200 ms Slow Clock Logs the Payment Before the Order".
- **Axes:** two horizontal 2px `#999` time axes from x=80 to x=660, "what happened" at y=100 and "what the logs say" at y=210 (12px `#444` labels at x=20); both span 1:59:59.900 to 2:00:00.300 (1.45 px per ms), 12px `#444` tick labels at 59:59.900 / 00:00.000 / 00:00.100 / 00:00.200 / 00:00.300.
- **True events (top axis):** blue `#2a78d6` 7px dot "order 2:00:00.000" at x=225; green `#008300` 7px dot "payment 2:00:00.150" at x=443 — payment clearly second.
- **Logged events (bottom axis):** blue dot "order 2:00:00.000" at x=225 (register clock is correct); green dot "payment 1:59:59.950" at x=153 — payment now first.
- **Mapping lines:** dashed `#6b7280` (dash 4/3) line from each top dot to its logged bottom dot; the payment's line slants left and crosses the order's vertical line.
- **Annotation (bold 13px red `#e74c3c`, centered near x=370, y=255):** "logged order is backwards — the effect precedes its cause by 50 ms".
- **Caption (12px `#444`, bottom right):** "clock error illustrative".

## Synced Doesn't Mean Identical

**Tags:** `common mistake` (red), `residual skew` (orange)

- **The belief** — "both machines run NTP, so their timestamps are directly comparable"
- **The residue** — good internet NTP still leaves each machine a few ms off, in either direction
- **The flip** — machines off by +5 and −4 ms disagree on the order of any events under 9 ms apart
- **The step** — a badly wrong clock gets stepped, so time can jump backward and durations go negative
- **The fix** — order same-machine events by clock; order cross-machine events by sequence IDs or causality

*Example (italic):* Two synced servers log the same request 4 ms apart in opposite directions — replaying the merged log runs half the conversation in reverse.

**Common mistake:** Treating NTP-synced timestamps as exact. NTP bounds the error at a few milliseconds, it does not erase it — cross-machine event order below that bound is a coin flip, so use counters or causal IDs when order truly matters.

### Visualization (canvas `c4`, 720×300)

Diverging horizontal bar chart: residual clock offsets of five NTP-synced machines around a zero reference line, showing they still straddle a few milliseconds.

- **Title (bold 15px, `#1a5276`, top center):** "Even Synced Machines Sit a Few Milliseconds Apart".
- **Axis:** vertical 2px `#999` zero line at x=360 from y=55 to y=250, 12px `#6b7280` label "reference time" at its top; no other gridlines.
- **Rows (y = 80, 120, 160, 200, 240, bars 14px tall, 30px per ms):** machines "machine 1"…"machine 5" (12px `#444` labels at x=20) with offsets `[+3, -2, +5, -4, +1]` ms — positive bars extend right in blue fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge (widths 90 / 150 / 30), negative bars extend left in orange fill `rgba(217,89,38,0.25)` with 2px `#d95926` edge (widths 60 / 120); 12px labels "+3 ms" / "−2 ms" / "+5 ms" / "−4 ms" / "+1 ms" at bar ends.
- **Annotation (bold 13px red `#e74c3c`, right side near x=430, y=180):** "machines 3 and 4 disagree by 9 ms — event order below that is a coin flip".
- **Caption (12px `#444`, bottom right):** "offsets illustrative — typical of internet NTP".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the drift line (50 ms/hour, 600 ms/day), the four timestamps (t1=100, t2=160, t3=162, t4=122 giving offset +50 ms and delay 20 ms), the 200 ms slow payment terminal, and the five residual offsets `[+3, -2, +5, -4, +1]` ms are invented and labeled illustrative; the offset/delay formulas and the stratum hierarchy (stratum 0 reference clocks, stratum 1 directly attached, stratum 2 downstream) are real NTP facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
