# Timezones & Timestamps

**Page type:** detail page (tutorial page: card-sections, each with a two-column layout table — text left 50% with tag pills / bullets / example / key-point, canvas right 50%)
**HTML title tag:** Timezones & Timestamps

**Subtitle:** "2024-03-10 02:30" — in whose clock? A timestamp without a timezone is a number without a unit: store the instant in UTC, translate to a wall clock only at display time

## The Order Placed at a Time That Doesn't Exist

Tags: `core idea` (blue), `running example` (green)

- **The record** — an order stamped "2024-03-10 02:30", with no timezone attached
- **The catch** — in New York that night, clocks jumped from 01:59 straight to 03:00
- **The gap** — 02:30 never appeared on any New York wall clock that day
- **The question** — the string cannot say whose clock it meant, so the instant is lost
- **The definition** — a timestamp without a timezone is a number without a unit

*Example:* The order parser crashed once a year, every March, on timestamps stamped inside the missing hour.

**Key point:** Wall-clock time is a display format, not a measurement. With DST, some local times happen twice in one night and some never happen at all.

### Visualization (canvas `c1`, 720×300)

Band diagram of the New York wall clock on the DST-spring-forward night, with the skipped hour highlighted.

- **Title (bold 16px, `#1a5276`, top center):** "New York Wall Clock, Night of 2024-03-10: an Hour Is Skipped".
- **Band:** horizontal strip 44px tall at y=130 spanning local 00:00→05:00 mapped to x=70..660; hour labels "00:00"…"05:00" (12px `#2c3e50`) below.
- **Segments:** 00:00–02:00 and 03:00–05:00 filled `rgba(42,120,214,0.18)` with 1.5px `#2a78d6` stroke and bold 12px blue in-band labels "clock exists"; 02:00–03:00 filled `rgba(231,76,60,0.15)` with 2px dashed (`#e74c3c`, dash 5/4) stroke and bold 12px red label "never happens".
- **Jump arrow:** orange (`#d95926`) 2.5px quadratic arc over the missing hour with an arrowhead, labeled bold 13px: "01:59 → 03:00: the clock jumps".
- **Orphan order:** red 6px dot below the band at the 02:30 position with a leader line into the band; bold 13px red label "order stamped \"2024-03-10 02:30\" points into the gap"; italic 12px `#6b7280` line below: "it cannot be a New York time — so whose clock stamped it?".

## One Instant, Four Clocks

Tags: `worked example` (green), `core idea` (blue)

- **One event** — a payment clears at the single instant 2024-03-10 07:30 UTC
- **New York** — reads 03:30, because it switched to UTC-4 earlier that morning
- **London** — reads 07:30, matching UTC — but only until its own switch three weeks later
- **Tokyo** — reads 16:30; UTC+9 all year, no DST at all
- **The lesson** — the instant is one fact; the wall-clock times are translations of it

*Example:* Support in Tokyo and the customer in New York argued about "when it happened" — both described the same instant.

**Key point:** Store the instant once, in UTC. Derive the wall-clock reading per viewer at display time — never the other way around.

### Visualization (canvas `c2`, 720×300)

Four horizontal 24-hour clock rows with one instant marked on each, tied by a dashed thread.

- **Title (bold 16px, `#1a5276`, top center):** "One Payment, One Instant — Four Wall-Clock Readings".
- **Rows** (thick 6px `#e5e9ef` line from x=150 to x=560 representing 0–24h; row name bold 13px in row color at left; 7px filled dot at the local hour with bold 13px reading and 12px `#6b7280` note beside it; offset text 12px `#6b7280` at x=575):
  - "UTC" (ink `#1a5276`), offset "±0", marker at 07:30 reading "07:30", note "the stored truth"
  - "New York" (blue `#2a78d6`), offset "UTC-4 (DST on)", marker at 03:30 reading "03:30", note "switched that morning"
  - "London" (aqua `#199e70`), offset "UTC+0 (winter)", marker at 07:30 reading "07:30", note "matches UTC until Mar 31"
  - "Tokyo" (violet `#4a3aa7`), offset "UTC+9 (no DST)", marker at 16:30 reading "16:30", note "same offset all year"
- **Thread:** dashed orange (`#d95926`, dash 4/4, 1.5px) polyline connecting the four markers.
- **Annotations:** bold 13px orange centered at y=262: "the dashed thread is ONE instant: 2024-03-10 07:30 UTC"; 12px `#6b7280` at y=282: "each row: that instant placed on a 24-hour local wall clock".

## The Mysterious Dip in Daily Revenue

Tags: `where it's used` (blue), `common mistake` (red)

- **The shop** — sells a steady $100 every hour, around the clock
- **Most days** — 24 hours × $100 = $2,400, and the daily chart is flat
- **March 10** — the local day has only 23 hours, so the bar reads $2,300
- **The panic** — a 4% overnight drop looks exactly like a real business problem
- **The mirror image** — November's 25-hour day shows a fake 4% spike

*Example:* The team spent a morning hunting a conversion bug; the answer was that the day itself was one hour short.

**Key point:** Before explaining a dip with a story about customers, check whether the day itself was a different length.

This section's viz cell holds both canvases side by side in a `.viz-pair` flex row.

### Visualization (canvas `c3a`, 310×300)

Bar chart of daily revenue with a single DST dip.

- **Title (bold 15px, `#1a5276`, top center):** "Daily Revenue (local days)".
- **Data:** days ["Mar 7", "Mar 8", "Mar 9", "Mar 10", "Mar 11", "Mar 12", "Mar 13"], values `[2400, 2400, 2400, 2300, 2400, 2400, 2400]`.
- **Axes:** y range 2,200–2,450 (truncated axis) with labels "$2,200" and "$2,400" (12px `#6b7280`); L-shaped `#6b7280` axis; day labels 12px, rotated ~-0.5 rad.
- **Bars:** 26px wide; normal bars fill `rgba(42,120,214,0.45)` with `#2a78d6` stroke; the Mar 10 dip bar red `#e74c3c` at 60% alpha with red stroke and bold red value label; values printed above every bar.
- **Annotations:** bold 13px red on two lines at y=44/59: "-$100: a 23-hour day," / "not a sales problem"; italic 12px `#6b7280` bottom center: "y-axis starts at $2,200".

### Visualization (canvas `c3b`, 310×300)

Hourly bar chart for Mar 10 with the missing 02:00 bar highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Mar 10, Hour by Hour".
- **Data:** 24 hour slots, each a uniform bar (fill `rgba(25,158,112,0.5)`) at the $100 level — except hour 02, which has no bar. The empty 02:00 slot is shaded `rgba(231,76,60,0.12)` full-height with a dashed red (`#e74c3c`, dash 4/3) outline.
- **Axes:** y labels "$100" and "$0" (12px `#6b7280`); x tick labels "0h", "6h", "12h", "18h", "23h" (12px `#2c3e50`).
- **Callout:** short red leader stub above the gap; bold 12px red text centered on two lines: "02:00 never happened —" / "23 bars × $100 = $2,300".
- **Caption (italic 12px `#6b7280`, bottom center):** "steady $100/hour, local hours".

## Whose Midnight? The Day-Boundary Trap

Tags: `rule of thumb` (orange), `common mistake` (red)

- **The order** — placed Saturday 23:00 in New York, stored correctly as Sunday 04:00 UTC
- **UTC grouping** — GROUP BY the UTC date puts it in Sunday's revenue
- **The customer's truth** — they bought on Saturday night; Saturday's report misses it
- **The rule** — store UTC, but group by the day boundary your question is about
- **The trap** — mixing UTC-day and local-day charts shifts every late-evening sale

*Example:* US evening sales — the busiest hours — all landed on the "wrong" day in the UTC report.

**Key point:** "Which day was this sale?" is a question about a timezone. Same stored instants, different midnights, different daily numbers.

### Visualization (canvas `c4`, 720×300)

Two day-boundary ribbons crossed by one order instant.

- **Title (bold 16px, `#1a5276`, top center):** "Same Stored Instant, Two Day Boundaries, Two Answers".
- **Axis:** UTC hours from Sat 12:00 UTC to Sun 12:00 UTC mapped to x=90..660.
- **Ribbons** (26px-thick `#e5e9ef` horizontal band; a 2.5px colored vertical tick at the ribbon's midnight labeled bold 12px "midnight"; bold 13px day names "Saturday" / "Sunday" centered in each half; row label bold 13px `#2c3e50` at left):
  - y=105, "UTC days", midnight at Sun 00:00 UTC (t=12), color violet `#4a3aa7`
  - y=185, "New York days", midnight at 05:00 UTC (t=17), color aqua `#199e70`
- **Order line:** vertical 2.5px orange (`#d95926`) line at Sun 04:00 UTC (t=16) crossing both ribbons, with 6px orange dots at each intersection; bold 13px orange label above: "order: Sat 23:00 NY = Sun 04:00 UTC". Note the order falls after UTC midnight but before New York midnight.
- **Verdicts (bold 12px, y=245):** violet at left: "UTC grouping: counted as SUNDAY"; aqua at right: "New York grouping: counted as SATURDAY".
- **Bottom annotation (bold 13px `#1a5276`, centered, y=280):** "store UTC, then pick the midnight your question means — and say which one you picked".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + a layout table. Every section uses `table.layout` (`td.text-col` 50% / `td.viz-col` 50%); section 3 places canvases `c3a`/`c3b` (310×300 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`). Text cell order: `.tags` pill row, `<ul>` bullets (each starting with `<b>bold term</b>` in `#1a5276`), italic `.example`, `.key-point` callout.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = bg `rgba(26,82,118,0.12)` / `#1a5276`, green = `rgba(39,174,96,0.15)` / `#27ae60`, red = `rgba(231,76,60,0.12)` / `#e74c3c`, orange = `rgba(230,126,34,0.15)` / `#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** intrinsic width/height read from attributes; shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`, red `#e74c3c`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
