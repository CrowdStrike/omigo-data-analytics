# Seasonality

**Page type:** detail page (tutorial card-sections: h2 per section, two-column layout table — text left 50% with tag pills/bullets/example/key-point, canvas right 50%)
**HTML title tag:** Seasonality

**Subtitle:** The part of a series that repeats on a calendar beat — weekend peaks every week, a December surge every year

## The Weekend Heartbeat

**Tags:** `core idea` (blue), `running example` (green)

- **Look at any week** — quiet Monday to Thursday, a Friday lift, then Saturday and Sunday tower
- **Look at the next week** — the same shape again, and again: it repeats every 7 days
- **That repetition is seasonality** — any pattern that returns on a fixed calendar beat
- **It has a cause that repeats** — weekends are busy because people are off work every week
- **Beats can stack** — this shop has a weekly rhythm and a yearly one (the December rush)

*Example:* Sunday hit $308 while Tuesday managed $224 — and the following Sunday towered over its week again.

**Key point:** Seasonality is the part of the line you can predict with a calendar alone — before knowing anything else about the business.

### Visualization (canvas `c1`, 720×300)

Line chart: four Monday-start weeks (28 days) of daily coffee-shop sales, weekends shaded.

- **Title (bold 15px, `#1a5276`, top center):** "Four weeks of daily sales — the same weekly shape repeats"
- **Data (28 daily totals, Mon..Sun per week):** week 1 `[233,224,254,241,250,277,308]`, week 2 `[235,230,235,257,253,277,295]`, week 3 `[248,237,228,251,260,289,281]`, week 4 `[250,241,243,232,268,291,295]`
- **Axes:** y from 200 to 325 with gridlines/labels at $225, $275, $325 (labels right-aligned gray `#6b7280`, gridlines `#e5e9ef`); L-shaped axis frame in `#999`; padding l:58 r:20 t:46 b:56; x positions center each of 28 days in its slot
- **Weekend shading:** for each week, Sat+Sun columns filled `rgba(0,131,0,0.10)` full plot height
- **Series:** connected line `#2a78d6` width 2, dots at each point — weekend dots green `#008300` radius 4.5, weekday dots blue `#2a78d6` radius 3
- **X labels:** day initials M T W T F S S (11px gray) under each point; below them "week 1"…"week 4" (12px gray) centered under each week
- **Annotation (bold 13px green `#008300`, left-aligned near top at y=$318):** "the peak lands on Sat + Sun, every single week"
- **Caption (12px `#6b7280`, bottom right):** "illustrative data"

## Average the Mondays, Average the Sundays

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **Grab 4 weeks** — 28 daily totals from January of year 2, no holidays in the way
- **Average each weekday** — the four Tuesdays: (224 + 230 + 237 + 241) ÷ 4 = $233
- **Line them up** — Mon 242, Tue 233, Wed 240, Thu 245, Fri 258, Sat 284, Sun 295
- **Read the beat** — weekdays hover in the 230s–250s; the weekend sits about $46 higher
- **Those 7 numbers** — the weekly seasonal profile, this shop's fingerprint

*Example:* Sunday averages $295 against Tuesday's $233 — a $62 gap that the calendar delivers every single week.

**Key point:** Averaging same weekdays across weeks cancels the day-to-day noise and leaves the seasonal profile standing.

### Visualization (canvas `c2`, 720×300)

Bar chart: day-of-week averages across the 4 weeks.

- **Title (bold 15px, `#1a5276`, top center):** "Average sales by day of week (4 weeks of data)"
- **Data (Mon..Sun averages, rounded):** `[242, 233, 240, 245, 258, 284, 295]`, labels `['Mon','Tue','Wed','Thu','Fri','Sat','Sun']`
- **Axes:** y from 0 to 330 with gridlines/labels at $0, $150, $300; L-shaped axis frame `#999`; padding l:58 r:20 t:50 b:44
- **Bars:** width 62% of a 1/7 slot; weekday bars `rgba(42,120,214,0.45)`, weekend bars (Sat, Sun) `rgba(0,131,0,0.55)`; bold 12px dollar value label above each bar (e.g. "$242"), gray day label below axis
- **Annotation (bold 13px green `#008300`, at ~42% width, y=$318):** "Sunday beats Tuesday by $62 — the calendar delivers it weekly"
- **Caption (12px `#6b7280`, bottom right):** "illustrative data"

## Compare Monday to Monday, Not Monday to Sunday

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Monday's till** — $235, right after Sunday's $308: "sales crashed 24%!"
- **Wrong yardstick** — Monday vs Sunday measures the weekly rhythm, not the business
- **Right yardstick** — Monday vs last Monday: 235 vs 233 = +$2, business as usual
- **The rule** — compare like with like: same day last week, same month last year
- **Dashboards too** — a "vs yesterday" tile will cry wolf every Monday morning, forever

*Example:* 235 vs 308 looks like a collapse; 235 vs 233 shows a normal, slightly better Monday.

**Key point:** A comparison across different points of a cycle measures the cycle, not the change. Always compare to the same point of the cycle.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: the same Monday judged two ways — two bar pairs side by side.

- **Title (bold 15px, `#1a5276`, top center):** "The same Monday ($235), judged two ways"
- **Axes:** y from 0 to 340 with gridlines/labels at $0, $150, $300; L-shaped axis frame `#999`; padding l:58 r:20 t:50 b:58
- **Left pair (centered at 26% width):** "Sun" $308 in gray `rgba(107,114,128,0.35)` next to "Mon" $235 in blue `rgba(42,120,214,0.55)`; bars 68px wide, bold value labels above, gray day labels below
- **Right pair (centered at 74% width):** "last Mon" $233 (gray) next to "Mon" $235 (blue)
- **Verdicts (bold 13px, centered at y=$325):** left in red `#e74c3c`: "vs yesterday: −$73 \"crash!\""; right in green `#008300`: "vs same day last week: +$2"
- **Captions (12px, below x labels):** left gray: "wrong yardstick — measures the weekly cycle"; right green: "right yardstick — measures the business"
- **Caption (12px `#6b7280`, bottom right):** "illustrative data"

## The December Surge That Looks Like Growth

**Tags:** `common mistake` (red), `where it's used` (orange)

- **December year 1** — the monthly average jumps from $258 to $298: "we're taking off!"
- **January** — back to $255; the "takeoff" left with the holiday decorations
- **The tell** — a similar Nov→Dec jump (about +$40) happens in both years
- **Real growth hides elsewhere** — Dec y1 $298 to Dec y2 $351 is +$53 year over year
- **The rule** — a spike that repeats on the calendar is season; year-over-year change is growth

*Example:* Nov→Dec adds about $40 in both years (season); Dec→Dec adds $53 (growth).

**Key point:** Before celebrating a surge, check last year's chart. If the same bump is there, the calendar did it — not the business.

### Visualization (canvas `c4`, 720×300)

Line chart: 24 monthly averages over two years, Decembers highlighted, Dec→Dec growth arrow.

- **Title (bold 15px, `#1a5276`, top center):** "December surge: season wearing a growth costume"
- **Data (24 monthly averages, Jan y1..Dec y2):** `[200,207,211,217,219,226,229,233,240,242,258,298, 255,261,268,270,274,282,282,289,294,296,315,351]`
- **Axes:** y from 180 to 385 with gridlines/labels at $200, $275, $350; L-shaped axis frame `#999`; padding l:58 r:20 t:46 b:46
- **Series:** line `rgba(42,120,214,0.5)` width 1.5; dots at each month — Decembers (indices 11 and 23) in yellow `#c98500` radius 6, all others blue `#2a78d6` radius 3
- **X labels:** month initials J F M A M J J A S O N D repeated twice (11px gray); "year 1" centered under months 1–12 and "year 2" under months 13–24
- **Season annotations (bold 12px yellow `#c98500`):** "Nov→Dec: +$40" near Dec y1, "Nov→Dec: +$36" near Dec y2; bold 13px yellow "same surge both years = season" at upper area (x≈month 9, y=$360)
- **Growth arrow:** dashed green `#008300` line (dash 5/4, width 2) from Dec y1 point ($298) to Dec y2 point ($351); bold 13px green label "Dec→Dec: +$53 = real growth" above it
- **Caption (12px `#6b7280`, bottom right):** "illustrative data"

## Regeneration instructions

- **Layout:** tutorial detail page — `<h1>` + `.subtitle`, then four `.card-section` blocks, each an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) followed by `table.layout` (full width, one row): left `td.text-col` 50% with `.tags` pill row, a `<ul>` of one-line bullets each opening with a `<b>` term (bold terms colored `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` 50% holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Charts:** shared JS palette `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; site palette #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange. Canvases declare intrinsic 720×300 and scale by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Data is hardcoded deterministic arrays (no `Math.random()`); same running coffee-shop series as the other time-series pages.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
