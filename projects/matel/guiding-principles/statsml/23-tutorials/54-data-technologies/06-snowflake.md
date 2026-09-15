# Snowflake

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Snowflake

**Subtitle:** Snowflake stores the data once in cheap cloud storage and lets every team rent its own compute engine on top — so nobody queues behind anybody else

## One Copy of the Data, Many Engines on Top

**Tags:** `core idea` (blue), `storage/compute split` (green), `cloud warehouse` (orange)

- **The old box** — a classic warehouse is one machine holding both the disks and the CPUs
- **The queue** — when analytics runs a heavy query, finance's month-end job waits behind it
- **The split** — Snowflake keeps all tables once in cloud object storage, compressed and shared
- **The engines** — each team spins up its own "virtual warehouse", a compute cluster it alone uses
- **No contention** — both teams scan the same orders table at 9am; neither slows the other down

*Example (italic):* Analytics hammers dashboards on ANALYTICS_WH while finance closes the books on FINANCE_WH — same 2 TB orders table, zero shared CPUs.

**Key point:** Storage and compute are separate products: data lives once in cheap object storage, and any number of independent compute engines read it without stepping on each other.

### Visualization (canvas `c1`, 720×300)

Layered architecture diagram: one shared storage box at the bottom, three virtual warehouse boxes above it (two running, one suspended), arrows pointing down to the storage.

- **Title (bold 15px, `#1a5276`, top center):** "One Storage Layer, Independent Compute Per Team".
- **Storage box:** rounded rect x=160, y=200, width 400, height 55, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 8px radius; bold 13px `#1a5276` label "cloud object storage" centered at y=222, 12px `#444` sub-label "orders table · 2 TB · stored exactly once" at y=240.
- **Warehouse boxes (y=85, each 170 wide × 50 tall, 8px radius, 12px `#2c3e50` two-line labels):**
  - x=60: "ANALYTICS_WH" / "Medium — running", fill `rgba(0,131,0,0.12)`, 2px `#008300` border
  - x=275: "FINANCE_WH" / "Small — running", fill `rgba(25,158,112,0.12)`, 2px `#199e70` border
  - x=490: "DS_WH" / "suspended — costs $0", fill `#f4f5f7`, 2px dashed `#6b7280` border, labels in `#6b7280`
- **Arrows:** 3px lines from the bottom center of each running box down to the storage box top (solid `#008300` and `#199e70`); dashed 2px `#6b7280` arrow from DS_WH.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=282, below the storage box):** "each engine reads the same bytes — none blocks another".
- **Caption (12px `#444`, bottom right):** "table size illustrative".

## Monday 9am: Both Teams Hit the Orders Table

**Tags:** `worked example` (blue), `pay per second` (green)

- **The ladder** — warehouse sizes double: XS burns 1 credit/hr, S 2, M 4, L 8, XL 16
- **The meter** — compute bills per second while running, with a 60-second minimum per resume
- **Analytics** — dashboards keep a Medium busy 9:00–10:00: 4 credits/hr × 1 hr = 4.0 credits
- **Finance** — a 12-minute close job on a Small, plus 5 idle minutes before auto-suspend: 17 min billed
- **Hand-check** — finance pays 2 × 17/60 = 0.57 credits; the morning totals 4.57 credits ≈ $13.70

*Example (italic):* At an illustrative $3 per credit, the whole 9am hour — a full analytics hour plus finance's close — costs $13.70, and DS_WH costs nothing because it never woke up.

**Key point:** You pay for seconds of compute per warehouse, not for the data sitting in storage — the size ladder doubles cost per step, so the bill is arithmetic you can do by hand.

### Visualization (canvas `c2`, 720×300)

Gantt-style timeline of the 9:00–10:00 hour: one bar per warehouse showing running, idle-but-billing, and suspended time, with credit totals at the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Hour, Two Warehouses: 4.57 Credits Total".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time 9:00 to 10:00 mapped 10px per minute, 12px `#444` tick labels every 15 min ("9:00", "9:15", "9:30", "9:45", "10:00"); light gridlines `#e5e9ef` at each tick.
- **Row 1 (bar center y=110, 26px tall), 12px `#444` label "ANALYTICS_WH (M)" at x=60 above the bar:** solid green `#008300` fill `rgba(0,131,0,0.30)` with 2px `#008300` border from x=60 to x=660 (9:00–10:00); bold 12px `#008300` label "running 60 min — 4.0 credits" centered inside.
- **Row 2 (bar center y=190, 26px tall), label "FINANCE_WH (S)":** blue segment fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, x=60 to x=180 (9:00–9:12) labeled "run 12 min" (12px `#2a78d6`); orange segment fill `rgba(217,89,38,0.25)`, 2px `#d95926` border, x=180 to x=230 (9:12–9:17) with bold 11px `#d95926` label "idle 5 min — still billing" above; dashed 2px `#6b7280` line x=230 to x=660 with 12px `#6b7280` label "suspended — $0"; bold 12px `#2a78d6` "0.57 credits" at the right end.
- **Annotation (bold 13px violet `#4a3aa7`, near x=400, y=60):** "4.57 credits × $3 ≈ $13.70 for the morning".
- **Caption (12px `#444`, bottom right):** "$3/credit illustrative; credit-per-hour rates and per-second billing are Snowflake's documented model".

## Clones and Shares That Copy Nothing

**Tags:** `where it's used` (blue), `zero-copy` (green)

- **The clone** — `CREATE TABLE dev CLONE prod` makes a full test copy in seconds, storing no new data
- **The trick** — the clone is metadata pointing at the same stored files; only edits create new bytes
- **The share** — another Snowflake account can query your table live, with no export pipeline at all
- **The payoff** — dev environments, experiments, and partner access stop multiplying storage bills
- **The drift** — a clone only grows as it diverges: touch 3% of a 2 TB table, store ~60 GB extra

*Example (italic):* A full copy of the 2 TB orders table would double storage; the clone starts at ~0 GB extra and after a month of dev edits holds only 60 GB of changed data.

**Key point:** Because data lives once in shared storage, "copies" are pointers — cloning and sharing cost nearly nothing until someone actually changes the data.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing extra storage consumed: traditional full copy vs a zero-copy clone at day 1 and after a month of edits.

- **Title (bold 15px, `#1a5276`, top center):** "Extra Storage for a 'Copy' of a 2 TB Table".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; 12px `#444` left-aligned row labels at x=20.
- **Rows (bar centers at y = 90, 150, 210, bars 26px tall, 11px `#444` value labels at bar ends):**
  - "full physical copy": red `#e74c3c` fill `rgba(231,76,60,0.25)`, 2px `#e74c3c` border, width 420, label "2,000 GB"
  - "zero-copy clone, day 1": green `#008300` fill `rgba(0,131,0,0.30)`, width 4, label "~0 GB (metadata only)"
  - "clone after a month of edits": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 13, label "60 GB (3% changed)"
- **Annotation (bold 13px green `#008300`, right side near y=140):** "seconds to create, ~0 bytes until you write".
- **Caption (12px `#444`, bottom right):** "sizes illustrative; bar widths proportional to GB".

## The Warehouse Nobody Turned Off

**Tags:** `common mistake` (red), `cost control` (orange)

- **The leak** — a warehouse bills every second it is resumed, even when zero queries are running
- **The habit** — someone resizes to XL "for one big backfill" on Friday and never sizes it back
- **The math** — an XL left running 24/7 burns 16 × 730 = 11,680 credits/month, ≈ $35,040 at $3
- **The fix** — set auto-suspend to a minute or two and let auto-resume wake it on the next query
- **The sizing** — try the smaller size first: a step down halves cost, and many jobs barely slow

*Example (italic):* The same monthly workload costs $270 on an auto-suspending Small, $2,136 on a business-hours Medium, and $35,040 on a forgotten always-on XL.

**Common mistake:** Treating compute like storage. Storage is cheap and passive; a resumed warehouse is a taxi with the meter running — idle time and oversizing, not queries, produce the horror bills.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of monthly compute cost for the same workload under three warehouse habits, with the always-on XL as the alarm bar.

- **Title (bold 15px, `#1a5276`, top center):** "Same Workload, Three Habits: $270 vs $35,040 a Month".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 440; 12px `#444` two-line row labels at x=20; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (bar centers at y = 85, 150, 215, bars 26px tall, bold 12px value labels at bar ends):**
  - "S, auto-suspend 60s / 45 active hr": green `#008300` fill `rgba(0,131,0,0.30)`, width 40, label "90 credits ≈ $270" in `#008300`
  - "M, suspended nights + weekends / 178 hr": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 130, label "712 credits ≈ $2,136" in `#2a78d6`
  - "XL, left running 24/7 / 730 hr": red `#e74c3c` fill `rgba(231,76,60,0.25)`, 2px `#e74c3c` border, width 440, bold 12px red label "11,680 credits ≈ $35,040"
- **Annotation (bold 13px magenta `#d55181`, near x=300, y=250):** "130× the bill for the same queries".
- **Caption (12px `#444`, bottom right):** "credit math exact for the stated hours and rates; $3/credit and hours illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness). Documented Snowflake facts: storage/compute separation, size ladder credits/hr (XS 1 / S 2 / M 4 / L 8 / XL 16), per-second billing with a 60-second minimum, auto-suspend/auto-resume, zero-copy cloning, live data sharing. Invented and labeled illustrative: the 2 TB table, team names, run times, active hours (45 / 178 / 730), the $3/credit price, and clone drift (3% → 60 GB). Credit arithmetic (4.0, 0.57, 4.57, 90, 712, 11,680) is exact given the stated hours and rates.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
