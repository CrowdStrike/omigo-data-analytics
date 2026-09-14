# Generational GC

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Generational GC

**Subtitle:** Most objects a program creates are thrown away almost immediately — so the garbage collector checks the newest objects often and the old survivors rarely, and saves enormous work

## Busing Tables at a Busy Cafe

**Tags:** `core idea` (blue), `objects die young` (green), `two generations` (orange)

- **The cafe** — a busy cafe hands out hundreds of paper cups a morning; most land in the trash within minutes
- **The mugs** — a handful of regulars' ceramic mugs sit on the shelf for months and never move
- **Smart staff** — bus the tables (where fresh cups pile up) every few minutes; dust the shelf once a week
- **Programs too** — most objects a program creates are garbage moments later; a few live for hours
- **The split** — new objects go to a small "nursery" swept often; survivors move to an old area swept rarely

*Example (italic):* A loop builds a temporary string on every pass and drops it a microsecond later — thousands of paper cups; the app's config object is the mug on the shelf.

**Key point:** Generational GC bets that most objects die young — so it sweeps the small nursery of new objects constantly and leaves the long-lived old generation almost alone.

### Visualization (canvas `c1`, 720×300)

Single-panel survival curve: percent of a batch of new objects still alive as they age through GC cycles, dropping off a cliff in the first cycle and flattening near 5%.

- **Title (bold 15px, `#1a5276`, top center):** "Most Objects Die Young: % Still Alive by Age".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x axis = object age in GC cycles 0 to 10, 12px `#444` tick labels "0", "1", ..., "10" every cycle; y axis = % still alive 0 to 100, 12px `#444` labels "0%", "25%", "50%", "75%", "100%" with light `#e5e9ef` gridlines.
- **Survival curve:** blue `#2a78d6` 3px line through hardcoded points at age = `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, alive % = `[100, 40, 18, 10, 7, 6, 5.5, 5.2, 5.1, 5.0, 5.0]`; fill under the curve `rgba(42,120,214,0.15)`.
- **Cliff marker:** vertical dashed green `#008300` (dash 4/3) line at age 1 from baseline to y=70; bold 12px green label beside it: "60% dead after one cycle".
- **Floor marker:** horizontal dashed `#6b7280` (dash 4/3) line at 5% across the plot; 12px `#6b7280` label at its right end: "long-lived survivors ~5%".
- **Annotation (bold 13px orange `#d95926`, near x=5.5 cycles, y=110):** two lines: "paper cups vanish fast —" / "the mugs stay on the shelf".
- **Caption (12px `#444`, bottom right):** "illustrative — typical shape across languages, not one measured program".

## One Morning, 1,000 Cups: Counting the Survivors

**Tags:** `worked example` (blue), `minor GC` (green)

- **The batch** — the program allocates 1,000 new objects; all of them start life in the nursery
- **First sweep** — the minor GC finds only 50 still reachable: it copies those 50 out, reclaims 950 slots
- **The bill** — a copying collector pays only for live objects, so the sweep cost 50 copies, not 1,000 checks
- **Second sweep** — of those 50 survivors, only 10 are still alive one cycle later; 40 more are reclaimed
- **Promotion** — the 10 that survived twice get moved ("promoted") to the old generation, off the busy floor

*Example (italic):* 1,000 allocated, 50 alive at sweep one, 10 alive at sweep two — the collector did 50 + 10 = 60 copies to reclaim 990 objects.

**Key point:** The minor GC touched 50 objects to free 950 — the cheaper your objects die, the cheaper the sweep, which is exactly why dying young is good news.

### Visualization (canvas `c2`, 720×300)

Horizontal funnel of three bars on a shared count axis: 1,000 allocated, 50 after the first minor GC, 10 promoted after the second — each bar labeled with what was reclaimed.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Allocated → 50 Survive → 10 Promoted".
- **Axis:** horizontal 2px `#999` line at y=250 from x=230 to x=680 (width 450), count 0 to 1,000 on a linear scale; tick labels "0", "250", "500", "750", "1,000" (12px `#444`) below.
- **Rows (top to bottom at y = 90, 150, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "allocated in the nursery": bar 0–1,000, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border
  - "alive after minor GC #1": bar 0–50, fill `rgba(0,131,0,0.35)`, 2px `#008300` border
  - "promoted after minor GC #2": bar 0–10, fill `rgba(217,89,38,0.35)`, 2px `#d95926` border
- **Bar style:** 26px-tall rounded bars; bold 13px count label at each bar's right end in the bar's border color ("1,000", "50", "10").
- **Reclaim callouts:** 12px `#6b7280` labels to the right of rows 2 and 3: "950 reclaimed" and "40 more reclaimed".
- **Annotation (bold 13px green `#008300`, near x=430, y=120):** two lines: "sweep cost = survivors copied," / "not garbage counted".
- **Caption (12px `#444`, bottom right):** "illustrative counts for one nursery batch".

## Why the Nursery Trick Wins

**Tags:** `where it's used` (blue), `pause times` (green), `throughput` (orange)

- **Everywhere** — Java, C#, JavaScript's V8, and CPython's cycle collector all lean on generations
- **Small room, often** — the nursery holds only recent objects, so each minor sweep is quick and frequent
- **Big room, rarely** — the old generation is scanned only in an occasional major GC, since mugs rarely die
- **The savings** — a full-heap sweep of our example touches all 1,050 live-or-dead slots; the minor sweep copies 50
- **User-visible** — shorter sweeps mean shorter pauses; a game or web server stutters less between frames

*Example (italic):* Five collection rounds cost a full-heap collector 5 × 1,050 = 5,250 slots examined; the generational one does 5 minor sweeps of ~50 copies each, about 250.

**Key point:** Checking the whole heap every time wastes effort on objects that almost never die — generations focus every sweep where the garbage actually is.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart over five collection rounds comparing work done per round: full-heap sweep (1,050 slots examined every round) vs generational minor sweep (survivors copied per round).

- **Title (bold 15px, `#1a5276`, top center):** "Work per Collection Round: Full-Heap Sweep vs Nursery Sweep".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x axis = rounds 1 to 5 as five grouped pairs, 12px `#444` tick labels "round 1" ... "round 5"; y axis = objects touched 0 to 1,200, 12px `#444` labels "0", "300", "600", "900", "1,200" with light `#e5e9ef` gridlines.
- **Full-heap bars:** magenta `#d55181`, fill `rgba(213,81,129,0.35)` with 2px border, values `[1050, 1050, 1050, 1050, 1050]`; 11px magenta value labels "1,050" above each bar.
- **Nursery bars:** green `#008300`, fill `rgba(0,131,0,0.35)` with 2px border, values `[50, 55, 45, 50, 50]`; 11px green value labels above each bar.
- **Legend (12px, top right inside plot):** magenta swatch "full-heap sweep", green swatch "generational minor sweep".
- **Annotation (bold 13px green `#008300`, centered near y=100):** "same garbage reclaimed, ~20× less work per round".
- **Caption (12px `#444`, bottom right):** "illustrative — round 1 matches the worked example (50 survivors)".

## "So Old Objects Never Get Collected?"

**Tags:** `common mistake` (red), `major GC` (orange)

- **The worry** — if the shelf is rarely checked, don't dead mugs pile up there forever?
- **Major GC** — no: an occasional full collection sweeps the old generation too, just far less often
- **Rare by design** — old objects rarely die, so scanning them every round would find almost nothing
- **The trade** — many tiny minor pauses plus a rare bigger major pause, instead of a big pause every time
- **Real leak risk** — a true leak is a reachable object you forgot about; no collector of any kind frees those

*Example (italic):* Over 30 seconds an app takes a 2 ms minor pause every second, then one 60 ms major pause — the shelf does get dusted, just once instead of thirty times.

**Common mistake:** Thinking generational GC abandons old objects. It still collects them in major GCs — "rarely swept" means "swept when it pays off", not "never swept".

### Visualization (canvas `c4`, 720×300)

Timeline of GC pauses over 30 seconds: thirty tiny minor-GC pause bars of 2 ms, and one tall major-GC bar of 60 ms at the 30-second mark, on a shared milliseconds axis.

- **Title (bold 15px, `#1a5276`, top center):** "30 Seconds of Pauses: Tiny Minor Sweeps, One Major Sweep".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x axis = time 0 to 32 s, 12px `#444` tick labels "0s", "5s", "10s", "15s", "20s", "25s", "30s"; y axis = pause length 0 to 70 ms, 12px `#444` labels "0", "20", "40", "60" with light `#e5e9ef` gridlines.
- **Minor pauses:** thirty blue `#2a78d6` 6px-wide bars at t = 1, 2, 3, ..., 30 s, every one exactly 2 ms tall; 12px blue label above the cluster near t=8: "minor GC: 2 ms each, every second".
- **Major pause:** one orange `#d95926` 10px-wide bar at t=30 s, 60 ms tall, drawn beside that second's minor bar; bold 12px orange label above it: "major GC: 60 ms".
- **Guide line:** horizontal dashed `#6b7280` (dash 4/3) line at 2 ms across the plot (hugging the minor bars); 11px `#6b7280` label at its left end: "2 ms".
- **Annotation (bold 13px violet `#4a3aa7`, near t=16 s, y=110):** two lines: "the old generation IS collected —" / "once, when it pays off".
- **Caption (12px `#444`, bottom right):** "illustrative pause pattern, not a measured runtime".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, curve points, and counts are the hardcoded arrays above (no randomness); the worked-example numbers (1,000 / 50 / 10 / 950 / 40) must appear identically in text and charts; all invented numbers keep their "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
