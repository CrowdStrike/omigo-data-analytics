# Happens-Before & Partial Order

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Happens-Before & Partial Order

**Subtitle:** In a distributed system time is not a line — only messages create order, and event pairs no message chain connects have no order at all

## Three Coworkers, No Shared Clock

**Tags:** `core idea` (blue), `distributed time` (green), `messages` (orange)

- **The office** — Ana, Ben, and Cara work in separate rooms with no shared clock on the wall
- **The events** — each keeps a private log: entries a1–a3 for Ana, b1–b3 for Ben, c1–c3 for Cara
- **The notes** — Ana slips note m1 to Ben (a1 to b2); later Ben slips note m2 to Cara (b3 to c2)
- **Clear order** — writing a note plainly happens before reading it, and each log reads top to bottom
- **No order** — Ana's a2 and Cara's c1 have no note chain between them, so neither one is "first"
- **The name** — this could-have-influenced relation is happens-before, and it is only a partial order

*Example (italic):* Ana's second entry and Cara's first entry both simply happened — asking which came "first" has no answer that either room could ever act on.

**Key point:** Happens-before orders a pair of events only when a chain of same-log steps and note deliveries connects them; every other pair is concurrent — time here is a web of arrows, not a line.

### Visualization (canvas `c1`, 720×300)

Space-time message diagram: three horizontal process lines (Ana, Ben, Cara), nine event dots, two message arrows, one concurrent pair highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Nine Events, Two Notes: Arrows Are the Only Order".
- **Process lines:** 2px `#6b7280` horizontal lines at y = 95, 165, 235, from x=90 to x=680; 13px bold `#2c3e50` labels "Ana", "Ben", "Cara" at x=25 on each line.
- **Events:** 7px-radius filled `#1a5276` dots with bold 12px `#1a5276` labels — Ana at x `[170, 340, 540]` labeled `["a1","a2","a3"]` (labels above the line); Ben at x `[150, 310, 460]` labeled `["b1","b2","b3"]` (above); Cara at x `[200, 560, 650]` labeled `["c1","c2","c3"]` (below).
- **Message m1:** blue `#2a78d6` 3px arrow with arrowhead from a1 (170, 95) to b2 (310, 165), 12px blue label "note m1" beside the midpoint.
- **Message m2:** green `#008300` 3px arrow from b3 (460, 165) to c2 (560, 235), 12px green label "note m2" beside the midpoint.
- **Concurrent highlight:** magenta `#d55181` dashed (dash 4/3) halo circles, radius 13, around a2 (340, 95) and c1 (200, 235).
- **Annotation (bold 13px magenta `#d55181`, near x=300, y=280):** "a2 and c1: no arrow chain either way — concurrent".
- **Caption (12px `#444`, bottom right):** "event spacing illustrative".

## Chaining the Arrows: 19 Ordered Pairs, 17 Concurrent

**Tags:** `worked example` (blue), `transitivity` (green)

- **Three rules** — same log top to bottom, every send before its receive, and chains: x→y and y→z give x→z
- **Same-log pairs** — 3 ordered pairs per person, times 3 people = 9 pairs before any note is counted
- **Note edges** — m1 gives a1 → b2 and m2 gives b3 → c2; chaining adds a1 → b3, a1 → c2, even a1 → c3
- **The tally** — of the 36 possible pairs among 9 events, 19 come out ordered and 17 stay concurrent
- **Hand-check** — a1 → c3 holds via a1 → b2 → b3 → c2 → c3, but nothing connects b2 and c1, so b2 ∥ c1

*Example (italic):* a1 reaches c3 through a four-step chain, yet a2 — written moments after a1 — is concurrent with all six of Ben's and Cara's events.

**Key point:** Build the relation mechanically: same-log edges, send-receive edges, then close under transitivity — any pair the chains never join is concurrent, and here that is nearly half of all 36 pairs.

### Visualization (canvas `c2`, 720×300)

Two-panel figure: left, the message diagram with the transitive chain a1 → b2 → b3 → c2 → c3 highlighted; right, a horizontal bar tally of pair types.

- **Title (bold 15px, `#1a5276`, top center):** "One Chain Orders a1 Through c3; 17 of 36 Pairs Stay Concurrent".
- **Left panel (x 20–440):** three 2px `#6b7280` lines at y = 85, 155, 225 from x=70 to x=430, 12px `#444` labels "Ana"/"Ben"/"Cara" at x=25; events (6px dots) — Ana at x `[110, 210, 330]`, Ben at x `[100, 190, 280]`, Cara at x `[130, 340, 400]`, bold 11px labels a1–c3 as in c1.
- **Chain highlight:** green `#008300` 3px segments a1 (110,85) → b2 (190,155), b2 → b3 along Ben's line (190,155)–(280,155), b3 (280,155) → c2 (340,225), c2 → c3 along Cara's line (340,225)–(400,225); the five chain dots filled `#008300`, the other four dots `#6b7280`.
- **Right panel bars:** three horizontal bars 22px tall starting at x=470, at y = 90, 150, 210; width 9px per pair — "same-log 9" width 81, fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; "via notes 10" width 90, fill `rgba(0,131,0,0.25)` with 2px `#008300` border; "concurrent 17" width 153, fill `rgba(213,81,129,0.25)` with 2px `#d55181` border; 12px `#2c3e50` labels above each bar, 12px value labels at bar ends.
- **Annotation (bold 12px violet `#4a3aa7`, near x=470, y=265):** "nearly half of all pairs have no order at all".
- **Caption (12px `#444`, bottom right):** "36 pairs among 9 events; counts exact for this diagram".

## The Rule Under Logical Clocks and Race Detectors

**Tags:** `where it's used` (blue), `logical clocks` (green), `race detection` (orange)

- **Lamport clocks** — number events so every arrow goes small to large: Ana 1,2,3; Ben 1,2,3; Cara 1,4,5
- **The receive rule** — a receive takes max(own, sender's) + 1, so c2 = max(1, 3) + 1 = 4 jumps Cara's count
- **Causal consistency** — a data store may show a reply only after the message it answers: these arrows
- **Race detection** — two writes to one variable with no happens-before path between them is a data race
- **The limit** — a3 and b3 both carry stamp 3; Lamport numbers respect arrows but cannot flag concurrency

*Example (italic):* Ben's b3 and Ana's a3 both get stamp 3 — the stamps order every arrowed pair correctly yet stay completely silent about this concurrent one.

**Key point:** Happens-before is the ground truth that logical clocks approximate, causal consistency enforces, and race detectors query — every one of those tools is defined in terms of this relation.

### Visualization (canvas `c3`, 720×300)

The same space-time diagram as c1 with a Lamport clock stamp printed beside every event, showing stamps rise along every arrow but tie across concurrent events.

- **Title (bold 15px, `#1a5276`, top center):** "Lamport Clocks: Every Arrow Goes Small → Large".
- **Layout:** identical lines, dots, event x positions, and m1/m2 arrows as canvas c1 (lines y = 95/165/235; Ana x `[170, 340, 540]`, Ben x `[150, 310, 460]`, Cara x `[200, 560, 650]`).
- **Labels:** each dot gets a two-part label — event name in 12px `#6b7280` plus its stamp in bold 13px yellow `#c98500`: Ana "a1·1", "a2·2", "a3·3"; Ben "b1·1", "b2·2", "b3·3"; Cara "c1·1", "c2·4", "c3·5" (above the line for Ana/Ben, below for Cara).
- **Receive-rule note (bold 12px aqua `#199e70`, near x=560, y=190):** "c2 = max(1, 3) + 1 = 4".
- **Tie highlight:** violet `#4a3aa7` dashed (4/3) halo circles radius 13 around a3 (540, 95) and b3 (460, 165).
- **Annotation (bold 13px violet `#4a3aa7`, near x=360, y=280):** "a3 and b3 both stamped 3 — clocks can't see concurrency".
- **Caption (12px `#444`, bottom right):** "stamps computed by the Lamport rule; spacing illustrative".

## Concurrent Does Not Mean At the Same Instant

**Tags:** `common mistake` (red), `concurrent` (orange)

- **The trap** — "concurrent" sounds like "at the same moment"; here it means no causal path either way
- **Six seconds apart** — a2 happens at 10:00:01 and c3 at 10:00:07 on a perfect wall clock, yet a2 ∥ c3
- **Why it holds** — no chain of notes runs from a2 to c3 or back, so neither could have influenced the other
- **Ordered and close** — b3 (10:00:04) → c2 (10:00:05) sit one second apart yet are strictly ordered by m2
- **Why we care** — a system may replay concurrent events in either order; ordered pairs it must never flip

*Example (italic):* A replica applying c3 before a2 is perfectly correct even though a2 came six wall-clock seconds earlier — no observer inside the system can tell the difference.

**Common mistake:** Reading concurrent as simultaneous. Concurrent events can be far apart on the wall clock — the claim is only that neither could have caused the other, so either replay order is legal.

### Visualization (canvas `c4`, 720×300)

Wall-clock timeline contrasting one concurrent pair six seconds apart with one ordered pair only one second apart.

- **Title (bold 15px, `#1a5276`, top center):** "Six Seconds Apart Yet Concurrent; One Second Apart Yet Ordered".
- **Axis:** 2px `#999` baseline at y=245 from x=70 to x=680; scale 76px per second, x(t) = 70 + t×76; 12px `#6b7280` tick labels "10:00:00", "10:00:02", "10:00:04", "10:00:06", "10:00:08" at t = 0, 2, 4, 6, 8; light `#e5e9ef` vertical gridlines at each labeled tick.
- **Row 1 (y=110), left label 12px `#444` at x=15: "concurrent pair":** magenta `#d55181` 7px dots at a2 (t=1, x=146) and c3 (t=7, x=602), bold 12px magenta event labels above; dashed magenta (4/3) 2px line between them with bold 12px magenta label "a2 ∥ c3 — 6 s apart, still concurrent" centered above the line.
- **Row 2 (y=180), left label: "ordered pair":** blue `#2a78d6` 7px dot b3 at (t=4, x=374), green `#008300` 7px dot c2 at (t=5, x=450), solid green 3px arrow from b3 to c2 with 12px green label "note m2: b3 → c2" below.
- **Annotation (bold 13px orange `#d95926`, near x=200, y=60):** "concurrent means no cause either way — not 'same instant'".
- **Caption (12px `#444`, bottom right):** "wall-clock times illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all coordinates and event positions are the hardcoded arrays above (no randomness); event spacing and the wall-clock times (a2 10:00:01, b3 10:00:04, c2 10:00:05, c3 10:00:07) are invented and labeled illustrative; the pair tally (9 same-log + 10 via notes = 19 ordered, 17 concurrent, of 36 pairs among 9 events) and the Lamport stamps (Ana 1,2,3; Ben 1,2,3; Cara 1,4,5 with c2 = max(1,3)+1 = 4) follow exactly from the two-message diagram.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
