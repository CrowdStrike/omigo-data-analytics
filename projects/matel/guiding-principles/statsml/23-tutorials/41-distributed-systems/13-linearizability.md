# Linearizability

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Linearizability

**Subtitle:** The strongest consistency promise: the system behaves as if there were only ONE copy of the data — once anyone sees a new value, nobody may ever see the old one again

## The Last Seat on Flight 214

**Tags:** `core idea` (blue), `one-copy illusion` (green), `strongest guarantee` (orange)

- **The flight** — flight 214 has one seat left, 14C, and two travelers refreshing from two continents
- **Two copies** — the airline keeps replicas in Paris and Tokyo, so each traveler reads a nearby copy
- **The sale** — Aisha buys 14C at 12:00:04; her Paris screen flips to SOLD the moment the purchase lands
- **The rewind** — at 12:00:06 Ben's Tokyo replica still shows AVAILABLE — the past has come back to life
- **The promise** — linearizability forbids exactly this: act like ONE copy, so SOLD can never rewind

*Example (italic):* Ben clicks buy at 12:00:06 on a seat that sold at 12:00:04 — a linearizable system would already have shown him SOLD.

**Key point:** A system is linearizable when every read returns the most recent completed write, as if all operations touched a single copy of the data in one real-time order.

### Visualization (canvas `c1`, 720×300)

Two-lane timeline showing what each traveler's screen displays for seat 14C over ten seconds; Ben's lane keeps showing AVAILABLE for two seconds after the purchase — the stale window a linearizable system must not have.

- **Title (bold 15px, `#1a5276`, top center):** "One Seat, Two Screens: SOLD Must Never Rewind to AVAILABLE".
- **Axes:** origin x=60, plot width 600 mapping seconds 0–10 (60px per second); time tick labels "12:00:00" to "12:00:10" every 2s, 12px `#6b7280`, along y=265; lane labels 12px `#2c3e50` at x=10: "Aisha (Paris)" beside y=110, "Ben (Tokyo)" beside y=190; lane bars 24px tall.
- **Aisha lane (y=110):** green `rgba(0,131,0,0.30)` bar with 2px `#008300` border from t=0 to t=4 labeled "AVAILABLE" (12px `#008300`), then magenta `rgba(213,81,129,0.30)` bar with 2px `#d55181` border from t=4 to t=10 labeled "SOLD" (12px `#d55181`).
- **Ben lane (y=190):** same green AVAILABLE bar from t=0 to t=6, then magenta SOLD bar from t=6 to t=10; the t=4–6 portion of the green bar gets a 3px `#d95926` outline and bold 12px `#d95926` label "stale — violates linearizability" below it.
- **Purchase marker:** vertical dashed `#6b7280` line (dash 4/3) at t=4 from y=70 to y=230, 12px `#6b7280` label "purchase completes 12:00:04" at its top.
- **Annotation (bold 13px `#d95926`, near t=6.5, y=60):** "once anyone sees SOLD, no later read may show AVAILABLE".
- **Caption (12px `#444`, bottom right):** "times illustrative".

## Four Reads Against One Write

**Tags:** `worked example` (blue), `overlapping ops` (green), `legal vs illegal` (orange)

- **The register** — one value, seat 14C, starts OPEN; a single write W flips it to SOLD
- **The write** — W(SOLD) is in flight from t=3s to t=6s; inside that window it may or may not have landed
- **Before** — R1 runs 1–2s and returns OPEN: legal, the write had not even started
- **Overlap** — R2 runs 4–5s and returns SOLD: legal, a read overlapping the write may see it early
- **The rewind** — R3 runs 5.5–6.5s and returns OPEN: ILLEGAL — R2 already returned SOLD before R3 began
- **After** — R4 runs 8–9s and returns SOLD: legal, and by then the only legal answer

*Example (italic):* R3 overlaps the write just like R2 did, yet OPEN is illegal for it — legality depends on what earlier reads already returned, not on overlap alone.

**Key point:** Operations that overlap the write may land on either side of it, but the moment any read returns SOLD, every read that starts afterward must return SOLD too.

### Visualization (canvas `c2`, 720×300)

Gantt-style timeline of one write and four reads on a shared 0–10s clock; each read bar carries its returned value and a legal/illegal verdict, with the point of no return marked where R2 completes.

- **Title (bold 15px, `#1a5276`, top center):** "Four Reads Against One Write: Legal or Illegal, Case by Case".
- **Axes:** origin x=60, plot width 600 mapping seconds 0–10 (60px per second); x tick labels "0s"–"10s" every 2s, 12px `#6b7280`, along y=262; light `#e5e9ef` vertical gridlines at each tick; row labels 12px `#2c3e50` at x=10.
- **Bars (18px tall, rounded 3px):**
  - Row "W" at y=70: blue `rgba(42,120,214,0.30)` bar, 2px `#2a78d6` border, seconds `[3, 6]`, 12px `#2a78d6` label "W: OPEN → SOLD".
  - Row "R1" at y=110: green `rgba(0,131,0,0.25)` bar, 2px `#008300` border, seconds `[1, 2]`, 12px `#008300` label "R1 → OPEN ✓".
  - Row "R2" at y=145: green bar, seconds `[4, 5]`, 12px `#008300` label "R2 → SOLD ✓".
  - Row "R3" at y=180: orange `rgba(217,89,38,0.25)` bar, 2px `#d95926` border, seconds `[5.5, 6.5]`, bold 12px `#d95926` label "R3 → OPEN ✗ illegal".
  - Row "R4" at y=215: green bar, seconds `[8, 9]`, 12px `#008300` label "R4 → SOLD ✓".
- **Point of no return:** vertical dashed `#6b7280` line (dash 4/3) at t=5 (where R2 completes) from y=55 to y=240, 12px `#6b7280` label "SOLD has been seen" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, near t=6.5, y=248):** "after R2 returns SOLD, OPEN is dead — even for reads overlapping the write".
- **Caption (12px `#444`, bottom right):** "seconds illustrative".

## Locks, Leaders, and the C in CAP

**Tags:** `where it's used` (blue), `CAP theorem` (green), `latency cost` (orange)

- **Locks** — a lock service that is not linearizable can tell two workers they both hold the lock
- **Leader election** — "who is leader" is one shared value; a stale read means two leaders (split brain)
- **Uniqueness** — "is this username taken?" must see the latest write, or two people claim the same name
- **CAP's C** — the C in the CAP theorem means exactly linearizability, nothing weaker
- **The price** — a same-region leader read costs ~12 ms and a cross-region quorum read ~95 ms: coordination is the fee

*Example (italic):* A local replica answers in 2 ms with no freshness guarantee; a consensus write across 3 regions for the same seat costs about 180 ms.

**Key point:** Reach for linearizability where a stale read breaks correctness — locks, leaders, unique names; everywhere else its coordination cost is usually not worth paying.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: latency of four read/write paths, from an uncoordinated local read to a fully linearizable cross-region consensus write.

- **Title (bold 15px, `#1a5276`, top center):** "The One-Copy Illusion Has a Price Tag in Milliseconds".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440 for 180 ms (pixel width = ms × 440/180); left-aligned 12px `#2c3e50` row labels at x=20.
- **Rows (14px tall bars, top to bottom at y = 70, 120, 170, 220):**
  - "local replica read — NOT linearizable: 2 ms": green `rgba(0,131,0,0.35)` bar width 5, 2px `#008300` border
  - "leader read, same region: 12 ms": blue `rgba(42,120,214,0.35)` bar width 29, 2px `#2a78d6` border
  - "quorum read, cross-region: 95 ms": blue bar width 232
  - "consensus write, 3 regions: 180 ms": orange `rgba(217,89,38,0.35)` bar width 440, 2px `#d95926` border
- **Width labels:** 11px `#6b7280` millisecond values ("2 ms", "12 ms", "95 ms", "180 ms") at each bar's right end.
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "the one-copy illusion is bought with round trips".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## Serializable Is Not Linearizable

**Tags:** `common mistake` (red), `serializability` (orange)

- **Two words** — they sound alike but promise different things about different scopes
- **Linearizability** — one object at a time; guarantees recency against real wall-clock time
- **Serializability** — many objects in one transaction; guarantees some order, not a recent one
- **No recency** — a serializable database may legally serve you yesterday's snapshot, perfectly ordered
- **Both at once** — "strict serializability" combines the two guarantees; that is what Spanner-style systems sell
- **The tell** — "single value, must be fresh?" → linearizable; "many rows, all-or-nothing?" → serializable

*Example (italic):* A serializable-but-stale bank read can show a balance from before the last deposit — perfectly ordered, perfectly out of date.

**Common mistake:** Assuming a "serializable" database gives fresh reads. Serializability orders transactions; it never promises that order matches real time — that extra promise is linearizability.

### Visualization (canvas `c4`, 720×300)

Comparison grid: three guarantee levels scored against the two distinct promises — single-object recency and multi-object transactions.

- **Title (bold 15px, `#1a5276`, top center):** "Two Promises, Two Scopes".
- **Grid:** column headers bold 12px `#2c3e50` at y=75: "fresh single value (recency)" centered at x=330, "multi-object transactions" centered at x=560; row labels bold 12px `#1a5276` at x=30 for rows at y = 115, 170, 225: "Linearizable", "Serializable", "Strict serializable"; 1px `#e5e9ef` gridlines separating rows (horizontal at y = 90, 145, 200, 250) and columns (vertical at x=210 and x=445, from y=60 to y=250).
- **Cells (bold 18px marks centered in each cell):** green `#008300` "✓" or orange `#d95926` "✗":
  - Linearizable: ✓ / ✗
  - Serializable: ✗ / ✓
  - Strict serializable: ✓ / ✓
- **Annotation (bold 13px blue `#2a78d6`, centered near y=280):** "CAP's C is the left column; ACID's I lives in the right".
- **Caption (12px `#444`, bottom right):** "schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the flight timeline (purchase at 12:00:04, Tokyo stale until 12:00:06), the c2 operation intervals (W `[3,6]`, R1 `[1,2]`, R2 `[4,5]`, R3 `[5.5,6.5]`, R4 `[8,9]` seconds) and their returned values, and the c3 latencies (2 / 12 / 95 / 180 ms) are invented and labeled illustrative or schematic; the c4 ✓/✗ grid (linearizable = recency only, serializable = transactions only, strict serializable = both) states the true theoretical relationship.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
