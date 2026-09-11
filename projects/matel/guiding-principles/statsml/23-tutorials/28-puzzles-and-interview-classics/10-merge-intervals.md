# Merge Intervals

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Merge Intervals

**Subtitle:** To see when a calendar is truly busy, sort the bookings by start time and sweep through them once, gluing every booking that overlaps the block you are building

## One Meeting Room, Six Bookings

**Tags:** `core idea` (blue), `overlapping ranges` (green), `running example` (orange)

- **The room** — an office has one meeting room; six teams booked slices of the same day
- **The bookings** — 9:00–10:30, 13:00–14:00, 10:00–11:00, 15:30–17:00, 13:30–15:00, 16:00–16:30
- **The overlap** — two teams both hold 10:00–10:30; the calendar shows six bars, not the truth
- **The question** — when is the room actually occupied, and when is it genuinely free?
- **The move** — glue every pair of bookings that touch or overlap into one solid busy block

*Example (italic):* The 9:00–10:30 and 10:00–11:00 bookings share the 10:00–10:30 half hour, so the room is really busy in one unbroken stretch from 9:00 to 11:00.

**Key point:** Merging intervals means collapsing every cluster of overlapping ranges into one solid block, so the timeline shows what is truly covered.

### Visualization (canvas `c1`, 720×300)

Timeline chart: the six raw bookings drawn as horizontal bars in arrival order on a shared clock axis, with the 10:00–10:30 overlap visibly stacked.

- **Title (bold 15px, `#1a5276`, top center):** "Six Bookings on One Room — Where Do They Overlap?".
- **Axis:** horizontal 2px `#999` line at y=255 from x=100 to x=700, clock time 8:00 to 18:00 (60px per hour); 12px `#444` tick labels "8:00", "9:00", ..., "18:00" every hour; light `#e5e9ef` vertical gridlines at each hour from y=55 to the axis.
- **Rows (top to bottom at y = 70, 100, 130, 160, 190, 220), each a 16px-tall rounded blue `rgba(42,120,214,0.35)` bar with a 2px `#2a78d6` border, booking label in 12px `#444` just left of the bar (or inside if the bar is wide):**
  - "9:00–10:30": x 160 to 250
  - "13:00–14:00": x 400 to 460
  - "10:00–11:00": x 220 to 280
  - "15:30–17:00": x 550 to 640
  - "13:30–15:00": x 430 to 520
  - "16:00–16:30": x 580 to 610
- **Overlap highlight:** vertical magenta `#d55181` dashed (dash 4/3) lines at x=220 and x=250 from y=60 to the axis, bracketing the shared 10:00–10:30 slice.
- **Annotation (bold 13px magenta `#d55181`, near x=175, y=48):** "two teams hold 10:00–10:30 at once".
- **Caption (12px `#444`, bottom right):** "illustrative bookings for one shared room".

## Sort by Start, Then Sweep Once

**Tags:** `worked example` (blue), `sort then sweep` (green)

- **Step 1: sort** — order by start time: 9:00, 10:00, 13:00, 13:30, 15:30, 16:00; ends come along
- **Step 2: sweep** — hold one open block; if the next booking starts before it ends, stretch the block
- **First glue** — block 9:00–10:30 meets 10:00–11:00; 10:00 is before 10:30, so it grows to 9:00–11:00
- **First gap** — 13:00 is after 11:00, so close the block and open a fresh one at 13:00–14:00
- **Keep going** — 13:30 lands inside, stretching it to 13:00–15:00; 15:30 starts block three
- **The result** — six bookings collapse into three busy blocks: 9:00–11:00, 13:00–15:00, 15:30–17:00

*Example (italic):* At every step the only comparison is "does the next start beat the current end?" — 10:00 vs 10:30 says glue, 13:00 vs 11:00 says close and move on.

**Key point:** After sorting by start, one left-to-right sweep with a single open block finds every merge — each booking is looked at exactly once.

### Visualization (canvas `c2`, 720×300)

Before/after timeline: the six sorted bookings on top, colored by the merged block they end up in, with the three final busy blocks drawn as thick bars beneath them.

- **Title (bold 15px, `#1a5276`, top center):** "Sorted Sweep: Six Bookings Collapse into Three Blocks".
- **Axis:** horizontal 2px `#999` line at y=260 from x=100 to x=700, clock time 8:00 to 18:00 (60px per hour); 12px `#444` tick labels every hour; light `#e5e9ef` hour gridlines from y=50.
- **Sorted bookings (thin 10px rounded bars, rows top to bottom at y = 60, 85, 110, 135, 160, 185), fill matches destination block:**
  - blue `rgba(42,120,214,0.45)`: "9:00–10:30" x 160–250, then "10:00–11:00" x 220–280
  - green `rgba(0,131,0,0.40)`: "13:00–14:00" x 400–460, then "13:30–15:00" x 430–520
  - orange `rgba(217,89,38,0.40)`: "15:30–17:00" x 550–640, then "16:00–16:30" x 580–610
- **Merged row (y=225):** three 18px-tall solid bars — blue `#2a78d6` x 160–280 labeled "9:00–11:00", green `#008300` x 400–520 labeled "13:00–15:00", orange `#d95926` x 550–640 labeled "15:30–17:00"; labels bold 12px in the bar color, above each bar.
- **Row caption:** 12px `#6b7280` label "merged busy blocks" at x=20, y=230; 12px `#6b7280` label "sorted bookings" at x=20, y=60.
- **Annotation (bold 13px `#1a5276`, near x=170, y=40):** "6 bookings in, 3 busy blocks out — one pass".

## Seven Booked Hours That Are Really Five and a Half

**Tags:** `where it's used` (blue), `double counting` (orange)

- **Naive total** — adding the six booking lengths gives 1.5+1+1+1.5+1.5+0.5 = 7.0 hours of "usage"
- **True total** — the merged blocks hold 2 + 2 + 1.5 = 5.5 hours; overlap was counted twice
- **The report** — naive math calls the room 70% used over a 10-hour day; the truth is 55%
- **Free slots** — only the merged view exposes the real gaps: 11:00–13:00 and 15:00–15:30
- **Same trick elsewhere** — user sessions, sensor uptime, ad exposure windows, gene ranges

*Example (italic):* A facilities report summed raw bookings and bought a second meeting room for a building whose room sat free for 2.5 hours every day.

**Key point:** Any "total time covered" number computed from raw overlapping ranges is inflated — merge first, then measure.

### Visualization (canvas `c3`, 720×300)

Two-bar comparison: total hours from summing raw bookings versus total hours after merging, with the double-counted overlap shown as a red tail on the naive bar.

- **Title (bold 15px, `#1a5276`, top center):** "Summed Bookings Say 7.0 h — the Room Is Busy 5.5 h".
- **Axis:** horizontal 2px `#999` line at y=235 from x=150 to x=680, hours 0 to 7.5 (70px per hour); 12px `#444` tick labels "0 h", "1 h", ..., "7 h" every hour; light `#e5e9ef` gridlines from y=70.
- **Bar 1 (y=110, 34px tall):** "sum of raw bookings" — blue `rgba(42,120,214,0.45)` from x=150 to x=535, then red `rgba(231,76,60,0.45)` segment from x=535 to x=640 with 2px `#e74c3c` border; bold 13px `#e74c3c` value label "7.0 h" right of the bar; row label 12px `#444` at x=20.
- **Bar 2 (y=175, 34px tall):** "merged busy blocks" — solid green `rgba(0,131,0,0.40)` with 2px `#008300` border from x=150 to x=535; bold 13px `#008300` value label "5.5 h" right of the bar; row label 12px `#444` at x=20.
- **Annotation (bold 13px `#e74c3c`, near x=430, y=85, arrowed to the red segment):** "1.5 phantom hours — overlap counted twice".
- **Caption (12px `#444`, bottom right):** "illustrative — one day, 10 working hours".

## The Booking Inside a Booking

**Tags:** `common mistake` (red), `max of ends` (orange)

- **The trap** — 16:00–16:30 sits entirely inside 15:30–17:00; it overlaps, so the sweep glues it
- **Wrong glue** — setting the block's end to the newcomer's end shrinks it to 15:30–16:30
- **Right glue** — the end must be max(old end, new end): max(17:00, 16:30) keeps 15:30–17:00
- **Lost time** — the wrong rule silently marks 16:30–17:00 as free while a meeting is running
- **The other trap** — skipping the sort: an unsorted sweep merges bookings that never touch

*Example (italic):* One wrong line — `end = new_end` instead of `end = max(end, new_end)` — told a team the room freed up at 16:30, and they walked into a live meeting.

**Common mistake:** When a range is swallowed by the current block, taking the newcomer's end instead of the max of both ends — contained intervals must never shrink the block.

### Visualization (canvas `c4`, 720×300)

Two-row timeline zoomed on the afternoon: the correct merged block versus the shrunken block from the `end = new_end` bug, with the lost half hour flagged in red.

- **Title (bold 15px, `#1a5276`, top center):** "A Contained Booking Must Not Shrink the Block".
- **Axis:** horizontal 2px `#999` line at y=245 from x=100 to x=700, clock time 15:00 to 17:30 (240px per hour); 12px `#444` tick labels "15:00", "15:30", "16:00", "16:30", "17:00", "17:30" every half hour; light `#e5e9ef` gridlines from y=55.
- **Inputs row (y=70):** two thin 12px bars — blue `rgba(42,120,214,0.45)` "15:30–17:00" x 220–580, and magenta `rgba(213,81,129,0.45)` "16:00–16:30" x 340–460 drawn 18px below it (y=92); 12px `#444` labels left of each; row caption 12px `#6b7280` "the two bookings" at x=20, y=76.
- **Right row (y=145, 20px tall):** green `#008300` solid bar x 220–580 labeled bold 12px green "end = max(17:00, 16:30) → 15:30–17:00"; row label 12px `#444` "correct" at x=20.
- **Wrong row (y=200, 20px tall):** orange `rgba(217,89,38,0.55)` bar x 220–460, then red `rgba(231,76,60,0.35)` dashed-border (dash 4/3, 2px `#e74c3c`) segment x 460–580 marked lost; bold 12px orange label "end = 16:30 → block cut short"; row label 12px `#444` "buggy" at x=20.
- **Annotation (bold 13px `#e74c3c`, near x=470, y=178, pointing at the red segment):** "16:30–17:00 lost — room shown free during a meeting".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red only marks the genuine error states (double counting in c3, the lost half hour in c4).
- **Data:** all bookings, merged blocks, pixel spans, and hour totals are the hardcoded literals above (no randomness). Clock-to-pixel mapping: c1/c2 use x = 100 + (hour − 8) × 60; c3 uses x = 150 + hours × 70; c4 uses x = 100 + (hour − 15) × 240. Text totals (7.0 h naive, 5.5 h merged, blocks 9:00–11:00 / 13:00–15:00 / 15:30–17:00) must match the chart bars exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
