# Offline-First & Sync

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Offline-First & Sync

**Subtitle:** An offline-first app treats the phone's local database as the real one — every edit saves instantly on the device, and a sync engine reconciles with the server whenever a connection appears

## The Sales Rep at 30,000 Feet

**Tags:** `core idea` (blue), `local writes` (green), `background sync` (orange)

- **The rep** — a field-sales rep boards a 9:00am flight with the customer app open and no wifi
- **The edits** — between 9:20 and 11:45 she updates 6 customer records: prices, notes, a new contact
- **The local save** — each edit writes to the phone's own database instantly; the app never says "no connection"
- **The queue** — behind the scenes, all 6 edits wait in an outbox queue, oldest first
- **The sync** — at 12:05, five minutes after landing, the phone finds signal and pushes all 6 in one burst

*Example (italic):* She lands at 12:00, and by 12:06 all 6 edits made mid-flight are on the server — she never saw a spinner or an error.

**Key point:** Offline-first means the local database is the source the app reads and writes; the network is just a background courier that catches up when it can.

### Visualization (canvas `c1`, 720×300)

Step chart of the outbox queue during the flight: queued edits climb from 0 to 6 while offline, then drain to 0 in one burst after landing.

- **Title (bold 15px, `#1a5276`, top center):** "One Flight, Six Edits: the Outbox Fills Offline, Drains on Landing".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "9:00" to "12:30" with 12px `#444` tick labels every 30 min; y = queued edits 0 to 6, gridlines `#e5e9ef` at 2/4/6.
- **Queue step line:** blue `#2a78d6` 3px step line; steps up by 1 at minutes-after-9:00 `[20, 50, 75, 100, 130, 165]` (i.e. 9:20, 9:50, 10:15, 10:40, 11:10, 11:45), holding values `[1, 2, 3, 4, 5, 6]`; vertical drop from 6 to 0 at minute 185 (12:05); light blue fill `rgba(42,120,214,0.25)` under the steps.
- **Offline band:** shaded `rgba(107,114,128,0.10)` rectangle from 9:00 to 12:05 with 12px `#6b7280` label "no connection" near its top left.
- **Sync marker:** vertical dashed `#008300` (dash 4/3) line at 12:05, bold 12px green `#008300` label "signal found — 6 edits pushed" beside it.
- **Annotation (bold 13px blue `#2a78d6`, near 10:30, y=80):** "every edit saved locally the instant she typed it".
- **Caption (12px `#444`, bottom right):** "edit times illustrative".

## Two Phones, One Customer Note

**Tags:** `worked example` (blue), `colliding edits` (green), `conflict` (red)

- **The setup** — rep A (tablet) and rep B (phone) both go offline holding the same customer note
- **A edits** — at 10:12 A changes the note to "Prefers email"; it sits in A's outbox
- **B edits** — at 10:47 B changes the same note to "Budget approved"; it sits in B's outbox
- **B syncs first** — B gets signal at 11:30, so the server note becomes "Budget approved"
- **A syncs later** — A lands at 12:05; last-write-wins by arrival overwrites the note to "Prefers email"
- **The merge instead** — a merging sync keeps both edits as two lines and flags the note for review

*Example (italic):* Under last-write-wins, B's 10:47 edit "Budget approved" vanishes at 12:05 with no error; under merge, the note reads both lines.

**Key point:** When two offline devices edit the same record, sync order — not edit order — decides the winner under naive last-write-wins; a merge policy keeps both edits.

### Visualization (canvas `c2`, 720×300)

Three-lane timeline (Device A, Device B, Server) with the four events as dots, arrows showing each sync landing on the server lane, and two outcome boxes comparing last-write-wins vs merge.

- **Title (bold 15px, `#1a5276`, top center):** "Same Note, Two Offline Edits: Sync Order Picks the Winner".
- **Lanes:** three horizontal 2px `#e5e9ef` lines at y = 90 (Device A), 150 (Device B), 210 (Server), each with a left 12px bold `#2c3e50` label at x=20; time axis x=110 to 640 spanning 10:00–12:15 with 12px `#444` tick labels at 10:00 / 10:30 / 11:00 / 11:30 / 12:00.
- **Edit dots:** blue `#2a78d6` 7px dot on lane A at 10:12 labeled 12px "edit: Prefers email"; aqua `#199e70` 7px dot on lane B at 10:47 labeled "edit: Budget approved".
- **Sync arrows:** 2px aqua `#199e70` arrow from lane B at 11:30 down to the server lane, 12px label "B syncs 11:30"; 2px blue `#2a78d6` arrow from lane A at 12:05 down to the server lane, 12px label "A syncs 12:05".
- **Server states:** 12px `#2c3e50` text under the server lane: "Budget approved" after 11:30, then at 12:05 a red `#e74c3c` strike-through "~~Budget approved~~" with bold 12px red "→ Prefers email (LWW)".
- **Outcome boxes (bottom, y≈255):** rounded box at x=110 fill `rgba(231,76,60,0.12)` labeled 12px "last-write-wins: 1 edit lost"; rounded box at x=400 fill `rgba(0,131,0,0.12)` labeled 12px "merge: both lines kept, 0 lost".
- **Annotation (bold 13px red `#e74c3c`, near 11:50, y=60):** "A edited first but synced last — and still wins".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative".

## Where the Sync Engine Hides

**Tags:** `where it's used` (blue), `field work` (green), `collaboration` (orange)

- **Notes apps** — your phone's notes save instantly in a tunnel and reconcile with the cloud later
- **Field work** — sales, delivery, and home-visit apps assume dead zones as the normal case
- **Collaborative tools** — shared-document editors are sync engines with a text box on top
- **The dead-zone math** — a field rep can spend a third of the workday without reliable signal
- **Without it** — an online-only app turns every dead zone into lost work and retyped forms

*Example (italic):* A home-visit nurse charts 9 patient visits a day in basements and elevators — an online-only form would fail on roughly a third of them.

**Key point:** Any app used where connectivity is unreliable — which is most places phones go — is secretly a sync engine with a UI attached.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: share of the workday spent without reliable connectivity, by role, showing why field roles force offline-first design.

- **Title (bold 15px, `#1a5276`, top center):** "Share of the Workday Without Reliable Signal, by Role".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 420 = 50%; 12px `#444` scale labels "0%", "25%", "50%" at x = 230 / 440 / 650 along y=255, light `#e5e9ef` gridlines at those x positions.
- **Rows (top to bottom at y = 75, 120, 165, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "Delivery driver — 45%": orange `#d95926` bar width 378
  - "Field-sales rep — 35%": blue `#2a78d6` bar width 294
  - "Home-visit nurse — 30%": aqua `#199e70` bar width 252
  - "Office worker — 5%": mute `#6b7280` bar width 42
- **Bar style:** 22px tall, 3px radius, solid fills, bold 12px matching-color percentage label at each bar's right end.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=250):** "field roles live in dead zones — the app must not care".
- **Caption (12px `#444`, bottom right):** "percentages illustrative".

## The Silent Overwrite

**Tags:** `common mistake` (red), `data loss` (orange)

- **Mistake one** — assuming connectivity: the app that shows a spinner in a dead zone loses the sale note
- **Mistake two** — naive last-write-wins: collisions resolve silently, so nobody knows an edit died
- **The scale** — a 12-rep team logging 480 edits in a week can collide on 34 of them
- **The silence** — under last-write-wins all 34 losers vanish with no error, no log, no review queue
- **The fix** — merge non-overlapping fields automatically and flag true conflicts (6 of the 34) to a human

*Example (italic):* Of 480 edits in a week, 34 collide; last-write-wins silently discards 34, while field-level merge loses 0 and asks a human about only 6.

**Common mistake:** Treating sync conflicts as rare enough to ignore. Last-write-wins is not a conflict resolver — it is a data deleter that never files a report.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart on one week of team edits: total edits, colliding edits, then edits lost under last-write-wins vs under field-level merge.

- **Title (bold 15px, `#1a5276`, top center):** "One Week, 12 Reps: What Each Conflict Policy Loses".
- **Axes:** origin x=80, baseline y=245, plot width 580, plot height 180; y = edits 0 to 480, gridlines `#e5e9ef` at 120/240/360/480 with 12px `#444` labels.
- **Bars (60px wide, centered at x = 160, 300, 440, 580), values `[480, 34, 34, 0]`, each with a bold 13px value label on top and a 12px `#444` category label below the baseline:**
  - "total edits": blue `rgba(42,120,214,0.35)` with 2px `#2a78d6` border, height 180
  - "collisions": yellow `#c98500` solid, height 13
  - "lost — last-write-wins": red `#e74c3c` solid, height 13
  - "lost — merge": green `#008300` 2px-border empty bar of height 3 drawn at the baseline, value label "0", plus 12px green note "6 flagged for review" above it
- **Annotation (bold 13px red `#e74c3c`, near x=440, y=90):** "34 edits deleted — and no one was told".
- **Caption (12px `#444`, bottom right):** "edit and collision counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the flight-edit times (9:20–11:45), the conflict timestamps (A edits 10:12 / B edits 10:47 / B syncs 11:30 / A syncs 12:05), the dead-zone percentages (45 / 35 / 30 / 5), and the weekly counts (480 edits / 34 collisions / 34 lost under LWW / 0 lost under merge, 6 flagged) are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
