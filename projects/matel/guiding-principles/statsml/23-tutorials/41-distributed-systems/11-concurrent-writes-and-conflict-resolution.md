# Concurrent Writes & Conflict Resolution

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Concurrent Writes & Conflict Resolution

**Subtitle:** When two copies of the same record are edited at the same time with no way to coordinate, the system must decide whose write survives — and every strategy has a cost

## Two Devices, One Contact, No Signal

**Tags:** `core idea` (blue), `offline edits` (green), `version vectors` (orange)

- **The contact** — Maya's phone number lives on both your phone and your laptop, kept in sync
- **The gap** — at 2:00pm both devices go offline (a flight); each keeps its own local copy
- **Edit one** — at 2:01pm the phone changes the number to 555-0142; its version vector becomes {P:1, L:0}
- **Edit two** — at 2:04pm the laptop changes it to 555-0177; its version vector becomes {P:0, L:1}
- **The sync** — at 2:10pm both reconnect; neither vector contains the other, so neither edit "came after"
- **The verdict** — the writes are concurrent: no order exists, and the system must resolve a conflict

*Example (italic):* At sync time the server compares {P:1, L:0} against {P:0, L:1} — each side has one edit the other never saw, so this is a genuine conflict, not a late arrival.

**Key point:** A conflict is not two writes close in time — it is two writes where neither one saw the other. Version vectors detect exactly this: if neither vector dominates, the writes are concurrent.

### Visualization (canvas `c1`, 720×300)

Two-lane timeline: phone lane and laptop lane, an offline window, one edit box per lane with its version vector, and a sync point where the vectors collide.

- **Title (bold 15px, `#1a5276`, top center):** "2:01 and 2:04 — Neither Edit Saw the Other".
- **Geometry:** time axis 2:00 to 2:12 mapped to x=60..660 (minute m at x = 60 + m×50); 2px `#999` baseline at y=260 with 12px `#444` tick labels at 2:00 / 2:04 / 2:08 / 2:12; phone lane center y=110, laptop lane center y=190, lane labels 12px `#444` at x=20.
- **Offline window:** light grid-gray band `rgba(229,233,239,0.6)` from x=60 (2:00) to x=560 (2:10), 12px `#6b7280` label "offline" at its top center (y=60).
- **Phone edit box:** blue `#2a78d6` rounded box (150×40, 8px radius, fill `rgba(42,120,214,0.15)`) centered at x=110, y=110, 12px `#2c3e50` text "555-0142" with 11px `#2a78d6` sub-label "{P:1, L:0}".
- **Laptop edit box:** aqua `#199e70` rounded box (same style, fill `rgba(25,158,112,0.15)`) centered at x=260, y=190, text "555-0177", sub-label "{P:0, L:1}" (11px `#199e70`).
- **Sync point:** vertical dashed `#6b7280` (dash 4/3) line at x=560 (2:10), 3px arrows from both boxes converging on an ink `#1a5276` diamond at (560, 150), bold 12px orange `#d95926` label "conflict: neither vector dominates" to its right.
- **Annotation (bold 13px violet `#4a3aa7`, near x=300, y=250):** "concurrent ≠ simultaneous — it means neither saw the other".
- **Caption (12px `#444`, bottom right):** "times and vectors illustrative".

## Three Ways to Resolve the Same Conflict

**Tags:** `worked example` (blue), `LWW` (orange), `siblings` (green), `merge` (green)

- **Last-write-wins** — compare timestamps: 2:04 beats 2:01, keep 555-0177, silently delete 555-0142
- **The cost** — the phone's write vanishes with no error, no log entry, no way to notice it existed
- **Keep both siblings** — store [555-0142, 555-0177] side by side and hand the pair to the application
- **The cost** — the app must show "two numbers found — which is right?" and let the user pick
- **Merge** — a single phone field can't merge, but the contact's tags set can: {work} ∪ {gym} = {work, gym}
- **The rule** — merge only works when the type has a natural combine step (sets union, counters add)

*Example (italic):* The same 2:01-vs-2:04 conflict yields three different databases: LWW keeps one value, siblings keeps two values, and the tags set merges into one value containing both edits.

**Key point:** There is no free resolution: LWW is simple but lossy, siblings are lossless but push work to the app, and merge is automatic but only for data types built to combine.

### Visualization (canvas `c2`, 720×300)

Three-row flow diagram: the same two concurrent inputs on the left, one strategy per row, and each strategy's surviving output on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Conflict, Three Outcomes".
- **Rows (centers at y = 90, 170, 250), each with a left 12px `#444` strategy label at x=20:** "last-write-wins", "keep siblings", "merge (tags set)".
- **Input boxes (every row):** blue `#2a78d6` box "555-0142 @2:01" at x=170 and aqua `#199e70` box "555-0177 @2:04" at x=330 (rows 1–2); row 3 inputs read "+work @2:01" (blue) and "+gym @2:04" (aqua). Box style: 140×34, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(25,158,112,0.15)`, 12px `#2c3e50` text.
- **Row 1 output:** 3px arrow to an aqua box "555-0177" at x=540; below the dropped input a bold 12px red `#e74c3c` strike label "✗ 555-0142 deleted silently".
- **Row 2 output:** 3px arrow to a yellow `#c98500` box (fill `rgba(201,133,0,0.15)`) at x=540 labeled "[0142, 0177] → ask app", 11px `#c98500` sub-label "2 siblings stored".
- **Row 3 output:** 3px arrow to a green `#008300` box (fill `rgba(0,131,0,0.12)`) at x=540 labeled "{work, gym}" with bold 12px green "✓ both edits kept".
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "only the mergeable type keeps both writes with zero questions asked".
- **Caption (12px `#444`, bottom right):** "values illustrative".

## Shopping Carts, Shared Docs, and CRDTs

**Tags:** `where it's used` (blue), `shopping carts` (green), `CRDTs` (orange)

- **Carts** — replica A of a cart holds {book, mug}, replica B holds {book, lamp} after a network split
- **LWW cart** — picking one whole replica keeps 2 items and silently drops the mug (or the lamp)
- **Set-union cart** — merging as a set keeps all 3 items; losing a customer's added item costs real money
- **Shared docs** — two people typing in one paragraph is thousands of tiny concurrent writes per hour
- **CRDT direction** — data types (sets, counters, lists) designed so any merge order converges to the same value
- **The trade** — CRDTs make merge automatic, but deletes get tricky (removing an item needs a tombstone)

*Example (italic):* After the split heals, the union cart shows book, mug, and lamp (3 items); the LWW cart shows 2 items and the customer never learns the mug is gone.

**Key point:** Systems that must accept writes on both sides of a network split — carts, docs, offline apps — either merge by type (the CRDT route) or surface siblings; picking a winner throws away a customer's action.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: cart size (items) for each replica before sync and for each resolution strategy after sync, with per-item segment labels.

- **Title (bold 15px, `#1a5276`, top center):** "The Split Cart: Union Keeps 3 Items, LWW Keeps 2".
- **Axis:** bars start at x=230, item unit width 130px (so 2 items = 260px, 3 items = 390px), 2px `#999` vertical baseline at x=230; left-aligned 12px `#444` row labels at x=20.
- **Rows (bar centers at y = 70, 115, 180, 225), bars 26px tall:**
  - "replica A (before)": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, 2 segments labeled "book", "mug" (12px `#2c3e50` inside each 130px segment)
  - "replica B (before)": aqua `#199e70` fill `rgba(25,158,112,0.28)`, 2 segments "book", "lamp"
  - "LWW result": orange `#d95926` fill `rgba(217,89,38,0.25)`, 2 segments "book", "lamp", plus a dashed red `#e74c3c` empty outline segment (130px) after them labeled 12px red "mug lost"
  - "set-union result": green `#008300` fill `rgba(0,131,0,0.22)`, 3 segments "book", "mug", "lamp", bold 12px green "✓ 3 items" at the bar end
- **Segment style:** 1px white gaps between segments; thin 1px solid border in each row's line color.
- **Annotation (bold 13px violet `#4a3aa7`, near x=280, y=270):** "a dropped cart item is a silent lost sale".
- **Caption (12px `#444`, bottom right):** "cart contents illustrative".

## Last-Write-Wins Loses Data by Design

**Tags:** `common mistake` (red), `clock skew` (orange), `LWW` (orange)

- **The comfort** — LWW sounds harmless ("the newest edit wins") so it becomes the unexamined default
- **The truth** — on every concurrent conflict LWW deletes one acknowledged write; that is its definition
- **Clock skew** — "last" is judged by device clocks, and device clocks disagree by seconds to minutes
- **The swap** — the phone's clock runs 4 minutes fast: its 2:01pm edit gets stamped 2:05pm
- **Wrong winner** — the laptop's genuinely later 2:04pm edit (stamped 2:04) loses to the older phone edit
- **The tell** — users report "my change disappeared" and no error or log entry exists anywhere

*Example (italic):* True order: phone edits at 2:01, laptop edits at 2:04 — but the fast phone clock stamps 2:05, so LWW keeps the older number and silently discards the newer one.

**Common mistake:** Treating LWW as a safe default. It is a policy of deliberate data loss, and with skewed clocks it doesn't even lose the *older* write — it loses whichever write got the unluckier timestamp.

### Visualization (canvas `c4`, 720×300)

Two-lane timeline plotting each edit at its TRUE time, with its stamped time printed beside it, and an arrow showing LWW crowning the wrong winner.

- **Title (bold 15px, `#1a5276`, top center):** "A 4-Minute-Fast Clock Makes the Older Edit Win".
- **Geometry:** true-time axis 2:00 to 2:08 mapped to x=60..660 (minute m at x = 60 + m×75), 2px `#999` baseline at y=250 with 12px `#444` tick labels every 2 minutes; phone lane center y=105, laptop lane center y=185, lane labels 12px `#444` at x=20 ("phone (clock +4 min)", "laptop (clock correct)").
- **Phone edit box:** blue `#2a78d6` rounded box (170×40, 8px radius, fill `rgba(42,120,214,0.15)`) centered at x=135 (true 2:01), 12px `#2c3e50` text "edit @ true 2:01", bold 12px orange `#d95926` sub-label "stamped 2:05".
- **Laptop edit box:** aqua `#199e70` rounded box (same style, fill `rgba(25,158,112,0.15)`) centered at x=360 (true 2:04), text "edit @ true 2:04", 12px `#199e70` sub-label "stamped 2:04".
- **LWW verdict:** 3px red `#e74c3c` arrow from a bold 12px `#444` label "LWW compares stamps: 2:05 > 2:04" at (x≈520, y=60) down to the phone box; bold 12px red "✗ newer laptop edit deleted" under the laptop box.
- **Skew marker:** dashed `#6b7280` (dash 4/3) horizontal bracket from x=135 to x=435 at y=70 labeled 11px `#6b7280` "+4 min skew shifts the stamp past 2:04".
- **Annotation (bold 13px red `#e74c3c`, near x=200, y=250):** "with skewed clocks, 'last' write isn't even the last one".
- **Caption (12px `#444`, bottom right):** "times and skew illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness) — edit times 2:01 / 2:04, sync 2:10, version vectors {P:1, L:0} / {P:0, L:1}, phone numbers 555-0142 / 555-0177, tag sets {work} ∪ {gym}, cart contents {book, mug} / {book, lamp} with LWW=2 items vs union=3 items, and the 4-minute clock skew stamping 2:01 as 2:05; all times, numbers, and cart contents are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
