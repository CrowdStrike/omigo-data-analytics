# Copy-on-Write

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Copy-on-Write

**Subtitle:** When two owners want the same data, don't copy it — share one original and copy only at the moment somebody writes

## Two Roommates, One Recipe Binder

**Tags:** `core idea` (blue), `sharing` (green), `lazy copy` (orange)

- **The binder** — two roommates both "have" the family recipe binder: 300 pages sitting on one shelf
- **No duplication** — nobody photocopies 300 pages up front; both just point at the same binder
- **The write** — one roommate wants to scribble on the lasagna page; only THAT page gets photocopied
- **The split** — after the scribble there are 301 physical pages: 299 shared, 2 private lasagna versions
- **The name** — copy-on-write (COW): share everything until a write forces a copy of just that piece

*Example (italic):* Both roommates read for months without a single copy made; the first edit costs one photocopied page, not 300.

**Key point:** Copy-on-write makes copies look instant and free by deferring the real work — physical copying happens per piece, only when a piece is first written.

### Visualization (canvas `c1`, 720×300)

Diagram: two owner boxes pointing at one shared page stack; after a write, one page splits into two private versions while the rest stays shared.

- **Title (bold 15px, `#1a5276`, top center):** "Share 300 Pages, Copy Only the One You Write".
- **Left panel (before, centered x≈200):** two blue `#2a78d6` rounded boxes at (140, 70) and (260, 70) labeled "owner A" / "owner B" (12px), arrows down to one stack of 3 offset gray-bordered rectangles at (170–230, 140–210) labeled "300 shared pages" (12px `#444`); 12px `#6b7280` caption below: "before: 300 physical pages".
- **Right panel (after, centered x≈520):** same two owner boxes at (460, 70) / (580, 70); shared stack at center labeled "299 shared"; owner A also points to a green `#008300` page at (445, 190) labeled "lasagna (A's copy)"; owner B points to an orange `#d95926` page at (595, 190) labeled "lasagna (B kept original)"; 12px `#6b7280` caption: "after one write: 301 physical pages".
- **Divider:** vertical dashed `#e5e9ef` line at x=360; bold 13px violet `#4a3aa7` label at top center of right panel: "write → copy that page only".
- **Arrow style:** 2px `#6b7280` lines with small arrowheads.
- **Caption (12px `#444`, bottom right):** "page counts illustrative".

## Counting the Pages by Hand

**Tags:** `worked example` (blue), `memory math` (green)

- **Setup** — a 4,000-page dataset is "copied" to a second owner: physical pages stay 4,000
- **Writes land** — the new owner edits 120 different pages over the next hour
- **The rule** — each first-write to a page adds exactly one physical page
- **Hand-check** — physical pages = 4,000 + 120 = 4,120, not the 8,000 an eager copy would need
- **The savings** — 8,000 − 4,120 = 3,880 pages never copied because nobody ever wrote them

*Example (italic):* An eager copy costs 4,000 pages on day one; COW costs 120 pages spread over the hour writes actually happen — 97% of the copy never occurs.

**Key point:** After the writes, memory = original + pages actually written (4,000 + 120 = 4,120) — the unwritten 3,880 pages are shared forever at zero cost.

### Visualization (canvas `c2`, 720×300)

Line chart of physical pages over the hour: eager copy jumps to 8,000 at t=0; COW starts at 4,000 and creeps up to 4,120 as writes land.

- **Title (bold 15px, `#1a5276`, top center):** "Eager Copy vs COW: 8,000 Pages vs 4,120".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = minutes 0 to 60, tick labels "0", "15", "30", "45", "60 min" (12px `#444`); y = physical pages 0 to 9000, gridlines `#e5e9ef` at 2000/4000/6000/8000 with 12px labels.
- **Eager line:** red `#e74c3c` 3px line: starts at (0, 4000), vertical step to 8000 at minute 0, then flat through points at minutes `[0, 15, 30, 45, 60]`, pages `[8000, 8000, 8000, 8000, 8000]`; 12px red label "eager copy: 8,000" above the line near x≈40.
- **COW line:** green `#008300` 3px line through minutes `[0, 15, 30, 45, 60]`, pages `[4000, 4045, 4080, 4105, 4120]`; fill under `rgba(0,131,0,0.10)`; bold 12px green label "COW: 4,120" at the line's right end.
- **Gap marker:** vertical double-headed 2px `#6b7280` arrow at x=60min between the two lines, bold 12px violet `#4a3aa7` label "3,880 pages never copied".
- **Caption (12px `#444`, bottom right):** "write pattern illustrative — 120 distinct pages written".

## Where the Trick Runs Your Machine

**Tags:** `where it's used` (blue), `fork` (green), `snapshots` (orange)

- **fork()** — when a process forks, the child shares all parent memory COW; a fork is near-instant
- **Snapshots** — ZFS, APFS, and Btrfs snapshot terabytes in milliseconds by sharing blocks COW
- **Strings & arrays** — Swift arrays and PHP strings copy-on-write behind ordinary assignment
- **Redis saves** — Redis forks to snapshot; the child reads frozen COW pages while the parent keeps serving
- **Databases** — MVCC storage keeps old row versions readable while writers make new copies

*Example (italic):* A 30 GB Redis process forks in a few milliseconds — the snapshot child shares all 30 GB and pays only for pages the parent rewrites during the save.

**Key point:** COW is why fork, filesystem snapshots, and "copying" a big value in Swift all feel instant — the copy is a promise, paid page by page only if writes arrive.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: apparent copy size vs physical bytes actually copied for four real uses.

- **Title (bold 15px, `#1a5276`, top center):** "What the Copy Claims vs What It Physically Copies".
- **Axis:** baseline at x=250, bars extend right, max width 420; 12px `#444` row labels at x=20.
- **Rows (top to bottom at y = 70, 120, 170, 220):**
  - "fork a 30 GB process": blue `rgba(42,120,214,0.30)` bar width 420 labeled "30 GB apparent"; overlay green `#008300` bar width 8 labeled "~0.6 GB rewritten"
  - "snapshot a 2 TB volume": blue bar width 420 "2 TB apparent"; overlay green bar width 4 "metadata only"
  - "assign a Swift array (1M items)": blue bar width 420 "1M items apparent"; overlay green bar width 2 "8 bytes (a pointer)"
  - "array then mutated": blue bar width 420; overlay orange `#d95926` bar width 420 with 12px orange label "full copy now — write triggered it"
- **Bar style:** 14px tall, 11px width labels at bar ends.
- **Annotation (bold 13px magenta `#d55181`, near y=255, centered):** "row 4 is the fine print: the first write pays the whole bill".
- **Caption (12px `#444`, bottom right):** "sizes illustrative".

## The Latency Hiding in the First Write

**Tags:** `common mistake` (red), `performance cliff` (orange)

- **The illusion** — benchmarks of "copying" look free because nobody wrote afterward
- **The cliff** — the first write to a shared page stalls while the real copy happens right then
- **Redis pain** — a write-heavy minute during a snapshot can double memory: every hot page copies
- **Fork bombs** — forking a huge process is cheap until both sides write everywhere at once
- **The mistake** — sizing memory for the shared state and forgetting writes un-share it

*Example (italic):* A 30 GB Redis under heavy writes during a save copies most hot pages — memory spikes toward 60 GB and the box starts swapping.

**Common mistake:** Treating COW's deferred cost as no cost. The copy still happens — just later, on the write path, exactly when your system is busiest.

### Visualization (canvas `c4`, 720×300)

Line chart: memory of a snapshotting process over 10 minutes under light vs heavy write load, showing the heavy case ballooning toward 2×.

- **Title (bold 15px, `#1a5276`, top center):** "During a COW Snapshot, Writes Un-Share Memory".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = minutes 0 to 10, ticks every 2 (12px `#444`); y = memory GB 0 to 64, gridlines at 16/32/48 with labels "16", "32", "48 GB".
- **Snapshot window:** light gray `rgba(107,114,128,0.10)` band from minute 2 to minute 8, 11px `#6b7280` label "snapshot in progress" at its top.
- **Light writes:** green `#008300` 3px line through minutes `[0, 2, 4, 6, 8, 10]`, GB `[30, 30, 31, 32, 33, 30]`; 12px green label "light writes: +3 GB peak".
- **Heavy writes:** red `#e74c3c` 3px line through the same minutes, GB `[30, 30, 42, 52, 58, 31]`; bold 12px red label "heavy writes: 58 GB peak" near its peak.
- **Limit line:** horizontal dashed `#d95926` (dash 4/3) line at 56 GB, 11px orange label "box RAM 56 GB" at left; the red line visibly crosses it.
- **Annotation (bold 12px orange `#d95926`, near x=6.5, y=55):** "hot pages all copied at once — swap begins".
- **Caption (12px `#444`, bottom right):** "GB values illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all curve points and bar widths are the hardcoded arrays above (no randomness); the 4,000 + 120 = 4,120 page arithmetic in text must match c2's plotted endpoint; all invented sizes carry "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
