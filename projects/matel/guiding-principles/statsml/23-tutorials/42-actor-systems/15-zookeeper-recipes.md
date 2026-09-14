# ZooKeeper Recipes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** ZooKeeper Recipes

**Subtitle:** Two small znode flags — auto-numbered and vanish-with-session — snap together into distributed locks, leader election, and group membership

## Five Workers, One 2am Report

**Tags:** `core idea` (blue), `two primitives` (green), `ZooKeeper` (orange)

- **The fleet** — five workers (w1–w5) all boot at 2am; exactly one must build the nightly report
- **The tree** — ZooKeeper keeps tiny named nodes (znodes) in a filesystem-like tree, e.g. `/jobs/report`
- **Sequential** — a create with the SEQUENTIAL flag gets a server-assigned counter suffix: `lock-0000000041`
- **Ephemeral** — an EPHEMERAL znode is tied to its creator's session; if heartbeats stop, it is deleted
- **The combo** — ephemeral + sequential = a numbered ticket that self-destructs when its holder dies

*Example (italic):* w3 connects and creates an ephemeral sequential znode; the server names it lock-0000000041 — if w3 crashes, the znode vanishes when its 10-second session times out.

**Key point:** A znode is just a small named node in a shared tree; the ephemeral and sequential flags are the only two primitives every recipe on this page is built from.

### Visualization (canvas `c1`, 720×300)

Two-panel diagram: left panel shows the SEQUENTIAL flag (three creates get server-assigned numbers 41, 42, 43), right panel shows the EPHEMERAL flag (session dies, znode auto-deleted).

- **Title (bold 15px, `#1a5276`, top center):** "Two Znode Flags: Sequential Numbers It, Ephemeral Ties It to a Session".
- **Divider:** vertical 1px `#e5e9ef` line at x=360 from y=45 to y=275.
- **Left panel header (bold 13px `#2a78d6`, centered at x=180, y=60):** "SEQUENTIAL".
- **Left rows at y = 95, 145, 195:** 12px `#444` label at x=25 ("w3 create()", "w1 create()", "w5 create()"), 2px `#6b7280` arrow to a blue rounded box (150×32, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text) at x=170 labeled "lock-0000000041" / "…42" / "…43".
- **Left annotation (bold 12px violet `#4a3aa7`, centered at x=180, y=250):** "the server assigns the next number".
- **Right panel header (bold 13px `#d95926`, centered at x=540, y=60):** "EPHEMERAL".
- **Right session box:** green rounded box (180×36, fill `rgba(0,131,0,0.12)`) at x=450, y=85, 12px text "w3 session (10s timeout)"; bold 14px red `#e74c3c` "✗" at its right edge with 12px red label "heartbeats stop" beside it.
- **Right znode box:** blue rounded box (180×36, fill `rgba(42,120,214,0.15)`) at x=450, y=155, 12px text "lock-0000000041"; dashed 2px `#6b7280` connector (dash 4/3) from session box to znode box, 11px mute label "owns".
- **Right result:** dashed grey rounded box (180×36, 1.5px `#6b7280` dashed border, no fill) at x=450, y=225, 12px `#6b7280` text "znode auto-deleted"; 2px red arrow from znode box down to it.
- **Caption (12px `#444`, bottom right):** "sequence numbers and timeout illustrative".

## The Lock: Lowest Ticket Wins, Watch the One Ahead

**Tags:** `worked example` (blue), `distributed lock` (green)

- **Take a ticket** — each worker creates an ephemeral sequential znode under `/jobs/report`: 41–45
- **Lowest wins** — w3 holds …41, the smallest number, so w3 holds the lock and builds the report
- **Queue up** — every waiter watches the znode one place ahead of its own, not the holder's
- **The crash** — at 2:07am w3 dies mid-job; its session times out and znode …41 is auto-deleted
- **The handoff** — only w1 (watching …41) is notified; it re-lists, sees …42 is lowest, takes the lock
- **No janitor** — nobody had to clean up w3's stale lock; ephemerality did it automatically

*Example (italic):* Tickets 41–45 belong to w3, w1, w5, w2, w4; when w3's znode vanishes at 2:07am, exactly one watch fires and w1 becomes the new holder.

**Key point:** Sequence numbers give a total order (who holds, who is next); ephemerality guarantees a dead holder releases automatically; watching only your predecessor means one wake-up per release.

### Visualization (canvas `c2`, 720×300)

Chain diagram: five ticket boxes in queue order, watch arrows pointing at each predecessor, the holder crashing, and the single notification that results.

- **Title (bold 15px, `#1a5276`, top center):** "The Lock Queue at 2:07am: One Crash, One Notification".
- **Ticket boxes (110×46, 8px radius, at y=115, x = 40, 175, 310, 445, 580):** two-line 12px `#2c3e50` text "w3 / …41", "w1 / …42", "w5 / …43", "w2 / …44", "w4 / …45"; the w3 box has green fill `rgba(0,131,0,0.12)` with bold 11px green `#008300` "HOLDER" above it; the other four have blue fill `rgba(42,120,214,0.15)`.
- **Watch arrows:** 2px `#6b7280` arrows from each waiter box to the box on its left (4 arrows), 11px `#6b7280` label "watch" above the w1→w3 arrow only.
- **Crash marker:** bold 16px red `#e74c3c` "✗" over the w3 box, bold 12px red label at y=75 near x=40: "session expires — znode …41 deleted".
- **Notification:** 3px orange `#d95926` arrow from the w3 box to the w1 box, bold 12px orange label under it at y=185: "watch fires — only w1 wakes".
- **Result box:** green rounded box (250×34, fill `rgba(0,131,0,0.12)`) centered at y=215, 12px text "w1 re-lists: …42 is lowest → holds lock".
- **Annotation (bold 13px green `#008300`, centered at y=268):** "w5, w2, w4 sleep through the whole handoff".
- **Caption (12px `#444`, bottom right):** "ticket numbers illustrative".

## The Same Trick Elects Leaders and Counts Heads

**Tags:** `where it's used` (blue), `election` (green), `membership` (orange)

- **Leader election** — the identical chain under `/election`; the lowest znode is leader, not lock holder
- **Failover** — when the leader's ephemeral znode vanishes, the next number is already leader-elect
- **Membership** — each worker keeps one ephemeral child under `/workers`; listing children = live roster
- **Barrier** — workers add children under `/barrier`; a watch on the parent opens it at the 5th arrival
- **In the wild** — Kafka, HBase, and HDFS have all used ZooKeeper for exactly these recipes

*Example (italic):* The same five 41–45 tickets placed under /election make w3 the leader at 2:00am and w1 the leader at 2:07am — election is the lock recipe under a different name.

**Key point:** Locks, elections, rosters, and barriers are not separate ZooKeeper features — they are the same create / list / watch moves over ephemeral and sequential znodes.

### Visualization (canvas `c3`, 720×300)

Three-row diagram mapping each recipe to its znode construction, showing that all three reuse the same two flags.

- **Title (bold 15px, `#1a5276`, top center):** "Three Recipes, Same Two Primitives".
- **Rows at y = 85, 155, 225; each row:** bold 12px `#1a5276` recipe label at x=20, znode sketch starting at x=170, one-line 12px `#444` rule text at x=430.
- **Row 1 "lock":** three small blue boxes (60×28, fill `rgba(42,120,214,0.15)`, 11px text) "…41", "…42", "…43" in a row with 1.5px `#6b7280` predecessor arrows; the "…41" box outlined 2px green `#008300`; rule text "lowest holds it; watch your predecessor".
- **Row 2 "leader election":** the same three-box chain, but the green-outlined "…41" box carries a bold 11px green label "LEADER" above it; rule text "lowest is leader; next number is heir".
- **Row 3 "membership / barrier":** one blue parent box (90×28) labeled "/workers" with five small green dots (6px radius, `#008300`) fanned to its right, 11px `#6b7280` label "5 ephemeral children"; rule text "list children = live roster; 5th arrival opens the barrier".
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=278):** "every recipe is create + list + watch — nothing else".

## Everyone Watching the Holder: the Herd Effect

**Tags:** `common mistake` (red), `herd effect` (orange)

- **The naive lock** — all waiters set a watch on the holder's znode …41 and wait for it to vanish
- **The stampede** — one delete fires N−1 watches; 499 waiters in a 500-fleet hit the server at once
- **The fix** — watch your predecessor: each release wakes exactly one client, whatever the fleet size
- **One-shot watches** — a ZooKeeper watch fires once; after it fires you must re-list and re-watch
- **Mid-queue deaths** — a fired watch means your predecessor died, not that you hold the lock

*Example (italic):* In a 500-worker fleet, herd-style watching wakes 499 clients on every release; predecessor-watching wakes 1 — and if w5 (…43) dies mid-queue, w2's watch fires but w1 still holds the lock.

**Common mistake:** Assuming "my watch fired, so I have the lock." A fired watch only means your predecessor's znode vanished — re-list the children, confirm you are lowest, and if not, set a watch on the new node just ahead of you.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: clients woken per lock release, herd style (everyone watches the holder) vs chain style (watch your predecessor), at three fleet sizes.

- **Title (bold 15px, `#1a5276`, top center):** "One Lock Release: How Many Waiters Wake Up?".
- **Layout:** row labels 12px `#444` at x=20; bars start at x=230, max width 440; each row has a red herd bar above a green chain bar, both 14px tall with 6px gap, 11px count labels at bar ends.
- **Rows (top of pair at y = 70, 135, 200):**
  - "fleet of 5": red `#e74c3c` bar width 90 labeled "4 wake", green `#008300` bar width 12 labeled "1 wakes"
  - "fleet of 50": red bar width 240 labeled "49 wake", green bar width 12 labeled "1 wakes"
  - "fleet of 500": red bar width 440 labeled "499 wake", green bar width 12 labeled "1 wakes"
- **Legend (11px, top right under title):** red swatch "watch the holder (herd)", green swatch "watch your predecessor".
- **Annotation (bold 13px red `#e74c3c`, centered near y=262):** "herd cost grows with the fleet; chain cost stays at 1".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic, wake counts exact (N−1 vs 1)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all geometry and values are the hardcoded numbers above (no randomness); sequence numbers 41–45, worker names, timestamps, and the 10-second session timeout are invented and labeled illustrative; wake counts (4 / 49 / 499 vs 1) follow exactly from N−1 vs 1 and match the text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
