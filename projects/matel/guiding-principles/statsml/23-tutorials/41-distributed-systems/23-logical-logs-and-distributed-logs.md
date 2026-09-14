# Logical Logs & Distributed Logs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Logical Logs & Distributed Logs

**Subtitle:** A distributed log is a numbered, append-only list of records that many readers walk at their own pace — the replicated ledger primitive behind Kafka-style systems, change capture, and event sourcing

## The Coffee Shop's Order Ledger

**Tags:** `core idea` (blue), `append-only` (green), `offsets` (orange)

- **The ledger** — a coffee shop app appends every order to one list; nothing is ever edited or erased
- **The number** — each record gets the next offset: order 107 is a flat white, order 108 an oat latte
- **The readers** — a search index, a cache, and a warehouse each read the same list independently
- **The bookmark** — each reader keeps a single number, its offset: how far into the list it has read
- **No coupling** — the writer never waits for readers; a slow warehouse can't slow the cash register

*Example (italic):* At 9:14am the app appends order 108; the cache reads it instantly, the warehouse is still back at 104 — both are correct, just at different bookmarks.

**Key point:** A log is an append-only numbered list, and a reader's entire progress is one integer — its offset. That single primitive is what "distributed log" systems replicate across machines.

### Visualization (canvas `c1`, 720×300)

Row of numbered log records with three reader bookmarks drawn as arrows below, each at a different position in the same list.

- **Title (bold 15px, `#1a5276`, top center):** "One Append-Only Log, Three Independent Bookmarks".
- **Log strip:** 8 boxes 70px wide, 46px tall, 6px gap, starting x=60, top y=80; fills `rgba(42,120,214,0.15)` with 2px `#2a78d6` borders; each box shows the offset (bold 13px `#1a5276`) over the order (11px `#2c3e50`): offsets `[101, 102, 103, 104, 105, 106, 107, 108]`, orders `["latte", "mocha", "drip", "chai", "espresso", "cortado", "flat white", "oat latte"]`.
- **Append arrow:** 3px `#008300` arrow entering the right end of the strip at box 108's edge, bold 12px green `#008300` label "writes append here" above it.
- **Bookmarks (arrows pointing up at a box's bottom edge, 3px, with bold 12px labels below):** aqua `#199e70` at box 104 labeled "warehouse @ 104"; violet `#4a3aa7` at box 106 labeled "search @ 106"; orange `#d95926` at box 108 labeled "cache @ 108"; stagger label rows at y = 200 / 230 / 260 to avoid overlap.
- **Annotation (bold 13px ink `#1a5276`, top left under the title, y=62):** "records are never edited or deleted — readers just move their own bookmark".
- **Caption (12px `#444`, bottom right):** "orders and offsets illustrative".

## Lag Is Just Subtraction: 1042 − 987 = 55

**Tags:** `worked example` (blue), `offset math` (green)

- **The head** — the producer has appended 1042 orders, so the log's head offset is 1042
- **The cache** — its bookmark reads 1042, so lag = 1042 − 1042 = 0: fully caught up
- **The search index** — bookmark 1030, so lag = 1042 − 1030 = 12 records behind
- **The warehouse** — bookmark 987, so lag = 1042 − 987 = 55; monitoring is one subtraction per reader
- **The rewind** — after a bad warehouse deploy, set its bookmark back to 900 and replay 142 records

*Example (italic):* The warehouse fix is moving one integer from 987 back to 900 — the log still holds every record, so reprocessing is just reading forward again.

**Key point:** Progress, lag, and replay are all arithmetic on offsets — no per-message receipts, no acknowledgement lists to reconcile, one number per reader.

### Visualization (canvas `c2`, 720×300)

Horizontal offset number line from 880 to 1060 with the head and three bookmarks marked, lag brackets above, and a rewind arrow below.

- **Title (bold 15px, `#1a5276`, top center):** "Head 1042, Three Bookmarks, Lag by Subtraction".
- **Axis:** 2px `#999` horizontal line at y=160 from x=60 to x=660; linear scale mapping offset 880 → x=60 and offset 1060 → x=660; 12px `#444` tick labels at offsets `[900, 950, 1000, 1050]`.
- **Head marker:** vertical 3px ink `#1a5276` line at offset 1042 from y=110 to y=210, bold 13px ink label "head 1042" above it.
- **Bookmark dots (10px diameter on the axis, bold 12px labels below at y=185):** aqua `#199e70` at 987 labeled "warehouse 987"; violet `#4a3aa7` at 1030 labeled "search 1030"; orange `#d95926` at 1042 labeled "cache 1042" (offset the cache label to y=205 so it clears the head label).
- **Lag brackets (2px lines with end ticks above the axis at y=120, bold 12px labels):** aqua bracket from 987 to 1042 labeled "lag 55"; violet bracket from 1030 to 1042 at y=95 labeled "lag 12"; orange 12px label "lag 0" beside the cache dot.
- **Rewind arrow:** dashed 3px magenta `#d55181` (dash 6/4) arrow below the axis at y=235 from offset 987 back to offset 900, magenta dot at 900, bold 12px magenta label "rewind to 900 — replay 142 records".
- **Annotation (bold 13px green `#008300`, top right, y=60):** "lag = head − bookmark; replay = move the bookmark back".
- **Caption (12px `#444`, bottom right):** "offsets illustrative; lags exact given them".

## The Backbone: CDC, Event Sourcing, Rebuilds

**Tags:** `where it's used` (blue), `Kafka-style` (green), `replay` (orange)

- **Kafka-style systems** — replicate this exact structure across machines so the ledger survives crashes
- **Change data capture** — every database insert and update is appended to a log other systems tail
- **Event sourcing** — the log of events is the truth; current state is just a replay of the log
- **Rebuilds** — a corrupted search index is repaired by resetting its offset to 0 and replaying
- **New consumers** — a fraud model added next year reads the full history without touching writers

*Example (italic):* When the cache box dies, nobody restores a backup — a fresh cache sets its bookmark to 0, replays the order log, and arrives at the same state.

**Key point:** Once the log is the source of truth, every downstream store becomes derived, disposable state — anything can be rebuilt by pointing a bookmark at offset 0.

### Visualization (canvas `c3`, 720×300)

Flow diagram: one producer feeding the replicated log strip, fanning out to three live consumers plus a dashed new consumer starting from offset 0.

- **Title (bold 15px, `#1a5276`, top center):** "One Log In, Any Number of Readers Out".
- **Producer box:** rounded box at x=30, y=125, 130×50, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` label "orders app (writer)".
- **Log strip (center):** at x=210, y=115, five 54px boxes 70px tall in a row, fills `rgba(26,82,118,0.12)`, 2px ink `#1a5276` border around the group, bold 12px ink label "replicated log" above and 11px `#6b7280` offsets "…1038–1042" inside; 3px `#2a78d6` arrow from producer into its left edge.
- **Consumer boxes (right column at x=540, 150×38 each, 12px text, 3px arrows from the log strip's right edge):** y=55 aqua `#199e70` border "search index @ 1030"; y=125 orange `#d95926` border "cache @ 1042"; y=195 violet `#4a3aa7` border "warehouse @ 987"; fills at 12% opacity of each border color.
- **New consumer:** dashed 2px green `#008300` box at x=540, y=250, 150×34, label "fraud model @ 0 (new)"; dashed green arrow from the log strip's LEFT edge to it, bold 12px green label "replays full history".
- **Annotation (bold 13px magenta `#d55181`, bottom left near y=270):** "add a reader without touching the writer".
- **Caption (12px `#444`, bottom right):** "offsets carried over from the worked example; illustrative".

## A Log Is Not a Queue

**Tags:** `common mistake` (red), `ordering` (orange)

- **The queue rule** — a queue hands each message to one worker and deletes it once acknowledged
- **The log rule** — reading deletes nothing; records leave only via a time or size retention limit
- **Own bookmarks** — three log readers all see order 987; three queue workers would split the work
- **Ordering** — a log guarantees order only within one partition; two partitions interleave freely
- **The trap** — keying orders randomly across partitions, then wondering why updates arrive scrambled

*Example (italic):* Put order 31's "paid" and "refunded" events on different partitions and a reader can apply the refund before the payment ever arrives.

**Common mistake:** Treating a log like a queue — expecting reads to remove messages and expecting one global order. Deletion is a retention policy, not a read, and order exists only per partition; key related events to the same partition.

### Visualization (canvas `c4`, 720×300)

Two-row comparison: a queue where consumed messages vanish and workers split the stream, vs a log where all records remain and every reader has its own bookmark.

- **Title (bold 15px, `#1a5276`, top center):** "Queue: Read Deletes. Log: Read Moves Your Bookmark.".
- **Row 1 (queue), 12px `#444` label "queue" at x=20, y=95:** five 66px boxes at y=70, 40px tall, starting x=90, offsets `[984, 985, 986, 987, 988]`; boxes 984 and 985 drawn with dashed 2px `#e74c3c` borders, no fill, 12px red `#e74c3c` strike-through text and a bold 12px red label "deleted after ack" beneath them; boxes 986–988 fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` borders; one 3px `#2a78d6` arrow from box 986 to a single box at x=560, y=70 labeled "one worker each".
- **Row 2 (log), label "log" at x=20, y=215:** the same five offsets `[984, 985, 986, 987, 988]` at y=190, all solid boxes fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` borders — nothing crossed out; three bookmark arrows below pointing up at boxes, bold 12px labels at y=255: aqua `#199e70` "warehouse @ 987", violet `#4a3aa7` "search @ 988", orange `#d95926` "cache @ 988" (offset the cache label to x+90 so the two 988 labels don't collide).
- **Divider:** 1px `#e5e9ef` horizontal line at y=150 from x=20 to x=700.
- **Annotation (bold 13px green `#008300`, right side near y=170):** "same records, three readers — nothing is deleted".
- **Caption (12px `#444`, bottom right):** "offsets illustrative; ordering holds only within one partition".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness). Section 1: offsets 101–108 with named coffee orders, bookmarks at 104 / 106 / 108. Section 2: head 1042, bookmarks cache 1042 / search 1030 / warehouse 987, lags 0 / 12 / 55 by subtraction, rewind to 900 replaying 142 records (1042 − 900). Section 4 reuses offsets 984–988. All offsets are invented and labeled illustrative; the lag and replay counts are exact arithmetic given them.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
