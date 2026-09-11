# Webmail Service

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Webmail Service

**Subtitle:** A mailbox is a sharded database — each user's mail, labels, and search index live together on one shard, which is what makes web-scale email tractable (a classic design exercise, not a description of any real company's internals)

## One User, One Shard, One Place to Look

**Tags:** `core idea` (blue), `shard key` (green), `mailbox` (orange)

- **The service** — an email system for 10 million users must store, label, and search everyone's mail
- **The shard key** — hash(user) assigns each mailbox to one shard; all of kim@'s mail lands together
- **What lives there** — kim@'s messages, her labels/folders as metadata rows, and her own search index
- **Labels are pointers** — "Inbox" and "Travel" are metadata rows tagging one stored message, not copies
- **The payoff** — "open inbox", "read thread", "search my mail" each touch exactly one shard

*Example (italic):* When kim@ opens her inbox, the front end asks shard 2 and only shard 2 — shards 1 and 3 never hear about the request.

**Key point:** Email has a natural shard key — the mailbox owner. Queries are almost always "my mail", so one user's whole world fits on one shard and reads never cross users.

### Visualization (canvas `c1`, 720×300)

Routing diagram: users hashed to three shard boxes; one inbox request hits exactly one shard while the others stay untouched.

- **Title (bold 15px, `#1a5276`, top center):** "hash(user) Picks the Shard — an Inbox Load Touches Exactly One".
- **User column (left):** four 12px `#2c3e50` labels "ana@", "bob@", "kim@", "raj@" at x=45, y = 75, 130, 185, 240; thin 1.5px `#6b7280` arrows from each to its shard box (ana@→shard 1, bob@→shard 3, kim@→shard 2, raj@→shard 1), 11px `#6b7280` label "hash(user)" centered above the arrow bundle at (x≈175, y=55).
- **Shard boxes (center column):** three rounded boxes 150×58, 8px radius, left edge x=280, tops y = 55, 135, 215; fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border; bold 12px `#1a5276` titles "shard 1" / "shard 2" / "shard 3" with 11px `#2c3e50` second line "msgs · labels · index" in each.
- **Query arrow:** green `#008300` 3px arrow from a rounded box at x=545, y=135 (150×58, fill `rgba(0,131,0,0.12)`, 12px text "kim@ opens Inbox") to the right edge of shard 2.
- **Untouched marks:** dashed 1.5px `#e74c3c` (dash 4/3) lines from the query box toward shard 1 and shard 3, each ending short with a 12px red "✗"; 11px `#e74c3c` label "never touched" at (x≈500, y≈95).
- **Annotation (bold 13px green `#008300`, bottom center y=288):** "one user's read = one shard, always".
- **Caption (12px `#444`, bottom right):** "generic design exercise — mapping illustrative".

## Delivering 1,000 Messages Through the Pipeline

**Tags:** `worked example` (blue), `delivery pipeline` (green)

- **The batch** — 1,000 messages arrive over SMTP in one minute (an illustrative traffic slice)
- **The scan** — spam and virus filters reject 380 at the gate; 620 messages continue to delivery
- **The dedupe** — the 620 carry 240 attachments; content hashes match 90 already stored, so 150 new blobs
- **The write** — each of the 620 is written to its recipient's shard with an "Inbox" label row
- **The index** — delivery appends the message's terms to that user's search index before acknowledging

*Example (italic):* Of 1,000 arriving messages, 380 die at the spam gate; the surviving 620 cost 620 shard writes, 620 index updates, and only 150 new attachment blobs.

**Key point:** Delivery is a pipeline — receive, scan, dedupe, write to the owner's shard, index. Every message is search-ready the moment it lands because indexing happens at delivery time, not at query time.

### Visualization (canvas `c2`, 720×300)

Left-to-right pipeline flow: five stage boxes with the message count surviving each stage, plus a blob-count callout under the dedupe stage.

- **Title (bold 15px, `#1a5276`, top center):** "One Minute of Delivery: 1,000 In, 620 Mailbox Writes, 150 New Blobs".
- **Stage boxes (row at y=110, each 122×64, 8px radius, 3px `#6b7280` arrows between):** left edges x = 20, 160, 300, 440, 580; labels (bold 12px `#1a5276` title + 11px `#2c3e50` count line):
  - "SMTP receive" / "1,000 msgs" — fill `rgba(42,120,214,0.15)`
  - "spam/virus scan" / "−380 rejected" — fill `rgba(231,76,60,0.12)`, count line in `#e74c3c`
  - "dedupe attachments" / "240 seen, 150 new" — fill `rgba(230,126,34,0.15)`
  - "write to shard" / "620 writes" — fill `rgba(0,131,0,0.12)`
  - "update index" / "620 updates" — fill `rgba(0,131,0,0.12)`
- **Survivor strip (under the boxes, y=205):** horizontal bars at x=20, 14px tall, 1px per 2 messages: blue `rgba(42,120,214,0.30)` bar width 500 labeled "1,000 arrived" (11px `#444`), below it at y=228 a green `#008300`-edged bar width 310 labeled "620 delivered", red `#e74c3c` 11px label "380 dropped" at x=350, y=228.
- **Blob callout (bold 12px orange `#d95926`, under the dedupe box at y=190):** "90 of 240 attachments already stored".
- **Annotation (bold 13px green `#008300`, bottom center y=280):** "indexed on delivery — searchable the second it lands".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Reads Stay Local, but Spam and Dedupe Go Global

**Tags:** `why it matters` (blue), `global systems` (green), `replication` (orange)

- **Simple reads** — every user-facing query is served by one shard; no fan-out, no cross-shard joins
- **Spam goes global** — one mailbox sees a scam twice; a global signal store sees the campaign 5,000 times
- **Dedupe goes global** — a 2 MB PDF sent to 40 users is one blob plus 40 pointers, not 80 MB of copies
- **The split** — shards serve per-user reads; separate global systems do the cross-user learning and storage
- **Replication** — each shard is copied to 3 machines, so one dead disk loses nobody's mail

*Example (italic):* The blob store keys attachments by content hash: 40 recipients of the same 2 MB PDF cost 2 MB of storage plus 40 tiny pointer rows.

**Key point:** Per-user sharding buys simple reads by giving up cross-user queries — so anything that must see across users, like spam signals or attachment dedupe, becomes its own global system beside the shards.

### Visualization (canvas `c3`, 720×300)

Two-lane diagram: the per-user lane (one shard with its 3 replicas) versus the global lane (spam signal store, blob store), with the delivery pipeline feeding both.

- **Title (bold 15px, `#1a5276`, top center):** "Two Worlds: the User's Shard vs the Global Systems".
- **Lane divider:** vertical dashed 1.5px `#e5e9ef` line at x=380 from y=50 to y=270; lane headers bold 13px `#1a5276`: "per-user (kim@'s shard)" at x=110 y=58, "global (all users)" at x=470 y=58.
- **Left lane:** rounded box 200×90 at x=60, y=90, fill `rgba(42,120,214,0.15)`, bold 12px `#1a5276` "kim@'s shard" with 11px `#2c3e50` lines "messages · labels · index"; behind it two offset outline-only copies (+10,+10 and +20,+20, 1.5px `#2a78d6` borders) suggesting replicas; 11px `#2c3e50` label "×3 replicas — a dead disk loses no mail" at x=60, y=225.
- **Right lane:** two rounded boxes 230×54 at x=430: top y=90 "spam signal store" with 11px line "campaign seen 5,000×" (fill `rgba(231,76,60,0.12)`); bottom y=170 "attachment blob store" with 11px line "one 2 MB PDF + 40 pointers" (fill `rgba(230,126,34,0.15)`).
- **Delivery arrows:** from a small 12px `#6b7280` "delivery" label at x=330, y=270, 2px `#6b7280` arrows up-left into the shard box and up-right into each global box.
- **Read arrow:** green `#008300` 3px arrow labeled "reads" (bold 12px green) entering the shard box from the left edge x=20, y=135 — no arrow crosses the divider into the global lane.
- **Annotation (bold 13px violet `#4a3aa7`, bottom center y=290):** "reads never leave the shard; learning and dedupe never fit inside one".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Search Is Indexed at Delivery, Not Scanned at Query

**Tags:** `common mistake` (red), `search index` (orange)

- **The temptation** — "search is just grep": scan the mailbox for the query word when the user searches
- **The size** — a 10-year mailbox holds 40,000 messages at ~8 KB each, so 320 MB to scan per search
- **The cost** — scanning 320 MB at 100 MB/s takes 3.2 seconds, on every single search, for every user
- **The index** — a per-user index maps term → message ids; one lookup reads ~50 KB in about 5 ms
- **The trade** — a little indexing work on every delivery makes every future search near-free

*Example (italic):* Searching "invoice" by scan reads 320 MB and takes 3.2 s; the delivery-time index answers the same query from a 50 KB posting list in 5 ms — about 640× faster.

**Common mistake:** Treating search as a query-time job. At mailbox scale the winning move is to pay at write time — index each message once on delivery — because a message is written once but searched forever.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart comparing the time to answer one search: full mailbox scan vs last-month scan vs index lookup, with a log-feel achieved by hardcoded pixel widths.

- **Title (bold 15px, `#1a5276`, top center):** "One Search for \"invoice\": Scan vs Delivery-Time Index".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 430; no real log axis — widths hardcoded.
- **Rows (bar tops at y = 80, 140, 200, bars 22px tall, left-aligned 12px `#444` labels at x=20, 11px `#444` time labels at bar ends):**
  - "scan whole mailbox — 320 MB": red `#e74c3c` bar width 430, end label "3,200 ms"
  - "scan last month only — 4 MB": orange `#d95926` bar width 210, end label "40 ms"
  - "index lookup — 50 KB posting list": green `#008300` bar width 12, end label "5 ms"
- **Bar fills:** red `rgba(231,76,60,0.25)` / orange `rgba(230,126,34,0.25)` / green `rgba(0,131,0,0.30)`, each with a 2px solid border in its line color.
- **Annotation (bold 13px green `#008300`, near x=300, y=245):** "640× faster — the index was built once per message at delivery".
- **Caption (12px `#444`, bottom right):** "sizes and speeds illustrative; widths schematic, not log-scaled".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all counts, sizes, and timings are the hardcoded literals above (no randomness); delivery counts (1,000 / 380 / 620 / 240 / 90 / 150), dedupe example (2 MB × 40 recipients), and search numbers (40,000 msgs × 8 KB = 320 MB, 100 MB/s → 3.2 s, 50 KB → 5 ms, ≈640×) are invented and labeled illustrative; text numbers must match chart numbers exactly.
- **Framing:** a generic system-design exercise using classic email/mailbox architecture; make no claims about any real company's current internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
