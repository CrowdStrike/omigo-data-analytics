# Pulsar

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Pulsar

**Subtitle:** Apache Pulsar splits messaging into a stateless serving layer and a separate storage layer — so adding a server means no data ever has to move

## The Broker That Stores Nothing

**Tags:** `core idea` (blue), `stateless brokers` (green), `Apache Pulsar` (orange)

- **The stream** — a clip-sharing site pushes every playback event through a topic, ~200 MB/s on a normal day
- **The Kafka way** — each broker owns partitions AND stores their data on its own disks; compute and storage are glued
- **The Pulsar way** — brokers only serve traffic; the bytes live in a separate BookKeeper layer of "bookies"
- **The spread** — a topic's segments stripe across many bookies instead of living on specific brokers
- **The payoff** — a broker holds no data, so any broker can take over any topic in seconds
- **The origin** — Pulsar was built at Yahoo for exactly this and became an Apache project

*Example (italic):* When a Pulsar broker dies, another broker picks up its topics immediately — no data copy, because the data was never on the dead broker.

**Key point:** Pulsar's counter-proposal to Kafka is compute/storage separation: brokers are a stateless serving layer, BookKeeper bookies are the storage layer, and the two scale independently.

### Visualization (canvas `c1`, 720×300)

Two-column architecture diagram: Kafka's fused broker (compute + storage in one box) vs Pulsar's two layers (stateless brokers above a shared bookie pool), with the topic's data drawn inside brokers on the left and striped across bookies on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Kafka Fuses Compute and Storage; Pulsar Splits Them".
- **Left half (Kafka), header bold 13px `#2c3e50` "Kafka" at x=170, y=55:** three rounded boxes at x=60/170/280 (width 100, height 90, y=75, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) labeled "broker 1"/"broker 2"/"broker 3" (12px), each containing a small solid blue `#2a78d6` disk icon (60×18 bar at box bottom) labeled 11px "partition data".
- **Right half (Pulsar), header bold 13px `#2c3e50` "Pulsar" at x=530, y=55:** top row of two rounded boxes at x=430/560 (width 110, height 40, y=75, fill `rgba(0,131,0,0.12)`, 2px `#008300` border) labeled "broker (serve only)"; bottom row of four boxes at x=410/490/570/650 (width 65, height 55, y=180, fill `rgba(25,158,112,0.15)`, 2px `#199e70` border) labeled "bookie 1"–"bookie 4", each with a small aqua segment bar labeled 11px "seg".
- **Arrows:** thin 2px `#6b7280` lines from each Pulsar broker box fanning to all four bookies (data can go anywhere); on the Kafka side no arrows — data is inside the broker boxes.
- **Annotation (bold 13px green `#008300`, right half near y=260):** "topic data stripes across all bookies — no broker owns it".
- **Annotation (bold 12px `#2a78d6`, left half near y=260):** "each partition lives on specific brokers".
- **Caption (12px `#444`, bottom right):** "schematic — box counts illustrative".

## The 9am Spike: Adding One Server, Two Ways

**Tags:** `worked example` (blue), `scaling out` (green)

- **The spike** — a viral clip pushes the playback topic from 200 to 500 MB/s at 9:00am; ops adds a 7th server
- **Kafka's bill** — the topic holds 600 GB on 6 brokers; balancing onto 7 means copying 600/7 ≈ 86 GB
- **The wait** — at a 200 MB/s replication throttle, 86 GB takes 430 s ≈ 7.2 minutes before broker 7 helps
- **The insult** — that copy traffic rides the same network that is already melting under the spike
- **Pulsar's bill** — the new broker registers, gets assigned topic bundles, and serves in ~10 seconds
- **Zero bytes** — no data moves, because the 600 GB already lives on the bookies, not on any broker

*Example (italic):* Same spike, same 600 GB topic: Kafka's new broker is useful at 9:07am after an 86 GB copy; Pulsar's is useful at 9:00:10 after copying nothing.

**Key point:** Adding a Kafka broker means rebalancing partitions — gigabytes over the network; adding a Pulsar broker is instant because a stateless broker has nothing to receive.

### Visualization (canvas `c2`, 720×300)

Line chart of the scale-out: gigabytes copied over the network in the minutes after the 9:00am "add a server" command, Kafka climbing to 86 GB vs Pulsar flat at 0.

- **Title (bold 15px, `#1a5276`, top center):** "Adding Server #7 at 9:00am: 86 GB Moves vs 0 GB Moves".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes after 9:00 from 0 to 8, 12px `#444` tick labels every 2 minutes ("9:00"–"9:08"); y = GB copied 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Kafka line:** red `#e74c3c` 3px line through minutes `[0, 1, 2, 3, 4, 5, 6, 7, 7.2, 8]`, GB `[0, 12, 24, 36, 48, 60, 72, 84, 86, 86]` — steady 200 MB/s climb, flat once the 86 GB rebalance finishes at 7.2 min.
- **Pulsar line:** green `#008300` 3px line through the same minute grid, GB all `[0, 0, 0, 0, 0, 0, 0, 0, 0, 0]` — flat on the baseline.
- **Ready markers:** vertical dashed `#6b7280` (dash 4/3) line at minute 0.17 with 12px green label "Pulsar broker serving (~10s)"; vertical dashed line at minute 7.2 with 12px red label "Kafka broker fully useful".
- **Annotation (bold 13px green `#008300`, near minute 4, y=70):** "stateless broker: nothing to copy".
- **Caption (12px `#444`, bottom right):** "sizes and throttle illustrative; 86 GB ÷ 200 MB/s = 430 s exact".

## Cheap History, Many Teams, Two Messaging Styles

**Tags:** `where it's used` (blue), `tiered storage` (green), `multi-tenancy` (orange)

- **Tiered storage** — old segments offload from bookies to S3, so a topic can retain months, not days
- **The bill** — a 3 TB topic all on bookie disks costs ~$300/mo at $100/TB; tiered it costs ~$92/mo
- **Multi-tenancy** — tenants and namespaces are built in, so many teams share one cluster with quotas
- **Geo-replication** — a namespace can replicate across regions with one config line, no add-on cluster
- **Queue or stream** — subscription modes cover both: exclusive/failover act like a stream, shared like a queue
- **For data scientists** — S3-backed retention means replaying a year of events to retrain a model

*Example (italic):* A fraud team replays 9 months of the payments topic straight from S3-offloaded segments — a Kafka cluster with 7-day retention simply no longer has that data.

**Key point:** Because storage is its own layer, Pulsar can push cold segments to S3, host many tenants, replicate across regions, and serve queue and stream consumers from the same topic.

### Visualization (canvas `c3`, 720×300)

Top: a segmented horizontal bar showing where a 30-day, 3 TB topic's bytes live (bookies vs S3). Bottom: two cost bars comparing all-on-bookies vs tiered.

- **Title (bold 15px, `#1a5276`, top center):** "A 30-Day, 3 TB Topic: Hot Tail on Bookies, Cold History on S3".
- **Storage bar (y=80, x=60 to 660, height 30):** left segment aqua fill `rgba(25,158,112,0.35)` with 2px `#199e70` border, width 540 (90% — days 1–27, 2.7 TB) labeled 12px "days 1–27 → offloaded to S3 (2.7 TB)"; right segment blue fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, width 60 (10% — days 28–30, 0.3 TB) labeled 12px above the bar "last 3 days on bookies (0.3 TB)".
- **Cost rows (bars start x=230, max width 400, 16px tall), left-aligned 12px `#444` labels at x=20:**
  - Row at y=170, "all on bookies: $300/mo": blue `#2a78d6` solid bar width 400, 11px label "$300" at bar end.
  - Row at y=215, "tiered (0.3 TB bookies + 2.7 TB S3): $92/mo": green `#008300` solid bar width 123, 11px label "$92" at bar end.
- **Annotation (bold 13px green `#008300`, near x=430, y=222):** "69% cheaper — and the topic keeps everything".
- **Caption (12px `#444`, bottom right):** "$100/TB-mo bookie SSD and $23/TB-mo S3 illustrative; $30 + $62 = $92 arithmetic exact".

## Not a Free Upgrade: Count the Moving Parts

**Tags:** `common mistake` (red), `operations` (orange)

- **The pitch** — stateless brokers sound like less to operate; the full picture is more services, not fewer
- **Kafka's stack** — one service type: brokers (with KRaft, coordination now lives inside them)
- **Pulsar's stack** — three service types: brokers to serve, bookies to store, ZooKeeper to coordinate
- **Bookies aren't free** — storage still needs capacity planning, disk monitoring, and recovery drills
- **The mistake** — adopting Pulsar for one small topic and inheriting three clusters' worth of ops
- **The real trade** — you pay in moving parts to buy instant scaling, tiering, and tenancy

*Example (italic):* A two-person team runs a 50 MB/s pipeline happily on 3 Kafka brokers; the same pipeline on Pulsar means brokers, a bookie ensemble, and ZooKeeper before the first message flows.

**Common mistake:** Reading "stateless brokers" as "less operational burden". The state did not disappear — it moved to BookKeeper, and now three distinct systems must be deployed, monitored, and upgraded.

### Visualization (canvas `c4`, 720×300)

Two-row diagram counting the service types each system needs: Kafka's single fused row vs Pulsar's three-layer stack, with the trade-off annotated.

- **Title (bold 15px, `#1a5276`, top center):** "What You Operate: One Service Type vs Three".
- **Row 1 (y=95), label 12px `#444` at x=20:** "Kafka"; one wide rounded box at x=180 (width 300, height 46, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) labeled 12px "brokers — serve + store + coordinate (KRaft)"; bold 12px blue `#2a78d6` "1 service type" at x=510, y=122.
- **Row 2 (y=185), label:** "Pulsar"; three rounded boxes left to right at x=180/340/500 (width 140, height 46, 8px radius) — green fill `rgba(0,131,0,0.12)` border `#008300` "brokers — serve", aqua fill `rgba(25,158,112,0.15)` border `#199e70` "bookies — store", orange fill `rgba(230,126,34,0.15)` border `#e67e22` "ZooKeeper — coordinate"; bold 12px red `#e74c3c` "3 service types" at x=510, y=252.
- **Box style:** 12px `#2c3e50` text centered; thin 2px `#6b7280` connector lines between the three Pulsar boxes.
- **Annotation (bold 13px orange `#d95926`, centered near y=278):** "the flexibility is real — so is the extra machinery".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); topic size (600 GB), throughputs (200/500 MB/s), storage prices ($100 and $23 per TB-month), and the 3 TB / 30-day retention are invented and labeled illustrative; the derived numbers (86 GB ≈ 600/7, 430 s = 86 GB ÷ 200 MB/s ≈ 7.2 min, $30 + $62 = $92, 69% cheaper) are exact arithmetic on those inputs. Architecture facts (Yahoo origin, stateless brokers over BookKeeper, tiered storage to S3, built-in multi-tenancy and geo-replication, exclusive/failover/shared subscription modes) are publicly documented Apache Pulsar behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
