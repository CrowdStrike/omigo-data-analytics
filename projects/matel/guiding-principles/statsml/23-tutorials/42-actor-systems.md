# Actor Systems

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Actor Systems

**Subtitle:** How the actor model runs thousands of tiny lock-free workers on a few threads — and how supervision, streams, clustering, and coordination services keep them honest at scale.

## Cards

Each card links to a topic page under `actor-systems/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | ACTOR FUNDAMENTALS | Akka — The Actor Model on the JVM | [42-actor-systems/01-akka-the-actor-model-on-the-jvm.md](42-actor-systems/01-akka-the-actor-model-on-the-jvm.md) | An actor is a tiny worker with a private notebook and a mailbox — it handles one message at a time, so nothing it owns ever needs a lock. | actor model, no locks, message passing |
| 2 | ACTOR FUNDAMENTALS | The ActorSystem | [42-actor-systems/02-the-actorsystem.md](42-actor-systems/02-the-actorsystem.md) | One heavyweight object owns the threads, the scheduler, and the config — so the thousands of actors inside it can stay featherweight. | shared threads, scheduler, one per app |
| 3 | ACTOR FUNDAMENTALS | Actor Hierarchy & Paths | [42-actor-systems/03-actor-hierarchy-and-paths.md](42-actor-systems/03-actor-hierarchy-and-paths.md) | Every actor is spawned by another actor, forming a tree with filesystem-like paths — and whoever spawns you is the one who supervises you. | parent-child tree, paths, who supervises |
| 4 | ACTOR FUNDAMENTALS | Messages & Mailboxes | [42-actor-systems/04-messages-and-mailboxes.md](42-actor-systems/04-messages-and-mailboxes.md) | An actor reads immutable messages from its mailbox one at a time — so its private state never needs a lock. | immutable messages, mailbox queue, one at a time |
| 5 | ACTOR FUNDAMENTALS | Tell vs Ask | [42-actor-systems/05-tell-vs-ask.md](42-actor-systems/05-tell-vs-ask.md) | Hand off a message and walk away (tell), or hand it off holding a claim ticket that expires (ask) — fire-and-forget or a Future with a timeout. | fire-and-forget, futures, timeouts |
| 6 | RUNTIME & FAULT TOLERANCE | Actor Lifecycle | [42-actor-systems/06-actor-lifecycle.md](42-actor-systems/06-actor-lifecycle.md) | An actor is born, works, crashes, and is reborn at the same address — its mailbox survives the restart, its in-memory state does not. | restart, same address, lifecycle hooks |
| 7 | RUNTIME & FAULT TOLERANCE | Supervision Strategies | [42-actor-systems/07-supervision-strategies.md](42-actor-systems/07-supervision-strategies.md) | When a child actor crashes, its parent picks the fix — resume, restart, stop, or escalate — instead of the child defending itself with try/catch everywhere. | let it crash, parent decides, escalate |
| 8 | RUNTIME & FAULT TOLERANCE | Dispatchers & the Thread Model | [42-actor-systems/08-dispatchers-and-the-thread-model.md](42-actor-systems/08-dispatchers-and-the-thread-model.md) | An actor system runs a thousand actors on a handful of real threads — which is why one blocking call inside an actor can freeze all of them at once. | thread pools, blocking danger, throughput |
| 9 | RUNTIME & FAULT TOLERANCE | Routers | [42-actor-systems/09-routers.md](42-actor-systems/09-routers.md) | A router is one actor address that quietly forwards each message to a pool of identical workers — senders talk to one name, many hands do the work. | worker pool, round-robin, one address |
| 10 | STREAMS & DISTRIBUTION | Akka Streams & Backpressure | [42-actor-systems/10-akka-streams-and-backpressure.md](42-actor-systems/10-akka-streams-and-backpressure.md) | The slow end sends demand upstream — a consumer that can only handle 1,000 items a second makes the producer emit exactly that, instead of drowning in a queue. | demand signals, flow control, no overflow |
| 11 | STREAMS & DISTRIBUTION | Akka Persistence | [42-actor-systems/11-akka-persistence.md](42-actor-systems/11-akka-persistence.md) | A persistent actor never saves its current state — it saves every event that changed the state, and rebuilds itself by replaying them after a crash. | event sourcing, journal, replay |
| 12 | STREAMS & DISTRIBUTION | Akka Cluster & Sharding | [42-actor-systems/12-akka-cluster-and-sharding.md](42-actor-systems/12-akka-cluster-and-sharding.md) | The actor tree spans machines — thousands of session actors spread across nodes, and a sender never needs to know which node holds which actor. | multi-node, sharding, location transparency |
| 13 | STREAMS & DISTRIBUTION | Delivery Guarantees in Akka | [42-actor-systems/13-delivery-guarantees-in-akka.md](42-actor-systems/13-delivery-guarantees-in-akka.md) | When one actor sends a message to another, Akka promises surprisingly little by default — the message arrives at most once, which means it might not arrive at all. | at-most-once, acks & retries, deduplication |
| 14 | COORDINATION | ZooKeeper & Coordination Services | [42-actor-systems/14-zookeeper-and-coordination-services.md](42-actor-systems/14-zookeeper-and-coordination-services.md) | Distributed systems outsource their hardest problems — who is leader, who is alive, what is the config — to one small store that never disagrees with itself. | consensus, znodes, source of truth |
| 15 | COORDINATION | ZooKeeper Recipes | [42-actor-systems/15-zookeeper-recipes.md](42-actor-systems/15-zookeeper-recipes.md) | Two small znode flags — auto-numbered and vanish-with-session — snap together into distributed locks, leader election, and group membership. | distributed locks, leader election, ephemeral nodes |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "ACTOR FUNDAMENTALS" `#2980b9`, "RUNTIME & FAULT TOLERANCE" `#27ae60`, "STREAMS & DISTRIBUTION" `#8e44ad`, "COORDINATION" `#e67e22`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`, `#e67e22`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
