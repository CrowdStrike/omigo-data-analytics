# Data Structures

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Data Structures

**Subtitle:** How programs organize data so that finding, adding, and summarizing things stays fast — from arrays and hash tables to the sketches that count a billion users in a few kilobytes.

## Cards

Each card links to a topic page under `data-structures/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | FOUNDATIONS | Arrays vs Linked Lists | [26-data-structures/01-arrays-vs-linked-lists.md](26-data-structures/01-arrays-vs-linked-lists.md) | Items side by side in numbered slots versus scattered and chained by next-address notes — and why side by side almost always wins. | contiguous memory, pointers, cache friendliness |
| 2 | FOUNDATIONS | Hash Tables | [26-data-structures/02-hash-tables.md](26-data-structures/02-hash-tables.md) | File every item into a numbered bucket computed from its key, so a lookup checks one small bucket instead of the whole pile. | buckets, hash function, constant-time lookup |
| 3 | FOUNDATIONS | Stacks & Queues | [26-data-structures/03-stacks-and-queues.md](26-data-structures/03-stacks-and-queues.md) | Two ways to store things in a row and take them back out — a stack returns the newest first, a queue returns the oldest first. | LIFO, FIFO, take-out order |
| 4 | FOUNDATIONS | Trees | [26-data-structures/04-trees.md](26-data-structures/04-trees.md) | Store items as a hierarchy of branch points — ask "left or right?" at each step, and a balanced tree finds anything in a handful of questions. | hierarchy, binary search, balance |
| 5 | FOUNDATIONS | Self-Balancing Trees | [26-data-structures/05-self-balancing-trees.md](26-data-structures/05-self-balancing-trees.md) | A search tree fed sorted data collapses into a slow chain — rotating nodes as they arrive keeps the height, and every lookup, a few steps. | rotations, height guarantee, sorted-input trap |
| 6 | FOUNDATIONS | Prefix, Infix & Postfix | [26-data-structures/06-prefix-infix-and-postfix.md](26-data-structures/06-prefix-infix-and-postfix.md) | One expression tree read in three depth-first orders gives three notations — the operator speaks before, between, or after its operands. | expression trees, traversal order, reverse Polish |
| 7 | SPECIALIZED STRUCTURES | Representing Graphs | [26-data-structures/07-representing-graphs.md](26-data-structures/07-representing-graphs.md) | A record of who connects to whom, written either as a big yes/no grid or as each node's short list of neighbors. | adjacency matrix, adjacency list, networks |
| 8 | SPECIALIZED STRUCTURES | Heaps & Priority Queues | [26-data-structures/08-heaps-and-priority-queues.md](26-data-structures/08-heaps-and-priority-queues.md) | A loosely ordered pile with one promise — the smallest item is always on top — so "what's next?" is answered instantly. | min on top, priority, scheduling |
| 9 | SPECIALIZED STRUCTURES | Tries | [26-data-structures/09-tries.md](26-data-structures/09-tries.md) | Store words letter by letter along shared branches, so every word starting with "ca" is found by walking two steps and reading below. | prefix tree, autocomplete, shared branches |
| 10 | SPECIALIZED STRUCTURES | Skip Lists | [26-data-structures/10-skip-lists.md](26-data-structures/10-skip-lists.md) | A sorted list with express lanes stacked on top — coin flips decide which items get promoted, and a search rides the fast lanes down. | express lanes, coin flips, sorted search |
| 11 | SPECIALIZED STRUCTURES | Union-Find | [26-data-structures/11-union-find.md](26-data-structures/11-union-find.md) | Track which things belong to the same group by giving each group one leader — merging two groups is just re-aiming one arrow. | disjoint sets, group leader, cheap merges |
| 12 | SPECIALIZED STRUCTURES | Segment Trees | [26-data-structures/12-segment-trees.md](26-data-structures/12-segment-trees.md) | Keep pre-added sums of pairs, quads, and bigger blocks stacked above the raw numbers, so any range total takes a handful of steps. | range queries, precomputed sums, fast updates |
| 13 | SPECIALIZED STRUCTURES | Bitmaps & Bitsets | [26-data-structures/13-bitmaps-and-bitsets.md](26-data-structures/13-bitmaps-and-bitsets.md) | One yes/no fact per numbered slot stored as a single bit — a whole cinema hall fits in 8 bytes, and set questions become machine instructions. | one bit per fact, bitwise ops, compact sets |
| 14 | SPECIALIZED STRUCTURES | Fenwick Trees | [26-data-structures/14-fenwick-trees.md](26-data-structures/14-fenwick-trees.md) | Overlapping partial sums hidden inside one flat array, so running totals and corrections to past values both finish in a few hops. | prefix sums, fast updates, one flat array |
| 15 | SPECIALIZED STRUCTURES | Suffix Arrays | [26-data-structures/15-suffix-arrays.md](26-data-structures/15-suffix-arrays.md) | Sort every tail of a text once and any substring — word or not — falls to binary search, because all its occurrences sit together in the sorted list. | sorted tails, substring search, binary search |
| 16 | DISK & DATABASES | B-Trees | [26-data-structures/16-b-trees.md](26-data-structures/16-b-trees.md) | Wide sorted blocks stacked only a few levels deep, so finding one record among billions takes a handful of steps — the structure behind almost every database index. | wide nodes, shallow depth, database indexes |
| 17 | DISK & DATABASES | LSM Trees | [26-data-structures/17-lsm-trees.md](26-data-structures/17-lsm-trees.md) | When data arrives faster than you can file it, jot everything at the end of a notepad and sort it later in batches. | write-heavy, append then sort, compaction |
| 18 | PROBABILISTIC SKETCHES | HyperLogLog | [26-data-structures/18-hyperloglog.md](26-data-structures/18-hyperloglog.md) | Guess the size of a crowd from one lucky coin streak — a count of a billion distinct users in 1.5 KB, off by only a couple percent. | distinct counts, tiny memory, lucky streaks |
| 19 | PROBABILISTIC SKETCHES | Count-Min Sketch | [26-data-structures/19-count-min-sketch.md](26-data-structures/19-count-min-sketch.md) | Count everything in a stream on a tiny fixed grid of shared boxes, and read an item's count as the smallest box it touches — it can only overshoot. | stream counting, shared counters, overcount only |
| 20 | PROBABILISTIC SKETCHES | T-Digest | [26-data-structures/20-t-digest.md](26-data-structures/20-t-digest.md) | Keep a few tiny summary buckets instead of every number, and still answer "what's the median?" or "what's the 95th percentile?" about a stream never stored. | percentiles, summary buckets, streaming quantiles |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "FOUNDATIONS" `#2980b9`, "SPECIALIZED STRUCTURES" `#27ae60`, "DISK & DATABASES" `#8e44ad`, "PROBABILISTIC SKETCHES" `#e67e22`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`, `#e67e22`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
