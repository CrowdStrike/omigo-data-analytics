# Storm & Samza

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Storm & Samza

**Subtitle:** Streaming's first generation — Storm processed events one tuple at a time, Samza read them off Kafka's partitioned log; together they taught every later engine what is actually hard about streaming

## Counting Hashtags Before the Batch Job Wakes Up

**Tags:** `core idea` (blue), `Storm` (orange), `real time` (green)

- **The feed** — a TV network watches 5,000 tweets/min during a premiere and wants live hashtag counts
- **The old way** — a nightly batch job would deliver the counts at 3am, hours after the show ended
- **The spout** — Storm's source component pulls each tweet off the firehose as one tuple
- **The bolts** — a split bolt extracts the hashtags; a count bolt keeps a running counter per tag
- **The topology** — spouts and bolts wired into a graph that runs forever, one tuple at a time
- **The heritage** — built at BackType, open-sourced via Twitter in 2011; the first widely used stream processor

*Example (italic):* At 9:02pm, ninety seconds into the premiere, the dashboard already shows #finale at 4,181 mentions — no batch job involved.

**Key point:** A Storm topology is a graph of spouts (sources) and bolts (transforms) that processes each event within milliseconds of arrival instead of hours later in a batch.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram of the hashtag topology: tweet spout → split bolt → two count-bolt tasks, with rates on the arrows.

- **Title (bold 15px, `#1a5276`, top center):** "A Storm Topology: Tweets In, Live Hashtag Counts Out".
- **Spout box:** blue rounded box (fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 8px radius) at x=40, y=115, 140×55; two 12px `#2c3e50` lines "tweet spout" (bold) and "5,000 tweets/min".
- **Split bolt box:** same style at x=255, y=115, 160×55; lines "split bolt" (bold) and "extracts hashtags"; 3px `#6b7280` arrow from spout to it, 11px `#6b7280` label "1 tuple / tweet" above the arrow.
- **Count bolt boxes:** two green boxes (fill `rgba(0,131,0,0.12)`, 2px `#008300` border) at x=510, y=65 and y=175, each 170×55; lines "count bolt task 1" / "counters #a–#m" and "count bolt task 2" / "counters #n–#z".
- **Fan-out arrows:** 3px `#6b7280` arrows from the split bolt to each count box; 11px `#6b7280` label "fields grouping by hashtag — 8,400 tags/min" centered between them at y≈150.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "one tuple at a time — latency ~100 ms, not a nightly batch".
- **Caption (12px `#444`, bottom right):** "rates illustrative".

## One Lost Tuple: the Ack Tree Replays the Whole Tweet

**Tags:** `worked example` (blue), `at-least-once` (orange)

- **The tree** — tweet 8814 carries two hashtags, so one spout tuple fans into two child tuples
- **The ack** — each bolt acks its tuple; only when the whole tree is acked is the tweet finished
- **The crash** — the worker counting #finale dies before acking, so the tree never completes
- **The timeout** — after 30 s (Storm's documented default, exact) the spout replays the whole tweet
- **The double count** — #DragonShow was already counted once, so the replay pushes both tags one too high

*Example (italic):* True counts are #finale 4,181 and #DragonShow 2,317; after the replay the dashboard reads 4,182 and 2,318.

**Key point:** Storm's ack tree gives at-least-once delivery — no tuple is silently lost, but a replayed tuple can be processed twice, so counters drift upward with every failure.

### Visualization (canvas `c2`, 720×300)

Tuple-tree diagram for tweet 8814: one spout tuple fanning into an acked child and a failed child, a replay arrow back to the spout, and a true-vs-reported count readout on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Crash, One Replay: Both Hashtags Count Twice".
- **Spout tuple box:** blue rounded box (fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`, 8px radius) at x=30, y=125, 165×50; lines "spout tuple" (bold 12px) and "tweet 8814 — 2 hashtags".
- **Acked child:** green box (fill `rgba(0,131,0,0.12)`, 2px `#008300`) at x=280, y=55, 190×50; lines "#DragonShow → task 2" and bold 12px green "✓ counted, acked"; 3px `#6b7280` arrow from spout box.
- **Failed child:** red box (fill `rgba(231,76,60,0.12)`, 2px `#e74c3c`) at x=280, y=195, 190×50; lines "#finale → task 1" and bold 12px red "✗ worker dies before ack"; 3px `#6b7280` arrow from spout box.
- **Timeout label:** 12px `#6b7280` text "tree incomplete → 30 s timeout (exact)" at x≈285, y=270.
- **Replay arrow:** dashed (dash 5/4) 2px orange `#d95926` arrow curving from the failed box back to the spout box's bottom edge, bold 12px orange label "spout replays the whole tweet" beside it at x≈70, y≈220.
- **Count readout (right side, starting x=510):** 12px `#444` header "true → reported" at y=90; bold 13px red `#e74c3c` lines "#finale 4,181 → 4,182" at y=120 and "#DragonShow 2,317 → 2,318" at y=150.
- **Annotation (bold 13px red `#e74c3c`, right side near y=200):** "nothing lost, some things twice".
- **Caption (12px `#444`, bottom right):** "counts illustrative; 30 s is Storm's documented default timeout".

## Samza's Bet: the Log Is the Stream, State Lives Local

**Tags:** `Samza` (orange), `where it's used` (blue), `the log` (green)

- **The log** — Samza, from LinkedIn, reads streams straight from Kafka's partitioned, replayable log
- **The partition** — each Kafka partition maps to one Samza task, so order holds within a partition
- **Local state** — each task keeps its counters in an embedded store (RocksDB) on its own disk
- **Recovery** — every state change also goes to a Kafka changelog; a restarted task replays it to rebuild
- **The lesson** — Flink and Kafka Streams kept both ideas: the log as substrate, state beside the task

*Example (italic):* When task 1's machine dies, its replacement replays the changelog and reopens with #finale = 4,181 intact — no remote database was ever queried.

**Key point:** Samza's lasting insight is that a replayable partitioned log plus local per-task state turns streaming from fragile plumbing into rebuildable computation.

### Visualization (canvas `c3`, 720×300)

Architecture diagram: three Kafka partitions on the left feeding three Samza tasks, each task with a local state store, and a dashed changelog arrow back to Kafka.

- **Title (bold 15px, `#1a5276`, top center):** "Samza: Partitioned Log In, Local State Beside Each Task".
- **Partition boxes (left column, x=35, each 160×40, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`, 6px radius):** at y=75 "hashtags — partition 0", y=140 "hashtags — partition 1", y=205 "hashtags — partition 2"; 11px `#6b7280` label "Kafka topic (replayable log)" above the column at y=60.
- **Task boxes (middle column, x=290, each 130×40, fill `rgba(0,131,0,0.12)`, 2px `#008300`):** "task 0", "task 1", "task 2" at the same y positions; 3px `#6b7280` arrow from each partition to its task (one-to-one).
- **State store boxes (right column, x=500, each 190×40, fill `rgba(25,158,112,0.12)`, 2px `#199e70`):** y=75 "local store (RocksDB)", y=140 "local store: #finale = 4,181", y=205 "local store (RocksDB)"; short 2px `#199e70` arrow from each task to its store.
- **Changelog arrow:** dashed (dash 5/4) 2px aqua `#199e70` arrow from the middle store box curving left and down to below the partition column, bold 12px aqua label "changelog → Kafka (replay to recover)" at x≈180, y≈272.
- **Annotation (bold 13px aqua `#199e70`, top right near x=500, y=52):** "recovery = replay the log".
- **Caption (12px `#444`, bottom right):** "counts illustrative; architecture as documented".

## Reading At-Least-Once Counts as Exact

**Tags:** `common mistake` (red), `state` (orange)

- **The mistake** — treating a Storm counter as the true count; every crash-and-replay inflates it
- **The mirror image** — turning acking off gives at-most-once: cheaper, but crashes silently drop tuples
- **The hard part** — exactly-once is not a delivery flag; it needs state snapshots tied to stream position
- **The lambda tax** — the era's workaround ran a nightly batch beside the stream: the same logic in two codebases
- **The fix** — later engines (Flink, Kafka Streams, Beam) checkpoint state and offsets together — no replay counts twice
- **The legacy** — Storm and Samza are mostly retired, but their vocabulary and lessons run every modern engine

*Example (italic):* After a day with 12 worker restarts, the same stream reads 49,760 (at-most-once), 50,240 (at-least-once), or 50,000 (checkpointed) — only one matches the true count of 50,000.

**Common mistake:** Believing at-least-once is close enough to exactly-once to ignore. The gap is state management — the exact problem streaming's first generation exposed and its second generation was built to solve.

### Visualization (canvas `c4`, 720×300)

Vertical bar chart: the reported daily count of #finale under three delivery guarantees, against a dashed reference line at the true count.

- **Title (bold 15px, `#1a5276`, top center):** "Same Stream, Three Guarantees, Three Answers".
- **Axes:** origin x=70, baseline y=245, plot width 600, plot height 170; y runs 49,600 to 50,400 with 12px `#444` tick labels and `#e5e9ef` gridlines at 49,800 (y=203), 50,000 (y=160), 50,200 (y=118).
- **True-count line:** dashed (dash 6/4) 2px ink `#1a5276` horizontal line across the plot at y=160, bold 12px ink label "true count 50,000" above it at the left end (x≈80, y≈150).
- **Bars (each 90px wide, value label bold 12px centered above the bar top, category label 12px `#444` below the baseline):**
  - "at-most-once (no acks)": red `#e74c3c` bar at x=130, top y=211, height 34 — value "49,760".
  - "at-least-once (Storm acks)": orange `#d95926` bar at x=330, top y=109, height 136 — value "50,240".
  - "checkpointed exactly-once": green `#008300` bar at x=530, top y=160, height 85 — value "50,000".
- **Annotation (bold 13px magenta `#d55181`, upper right near x=430, y=70):** "the fix was checkpointed state (Flink, Kafka Streams) — not luck".
- **Caption (12px `#444`, bottom right):** "counts illustrative — a day with 12 worker restarts".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all coordinates and values are the hardcoded literals above (no randomness); tweet rates, hashtag counts (4,181 / 2,317 / 50,000-series) are invented and labeled illustrative; the 30-second replay timeout is Storm's documented default (exact); Storm's BackType/Twitter 2011 origin, Samza's LinkedIn/Kafka origin, and the changelog-recovery architecture are publicly documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
