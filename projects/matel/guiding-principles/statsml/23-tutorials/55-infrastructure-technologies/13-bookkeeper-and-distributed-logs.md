# BookKeeper & Distributed Logs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** BookKeeper & Distributed Logs

**Subtitle:** Apache BookKeeper does one job — keep an append-only log alive on several machines at once — because the replicated log turns out to be the part every distributed system needs

## A Service Whose Only Job Is the Log

**Tags:** `core idea` (blue), `append-only` (green), `BookKeeper` (orange)

- **The ledger** — a payments service appends events to ledger 42: entry 514, 515, 516... never edited
- **The bookies** — storage nodes B1, B2, B3 each keep copies of entries; no node holds everything
- **The 3-2-2 recipe** — ensemble of 3 bookies, each entry written to 2, confirmed after 2 acks
- **The striping** — entry 514 lands on B2+B3, 515 on B3+B1, 516 on B1+B2, round-robin forever
- **The point** — BookKeeper offers nothing else: no queries, no topics, just durable numbered entries

*Example (italic):* Six entries (514–519) striped 2-of-3 leave each bookie holding four entries — every entry survives any single machine dying.

**Key point:** A ledger is a replicated, append-only sequence of numbered entries; bookies are dumb storage for ledger fragments, and the 3-2-2 quorum recipe decides who stores and who must ack.

### Visualization (canvas `c1`, 720×300)

Grid chart: which bookie holds which entry under 3-2-2 round-robin striping — 6 entry columns × 3 bookie rows, filled cell = copy stored.

- **Title (bold 15px, `#1a5276`, top center):** "Ledger 42, Striped 2-of-3: Every Entry Lives on Exactly Two Bookies".
- **Column headers (bold 12px `#444`, y=70):** entry ids `["514","515","516","517","518","519"]` centered over columns at x = 160 + col*85 (col 0..5).
- **Row labels (bold 13px `#1a5276`, x=50, right-aligned):** `["B1","B2","B3"]` at row centers y = 110, 165, 220.
- **Cells:** 70×40 rounded rects (6px radius) at x = 125 + col*85, y = 90 + row*55; filled cells use `rgba(42,120,214,0.30)` with 2px `#2a78d6` border and a bold 12px `#1a5276` check "●"; empty cells 1px `#e5e9ef` border only.
- **Filled pattern (hardcoded, entry e on bookies e mod 3 and (e+1) mod 3):** B1 holds 515, 516, 518, 519; B2 holds 514, 516, 517, 519; B3 holds 514, 515, 517, 518.
- **Annotation (bold 13px green `#008300`, right side near x=560, y=262):** "each bookie stores 4 of 6 — lose any one, lose nothing".
- **Caption (12px `#444`, bottom left at x=60, y=285):** "entry ids illustrative; striping rule exact for ensemble 3, write quorum 2".

## Entry 517 Survives a Bookie Crash

**Tags:** `worked example` (blue), `quorum acks` (green)

- **The write** — at t=0ms the client sends entry 517 to its two assigned bookies, B2 and B3
- **First ack** — B3 acks at t=25ms; ack quorum is 2, so 517 is not yet confirmed to the app
- **The crash** — B2 dies at t=40ms mid-write; the client stops waiting on it
- **Ensemble change** — at t=80ms the client swaps B2 for a fresh bookie B4 in the ensemble
- **Recovery** — 517 is resent to B4, which acks at t=140ms: two acks, entry confirmed
- **Life goes on** — entry 518 appends at t=160ms against the new ensemble {B1, B3, B4}

*Example (italic):* The app sees entry 517 confirmed at t=140ms instead of ~30ms — a 110ms hiccup, zero data lost, and the writer never restarted.

**Key point:** An append is confirmed only after the ack quorum (2) of bookies acknowledge it; a crashed bookie is replaced by an ensemble change and unacked entries are rewritten — the log never lies about what is durable.

### Visualization (canvas `c2`, 720×300)

Timeline lane chart of the crash: three bookie lanes with ack/crash/resend events on a shared 0–200ms axis.

- **Title (bold 15px, `#1a5276`, top center):** "One Append, One Crash: Entry 517 Confirmed at t=140ms".
- **Axes:** origin x=70, baseline y=250, plot width 580; x = time 0 to 200ms with 12px `#444` tick labels every 50ms; three horizontal lanes as 1px `#e5e9ef` lines at y = 90 (B2), 145 (B3), 200 (B4), bold 13px `#1a5276` lane labels at x=20.
- **Events (10px-radius dots + 12px labels):** send markers (blue `#2a78d6`) at t=0 on B2 and B3 lanes labeled "517 sent"; ack dot (green `#008300`) at t=25 on B3 labeled "ack 1/2"; crash mark (red `#e74c3c` bold "✗", 16px) at t=40 on B2 labeled "B2 dies"; ensemble-change diamond (orange `#d95926`) at t=80 on B4 lane labeled "B4 joins"; resend dot (blue) at t=110 on B4 labeled "517 resent"; ack dot (green) at t=140 on B4 labeled "ack 2/2".
- **Confirm marker:** vertical dashed `#6b7280` (dash 4/3) line at t=140 from y=60 to y=250, bold 13px green `#008300` label "confirmed" at its top.
- **Continuation arrow:** short 2px `#2a78d6` arrow at t=160 near y=60 labeled 12px `#444` "518 appends on {B1,B3,B4}".
- **Caption (12px `#444`, bottom right):** "timings illustrative; ack rule (2 of 2) exact".

## The Log Is the Primitive Under Everything

**Tags:** `where it's used` (blue), `log as primitive` (green), `Pulsar` (orange)

- **Databases** — replicas stay in sync by replaying the primary's write-ahead log, entry by entry
- **Consensus** — Raft and Paxos are, at bottom, protocols for agreeing on one shared log
- **Messaging** — Kafka and Pulsar sell the log itself as the product: topics are logs consumers replay
- **The factoring** — BookKeeper (from Yahoo) extracts that shared primitive into a reusable service
- **In production** — Apache Pulsar stores every topic's messages as BookKeeper ledgers

*Example (italic):* Pulsar brokers keep no message data themselves — a broker crash loses nothing because the topic is really a chain of BookKeeper ledgers.

**Key point:** Most distributed systems secretly rebuild the same thing — a replicated, ordered, append-only log; BookKeeper's insight is to build it once, well, and let systems above stay stateless.

### Visualization (canvas `c3`, 720×300)

Layered stack diagram: three systems that each need a replicated log, sitting on one shared log layer.

- **Title (bold 15px, `#1a5276`, top center):** "Different Products, Same Foundation".
- **Top row (y=80, boxes 190×52, 8px radius, 12px `#2c3e50` two-line text):** blue-tinted `rgba(42,120,214,0.15)` box at x=40 "database replication / (WAL shipping)"; violet-tinted `rgba(74,58,167,0.12)` box at x=265 "consensus / (Raft: agree on a log)"; aqua-tinted `rgba(25,158,112,0.12)` box at x=490 "messaging / (Pulsar topics)".
- **Arrows:** 3px `#6b7280` vertical arrows from each top box (centers x=135, 360, 585) down from y=132 to y=175.
- **Bottom layer (x=40, y=180, 640×62, 8px radius):** fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border, bold 14px `#1a5276` centered text "the replicated append-only log — BookKeeper ledgers on bookies".
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "build the log once; everything above it can be stateless".

## "Acked" Does Not Mean "On Every Bookie"

**Tags:** `common mistake` (red), `read path` (orange)

- **The confusion** — people picture 3 full copies; with write quorum 2, each entry is on only 2 of 3
- **Reads roam** — a read goes to any bookie holding that entry's copy, not to a leader
- **The miss** — asking B1 for entry 517 finds nothing: 517 was striped to B2 and B3 only
- **The client knows** — the striping rule is deterministic, so clients compute who holds what
- **The real risk** — with 2 copies, losing B2 and B3 together loses 517; re-replication closes that gap

*Example (italic):* After the crash, entry 517's two copies sit on B3 and B4 — a read sent to B1 must be redirected, and only losing both B3 and B4 before re-replication would lose it.

**Common mistake:** Assuming a quorum-acked entry is everywhere. Ack quorum 2 buys durability on exactly 2 bookies — the read path and the repair process are built around knowing which 2.

### Visualization (canvas `c4`, 720×300)

Two-row diagram: the assumed "entry on all 3 bookies" picture vs the actual 2-of-3 placement for entry 517.

- **Title (bold 15px, `#1a5276`, top center):** "Where Entry 517 Actually Lives".
- **Row 1 (y=85), label bold 12px `#444` at x=20:** "assumed (wrong)"; three 130×44 rounded boxes at x = 180, 360, 540 labeled "B1: 517", "B2: 517", "B3: 517", all filled `rgba(231,76,60,0.12)` with 2px `#e74c3c` border; bold 12px red `#e74c3c` note "3 copies everywhere" under the row at y=145.
- **Row 2 (y=185), label:** "actual (Qw=2)"; box at x=180 dashed 2px `#6b7280` border, no fill, labeled "B1: —"; boxes at x=360 and x=540 filled `rgba(0,131,0,0.12)` with 2px `#008300` border labeled "B2: 517 ✓" and "B3: 517 ✓".
- **Read arrow:** 2px `#d95926` curved arrow from a 12px `#d95926` label "read 517" at (x=60, y=250) to the B1 box, then a dashed `#d95926` redirect arrow from B1 to B2 with bold 12px orange label "redirect".
- **Box text:** 12px `#2c3e50`, centered; 8px corner radius throughout.
- **Annotation (bold 13px violet `#4a3aa7`, bottom center near y=285):** "clients compute the placement — striping is deterministic, not discovered".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); entry ids (514–519), ledger id 42, and event timings (0/25/40/80/110/140/160ms) are invented and labeled illustrative; the 3-2-2 quorum rule, the round-robin striping pattern (entry e on bookies e mod 3 and (e+1) mod 3), and the 2-ack confirmation rule are exact BookKeeper semantics.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
