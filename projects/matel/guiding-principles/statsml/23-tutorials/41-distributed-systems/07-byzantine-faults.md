# Byzantine Faults

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Byzantine Faults

**Subtitle:** A crashed node goes silent, but a byzantine node keeps talking and lies — it can even tell every peer a different story

## The General Who Sends Two Different Orders

**Tags:** `core idea` (blue), `nodes that lie` (red), `generals story` (orange)

- **The plan** — three generals surround a city; they win only if all three attack together at dawn
- **The messages** — each general sends riders to the other two carrying "attack" or "retreat"
- **A crash** — a captured rider is just a missing message; the loyal generals wait and resend
- **A lie** — traitor Cara sends "attack" to Alice and "retreat" to Bob: one sender, two stories
- **The trap** — Alice and Bob compare notes, each sees a contradiction, but neither can prove who lied

*Example (italic):* At dawn Alice attacks alone while Bob stays camped — one two-faced general defeats two loyal ones without firing a shot.

**Key point:** A byzantine fault is a node that keeps running but sends wrong or conflicting information — it lies rather than dies, so different peers may see different versions of reality.

### Visualization (canvas `c1`, 720×300)

Message-flow diagram: traitor Cara at the left sends opposite orders to Alice and Bob; a dashed compare-notes link between Alice and Bob ends in a contradiction.

- **Title (bold 15px, `#1a5276`, top center):** "One Traitor, Two Stories: Cara Splits the Loyal Generals".
- **Cara box:** rounded box at x=60, y=125, 150×50, fill `rgba(217,89,38,0.15)`, 2px `#d95926` border, bold 13px `#d95926` text "Cara (traitor)".
- **Alice box:** rounded box at x=470, y=55, 170×50, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 13px `#2c3e50` text "Alice (loyal)".
- **Bob box:** rounded box at x=470, y=200, 170×50, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 13px `#2c3e50` text "Bob (loyal)".
- **Arrows:** 3px green `#008300` arrow from Cara to Alice with bold 13px green label "ATTACK at dawn" above it; 3px orange `#d95926` arrow from Cara to Bob with bold 13px orange label "RETREAT" below it.
- **Compare-notes link:** vertical dashed `#6b7280` (dash 4/3) line between Alice and Bob boxes at x≈555, 12px `#6b7280` label "compare notes: who lied?" beside it.
- **Annotation (bold 13px red `#e74c3c`, centered near y=280):** "same node, two different messages — that is a byzantine fault".
- **Caption (12px `#444`, bottom right):** "story schematic, illustrative".

## Four Nodes, One Liar: Checking 3f+1 by Hand

**Tags:** `worked example` (blue), `3f+1 rule` (green), `voting rounds` (orange)

- **The cluster** — four database nodes A, B, C, D vote on committing order #4721
- **Crash fault** — D goes dark: A, B, C still count 3 "commit" votes of 4 and commit safely
- **Byzantine fault** — D answers "commit" to A but "abort" to B and C: round-1 tallies split, A counts 4-0 while B and C count 3-1
- **The fix** — a gossip round: each node reports what D told it; 2 of 3 reports say "abort", so all record D as abort
- **Hand-check 3f+1** — f=1 liar needs 3(1)+1 = 4 nodes: the 3 honest reports out-vote D's split story
- **Too few** — with only 3 nodes and 1 liar, the two honest nodes tie 1-1 on D's word and can never settle it

*Example (italic):* After the gossip round every node holds the identical tally — 3 commit, 1 abort — and the cluster commits #4721 in one consistent voice.

**Key point:** Surviving f crashed nodes takes 2f+1 replicas, but surviving f liars takes 3f+1 plus an extra round of cross-checking, because a liar can tell every peer a different story.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: commit/abort tallies seen at nodes A, B, C — round 1 (tallies differ) on the left, round 2 after gossip (tallies agree) on the right.

- **Title (bold 15px, `#1a5276`, top center):** "D Lies: Round-1 Tallies Split, the Gossip Round Realigns Them".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 180; y = votes 0 to 4, gridlines `#e5e9ef` at 1/2/3, 12px `#444` y-tick labels; 45px of height per vote.
- **Left group (12px `#444` label "Round 1: what each node counts" centered at x≈200, y=262):** bar pairs for nodes A, B, C at x = 100, 190, 280; commit bars blue `#2a78d6` (fill `rgba(42,120,214,0.55)`) heights for values `[4, 3, 3]`; abort bars orange `#d95926` (fill `rgba(217,89,38,0.55)`) heights for values `[0, 1, 1]`; bars 28px wide, 6px gap in a pair, 12px node labels "A"/"B"/"C" under each pair.
- **Divider:** vertical dashed `#6b7280` (dash 4/3) line at x=370 from y=60 to y=245.
- **Right group (12px `#444` label "Round 2: after swapping notes" centered at x≈530, y=262):** same bar-pair style at x = 430, 520, 610; commit values `[3, 3, 3]`, abort values `[1, 1, 1]` — identical tallies at every node.
- **Value labels:** 11px `#444` counts on top of every bar.
- **Legend (12px, top right):** blue swatch "commit", orange swatch "abort".
- **Annotation (bold 13px green `#008300`, over the right group near y=75):** "3 honest votes out-vote 1 liar — 3f+1 with f=1".
- **Caption (12px `#444`, bottom right):** "order #4721 illustrative; tallies exact for the story".

## Blockchains and Flight Computers Budget for Liars

**Tags:** `where it's used` (blue), `fault model` (green), `cost` (orange)

- **Blockchains** — strangers' machines join freely, and a peer may cheat for profit, so the protocol assumes liars
- **Flight control** — a frying sensor can babble different values to different channels, so flight computers vote in redundant triples
- **Datacenters** — inside one company's fleet, Raft and Paxos deliberately assume crash-only faults: nodes die, they never lie
- **The price** — tolerating f liars costs 3f+1 replicas instead of 2f+1, plus far chattier all-to-all voting rounds
- **The choice** — the fault model is a budget: pick it by who can touch your nodes, not by fashion

*Example (italic):* A payments company runs a 3-node Raft cluster inside its own datacenter but a 4-node BFT protocol for the ledger it shares with rival banks.

**Key point:** Most datacenter systems deliberately assume only crashes because it is cheaper; you pay byzantine prices when nodes belong to strangers or when lives ride on babbling hardware.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: replicas required to tolerate f faults — crash model (2f+1) vs byzantine model (3f+1) for f = 1, 2, 3.

- **Title (bold 15px, `#1a5276`, top center):** "Tolerating Liars Costs More Nodes: 2f+1 vs 3f+1".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = replicas 0 to 10, gridlines `#e5e9ef` at 2/4/6/8, 12px `#444` y-tick labels; 18px of height per replica.
- **Groups:** three pairs centered at x = 170, 360, 550 with 12px `#444` x-labels "f = 1", "f = 2", "f = 3".
- **Crash bars:** blue `#2a78d6` (fill `rgba(42,120,214,0.55)`), 44px wide, heights for values `[3, 5, 7]`, bold 12px blue value labels on top.
- **Byzantine bars:** violet `#4a3aa7` (fill `rgba(74,58,167,0.5)`), 44px wide, 10px right of the crash bar, heights for values `[4, 7, 10]`, bold 12px violet value labels on top.
- **Legend (12px, top left inside plot):** blue swatch "crash-only (2f+1)", violet swatch "byzantine (3f+1)".
- **Annotation (bold 13px magenta `#d55181`, near x=360, y=70):** "every extra liar costs one more node than an extra crasher".
- **Caption (12px `#444`, bottom right):** "replica counts exact (2f+1 vs 3f+1); message costs not shown".

## A Corrupted Node Is Not Malicious — It Just Looks That Way

**Tags:** `common mistake` (red), `corruption vs malice` (orange), `right-sizing` (green)

- **The confusion** — "byzantine" does not require an attacker: a bad RAM stick lies as convincingly as a hacker
- **The bit flip** — one flipped bit turns order #4721 into #4977, and node D reports each number to different peers
- **Same symptom** — peers cannot distinguish corruption from malice; a protocol only ever sees the messages
- **The overpay** — running full 3f+1 BFT inside a trusted datacenter buys protection checksums already give
- **The underpay** — running crash-only Raft across mutually distrusting organizations invites the traitor in
- **Middle ground** — checksums and signed messages catch flipped bits without paying for a full BFT protocol

*Example (italic):* A team ran a 4-node BFT ledger for an internal metrics store; a 3-node Raft cluster with checksummed messages did the same job with far less traffic.

**Common mistake:** Paying byzantine costs when crash tolerance plus checksums suffices — and the reverse, assuming a lying node must be hacked when a single flipped bit produces the same two-faced behavior.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a hacked node and a bit-flipped node both emit conflicting messages, converging on the same "byzantine either way" verdict box.

- **Title (bold 15px, `#1a5276`, top center):** "Hacked or Flipped, the Peers See the Same Thing".
- **Row 1 (y=95), 12px `#444` label at x=20:** "malice"; rounded box at x=110 (170×44) fill `rgba(231,76,60,0.12)`, 12px `#2c3e50` text "attacker controls D"; two 3px `#e74c3c` arrows to a mid box at x=330 (180×44) fill `rgba(42,120,214,0.15)` text "\"commit\" to A / \"abort\" to B".
- **Row 2 (y=205), label:** "bad RAM"; rounded box at x=110 (170×44) fill `rgba(201,133,0,0.15)`, 2px `#c98500` border, 12px `#2c3e50` text "bit flip in D's memory"; two 3px `#c98500` arrows to a mid box at x=330 (180×44) fill `rgba(42,120,214,0.15)` text "\"#4721\" to A / \"#4977\" to B".
- **Verdict box:** both mid boxes send 3px `#6b7280` arrows converging on a rounded box at x=560 (140×70, vertically centered at y=150), fill `rgba(213,81,129,0.12)`, 2px `#d55181` border, bold 12px `#d55181` text "conflicting messages — byzantine either way".
- **Box style:** 8px radius, 12px `#2c3e50` text unless colored above.
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "the protocol cannot read intent — only messages".
- **Caption (12px `#444`, bottom right):** "order numbers illustrative; 4977 is 4721 with one bit flipped".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); order numbers #4721/#4977 and the generals story are invented and labeled illustrative; c2 tallies (round 1 commit `[4,3,3]` / abort `[0,1,1]`, round 2 commit `[3,3,3]` / abort `[1,1,1]`) are exact for the story; c3 replica counts (crash `[3,5,7]`, byzantine `[4,7,10]` for f = 1, 2, 3) are the true 2f+1 and 3f+1 values.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
