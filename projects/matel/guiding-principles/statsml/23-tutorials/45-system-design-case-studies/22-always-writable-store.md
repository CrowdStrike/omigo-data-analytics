# Always-Writable Store

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Always-Writable Store

**Subtitle:** A landmark always-available key-value design: the shopping cart must always accept writes, so choose availability over consistency — a ring of replicas, sloppy quorums, vector clocks, and a cart that merges by union

## The Cart That Must Never Say No

**Tags:** `core idea` (blue), `availability first` (green), `availability trade` (orange)

- **The rule** — the design starts from a business rule: add-to-cart must always work
- **The cost of no** — a refused add-to-cart is lost revenue; a slightly stale cart is only a nuisance
- **The choice** — when too few replicas are reachable, refuse the write or accept it; this design accepts
- **The price** — accepting writes on both sides of a failure means the cart can split into versions
- **The name** — the term of art is an "always writable" store: eventual consistency, chosen on purpose

*Example (italic):* During a 3-minute outage cutting off 2 of 3 replicas at ~50 add-to-cart writes per second, a strict store refuses ≈9,000 adds; an always-writable store accepts every one and reconciles later (rates illustrative).

**Key point:** This is a business rule turned into architecture — the cart must always take the write, so consistency is relaxed and repaired afterward instead of enforced up front.

### Visualization (canvas `c1`, 720×300)

Timeline chart comparing accepted add-to-cart writes per second during a replica failure: a strict-quorum store (drops to 0) vs an always-writable store (stays level), on a shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "2 of 3 Replicas Cut Off at Minute 3: Strict Quorum Refuses, Always-Writable Accepts".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 10 with 12px `#444` tick labels every 2 minutes; y = accepted writes/sec 0 to 60, gridlines `#e5e9ef` at 15/30/45.
- **Failure band:** fill `rgba(231,76,60,0.08)` between minutes 3 and 6, dashed `#6b7280` (dash 4/3) vertical edges, 12px `#6b7280` label "2 of 3 replicas cut off" at the top of the band.
- **Strict-quorum line:** red `#e74c3c` 3px line through minutes `[0, 2, 3, 3.1, 6, 6.1, 8, 10]`, writes/sec `[50, 52, 51, 0, 0, 49, 51, 50]` — vertical cliff to 0 at minute 3, back up at minute 6.
- **Always-writable line:** green `#008300` 3px line through minutes `[0, 2, 4, 6, 8, 10]`, writes/sec `[50, 52, 49, 51, 50, 50]` — flat through the outage.
- **Annotation (bold 13px green `#008300`, near minute 7, y=90):** "3 min × ~50 writes/s ≈ 9,000 carts saved".
- **Caption (12px `#444`, bottom right):** "rates illustrative; the trade-off is the documented design".

## A Ring of Nodes and a Sloppy Quorum

**Tags:** `worked example` (blue), `consistent hashing` (green), `N, R, W` (orange)

- **The ring** — every key is hashed onto a circle; each node owns the arc that ends at its position
- **The walk** — `cart:alice` hashes to 72°; the next N=3 nodes clockwise — B, C, D — hold the replicas
- **The knobs** — N=3 copies, W=2 acks to accept a write, R=2 replies to read (the commonly cited config)
- **The slop** — D is down, so the write skips to the next healthy node E, tagged with a hint "this is D's"
- **The handoff** — E hands the hinted copy back when D recovers; Merkle-tree anti-entropy repairs the rest

*Example (italic):* With W=2, acks from B and C are enough — the cart write succeeds even while replica D is dead, and E babysits D's copy until it returns.

**Key point:** A sloppy quorum means "the first N healthy nodes on the ring", not a fixed set — a write never waits on a dead node, and hinted handoff walks the stray copy home.

### Visualization (canvas `c2`, 720×300)

Consistent-hashing ring diagram: six node dots on a circle, the hashed key marked on the arc, the N=3 replica walk highlighted, the failed node crossed out, and a dashed hint arrow to its stand-in; N/R/W panel on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Key at 72° → Replicas B, C, D — D Is Down, the Hint Goes to E".
- **Ring:** circle centered (250,165), radius 100, 2px `#e5e9ef` stroke.
- **Node dots (radius 14, fill `rgba(42,120,214,0.18)`, 2px `#2a78d6` border, bold 13px `#1a5276` letter centered), clockwise from top:** A (250,65), B (337,115), C (337,215), D (250,265), E (163,215), F (163,115).
- **Key marker:** magenta `#d55181` filled dot radius 6 at (301,78) on the arc between A and B, bold 12px magenta label "hash(cart:alice)" to its upper right.
- **Replica highlights:** B and C redrawn with 3px green `#008300` borders and 11px green "✓ ack" labels beside them; D redrawn with 3px red `#e74c3c` border and bold 12px red "down ✗" label below it.
- **Hint arrow:** 2px dashed orange `#d95926` (dash 5/4) arrow from D to E, bold 12px orange label "hinted handoff: E holds D's copy" along it, below the ring.
- **Right panel (13px `#2c3e50`, starting x=480, y=110, line height 26):** "N = 3 replicas", "W = 2 acks to write", "R = 2 replies to read"; below at y=200, bold 12px green `#008300`: "B ✓ + C ✓ = W met → write accepted".
- **Caption (12px `#444`, bottom right):** "6-node ring illustrative; (N,R,W) = (3,2,2) as commonly configured".

## Two Carts, One Customer: The Union Merge

**Tags:** `vector clocks` (blue), `merge by union` (green), `accepted cost` (orange)

- **The split** — Alice's cart is {book} with clock [Sx,1]; her laptop and phone write during a partition
- **Branch one** — the laptop adds a lamp through node Sx: {book, lamp}, clock [Sx,2]
- **Branch two** — the phone deletes the book and adds a charger through Sy: {charger}, clock [Sx,1][Sy,1]
- **The detection** — neither clock descends from the other, so the store keeps both versions as siblings
- **The merge** — the next read returns both; the cart application merges by union: {book, lamp, charger}
- **The cost** — the deleted book reappears; the design accepts this so that an add is never, ever lost

*Example (italic):* Alice deleted the book on her phone, yet after the union merge it sits in her cart next to the lamp and the charger — the documented price of never refusing a write.

**Key point:** Vector clocks let the store detect a conflict, but it never resolves one — resolution is pushed to the application, and the cart's rule is union, which can resurrect a deleted item.

### Visualization (canvas `c3`, 720×300)

Two-branch flow diagram: one cart forks into two divergent versions during a partition, the read surfaces both siblings, and the union merge produces a cart with the deleted item back.

- **Title (bold 15px, `#1a5276`, top center):** "Vector Clocks Detect the Split; the Cart Merges by Union".
- **Box style:** rounded 8px radius, 44px tall, 12px `#2c3e50` two-line text; blue fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` border, orange fill `rgba(217,89,38,0.12)` with 2px `#d95926` border, green fill `rgba(0,131,0,0.12)` with 2px `#008300` border.
- **Start box (blue):** at x=25, y=130, width 130: "cart {book}" / "[Sx,1]".
- **Partition divider:** vertical dashed red `#e74c3c` (dash 4/3) line at x=195 from y=55 to y=250, 12px red label "partition" at its top.
- **Branch boxes (blue), fed by 2px `#6b7280` arrows from the start box:** top at x=215, y=60, width 190: "laptop adds lamp" / "{book, lamp}  [Sx,2]"; bottom at x=215, y=200, width 190: "phone deletes book, adds charger" / "{charger}  [Sx,1][Sy,1]".
- **Sibling box (orange), fed by arrows from both branches:** at x=435, y=130, width 140: "read returns BOTH" / "conflict → siblings".
- **Merge box (green), fed by one arrow:** at x=592, y=130, width 124: "union merge" / "{book,lamp,charger}".
- **Annotation (bold 13px red `#e74c3c`, centered near y=278):** "the deleted book is back — the design's accepted cost".

## The Cart Had a Merge Rule — Your Data May Not

**Tags:** `common mistake` (red), `when it applies` (orange)

- **The hidden premise** — the design works because two carts can be combined without asking a human
- **Safe by union** — a set of cart items merges cleanly; the worst outcome is one unwanted extra row
- **Safe by clock** — display preferences can take the later write; the loss is one overwritten edit
- **Safe by counting** — likes merge only if you store increments, never a read-then-write total
- **No rule exists** — a balance of $100 and a balance of $80 have no correct combination
- **The knob trap** — running R=1, W=1 for speed makes stale reads routine, not a rare corner case

*Example (italic):* A team copies the always-writable design for account balances, gets two sibling versions after a partition, and finds that $100 and $80 merge to neither $180 nor $80 nor $100.

**Common mistake:** Treating the trade as a free lunch. It buys availability by exporting conflict resolution to the application — copy it only where a merge rule as forgiving as the cart's union actually exists.

### Visualization (canvas `c4`, 720×300)

Four-row comparison table: a data type, the merge rule available for it, and a verdict — the first three safe, the last with no valid rule.

- **Title (bold 15px, `#1a5276`, top center):** "Availability Is Only Free When a Merge Rule Exists".
- **Column headers (bold 13px `#1a5276`, y=52):** "the data" left-aligned at x=40, "merge rule after a split" at x=250, "verdict" at x=520.
- **Header rule:** 1px `#e5e9ef` line at y=62 from x=30 to x=690.
- **Rows (four bands, 52px tall, centers at y = 96, 148, 200, 252):** each row draws a rounded 8px box from x=30 to x=690, height 44, fill and border per verdict.
- **Row 1 (green fill `rgba(0,131,0,0.10)`, 2px `#008300`):** data "cart: {book, lamp}"; rule "union of both sets"; verdict bold green `#008300` "✓ safe — worst case one extra item".
- **Row 2 (green fill `rgba(0,131,0,0.10)`, 2px `#008300`):** data "profile: theme = dark"; rule "later timestamp wins"; verdict bold green `#008300` "✓ safe — worst case one lost edit".
- **Row 3 (orange fill `rgba(217,89,38,0.10)`, 2px `#d95926`):** data "like count: 42"; rule "sum the increments, not the totals"; verdict bold orange `#d95926` "~ safe only if you store deltas".
- **Row 4 (red fill `rgba(231,76,60,0.10)`, 2px `#e74c3c`):** data "balance: $100 vs $80"; rule "no rule — $180? $80? $100?"; verdict bold red `#e74c3c` "✗ unsafe — needs a strict quorum".
- **Row text:** data column 13px `#2c3e50` at x=40; rule column 12px `#444` at x=250; verdict column bold 12px at x=520.
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=288):** "the ring and the quorums are reusable — the union merge is not".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); write rates, the 6-node ring, latencies, and cart contents are invented and labeled illustrative; (N,R,W) = (3,2,2), the union-merge cart behavior, hinted handoff, and Merkle-tree anti-entropy are the widely published always-writable key-value design of the late 2000s, described here without naming the company or its internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
