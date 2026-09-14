# Quorum Strategies

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Quorum Strategies

**Subtitle:** Every quorum rule is a way of drawing groups so that any two groups must share a member — counting heads is only the simplest way, and the five common strategies buy smaller groups, uneven power, or plain availability with what they give up

## Majority Quorum: 3 of 5 Must Say Yes

**Tags:** `majority quorum` (blue), `simplest strategy` (green), `2 failures survived` (orange)

- **The setup** — one value lives on N = 5 equal servers A, B, C, D, E; every server gets one vote
- **The rule** — a group counts as a quorum once ⌊N/2⌋ + 1 = 3 servers are in it
- **Why it overlaps** — two groups of 3 out of 5 cannot avoid each other, because 3 + 3 = 6 > 5
- **The failure budget** — 3 of 5 still answer with 2 servers dead, so 2 crashes are survivable
- **How many groups** — there are 10 different 3-server groups, and each server sits in 6 of them
- **Even load** — every server carries the same share of the work, since all 10 groups are equally legal
- **Even votes are wasted** — 6 servers also need 4 to agree, so it survives the same 2 failures as 5

*Example (italic):* Servers A, B, C accept a write; a later group C, D, E asks all three and hears the new value from C.

**Key point:** A majority works because two sets each holding more than half of a fixed pool are too big to be disjoint — the arithmetic, not the machines, is what forces the overlap.

### Visualization (canvas `c1`, 720×300)

Five equal server circles in a row, all 10 possible 3-server quorums drawn as a stacked tally on the right, and the 3 + 3 > 5 arithmetic annotated below the row.

- **Title (bold 15px, `#1a5276`, top center):** "Majority: Any 3 of 5 Servers, Any 3 Overlap Any Other 3".
- **Server row:** names `['A','B','C','D','E']` at x = `[70, 145, 220, 295, 370]`, y=120, circles radius 26, white fill, 2px `#1a5276` stroke, bold 14px `#1a5276` letters.
- **Vote labels (12px `#6b7280`, centered under each circle at y=168):** "1 vote" under each of the five.
- **Two example rings:** 2.5px blue `#2a78d6` ellipse centered (145, 120) rx=115 ry=44 around A, B, C; 2.5px green `#008300` ellipse centered (295, 120) rx=115 ry=44 around C, D, E; filled circle radius 34 `rgba(217,89,38,0.15)` behind C (x=220) drawn first.
- **Ring labels (bold 12px, centered):** "group of 3" in blue at (110, 62); "another group of 3" in green at (330, 62).
- **Overlap annotation (bold 13px orange `#d95926`, centered at (220, 205)):** "3 + 3 > 5 — they must share a server".
- **Sub-annotation (12px `#444`, centered at (220, 226)):** "here they share C".
- **Quorum tally (right panel, x from 470 to 700):** heading bold 13px `#1a5276` "all 10 legal groups" at (470, 62), left-aligned; then 10 rows at y = 82, 100, 118, 136, 154, 172, 190, 208, 226, 244 — each row prints the group as 12px `#444` text at x=470: "ABC", "ABD", "ABE", "ACD", "ACE", "ADE", "BCD", "BCE", "BDE", "CDE".
- **Tally side note (bold 12px violet `#4a3aa7`, left-aligned at x=560, two lines y=140/158):** "each server appears" / "in 6 of the 10".
- **Caption (12px `#444`, bottom right):** "N = 5, quorum = 3, survives 2 failures".

## Weighted Vote Quorum: the Witness Holds the Odd Vote

**Tags:** `weighted votes` (blue), `witness server` (green), `split brain blocked` (orange)

- **The change** — servers keep unequal votes, and a quorum is a majority of votes, not of machines
- **The setup** — two sites hold two data servers each at 2 votes, plus a tiny witness at 1 vote: 9 total
- **The rule** — any group holding 5 of the 9 votes is a quorum, because 5 + 5 = 10 > 9
- **A site alone loses** — two servers at one site hold 4 votes, one short, so neither site can act by itself
- **The witness decides** — whichever site still reaches the witness gets 4 + 1 = 5 votes and keeps serving
- **Split brain blocked** — the witness sits in exactly one group at a time, so two sites never both proceed
- **The witness is cheap** — it stores votes and metadata only, so it needs no room for the actual data
- **The concentration risk** — the more votes one machine holds, the more its single crash can freeze the group

*Example (italic):* Site X's two servers plus the witness hold 5 of 9 votes and keep accepting writes while Site Y, on 4 votes, refuses.

**Key point:** Weighting votes lets an odd tiebreaker with no data on it decide which half of a split network stays alive — the overlap rule now counts votes rather than machines.

### Visualization (canvas `c2`, 720×300)

Two sites of two servers facing each other across a dashed partition line, a small witness box on the winning side, and a vote tally bar showing 5 votes versus 4.

- **Title (bold 15px, `#1a5276`, top center):** "Weighted Votes: 2 + 2 + 1 Beats 2 + 2".
- **Partition line:** vertical dashed (dash 7/5) 2px `#6b7280` line at x=360 from y=48 to y=196; 12px `#6b7280` label "network split" centered at (360, 214).
- **Site X (left, winning):** label bold 13px green `#008300` "Site X" centered at (150, 62); two circles radius 24 at (95, 110) and (205, 110), white fill, 2px green stroke, bold 13px green letters "X1", "X2"; 12px `#008300` "2 votes" centered under each at y=150.
- **Witness:** box 96×30 centered at (150, 176) — draw at (102, 176), fill `rgba(74,58,167,0.12)`, 2px violet `#4a3aa7` border; bold 12px violet "witness · 1" centered at (150, 195).
- **Site Y (right, refusing):** label bold 13px `#6b7280` "Site Y" centered at (560, 62); two circles radius 24 at (505, 110) and (615, 110), white fill, 2px `#6b7280` stroke, bold 13px `#6b7280` letters "Y1", "Y2"; 12px `#6b7280` "2 votes" centered under each at y=150.
- **Tally bars (bottom band):** baseline y=262; each bar 28px tall drawn upward from y=262. Left bar width 150 from x=75, fill `rgba(0,131,0,0.30)`, 2px green border; bold 13px green label "5 of 9 votes — serving" centered at (150, 252). Right bar width 120 from x=500, fill `rgba(107,114,128,0.22)`, 2px `#6b7280` border; bold 13px `#6b7280` label "4 of 9 votes — refusing" centered at (560, 252).
- **Annotation (bold 12px orange `#d95926`, centered at (360, 240), two lines y=240/256):** "the witness holds no data —" / "it only breaks the tie".
- **Caption (12px `#444`, bottom right):** "total 9 votes, quorum 5, one witness".

## Grid Quorum: Read a Row, Write a Column

**Tags:** `grid quorum` (blue), `asymmetric sets` (green), `needs a write orderer` (red)

- **The layout** — nine servers sit in a 3 × 3 grid rather than a flat list of nine equals
- **The read set** — a read asks one complete row: 3 servers, not the 5 a majority of nine needs
- **The write set** — a write goes to one complete column, also 3 servers, so both sides stay cheap
- **The geometry** — every row crosses every column in exactly one cell, so a read always sees the write
- **The saving grows** — at 7 × 7 a row is 7 of 49 servers, where a majority would demand 25 of 49
- **The catch** — two writes on different columns share no cell, so writes need a leader to order them
- **The fragility** — one dead server per column blocks every write, though 6 of 9 are still healthy

*Example (italic):* A write fills column 3 (S3, S6, S9) and a read of row 2 (S4, S5, S6) meets it at the single shared cell S6.

**Key point:** Geometry can shrink a quorum well below half, but the shape decides which pairs of sets are forced to meet — here reads meet writes, while writes never meet each other.

### Visualization (canvas `c3`, 720×300)

A 3×3 grid of servers with one row shaded as the read set and one column shaded as the write set, their single crossing cell marked, plus the 3-of-9 versus 5-of-9 comparison on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Grid: One Row Meets One Column in Exactly One Cell".
- **Grid:** cells 66×52 with 10px gaps, origin (70, 66); labels "S1"–"S9" row-major, bold 12px centered; base cells fill `#f4f6f9` with 1px `#6b7280` border.
- **Read row (middle row, S4 S5 S6):** fill `rgba(0,131,0,0.18)`, 2px `#008300` border.
- **Write column (right column, S3 S6 S9):** fill `rgba(42,120,214,0.25)`, 2px `#2a78d6` border.
- **Crossing cell (S6, in both):** fill `rgba(217,89,38,0.30)`, 2px `#d95926` border.
- **Row label (bold 12px green `#008300`, right-aligned at x=60, y = middle row center + 4):** "read".
- **Column label (bold 12px blue `#2a78d6`, centered above the right column at y=56):** "write".
- **Crossing label (bold 12px orange `#d95926`, centered under the right column at y = grid bottom + 20):** "the one shared cell".
- **Right panel (left-aligned at x=340):** bold 13px `#1a5276` "read = a row = 3 of 9" at y=90; bold 13px `#1a5276` "write = a column = 3 of 9" at y=112; 13px `#6b7280` "a majority of nine = 5 of 9" at y=140.
- **Comparison bars (left-aligned at x=340):** two bars 20px tall, width scaled 24px per server — grid bar width 72 at y=156 fill `rgba(42,120,214,0.30)` 2px blue border, majority bar width 120 at y=186 fill `rgba(107,114,128,0.22)` 2px `#6b7280` border; 12px labels to the right of each bar at x=420: blue "3 servers" at y=171, `#6b7280` "5 servers" at y=201.
- **Annotation (bold 12px violet `#4a3aa7`, left-aligned at x=340, two lines y=228/246):** "at 7 × 7 a row is 7 of 49 —" / "a majority would need 25".
- **Caption (12px `#444`, bottom right):** "cheaper sets, but writes must be ordered elsewhere".

## Tree Quorum: Walk From Root to Leaf

**Tags:** `tree quorum` (blue), `root to leaf path` (green), `root is a hotspot` (orange)

- **The layout** — seven servers form a binary tree: one root, two middles, four leaves at the bottom
- **The rule** — a quorum is any straight path from the root down to one leaf: 3 servers of the 7
- **Why it overlaps** — every root-to-leaf path starts at the root, so any two paths share it at minimum
- **The saving** — 3 of 7 servers beats the 4 of 7 a majority needs, and deeper trees save far more
- **Deeper trees** — a 15-server tree of depth 4 has 4-server paths, where a majority would need 8
- **When the root dies** — the rule substitutes a path in each subtree instead, so a quorum becomes 4 of 7
- **The hotspot** — while the root is healthy every quorum contains it, so it absorbs all the traffic

*Example (italic):* The path root → M1 → L2 is a quorum of 3, and any other root-to-leaf path meets it at the root.

**Key point:** A tree buys the smallest quorums of the five strategies by routing every group through one shared server — which is exactly why that server becomes the bottleneck.

### Visualization (canvas `c4`, 720×300)

A 7-node binary tree with two root-to-leaf paths highlighted meeting at the root, and the fallback path-pair used when the root fails shown to the right.

- **Title (bold 15px, `#1a5276`, top center):** "Tree: Two Root-to-Leaf Paths Always Share the Root".
- **Main tree (left, x centered near 210):** root "R" at (210, 70); middles "M1" at (140, 150), "M2" at (280, 150); leaves "L1" (95, 230), "L2" (185, 230), "L3" (245, 230), "L4" (335, 230). Circles radius 22, white fill, 2px stroke, bold 12px letters.
- **Edges:** 1.5px `#6b7280` lines root→M1, root→M2, M1→L1, M1→L2, M2→L3, M2→L4, drawn before the circles.
- **Path A (R, M1, L2):** edges redrawn 3px green `#008300`; nodes R, M1, L2 stroked green with green letters.
- **Path B (R, M2, L3):** edges redrawn 3px blue `#2a78d6`; nodes M2, L3 stroked blue with blue letters.
- **Root marker:** filled circle radius 30 `rgba(217,89,38,0.15)` behind R drawn before the edges; bold 12px orange `#d95926` "shared" centered at (210, 34).
- **Path labels (bold 12px, centered):** green "path A = 3 servers" at (110, 268); blue "path B = 3 servers" at (320, 268).
- **Right panel (left-aligned at x=430):** bold 13px `#1a5276` "quorum = 3 of 7" at y=76; 13px `#6b7280` "a majority of seven = 4 of 7" at y=100; bold 13px violet `#4a3aa7` "at 15 servers: a path is 4," at y=138 and "a majority is 8" at y=156.
- **Root-failure note (bold 12px orange `#d95926`, left-aligned at x=430, three lines y=196/214/232):** "if the root dies:" / "one path per subtree instead —" / "4 of 7, still overlapping".
- **Caption (12px `#444`, bottom right):** "smallest quorums, at the cost of a hot root".

## Sloppy Quorum: the One That Gives Up the Overlap

**Tags:** `sloppy quorum` (blue), `availability first` (orange), `not really a quorum` (red)

- **The setup** — a key's three home replicas sit next to each other on a hash ring: A, B, C with W = 2
- **The normal write** — a write to A and B reaches 2 of the 3 homes and the overlap rule still holds
- **When B is unreachable** — a strict quorum would refuse the write; a sloppy one refuses to refuse
- **The substitute** — the write goes to A plus the next reachable server on the ring, D, which is not a home
- **The broken guarantee** — a later read of B and C touches neither A nor D, so it can miss that write
- **Hinted handoff** — D stores the value plus a note naming B, and delivers it once B answers again
- **The trade** — writes keep succeeding through a partition, and reads may be stale until handoff finishes
- **Why it is offered** — for a shopping cart a stale read beats a refused write, so many stores default to it

*Example (italic):* D holds the write with a hint that says "this belongs to B", and hands it over minutes later when B returns.

**Key point:** A sloppy quorum still counts to W, but the servers it counts are not the ones a read will ask — the count survives and the intersection guarantee does not.

### Visualization (canvas `c5`, 720×300)

A hash ring with home replicas A, B, C for one key, B crossed out, the write landing on A and stand-in D, and a dashed handoff arrow from D back to B.

- **Title (bold 15px, `#1a5276`, top center):** "Sloppy Quorum: the Write Counts, the Overlap Does Not".
- **Ring:** 2px `#e5e9ef` circle centered (250, 168) radius 100.
- **Ring servers (radius 20, white fill, bold 12px letters, placed on the ring):** A at angle −90° (250, 68); B at −18° (345, 137); C at 54° (309, 249); D at 126° (191, 249); E at 198° (155, 137). Compute positions from the centre and radius so the labels sit on the circle.
- **Home-replica arc (bold 12px `#1a5276`, centered at (250, 168), two lines y=162/180):** "key k → homes" / "A, B, C".
- **A (reachable home):** 2px green `#008300` stroke, green letter.
- **B (unreachable home):** 2px `#6b7280` stroke, `#6b7280` letter, plus a 2px red `#e74c3c` cross drawn through it (two diagonal strokes 12px each side); 11px red "unreachable" centered at (378, 116) — keep at 12px minimum, so use 12px.
- **C (reachable home):** 2px `#6b7280` stroke, `#6b7280` letter.
- **D (stand-in):** white fill, 2px dashed (dash 5/4) orange `#d95926` stroke, bold 12px orange letter; bold 12px orange "stand-in" centered at (191, 283).
- **Client:** box 128×28 at (452, 54), fill `rgba(0,131,0,0.12)`, 2px green `#008300` border; bold 12px green "client: write v7" centered at (516, 72).
- **Write arrows (2px green, arrowheads 8px):** from (470, 68) to A's right edge (272, 62); from (490, 82) curving to D — draw a straight 2px green line from (470, 84) to (211, 240) with the arrowhead at D.
- **Write label (bold 12px green `#008300`, left-aligned at x=430, two lines y=112/130):** "W = 2 reached: A and D —" / "but D is not a home replica".
- **Handoff arrow:** 2px dashed (dash 6/4) aqua `#199e70` quadratic curve from D (191, 269) through control point (300, 300) to B (345, 157), arrowhead at B; bold 12px aqua "hinted handoff" centered at (300, 296) — shift to y=294 to stay inside the canvas.
- **Stale-read annotation (bold 12px red `#e74c3c`, left-aligned at x=430, two lines y=176/194):** "a read of B and C touches" / "neither A nor D — stale".
- **Caption (12px `#444`, bottom right):** "availability bought by suspending the intersection rule".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then five `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; all charts pushed into a `__charts` array of functions, drawn once on load and redrawn on a 150ms-debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** every number is hardcoded and arithmetically checked, no randomness. The majority section's 5 servers / quorum 3 / 2 failures / 10 groups (C(5,3)=10) / 6 groups per server (C(4,2)=6) must match the tally list in `c1`. The weighted section's votes 2+2+2+2+1 = 9, quorum 5, site total 4, and 4+1 = 5 must match the bars in `c2`. The grid section's 3-of-9 row and column, single crossing cell S6, 5-of-9 majority, and 7-of-49 versus 25-of-49 figures must match `c3`. The tree section's 7 nodes, 3-server path, 4-of-7 majority, 15-node depth-4 path of 4 versus majority 8, and 4-of-7 root-failure fallback must match `c4`. The sloppy section's homes A, B, C, W = 2, stand-in D, and the stale read of B and C must match `c5`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
