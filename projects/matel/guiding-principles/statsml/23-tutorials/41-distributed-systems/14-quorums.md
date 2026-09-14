# Quorums

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Quorums

**Subtitle:** Replicate a value on 5 servers, write to 3, read from 3 — because 3 + 3 > 5, every read group overlaps every write group and touches at least one up-to-date copy

## Any Two Committees of Three Share a Member

**Tags:** `core idea` (blue), `overlap` (green), `R + W > N` (orange)

- **The setup** — one value is replicated on N = 5 servers named A, B, C, D, E
- **The write rule** — a write counts as done once W = 3 of the 5 servers have stored it
- **The read rule** — a read asks R = 3 servers and keeps the answer with the highest version number
- **The overlap** — any 3 servers and any other 3 servers out of 5 must share at least one: 3 + 3 > 5
- **The guarantee** — with R + W > N, every read set touches at least one server holding the newest write

*Example (italic):* Two committees of 3 drawn from the same 5 people always share a member — that shared member is how the read hears the news.

**Key point:** If R + W > N, the read set and the write set are too big to miss each other — every read overlaps every write on at least one server.

### Visualization (canvas `c1`, 720×300)

Five server circles in a row with two overlapping set rings: a blue write-set ring around A, B, C and a green read-set ring around C, D, E, sharing server C.

- **Title (bold 15px, `#1a5276`, top center):** "Five Servers: the Write Set and Read Set Must Overlap".
- **Server row (shared geometry `Q` with `c2`):** names `['A','B','C','D','E']` at x = `[130, 245, 360, 475, 590]`, y=170, circles radius 24, white fill, 2px `#1a5276` stroke, bold 14px `#1a5276` letters.
- **Overlap highlight:** filled circle radius 34, `rgba(217,89,38,0.15)`, behind server C (x=360), drawn before the rings.
- **Write ring:** 2.5px blue `#2a78d6` ellipse centered (245, 170), rx=160, ry=46, enclosing A, B, C.
- **Read ring:** 2.5px green `#008300` ellipse centered (475, 170), rx=160, ry=46, enclosing C, D, E.
- **Set labels (bold 13px, centered):** "write set, W = 3" in blue at (170, 105); "read set, R = 3" in green at (550, 105).
- **Overlap label (bold 12px orange `#d95926`, centered):** "shared server" at (360, 238).
- **Annotation (bold 13px orange `#d95926`, centered at (360, 266)):** "3 + 3 > 5 — the two sets must share a server".
- **Caption (12px `#444`, bottom right):** "N = 5 servers, any W = 3 and R = 3 overlap".

## Write to A, B, C — Then Read C, D, E

**Tags:** `worked example` (blue), `versions` (green), `broken config` (red)

- **The write** — version 2 lands on A, B, C (W = 3); D and E still hold the old version 1
- **The read** — a read asks C, D, E (R = 3): D and E answer v1, but C answers v2
- **Version wins** — the reader keeps the highest version it saw, so it returns C's v2 — correct
- **The broken config** — R = 2, W = 3 gives 2 + 3 = 5, not > 5, so overlap is no longer forced
- **The miss** — the 2-server read {D, E} shares nothing with {A, B, C}: it sees v1 twice — stale
- **No warning** — the stale read looks perfectly healthy; both servers agree on the wrong answer

*Example (italic):* The read {C, D, E} sees versions 2, 1, 1 and returns v2; the read {D, E} sees 1, 1 and confidently returns stale data.

**Key point:** Version numbers pick the newest copy inside the read set — but only R + W > N guarantees the newest copy is in the set at all.

### Visualization (canvas `c2`, 720×300)

The same five-server row with version labels after the write, a blue write ring around A, B, C and a dashed orange read ring around only D and E that misses the write entirely.

- **Title (bold 15px, `#1a5276`, top center):** "The Broken Config: R = 2, W = 3 Can Miss the Write".
- **Server row:** identical geometry to `c1` (same names, x positions, y=170, radius 24); A, B, C stroked and lettered 2px blue `#2a78d6` (fresh), D, E stroked and lettered 2px `#6b7280` (stale).
- **Version labels (bold 12px, centered at y=232 under each server):** "v2", "v2", "v2" in blue under A, B, C; "v1", "v1" in `#6b7280` under D, E.
- **Write ring:** 2.5px blue `#2a78d6` ellipse centered (245, 170), rx=160, ry=46, enclosing A, B, C.
- **Broken read ring:** 2.5px dashed (dash 6/4) orange `#d95926` ellipse centered (532, 170), rx=105, ry=44, enclosing only D and E — no overlap with the write ring.
- **Set labels (bold 13px, centered):** "write landed on A, B, C" in blue at (190, 105); "read asks only D, E" in orange at (560, 105).
- **Annotation (bold 13px orange `#d95926`, centered at (360, 266)):** "2 + 3 = 5, not > 5 — the read sees v1 twice and returns stale data".
- **Caption (12px `#444`, bottom right):** "a read of {C, D, E} would have caught v2 on C".

## Majority, Grid, and Sloppy Quorums

**Tags:** `quorum flavors` (blue), `grids & trees` (green), `sloppy quorum` (orange)

- **Majority quorum** — ⌊N/2⌋ + 1 servers (3 of 5): the default in consensus systems like Raft
- **Grid quorum** — arrange 9 servers 3×3; a quorum is one row plus one column: 5 of 9 worst case
- **Why grids work** — any row-plus-column crosses any other, so overlap holds; at 7×7 it is 13 of 49
- **Tree quorum** — arrange servers in a tree; overlapping root-to-leaf paths shrink quorums further
- **Sloppy quorum** — Dynamo-style: in a partition, ANY W reachable servers accept the write, even stand-ins
- **Hinted handoff** — a stand-in stores the write plus a hint and hands it to the home server later

*Example (italic):* You may hear "soft quorum" — the standard term is sloppy quorum: any W reachable servers take the write, home or not.

**Key point:** A quorum is any family of groups where every two must intersect — majorities are the simplest such family, not the only one.

### Visualization (canvas `c3`, 720×300)

A 3×3 grid of servers S1–S9 with one row and one column highlighted as a quorum and their crossing cell marked, plus the 5-of-9 arithmetic and the 7×7 payoff on the right.

- **Title (bold 15px, `#1a5276`, top center):** "A Grid Quorum: One Row Plus One Column of 3 × 3".
- **Grid:** cells 64×50 with 10px gaps, origin (105, 70); labels "S1"–"S9" row-major, bold 12px `#1a5276` centered; base cells fill `#f4f6f9` with 1px `#6b7280` border.
- **Picked row (middle row, S4 S5 S6):** fill `rgba(42,120,214,0.25)`, 2px `#2a78d6` border; bold 12px blue label "row" right-aligned left of the row.
- **Picked column (right column, S3 S6 S9):** fill `rgba(0,131,0,0.18)`, 2px `#008300` border; bold 12px green label "column" centered above it.
- **Crossing cell (S6, in both):** fill `rgba(217,89,38,0.30)`, 2px `#d95926` border; bold 12px orange label "crossing" centered below the column, 12px under the grid.
- **Right-side text (left-aligned at x=400):** bold 13px `#1a5276` "quorum = one row + one column" at y=95; 13px `#444` "3 + 3 − 1 crossing = 5 of 9 servers" at y=120; 13px `#4a3aa7` two lines "at 7×7: a quorum is 13 of 49 —" / "far below the 25 a majority needs" at y=215/233.
- **Annotation (bold 13px green `#008300`, left-aligned at x=400, two lines y=160/178):** "any two row+column picks" / "always cross somewhere".
- **Caption (12px `#444`, bottom right):** "crossings replace raw size as the overlap guarantee".

## Overlap Is Not Consistency

**Tags:** `common mistake` (red), `availability trade` (orange)

- **What overlap buys** — R + W > N guarantees a read touches the newest copy — nothing more
- **Not consistency** — two clients writing at once still conflict; overlap neither orders nor merges them
- **Version ties** — concurrent writes can carry clashing versions; something must resolve the conflict
- **Sloppy suspends it** — writes on stand-ins may share no server with reads of the home set
- **Emergency math** — during a partition R + W > N no longer applies; that is the price of availability

*Example (italic):* During a partition, a write lands on stand-in F while a read asks home servers C, D, E — the counts still say 3 + 3 > 5, but the sets never meet.

**Common mistake:** Reading "R + W > N" as "the system is consistent." It only guarantees the read touches the newest copy — conflicts remain, and a sloppy quorum trades even that away during a partition.

### Visualization (canvas `c4`, 720×300)

A partition scene: home servers A, B, C unreachable behind a dashed partition line, a client writing v3 to the reachable D, E plus stand-in F, and a hinted-handoff curve returning the write from F to A later.

- **Title (bold 15px, `#1a5276`, top center):** "Sloppy Quorum: Any W Reachable Servers Take the Write".
- **Server row (y=150, radius 22):** unreachable homes A (x=80), B (x=165), C (x=250) stroked and lettered `#6b7280`; reachable D (x=400), E (x=485) stroked and lettered blue `#2a78d6`; stand-in F (x=585) white fill with dashed (dash 5/4) 2px orange `#d95926` stroke and bold orange "F".
- **Partition line:** vertical dashed (dash 7/5) 2px `#6b7280` line at x=322 from y=70 to y=215, 11px `#6b7280` label "partition" at (322, 230).
- **Group labels:** 11px `#6b7280` "home servers — unreachable" centered at (165, 196); bold 11px orange "stand-in" centered at (585, 196).
- **Client:** box 122×26 at (432, 48), fill `rgba(0,131,0,0.12)`, 2px green `#008300` border, bold 12px green "client: write v3" centered at (493, 65); 2px green arrows from (493, 76) to the tops of D, E, F (arrowheads at y = 124); bold 12px green label "W = 3 reachable: D, E, F" centered at (493, 122).
- **Hinted handoff:** 2px dashed (dash 6/4) aqua `#199e70` quadratic curve from the bottom of F (585, 176) through control point (332, 285) to the bottom of A (80, 178), arrowhead at A; bold 12px aqua label centered at (340, 252): "hinted handoff — F returns the write after the partition heals".
- **Annotation (bold 12px orange `#d95926`, centered at x=150, two lines y=60/76):** "availability kept —" / "overlap guarantee suspended".
- **Caption (12px `#444`, bottom right):** "the standard term is sloppy quorum, not soft quorum".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; all charts pushed into a `__charts` array of functions, drawn once on load and redrawn on a 150ms-debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all server names, x positions, ring geometries, version labels, grid picks, and quorum arithmetic are the hardcoded values above (no randomness); `c1` and `c2` must share identical server coordinates; the N=5 / W=3 / R=3 numbers, the v2-on-A,B,C / v1-on-D,E versions, the broken R=2 miss, the 5-of-9 and 13-of-49 grid figures, and the D, E, F sloppy write in the text must match the charts exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
