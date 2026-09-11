# Collaborative Editor

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Collaborative Editor

**Subtitle:** Two people type in the same sentence at the same time — the editor must merge both keystrokes so every screen converges to the identical text

## Two Cursors in the Same Sentence

**Tags:** `core idea` (blue), `concurrency` (green), `collaboration` (orange)

- **The doc** — two teammates edit the same shared sentence from two laptops at the same moment
- **The keystrokes** — A inserts a word near the front while B fixes a typo at the end, 7ms apart
- **The naive save** — treat the doc as one value: whoever's save lands last overwrites the other's
- **The loss** — with last-write-wins, A's new word silently vanishes when B's save arrives later
- **The goal** — both edits must survive and every replica must converge to the same final text

*Example (italic):* A's save lands at 10:00:00.012 and B's at 10:00:00.019; with last-write-wins the doc becomes B's copy alone, and A watches her word disappear.

**Key point:** Real-time editing is a convergence problem — concurrent edits must merge so all replicas end at the identical document — not a race where the last full-document save wins.

### Visualization (canvas `c1`, 720×300)

Flow diagram: two users' edits funneling into a doc stored as a single string, with last-write-wins discarding one edit.

- **Title (bold 15px, `#1a5276`, top center):** "Same Second, Two Keystrokes: Last-Write-Wins Keeps Only One".
- **User boxes (rounded 8px, 190×44, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` text):** "User A: insert a word — .012" at (60, 70) and "User B: fix a typo — .019" at (60, 180).
- **Server box (rounded, 170×44, fill `#f8f9fa`, 2px `#6b7280` border):** "doc stored as ONE string" at (330, 125); 3px `#2a78d6` arrow from A's box and 3px `#199e70` arrow from B's box into its left edge.
- **Result box (rounded, 170×44, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border):** "final doc = B's copy only" at (540, 125), 3px `#6b7280` arrow from server box.
- **Loss marker:** bold 14px red `#e74c3c` "✗ A's word lost" at (555, 105), plus a red diagonal strike across A's arrow near x=290.
- **Annotation (bold 13px red `#e74c3c`, centered near y=255):** "last-write-wins keeps the later save, not both edits".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative".

## The "cat" Merge, By Hand

**Tags:** `worked example` (blue), `operational transformation` (green)

- **The start** — the shared document is the 3-letter word "cat": positions 0='c', 1='a', 2='t'
- **User A** — inserts "o" at position 1, so her local copy becomes "coat"
- **User B** — concurrently deletes position 2 (the "t"), so his local copy becomes "ca"
- **Naive replay** — server applies A, then B's raw delete@2 on "coat" removes the "a": "cot" — wrong
- **The transform** — A's insert at 1 shifts B's delete to position 3; "coat" minus the "t" is "coa"
- **Both sides** — B's replica applies A's insert@1 to "ca" and also lands on "coa": convergence

*Example (italic):* Without the transform the server shows "cot" (deleted "a" by mistake) while B's own screen shows "coa" — with the one-index shift, every replica shows "coa".

**Key point:** Operational Transformation rewrites each incoming operation's position against concurrent ops already applied — an insert before you shifts your index right by one — classically with a central server choosing the order.

### Visualization (canvas `c2`, 720×300)

Branching state diagram: "cat" splits into A's and B's local edits, then the no-transform path diverges to "cot" while the transformed path converges to "coa" on both replicas.

- **Title (bold 15px, `#1a5276`, top center):** "insert 'o'@1 + delete@2: One Shifted Index Decides Convergence".
- **Word boxes (rounded 8px, 90×40, bold 16px monospace `#2c3e50` centered):** start "cat" at (50, 130) fill `rgba(42,120,214,0.15)`.
- **Branch A:** 3px `#2a78d6` arrow labeled 12px "A: insert 'o' @1" to box "coat" at (220, 55).
- **Branch B:** 3px `#199e70` arrow labeled 12px "B: delete @2" to box "ca" at (220, 205).
- **Wrong path:** 3px `#e74c3c` arrow from "coat" labeled 12px red "raw delete @2" to box "cot" at (450, 40) fill `rgba(231,76,60,0.12)`, bold 13px red "✗ deleted 'a'" at its right.
- **Right path:** 3px `#008300` arrow from "coat" labeled 12px green "transformed: delete @3" to box "coa" at (450, 115) fill `rgba(0,131,0,0.12)`, bold 13px green "✓" at its right.
- **B's replica path:** 3px `#008300` arrow from "ca" labeled 12px "A's insert 'o' @1 (unchanged)" to box "coa" at (450, 205) fill `rgba(0,131,0,0.12)`; dashed 2px `#008300` bracket joining the two "coa" boxes labeled bold 12px green "same text everywhere".
- **Annotation (bold 13px violet `#4a3aa7`, near (560, 270)):** "the whole trick: 2 → 3".

## CRDTs: Name the Letters, Skip the Referee

**Tags:** `where it's used` (blue), `CRDT` (green), `decentralized` (orange)

- **The alternative** — a CRDT gives each character a stable unique ID that never shifts as text changes
- **No positions** — a delete says "remove the letter with ID s3", never "delete whatever is at 2"
- **Ordering** — concurrent inserts between the same neighbors are ranked by ID, so all replicas agree
- **No referee** — merges are deterministic, so peers can sync in any order without a central server
- **The trade** — per-character IDs add metadata; OT keeps documents lean but leans on the server

*Example (italic):* In a CRDT the "t" in "cat" is ID s3 forever; A's insert cannot shift it, so B's "delete s3" removes the right letter on every replica without any transform.

**Key point:** OT fixes positions with transforms plus a server-picked order; CRDTs remove positions entirely with stable IDs — two published academic routes to the same convergence guarantee.

### Visualization (canvas `c3`, 720×300)

Two-row diagram of the same "cat" example as ID-tagged character cells: the start row, then the merged row where the insert slots between neighbors and the delete tombstones its target.

- **Title (bold 15px, `#1a5276`, top center):** "Every Letter Gets a Permanent ID — 'delete s3' Can't Miss".
- **Row 1 (cells at y=75), 12px `#444` label "start" at x=20:** three cells (80×48, rounded 6px, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) at x=170/270/370: bold 16px letter on top ("c", "a", "t"), 11px `#6b7280` ID below ("id s1", "id s2", "id s3").
- **Row 2 (cells at y=190), label "after merge":** four cells at x=140/240/340/440: "c / s1" blue; "o / a4" fill `rgba(0,131,0,0.12)`, 2px `#008300` border; "a / s2" blue; "t / s3" fill `#f8f9fa`, 2px dashed `#e74c3c` border with a red strikethrough across the letter (tombstone).
- **Op labels:** bold 12px green `#008300` "A: insert (a4,'o') between s1 and s2" at (140, 150) with a short arrow to the "o" cell; bold 12px red `#e74c3c` "B: delete id s3" at (440, 150) with arrow to the tombstoned cell.
- **Read-out:** bold 14px `#1a5276` "reads: \"coa\"" at (560, 214).
- **Annotation (bold 13px green `#008300`, centered near y=270):** "IDs never move, so both edits hit the right letters on every replica".
- **Caption (12px `#444`, bottom right):** "IDs illustrative; real CRDTs use (site, counter) pairs".

## A Document Is a Log, Not a File

**Tags:** `common mistake` (red), `storage` (orange)

- **The mistake** — storing the doc as one string and overwriting it on save: concurrent edits get lost
- **Timestamps don't help** — two edits 7ms apart are still concurrent; clock order still drops intent
- **The log** — store the ordered history of operations; the document is the result of replaying it
- **Snapshots** — replaying 84,132 ops is slow, so save a full snapshot every 1,000 ops, replay the tail
- **Cursors too** — presence (each user's colored cursor) is just a position, transformed like any op

*Example (italic):* Opening a doc with 84,132 ops in its history loads the snapshot taken at op 84,000 and replays only the last 132 operations — milliseconds, not minutes.

**Common mistake:** Treating the document as a value to overwrite. The unit of truth is the operation; last-write-wins on whole documents — with or without timestamps — silently discards keystrokes.

### Visualization (canvas `c4`, 720×300)

Operation-log strip with periodic snapshot markers, plus a zoom-in showing that opening the doc loads one snapshot and replays a short tail.

- **Title (bold 15px, `#1a5276`, top center):** "84,132 Ops in the Log, a Snapshot Every 1,000".
- **Log strip:** rectangle x=60 to x=660 at y=95, 32px tall, fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border; 12px `#444` end labels "op 0" below x=60 and "op 84,132" below x=660.
- **Snapshot markers:** 3px `#008300` vertical ticks across the strip at x = 60, 180, 300, 420, 540, 645; 12px green `#008300` label "snapshots every 1,000 ops (spacing schematic)" at (60, 70).
- **Zoom guides:** dashed 2px `#6b7280` lines from strip points x=645 and x=660 down to a zoom group at y=190.
- **Zoom group (rounded boxes 44px tall, 12px text):** green box "load snapshot @ op 84,000" (200px wide, fill `rgba(0,131,0,0.12)`, 2px `#008300`) at (130, 190); 3px `#d95926` arrow to orange box "replay 132 ops" (140px, fill `rgba(230,126,34,0.15)`, 2px `#d95926`) at (380, 190); 3px `#6b7280` arrow to blue box "current doc" (110px, 2px `#2a78d6`) at (570, 190).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=265):** "open = 1 snapshot + 132 ops replayed, never all 84,132".
- **Caption (12px `#444`, bottom right):** "op counts illustrative; strip widths schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** everything is the hardcoded literals above (no randomness); the "cat" → "coa"/"cot" strings, character IDs (s1/s2/s3/a4), timestamps, and op counts (84,132 total, snapshots every 1,000, snapshot at 84,000, tail of 132) are invented and labeled illustrative/schematic; text numbers and chart numbers must match exactly.
- **Framing:** generic system-design exercise; OT and CRDTs are published academic concepts — make no claims about the real company's current internal systems, and keep body prose about "a collaborative editor".
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
