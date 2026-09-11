# File Sync

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** File Sync

**Subtitle:** File sync as a distributed-systems problem — hash the chunks, dedupe the storage, diff the lists; a generic design exercise built from publicly well-known sync ideas, not a description of the real company's current internals

## One File Becomes a List of Hashes

**Tags:** `core idea` (blue), `content addressing` (green), `dedupe` (orange)

- **The file** — a photo tool saves photo_A.raw; the sync client splits it into fixed 4MB chunks
- **The hash** — each chunk is hashed, and the hash becomes the chunk's name in the block store
- **The dedupe** — a chunk already in the store under that hash is never uploaded or stored again
- **The twin** — photo_B.raw shares 2 of its 3 chunks with photo_A, so only 1 new chunk is stored
- **The ledger** — a file is just an ordered list of chunk hashes; the bytes live once in the store

*Example (italic):* photo_A and photo_B name 6 chunks between them, but the store holds only 4 — the 2 shared chunks exist exactly once.

**Key point:** Content-addressed storage names each chunk by the hash of its own bytes — identical chunks collapse into one stored copy, and a file becomes nothing more than an ordered list of hashes.

### Visualization (canvas `c1`, 720×300)

Flow diagram: two files listed as rows of hash-labeled chunk boxes, arrows converging into one block store that holds only the unique chunks.

- **Title (bold 15px, `#1a5276`, top center):** "Hash Each Chunk: Identical Chunks Are Stored Once".
- **Row 1 (boxes at y=64), label 12px `#444` at x=20:** "photo_A.raw"; three blue `#2a78d6` rounded boxes (100×36px, 8px radius, fill `rgba(42,120,214,0.15)`) at x=150, 262, 374 labeled 12px "h=a3f1", "h=9c2e", "h=77b0".
- **Row 2 (boxes at y=134), label:** "photo_B.raw"; boxes at the same x positions labeled "h=a3f1", "h=9c2e", "h=e412" — the first two carry the same hashes as row 1, drawn with a 2px green `#008300` border to mark the duplicates.
- **Block store (y=228):** one wide rounded rect x=150 to x=620, 44px tall, fill `rgba(0,131,0,0.10)`, 12px `#2c3e50` label "block store — 4 unique chunks: a3f1 · 9c2e · 77b0 · e412".
- **Arrows:** 2px `#6b7280` lines from each chunk box down to the store; the four arrows from duplicate hashes land on the same store slots.
- **Annotation (bold 13px green `#008300`, right side near y=110):** "6 chunks named, 4 stored".
- **Caption (12px `#444`, bottom right):** "hashes illustrative".

## The One-Character Edit to a 100MB File

**Tags:** `worked example` (blue), `chunk diff` (green)

- **The setup** — report.bin is 100MB, split into 25 chunks of 4MB; the server holds version 7's list
- **The edit** — one character changes inside chunk 12; the other 24 chunks hash exactly the same
- **The compare** — the client re-hashes all 25 chunks, diffs against v7's list, and finds 1 mismatch
- **The upload** — only chunk 12 (4MB) crosses the wire, plus the new 25-hash list committed as v8
- **The math** — 4MB instead of 100MB is a 25× saving: ~2s instead of ~40s on a 20 Mbps uplink

*Example (italic):* Fixing a typo in the 100MB report uploads 4MB in about 2 seconds; re-uploading the whole file would take about 40.

**Key point:** Sync is a chunk-list diff — the transfer cost scales with how much of the file changed, not with how big the file is.

### Visualization (canvas `c2`, 720×300)

Left: a 5×5 grid of the 25 chunks with only chunk 12 marked changed. Right: two horizontal bars comparing bytes uploaded, full file vs chunk diff.

- **Title (bold 15px, `#1a5276`, top center):** "1 Character Changed → 1 Chunk of 25 Uploaded".
- **Chunk grid:** 25 squares (5 columns × 5 rows), each 34×34px with 8px gaps, grid origin x=70, y=70; 24 squares fill `rgba(42,120,214,0.25)` with 11px `#6b7280` numbers 1–25 centered; chunk 12 fills solid red `#e74c3c` with bold 11px white "12"; 12px `#444` label "report.bin — 25 × 4MB chunks" under the grid at y=292.
- **Bars (right half):** rows at y=110 and y=180, bars start at x=390, 16px tall; labels 12px `#444` above each bar.
  - "full re-upload: 100MB (~40s at 20 Mbps)": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 280.
  - "chunk diff: 4MB (~2s)": solid green `#008300`, width 11 (280 × 4/100).
- **Annotation (bold 13px green `#008300`, near x=390, y=250):** "25× less data on the wire".
- **Caption (12px `#444`, bottom right):** "fixed 4MB chunks, times illustrative".

## Small Metadata Here, Huge Blocks There

**Tags:** `architecture` (blue), `metadata vs blocks` (green), `LAN sync` (orange)

- **The split** — chunk lists, versions, and per-device sync state live in a small metadata database
- **The blocks** — the chunk bytes live in cheap blob storage, named by the hash of their own contents
- **Why split** — metadata is tiny and hot (every sync reads it); blocks are huge, cold, and scale differently
- **The notify** — device B keeps a long-poll connection open; A's commit answers it with "namespace changed"
- **The pull** — B asks the metadata service what changed, then fetches only the missing chunks by hash
- **LAN sync** — if a desktop beside B already holds those chunks, B fetches them over the LAN, not the cloud

*Example (italic):* After A commits, B's long-poll returns in under a second; B pulls the single 4MB chunk from A over the office LAN, never touching the block store.

**Key point:** Separating the metadata service from block storage lets the small consistent part (names, versions, sync state) and the giant dumb part (bytes) scale, cache, and fail independently.

### Visualization (canvas `c3`, 720×300)

Architecture flow diagram: two devices, a metadata service, and a block store, with numbered arrows for commit, notify, and fetch, plus a dashed LAN shortcut.

- **Title (bold 15px, `#1a5276`, top center):** "Commit Metadata, Upload Blocks, Nudge the Other Devices".
- **Boxes (rounded 8px, 12px `#2c3e50` text):** "device A — edits file" blue fill `rgba(42,120,214,0.15)` at (x=40, y=120, 140×44); "metadata service — namespace, versions, sync state" ink-bordered fill `rgba(26,82,118,0.10)` at (x=280, y=48, 200×48); "block store — chunk bytes keyed by hash" green fill `rgba(0,131,0,0.10)` at (x=280, y=204, 200×48); "device B — long-poll open" blue fill at (x=560, y=120, 140×44).
- **Arrows (2px, 11–12px labels in the arrow color):** A→metadata blue "1. commit chunk list v8"; A→block store green "2. upload chunk 12 (4MB)"; metadata→B violet `#4a3aa7` "3. notify: namespace changed"; B→block store green "4. fetch missing chunk by hash".
- **LAN shortcut:** dashed aqua `#199e70` (dash 5/4) arrow straight from A to B along y=180, bold 12px aqua label "LAN sync: B pulls the 4MB chunk from A, not from the cloud".
- **Annotation (bold 13px violet `#4a3aa7`, bottom center near y=285):** "metadata is small and hot; blocks are huge and cold — scale them separately".

## Two Laptops Edit the Same File Offline

**Tags:** `common mistake` (red), `conflicts` (orange)

- **The scene** — laptops A and B both edit budget.xlsx version 7 while offline on the same flight
- **The collision** — both land, come online, and try to commit a version 8 with different chunk lists
- **No merge** — the file is opaque binary chunks to the sync layer; it cannot line-merge a spreadsheet
- **The rule** — the first commit wins v8; the loser is saved beside it as "budget.xlsx (conflicted copy)"
- **The mistake** — last-writer-wins looks simpler but silently throws away one person's work

*Example (italic):* A commits first; B's upload lands as "budget.xlsx (B's conflicted copy)" next to the original — both edits sit in the folder for a human to reconcile.

**Common mistake:** Resolving conflicts by "latest timestamp wins" — device clocks skew, and either way one device's edit is destroyed; for arbitrary binaries, keeping both copies is the only safe generic answer.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: last-writer-wins destroying an edit vs conflicted-copy keeping both, shown as file boxes merging at the server.

- **Title (bold 15px, `#1a5276`, top center):** "Offline Edits Collide: Keep Both, Never Silently Overwrite".
- **Row 1 (y=95), label 12px `#444` at x=20:** "last-writer-wins"; two blue `#2a78d6` rounded boxes at x=170 and x=170 offset stacked (y=72 and y=112, 150×34, fill `rgba(42,120,214,0.15)`) labeled "A: v7 + A's edit" and "B: v7 + B's edit"; 3px arrows converging to a red `#e74c3c` box at x=430 (170×44, fill `rgba(231,76,60,0.12)`) labeled "B overwrites A's version" with bold 12px red "✗ A's edit silently lost" at its right.
- **Row 2 (y=215), label:** "conflicted copy"; the same two blue boxes at y=192 and y=232; arrows to a green `#008300` box at x=430 (190×48, fill `rgba(0,131,0,0.12)`) labeled "budget.xlsx (A) + budget.xlsx (B's conflicted copy)" with bold 12px green "✓ both edits survive".
- **Box style:** 8px radius, 12px `#2c3e50` text, 2px borders in the box color.
- **Annotation (bold 13px orange `#d95926`, centered near y=285):** "sync can move bytes; it cannot merge arbitrary binaries".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); chunk hashes, transfer times, and LAN speeds are invented and labeled illustrative; the arithmetic is exact and must stay consistent between text and charts (100MB / 4MB = 25 chunks, 4/100 = 25× saving, 100MB at 20 Mbps ≈ 40s, 4MB ≈ 2s, bar widths 280 vs 11 = 280 × 4/100).
- **Framing:** the page is a generic design exercise using publicly well-known sync concepts (content-addressed chunking, metadata/block split, long-poll notification, conflicted copies, LAN sync); it makes no claims about the real company's current internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
