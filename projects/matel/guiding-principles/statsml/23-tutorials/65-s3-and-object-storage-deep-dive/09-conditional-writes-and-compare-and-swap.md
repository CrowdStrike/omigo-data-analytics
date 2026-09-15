# Conditional Writes &amp; Compare-and-Swap

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Conditional Writes &amp; Compare-and-Swap

**Subtitle:** Two conditions you can attach to a write — only if nothing is there yet, and only if nothing has changed since you read it — turn a silently lost update into a rejection you can retry

## Two Writers, One Key, One Lost Update

**Tags:** `core idea` (blue), `lost update` (red), `running example` (orange)

- **The setup** — one small file in a bucket holds the current version of a dataset
- **Both read version 7** — two jobs, Alice and Bob, read the file and see the same version
- **Both write version 8** — each one computes its own next version and writes the same name
- **Last write wins** — the file ends up holding Bob's bytes only, as if Alice never wrote
- **Alice's work is gone** — her data files are still in the bucket but never listed again
- **No error either way** — both writes succeed and neither writer is warned about the other
- **Retrying will not help** — both writes already succeeded, so there is nothing to redo
- **This is a lost update** — read, compute, write, with another writer landing in between

*Example (illustrative):* Alice's version adds 3 data files and Bob's adds 2; the file ends up listing Bob's 2, and Alice's 3 are never read again.

**Key point:** Reading and then writing are two separate steps. Nothing in a plain write stops another writer from landing in between.

### Visualization (canvas `c1`, 720×300)

Two-lane sequence of the lost update: both writers read v7, both write v8, Bob's write replaces Alice's, and nothing reports a conflict.

- **Time mapping:** `x0 = 130`, `x1 = 660`, `tMax = 10`, so `tx(t) = 130 + (x1 − x0) × t / tMax = 130 + 53t`. All event and box positions are computed from `tx` in JS.
- **Title (bold 15px, `#1a5276`, centered at y=22):** "The Lost Update: Both Read v7, Both Write v8".
- **Annotation (bold 13px red `#e74c3c`, centered at y=46):** "both writes succeeded — nothing reported a conflict".
- **Lane rules:** 1px `#e5e9ef` lines at y=96 (Alice) and y=158 (Bob) from `x0` to `x1`, each with a grey arrow head at the right end; 12px `#6b7280` "time →" right-aligned at (660, 138).
- **Lane labels (bold 13px, left-aligned at x=50):** "Alice" `#2a78d6` at y=96, "Bob" `#4a3aa7` at y=158, "the file" `#1a5276` at y=236.
- **Alice's events (blue `#2a78d6`, r=6 dots, bold 12px labels centered 14px above the lane):** `tx(1)` "reads v7"; `tx(5)` "writes v8 — accepted".
- **Bob's events (violet `#4a3aa7`, r=6 dots, bold 12px labels centered 22px below the lane):** `tx(2)` "reads v7"; `tx(7)` "writes v8 — accepted".
- **File-state boxes (height 32, 6px radius, y=216, 12.5px monospace `#2c3e50` centered):** "v7" width 104 centered at `tx(1.5)`, fill `rgba(26,82,118,0.10)` border 1.5px `#1a5276`; "v8 (Alice)" width 104 centered at `tx(5)`, fill `rgba(42,120,214,0.15)` border 2px `#2a78d6`; "v8 (Bob)" width 104 centered at `tx(7)`, fill `rgba(74,58,167,0.15)` border 2px `#4a3aa7`.
- **Vertical connectors (1.5px dashed 3/3):** from each read dot down to the "v7" box in the actor's colour; from each write dot down to the box it produces.
- **Overwrite mark:** 2.5px red `#e74c3c` diagonal cross inside the "v8 (Alice)" box, and bold 12px red "overwritten" centered under it at (`tx(5)`, 266).
- **Side note (12px `#6b7280`, left-aligned at (60, 204)):** "Alice's 3 files are dropped; Bob's 2 are all that is left".
- **Caption (12px `#444`, bottom right):** "two-writer trace illustrative; last write wins on a plain write".

## Write Only If Nothing Is There Yet

**Tags:** `worked example` (blue), `write if absent` (green), `rejected` (red)

- **The condition** — the write is allowed only if nothing exists yet at that name
- **One winner** — the storage service picks the winner itself, with no lock service
- **The loser is told** — its write is rejected outright and nothing at all gets stored
- **Alice wins** — her version 8 file is created, and that name now belongs to her
- **Bob is rejected** — his write of the same name fails instead of overwriting it
- **Bob reads again** — he re-reads Alice's version 8 and uses it as his new base
- **Bob replays his change** — he adds his own 2 files on top and then writes version 9
- **Nothing is lost** — version 9 lists all 5 data files: Alice's 3 plus Bob's 2

*Example (illustrative):* Bob now makes two write attempts instead of one — the first rejected, the second accepted — and version 9 lists 3 + 2 = 5 data files.

**Key point:** A rejection is the good outcome. The loser finds out it lost and can redo its work on top of the winner, so no work disappears.

### Visualization (canvas `c2`, 720×300)

The same two-lane sequence with the condition attached: Alice's write is accepted, Bob's is rejected, then Bob re-reads and lands version 9. Both writers' work survives.

- **Time mapping:** `x0 = 130`, `x1 = 670`, `tMax = 10`, so `tx(t) = 130 + 54t`. All event and box positions are computed from `tx` in JS.
- **Title (bold 15px, `#1a5276`, centered at y=22):** "The Same Race, Now Rejected Instead of Overwritten".
- **Survival annotation (bold 13px green `#008300`, centered at y=44):** "all 5 files present at v9 — 3 from Alice, 2 from Bob".
- **No-lock annotation (bold 12px violet `#4a3aa7`, centered at y=62):** "no lock service — the bucket alone picked the winner".
- **Lane rules:** 1px `#e5e9ef` lines at y=100 (Alice) and y=176 (Bob) from `x0` to `x1`.
- **Lane labels (bold 13px, left-aligned at x=50):** "Alice" `#2a78d6` at y=100, "Bob" `#4a3aa7` at y=176, "key state" `#1a5276` at y=246.
- **Alice's events (blue, r=6 dots, bold 12px labels centered 14px above the lane):** `tx(1)` "reads v7"; `tx(4.5)` "writes v8 — accepted".
- **Bob's events (r=6 dots, bold 12px labels centered 22px below the lane, the third on a second line at +38 to clear the rejected label):** `tx(2)` violet "reads v7"; `tx(5.5)` red `#e74c3c` "writes v8 — rejected"; `tx(7.5)` violet "re-reads v8, adds 2 files" (drawn at +38); `tx(8.5)` green `#008300` "writes v9 — accepted".
- **Key-state boxes (height 28, 6px radius, y=228):** "absent" width 80 centered at `tx(1)`, fill `#f4f6f9` border 1.5px `#e5e9ef`, label 12px `#6b7280`; "v8 = Alice" width 150 centered at `tx(4.5)`, fill `rgba(42,120,214,0.15)` border 2px `#2a78d6`; "v9 = Alice + Bob" width 170 centered at `tx(8.5)`, fill `rgba(0,131,0,0.14)` border 2px `#008300`; the last two labelled 12px monospace `#2c3e50`.
- **Rejection mark:** bold 15px red `#e74c3c` "✗" above Bob's rejected dot, and a 1.5px red dashed 3/3 vertical line from just below that dot's label (y = `bY + 28`) down to y=222 — it starts under the label so it does not strike through it, and stops short of every state box, showing that no state changed.
- **Success connectors (1.5px dashed 3/3):** Alice's write dot → the "v8 = Alice" box in blue; Bob's final write dot → the "v9 = Alice + Bob" box in green.
- **Time axis:** 1.5px `#6b7280` line at y=272 from `x0` to `x1` with an arrow head; 12px `#6b7280` "time →" right-aligned at (`x1`, 264).
- **Caption (12px `#444`, bottom right):** "trace illustrative; rejecting a write when the name exists is documented".

## Write Only If It Has Not Changed

**Tags:** `compare-and-swap` (blue), `write if unchanged` (green), `one file only` (red)

- **The condition** — the write is allowed only if the file is still the one you read
- **A change tag** — every write gives the file a new tag; the service calls it an ETag
- **Compare and swap** — read the tag, compute the change, write back with that same tag
- **Both read the same tag** — Alice and Bob see the file at tag e1 before either writes
- **Alice's write lands** — it is accepted, and the file's tag moves from e1 to e2
- **Bob's write fails** — the tag he read, e1, no longer matches the file's current e2
- **Bob retries** — he reads again at e2, redoes his change, and lands the file at tag e3
- **One file only** — two files still cannot be changed together in a single write

*Example (illustrative):* Bob's retry reads the file again, sees tag e2 and Alice's change, redoes his own change on top, and his write is accepted at tag e3.

**Key point:** This is the classic compare-and-swap, done with one write. It covers one file at a time, which is why a commit step funnels through a single file.

### Visualization (canvas `c3`, 720×300)

State machine for one file: tag e1 → e2 as Alice's conditional write is accepted, Bob's rejected because the tag he read is stale, plus an honest one-file note.

- **Title (bold 15px, `#1a5276`, centered at y=22):** "Compare-and-Swap on One File: One Write Lands, the Other Is Rejected".
- **State circles (radius 38, centers on y=120, x=180 and x=470):** "e1" fill `rgba(26,82,118,0.10)` border 2.5px `#1a5276`; "e2" fill `rgba(0,131,0,0.13)` border 2.5px `#008300`; bold 16px monospace tag label centered in the border colour, 12px `#6b7280` caption 56px below each centre — "the file is v7" and "the file is v8".
- **Accepted transition:** 2.5px `#008300` curve from (218,108) through control (325,62) to (430,106) with an arrow head at the right end; bold 12.5px `#008300` "Alice: write if still e1 → accepted" centered at (325, 56).
- **Reads (12px `#6b7280`, left-aligned at (60, 224) and (60, 242)):** "Alice reads → tag e1" and "Bob reads → tag e1"; one 1.5px dashed 3/3 grey line from (150,216) up to (164,156), the lower edge of the e1 circle.
- **Rejected transition:** 2.5px red `#e74c3c` line from (196,158) to (296,194) with no arrow head, and a bold 18px red "✗" at (308,200) showing the write never lands; bold 12.5px red "Bob: write if still e1 → rejected" left-aligned at (325, 198).
- **Staleness note (bold 12px magenta `#d55181`, left-aligned at (325, 218)):** "the e1 Bob read no longer matches the current e2".
- **Retry path:** 2px violet `#4a3aa7` dashed 4/3 arrow from (512,120) to (610,120) with an arrow head; two 12px violet lines left-aligned at (552, 152) and (552, 168): "Bob re-reads e2," / "retries → e3".
- **Limit strip (x=50, y=262, width 620, height 30, 6px radius):** fill `rgba(217,89,38,0.12)`, border 2px `#d95926`, bold 12.5px `#d95926` centered "one file at a time — two files cannot be changed together in one write" (kept short so it fits inside the 620px strip).
- **No caption:** the limit strip carries the honest note. The tags `e1`, `e2`, `e3` are short symbolic placeholders, not real hash values.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `01-buckets-keys-and-the-flat-namespace.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead`, `rgba`, `dot`, `dashLine` and `box` as on this page.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`, red `#e74c3c`. Red is reserved for genuine failure: the overwrite in `c1`, the rejected write in `c2`, the rejected write in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, so each holds one line at the 50/50 split without wrapping while still carrying a full clause. An earlier pass clipped them to ~55–70 characters and was rejected for compromising quality; do not clip them again, and equally do not pad them back out to the folder default (~90–100). Eight bullets per section, three sections, one canvas each.
- **Register and vocabulary.** Plain simple technical English for a first-time reader, with much less API surface than the earlier draft: say "reads" and "writes", not GET and PUT; say "accepted" / "rejected", not 200 and 412 Precondition Failed; say "write only if nothing exists yet at that name" and "write only if the file is still the one you read", not `If-None-Match: *` and `If-Match` — those header names teach nothing here and appear nowhere on the page. `ETag` is named exactly once, in one bullet, because the tag it refers to is drawn in `c3`. No analogies, no invented scenes, no story framing: Alice and Bob are two jobs writing to one file, nothing more.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No history section.** A fourth section covered the pre-2024 workaround (an external catalog or log store holding the commit decision, Iceberg via DynamoDB or Glue, Delta's log store, a split system of record, its operational cost, and a `c4` architecture diagram). It was cut for being product and architecture history rather than the concept itself, and for pushing a tutorial page past what it needs. That material belongs on the table-format commit page, not here.
  - **No feature-launch dates or status codes.** The November 2024 launch, the December 2020 consistency change, and the numeric HTTP codes went with it; they are reference detail, not tutorial content.
  - **No conditional-read aside.** Conditional reads that save bandwidth are a different feature and only confuse the point here.
  - **Nothing folded back in from the cut section** — the three surviving sections were not lengthened to absorb it.
- **Data:** every value is a hardcoded literal; no `Math.random()` anywhere.
  - **Illustrative and labelled as such:** Alice and Bob, the version numbers v7/v8/v9, the tags `e1`/`e2`/`e3`, and the file counts. Each chart caption or strip says so.
  - **Arithmetic that must reconcile:** Alice contributes 3 data files and Bob 2, so section one's outcome references Bob's 2 alone while section two's outcome references 3 + 2 = 5. Bob makes 2 write attempts in section two against Alice's 1.
  - **Documented behaviour the page stands on:** a plain write to an existing name replaces it and reports success, so read-then-write from two writers can lose an update; a write can be conditioned on nothing existing at that name, and the losing writer is rejected with nothing stored; a write can be conditioned on the file still carrying the tag you read, giving compare-and-swap on that one file; both conditions cover a single file only, with no multi-file atomicity.
  - **Computed geometry:** `c1` maps t=0…10 onto x=130…660 (`tx(t) = 130 + 53t`) and `c2` onto x=130…670 (`tx(t) = 130 + 54t`); every dot, box centre and connector is derived from `tx` in JS at render time rather than hardcoded. `c3`'s two state centres (x=180, x=470 on y=120) are documented literals.
  - **No credential-shaped strings** appear anywhere on the page; `e1`/`e2`/`e3` are deliberately not realistic hashes.
