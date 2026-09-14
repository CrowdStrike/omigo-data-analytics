# Git & Content-Addressed History

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Git & Content-Addressed History

**Subtitle:** Git names every file, folder, and commit by the hash of its content — so history is a chain of fingerprints that no one can quietly rewrite

## Every Object Is Named by Its Own Content

**Tags:** `core idea` (blue), `content addressing` (green), `SHA-1` (orange)

- **The repo** — a coffee shop keeps `menu.txt` and `prices.txt` in git and commits a change to the menu
- **The blob** — git hashes each file's bytes and stores the content under that hash (`f3a91c2` for the menu)
- **The tree** — a directory is a list of names and blob hashes, itself hashed and stored (`c41d9a0`)
- **The dedup** — `prices.txt` didn't change, so both commits' trees point at the same blob `7d05e8b`, stored once
- **The tamper alarm** — flip one byte in a stored object and its content no longer matches its name
- **Snapshots, not diffs** — every commit stores a full tree; `git diff` is computed on demand between trees

*Example (italic):* After editing only the menu, the new commit adds one new blob and one new tree — the unchanged `prices.txt` blob `7d05e8b` is reused, not copied.

**Key point:** Git is a key-value store where the key is the SHA-1 hash of the value — identical content is stored once, and any corruption is self-evident because the hash would no longer match.

### Visualization (canvas `c1`, 720×300)

Object-graph diagram: two commits, each pointing to its own tree, with the two trees sharing one unchanged blob — showing blob/tree/commit as boxes named by short hashes.

- **Title (bold 15px, `#1a5276`, top center):** "Two Snapshots, Three Object Types, One Shared Blob".
- **Commit boxes (left column, 160×52, 8px radius, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border):** commit 1 at (x=30, y=68) labeled "commit 2ec86f4" + 11px sub-line "tree c41d9a0"; commit 2 at (x=30, y=190) labeled "commit 8b17e5d" + 11px sub-line "tree 6f2a3b9, parent 2ec86f4".
- **Tree boxes (middle column, 140×48, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border):** tree 1 at (x=265, y=70) labeled "tree c41d9a0"; tree 2 at (x=265, y=192) labeled "tree 6f2a3b9".
- **Blob boxes (right column, 170×46, fill `rgba(0,131,0,0.12)`, 2px `#008300` border):** at (x=500, y=30) "blob f3a91c2 — menu v1"; at (x=500, y=127) "blob a90dd41 — menu v2"; at (x=500, y=224) "blob 7d05e8b — prices".
- **Arrows:** 2px `#6b7280` lines with small arrowheads: commit 1 → tree 1, commit 2 → tree 2; tree 1 → menu-v1 blob and prices blob; tree 2 → menu-v2 blob and prices blob; dashed 2px `#4a3aa7` (dash 4/3) arrow from commit 2 up to commit 1 labeled 11px "parent".
- **Annotation (bold 13px green `#008300`, near x=500, y=290):** "prices.txt stored once — both trees point at 7d05e8b".
- **Caption (12px `#444`, bottom left):** "hashes shortened to 7 chars, illustrative".

## Change One Old Byte, Every Hash Downstream Changes

**Tags:** `worked example` (blue), `tamper-evident` (red)

- **The chain** — three commits: `a1f09b3` ← `b7d21c8` ← `c9e44f0`, each storing its parent's hash inside itself
- **The edit** — someone rewrites one price inside the file recorded by the first commit
- **Hand-check step 1** — the edited file is new content, so it hashes to a new blob and a new tree
- **Hand-check step 2** — the first commit's content changed (new tree hash), so it becomes `d3c07a5`
- **The cascade** — the middle commit named parent `a1f09b3`; keeping the edit forces remakes `f18b940`, `05ce7ab`
- **The reveal** — anyone still holding tip `c9e44f0` sees instantly that `05ce7ab` is a different history

*Example (italic):* One edited byte in the oldest commit turns the chain a1f09b3 → b7d21c8 → c9e44f0 into d3c07a5 → f18b940 → 05ce7ab — all three names change.

**Key point:** A commit's ID hashes its tree, parent IDs, author, and message, so each commit transitively seals everything behind it — rewriting any old commit changes its hash and therefore every descendant's.

### Visualization (canvas `c2`, 720×300)

Two-row before/after chain diagram: the original three-commit chain on top, the fully re-hashed chain after tampering with the oldest commit on the bottom.

- **Title (bold 15px, `#1a5276`, top center):** "One Tampered Byte Re-Names the Entire Chain".
- **Row 1 (boxes at y=78), label 12px `#444` at x=20 (y=104):** "original"; three rounded boxes (150×46, 8px radius, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 13px `#2c3e50` text) at x=110 "a1f09b3", x=320 "b7d21c8", x=530 "c9e44f0"; 2px `#6b7280` arrows pointing LEFT (child to parent) between boxes, 11px `#6b7280` label "parent" above each arrow.
- **Row 2 (boxes at y=192), label at x=20 (y=218):** "after edit"; same geometry, boxes fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border: x=110 "d3c07a5", x=320 "f18b940", x=530 "05ce7ab".
- **Edit marker:** bold 12px red `#e74c3c` text "1 byte changed here" at (x=110, y=258) with a short 2px red arrow up into the first row-2 box.
- **Cascade arrows:** dashed 2px `#e74c3c` (dash 4/3) vertical arrows from each row-1 box down to its row-2 counterpart, 11px red labels "new hash" beside each.
- **Annotation (bold 13px `#1a5276`, right side near y=150):** "no descendant survives — history is tamper-evident by construction".
- **Caption (12px `#444`, bottom right):** "hashes illustrative; the cascade is exact behavior".

## A Branch Is a 41-Byte File Pointing Into the DAG

**Tags:** `where it's used` (blue), `DAG` (green), `branching` (orange)

- **The DAG** — commits form a directed acyclic graph: arrows go child-to-parent, never in a cycle
- **The merge** — a merge commit is nothing special: an ordinary commit that lists two parent hashes
- **The branch** — `main` is a file holding one 40-char hash plus a newline: 41 bytes, freely movable
- **Free branching** — creating a branch copies nothing; it writes one tiny pointer file, so teams branch constantly
- **Distributed** — every clone holds the full DAG, and shared hashes guarantee everyone's objects agree

*Example (italic):* The coffee shop forks a `feature` branch, adds 2 commits, and merges — the merge commit 91c4e07 records both parents 3fd82b1 and e57a90c.

**Key point:** Because a branch is only a movable pointer into an immutable DAG, branching and merging cost almost nothing — the design decision that reshaped how everyone works.

### Visualization (canvas `c3`, 720×300)

Commit DAG diagram: a main line of commits, a feature branch that forks and merges back, and branch-pointer tags showing that each branch is just a tiny file.

- **Title (bold 15px, `#1a5276`, top center):** "The DAG: a Merge Commit Simply Has Two Parents".
- **Main-line commits (circles radius 22, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 11px `#2c3e50` hash labels inside):** at (x=90, y=130) "0b3d5f2", (x=230, y=130) "7a91cc4", (x=370, y=130) "3fd82b1", and the merge at (x=610, y=130) "91c4e07" with fill `rgba(74,58,167,0.15)` and 2px `#4a3aa7` border.
- **Feature commits (same circle style, fill `rgba(0,131,0,0.12)`, 2px `#008300` border):** at (x=440, y=230) "b26f014", (x=550, y=230) "e57a90c".
- **Parent arrows (2px `#6b7280`, arrowheads, child to parent):** 7a91cc4→0b3d5f2, 3fd82b1→7a91cc4, b26f014→3fd82b1, e57a90c→b26f014, and TWO arrows out of the merge: 91c4e07→3fd82b1 and 91c4e07→e57a90c, the second in 2px `#4a3aa7`.
- **Branch tags (rounded 8px-radius pills, 12px bold text):** "main" pill (fill `rgba(42,120,214,0.25)`, `#1a5276` text) at (x=585, y=55) with a short arrow down to 91c4e07; "feature" pill (fill `rgba(0,131,0,0.20)`, `#008300` text) at (x=530, y=283) with a short arrow up to e57a90c.
- **Annotation (bold 13px orange `#d95926`, near x=60, y=60):** "each branch pointer: one 41-byte file".
- **Caption (12px `#444`, bottom left):** "hashes illustrative; arrows point child → parent".

## Rebase Never Edits a Commit — It Makes New Ones

**Tags:** `common mistake` (red), `rebase` (orange), `reflog` (blue)

- **The confusion** — "rewriting history" sounds like editing old commits in place; git cannot do that
- **What really happens** — rebase and amend manufacture brand-new commits, then move the branch pointer
- **The originals persist** — the old commits stay in the object store until garbage collection runs
- **The rescue** — `git reflog` records where the pointer used to be, so "lost" work is one reset away
- **The mistake** — panicking after a bad rebase and re-typing work that is still sitting in the object store

*Example (italic):* A botched rebase moves `feature` to 4c1d88e, but reflog still shows the old tip e57a90c — `git reset --hard e57a90c` restores it untouched.

**Common mistake:** Believing a rebase destroyed your commits. Objects are immutable; only the pointer moved, and until gc runs the reflog can point you straight back to the originals.

### Visualization (canvas `c4`, 720×300)

Before/after rebase diagram: old feature commits grayed but still present in the object store, new replacement commits on top of main, the branch pointer moved, and a reflog arrow back to the old tip.

- **Title (bold 15px, `#1a5276`, top center):** "Rebase = New Commits + a Moved Pointer (Old Ones Remain)".
- **Main line (circles radius 20, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 11px labels):** at (x=80, y=150) "0b3d5f2", (x=210, y=150) "7a91cc4", (x=340, y=150) "3fd82b1".
- **Old feature commits (fill `rgba(107,114,128,0.12)`, 2px dashed `#6b7280` border, 11px `#6b7280` labels):** at (x=430, y=245) "b26f014", (x=545, y=245) "e57a90c"; parent arrows in dashed 2px `#6b7280`: b26f014→7a91cc4, e57a90c→b26f014; 11px `#6b7280` caption under them (y=285): "still in the object store until gc".
- **New rebased commits (fill `rgba(0,131,0,0.12)`, 2px `#008300` border):** at (x=470, y=70) "9d02a6e", (x=590, y=70) "4c1d88e"; solid 2px `#6b7280` parent arrows: 9d02a6e→3fd82b1, 4c1d88e→9d02a6e.
- **Branch pointer:** "feature" pill (fill `rgba(0,131,0,0.20)`, bold 12px `#008300`) at (x=640, y=25) with an arrow to 4c1d88e; a dashed 2px `#d95926` (dash 4/3) curved arrow from the pill down to e57a90c labeled bold 12px orange "reflog remembers".
- **Annotation (bold 13px `#e74c3c`, near x=80, y=60):** "nothing was edited — two new commits were made".
- **Caption (12px `#444`, bottom right):** "hashes illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all box/circle positions and hash strings are the hardcoded literals above (no randomness); every 7-char hash is invented and labeled illustrative; the cascade behavior (one edit re-hashes all descendants), the two-parent merge structure, and the 41-byte branch-file size (40 hex chars + newline) are exact git behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
