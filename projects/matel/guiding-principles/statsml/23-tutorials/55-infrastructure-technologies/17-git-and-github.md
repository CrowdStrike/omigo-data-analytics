# Git & GitHub

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Git & GitHub

**Subtitle:** Git stores your project as a graph of snapshots named by their own hashes — GitHub put that graph on the web and turned version control into the social network of code

## Three Commits of a Report Script

**Tags:** `core idea` (blue), `snapshots` (green), `content-addressed` (orange)

- **The repo** — a data scientist tracks two files, `report.py` and `data.csv`; three saves become three commits
- **The snapshot** — each commit stores a full picture of every file, not the lines that changed
- **The hash** — a commit's name (shown as `a4f9`) is computed from its content plus its parent's hash
- **The chain** — `c1d8` points to parent `b7e2`, which points to `a4f9`: history is a graph, not a log
- **The pointer** — the branch `main` is just a movable label sitting on the newest commit
- **All local** — the entire graph lives on the laptop; committing never needs a server

*Example (italic):* Commit `c1d8` changes only `report.py`, yet it records a complete snapshot of both files — the unchanged `data.csv` is reused by hash, not copied again.

**Key point:** A Git commit is a content-addressed snapshot of the whole project — the hash names the content, the parent link builds the graph, and a branch is only a pointer to one node.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the three-commit chain: snapshot boxes linked by parent arrows, with the `main` pointer on the newest commit.

- **Title (bold 15px, `#1a5276`, top center):** "History Is a Chain of Snapshots, Each Named by Its Hash".
- **Commit boxes (left to right at y=120, each 160px wide, 84px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border):** box 1 at x=60 header bold 13px `#1a5276` "a4f9", body 12px `#2c3e50` "report.py v1" / "data.csv v1"; box 2 at x=280 header "b7e2", body "report.py v2" / "data.csv v1"; box 3 at x=500 header "c1d8", body "report.py v3" / "data.csv v1".
- **Parent arrows:** 3px `#6b7280` arrows pointing LEFT (child to parent): from box 2's left edge to box 1's right edge, from box 3's left edge to box 2's right edge; 11px `#6b7280` label "parent" above each arrow.
- **Branch pointer:** green `#008300` rounded tag (fill `rgba(0,131,0,0.12)`, bold 12px text "main") at (x=545, y=70), 2px green arrow down to the top of box 3.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=250):** "each box is a full snapshot — data.csv v1 is stored once and shared by hash".
- **Caption (12px `#444`, bottom right):** "4-char hashes invented for display; real Git hashes are 40 hex characters (exact)".

## A Branch, a Merge, and Two Parents

**Tags:** `worked example` (blue), `branching` (green)

- **The branch** — `git branch plot-fix` adds one more pointer at `c1d8`; zero files are copied
- **Two lines** — commit `d0c3` grows `plot-fix` while commit `e5a1` grows `main`; the graph forks
- **The merge** — commit `f9b4` has two parents, `e5a1` and `d0c3`, stitching the fork back together
- **Hand-check** — parent counts: `a4f9` has 0, then `b7e2` `c1d8` `d0c3` `e5a1` have 1 each, `f9b4` has 2 (exact)
- **Cheap by design** — a new branch costs one tiny file holding a hash, which is why Git users branch freely

*Example (italic):* After the merge, `main` moves to `f9b4`; deleting `plot-fix` removes only the label — commit `d0c3` stays reachable through the merge's second parent.

**Key point:** Branching and merging are pointer operations on the graph, not file copies — a merge commit is just a snapshot with two parents.

### Visualization (canvas `c2`, 720×300)

DAG of the full six-commit history: the straight chain, the fork into two lanes, and the merge node with two parent arrows.

- **Title (bold 15px, `#1a5276`, top center):** "The Fork and the Merge: One Graph, Six Commits, Two Parents at f9b4".
- **Nodes (circles radius 26, 2px border, fill `rgba(42,120,214,0.15)` blue `#2a78d6` unless noted, bold 12px `#1a5276` hash label centered):** `a4f9` at (90,160), `b7e2` at (200,160), `c1d8` at (310,160), `e5a1` at (450,95) on the main lane, `d0c3` at (450,225) on the plot-fix lane, merge `f9b4` at (590,160) with fill `rgba(0,131,0,0.12)` and 2px `#008300` border.
- **Parent arrows (3px `#6b7280`, pointing left/back toward parents):** b7e2→a4f9, c1d8→b7e2, e5a1→c1d8 (diagonal up-lane), d0c3→c1d8 (diagonal down-lane), f9b4→e5a1 and f9b4→d0c3 (the two merge parents).
- **Lane labels:** bold 12px green `#008300` tag "main" at (600, 55) with arrow to `f9b4`; bold 12px orange `#d95926` tag "plot-fix" at (440, 275) with arrow to `d0c3`.
- **Annotation (bold 13px green `#008300`, near x=560, y=230):** "f9b4 has 2 parents — that is all a merge is".
- **Caption (12px `#444`, bottom right):** "hashes invented for display; parent counts exact for this graph".

## GitHub Made the Graph Social

**Tags:** `where it's used` (blue), `history` (orange), `pull requests` (green)

- **Born in a feud** — Linus Torvalds wrote Git in 2005, in a few weeks, after the BitKeeper license fallout
- **Distributed** — every clone holds the full graph; no central server is required to work or commit
- **GitHub, 2008** — put repositories on the web and made them social: forks, pull requests, issues, review
- **The pull request** — "here is my branch, please review and merge it" became the unit of collaboration
- **Network effects** — code, contributors, and reputation pooled in one place; open source made it home
- **The exit** — Microsoft acquired GitHub in 2018, by which point the workflow was the industry default

*Example (italic):* A common interview exercise today is simply "open a pull request against this repo" — the GitHub workflow is itself a tested job skill.

**Key point:** Git supplied the distributed snapshot graph; GitHub wrapped a social layer — fork, pull request, review, merge — around it, and network effects made the pair the default home of open source.

### Visualization (canvas `c3`, 720×300)

Two-lane flow diagram of the pull request loop: your fork's lane below, the maintainer's repo above, with the review/push-fixes cycle in the middle.

- **Title (bold 15px, `#1a5276`, top center):** "The Pull Request Loop: How Strangers Ship Code Together".
- **Lane labels (12px `#444`, left-aligned at x=20):** "maintainer's repo" at y=95, "your fork" at y=215; a 1px `#e5e9ef` horizontal divider at y=155 from x=15 to x=705.
- **Bottom lane boxes (y=190, each 130px wide, 40px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text):** "fork the repo" at x=130, "branch + commit" at x=300, "push fixes" at x=470; 3px `#6b7280` arrows left-to-right between the first two.
- **Top lane boxes (y=70, same style):** "open pull request" at x=300 fill `rgba(230,126,34,0.15)` border `#e67e22`; "review comments" at x=470 fill `rgba(213,81,129,0.12)` border `#d55181`; "merge" at x=620, 80px wide, fill `rgba(0,131,0,0.12)` border 2px `#008300` with bold 12px green "✓" after the label.
- **Cross-lane arrows (3px):** blue `#2a78d6` up-arrow from "branch + commit" to "open pull request"; magenta `#d55181` down-arrow from "review comments" to "push fixes"; blue up-arrow from "push fixes" back to "review comments" (the loop); green `#008300` arrow from "review comments" to "merge".
- **Loop label (bold 12px magenta `#d55181`, near x=540, y=150):** "review ↔ fix, until approved".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the pull request turned merging branches into a public conversation".

## Commits Are Not Diffs

**Tags:** `common mistake` (red), `storage model` (orange)

- **The belief** — many users think a commit stores "the diff": the lines added and the lines removed
- **The reality** — a commit stores a full snapshot tree; `git diff` computes differences on demand
- **No duplication** — an unchanged file keeps its hash, so every snapshot shares the same stored copy
- **Hand-check** — over the first three commits `data.csv` never changed: Git stores 4 blobs, not 6 (exact)
- **Why it bites** — diff-thinking makes rebase, cherry-pick, and "where did my change go" feel like magic

*Example (italic):* Cherry-picking `d0c3` does not replay a patch file stored inside the commit — Git computes the diff against `d0c3`'s parent at that moment, then applies it.

**Common mistake:** Reading `git show`'s diff output as the commit's contents. The diff is a view computed by comparing two snapshots; the commit itself stores the whole tree.

### Visualization (canvas `c4`, 720×300)

Two-row diagram contrasting the wrong mental model (a pile of patches) with what Git actually stores (snapshots sharing blobs by hash), for the first three commits.

- **Title (bold 15px, `#1a5276`, top center):** "What a Commit Stores: Not a Patch, a Whole Snapshot".
- **Row 1 (y=85), label 12px `#444` at x=20:** "the mental model (wrong)"; three rounded boxes 130px wide, 40px tall at x=190, x=360, x=530, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, 12px text "patch #1", "patch #2", "patch #3", joined by 3px `#6b7280` arrows; bold 12px red `#e74c3c` label "✗ diffs are computed, never stored" at (x=190, y=135).
- **Row 2 (y=185), label:** "what Git stores"; three snapshot boxes 150px wide, 56px tall at x=170, x=350, x=530, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px two-line text "a4f9: report v1 + csv", "b7e2: report v2 + csv", "c1d8: report v3 + csv"; a single green `#008300` rounded blob box 110px wide, 32px tall at (x=370, y=258) labeled "data.csv blob ×1" with thin 1.5px dashed `#008300` lines up to all three snapshot boxes.
- **Blob count tally (bold 12px green `#008300`, right side near x=560, y=270):** "4 blobs stored, not 6".
- **Annotation (bold 13px orange `#d95926`, near x=190, y=160):** "snapshots share unchanged files by hash".
- **Caption (12px `#444`, bottom right):** "blob counts exact for this 3-commit example".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all node positions, box geometry, and labels are the hardcoded values above (no randomness); the 4-char hashes (`a4f9`, `b7e2`, `c1d8`, `d0c3`, `e5a1`, `f9b4`) are invented for display and labeled as such; parent counts (merge = 2 parents) and the blob tally (4 stored vs 6 naive) are exact for this example graph; the historical facts (Git 2005 after the BitKeeper fallout, GitHub 2008, Microsoft acquisition 2018) are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
