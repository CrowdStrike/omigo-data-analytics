# Aider

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Aider

**Subtitle:** Aider is a terminal AI pair programmer that commits every AI edit to git — so any change it makes is one `git revert` away from undone

## Every AI Edit Becomes a Commit

**Tags:** `core idea` (blue), `git safety net` (green), `terminal tool` (orange)

- **The tool** — Aider runs in the terminal, chats about your repo, and edits the files directly
- **The ask** — you type "rename `calc_total` to `order_total` everywhere" and it applies the diff
- **The signature move** — the instant an edit lands, Aider auto-commits it with a descriptive message
- **The undo** — a bad edit is not a cleanup job; it is one `git revert` of one commit
- **The contrast** — pasting AI code by hand blends good and bad changes into one tangled working tree

*Example (italic):* Aider renames the function across 3 files and commits "rename calc_total to order_total"; you dislike it, run `git revert HEAD`, and the repo is back in seconds.

**Key point:** Aider turns every AI change into its own git commit, so undoing the AI never means untangling — it means reverting exactly one commit.

### Visualization (canvas `c1`, 720×300)

Two-lane diagram comparing undo paths: hand-pasted AI code (one tangled blob, manual unpick) vs Aider (a chain of small commits, one reverted cleanly).

- **Title (bold 15px, `#1a5276`, top center):** "Undoing a Bad AI Edit: Tangled Paste vs One Revert".
- **Lane labels (12px `#444`, left at x=20):** "hand paste" at y=110, "aider" at y=220.
- **Lane 1 (y=90–130):** one wide rounded box x=110 to x=520, 40px tall, fill `rgba(217,89,38,0.15)`, 2px `#d95926` border, 12px `#2c3e50` label "3 edits mixed in one working tree"; 3px `#d95926` arrow to bold 12px red `#e74c3c` text at x=545 "unpick by hand".
- **Lane 2 (y=200–240):** four rounded boxes 85px wide, 40px tall at x = 110, 215, 320, 425, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px labels "commit 1", "commit 2", "commit 3", "commit 4"; commit 4 box redrawn with fill `rgba(231,76,60,0.12)` and 2px `#e74c3c` border; 3px `#008300` arrow from commit 4 to bold 12px green `#008300` text at x=545 "git revert — done".
- **Connector arrows between lane-2 boxes:** 2px `#6b7280` horizontal arrows in the 20px gaps.
- **Annotation (bold 13px `#1a5276`, centered near y=270):** "small commits make the bad one cheap to remove".
- **Caption (12px `#444`, bottom right):** "boxes schematic".

## A Session, Commit by Commit

**Tags:** `worked example` (blue), `git revert` (green)

- **The session** — six asks against a coffee-shop orders script, each landing as its own commit
- **Files touched per edit** — `[2, 1, 3, 4, 1, 2]` files for edits 1 through 6 (illustrative)
- **The bad one** — edit 4 swaps the CSV parser and the test suite goes red
- **The fix** — `git revert` of commit 4 alone; its 4 files snap back, the other 9 file-changes stay
- **Hand-check** — 13 file-changes across the session; the revert touches exactly 4, never the other 9
- **The trail** — the revert is itself a commit, so history shows the mistake and the recovery

*Example (italic):* Edit 4 breaks the tests at minute 18; one revert at minute 19 restores green, and edits 5 and 6 continue on top as if nothing happened.

**Key point:** Because each AI edit is isolated in one commit, reverting edit 4 cannot disturb edits 1–3 or 5–6 — the blast radius of a bad edit is exactly one commit.

### Visualization (canvas `c2`, 720×300)

Timeline of the session: seven commit boxes on a shared minute axis (six edits plus the revert), with a test-status strip below going green → red → green.

- **Title (bold 15px, `#1a5276`, top center):** "Six Edits, One Bad, One Revert: the Session as Commits".
- **Axis:** horizontal 2px `#999` baseline at y=245, from x=60 to x=660; minute tick labels "0"–"30" every 5 minutes (12px `#444`), edits at minutes `[2, 7, 12, 18, 19, 23, 28]`.
- **Commit boxes (70px wide, 44px tall, centered above their minute, tops at y=120):** edits 1–3 fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` border, 12px labels "edit 1 · 2 files", "edit 2 · 1 file", "edit 3 · 3 files"; edit 4 fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, label "edit 4 · 4 files"; revert box fill `rgba(230,126,34,0.15)`, 2px `#e67e22` border, label "revert 4"; edits 5–6 blue again, labels "edit 5 · 1 file", "edit 6 · 2 files".
- **Test strip (14px tall, y=200–214):** segments along the axis — green `#008300` fill from minute 0 to 18, red `#e74c3c` fill from 18 to 19, green from 19 to 30; 11px labels "pass" / "fail" / "pass" inside the segments.
- **Annotation (bold 13px green `#008300`, near minute 21, y=90):** "1 minute red — revert of one commit, not a cleanup".
- **Caption (12px `#444`, bottom right):** "minutes and file counts illustrative".

## Cheap Undo Makes Bold Delegation

**Tags:** `where it's used` (blue), `repo map` (green), `many models` (orange)

- **The safety math** — when undo costs seconds instead of an afternoon, you can hand the AI bigger jobs
- **The repo map** — Aider builds a compact map of the repo's files and symbols so the model sees the right context
- **Model choice** — it is open source and connects to many model providers rather than one vendor
- **The behavior shift** — people who trust the undo ask for multi-file refactors, not one-line patches
- **The general lesson** — revertibility, not model quality alone, sets how much you dare to delegate

*Example (italic):* A developer who would never paste an AI's 4-file refactor by hand accepts it from Aider, because a wrong guess costs one revert instead of an hour of unpicking.

**Key point:** Making AI edits cheap to undo changes the delegation calculus — the safety net, as much as the model, decides how large a task you are willing to hand over.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: size of task people are willing to delegate at three different undo costs (illustrative).

- **Title (bold 15px, `#1a5276`, top center):** "The Bolder You Can Undo, the Bigger You Delegate".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 420; no numeric x-axis — bar-end labels carry the values.
- **Rows (bar tops at y = 80, 145, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "undo = 1 revert (aider)": green `#008300` bar width 420, 12px bar-end label "multi-file refactor"
  - "undo = manual diff unpick": blue `#2a78d6` bar width 180, 12px bar-end label "one function"
  - "undo = unclear / mixed tree": orange `#d95926` bar width 60, 12px bar-end label "one line, maybe"
- **Bar style:** 26px tall, fills at 0.30 alpha of their color with a solid 2px border in the same color.
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "the safety net sets the task size, not just the model".
- **Caption (12px `#444`, bottom right):** "bar widths illustrative".

## A Revert Is Not a Review

**Tags:** `common mistake` (red), `history` (orange)

- **The confusion** — "I can always revert" slides into "I don't need to read the diff"
- **The silent bug** — a plausible edit that passes tests ships unread; revertible, but nobody knew to revert
- **What revert does** — `git revert` adds an inverse commit; it undoes the change, it does not erase history
- **The dirty-tree trap** — starting with uncommitted edits mixes your work into the story of the AI's commits
- **The habit** — skim every auto-commit's diff; the commit is the undo button, the skim is the review

*Example (italic):* Edit 4's parser swap could have shipped quietly if it had passed the tests — the revert only helped because someone was actually looking at the result.

**Common mistake:** Treating revertibility as a substitute for review. Aider makes bad edits cheap to undo, but only a human reading the diff decides that an edit is bad in the first place.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: skipping the diff (silent bug ships despite being revertible) vs skimming the diff (bug caught and reverted).

- **Title (bold 15px, `#1a5276`, top center):** "Revertible Is Not the Same as Reviewed".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "no review"; blue `#2a78d6` rounded box at x=130 labeled "AI edit auto-committed" (12px), 3px arrow to a mute `#6b7280` box at x=340 labeled "diff never read", 3px arrow to a red `#e74c3c` box at x=530 labeled "subtle bug ships" with bold 12px red "✗ revert never triggered".
- **Row 2 (boxes centered on y=215), label:** "skim the diff"; blue box at x=130 "AI edit auto-committed", 3px arrow to a green `#008300` box at x=340 labeled "30-second diff skim", 3px arrow to a green box at x=530 labeled "git revert — undone" with bold 12px green "✓".
- **Box style:** 150–170px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(107,114,128,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "the commit is the undo button; the skim is the review".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); session minutes `[2, 7, 12, 18, 19, 23, 28]`, files-per-edit `[2, 1, 3, 4, 1, 2]`, and the delegation bar widths are invented and labeled illustrative; Aider facts (terminal pair programmer, auto-commit of every AI edit, repo map, multiple model providers, open source) are publicly documented behavior; `git revert` adding an inverse commit rather than erasing history is exact git semantics.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
