# Tags & Releases

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Tags & Releases

**Subtitle:** A tag is a name pinned to exactly one commit, forever — the immutable pointer that CI builds and ships, so v2.1.0 means the same bytes next year

## The Name That Never Moves

**Tags:** `core idea` (blue), `immutable pointer` (green), `git` (orange)

- **The library** — a three-person team maintains a JSON parser; commit `3f2a9c1` lands on Monday
- **The branch** — `main` points at `3f2a9c1` on Monday, but by Friday it has moved two commits ahead
- **The tag** — `v2.1.0` is also placed on `3f2a9c1`, and by convention it never moves again
- **The contrast** — a branch says "the latest of this line"; a tag says "exactly this, forever"
- **Two kinds** — lightweight tags are bare pointers; annotated tags carry tagger, date, message, signature
- **Release grade** — releases use annotated tags: the who/when/why travels with the pointer

*Example (italic):* On Friday `main` names commit `e04`, but `v2.1.0` still names `3f2a9c1` — asking git for the tag next year returns the identical snapshot.

**Key point:** A tag freezes one commit under a human-readable name; unlike a branch it does not follow new work, which is exactly what makes it safe to build and ship from.

### Visualization (canvas `c1`, 720×300)

Commit-line diagram: five commits on a horizontal lane; the `main` branch pointer shown moving from Monday to Friday while the `v2.1.0` tag stays pinned to the same commit.

- **Title (bold 15px, `#1a5276`, top center):** "The Branch Follows the Line; the Tag Pins One Commit".
- **Commit lane:** 2px `#999` horizontal line at y=165 from x=80 to x=650; five commit dots (radius 11, fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` stroke) at x = `[110, 230, 350, 470, 590]`; 12px `#444` short-sha labels below each at y=195: `a1f`, `8c2`, `3f2`, `b77`, `e04`.
- **Monday pointer:** dashed `#6b7280` (dash 4/3) arrow from (350, 245) up to the dot at x=350, 12px `#6b7280` label "main (Mon)" at y=262.
- **Friday pointer:** solid 3px `#2a78d6` arrow from (590, 245) up to the dot at x=590, bold 12px `#2a78d6` label "main (Fri)" at y=262.
- **Tag marker:** green `#008300` rounded box (110×30, 8px radius, fill `rgba(0,131,0,0.12)`) centered at (350, 90) labeled bold 12px "v2.1.0", with a 3px `#008300` arrow down to the dot at x=350.
- **Annotation (bold 13px green `#008300`, near x=160, y=60):** "the branch moved on; v2.1.0 still names 3f2".
- **Caption (12px `#444`, bottom right):** "commit shas illustrative".

## Pushing the Tag Fires the Release

**Tags:** `worked example` (blue), `CI pipeline` (green)

- **The trigger** — the maintainer runs `git tag -a v2.1.0` on `3f2a9c1` and pushes the tag to the server
- **The build** — CI wakes on the tag push, checks out exactly `3f2a9c1`, not whatever `main` is now
- **The gate** — the full suite runs: 412 tests must pass before anything is published
- **The fan-out** — one green build publishes three artifacts: package registry, container image, release page
- **One source of truth** — the tag connects source, build, and artifact: all three trace back to `3f2a9c1`

*Example (italic):* The push at 4:10pm triggers CI; 412 tests pass on `3f2a9c1`, and by 4:25pm the registry package, the container image, and the release-page binaries all carry the label v2.1.0.

**Key point:** The release pipeline pattern is tag-push → CI builds from exactly that commit → test → publish; the tag is the single identifier tying the shipped artifact back to its source.

### Visualization (canvas `c2`, 720×300)

Left-to-right flow diagram: tag push, CI checkout of the pinned commit, build+test gate, then a fan-out to three published artifacts.

- **Title (bold 15px, `#1a5276`, top center):** "Push v2.1.0 → CI Builds Exactly Commit 3f2a9c1 → Publish".
- **Box style:** rounded 8px radius, 40px tall, 12px `#2c3e50` text centered, 3px arrows between boxes.
- **Stage 1 (blue):** box 150px wide at x=25, y=140, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, label "git push tag v2.1.0".
- **Stage 2 (blue):** box 160px wide at x=210, y=140, same style, label "CI checkout 3f2a9c1".
- **Stage 3 (green):** box 140px wide at x=405, y=140, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, label "build + 412 tests ✓".
- **Fan-out (three green boxes 145px wide at x=555, y = 60 / 140 / 220):** "package registry", "container image", "release page + notes"; 3px `#008300` arrows from stage 3 to each.
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=360, y=270):** "one commit, one build, three artifacts — all named v2.1.0".
- **Caption (12px `#444`, bottom right):** "pipeline stages schematic; test count illustrative".

## Why Reproducibility Hangs on the Tag

**Tags:** `where it's used` (blue), `provenance` (green), `changelog` (orange)

- **Same bytes** — a bug report against v2.1.0 next year must check out the exact code that shipped
- **The changelog** — release notes are generated from the commits between tags: `v2.0.0..v2.1.0`
- **The count** — that range holds 19 commits: 9 features, 5 fixes, 3 docs, 2 perf — the notes write themselves
- **Signed tags** — an annotated tag signed with the maintainer's key proves who cut the release
- **Supply chain** — signed tag plus CI-built artifact is the provenance trail auditors ask for

*Example (italic):* A user on v2.1.0 hits a crash in March; the maintainer checks out the tag, reproduces on the exact shipped code, and confirms commit 14 of the 19 in the range introduced it.

**Key point:** Tags make releases reproducible and explainable — the diff between two tags is the changelog, and a signed tag is the proof the release came from the maintainers.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: the 19 commits between v2.0.0 and v2.1.0 grouped by type — the raw material of the generated release notes.

- **Title (bold 15px, `#1a5276`, top center):** "Release Notes = the 19 Commits Between v2.0.0 and v2.1.0".
- **Axis:** vertical 2px `#999` baseline at x=170, bars extend right, max width 420 (9 commits = 420px, linear scale); light gridlines `#e5e9ef` at 3 and 6 commits.
- **Rows (top to bottom at y = 75, 125, 175, 225), each with a right-aligned 12px `#444` label at x=160:**
  - "features": blue `#2a78d6` bar width 420, bold 12px count label "9" at bar end
  - "fixes": green `#008300` bar width 233, label "5"
  - "docs": aqua `#199e70` bar width 140, label "3"
  - "perf": orange `#d95926` bar width 93, label "2"
- **Bar style:** 22px tall, fills at 0.30 alpha with solid 2px same-hue border, count labels bold 12px in the bar's hue.
- **Annotation (bold 13px violet `#4a3aa7`, near x=380, y=250):** "generated from the tag range — no hand-written list to forget".
- **Caption (12px `#444`, bottom right):** "commit counts illustrative".

## Moving a Tag Breaks Everyone

**Tags:** `common mistake` (red), `trust` (orange)

- **The temptation** — a bug is found minutes after tagging; re-pointing v1.4.2 at the fix looks harmless
- **The break** — users who installed on Monday and Thursday now hold different bytes under one name
- **Caches lie** — mirrors and build caches keyed on "v1.4.2" silently serve the stale Monday build
- **Registry rule** — this is why package registries forbid republishing a version: cut v1.4.3 instead
- **Branch release** — releasing from a branch head that then moves leaves "which commit WAS v2.1.0?"
- **Local builds** — artifacts built on a laptop instead of from the tagged CI build have no provenance

*Example (italic):* v1.4.2 is re-pointed on Thursday; two users file contradictory bug reports for "the same version", and a cached mirror keeps shipping the Monday build for weeks.

**Common mistake:** Treating a tag as editable. Once published it is a promise — fixes get a new tag (v1.4.3), never a moved one, and the shipped artifact must come from the tagged CI build, not a local machine.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: re-pointing a published tag (two users end up with different builds under one name) vs cutting a new tag (everyone agrees).

- **Title (bold 15px, `#1a5276`, top center):** "Never Move a Published Tag — Cut a New One".
- **Row 1 (y=95), label 12px `#444` at x=20:** "tag moved"; blue `#2a78d6` rounded box at x=140 labeled "v1.4.2 → abc (Mon)" (12px), 3px arrow to a red `#e74c3c` box at x=350 labeled "v1.4.2 → def (Thu)", arrow to bold 12px red text at x=560 "✗ two builds, one name".
- **Row 2 (y=205), label:** "new tag"; blue box at x=140 "v1.4.2 → abc (Mon)", 3px arrow to a green `#008300` box at x=350 labeled "v1.4.3 → def (Thu)", arrow to bold 12px green text at x=560 "✓ both names stay true".
- **Box style:** 165px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "a published tag is a promise — registries refuse a republished version for this reason".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all coordinates and values are the hardcoded arrays above (no randomness); commit shas, the 412-test count, and the 19-commit changelog split (9 / 5 / 3 / 2) are invented and labeled illustrative; text numbers match chart numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
