# npm & Package Ecosystems

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** npm & Package Ecosystems

**Subtitle:** Listing 5 libraries in package.json quietly installs their libraries, and theirs, and theirs — 5 lines pull in 500 packages

## Five Lines, Five Hundred Folders

**Tags:** `core idea` (blue), `dependencies` (green), `npm` (orange)

- **The site** — a coffee shop's online ordering page is built with 5 libraries listed in package.json
- **The five** — a web framework, a date formatter, an HTTP client, a form validator, a payment helper
- **The install** — `npm install` reads the 5 lines, then fetches what each of those 5 needs to run
- **The chain** — the date formatter needs a timezone table, which needs a locale list, and so on
- **The count** — when the download stops, node_modules holds 500 packages, not 5

*Example (italic):* The developer typed 5 names; the other 495 packages arrived because a dependency's dependency's dependency asked for them.

**Key point:** A package manager resolves dependencies recursively — your short list is only the root of a deep tree, and you install the whole tree.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart: number of packages installed at each depth of the dependency tree, from the 5 chosen directly (level 0) down to level 4.

- **Title (bold 15px, `#1a5276`, top center):** "5 Lines in package.json Become 500 Packages on Disk".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = tree depth with 12px `#444` labels "level 0 (you)", "level 1", "level 2", "level 3", "level 4"; y = packages 0 to 250, gridlines `#e5e9ef` at 50/100/150/200.
- **Bars:** 5 bars, 70px wide, evenly spaced; heights from counts `[5, 38, 161, 224, 72]`; level-0 bar solid green `#008300`, levels 1–4 fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border; 12px `#2c3e50` count label above each bar.
- **Annotation (bold 13px magenta `#d55181`, upper left near x=110, y=70):** "you picked 5 — the tree picked the other 495".
- **Caption (12px `#444`, bottom right):** "package counts illustrative".

## Counting the Tree Level by Level

**Tags:** `worked example` (blue), `semver` (green), `lockfile` (orange)

- **Level 0** — the 5 packages the developer named; each line is a name plus a version range
- **The caret** — `"date-fmt": "^1.4.2"` accepts any 1.x from 1.4.2 up, but never 2.0.0
- **The tilde** — `~1.4.2` is stricter: only patch updates, anything from 1.4.2 up to but below 1.5.0
- **Hand-count** — level 1 adds 38 packages (total 43), level 2 adds 161 (204), level 3 adds 224 (428), level 4 adds 72 (500)
- **The lockfile** — package-lock.json writes down the exact version chosen for all 500 slots

*Example (italic):* The range `^1.4.2` names a family of versions; the lockfile records the one family member — say 1.4.6 — that actually got installed.

**Key point:** Ranges (`^`, `~`) say what is acceptable; the lockfile says what was actually resolved — 500 exact versions, one per node of the tree.

### Visualization (canvas `c2`, 720×300)

Line chart of the cumulative package count as the resolver walks the tree one level deeper, from 5 to 500.

- **Title (bold 15px, `#1a5276`, top center):** "Cumulative Packages After Each Level: 5 → 43 → 204 → 428 → 500".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = tree depth 0 to 4 with 12px `#444` tick labels "level 0"–"level 4"; y = total packages 0 to 500, gridlines `#e5e9ef` at 125/250/375.
- **Line:** blue `#2a78d6` 3px line through levels `[0, 1, 2, 3, 4]`, cumulative totals `[5, 43, 204, 428, 500]`; 5px filled circle markers, 12px `#2c3e50` value label above each point.
- **First point:** the level-0 marker drawn green `#008300` with bold 12px green label "the 5 you chose" above it.
- **Annotation (bold 13px violet `#4a3aa7`, near x = level 2, y=70):** "every range (^ ~) picks a version for one of these 500 slots".
- **Caption (12px `#444`, bottom right):** "counts illustrative — real trees vary per install date".

## Every Ecosystem Rebuilt the Same Machine

**Tags:** `where it's used` (blue), `supply chain` (red), `reproducible builds` (green)

- **The pattern** — a manifest of ranges, a lockfile of exact versions, a public registry of packages
- **Python** — pip reads requirements.txt or pyproject.toml and downloads from PyPI
- **Rust** — cargo reads Cargo.toml, pins everything in Cargo.lock, fetches from crates.io
- **Java** — maven reads pom.xml and pulls jars from Maven Central
- **The risk** — you audited the 5 you chose; a bad update anywhere in the other 495 ships in your build
- **The defense** — committed lockfiles make every machine install the same 500 exact versions

*Example (italic):* A data scientist meets the same machine the day `pip install` succeeds on their laptop but fails on the server — the two resolved different trees.

**Key point:** npm, pip, cargo, and maven are one idea in four costumes — and in all four, trusting 5 packages means trusting the 500 they drag in.

### Visualization (canvas `c3`, 720×300)

Grid diagram: four ecosystems as rows, three shared concepts as columns (manifest, lockfile, registry), showing the same machine under different names.

- **Title (bold 15px, `#1a5276`, top center):** "Four Ecosystems, One Machine: Manifest → Lockfile → Registry".
- **Column headers (bold 13px `#1a5276`, y=60):** "manifest (ranges)" at x=210, "lockfile (exact)" at x=410, "registry" at x=600, centered.
- **Row labels (bold 12px `#2c3e50`, x=20, at y = 100, 150, 200, 250):** "npm / JS", "pip / Python", "cargo / Rust", "maven / Java".
- **Cells:** rounded boxes 150×32 (8px radius) centered under each header per row, 12px `#2c3e50` text; manifest column fill `rgba(42,120,214,0.15)`: "package.json", "pyproject.toml", "Cargo.toml", "pom.xml"; lockfile column fill `rgba(0,131,0,0.12)`: "package-lock.json", "pip freeze / lock", "Cargo.lock", "pinned in pom"; registry column fill `rgba(74,58,167,0.12)`: "npm registry", "PyPI", "crates.io", "Maven Central".
- **Arrows:** 2px `#6b7280` arrows between adjacent boxes in each row.
- **Annotation (bold 13px aqua `#199e70`, centered near y=285):** "learn the machine once — the column names transfer across languages".

## The Lockfile Nobody Committed

**Tags:** `common mistake` (red), `caret ranges` (orange)

- **The setup** — the coffee shop site works; package-lock.json is left out of version control
- **The drift** — three weeks later the date formatter releases 1.9.0, still inside the `^1.4.2` range
- **The surprise** — a teammate's fresh `npm install` resolves 1.9.0, which renames a function
- **The break** — the build fails only on the teammate's machine: "works on mine" is born
- **The fix** — commit the lockfile so all machines install the exact same 500 versions

*Example (italic):* Nothing in the project's own code changed, yet Friday's install differs from Monday's — the range was a moving target the whole time.

**Common mistake:** Treating the lockfile as clutter and ignoring it. Without it, `^` ranges silently re-resolve on every install, and each machine can end up with a different tree.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: an install without a committed lockfile (breaks) vs with one (identical everywhere), shown as boxes flowing left to right.

- **Title (bold 15px, `#1a5276`, top center):** "Same package.json, Different Installs: Why the Lockfile Matters".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no lockfile"; blue `#2a78d6` rounded box at x=160 labeled "range ^1.4.2" (12px), 3px arrow to a yellow `#c98500` box at x=350 labeled "registry serves newest: 1.9.0", 3px arrow to a red `#e74c3c` box at x=560 labeled "renamed function — build fails" with bold 12px red "✗ works on mine".
- **Row 2 (y=205), label:** "lockfile committed"; blue box at x=160 "range ^1.4.2", 3px arrow to a green `#008300` box at x=350 labeled "lock pins 1.4.6", 3px arrow to a green box at x=560 labeled "same 500 versions everywhere" with bold 12px green "✓".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(201,133,0,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "a range is a promise to drift; a lockfile is a photograph".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); per-level counts `[5, 38, 161, 224, 72]` and cumulative totals `[5, 43, 204, 428, 500]` are invented and labeled illustrative; semver range meanings (`^1.4.2` = ≥1.4.2 <2.0.0, `~1.4.2` = ≥1.4.2 <1.5.0) and the ecosystem file/registry names (package.json/PyPI/Cargo.lock/Maven Central etc.) are exact documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
