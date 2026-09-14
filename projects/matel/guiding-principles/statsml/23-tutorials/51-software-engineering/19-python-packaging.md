# Python Packaging

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Python Packaging

**Subtitle:** pip installs packages from PyPI into an environment that holds one version of everything — the famous mess, and the one habit (per-project environments) that tames it

## Two Projects, One Pandas Slot

**Tags:** `core idea` (blue), `pip & PyPI` (green), `isolation` (orange)

- **Two projects** — Maya's churn model was built on pandas 1.5.3; her new dashboard needs 2.2.0
- **The registry** — both versions sit on PyPI, the public index where anyone publishes Python packages
- **The installer** — `pip install pandas==2.2.0` downloads from PyPI into whatever environment is active
- **The constraint** — an environment holds exactly ONE version of each package, never two side by side
- **The fix** — one virtual environment per project: `venv-churn` keeps 1.5.3, `venv-dash` gets 2.2.0

*Example (italic):* Maya runs `pip install pandas==2.2.0` for the dashboard in her shared environment — it silently replaces 1.5.3, and the churn model breaks that same afternoon.

**Key point:** PyPI is the registry, pip is the installer, and a virtual environment is a private per-project set of packages — isolation exists because one environment can only hold one version of anything.

### Visualization (canvas `c1`, 720×300)

Conflict-vs-isolation diagram: two projects funneling into one shared environment (conflict) on top, the same two projects each with their own venv (resolved) below.

- **Title (bold 15px, `#1a5276`, top center):** "One Environment = One Pandas: Share It and Someone Breaks".
- **Top half, label 12px `#444` at x=20, y=75:** "shared env"; blue `#2a78d6` rounded boxes at (x=110, y=60) labeled "project A — pandas 1.5.3" and (x=110, y=112) labeled "project B — pandas 2.2.0" (each 190px wide, 36px tall); 3px arrows from both converging on a red `#e74c3c` box at (x=430, y=86) labeled "one pandas slot — conflict" with bold 12px red "✗ last install wins" beneath it.
- **Bottom half, label 12px `#444` at x=20, y=210:** "one venv each"; blue box (x=110, y=186) "project A" with 3px arrow to green `#008300` box (x=380, y=186) "venv-churn: pandas 1.5.3"; blue box (x=110, y=238) "project B" with arrow to green box (x=380, y=238) "venv-dash: pandas 2.2.0"; bold 12px green "✓ both run" at (x=630, y=218).
- **Divider:** dashed `#6b7280` (dash 4/3) horizontal line at y=165.
- **Box style:** 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, near x=560, y=150):** "isolation is the whole trick".

## Rebuilding the Exact Same Environment

**Tags:** `worked example` (blue), `pinning` (green), `reproducible` (orange)

- **The spec** — requirements file with exact pins: `pandas==1.5.3`, `numpy==1.24.4`, `scikit-learn==1.2.2`
- **Plus Python** — record the interpreter too: this project was built and tested on Python 3.10.13
- **The rebuild** — on a new machine: create a fresh venv, activate it, `pip install -r requirements.txt`
- **Unpinned drift** — a bare `pip install pandas` there grabs today's latest, 2.2.0, not 1.5.3
- **Hand-check** — unpinned, all 3 packages plus Python differ from the laptop; pinned, 0 of them differ

*Example (italic):* Maya's teammate rebuilds from the pinned spec on a brand-new server and gets pandas 1.5.3, numpy 1.24.4, scikit-learn 1.2.2 on Python 3.10.13 — byte-for-byte the versions the model was validated on.

**Key point:** Reproducibility is a written spec — exact package versions plus the Python version — and the discipline of rebuilding environments from that spec instead of copying or guessing.

### Visualization (canvas `c2`, 720×300)

Version-diff table drawn on canvas: three packages plus Python, compared across the old laptop, a new machine installed unpinned, and a new machine rebuilt from the pinned spec.

- **Title (bold 15px, `#1a5276`, top center):** "Same Spec In, Same Environment Out — Unpinned Drifts".
- **Column headers (bold 13px `#1a5276`, y=70):** "package" at x=40, "old laptop" at x=250, "new: unpinned" at x=420, "new: pinned" at x=590.
- **Rows (12px text, y = 108, 150, 192, 234; row separators 1px `#e5e9ef`):**
  - "pandas": `#444` "1.5.3" — red `#e74c3c` bold "2.2.0 ✗" — green `#008300` bold "1.5.3 ✓"
  - "numpy": "1.24.4" — red "1.26.4 ✗" — green "1.24.4 ✓"
  - "scikit-learn": "1.2.2" — red "1.4.0 ✗" — green "1.2.2 ✓"
  - "Python itself": "3.10.13" — red "3.12.4 ✗" — green "3.10.13 ✓"
- **Column tint:** unpinned column background `rgba(231,76,60,0.06)`, pinned column `rgba(0,131,0,0.06)`, full column height 85–250.
- **Annotation (bold 13px green `#008300`, centered near y=275):** "pin versions + record Python = 0 surprises on rebuild".
- **Caption (12px `#444`, bottom right):** "versions illustrative".

## Why the Mess Is Famous

**Tags:** `history` (blue), `native code` (orange), `pip vs conda` (green)

- **Layered tools** — setup.py, wheels, then pyproject.toml: each era added a layer, none was removed
- **Native code** — numpy and scipy wrap compiled C/Fortran; pip once built them from source locally
- **Wheels** — prebuilt binary packages (2012) made `pip install numpy` seconds instead of a compiler hunt
- **Why conda** — it shipped prebuilt scientific binaries (and non-Python libs) before wheels could
- **Spec sprawl** — requirements.txt, pins, lockfiles: three overlapping ways to say "these versions"
- **Consolidation** — pyproject.toml is the standard metadata home; modern resolvers add lockfiles

*Example (italic):* In 2010, `pip install scipy` could mean compiling Fortran for twenty minutes and failing; today a wheel installs the same package in seconds — conda was born in that gap.

**Key point:** The mess is not carelessness — it is decades of hard, real problems (compiled scientific code, no lockfile standard) solved by parallel tools that all still exist, now slowly consolidating around pyproject.toml.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline of Python packaging milestones, 2000 to 2026, showing tools accumulating rather than replacing each other.

- **Title (bold 15px, `#1a5276`, top center):** "26 Years of Layers: Each Tool Added, None Removed".
- **Timeline:** 3px `#1a5276` horizontal line from (70, 165) to (660, 165); small 12px `#444` year ticks "2000" at x=70, "2010" at x=297, "2020" at x=524, "2026" at x=660 (scale ≈ 22.7px/year).
- **Milestone dots (8px radius) with 12px labels, alternating above (label y=115, stem to the line) and below (label y=215):**
  - x=70, above, `#6b7280`: "2000 — setup.py / distutils"
  - x=138, below, `#2a78d6`: "2003 — PyPI, the public registry"
  - x=252, above, `#2a78d6`: "2008 — pip, the installer"
  - x=343, below, `#d95926`: "2012 — wheels + conda (compiled code)"
  - x=434, above, `#008300`: "2016 — pyproject.toml standard"
  - x=593, below, `#4a3aa7`: "2020s — fast resolvers, lockfiles"
- **Annotation (bold 13px magenta `#d55181`, centered near y=268):** "the famous mess = every layer still in use somewhere".
- **Caption (12px `#444`, bottom right):** "milestone years approximate".

## The Environment You Mutated for a Year

**Tags:** `common mistake` (red), `system Python` (orange)

- **The beginner trap** — `sudo pip install` into system Python mixes project packages into the OS's own
- **The drift** — one big environment mutated for months collects packages no spec file ever recorded
- **The dead end** — "what does this project need?" becomes unanswerable after enough ad-hoc installs
- **The habit** — always work inside a per-project environment, never bare system Python
- **The reflex** — treat environments as disposable: delete and rebuild from the spec, don't nurse them

*Example (italic):* After 12 months of ad-hoc installs, Maya's environment holds 212 packages while her requirements file lists 12 — the other 200 are unrecoverable guesses when the laptop is replaced.

**Common mistake:** Treating the environment as the source of truth. The environment is disposable; the spec file is the asset — if you cannot delete your environment and rebuild it from the spec today, the project is not reproducible.

### Visualization (canvas `c4`, 720×300)

Line chart over 12 months: packages actually installed in the mutated environment (climbing) vs packages recorded in the spec file (flat), with the unrecorded gap shaded.

- **Title (bold 15px, `#1a5276`, top center):** "12 Months of Drift: 212 Installed, 12 Written Down".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 12 with 12px `#444` tick labels every 3 months ("0m"–"12m"); y = packages 0 to 240, gridlines `#e5e9ef` at 60/120/180.
- **Installed line:** orange `#d95926` 3px line through months `[0, 2, 4, 6, 8, 10, 12]`, packages `[9, 38, 74, 121, 160, 189, 212]`.
- **Recorded line:** green `#008300` 3px line through the same months, packages `[9, 9, 12, 12, 12, 12, 12]` — flat.
- **Gap fill:** `rgba(217,89,38,0.15)` between the two lines.
- **Labels:** bold 12px orange "installed in env" near (x≈9m, y above the orange line); bold 12px green "in requirements.txt" near (x≈9m, y just above the green line).
- **Annotation (bold 13px red `#e74c3c`, near x=6m, y=80):** "200 packages no spec ever recorded".
- **Caption (12px `#444`, bottom right):** "package counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and version strings above (no randomness); package versions, package counts, and the drift curve are invented and labeled illustrative; timeline milestone years are approximate public history (PyPI 2003, pip 2008, wheels and conda 2012, pyproject.toml 2016).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
