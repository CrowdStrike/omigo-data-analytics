# Semantic Versioning & Dependency Hell

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Semantic Versioning & Dependency Hell

**Subtitle:** MAJOR.MINOR.PATCH is a promise about what a release may break — and dependency hell is what happens when the promises collide

## Three Numbers, Three Promises

**Tags:** `core idea` (blue), `the promise` (green), `semver.org` (orange)

- **The library** — an orders dashboard depends on `datelib 1.4.2` to parse order timestamps
- **Patch (1.4.3)** — fixes a leap-year bug and nothing else; safe to take without reading anything
- **Minor (1.5.0)** — adds a new `parseISO()` function; every existing call keeps working
- **Major (2.0.0)** — renames `parse()` to `parseDate()`; upgrading means editing your code
- **The spec** — these rules are the published semver.org specification, not folklore

*Example (italic):* datelib ships 1.4.2 → 1.4.3 → 1.5.0 → 2.0.0 over a year; only the last release forces the dashboard team to change a line.

**Key point:** MAJOR.MINOR.PATCH is a machine-readable promise: patch = bug fixes only, minor = additive and backward compatible, major = breaking changes allowed.

### Visualization (canvas `c1`, 720×300)

Release ladder: four version boxes on a left-to-right timeline, the changed digit colored by bump type, with a safe/breaking badge over each step.

- **Title (bold 15px, `#1a5276`, top center):** "The Digit That Changes Is the Promise".
- **Boxes:** four rounded boxes 130×54 (8px radius, fill `rgba(42,120,214,0.12)`, 1px `#2a78d6` border) centered at x = `[120, 280, 440, 600]`, y=150; bold 16px version text inside: `["1.4.2", "1.4.3", "1.5.0", "2.0.0"]` — base version all ink `#1a5276`; in 1.4.3 the final "3" green `#008300`; in 1.5.0 the "5" blue `#2a78d6`; in 2.0.0 the "2" red `#e74c3c`.
- **Arrows:** 3px `#6b7280` horizontal arrows between consecutive boxes at y=150.
- **Badges (bold 12px, above each of the last three boxes at y=105):** green `#008300` "patch — bug fix only", blue `#2a78d6` "minor — additive", red `#e74c3c` "major — breaking".
- **Labels (12px `#444`, under boxes at y=195):** `["current", "take blindly", "take safely", "read changelog"]`.
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=245):** "one glance at which digit moved tells a machine what may break".
- **Caption (12px `#444`, bottom right):** "rules per the semver.org spec".

## What ^1.4.2 Actually Accepts

**Tags:** `worked example` (blue), `ranges & lockfiles` (green)

- **The range** — the app declares `datelib: ^1.4.2`, meaning any 1.x.y at or above 1.4.2
- **Trust encoded** — the caret accepts minors and patches because semver promises they don't break
- **Hand-check** — of releases 1.4.1, 1.4.3, 1.5.0, 1.9.9, 2.0.0, exactly three satisfy `^1.4.2`
- **Rejected** — 1.4.1 is below the floor; 2.0.0 crosses a major, which the caret never does
- **The lockfile** — the resolver picks the newest match (1.9.9) and the lockfile pins it exactly

*Example (italic):* Two teammates install a month apart; without a lockfile one resolves 1.5.0 and the other 1.9.9 — with one, both get exactly 1.9.9.

**Key point:** Ranges are how tools consume the promise (accept anything declared compatible); lockfiles record the exact resolved version so every install is reproducible.

### Visualization (canvas `c2`, 720×300)

Acceptance band: five candidate versions on a horizontal axis, check or cross above each, a green band spanning what `^1.4.2` accepts, and a lockfile pin marker.

- **Title (bold 15px, `#1a5276`, top center):** "^1.4.2 — Three of Five Releases Qualify".
- **Axis:** 2px `#999` baseline at y=210 from x=70 to x=650; five ticks centered at x = `[110, 230, 350, 470, 590]` with 13px `#444` labels `["1.4.1", "1.4.3", "1.5.0", "1.9.9", "2.0.0"]` at y=232.
- **Verdicts (bold 18px, at y=185 over each tick):** red `#e74c3c` "✗", green `#008300` "✓", green "✓", green "✓", red "✗".
- **Acceptance band:** fill `rgba(0,131,0,0.12)` rectangle x=190 to x=530, y=95 to y=210; bold 12px `#008300` label "^1.4.2 accepts: any 1.x.y ≥ 1.4.2" centered at y=110.
- **Lockfile pin:** 3px violet `#4a3aa7` vertical arrow pointing down at x=470 from y=130 to y=180, bold 12px violet label "lockfile pins 1.9.9" at y=125.
- **Rejection notes (11px `#e74c3c`):** "below floor" under 1.4.1 at y=250; "major bump" under 2.0.0 at y=250.
- **Annotation (bold 13px red `#e74c3c`, right side at y=80):** "the caret never crosses a major".
- **Caption (12px `#444`, bottom right):** "release list illustrative".

## When No Version Satisfies Everyone

**Tags:** `where it's used` (blue), `dependency hell` (red), `ecosystems` (orange)

- **The diamond** — the app uses libraries A and B; A requires `C >= 2.0` while B requires `C < 2.0`
- **No solution** — a single copy of C cannot satisfy both transitive constraints at once
- **npm's answer** — install C 2.1 for A and C 1.8 for B side by side, accepting duplication and bloat
- **Python's answer** — one version of C per environment, so the same conflict is fatal at install
- **Virtualenvs** — Python isolates each project in its own environment largely to dodge this

*Example (italic):* `npm install` quietly ships both C 1.8 and C 2.1 inside node_modules; `pip install` halts with "cannot resolve C" and installs nothing.

**Key point:** Dependency hell is a constraint-solving failure over transitive requirements; ecosystems differ only in whether they duplicate (npm) or refuse (Python).

### Visualization (canvas `c3`, 720×300)

Two-panel diamond-dependency diagram: npm resolving the conflict with two side-by-side copies, Python failing because one environment holds one version.

- **Title (bold 15px, `#1a5276`, top center):** "Same Diamond, Two Ecosystem Answers".
- **Panels:** left panel x=20–350, right panel x=370–700; 1px `#e5e9ef` divider line at x=360 from y=45 to y=280; panel titles bold 13px at y=58 — green `#008300` "npm: duplicate" centered at x=185, red `#e74c3c` "Python: one slot" centered at x=535.
- **Box style:** rounded boxes ~120×32, 8px radius, 12px `#2c3e50` text; App boxes fill `rgba(42,120,214,0.15)` with `#2a78d6` border; OK boxes fill `rgba(0,131,0,0.12)` with `#008300` border; fail box fill `rgba(231,76,60,0.12)` with `#e74c3c` border. Arrows 2px `#6b7280`.
- **Left panel:** "App" centered at (185, 85); arrows to "A needs C≥2.0" at (105, 150) and "B needs C<2.0" at (265, 150); arrow from A down to green box "C 2.1" at (105, 215); arrow from B down to green box "C 1.8" at (265, 215); bold 12px `#008300` label "two copies, both happy" centered at (185, 262).
- **Right panel:** "App" centered at (535, 85); arrows to "A needs C≥2.0" at (455, 150) and "B needs C<2.0" at (615, 150); both arrows converge on one red box "C = ???" at (535, 215); bold 12px `#e74c3c` label "no version fits — install fails" centered at (535, 262).
- **Annotation (bold 13px orange `#d95926`, centered at x=360, y=290):** "npm pays in disk and duplication; Python pays in hard failures".

## The Promise Is Social, Not Enforced

**Tags:** `common mistake` (red), `Hyrum's law` (orange), `0.x` (blue)

- **No referee** — nothing in the tooling checks that a release labeled "patch" is actually harmless
- **Hyrum's law** — with enough users every observable behavior is depended on, so even fixes break
- **0.x rule** — below 1.0.0 the spec says anything may change; many libraries camp on 0.x for years
- **Illustrative audit** — upgrade breakage rates: patch 8%, minor 14%, 0.x minor 21%, major 46%
- **The routine** — pin with lockfiles, upgrade deliberately and often, read the changelog for majors

*Example (italic):* A "patch" 1.4.6 tightens timestamp validation; the dashboard had been feeding it slightly malformed strings and dies on deploy — Hyrum's law wins.

**Common mistake:** Treating semver as a guarantee. It is a declared intent — trust it enough to use ranges, but verify with lockfiles, tests, and changelogs, because a patch can still break you.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: share of upgrades that broke the downstream build, by bump type, from an illustrative 200-upgrade audit.

- **Title (bold 15px, `#1a5276`, top center):** "Broke the Build: Breakage Rate by Bump Type (illustrative)".
- **Layout:** bars start at x=170, scale 9px per percentage point; 2px `#999` vertical baseline at x=170 from y=60 to y=250; row labels 12px `#444` right-aligned at x=160.
- **Gridlines:** vertical `#e5e9ef` lines at 10/20/30/40% (x = 260, 350, 440, 530) from y=60 to y=250, 11px `#6b7280` labels "10%"–"40%" at y=264.
- **Rows (18px-tall bars centered at y = 80, 130, 180, 230):**
  - "patch": green `#008300` bar width 72 (8%), 12px label "8%" at bar end
  - "minor": blue `#2a78d6` bar width 126 (14%), label "14%"
  - "0.x minor": orange `#d95926` bar width 189 (21%), label "21%"
  - "major": red `#e74c3c` bar width 414 (46%), label "46%"
- **Annotation (bold 13px magenta `#d55181`, near x=300, y=105):** "even patches break someone — Hyrum's law".
- **Caption (12px `#444`, bottom right):** "rates illustrative — audit is invented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the datelib release list, candidate versions, diamond constraints, and breakage rates (8 / 14 / 21 / 46 percent) are invented and labeled illustrative; the semver bump rules and caret-range semantics are the real semver.org specification.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
