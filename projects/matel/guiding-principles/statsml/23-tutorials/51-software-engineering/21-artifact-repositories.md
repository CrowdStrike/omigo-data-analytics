# Artifact Repositories

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Artifact Repositories

**Subtitle:** The company's internal shelf — a private place where everything the company builds is published like a real package, and a controlled gate for everything it downloads

## The Shelf for Things You Build

**Tags:** `core idea` (blue), `hosted repo` (green), `internal shelf` (orange)

- **The build output** — the payments team ships `pay-core 1.4.0`, a library three other teams import
- **Nowhere public** — internal code can't go on npm or PyPI, and email or shared drives lose versions
- **The shelf** — a hosted repo stores it like a real package: named, versioned, fetchable by any build
- **Not just libraries** — container images, ML models, and release binaries sit on the same shelf
- **Immutable** — once 1.4.0 is published, that exact byte-for-byte file is what every consumer gets

*Example (italic):* Three teams run `pip install pay-core==1.4.0` against the internal repo and all get the identical artifact built last Tuesday.

**Key point:** An artifact repository is the company's internal shelf — a private registry where build outputs are published, versioned, and consumed exactly like public packages.

### Visualization (canvas `c1`, 720×300)

Shelf diagram: CI publishes versioned artifacts into a central hosted-repo box; three consumer teams pull from it.

- **Title (bold 15px, `#1a5276`, top center):** "The Hosted Repo: One Shelf, Every Version, Every Kind of Artifact".
- **Publisher (left):** blue `#2a78d6` rounded box at x=30, y=130, 120×44, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` label "CI build"; 3px `#2a78d6` arrow to the shelf.
- **Shelf (center):** ink `#1a5276` 2px rounded rectangle at x=220, y=62, 280×190, bold 13px `#1a5276` header "hosted repo (internal)"; inside, five 12px `#2c3e50` artifact rows in `rgba(26,82,118,0.08)` pill boxes at y = 100, 130, 160, 190, 220: "pay-core 1.2.0", "pay-core 1.3.0", "pay-core 1.4.0", "fraud-model v7 (ML)", "api-image 2.1 (container)".
- **Consumers (right):** three green `#008300` rounded boxes at x=570, y = 80, 140, 200, each 120×40, fill `rgba(0,131,0,0.12)`, 12px labels "checkout team", "risk team", "CI pipeline"; 2px `#008300` arrows from the shelf's right edge to each.
- **Annotation (bold 13px green `#008300`, under the arrows near y=270):** "everyone pulls the same bytes for 1.4.0".
- **Caption (12px `#444`, bottom right):** "artifacts illustrative".

## One URL, Three Repos Behind It

**Tags:** `worked example` (blue), `proxy cache` (green), `virtual repo` (orange)

- **One URL** — the build tool points at a single virtual repo address and needs no other config
- **The split** — of 180 packages one build resolves, 3 are internal (hosted) and 177 are public
- **Cache hits** — 168 of the 177 public packages are served from the local proxy cache instantly
- **Cache misses** — the other 9 are fetched from PyPI once, stored, then served locally forever after
- **The aggregation** — virtual = hosted + proxy behind one address; tools can't tell the difference

*Example (italic):* The build asks one URL for 180 packages; the virtual repo answers 171 from inside the building and reaches the internet only 9 times.

**Key point:** One system plays three roles — hosted repos hold your artifacts, proxy repos cache public registries, and a virtual repo aggregates both behind a single URL.

### Visualization (canvas `c2`, 720×300)

Flow diagram of one build's 180-package resolution fanning out through the virtual repo into hosted, cached, and upstream sources.

- **Title (bold 15px, `#1a5276`, top center):** "Resolving 180 Packages: 171 Local, 9 From the Internet".
- **Build box (left):** blue `#2a78d6` rounded box at x=25, y=125, 130×50, fill `rgba(42,120,214,0.15)`, 12px labels "CI build" / "asks for 180"; 3px `#2a78d6` arrow to the virtual box.
- **Virtual box (center-left):** ink `#1a5276` 2px rounded box at x=230, y=115, 150×70, bold 12px `#1a5276` label "virtual repo" and 12px `#6b7280` "one URL".
- **Three branches (right), rounded boxes 190×46 at x=470:**
  - y=55: green `#008300` box, fill `rgba(0,131,0,0.12)`, "hosted (internal) — 3 pkgs"; 3px green arrow from virtual.
  - y=127: aqua `#199e70` box, fill `rgba(25,158,112,0.12)`, "proxy cache hit — 168 pkgs"; 5px aqua arrow (thickest — carries most traffic).
  - y=199: orange `#d95926` box, fill `rgba(217,89,38,0.12)`, "fetched from PyPI — 9 pkgs"; 2px orange dashed (dash 5/4) arrow continuing right to a 12px `#6b7280` cloud label "public registry" at x≈685.
- **Branch counts:** bold 12px labels in each branch's color ("3", "168", "9") on the arrows.
- **Annotation (bold 13px aqua `#199e70`, near x=300, y=265):** "168 + 3 = 171 requests never leave the building".
- **Caption (12px `#444`, bottom right):** "package counts illustrative".

## The Gate Where Supply-Chain Policy Lives

**Tags:** `where it's used` (blue), `supply chain` (green), `security` (red)

- **The choke point** — every package entering the company passes the proxy, so policy lives there
- **Allow/deny** — known-bad or unvetted packages are blocked at the gate before any build sees them
- **Scanning** — artifacts are vulnerability-scanned on entry; a flagged CVE quarantines that version
- **Dependency confusion** — internal names must never resolve to public lookalikes, a documented attack
- **Outage insurance** — when the public registry goes down, builds keep running from the cache

*Example (italic):* An attacker publishes a public `pay-core 99.0.0`; the rule "internal names resolve internal-only" makes the lookalike unreachable.

**Key point:** The proxy repo is supply-chain infrastructure — the single gate where allow/deny policy, vulnerability scanning, and dependency-confusion defense are enforced.

### Visualization (canvas `c3`, 720×300)

Timeline chart of successful builds per hour during a 60-minute public registry outage: direct-to-registry builds collapse to zero, proxy-cached builds stay flat.

- **Title (bold 15px, `#1a5276`, top center):** "Public Registry Down for an Hour: Cached Builds Never Notice".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 120 with 12px `#444` tick labels every 30 min ("0", "30", "60", "90", "120"); y = successful builds/hr 0 to 50, gridlines `#e5e9ef` at 10/20/30/40.
- **Outage band:** `rgba(231,76,60,0.08)` rectangle from minute 30 to minute 90, full plot height, 12px `#e74c3c` label "registry outage" at its top.
- **Direct line:** red `#e74c3c` 3px line through minutes `[0, 20, 30, 32, 50, 70, 90, 92, 110, 120]`, builds `[40, 41, 42, 0, 0, 0, 0, 38, 40, 41]` — vertical cliff to 0 at minute 30, recovery after minute 90.
- **Proxy line:** green `#008300` 3px line through minutes `[0, 20, 40, 60, 80, 100, 120]`, builds `[40, 41, 42, 40, 41, 40, 42]` — flat.
- **Legend (12px, top right inside plot):** red swatch "builds hitting registry directly", green swatch "builds via proxy cache".
- **Annotation (bold 13px green `#008300`, near minute 60, y=95):** "cache serves the same 168 packages it always did".
- **Caption (12px `#444`, bottom right):** "build counts illustrative".

## Releases Are Forever, Snapshots Are Not

**Tags:** `common mistake` (red), `lifecycle` (orange), `promotion` (green)

- **The mistake** — republishing changed bytes under the same version, so "1.4.0" means two things
- **The symptom** — a fixed bug reappears, and two machines "on 1.4.0" behave differently
- **Retention** — snapshot builds are pruned (e.g., after 30 days); release versions are kept forever
- **Promotion** — an artifact is copied dev → staging → release, passing a quality gate at each hop
- **Traceability** — every release carries the commit hash that built it, so bytes trace back to source

*Example (italic):* Build 1.4.0 moves from dev to staging when tests pass and to release after sign-off — the release copy is never overwritten, only succeeded by 1.4.1.

**Common mistake:** Treating the shelf like a scratch directory. A version number is a promise: fix bugs by publishing 1.4.1, never by quietly replacing 1.4.0 — build outputs deserve the same rigor as source: versioned, immutable, traceable.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: overwriting a release in place (broken) vs promoting through dev → staging → release repos (correct), with retention notes.

- **Title (bold 15px, `#1a5276`, top center):** "Overwrite vs Promote: How 1.4.0 Should Travel".
- **Row 1 (y=95), label 12px `#444` at x=20:** "overwrite"; blue `#2a78d6` rounded box at x=150 labeled "pay-core 1.4.0" (12px), 3px arrow to a red `#e74c3c` box at x=400 labeled "1.4.0 republished, new bytes" with bold 12px red "✗ same name, different artifact" beneath at y=140.
- **Row 2 (y=205), label:** "promote"; three rounded boxes left to right — blue `#2a78d6` at x=150 "dev-snapshots", green `#008300` at x=350 "staging", green `#008300` at x=545 "releases" — joined by 3px green arrows with 11px `#6b7280` gate labels above each arrow: "tests pass", "sign-off".
- **Retention notes (11px `#6b7280`, under row-2 boxes at y=250):** "pruned after 30 days" under dev-snapshots, "kept forever + commit hash" under releases.
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=285):** "promotion copies immutable artifacts through quality gates — nothing is edited in place".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); package counts (3 / 168 / 9 of 180), build rates (~40/hr), outage window (minutes 30–90), and the 30-day snapshot retention are invented and labeled illustrative; text numbers match chart numbers (171 = 3 + 168 local requests).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
