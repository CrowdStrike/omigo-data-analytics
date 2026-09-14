# The Three Registries Compared

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Three Registries Compared

**Subtitle:** npm, PyPI, and Maven Central are institutions with different rulebooks — and each ecosystem's famous incidents trace straight back to its rules

## The Eleven Lines That Broke the Internet

**Tags:** `core idea` (blue), `left-pad 2016` (orange), `npm` (green)

- **The package** — left-pad was an 11-line npm package that padded a string with spaces on the left
- **The unpublish** — in March 2016 its author removed it from npm during a naming dispute
- **The blast radius** — Babel, React tooling, and thousands of other packages depended on it
- **The outage** — installs failed worldwide within minutes; the missing 11 lines halted builds everywhere
- **The rule change** — npm then restricted unpublishing: blocked after 72 hours or once anything depends on it
- **The lesson** — a registry that lets authors delete published code makes every build a hostage to one account

*Example (italic):* A team's CI pipeline that passed at 2pm failed at 2:30pm with "left-pad not found" — nothing in their own code had changed.

**Key point:** A package registry is not a folder of files; it is an institution, and its unpublish policy decides whether yesterday's working build still works today.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the left-pad incident: one tiny package fanning out to major dependents, converging on a global build failure, with the policy change as a footer.

- **Title (bold 15px, `#1a5276`, top center):** "left-pad, March 2016: One Unpublish, Thousands of Broken Builds".
- **Source box (x=30, y=125, 160×46, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border):** "left-pad — 11 lines" (bold 12px `#2c3e50`), small 11px `#6b7280` label "unpublished" beneath it with a red `#e74c3c` ✗ over the box corner.
- **Mid boxes (x=270, 140×38 each, at y=70 / 130 / 190, fill `rgba(42,120,214,0.10)`, 1.5px `#2a78d6` border, 12px text):** "Babel", "React tooling", "thousands more…"; 2.5px `#6b7280` arrows fan from the source box to each.
- **Sink box (x=500, y=118, 180×56, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border):** bold 12px red "installs fail worldwide"; 2.5px arrows converge from the three mid boxes.
- **Annotation (bold 13px orange `#d95926`, centered near y=40):** "11 lines were a load-bearing wall".
- **Footer strip (12px `#008300`, centered at y=272):** "npm's fix: unpublish blocked after 72h or once a package has dependents".
- **Caption (11px `#444`, bottom right):** "incident documented; box layout schematic".

## Claiming the Same Name on Three Registries

**Tags:** `worked example` (blue), `namespacing` (green), `typosquatting` (red)

- **The task** — a company, Acme, wants to publish its helper library on all three registries
- **npm** — flat namespace, first come first served; scopes (`@acme/utils`) were bolted on later, optional
- **PyPI** — flat and first-come too, no namespaces at all: whoever registers `acme-utils` first owns it
- **Maven Central** — requires a reverse-domain group ID (`com.acme:utils`) and proof you control acme.com
- **The squatter test** — on npm and PyPI a stranger can take `acme-utils` today; on Maven they cannot
- **The documented cost** — PyPI's flat namespace made it typosquatting's playground (`reqeusts` vs `requests`)

*Example (italic):* Acme publishes `@acme/utils` on npm, races to grab `acme-utils` on PyPI before anyone else, and proves domain ownership for `com.acme:utils` on Maven Central.

**Key point:** Namespacing is the registry deciding who may use a name — mandatory reverse-domain IDs make squatting structurally hard, while flat first-come namespaces make it a race.

### Visualization (canvas `c2`, 720×300)

Three side-by-side panels showing how Acme claims a name on each registry, each with a "squat difficulty" meter bar underneath.

- **Title (bold 15px, `#1a5276`, top center):** "Who Gets the Name 'acme-utils'? Three Answers".
- **Panels (three rounded boxes 210×150 at x = 30 / 255 / 480, y=55, 8px radius, fill `#f8f9fa`, 1.5px `#e5e9ef` border):** header bold 13px `#1a5276` "npm" / "PyPI" / "Maven Central" at each panel top.
- **Panel body (12px `#2c3e50`, two lines each):** npm: "flat + optional scopes" / "`@acme/utils`"; PyPI: "flat, first-come only" / "`acme-utils` — race to claim"; Maven: "reverse-domain required" / "`com.acme:utils` — prove domain".
- **Squat-difficulty meters (below each panel at y=230, track 180×14, fill `#e5e9ef`, 7px radius):** npm bar width 36 (20%) orange `#d95926`; PyPI bar width 27 (15%) red `#e74c3c`; Maven bar width 171 (95%) green `#008300`; 11px `#6b7280` label "squat difficulty" under each, bold 12px value labels "20%" / "15%" / "95%" at bar ends.
- **Annotation (bold 12px red `#e74c3c`, under the PyPI meter, y=280):** "flat + first-come = typosquatting's playground".
- **Caption (11px `#444`, bottom right):** "difficulty percentages illustrative; rules exact".

## Policy Choices Become Security Incidents

**Tags:** `where it's used` (blue), `supply chain` (orange), `immutability` (green)

- **The pattern** — each registry's famous incidents map onto its written policies, not bad luck
- **Immutability** — Maven Central artifacts are effectively never removed: a build from 2010 still resolves
- **Unpublish rules** — npm's left-pad outage was an unpublish-policy failure, fixed by changing the policy
- **Signing** — Maven Central requires GPG-signed artifacts; npm and PyPI treat signing as optional
- **Hardening** — after typosquatting waves, PyPI mandated 2FA for critical packages; npm did the same for top packages (both 2022)
- **The reading** — immutability, namespacing, unpublish rules, and signing are supply-chain security decisions

*Example (italic):* A JVM project untouched since 2010 builds today because Maven Central promised immutability; an npm project from 2015 needed the registry to change its rules to get the same guarantee.

**Key point:** When you pick an ecosystem you inherit its registry's constitution — reproducibility, name safety, and tamper resistance are set by registry policy long before your code runs.

### Visualization (canvas `c3`, 720×300)

Policy matrix: four policy rows by three registry columns, each cell a colored chip summarizing the rule (green = strict/protective, orange = partial, red = loose).

- **Title (bold 15px, `#1a5276`, top center):** "The Rulebooks Side by Side".
- **Column headers (bold 13px `#1a5276`, centered at x = 250 / 415 / 580, y=55):** "npm", "PyPI", "Maven Central".
- **Row labels (12px `#444`, right-aligned at x=170, rows at y = 90 / 140 / 190 / 240):** "Immutability", "Namespacing", "Signing", "2FA / accounts".
- **Cells (rounded chips 150×36, 6px radius, centered on the column x positions; fills `rgba(0,131,0,0.14)` green / `rgba(217,89,38,0.14)` orange / `rgba(231,76,60,0.12)` red; 11px `#2c3e50` text, colored 1.5px border matching the tone):**
  - Immutability: npm "unpublish restricted (post-2016)" orange; PyPI "maintainers can delete releases" orange; Maven "never removed" green.
  - Namespacing: npm "flat + optional scopes" orange; PyPI "flat, first-come" red; Maven "reverse-domain required" green.
  - Signing: npm "optional provenance" orange; PyPI "optional attestations" orange; Maven "GPG signature required" green.
  - 2FA / accounts: npm "required for top packages" green; PyPI "required for critical packages" green; Maven "domain-verified publishers" green.
- **Annotation (bold 12px violet `#4a3aa7`, bottom center y=278):** "every famous incident sits in an orange or red cell".
- **Caption (11px `#444`, bottom right):** "policies as publicly documented, simplified to one phrase each".

## Building Straight Against the Public Registry

**Tags:** `common mistake` (red), `internal mirror` (green)

- **The mistake** — pointing every build at the public registry and trusting it to be up and unchanged
- **What can move** — registry outages, unpublished packages, policy changes, and yanked releases all break you
- **The insulation** — an internal mirror/proxy caches every artifact your builds have ever fetched
- **The payoff** — when the registry is down or a package vanishes, your builds pull the cached copy
- **The bonus** — a proxy is also a checkpoint: one place to pin versions, scan artifacts, and audit intake
- **The scope** — this applies to all three ecosystems; even immutable Maven Central has outages

*Example (italic):* During a registry outage, the team with a proxy ships on schedule from cache; the team without one watches every CI job fail until the registry recovers.

**Common mistake:** Treating the public registry as infrastructure you control. It is someone else's institution — an internal mirror is how a company makes registry policy surprises somebody else's problem.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: builds hitting the public registry directly (outage breaks the build) vs builds behind an internal mirror (cached artifact, build succeeds).

- **Title (bold 15px, `#1a5276`, top center):** "Registry Outage: Direct Dependence vs an Internal Mirror".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "direct"; blue `rgba(42,120,214,0.15)` rounded box at x=110 "CI build" → 2.5px arrow → red `rgba(231,76,60,0.12)` box at x=330 "public registry — down" with red ✗ → arrow → red box at x=560 "build fails" with bold 12px red "✗ blocked".
- **Row 2 (boxes centered on y=215), label:** "mirrored"; blue box at x=110 "CI build" → arrow → green `rgba(0,131,0,0.12)` box at x=330 "internal mirror — cache hit" → arrow → green box at x=560 "build succeeds" with bold 12px green "✓"; dashed 1.5px `#6b7280` (dash 4/3) line from the mirror box up toward the registry box labeled 11px `#6b7280` "refills later".
- **Box style:** 150–180px wide, 42px tall, 8px radius, 12px `#2c3e50` text, borders matching the fill tone.
- **Annotation (bold 13px green `#008300`, centered near y=270):** "the mirror turns a registry incident into a non-event".
- **Caption (11px `#444`, bottom right):** "flow schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all geometry and values are the hardcoded literals above (no randomness). The left-pad incident (11 lines, March 2016, npm's post-incident 72-hour/dependents unpublish rule), Maven Central's reverse-domain group IDs, required signing and immutability, and the 2022 npm/PyPI 2FA mandates are documented history/policy; the squat-difficulty percentages (35/15/95) and all diagram layouts are illustrative and labeled as such in captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
