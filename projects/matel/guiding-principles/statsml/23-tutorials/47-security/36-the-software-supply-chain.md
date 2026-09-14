# The Software Supply Chain

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Software Supply Chain

**Subtitle:** Installing one package means trusting its author, their account, their dependencies, and the registry — typosquatting, hijacked maintainers, lockfiles, and SBOMs are all about managing that trust

## One Install, Eighty Strangers

**Tags:** `core idea` (blue), `trust chain` (green), `dependencies` (orange)

- **The install** — a developer adds one charting library, `chartkit`, to a dashboard project
- **The fan-out** — chartkit pulls 4 direct dependencies; those pull 19 more; those pull 56 more
- **The total** — one deliberate choice lands 80 packages by 62 different maintainers on the machine
- **The chain** — each package means trusting its author, that author's account, and their registry
- **The recursion** — you never picked 79 of the 80; chartkit's author picked some, strangers picked the rest

*Example (italic):* The developer reviewed chartkit's docs for an hour — and reviewed the other 79 packages, written by 62 people they will never meet, for zero minutes.

**Key point:** A dependency is a trust decision made on your behalf, recursively: one install extends trust to every author, account, and registry in the tree — that whole tree is the software supply chain.

### Visualization (canvas `c1`, 720×300)

Bar chart of packages added at each dependency depth, with a cumulative trust line climbing to 80.

- **Title (bold 15px, `#1a5276`, top center):** "Installing chartkit: 1 Choice, 80 Packages, 62 Maintainers".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = dependency depth with 12px `#444` labels "you install (depth 0)", "depth 1", "depth 2", "depth 3"; y = packages 0 to 80, gridlines `#e5e9ef` at 20/40/60.
- **Bars:** four bars centered at x = 150, 300, 450, 600, width 70, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, heights for values `[1, 4, 19, 56]`, 12px `#2a78d6` value labels above each bar.
- **Cumulative line:** green `#008300` 3px line through the bar centers at cumulative values `[1, 5, 24, 80]`, 4px green dots, 12px green labels "1", "5", "24", "80" beside the dots.
- **Annotation (bold 13px magenta `#d55181`, near x=380, y=75):** "you reviewed 1 — you're trusting 80".
- **Caption (12px `#444`, bottom right):** "package counts illustrative".

## Three Documented Ways the Chain Breaks

**Tags:** `worked example` (blue), `attack patterns` (red), `awareness` (orange)

- **Typosquatting** — a package named `chartkti` sits one keystroke from `chartkit`, catching typos
- **Account takeover** — a trusted package's stolen account ships v3.3 with malice to every auto-updater
- **Dependency confusion** — a public `acme-utils` v99 outranks a company's internal `acme-utils` in the resolver
- **The pattern** — none attack your code; all attack the trust you extended when you ran install
- **The near-miss** — the 2024 xz-utils backdoor (maintainer takeover) was caught by one curious engineer

*Example (italic):* Nothing on the developer's machine was hacked — in each pattern they ran a normal install command, and the trust chain delivered the attacker's code for them.

**Key point:** These are publicly documented patterns to recognize, not recipes: each one exploits a different trusted link — your typing, a maintainer's account, or the registry's name resolution.

### Visualization (canvas `c2`, 720×300)

Three-row flow diagram: each documented pattern as a two-box flow showing which trust link breaks.

- **Title (bold 15px, `#1a5276`, top center):** "Same Install Command, Three Broken Trust Links".
- **Row 1 (y=90), label 12px `#444` at x=20:** "typosquatting"; blue `#2a78d6` rounded box at x=170 labeled "you type: chartkti" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "near-twin package installs", bold 12px red "one letter off" beneath.
- **Row 2 (y=160), label:** "account takeover"; blue box at x=170 labeled "chartkit v3.2 — trusted for years", 3px arrow to a red box at x=430 labeled "v3.3 ships from stolen account", bold 12px red "auto-updaters get it first" beneath.
- **Row 3 (y=230), label:** "dependency confusion"; blue box at x=170 labeled "internal pkg: acme-utils v1.2", 3px arrow to a red box at x=430 labeled "public acme-utils v99 wins", bold 12px red "resolver prefers higher version" beneath.
- **Box style:** 190–210px wide, 38px tall, 8px radius, fills `rgba(42,120,214,0.15)` for trusted boxes and `rgba(231,76,60,0.12)` for broken-trust boxes, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=282):** "the break is upstream of your machine — before your code even runs".

## Layered Defenses, Because No Single One Covers It

**Tags:** `where it's used` (blue), `defenses` (green)

- **Lockfile + hash** — an integrity hash per package means you install exactly the bytes you reviewed
- **Version pinning** — upgrades become deliberate reviewed events, not silent overnight surprises
- **Minimal footprint** — every dependency you skip is a maintainer account you no longer depend on
- **Registry 2FA** — a stolen maintainer password alone can no longer publish a poisoned version
- **SBOM** — a bill of materials, so when the next disclosure lands you know in minutes if it's inside
- **Provenance** — signing efforts tie a published package back to the exact source that built it

*Example (italic):* When a disclosure names a library, the team with an SBOM answers "are we exposed?" in minutes; the team without one greps every repo for days.

**Key point:** Each defense covers a different link — lockfiles freeze bytes, 2FA guards accounts, SBOMs speed response — so real protection comes from layering several, not picking one.

### Visualization (canvas `c3`, 720×300)

Coverage matrix: five defenses (rows) against the three attack patterns (columns), marked full / partial / none.

- **Title (bold 15px, `#1a5276`, top center):** "Which Defense Covers Which Attack".
- **Column headers (bold 12px `#1a5276`) at y=62:** "typosquat" at x=330, "takeover" at x=470, "confusion" at x=610 (centered).
- **Rows at y = 95, 133, 171, 209, 247, each with a left-aligned 12px `#444` label at x=20:** "lockfile + integrity hash", "version pinning", "minimal dependencies", "maintainer 2FA", "SBOM (respond fast)".
- **Cell marks (centered on the column x positions):** bold 15px green `#008300` "✓" for full, bold 15px orange `#d95926` "△" for partial, 13px `#9aa3ad` "—" for none:
  - lockfile + hash: — / ✓ / ✓
  - version pinning: — / ✓ / △
  - minimal dependencies: △ / △ / △
  - maintainer 2FA: — / ✓ / —
  - SBOM: △ / △ / △
- **Row separators:** 1px `#e5e9ef` horizontal lines between rows across x=20–700.
- **Legend (12px, y=278):** green "✓ blocks it", orange "△ shrinks or speeds response", grey "— no help", spaced across the bottom.
- **Annotation (bold 13px magenta `#d55181`, right-aligned near x=700, y=40):** "no row covers all three — layer them".

## Auto-Update Is Not the Safe Setting

**Tags:** `common mistake` (red), `upgrades` (orange)

- **The instinct** — "always run the latest version" sounds like the security-conscious choice
- **The flip side** — an account-takeover release reaches auto-updaters within hours of publishing
- **The numbers** — a poisoned v3.3 reaches 60% of auto-updating installs in 48 hours, then gets yanked
- **The soak** — teams pinning with a 7-day upgrade delay never installed it at all
- **The balance** — never updating is also wrong: known CVEs stay open; the fix is deliberate, prompt upgrades

*Example (italic):* The poisoned v3.3 lives on the registry for 2 days; auto-updaters spend the weekend compromised, while the 7-day-soak team upgrades the following week to a clean v3.4.

**Common mistake:** Treating "latest, automatically" as the secure default. Auto-update turns a maintainer compromise into your compromise within hours — pin versions, let new releases soak briefly, then upgrade deliberately and promptly.

### Visualization (canvas `c4`, 720×300)

Timeline chart: share of installs running the poisoned v3.3 over 7 days — auto-updaters spike, soak-window pinners stay at zero.

- **Title (bold 15px, `#1a5276`, top center):** "48 Hours of Exposure: Auto-Update vs a 7-Day Soak Window".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days 0 to 7 with 12px `#444` tick labels "day 0" through "day 7"; y = % of installs on v3.3, 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Auto-update line:** red `#e74c3c` 3px line through days `[0, 0.5, 1, 2, 2.2, 3, 4, 7]`, percent `[0, 20, 45, 60, 60, 15, 4, 0]` — fast climb to 60% by day 2, cliff after the yank as fleets remediate.
- **Soak line:** green `#008300` 3px line through the same day grid, percent `[0, 0, 0, 0, 0, 0, 0, 0]` — flat on the baseline.
- **Markers:** vertical dashed `#6b7280` (dash 4/3) lines at day 0 and day 2, 12px `#6b7280` labels "malicious v3.3 published" and "yanked" at their tops.
- **Annotation (bold 13px green `#008300`, near day 4.5, y=120):** "the soak window never installed it".
- **Annotation (bold 13px red `#e74c3c`, near day 1.2, y=70):** "60% of auto-updaters in 48h".
- **Caption (12px `#444`, bottom right):** "adoption percentages illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); dependency counts, maintainer counts, and adoption percentages are invented and labeled illustrative; `chartkit`, `chartkti`, and `acme-utils` are fictional names; the xz-utils 2024 incident is referenced at headline level only. Defensive framing throughout — the page describes publicly documented patterns for awareness and defense, never attack instructions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
