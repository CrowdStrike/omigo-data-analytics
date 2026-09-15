# Eclipse

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Eclipse

**Subtitle:** The Java IDE that ruled the 2000s by making everything a plugin — and a case study in how an incumbent dev tool loses its throne without ever shipping a bad release

## The $40 Million Giveaway

**Tags:** `core idea` (blue), `platform story` (green), `2001` (orange)

- **The donation** — in 2001 IBM open-sourced Eclipse, a documented code contribution valued around $40M
- **The price** — Eclipse was free when the polished rivals (JBuilder, early IntelliJ) were paid products
- **The takeover** — through the 2000s it became THE Java IDE, the default answer to "what do I code in?"
- **The lever** — free plus radically extensible beat paid plus polished, and killed the old incumbent
- **The irony** — Eclipse itself dethroned an incumbent before later becoming the dethroned incumbent

*Example (italic):* A 2001 Java team paying per-seat for JBuilder switches to Eclipse in an afternoon — same job done, license cost zero, and a plugin for everything.

**Key point:** Eclipse's rise was a platform play: give away a free, extensible foundation and let the ecosystem — not one vendor's feature list — do the competing.

### Visualization (canvas `c1`, 720×300)

Two-line timeline of Java IDE mindshare through the 2000s: Eclipse rising as the paid incumbents fall, with a marker at the 2001 donation.

- **Title (bold 15px, `#1a5276`, top center):** "2001–2011: Free + Extensible Overtakes the Paid Incumbents".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = years 2001 to 2011 with 12px `#444` tick labels every 2 years; y = share of Java developers 0% to 80%, gridlines `#e5e9ef` at 20/40/60.
- **Eclipse line:** blue `#2a78d6` 3px line through years `[2001, 2003, 2005, 2007, 2009, 2011]`, share `[3, 30, 52, 64, 68, 66]` — steep rise, plateau near two-thirds.
- **Paid incumbents line:** magenta `#d55181` 3px line through the same years, share `[45, 30, 15, 8, 5, 3]` — steady collapse.
- **Donation marker:** vertical dashed `#6b7280` (dash 4/3) line at 2001, 12px `#6b7280` label "IBM open-sources ~$40M of code" at its top.
- **Annotation (bold 13px blue `#2a78d6`, near 2007, y=80):** "free platform becomes THE Java IDE".
- **Caption (12px `#444`, bottom right):** "share curves illustrative; donation value documented".

## Everything Is a Plugin

**Tags:** `worked example` (blue), `OSGi` (green), `architecture` (orange)

- **The kernel** — since 3.0 (2004) the core is a small OSGi runtime (Equinox); Java tooling is plugins on it
- **The stack** — workspace, editors, perspectives, and the incremental compiler are all plugin layers
- **The count** — a stock Java install already loads on the order of 400 plugins before you add any
- **Beyond Java** — the same platform hosts C++ (CDT), embedded toolchains, and whole vendor products
- **Ahead of its time** — errors flagged as you typed while javac-based rivals waited for a build

*Example (italic):* A chip vendor ships its "own" embedded IDE that is really Eclipse plus ~150 plugins — its debugger and flash tools sit on the same OSGi runtime as the Java tools.

**Key point:** Eclipse wasn't an IDE with plugins; it was a plugin platform that happened to ship an IDE — the idea VS Code later perfected with a curated extension marketplace.

### Visualization (canvas `c2`, 720×300)

Layered stack diagram: OSGi runtime at the bottom, platform plugins above, language tooling next, vendor products on top — everything above the base drawn as plugin boxes.

- **Title (bold 15px, `#1a5276`, top center):** "One Runtime, Everything Else Is a Plugin".
- **Layout:** four horizontal layers, each a rounded box row 560px wide centered at x=360, 44px tall, 8px radius, stacked at y = 220, 168, 116, 64 (bottom to top), 12px `#2c3e50` labels centered.
- **Layer 1 (y=220):** solid ink `#1a5276` box, white 12px bold text "OSGi runtime (Equinox) — the only non-plugin part".
- **Layer 2 (y=168):** fill `rgba(42,120,214,0.15)`, border 2px `#2a78d6`, text "platform plugins: workspace · editors · perspectives · incremental builder".
- **Layer 3 (y=116):** three side-by-side boxes 180px wide at x=80/260/440, fill `rgba(0,131,0,0.12)`, border 2px `#008300`, texts "JDT (Java)", "CDT (C++)", "embedded toolchains".
- **Layer 4 (y=64):** fill `rgba(230,126,34,0.15)`, border 2px `#e67e22`, text "vendor products built on the platform (~150 extra plugins each)".
- **Side bracket:** right-side vertical bracket line `#6b7280` spanning layers 2–4 with rotated 12px `#6b7280` label "~400 plugins in a stock install".
- **Annotation (bold 13px violet `#4a3aa7`, bottom center y=278):** "the IDE is just one plugin set among many — VS Code perfected this idea later".
- **Caption (12px `#444`, bottom right):** "plugin counts order-of-magnitude, illustrative".

## How the Incumbent Slipped

**Tags:** `why it matters` (blue), `structural decline` (red), `survey history` (orange)

- **Sprawl** — the openness that made it powerful made it slow, inconsistent, and fragile at scale
- **Folklore** — competing plugins and corrupted-workspace stories became every Java team's shared scar
- **Committee** — release trains of dozens of loosely-coordinated foundation projects, no single owner
- **The rivals** — IntelliJ polished one product with taste; VS Code reset expectations for startup speed
- **The trajectory** — public developer surveys track a slide from dominant Java IDE to single-digit share

*Example (italic):* By the mid-2020s survey era, roughly 7 in 10 Java developers report IntelliJ and under 1 in 10 report Eclipse — a full inversion of the late-2000s picture.

**Key point:** Eclipse's decline was structural, not one bad decision — platform sprawl plus governance-by-committee accumulated UX debt while single-owner rivals compounded polish.

### Visualization (canvas `c3`, 720×300)

Three-line survey-trajectory chart, 2014–2024: Eclipse falling, IntelliJ rising among Java developers, VS Code rising among Java developers too.

- **Title (bold 15px, `#1a5276`, top center):** "The Survey Slide: Dominant to Single-Digit in a Decade".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = years 2014 to 2024 with 12px `#444` tick labels every 2 years; y = survey share 0% to 80%, gridlines `#e5e9ef` at 20/40/60.
- **Eclipse line:** red `#e74c3c` 3px line through years `[2014, 2016, 2018, 2020, 2022, 2024]`, share `[48, 40, 28, 20, 13, 9]`.
- **IntelliJ line:** green `#008300` 3px line, same years, share `[30, 38, 50, 60, 68, 72]`.
- **VS Code line:** aqua `#199e70` 2px dashed (dash 6/4) line, same years, share `[0, 7, 15, 25, 35, 40]`, 11px `#199e70` label "VS Code (Java devs)" at its right end.
- **Line labels:** bold 12px red "Eclipse" near (2015, 52); bold 12px green "IntelliJ (Java devs)" near (2019, 56).
- **Annotation (bold 13px red `#e74c3c`, near 2022, y=190):** "9% by 2024 — single digits".
- **Caption (12px `#444`, bottom right):** "survey trajectory approximate; smoothed, illustrative".

## The Wrong Lesson — and What Survives

**Tags:** `common mistake` (red), `what survives` (green)

- **The mistake** — reading the decline as "plugins were the flaw"; VS Code won with the same plugin idea
- **The real flaw** — extensibility without curation: thousands of plugins, no editor-in-chief, entropy
- **Still real** — enterprise Java shops and vendor embedded tools still run on the platform today
- **The institution** — the Eclipse Foundation endures, now stewarding Jakarta EE and other projects
- **Rented, not owned** — incumbency in dev tools lasts only until someone resets expectations

*Example (italic):* A 2024 embedded engineer flashing firmware from a vendor IDE is running Eclipse without thinking about it — the platform outlived the product's popularity.

**Common mistake:** Blaming the architecture. The plugin platform was the right idea executed without curation — the durable lessons are that extensibility without a curator becomes entropy, and a platform can outlive its product's mindshare.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram contrasting the same plugin idea run without and with curation, ending in where each path landed.

- **Title (bold 15px, `#1a5276`, top center):** "Same Idea, Two Endings: Extensibility With and Without a Curator".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no curator"; blue `#2a78d6` rounded box at x=150 labeled "open platform, thousands of plugins" (12px), 3px arrow to a red `#e74c3c` box at x=400 labeled "conflicts · slow startup · broken workspaces" with bold 12px red "✗ users drift to rivals".
- **Row 2 (y=205), label:** "curated (VS Code's version)"; blue box at x=150 "same plugin idea, one product owner", 3px arrow to a green `#008300` box at x=400 labeled "fast core · vetted marketplace" with bold 12px green "✓ coherence at scale".
- **Box style:** 170–200px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, borders 2px in the row color.
- **Survivor strip (y=262):** 12px `#6b7280` text centered: "what survives: enterprise Java shops · vendor embedded IDEs · the Eclipse Foundation (Jakarta EE)".
- **Annotation (bold 13px orange `#d95926`, centered near y=282):** "the platform outlived the product's popularity".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the ~$40M IBM contribution, the OSGi/Equinox architecture, the survey slide to single digits, and the Foundation's Jakarta EE stewardship are documented history; exact share curves and plugin counts are invented and labeled illustrative or approximate.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
