# Agent Plugins

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Agent Plugins

**Subtitle:** One installable bundle that ships skills, commands, and connectors into an agent — a browser extension, but for your AI assistant

## An App Store for Your Agent

**Tags:** `core idea` (blue), `install bundle` (green)

- **The problem** — Bob wants his agent to do code review: three skills, a shortcut command, a tracker connector
- **By hand** — he would copy each piece into place separately and repeat it on every machine
- **The plugin** — an author packages all the pieces into one named bundle
- **The install** — one command adds the whole bundle; uninstall removes it just as cleanly
- **The analogy** — browser extensions and phone apps did the same for browsers and phones

*Example (italic):* Installing a "code-review" plugin is like hiring a contractor who shows up with their own toolbox, checklists, and phone contacts — one handshake, full kit.

**Key point:** A plugin is the shipping container, not the cargo — its value is that many pieces arrive, update, and leave as one unit.

### Visualization (canvas `c1`, 720×300)

Bundle diagram: a plugin box containing four labeled pieces, with one arrow installing the whole box into an agent.

- **Title (bold 15px, `#1a5276`, top center):** "One Bundle, Many Pieces".
- **Plugin box:** rounded rect x=40, y=56, 330×200, fill `rgba(26,82,118,0.04)`, 2px `#1a5276` border; bold 13px `#1a5276` left-aligned label "code-review plugin" at (56, 80).
- **Piece rows (rounded rects 280×34 at x=64, tops y = `[92, 132, 172, 212]`):**
  - **Skills (green `#008300`, fill `rgba(0,131,0,0.07)`):** bold 12px left-aligned "skills — review checklist, style rules" at (78, y+22).
  - **Command (blue `#2a78d6`, fill `rgba(42,120,214,0.07)`):** "command — /review shortcut".
  - **Connector (yellow `#c98500`, fill `rgba(201,133,0,0.07)`):** "connector — issue-tracker access".
  - **Hooks (violet `#4a3aa7`, fill `rgba(74,58,167,0.07)`):** "hooks — auto-check before commits".
- **Install arrow:** thick 5px `#008300` arrow from (370, 156) to (470, 156) with a 9px arrowhead; bold 12px `#008300` "one install" centered above at (420, 142).
- **Agent box:** rounded rect x=480, y=104, 200×104, fill `rgba(0,131,0,0.05)`, 2px `#008300` border; bold 13px `#008300` centered "the agent" at (580, 130); 11px `#2c3e50` centered lines "all four pieces" / "now available" at (580, 154/172); bold 11px `#008300` "uninstall removes all four" at (580, 194).
- **Annotation (bold 12px orange `#d95926`, centered at y=278):** "no piece is new technology — the bundle and the one-step install are the invention".
- **Caption (11px `#444`, bottom right, y=294):** "bundle contents illustrative".

## Set Up a Five-Person Team, Count the Steps

**Tags:** `worked example` (blue), `distribution` (orange)

- **The kit** — the code-review setup has 4 pieces: skills, a command, a connector, hooks (illustrative)
- **By hand** — each person places 4 pieces: 4 × 5 = 20 manual steps, each a chance to differ
- **With a plugin** — each person runs one install: 1 × 5 = 5 steps, every copy identical
- **An update ships** — by hand it's another 20 touches; the plugin updates in 5, from one source
- **The claim to check** — steps grow as pieces × people by hand, but only as people with a plugin

*Example (italic):* The arithmetic is the whole pitch: 4 pieces × 5 people = 20 hand-copied setups, against 5 identical installs from one source of truth.

**Key point:** Plugins turn a multiplication into an addition — and every installed copy stays identical, which hand-copying never guarantees.

### Visualization (canvas `c2`, 720×300)

Bar chart: setup steps for a 5-person team, by hand vs via plugin, at rollout and at first update.

- **Title (bold 15px, `#1a5276`, top center):** "4 Pieces × 5 People: Setup Steps (illustrative)".
- **Axes:** baseline y=230, plot top y=70; y = steps 0–20 with `#e5e9ef` gridlines at 5/10/15/20 and 12px `#444` right-aligned tick labels at x=64; axis lines 1px `#999` from (70,70) to (70,230) to (660,230); scale 8px per step.
- **Grouped bars (two groups: "first rollout" centered ~x=240, "first update" centered ~x=500; in each group two bars 80px wide with 20px gap):**
  - Group 1: "by hand" 20 steps fill `#d55181` (center x=190), "plugin" 5 steps fill `#008300` (center x=290).
  - Group 2: "by hand" 20 steps fill `#d55181` (center x=450), "plugin" 5 steps fill `#008300` (center x=550).
  - Bold 13px value labels above each bar in the bar's color: "20", "5", "20", "5".
- **Group labels (12px `#444`, centered at y=250):** "first rollout" at x=240, "every update after" at x=500.
- **Bar legend (12px, y=282):** magenta swatch + "by hand, piece by piece" at x=170; green swatch + "one plugin install" at x=420 (left-aligned text next to 12×12 swatches).
- **Annotation (bold 12px orange `#d95926`, centered at (365, 90)):** "the gap repeats at every update — drift never gets a chance to start".
- **Caption (11px `#444`, bottom right, y=297):** "step counts illustrative".

## From One Author to a Whole Marketplace

**Tags:** `where it's used` (blue), `marketplaces` (green)

- **The author** — one person packages the team's working setup into a plugin once
- **The marketplace** — a shared catalog where plugins are published, found, and installed from
- **Inside a company** — a private catalog spreads one blessed setup across every team
- **In public** — coding agents like Claude Code install plugins from community marketplaces
- **The flywheel** — good setups stop being tribal knowledge and start being installable

*Example (italic):* The best prompt engineer at Vendor A used to share tips in chat; now their whole setup is a plugin with a version number and a changelog.

**Key point:** Marketplaces make agent setups a distributable artifact — discovered, installed, and updated like packages, not pasted like snippets.

### Visualization (canvas `c3`, 720×300)

Distribution flow: one author publishes to a marketplace, five team members install from it, an update propagates.

- **Title (bold 15px, `#1a5276`, top center):** "Publish Once, Install Everywhere".
- **Author box:** rounded rect x=36, y=110, 140×60, fill `rgba(42,120,214,0.07)`, 2px `#2a78d6` border; bold 12px `#2a78d6` centered lines "author" / "packages the kit" at (106, 134/152).
- **Marketplace box:** rounded rect x=280, y=98, 170×84, fill `rgba(201,133,0,0.08)`, 2px `#c98500` border; bold 13px `#c98500` centered "marketplace" at (365, 124); 11px `#2c3e50` centered "code-review plugin" at (365, 144); bold 11px `#c98500` "v1 → v2" at (365, 164).
- **Publish arrow:** 2.5px `#2a78d6` from (176, 140) to (274, 140) with arrowhead; bold 11px `#2a78d6` "publish" centered above at (225, 128).
- **Five installer boxes:** rounded rects 130×30 at x=540, tops y = `[54, 104, 154, 204, 254−10→see note]` — use tops `[54, 102, 150, 198, 246]` and height 28; fill `rgba(0,131,0,0.06)`, 1.5px `#008300` border; 11px `#008300` centered labels "teammate 1" … "teammate 5" at each center.
- **Fan arrows:** 1.5px `#008300` lines from (450, 140) to each installer box's left edge with 6px arrowheads.
- **Annotation (bold 12px orange `#d95926`, left-aligned at (36, 232)):** "v2 ships once —" and second line "all five update" at (36, 250) (kept clear of the boxes).
- **Caption (11px `#444`, bottom right, y=296):** "team size illustrative".

## Read the Label Before You Install

**Tags:** `common mistake` (red), `trust & permissions` (orange)

- **Real power** — an installed plugin acts with the agent's permissions: files, commands, connections
- **The mistake** — installing from an unknown author with the same care as opening a webpage
- **The right frame** — treat it like any software dependency: check the author, read what it bundles
- **Plugin vs skill** — a skill is one document; a plugin can carry many skills plus live connectors
- **House rules** — companies curate an approved catalog instead of allowing everything

*Example (italic):* Alice reviews a plugin's contents the way she reviews a new library dependency — who wrote it, what it can touch, and what changed since the last version.

**Common mistake:** Judging a plugin by its description instead of its contents — the description says what it's for; the bundle defines what it can do.

### Visualization (canvas `c4`, 720×300)

Two-panel contrast: what the install page says vs what the bundle can actually do, with a checklist verdict.

- **Title (bold 15px, `#1a5276`, top center):** "The Description vs the Bundle".
- **Left panel:** rounded rect x=30, y=52, 320×180, fill `rgba(42,120,214,0.05)`, 2px `#2a78d6` border; bold 13px `#2a78d6` centered header "what the listing says" at (190, 76).
  - 12px `#2c3e50` centered lines at y=104/124/144: "“makes code review easy!”", "5 stars, 2,000 installs", "one-line description".
  - Bold 11px `#6b7280` centered at (190, 176): "marketing, not a permission list".
- **Right panel:** rounded rect x=380, y=52, 310×180, fill `rgba(217,89,38,0.05)`, 2px `#d95926` border; bold 13px `#d95926` centered header "what the bundle can do" at (535, 76).
  - 12px `#2c3e50` centered lines at y=104/124/144: "read and edit your files", "run commands on your machine", "reach external services".
  - Bold 11px `#d95926` centered at (535, 176): "runs with the agent's permissions".
- **Verdict row (bold 12px `#008300`, centered at y=258):** "check the author, open the bundle, prefer a curated catalog — like any dependency".
- **Annotation:** the verdict row doubles as the annotation; no separate orange line (or place orange annotation at y=258 and verdict at y=276 — implement the single green verdict line at y=258 plus orange line "install is trust" pattern: use orange bold 12px "installing is trusting — same bar as adding a library" centered at y=276).
- **Caption (11px `#444`, bottom right, y=294):** "panels simplified".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** the c2 bars must read exactly 20 / 5 / 20 / 5 to match the text's 4 × 5 = 20 vs 1 × 5 = 5 arithmetic; all values hardcoded, no randomness.
- **Content discipline:** Alice/Bob for people, "Vendor A" for hypothetical companies; product mentions limited to documented public behavior (coding agents such as Claude Code install plugins from marketplaces); the security section states capability facts neutrally, no fear-mongering.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
