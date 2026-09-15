# IDEs — The Landscape

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** IDEs — The Landscape

**Subtitle:** From text editor to code-aware workbench — a text editor manipulates characters, an IDE understands the code, and that one difference maps the whole landscape

## One Rename, Two Kinds of Editor

**Tags:** `core idea` (blue), `semantic model` (green), `rename test` (orange)

- **The task** — a project mentions `total_price` 41 times: 28 real call sites in 12 files, 9 times in comments, 4 inside strings
- **The text editor** — find-and-replace sees characters, so it changes all 41 mentions, including comments and strings
- **The damage** — 13 of those 41 edits are wrong: comments now describe a function that never existed, strings lie
- **The IDE** — it parses the project into a semantic model, so "rename this function" changes exactly the 28 call sites
- **The dividing line** — a text editor operates on characters; an IDE operates on the code's meaning

*Example (italic):* One rename of `total_price` to `order_total`: find-and-replace makes 41 edits (13 wrong), semantic rename makes 28 edits (0 wrong).

**Key point:** The rename test is the honest boundary of the landscape — the moment a tool edits by what the code MEANS rather than what the text SAYS, it has crossed from editor to IDE.

### Visualization (canvas `c1`, 720×300)

Two-row horizontal stacked bar comparison: find-and-replace edits (41, mixed right and wrong) vs semantic rename edits (28, all correct), same 10px-per-hit scale.

- **Title (bold 15px, `#1a5276`, top center):** "Rename total_price: 41 Character Matches, 28 Real Call Sites".
- **Layout:** row labels 12px `#444` left-aligned at x=20; bars start at x=200, scale 10px per hit; bar height 26px; rows at y=105 and y=200.
- **Row 1 label:** "find-and-replace (characters)"; stacked bar: blue `#2a78d6` segment width 280 ("28 call sites"), orange `#d95926` segment width 90 ("9 comments"), red `#e74c3c` segment width 40 ("4 strings"); 11px white segment labels; bold 12px red `#e74c3c` annotation above the orange+red segments at y=88: "13 wrong edits".
- **Row 2 label:** "semantic rename (code)"; green `#008300` bar width 280 ("28 call sites changed"), then grey `rgba(107,114,128,0.25)` bar width 130 with 11px `#6b7280` label "13 mentions untouched"; bold 12px green `#008300` annotation above at y=183: "0 wrong edits".
- **Shared scale marker:** thin `#e5e9ef` vertical gridlines at x=200+100/200/300/400 with 11px `#999` labels "10 / 20 / 30 / 40 hits" at y=260.
- **Caption (12px `#444`, bottom right):** "counts illustrative — one mid-size project".

## Five Tools, One Project Model

**Tags:** `worked example` (blue), `integration` (green)

- **The workbench** — an IDE bundles code intelligence, a debugger, a test runner, version control, and a build system
- **The trick** — the value is not five tools in one window; it is five tools reading ONE shared project model
- **One rename ripples** — the 28-site rename above marks 12 files changed; every integrated tool reacts to that fact
- **Hand-check** — test runner re-runs only the 6 tests touching the function; build recompiles only the 12 files; version control stages the same 12
- **Without the model** — five separate tools each re-discover the project, disagree, and you become the integration

*Example (italic):* After the rename, the test runner already knows which 6 tests to re-run and the build recompiles 12 files, not 400 — nobody typed a file list anywhere.

**Key point:** An IDE is an integration around a single semantic project model — each tool answers from the same picture of the code, which is why one action can ripple correctly through all of them.

### Visualization (canvas `c2`, 720×300)

Hub-and-spoke diagram: a central "one project model" box with five tool boxes around it, spokes labeled with what each tool learned from the 28-site rename.

- **Title (bold 15px, `#1a5276`, top center):** "One Rename, Five Tools React: the Shared Project Model".
- **Hub:** rounded box 180×46 centered at (360, 150), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border, bold 13px `#1a5276` text "one project model".
- **Spoke boxes (each 160×40, 8px radius, 12px `#2c3e50` text, 2px colored border, matching fill at 0.12 alpha), connected to the hub by 2px `#6b7280` lines:**
  - "code intelligence" blue `#2a78d6` centered at (140, 72), spoke label 11px `#444`: "28 call sites"
  - "debugger" violet `#4a3aa7` centered at (580, 72), spoke label: "breakpoints follow name"
  - "test runner" green `#008300` centered at (130, 238), spoke label: "6 affected tests"
  - "version control" orange `#d95926` centered at (590, 238), spoke label: "12 files staged"
  - "build system" aqua `#199e70` centered at (360, 258), spoke label: "12 files recompiled"
- **Annotation (bold 12px magenta `#d55181`, near x=360, y=105):** "every answer comes from the same model".
- **Caption (12px `#444`, bottom right):** "file and test counts illustrative".

## The Landscape in Five Families

**Tags:** `where it's used` (blue), `survey` (green), `eras` (orange)

- **The platform editor** — VS Code won breadth: lightweight core, everything else an extension, one editor for any stack
- **The deep-model family** — JetBrains/IntelliJ builds the richest semantic model per language; refactoring is its home turf
- **The vendor giants** — Visual Studio for .NET, Xcode for Apple, Android Studio for Android: each the official door to its platform
- **The veteran extensibles** — vim and emacs predate the term IDE and still absorb every new capability via scripting
- **The agentic wave** — AI-first editors put a model in the loop, the current disruption reshuffling the map
- **The recurring pattern** — each era's winner made a new capability FIRST-CLASS: Eclipse the plugin platform, VS Code lightweight + extensions + LSP, the AI editors the model in the loop

*Example (italic):* The same Python file opens in all five families; what differs is how much each one understands about the project around it.

**Key point:** The families differ on two honest axes — how deep the semantic model goes and how broad the ecosystem reaches — and every era's winner is the tool that made the era's new capability first-class instead of bolted-on.

### Visualization (canvas `c3`, 720×300)

Scatter quadrant map: x = semantic depth, y = ecosystem breadth, one labeled dot per family member, positions hardcoded.

- **Title (bold 15px, `#1a5276`, top center):** "The Map: Semantic Depth vs Ecosystem Breadth".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 175; x from 0 to 10 labeled "semantic depth →" (12px `#444`, centered below baseline); y from 0 to 10 labeled "ecosystem breadth →" (12px `#444`, rotated at left); light `#e5e9ef` gridlines at 2.5/5/7.5 on both axes.
- **Dots (radius 7, bold 12px labels beside each, position = (depth, breadth) mapped linearly onto the plot):**
  - "VS Code" blue `#2a78d6` at (6, 10)
  - "JetBrains" violet `#4a3aa7` at (10, 6)
  - "Visual Studio" orange `#d95926` at (9, 4)
  - "Xcode" magenta `#d55181` at (8, 2)
  - "Android Studio" aqua `#199e70` at (9, 3)
  - "vim / emacs" mute `#6b7280` at (3, 7)
  - "AI-first editors" green `#008300` at (6, 5), with a short dashed green arrow pointing up-right and 11px green label "moving fast"
- **Annotation (bold 12px `#1a5276`, top-left of plot near (1.5, 9.5)):** "no corner is 'best' — corners are trade-offs".
- **Caption (12px `#444`, bottom right):** "positions illustrative, not benchmarks".

## Choosing Without the Holy War

**Tags:** `common mistake` (red), `rule of thumb` (orange)

- **The mistake** — shopping for "the best IDE" in the abstract, as if one winner existed independent of your work
- **The platform decides** — shipping to iPhone or Android means Xcode or Android Studio; there is no vote
- **Depth vs breadth** — living all day in one language favors JetBrains; juggling many stacks favors VS Code
- **Muscle memory compounds** — vim/emacs keybindings ride into every other editor via keybinding modes, so that investment is never stranded
- **Betting on eras** — judge a new wave by what it makes first-class, not by its demo polish

*Example (italic):* A team argues editors for a week, then remembers the app ships to iPhone — Xcode was the answer on day one, and the vim users just turn on its vim mode.

**Common mistake:** Treating the IDE choice as a taste contest. Half the time the platform chooses for you; the rest is a depth-vs-breadth trade-off you can read straight off the map — and keybindings are portable, so no muscle memory is ever wasted.

### Visualization (canvas `c4`, 720×300)

Three-row decision flow: a question box on the left, an arrow, and the family it points to on the right, with a footer note about portable keybindings.

- **Title (bold 15px, `#1a5276`, top center):** "How to Choose: Three Questions, Then Stop Arguing".
- **Rows (question box 250×40 at x=40, 3px `#6b7280` arrow, answer box 300×40 at x=380; boxes 8px radius, 12px `#2c3e50` text, question fill `rgba(26,82,118,0.10)` with 2px `#1a5276` border):**
  - Row 1 (y=80): "shipping to iPhone / Android?" → answer "platform picks: Xcode / Android Studio", fill `rgba(217,89,38,0.12)`, 2px `#d95926` border
  - Row 2 (y=145): "all day in one language?" → answer "depth: JetBrains family", fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border
  - Row 3 (y=210): "many stacks, many tools?" → answer "breadth: VS Code + extensions", fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border
- **Footer note (bold 12px green `#008300`, centered at y=270):** "vim/emacs muscle memory rides along everywhere — keybinding modes".
- **Caption (12px `#444`, bottom right):** "decision order illustrative — top question wins".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); rename counts (41 = 28 call sites + 9 comments + 4 strings, 13 wrong edits, 12 files, 6 tests) and the quadrant coordinates are invented and labeled illustrative; the same counts appear in text and charts and must stay in sync.
- This is a hub/survey page but it stays self-contained: no cross-page links anywhere. In regenerated HTML, any card links would use `.html` extensions (this page has none).
