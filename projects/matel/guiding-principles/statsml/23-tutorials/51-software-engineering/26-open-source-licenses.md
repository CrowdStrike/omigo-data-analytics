# Open Source Licenses

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Open Source Licenses

**Subtitle:** The license is the part of a project that tells you what you owe back when you use it

## What Do You Owe Back?

**Tags:** `core idea` (blue), `permissive vs copyleft` (green), `one axis` (orange)

- **The situation** — a developer copies a small open source library straight into a closed-source product
- **Permissive answer** — keep the copyright notice and license text, then ship the product closed
- **Copyleft answer** — distribute a modified version and the recipient may demand that source, same terms
- **The single axis** — every popular license is a point between "just the notice" and "hand over source"
- **The rest is paperwork** — patent grants, trademark limits, change notes, NOTICE files decorate that axis
- **No license at all** — the strictest case: default copyright applies, so nobody has permission to copy it at all
- **Public repository ≠ permission** — code visible on a hosting site with no license file is all-rights-reserved

*Example (italic):* The developer keeps a permissive library's notice in an "Open Source Licenses" screen and ships; the same product with a GPL library would have to hand out the whole application's source.

**Key point:** A license answers exactly one question — what you must give back — and every other clause exists to make that answer enforceable.

### Visualization (canvas `c1`, 720×300)

A horizontal "what you must give back" axis with six license markers, a green "closed product OK" band above it and a violet "share-alike" band below it, deliberately overlapping on the weak-copyleft licenses.

- **Title (bold 15px, `#1a5276`, top center):** "One Axis: What You Must Give Back".
- **Annotation (bold 13px orange `#d95926`, centered y=46):** "the same freedom to use — different price on redistribution".
- **Green band (rect x=72 → x=566, y=88, height 26, fill `rgba(0,131,0,0.14)`, 1.5px `#008300` border):** bold 12px `#008300` label "closed product OK" centered at x=319, y=105.
- **Violet band (rect x=406 → x=684, y=196, height 26, fill `rgba(74,58,167,0.14)`, 1.5px `#4a3aa7` border):** bold 12px `#4a3aa7` label "share-alike applies" centered at x=545, y=213.
- **Axis (2px `#1a5276` line at y=152, from x=60 to x=690)** with a 6px tick up/down at each marker x.
- **Markers (x = 100 / 216 / 332 / 448 / 564 / 660), each a bold 12.5px `#1a5276` name at y=140 and a two-line 11px `#6b7280` obligation under the axis at y=170 / 184:**
  - x=100 "MIT / BSD" — "nothing but", "the notice"
  - x=216 "Apache 2.0" — "notice + patent", "grant + changes"
  - x=332 "MPL 2.0" — "only the files", "you changed"
  - x=448 "LGPL" — "only the library", "itself"
  - x=564 "GPL" — "the whole work", "you distribute"
  - x=660 "AGPL" — "even if you", "only host it"
- **Overlap note (11px `#4a3aa7`, centered at x=490, y=246):** "MPL and LGPL sit in both bands — file-level or library-level share-alike".
- **Left tail box (rounded 150×34 at x=30, y=258, 6px radius, fill `rgba(107,114,128,0.14)`, 1.5px `#6b7280` border, 11px `#2c3e50` centered):** "no license = nobody may copy".
- **Caption (11px `#444`, bottom right):** "marker spacing schematic; obligations as written in each license".

## MIT, BSD, and Apache 2.0 Side by Side

**Tags:** `worked example` (blue), `permissive family` (green), `patent grant` (orange)

- **MIT** — about 170 words: keep the copyright notice and license text, accept no warranty, nothing else
- **BSD 2-Clause** — effectively MIT with the same two obligations, just organised as a numbered list
- **BSD 3-Clause** — adds clause 3: do not use the authors' names to endorse or promote your product
- **Apache 2.0** — dated January 2004 and roughly 1,400 words of terms; same freedom, far more machinery
- **Its patent grant** — every contributor grants you their patents in the work, stated explicitly in section 3
- **Its retaliation clause** — sue anyone over patents in the work and your own patent grant terminates
- **Its paperwork** — you must mark files you changed and pass along the project's NOTICE file if one exists
- **Why companies pick it** — the explicit patent grant is what a legal department wants before approving it

*Example (italic):* A team swaps an MIT dependency for an Apache-2.0 one and their obligations grow by two lines of housekeeping — state the changes, ship the NOTICE — while the freedom to ship closed is unchanged.

**Key point:** Inside the permissive family the difference is patents and paperwork, not freedom — all three let you ship a closed product.

### Visualization (canvas `c2`, 720×300)

Feature matrix: six requirement rows by three license columns, each cell a green "yes" chip or a grey "—" chip, with the word-count row carrying numbers instead.

- **Title (bold 15px, `#1a5276`, top center):** "The Difference Is Patents and Paperwork, Not Freedom".
- **Column headers (bold 13px `#1a5276`, centered at x = 330 / 470 / 610, y=70):** "MIT", "BSD-3", "Apache-2.0".
- **Row labels (12px `#444`, right-aligned at x=252, rows at y = 100 / 130 / 160 / 190 / 220 / 250):** "keep the notice", "no-endorsement clause", "explicit patent grant", "state your changes", "NOTICE file", "words of terms".
- **Chips (rounded 126×24, 6px radius, centered on the column x; green = `rgba(0,131,0,0.14)` fill with `#008300` 1.5px border and bold 11.5px `#008300` text; grey = `rgba(107,114,128,0.10)` fill with `#6b7280` border and 11.5px `#6b7280` text):**
  - keep the notice: MIT green "required", BSD-3 green "required", Apache green "required"
  - no-endorsement clause: MIT grey "—", BSD-3 green "clause 3", Apache green "§6: no trademarks"
  - explicit patent grant: MIT grey "—", BSD-3 grey "—", Apache green "§3, with retaliation"
  - state your changes: MIT grey "—", BSD-3 grey "—", Apache green "§4(b) required"
  - NOTICE file: MIT grey "—", BSD-3 grey "—", Apache green "§4(d) pass it along"
  - words of terms: MIT grey "~170", BSD-3 grey "~220", Apache grey "~1,400"
- **Annotation (bold 12px violet `#4a3aa7`, centered y=282):** "all three: ship a closed product, keep the notice, no warranty".
- **Caption (11px `#444`, bottom right):** "word counts approximate; clause numbers as in the license texts".

## Three Sizes of Share-Alike

**Tags:** `worked example` (blue), `copyleft` (orange), `hosting gap` (red)

- **GPL** — distribute a binary built from GPL code and the whole work's source must be offered under GPL
- **LGPL** — link to the library and your own code stays yours; change the library and that part is share-alike
- **The LGPL catch** — you still ship the library's source and let users relink it against their own build
- **AGPL** — closes the hosting gap: let people use it over a network and they can demand the source
- **Why hosting matters** — GPL is triggered by distribution, and a website distributes no binary at all
- **Who carries it** — the obligation lands on whoever ships or hosts, not on the original author
- **The blunt policy** — many companies ban AGPL dependencies outright rather than audit every service

*Example (italic):* The same library under LGPL lets a closed app link it and ship; under AGPL, putting that app behind a web endpoint gives every visitor a claim on the source.

**Key point:** Copyleft comes in three reaches — the whole work, the library only, or the whole work even when you never hand out a binary.

### Visualization (canvas `c3`, 720×300)

A 3×3 trigger grid: three ways of shipping (rows) against GPL / LGPL / AGPL (columns), each cell saying whether "must publish source" fires.

- **Title (bold 15px, `#1a5276`, top center):** "Which Situations Trigger 'Must Publish Source'?".
- **Column headers (bold 13px `#1a5276`, centered at x = 340 / 480 / 620, y=76):** "GPL", "LGPL", "AGPL".
- **Row labels (two lines, 12px `#444`, right-aligned at x=262, row centers y = 120 / 180 / 240):** "ship a binary" / "to customers"; "link the library," / "ship your app closed"; "run it as a website," / "ship nothing".
- **Cells (rounded 128×44, 6px radius, centered on the column x and row center; red tone = `rgba(231,76,60,0.12)` fill, `#e74c3c` border, bold 11.5px `#e74c3c` text; orange tone = `rgba(217,89,38,0.14)` fill, `#d95926` border, bold 11.5px `#d95926` text; green tone = `rgba(0,131,0,0.12)` fill, `#008300` border, bold 11.5px `#008300` text; each cell has a symbol line and a short second line at 10.5px):**
  - ship a binary: GPL red "✗ yes" / "whole work's source"; LGPL orange "~ partly" / "library source + relink"; AGPL red "✗ yes" / "whole work's source"
  - link the library: GPL red "✗ yes" / "your code too"; LGPL green "✓ no" / "your code stays yours"; AGPL red "✗ yes" / "your code too"
  - run it as a website: GPL green "✓ no" / "no distribution"; LGPL green "✓ no" / "no distribution"; AGPL red "✗ yes" / "network use counts"
- **Annotation (bold 12.5px red `#e74c3c`, centered y=284):** "the bottom-right cell is why AGPL gets banned by policy".
- **Caption (11px `#444`, bottom right):** "simplified to one phrase per cell; the license text governs".

## Where the Choice Bites

**Tags:** `where it's used` (blue), `dependency tree` (green), `compliance` (orange)

- **It propagates** — the most restrictive license anywhere in your dependency tree sets your obligations
- **One leaf is enough** — a single AGPL package four levels down colours the whole shipped artifact
- **A real clash** — Apache-2.0 code cannot be combined with GPLv2-only code; GPLv3 accepts Apache-2.0 terms
- **Picking yours** — MIT on your own library maximises adoption because nobody has to ask a lawyer first
- **The other lever** — AGPL on your own project maximises leverage over anyone who wants to host it as a service
- **Compliance is mechanical** — keep the notices, ship the license texts, and record an SPDX id per dependency
- **SPDX ids** — short strings like `MIT`, `Apache-2.0`, `AGPL-3.0-only` make the tree machine-checkable in CI

*Example (italic):* Swapping one AGPL leaf for an Apache-2.0 alternative changes nothing the product does and everything the product owes.

**Key point:** You do not choose one license — you inherit the union of every license in the tree, so the audit belongs in the build, not in a lawyer's inbox.

### Visualization (canvas `c4`, 720×300)

Two small dependency trees side by side: the left one has an AGPL leaf and a red obligation banner for the shipped artifact; the right one swaps that leaf for Apache-2.0 and turns green.

- **Title (bold 15px, `#1a5276`, top center):** "One Leaf Sets the Whole Artifact's Obligation".
- **Panel frames (two rounded boxes 320×214 at x = 25 / 375, y=48, 8px radius, fill `#f8f9fa`, 1.5px `#e5e9ef` border).**
- **Left panel — root node (rounded 150×32 centered at x=185, y=76, fill `rgba(231,76,60,0.12)`, 1.5px `#e74c3c` border, bold 12px `#e74c3c`):** "your app — AGPL".
- **Left panel — mid nodes (rounded 118×28 at y=136, centered x=105 and x=265, fill `rgba(42,120,214,0.10)`, 1.5px `#2a78d6`, 11.5px `#2c3e50`):** "web-fw · MIT", "orm · Apache-2.0"; 2px `#6b7280` arrows from the root down to each.
- **Left panel — leaf nodes (rounded 118×28 at y=192, centered x=105 and x=265):** left leaf grey/blue "utils · BSD-3"; right leaf red fill `rgba(231,76,60,0.12)` with `#e74c3c` border and bold 11.5px `#e74c3c` text "metrics · AGPL"; arrows from each mid node down to the leaf beneath it.
- **Left banner (rounded 300×26 at x=35, y=232, fill `rgba(231,76,60,0.12)`, `#e74c3c` border, bold 12px `#e74c3c` centered):** "hosting it exposes your source".
- **Right panel — same geometry shifted +350 in x, root (bold 12px `#008300`, green fill `rgba(0,131,0,0.12)`):** "your app — closed OK"; mids identical; the red leaf replaced by green "metrics · Apache-2.0".
- **Right banner (rounded 300×26 at x=385, y=232, green fill/border, bold 12px `#008300` centered):** "keep notices, ship closed".
- **Annotation (bold 12.5px orange `#d95926`, centered y=286):** "same features, one dependency swapped, opposite obligation".
- **Caption (11px `#444`, bottom right):** "illustrative tree; package names generic".

## Footnote

`<p class="footnote">` after the last section, 0.8rem `#6b7280`:

Many more licenses exist than the ones on this page — MPL 2.0, EPL 2.0, ISC, Zlib, the Unlicense and CC0, CDDL, and the Artistic License among them, plus the Creative Commons family that usually covers data and documentation rather than code. SPDX identifiers are the standard way to record which one applies to a given file or package. License terms also change between versions, so the authoritative text is always the one shipped with the version you actually use.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%), then the closing `.footnote` paragraph.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius; `.footnote` 0.8rem `#6b7280`. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers: `roundedBox`, `lineArrow`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all geometry and values are the hardcoded literals above (no randomness anywhere, no `Math.random()`). Factual items: MIT's text is about 170 words, BSD 3-Clause about 220, Apache 2.0's terms about 1,400 (Apache 2.0 is dated January 2004); Apache 2.0 §3 patent grant with retaliation, §4(b) change statements, §4(d) NOTICE handling, §6 trademark disclaimer; BSD 3-Clause clause 3 is the no-endorsement clause; GPL triggers on distribution, LGPL on modifying the library, AGPL additionally on network interaction; Apache-2.0 is incompatible with GPLv2-only and compatible with GPLv3. Package names in the dependency trees and the axis marker spacing are illustrative and labeled as such in captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
