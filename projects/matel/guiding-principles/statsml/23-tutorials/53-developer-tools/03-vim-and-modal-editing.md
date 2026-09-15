# Vim & Modal Editing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Vim & Modal Editing

**Subtitle:** In vim the same keys mean different things in different modes — which turns editing into a small language of verbs and nouns you compose: `d3w` says "delete three words"

## One Keyboard, Two Meanings

**Tags:** `core idea` (blue), `modes` (green), `editor as language` (orange)

- **The file** — a config line reads `retries = "3"`; you want the number inside the quotes changed
- **The surprise** — in vim, pressing `d` types nothing; the keys are not inserting letters
- **Two modes** — insert mode types characters; normal mode treats every key as a command
- **The switch** — `i` drops you into insert mode to type; `Esc` returns you to command territory
- **The payoff** — with typing moved to its own mode, the whole keyboard is freed to be operations

*Example (italic):* In normal mode `x` deletes a character and `j` moves down a line; press `i` first and the very same keys just type the letters "x" and "j".

**Key point:** Modal editing gives one keyboard several meanings — normal mode for navigating and operating, insert mode for typing, visual mode for selecting — so commands need no Ctrl-chords at all.

### Visualization (canvas `c1`, 720×300)

Two-row key diagram: the same five keys shown once in insert mode (they type themselves) and once in normal mode (each is a command), with the mode-switch keys between the rows.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Five Keys in Two Modes".
- **Row 1 (key boxes centered on y=95), left label 12px `#444` at x=20:** "insert mode — keys type"; five rounded boxes 90×44, 8px radius, fill `rgba(42,120,214,0.15)`, at x = 150, 265, 380, 495, 610 (left edges); each shows the key bold 16px `#1a5276` (`d`, `w`, `x`, `i`, `j`) with 11px `#444` meaning under it: "types d", "types w", "types x", "types i", "types j".
- **Row 2 (boxes centered on y=205), left label:** "normal mode — keys act"; same five x-positions and box size, fill `rgba(0,131,0,0.12)`, same key letters, meanings 11px `#2c3e50`: "delete (verb)", "word (noun)", "delete char", "enter insert", "down a line".
- **Mode switch:** two small 2px `#6b7280` arrows between the rows near x=90 — downward arrow labeled 12px "`i` →" and upward arrow labeled "← `Esc`".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "typing gets its own mode — every key is freed to be a command".

## Speaking Sentences: d3w and ci"

**Tags:** `worked example` (blue), `grammar` (green)

- **Verbs** — `d` delete, `c` change, `y` copy: roughly 10 operators say WHAT to do
- **Nouns** — `w` word, `ap` a paragraph, `i"` inside quotes, `3j` down 3 lines: ~15 targets say WHERE
- **Sentences** — `d3w` deletes the next three words; `ci"` changes the text inside the quotes
- **The multiplication** — 10 verbs × 15 nouns ≈ 150 distinct edits from only 25 memorized primitives
- **The free bonus** — `.` replays the whole last edit, because a sentence is one thing to repeat

*Example (italic):* On `retries = "3"`, typing `ci"` wipes the 3 and lands you in insert mode between the quotes; type `9`, press `Esc` — the whole edit took five keystrokes.

**Key point:** This is the deep idea: you don't memorize hundreds of commands, you compose operator + motion/text-object like verb + noun — editing becomes a language with a grammar.

### Visualization (canvas `c2`, 720×300)

Grammar matrix: 4 verbs down the side, 5 nouns across the top, every cell showing the composed command — making the multiplication visible.

- **Title (bold 15px, `#1a5276`, top center):** "Verb × Noun: 25 Primitives, ~150 Sentences".
- **Grid geometry:** origin x=140, y=80; 5 columns 100px wide, 4 rows 40px tall (grid spans to x=640, y=240); 1px `#e5e9ef` cell borders.
- **Column headers (12px `#444`, centered above each column at y=70):** "`w` word", "`3w` 3 words", "`i\"` in quotes", "`ap` a paragraph", "`$` to line end".
- **Row labels (12px `#444`, left-aligned at x=25, vertically centered per row):** "`d` delete", "`c` change", "`y` copy", "`>` indent".
- **Cells:** monospace bold 13px `#1a5276` on fill `rgba(42,120,214,0.08)`, contents row by row: `dw, d3w, di", dap, d$` / `cw, c3w, ci", cap, c$` / `yw, y3w, yi", yap, y$` / `>w, >3w, >i", >ap, >$`.
- **Highlights:** cells `d3w` and `ci"` get fill `rgba(0,131,0,0.15)` and a 2px `#008300` border.
- **Annotation (bold 13px green `#008300`, centered near y=268):** "shown: 4 × 5 = 20 — full vim: 10 verbs × 15 nouns ≈ 150 operations".
- **Caption (12px `#444`, bottom right):** "operator and text-object counts approximate".

## A Week Crippled, Fifty Years Paid Back

**Tags:** `where it's used` (blue), `learning curve` (green), `history` (orange)

- **1976** — Bill Joy wrote vi for terminals so slow that keystrokes were all you could afford
- **Week one** — the honest cost: several days of feeling crippled while old reflexes misfire
- **The payback** — once the grammar sticks, text manipulation stays permanently faster
- **The language outlived the editor** — vim keybindings now live inside VS Code, JetBrains, browsers, shells
- **Always there** — vi ships on virtually every server; over SSH it is the editor you can count on

*Example (italic):* A developer starts near 55% of their old editing speed, breaks even around day 10, and settles roughly 35% faster on edit-heavy work (speeds illustrative).

**Key point:** The curve is front-loaded pain for a permanent skill — and because the grammar is embedded in nearly every modern editor, learning it pays back everywhere, not just in vim.

### Visualization (canvas `c3`, 720×300)

Line chart of editing speed versus days of vim use: a real dip below the old-editor baseline, a crossover near day 10, then a lasting plateau above it.

- **Title (bold 15px, `#1a5276`, top center):** "The Honest Learning Curve: One Bad Week, Then a Permanent Gain".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days 0 to 30 with 12px `#444` tick labels every 5 days; y = editing speed as % of old editor, 0 to 150, gridlines `#e5e9ef` at 50 and 150.
- **Baseline:** horizontal dashed `#6b7280` (dash 4/3) line at y for 100%, 12px `#6b7280` label "old editor = 100%" at its right end.
- **Vim line:** blue `#2a78d6` 3px line through days `[0, 2, 5, 7, 10, 14, 21, 30]`, speed `[55, 60, 70, 85, 100, 115, 128, 135]`.
- **Crossover marker:** 5px `#008300` dot where the line meets 100% at day 10, bold 12px green label "breaks even ~day 10" beside it.
- **Annotation (bold 13px red `#e74c3c`, near day 3, y=215):** "the crippled week".
- **Annotation (bold 13px green `#008300`, near day 23, y=90):** "settles ~35% faster on edits".
- **Caption (12px `#444`, bottom right):** "speeds illustrative; gain applies to edit-heavy work".

## Half-Learned Vim Is the Worst of Both

**Tags:** `common mistake` (red), `half-learning` (orange)

- **The trap** — staying in insert mode and steering with arrow keys reproduces a worse ordinary editor
- **No grammar, no gain** — the speedup lives in verb+noun sentences, not in the mode switch itself
- **Where it pays** — refactoring, log surgery, config edits: edit-heavy work on existing text
- **Where it doesn't** — writing fresh prose top-to-bottom is about the same speed in any editor
- **The test** — if you never use `.`, counts, or text objects, you carry the cost without the payoff

*Example (italic):* Repeating an edit on the next line is `j.` — two keystrokes; done with arrow keys and backspace it takes about 14 (counts illustrative).

**Common mistake:** Half-learning vim — insert mode plus arrow keys forever — captures none of the payoff; either learn the ~10 verbs and ~15 nouns or the mode switch is pure overhead.

### Visualization (canvas `c4`, 720×300)

Grouped horizontal bar chart: keystrokes for three real edits, grammar-driven vim (green) versus arrow-key editing (red) — the red bars are also what half-learned vim scores.

- **Title (bold 15px, `#1a5276`, top center):** "Keystrokes per Edit: Grammar vs Arrow Keys".
- **Layout:** three task groups with left-aligned 12px `#444` labels at x=20; bars start at x=250, scale 20px per keystroke, 14px tall; within a group the green bar sits 9px above the group center line and the red bar 9px below; group center lines at y = 75, 145, 215.
- **Group 1 — "change value in quotes (`ci\"9` Esc)":** green `#008300` bar width 100 (5 keys), red `#e74c3c` bar width 360 (18 keys).
- **Group 2 — "delete next 3 words (`d3w`)":** green bar width 60 (3 keys), red bar width 240 (12 keys).
- **Group 3 — "repeat edit on next line (`j.`)":** green bar width 40 (2 keys), red bar width 280 (14 keys).
- **Bar labels:** keystroke counts 12px bold at each bar's right end, matching bar color.
- **Annotation (bold 13px orange `#d95926`, centered near y=262):** "half-learned vim scores the red bars — plus an extra Esc".
- **Caption (12px `#444`, bottom right):** "keystroke counts illustrative; arrow-key counts assume no mouse".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; inline vim commands in monospace; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); learning-curve speeds and keystroke counts are invented and labeled illustrative; the verb/noun multiplication (10 × 15 ≈ 150 from 25 primitives) and the vi lineage (Bill Joy, 1976, slow terminals) are stated facts; text numbers match chart numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
