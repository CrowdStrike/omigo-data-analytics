# Lexing (lex)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Lexing (lex)

**Subtitle:** Before a computer can understand anything you type, it chops the stream of raw characters into labeled words called tokens — lexing is that chopping

## The Spreadsheet Reads Your Formula

**Tags:** `core idea` (blue), `tokens` (green), `character chopping` (orange)

- **The cell** — you type `total = 12 + 3.5 * 40` into a spreadsheet cell and press enter
- **What arrives** — the computer receives 21 raw characters, one keystroke at a time, no words yet
- **The chop** — lexing groups those characters into 7 meaningful chunks: a name, numbers, symbols
- **Tokens** — each chunk gets a type label: NAME `total`, EQUALS `=`, NUMBER `12`, PLUS `+`, and so on
- **Spaces vanish** — the 6 spaces only separate words; the lexer reads them and throws them away

*Example (italic):* You read "12+3.5" as three things — a twelve, a plus, a three-and-a-half — without thinking; lexing is a program doing exactly that grouping.

**Key point:** A lexer turns raw characters into labeled words called tokens: `total = 12 + 3.5 * 40` goes in as 21 characters and comes out as 7 tokens.

### Visualization (canvas `c1`, 720×300)

Two-row chopping diagram: the raw formula drawn as 21 individual character cells on top, connector lines falling to 7 colored token boxes below, with the 6 space characters visibly dropped.

- **Title (bold 15px, `#1a5276`, top center):** "One Keystroke Stream, Seven Tokens".
- **Character strip (top row, y=70, cell height 30):** 21 cells of width 26px starting at x=87 (strip spans 546px, ends x=633); each cell has a 1px `#e5e9ef` border and its character centered in 13px monospace `#2c3e50`; the characters in order are `t o t a l ␣ = ␣ 1 2 ␣ + ␣ 3 . 5 ␣ * ␣ 4 0` with the 6 spaces drawn as "␣" in 11px `#6b7280`; 11px `#6b7280` position labels "1", "5", "10", "15", "21" above cells 1, 5, 10, 15, 21.
- **Token row (y=185, box height 44):** 7 rounded boxes (4px radius), each aligned under the characters it covers — NAME `total` (chars 1–5), EQUALS `=` (7), NUMBER `12` (9–10), PLUS `+` (12), NUMBER `3.5` (14–16), STAR `*` (18), NUMBER `40` (20–21); fills by type: NAME `rgba(42,120,214,0.15)` border `#2a78d6`, operators `rgba(217,89,38,0.15)` border `#d95926`, NUMBERs `rgba(0,131,0,0.12)` border `#008300`; token type in 11px mute `#6b7280` above each box, lexeme centered inside in bold 13px matching the border color.
- **Connectors:** 1px `#6b7280` lines from the bottom of each character group to the top of its token box; no lines leave the 6 space cells.
- **Annotation (bold 12px orange `#d95926`, near x=470, y=262):** "6 spaces are thrown away — only the words survive".
- **Caption (12px `#444`, bottom left):** "the formula is the page's single running example — 21 characters in, 7 tokens out".

## Chopping 21 Characters into 7 Tokens

**Tags:** `worked example` (blue), `longest match` (green)

- **Start** — the cursor sits on char 1 `t`; letters keep coming, so it keeps reading until the space
- **First cut** — chars 1–5 spell `total`, then char 6 is a space: emit NAME `total`, skip the space
- **One-char tokens** — char 7 `=` is a symbol that stands alone: emit EQUALS and step past it
- **Longest match** — at char 14 it reads `3`, sees `.`, keeps going, sees `5`: one NUMBER `3.5`, not three pieces
- **Done** — one left-to-right walk over 21 chars: NAME, `=`, `12`, `+`, `3.5`, `*`, `40` — 7 tokens, 6 spaces dropped

*Example (italic):* Redo it by hand — put a finger on each character of `total = 12 + 3.5 * 40` and cut wherever the kind of character changes or a space appears; you get the same 7 pieces.

**Key point:** One pass, left to right, always grabbing the longest chunk that still looks like a single token — that greedy rule is essentially the whole algorithm.

### Visualization (canvas `c2`, 720×300)

Lane chart of the scan: the 21 character positions along the x axis with the characters printed beneath, colored span bars above marking where each of the 7 tokens starts and ends, and a callout on `3.5` showing the longest-match rule.

- **Title (bold 15px, `#1a5276`, top center):** "The Cursor Walks Left to Right, Cutting at Boundaries".
- **Axis:** horizontal 2px `#999` line at y=245 from x=70 to x=670 (width 600); character position p (1–21) maps to center x = 70 + (p − 0.5) × 600/21; each character printed at its position in 12px monospace `#2c3e50` just below the axis (spaces as "␣" in `#6b7280`); 11px `#6b7280` position numbers "1", "5", "10", "15", "21" under characters 1, 5, 10, 15, 21.
- **Token bars (22px tall, y=150, 3px radius):** blue `rgba(42,120,214,0.35)` bar over chars 1–5, orange `rgba(217,89,38,0.35)` bars over chars 7, 12, and 18, green `rgba(0,131,0,0.30)` bars over chars 9–10, 14–16, and 20–21; each bar has a 12px label above it in the matching solid color (`#2a78d6` / `#d95926` / `#008300`) reading "NAME total", "=", "NUMBER 12", "+", "NUMBER 3.5", "*", "NUMBER 40", staggered between y=132 and y=112 so neighbors never overlap.
- **Cut markers:** vertical dashed `#6b7280` (dash 4/3) lines from y=150 down to the axis at each token boundary (before chars 1, 7, 9, 12, 14, 18, 20 and after char 21).
- **Annotation (bold 12px green `#008300`, near x=430, y=58, two lines):** "sees '3', then '.', then '5' —" / "keeps going while it still looks like one number".
- **Caption (12px `#444`, bottom right):** "every cut lands on a space or a change of character kind".

## Every Language Tool Starts by Lexing

**Tags:** `where it's used` (blue), `pipeline` (green), `history` (orange)

- **Everywhere** — compilers, spreadsheets, SQL engines, JSON readers, and syntax highlighters all lex first
- **The name** — the 1975 Unix tool `lex` generated lexers from token patterns; the job inherited its name
- **Patterns** — each token type is one simple rule, e.g. NUMBER = digits with an optional dot
- **Error messages** — "unexpected token" in an error message is the lexer's vocabulary showing through
- **Cheap first pass** — lexing is fast and dumb on purpose, so later stages never touch raw characters again

*Example (italic):* When your editor colors `3.5` green and `total` blue before you even finish the line, a lexer just ran over your keystrokes.

**Key point:** Every tool that reads a language makes the same first move — chop characters into tokens — so everything downstream can think in words instead of letters.

### Visualization (canvas `c3`, 720×300)

Left-to-right pipeline diagram: five stage boxes from raw characters to the final answer 152, with the lexer stage highlighted and per-stage counts showing how 21 characters shrink to 7 tokens to one value.

- **Title (bold 15px, `#1a5276`, top center):** "Lexing Is Step One of Every Language Pipeline".
- **Stage boxes (five, each 116×74, y=105, 6px radius, arrows between):** at x = 20, 160, 300, 440, 580; connected by 2px `#6b7280` arrows with solid arrowheads. Box 1 "CHARACTERS" (white fill, 1px `#e5e9ef` border): `total = 12 + 3.5 * 40` wrapped in 11px monospace `#2c3e50`. Box 2 "LEXER" (fill `#1a5276`, bold 14px white label) — the highlighted stage. Box 3 "TOKENS" (white, `#e5e9ef` border): `total  =  12  +  3.5  *  40` in 11px monospace `#2c3e50`. Box 4 "PARSER" (white, `#e5e9ef` border, bold 13px `#1a5276` label): 11px `#6b7280` note "builds the grammar tree". Box 5 "ANSWER" (fill `rgba(0,131,0,0.12)`, border `#008300`): bold 20px `#008300` "152" centered (12 + 3.5 × 40 = 152).
- **Stage labels (bold 12px `#1a5276`, above each box):** "you type", "chop", "words", "grammar", "compute"; count labels (12px `#444`, below each box): "21 characters", "", "7 tokens", "1 tree", "1 value".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=232):** "after the lexer, no stage ever reads a raw character again".
- **Footnote (11px `#6b7280`, bottom left):** "for scale: a 1,000-line program yields on the order of 8,000 tokens".
- **Caption (12px `#444`, bottom right):** "token-count scale figure is illustrative; the formula's 21 → 7 → 152 numbers are exact".

## Lexing Is Not Understanding

**Tags:** `common mistake` (red), `lexer vs parser` (orange)

- **Two jobs** — the lexer checks that the words are legal; the parser checks that the sentence makes sense
- **Legal nonsense** — `total = 12 + + 40` lexes fine into 6 tokens; only the parser rejects it
- **Illegal word** — `total = 12 $ 40` dies inside the lexer: `$` matches no token rule at all
- **No memory** — the lexer never looks back and never counts brackets; it labels one chunk at a time
- **The tell** — "unexpected character" is a lexer complaint; "unexpected token" is a parser complaint

*Example (italic):* A spell-checker happily accepts "colorless green ideas sleep furiously" because every word is legal — just as a lexer accepts `12 + + 40`.

**Common mistake:** Expecting the lexer to catch grammar mistakes. It only knows spelling: any string of legal tokens sails through it, however meaningless their order.

### Visualization (canvas `c4`, 720×300)

Two-row gate chart: each row shows a broken input passing through a LEX gate and a PARSE gate, making visible that one input fails at the second gate and the other never gets past the first.

- **Title (bold 15px, `#1a5276`, top center):** "The Lexer Checks Spelling, the Parser Checks Grammar".
- **Column guides:** input strings at x=30, LEX gate boxes centered at x=390, PARSE gate boxes centered at x=580; gates are 130×56 rounded boxes (6px radius) with bold 13px stage labels "LEX" and "PARSE" inside at the top; 2px `#6b7280` arrows connect input → LEX → PARSE.
- **Row 1 (y=105):** input `total = 12 + + 40` in 13px monospace `#2c3e50`; LEX gate fill `rgba(0,131,0,0.12)` border `#008300` with bold 16px `#008300` check mark and 11px `#008300` note "6 legal tokens"; PARSE gate fill `rgba(231,76,60,0.12)` border `#e74c3c` with bold 16px `#e74c3c` cross and 11px `#e74c3c` note "two pluses in a row".
- **Row 2 (y=200):** input `total = 12 $ 40` in 13px monospace `#2c3e50`; LEX gate fill `rgba(231,76,60,0.12)` border `#e74c3c` with bold 16px `#e74c3c` cross and 11px `#e74c3c` note "'$' is not a token"; PARSE gate drawn dashed 1px `#6b7280` with 11px `#6b7280` note "never reached" and no arrow into it.
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "nonsense sentences made of legal words pass the lexer — grammar is the parser's job".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all strings, character positions, token spans, and counts are the hardcoded literals above (no randomness); the single running example `total = 12 + 3.5 * 40` (21 chars, 7 tokens, 6 spaces, value 152) must match between text and charts; the 8,000-tokens scale figure in c3 is invented and stays labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
