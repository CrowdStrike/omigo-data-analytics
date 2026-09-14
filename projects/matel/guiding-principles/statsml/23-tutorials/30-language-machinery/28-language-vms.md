# Language VMs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Language VMs

**Subtitle:** Bytecode is a program written for a computer that was never built — and a small program called a virtual machine plays that computer on whatever real hardware you actually have

## One Recipe Card, Any Kitchen

**Tags:** `core idea` (blue), `bytecode` (green), `virtual machine` (orange)

- **The chain** — a coffee chain has one master recipe but hundreds of shops with different machines
- **The card** — head office writes each recipe as a card of tiny fixed symbols: GRIND, POUR, STEAM
- **The translation** — the chef's free-form notes are compiled once into that strict card format
- **The trained reader** — every shop trains one barista to read cards; that reader is the VM
- **The made-up machine** — the card's symbols are bytecode: orders for a machine nobody ever built
- **Write once** — head office writes the card one time; any shop with a trained reader can run it

*Example (italic):* A new shop opens in another country and rewrites nothing — it trains one card-reader, and every drink on the menu works from day one.

**Key point:** A language VM is a pretend computer: programs compile to its bytecode once, and each real machine only needs one program — the VM — to run them all.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: one source box compiles into one bytecode card, which fans out to three different machines that each run it through their own VM and get the same result.

- **Title (bold 15px, `#1a5276`, top center):** "One Card, Three Kitchens: the Same Recipe Runs Everywhere".
- **Source box:** rounded rect x=30–190, y=105–165, fill `rgba(42,120,214,0.12)`, 2px blue `#2a78d6` border; bold 12px blue text centered on two lines: "chef's notes" / "(source code)".
- **Compile arrow:** 3px `#6b7280` arrow with arrowhead from (190, 135) to (250, 135); 11px `#6b7280` label "compile once" above it.
- **Bytecode box:** rounded rect x=250–430, y=105–165, fill `rgba(217,89,38,0.12)`, 2px orange `#d95926` border; bold 12px orange text on two lines: "recipe card" / "(bytecode)".
- **Fan-out arrows:** three 2px `#6b7280` arrows from (430, 135) to (505, 75), (505, 135), (505, 195).
- **Machine boxes:** three rounded rects x=505–690, 44px tall, centered at y=75, 135, 195; fill `rgba(0,131,0,0.10)`, 2px green `#008300` border; each has bold 12px green line "laptop VM" / "phone VM" / "server VM" plus an 11px `#6b7280` line "same drink out" beneath it.
- **Annotation (bold 13px violet `#4a3aa7`, near x=200, y=245):** "write the card once — every kitchen with a reader can make the drink".
- **Caption (12px `#444`, bottom right):** "illustrative — kitchens stand in for chips and operating systems".

## Running the Coffee Bill by Hand

**Tags:** `worked example` (blue), `stack machine` (green)

- **The order** — 2 lattes at $4 each plus one $3 muffin; the till must compute 4 × 2 + 3
- **Five instructions** — the whole bytecode program is PUSH 4, PUSH 2, MUL, PUSH 3, ADD
- **The stack** — the VM keeps a stack of numbers: PUSH adds one, MUL and ADD eat two and leave one
- **Step by step** — the stack goes 4 → 4, 2 → 8 → 8, 3 → 11; after five steps, 11 sits on top
- **Same everywhere** — any VM on any chip walking these five steps must finish with 11 on the stack

*Example (italic):* Trace it on paper: after MUL the stack holds just 8, after ADD just 11 — you have executed bytecode by hand, no silicon required.

**Key point:** A VM is just a loop — read the next instruction, move numbers on a stack — and the $11 answer falls out after five steps.

### Visualization (canvas `c2`, 720×300)

Six-column stack-trace diagram: the stack drawn as boxes after each instruction, from empty to the final 11, with the instruction that caused each change labeled underneath.

- **Title (bold 15px, `#1a5276`, top center):** "Five Instructions, One Stack: 4 × 2 + 3 = 11".
- **Columns:** six columns centered at x = `[95, 205, 315, 425, 535, 645]`, each with a light `#e5e9ef` 2px shelf line 76px wide at baseline y=245.
- **Instruction labels:** 12px monospace `#444` centered below each shelf at y=272: "start", "PUSH 4", "PUSH 2", "MUL", "PUSH 3", "ADD".
- **Stack cells:** 76×32 rects stacked upward from the shelf; fill `rgba(42,120,214,0.15)`, 2px blue `#2a78d6` border, bold 13px blue centered value. Contents per column, bottom to top: `[]`, `[4]`, `[4, 2]`, `[8]`, `[8, 3]`, `[11]`.
- **Final cell:** the single `11` cell uses fill `rgba(0,131,0,0.15)` with green `#008300` border and bold green value.
- **Step arrows:** small 2px `#6b7280` arrows between neighboring columns at y=210.
- **Annotation (bold 12px green `#008300`, near x=590, y=140):** two lines: "top of stack: $11 —" / "the bill, on any machine".

## Why Invent a Machine That Doesn't Exist

**Tags:** `where it's used` (blue), `portability` (green), `one optimizer` (orange)

- **Python too** — every `.py` file compiles to bytecode first; the `.pyc` files are its recipe cards
- **The math** — 4 languages on 3 kinds of chips need 12 direct translators, but only 4 + 3 = 7 via bytecode
- **One optimizer** — a speed-up built into the VM helps every program ever written for it, for free
- **Sandbox** — the VM can refuse dangerous instructions, so bytecode from strangers runs safely
- **Growth** — a 5th language costs 3 more direct compilers, but only 1 new front-end with bytecode

*Example (italic):* A Spark job written on a Mac laptop runs unchanged on a Linux cluster, because both machines just run the same JVM bytecode.

**Key point:** The imaginary machine is a meeting point — each language compiles down to it once, each real chip implements it once: 7 tools instead of 12.

### Visualization (canvas `c3`, 720×300)

Two-bar comparison: the number of translator programs needed to connect 4 languages to 3 kinds of chips, with direct compilation versus going through one shared bytecode.

- **Title (bold 15px, `#1a5276`, top center):** "4 Languages, 3 Kinds of Chips: 12 Translators or 7".
- **Axes:** origin x=110, baseline y=245, plot width 520, plot height 190; y = number of tools 0 to 16 (≈11.9px per unit); light `#e5e9ef` gridlines at 4, 8, 12 (y≈198, 150, 103) with 12px `#444` tick labels at x=100, right-aligned.
- **Bar 1 (direct):** rect x=200, width 140, top y=103, down to baseline (value 12); fill `rgba(217,89,38,0.35)`, 2px orange `#d95926` border; bold 15px orange "12" centered above the bar; 12px `#444` two-line label below the axis: "direct compilers" / "4 × 3".
- **Bar 2 (bytecode):** rect x=440, width 140, top y=162, down to baseline (value 7); fill `rgba(0,131,0,0.30)`, 2px green `#008300` border; bold 15px green "7" above; label below: "with bytecode" / "4 + 3".
- **Annotation (bold 12px violet `#4a3aa7`, near x=380, y=70):** two lines: "add a 5th language: 15 vs 8 —" / "one new front-end, not three compilers".
- **Caption (12px `#444`, bottom right):** "counts from the 4-language, 3-chip example above".

## "Interpreted" vs "Compiled" Is Not a Fight

**Tags:** `common mistake` (red), `compiled vs interpreted` (orange)

- **The fight** — people ask "is Python compiled or interpreted?" as if it must be one or the other
- **Both** — Python compiles to bytecode, then a VM interprets that bytecode; the honest answer is both
- **JIT** — Java's VM goes further: it compiles hot bytecode into real machine code while running
- **The spectrum** — pure interpreters, bytecode VMs, JIT VMs, and native compilers sit on one line
- **What to ask** — not "compiled or interpreted?" but "what runs it, and when is it translated?"

*Example (italic):* The same Java program is bytecode at launch and native machine code minutes later — a single label cannot describe it.

**Common mistake:** Treating "compiled" and "interpreted" as properties of a language. They describe how a particular runner works — and a language VM happily does both at once.

### Visualization (canvas `c4`, 720×300)

Single horizontal spectrum: four ways of running a program placed on a "when is it translated" axis, with language VMs sitting in the middle and JITs sliding right at runtime.

- **Title (bold 15px, `#1a5276`, top center):** "One Line, Not Two Camps: When Is the Program Translated?".
- **Axis:** horizontal 2px `#999` line at y=180 from x=70 to x=670 with arrowheads at both ends; 12px `#444` end labels below: "translated while running" (left, x=70) and "translated before running" (right, x=670, right-aligned).
- **Dots (8px), labels 12px in the dot's color, staggered above/below to avoid overlap:**
  - magenta `#d55181` dot at x=120: label above (y=150) "pure interpreter (shell script)"
  - blue `#2a78d6` dot at x=290: label below (y=210) "bytecode VM (Python)"
  - violet `#4a3aa7` dot at x=460: label above (y=150) "JIT VM (Java)"
  - green `#008300` dot at x=630: label below (y=210) "native compiler (C)"
- **JIT slide:** short dashed (dash 4/3) 2px violet arrow from x=470 to x=560 at y=165; 11px violet label above it: "hot code slides right at runtime".
- **Annotation (bold 13px ink `#1a5276`, centered near x=360, y=260):** "language VMs live in the middle — compile first, interpret after".
- **Caption (12px `#444`, bottom right):** "positions illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the stack trace in `c2` must show exactly `[4]`, `[4, 2]`, `[8]`, `[8, 3]`, `[11]` to match the text's 4 × 2 + 3 = 11, and `c3` must show 12 vs 7 to match the 4 × 3 vs 4 + 3 bullets.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
