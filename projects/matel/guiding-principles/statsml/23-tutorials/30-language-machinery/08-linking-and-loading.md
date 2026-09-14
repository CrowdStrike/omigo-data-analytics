# Linking & Loading

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Linking & Loading

**Subtitle:** A program is stitched together from separately compiled pieces — the linker turns every name into an address, the loader carries the result into memory, and one shared piece serving many programs is where DLL hell begins

## Three Cooks, One Cookbook

**Tags:** `core idea` (blue), `object files` (green), `name resolution` (orange)

- **The cookbook** — three cooks each write their own chapter of recipes, working alone, in any order
- **Cross-references** — a chapter says "now make the sauce (see Maria's recipe)" — a name, not a page
- **Object files** — compiled code works the same way: each `.o` file calls other functions by name only
- **The linker** — the editor who stacks the chapters, assigns page numbers, and fixes every reference
- **The loader** — the print shop: it copies the finished book into memory so the reader can start
- **One program** — after linking, no name is left dangling; every "see Maria" is now "see page 200"

*Example (italic):* Maria's chapter never knew what page it would land on — the editor decided that at binding time, then went back and rewrote every "see Maria's recipe" as a page number.

**Key point:** Compiling makes chapters that refer to each other by name; linking binds them into one book where every name has become an exact page number.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: three object-file boxes with dangling name references feed into a linker box, which emits one program box with all references resolved to addresses.

- **Title (bold 15px, `#1a5276`, top center):** "Three Separate Chapters → One Bound Book".
- **Object-file boxes (left column, x=30, width 170, height 48, at y=70, 140, 210):** 1px `#1a5276` border, 4px radius, fill `rgba(42,120,214,0.10)`; bold 12px `#1a5276` name on line one, 11px `#6b7280` note on line two — "main.o" / "calls greet(?), add(?)"; "ui.o" / "defines greet, calls add(?)"; "math.o" / "defines add".
- **Linker box (center, x=300, y=125, width 130, height 60):** fill `rgba(217,89,38,0.12)`, 2px `#d95926` border; bold 13px `#d95926` label "LINKER" with 11px `#6b7280` line "names → addresses" beneath.
- **Program box (right, x=520, y=70, width 170, height 190):** 2px `#008300` border, 4px radius; bold 12px `#008300` header "program (470 bytes)"; inside, three stacked 40px-tall segments with 11px labels: "main.o @ 0" (fill `rgba(42,120,214,0.25)`), "ui.o @ 200" (fill `rgba(0,131,0,0.20)`), "math.o @ 350" (fill `rgba(217,89,38,0.20)`).
- **Arrows:** 2px `#6b7280` arrows from each object box to the linker box, one 2px `#008300` arrow from linker to program box.
- **Annotation (bold 12px green `#008300`, below program box near x=520, y=280):** "no name left unresolved".
- **Caption (11px `#444`, bottom left):** "file names and sizes illustrative".

## Patching the Page Numbers by Hand

**Tags:** `worked example` (blue), `addresses` (green)

- **Three pieces** — main.o is 200 bytes, ui.o is 150 bytes, math.o is 120 bytes; none knows its address
- **Stacking** — the linker places main.o at 0, ui.o at 200, math.o at 350: one 470-byte program
- **The blanks** — main.o has "call greet" at byte 40 and "call add" at byte 90, both still just names
- **The patch** — greet lives at 200, add at 350, so the linker writes 200 into byte 40 and 350 into byte 90
- **Loading** — the loader copies the finished 470 bytes into memory and jumps to byte 0 to start
- **Redo it** — you can check every number: start addresses are just running totals of the sizes

*Example (italic):* 200 + 150 + 120 = 470 bytes total; ui.o starts at 200 because main.o's 200 bytes come first, and math.o starts at 200 + 150 = 350.

**Key point:** Linking is two mechanical steps — pick a start address for each piece by adding up sizes, then overwrite every named call with the address it resolves to.

### Visualization (canvas `c2`, 720×300)

Single horizontal memory bar for the linked 470-byte program, split into the three pieces, with two curved patch arrows from the call sites in main.o to the addresses they now jump to.

- **Title (bold 15px, `#1a5276`, top center):** "The Linked Program: 470 Bytes, Every Call Patched".
- **Memory bar:** rectangle from x=70 to x=650 (width 580) at y=150, height 44, representing addresses 0 to 470 (scale 580/470 px per byte); 1px `#1a5276` outline.
- **Segments:** main.o addresses 0–200 (x=70 to x=317) fill `rgba(42,120,214,0.25)`; ui.o 200–350 (x=317 to x=502) fill `rgba(0,131,0,0.20)`; math.o 350–470 (x=502 to x=650) fill `rgba(217,89,38,0.20)`; bold 12px centered segment labels in `#1a5276`: "main.o", "ui.o (greet)", "math.o (add)".
- **Address ticks (12px `#444`, below the bar):** "0" at x=70, "200" at x=317, "350" at x=502, "470" at x=650.
- **Call-site dots:** two 6px `#2a78d6` dots inside the main.o segment at byte 40 (x≈119) and byte 90 (x≈181) on the bar's midline, 11px `#2a78d6` labels below the dots' segment: "byte 40" and "byte 90".
- **Patch arrows:** two 2px curved arrows over the top of the bar — blue `#2a78d6` from byte 40 up to apex y=95 landing at x=317 with arrowhead, 12px blue label "call greet → 200" near its apex; orange `#d95926` from byte 90 up to apex y=70 landing at x=502 with arrowhead, 12px orange label "call add → 350" near its apex (staggered so labels don't collide).
- **Annotation (bold 13px green `#008300`, centered near y=250):** "the linker turned every name into an address".
- **Caption (11px `#444`, bottom right):** "sizes and offsets illustrative".

## One Shared Copy Instead of Five

**Tags:** `where it's used` (blue), `shared libraries` (green), `loading` (orange)

- **Common code** — five different apps all need the same 2 MB of text-drawing code, like a dictionary
- **Static linking** — copy the 2 MB into each program: five copies, 5 × 2 MB = 10 MB on disk
- **Dynamic linking** — each program instead carries a note: "borrow photo.dll when I start"
- **Load-time link** — the loader finds the one shared 2 MB copy and wires the notes to it: 10 MB → 2 MB
- **One fix, all apps** — patch a bug in the shared copy once and all five programs pick it up next launch
- **Names** — this shared piece is a DLL on Windows, a `.so` on Linux, a `.dylib` on a Mac

*Example (italic):* Instead of binding a full dictionary into every cookbook, the library keeps one dictionary on the shelf and each book just says "see the shared dictionary" — the loader is the librarian who fetches it.

**Key point:** Dynamic linking defers the last linking step to load time so many programs can share one copy — trading five private 2 MB copies for a single shared one.

### Visualization (canvas `c3`, 720×300)

Two-bar comparison of disk space for the same five apps: one stacked bar of five static 2 MB copies versus one short bar for a single shared copy.

- **Title (bold 15px, `#1a5276`, top center):** "Five Apps Needing the Same 2 MB Library".
- **Axes:** origin x=90, baseline y=250, plot height 190; y axis 0 to 10 MB, 12px `#444` tick labels "0", "2", "4", "6", "8", "10 MB" with light `#e5e9ef` gridlines every 2 MB (19px per MB).
- **Bar 1 (static, x=190, width 130):** stacked bar of five slices, each 2 MB tall (38px), fills alternating `rgba(42,120,214,0.35)` and `rgba(42,120,214,0.20)` with 1px white gaps, 11px `#1a5276` slice labels "app 1 copy" … "app 5 copy"; total height 190px reaching 10 MB; bold 13px `#1a5276` label below baseline: "static: a copy in every app".
- **Bar 2 (shared, x=440, width 130):** single 2 MB bar (height 38, top at y=212), fill `rgba(0,131,0,0.35)`, 2px `#008300` border, 11px `#008300` label inside "one shared photo.dll"; bold 13px `#008300` label below baseline: "dynamic: one shared copy".
- **Delta marker:** dashed `#6b7280` (dash 4/3) horizontal line at the 10 MB level from bar 1 across to above bar 2, with a 2px green arrow dropping to bar 2's top.
- **Annotation (bold 13px green `#008300`, near x=520, y=120):** two lines: "10 MB → 2 MB" / "and one bug fix updates all five".
- **Caption (11px `#444`, bottom right):** "sizes illustrative".

## When the Shared Copy Changes: DLL Hell

**Tags:** `common mistake` (red), `versioning` (orange)

- **The setup** — App A and App B both run against the single shared photo.dll v1.0; both work fine
- **The overwrite** — App B's installer ships photo.dll v2.0 and replaces the one shared copy in place
- **The break** — v2.0 renamed a function App A called; next launch, App A crashes though A never changed
- **Why "hell"** — one file, many masters: fixing A's version can break B, and reinstalls loop forever
- **The escape** — modern fixes are side-by-side versions or bundling private copies, paying disk back
- **The lesson** — sharing saved 8 MB in the last section; the price is that an update is never private

*Example (italic):* The library rebinds its one shared dictionary to a new edition that drops a word; every old cookbook that pointed at that word now opens to a page that isn't there.

**Common mistake:** Assuming a shared library update only affects the app that shipped it — every program pointing at that one copy re-links against the new version at its next load.

### Visualization (canvas `c4`, 720×300)

Before/after panel diagram: on the left both apps point to shared v1.0 and work; on the right the same apps point at the overwritten v2.0 and App A breaks.

- **Title (bold 15px, `#1a5276`, top center):** "One Shared File, Two Masters".
- **Divider:** vertical 1px `#e5e9ef` line at x=360 from y=50 to y=270; bold 12px `#444` panel headers at y=58 — "before: both on v1.0" (centered x=185) and "after: B's installer overwrote it" (centered x=540).
- **Left panel:** app boxes "App A" (x=50, y=85) and "App B" (x=50, y=185), each 90×40, 1px `#1a5276` border, fill `rgba(42,120,214,0.10)`, bold 12px `#1a5276` labels; shared box "photo.dll v1.0" at x=210, y=135, 120×44, fill `rgba(0,131,0,0.15)`, 2px `#008300` border, bold 12px `#008300` label; 2px `#008300` arrows from both apps to the shared box; bold 13px `#008300` check marks "OK" beside each app.
- **Right panel:** same layout shifted (apps at x=400, shared box "photo.dll v2.0" at x=560, y=135, fill `rgba(217,89,38,0.15)`, 2px `#d95926` border, bold 12px `#d95926` label); App B's arrow 2px `#008300` with "OK"; App A's arrow 2px `#e74c3c` dashed (dash 5/3) with a bold 14px `#e74c3c` "X" at its midpoint and an 11px `#e74c3c` note under App A: "function it called is gone".
- **Annotation (bold 13px red `#e74c3c`, centered near y=285):** "A never changed, yet A is the one that crashed — that's DLL hell".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all sizes, addresses, offsets, and megabyte figures are the hardcoded literals above (no randomness); segment x-positions on c2 follow the stated 580/470 px-per-byte scale; invented numbers carry the "illustrative" captions specified per chart.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
