# The Rules of Three, Five, and Zero (C++)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Rules of Three, Five, and Zero (C++)

**Subtitle:** When a C++ class owns raw memory, four decisions come as a package — how it's freed, copied, moved, or not owned at all — C++ names the answers the rules of three, five, and zero

## One Copy Line, Two Owners of the Same Memory

**Tags:** `core idea` (blue), `resource ownership` (green), `C++` (orange)

- **The class** — a coffee shop's till software has a `DayLog` class holding a pointer to 500 order records on the heap
- **The copy** — `DayLog backup = live;` uses the compiler's default copy: it duplicates the 8-byte pointer, not the 500 records
- **Two owners** — `live` and `backup` now point at the same heap block; an edit through one shows up in the other
- **Closing time** — both destructors run, both call `delete` on the same block: a double free, and the till crashes
- **The rule** — if you had to write a destructor, the compiler's copy defaults are almost certainly wrong too

*Example (italic):* The backup taken at 3pm crashes the till at 9pm close — six hours after the harmless-looking copy line ran.

**Key point:** Rule of three — a class that manages a resource must define destructor, copy constructor, and copy assignment together, because the compiler's defaults copy pointers, not what they point to.

### Visualization (canvas `c1`, 720×300)

Ownership diagram: two `DayLog` boxes both pointing at one heap block, with the double-delete crash marked at closing time.

- **Title (bold 15px, `#1a5276`, top center):** "One Copy Line, Two Owners, One Crash at Close".
- **DayLog boxes:** two rounded boxes 170px wide, 40px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` text: "live — ptr →" at (x=80, y=70) and "backup — ptr →" at (x=80, y=180).
- **Copy label:** 12px `#6b7280` under the backup box at y=232: "default copy: pointer only (8 bytes)".
- **Heap block:** rounded box 180px wide, 56px tall at (x=380, y=118), fill `rgba(25,158,112,0.15)`, 2px `#199e70` border, 12px `#2c3e50` label "heap block: 500 order records".
- **Arrows:** 3px `#2c3e50` lines with small filled arrowheads from the right edge of each DayLog box to the left edge of the heap block.
- **Crash marker:** bold 13px red `#e74c3c` text right of the heap block at (x=590, y=140): "at close: delete ×2 on one block ✗".
- **Annotation (bold 13px green `#008300`, bottom center near y=278):** "a deep copy would give backup its own 500-record block".
- **Caption (12px `#444`, bottom right):** "record counts illustrative".

## Counting news Against deletes

**Tags:** `worked example` (blue), `hand check` (green)

- **The run** — the till opens one log in the morning (1 `new`) and takes two backups during the day
- **Default copies** — shallow: still just 1 `new`, but 3 destructors fire at close, so 3 `delete`s hit 1 block
- **The ledger** — deletes minus news = 3 − 1 = 2; every delete past the first frees memory that is already gone
- **Deep copies** — a rule-of-three copy constructor does its own `new`: 3 news, 3 deletes, ledger = 0
- **Hand check** — each deep backup copies all 500 records, so the two backups copy 1,000 records in total

*Example (italic):* Shallow: 1 new vs 3 deletes — ledger −2, crash; deep: 3 news vs 3 deletes — ledger 0, clean close.

**Key point:** A correctly written resource class keeps the allocation ledger balanced — in this run, 3 news against 3 deletes — no matter how many copies the program makes.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: `new` count vs `delete` count for the day's run, under the default shallow copy and under the rule-of-three deep copy.

- **Title (bold 15px, `#1a5276`, top center):** "The Allocation Ledger: news Must Equal deletes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = count 0 to 4, gridlines `#e5e9ef` at 1/2/3 with 12px `#444` tick labels; no x gridlines.
- **Groups:** "default (shallow)" centered at x=210 and "rule of three (deep)" centered at x=470, 13px `#444` labels below the baseline.
- **Bars:** 60px wide, 20px gap within a group; heights from counts news `[1, 3]`, deletes `[3, 3]` (45px per count). Shallow group: `new` bar blue `#2a78d6` (value 1), `delete` bar red `#e74c3c` (value 3, mismatch). Deep group: `new` bar blue (value 3), `delete` bar green `#008300` (value 3). Bold 12px value labels on top of each bar; 11px `#444` "new"/"delete" labels under each bar.
- **Annotation (bold 13px red `#e74c3c`, above the shallow group near y=70):** "2 deletes hit freed memory".
- **Annotation (bold 13px green `#008300`, above the deep group near y=70):** "ledger balances: 3 = 3".
- **Caption (12px `#444`, bottom right):** "one log opened, two backups taken — counts exact for this example".

## Why Three Grew to Five: the 2 GB Matrix

**Tags:** `where it's used` (blue), `move semantics` (green), `performance` (orange)

- **The matrix** — a data scientist's 2 GB feature matrix lives in one heap block owned by a `Matrix` class
- **The copy price** — a deep copy duplicates every byte: about 800 ms for 2 GB at ~2.5 GB/s memory bandwidth
- **The move** — a move constructor steals the pointer and nulls the source: three pointer writes, microseconds
- **Rule of five** — C++11 adds move constructor and move assignment to the three; write all five together
- **Where it bites** — returning a matrix from a function, growing a vector, sorting: without moves, all copies

*Example (italic):* Copying costs about 4 ms at 10 MB, 80 ms at 200 MB, 800 ms at 2 GB — a move costs the same near-zero time at every size.

**Key point:** Rule of five = rule of three + the two move operations; moves let ownership of a huge block transfer for the price of a pointer swap instead of a byte-by-byte copy.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: deep-copy time at three matrix sizes vs the flat cost of a move, on a schematic log-feel scale.

- **Title (bold 15px, `#1a5276`, top center):** "Copy Duplicates Every Byte; Move Swaps a Pointer".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "copy 10 MB": blue `#2a78d6` bar width 150, 11px `#444` label "4 ms" at bar end
  - "copy 200 MB": blue bar width 280, label "80 ms"
  - "copy 2 GB": blue bar width 440, label "800 ms"
  - "move (any size)": green `#008300` bar width 4, bold 11px green label "~0.0001 ms"
- **Bar style:** 16px tall, copy bars fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, move bar solid green.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=250):** "the move costs the same at every size — three pointer writes".
- **Caption (12px `#444`, bottom right):** "times illustrative at ~2.5 GB/s; pixel widths log-scaled schematic".

## Write Zero or Write Five, Never In Between

**Tags:** `common mistake` (red), `rule of zero` (green)

- **The half-fix** — writing only the destructor stops the leak but leaves the double-free copy bug alive
- **The silent slowdown** — declaring a destructor stops the compiler generating moves; every "move" becomes a copy
- **The tell** — the code still compiles and passes tests, but returning the 2 GB matrix now quietly costs 800 ms
- **Rule of zero** — hold the data in `std::vector` or `unique_ptr` and you need none of the five; they manage themselves
- **The decision** — define zero of the special functions or all five; one through four is the bug zone

*Example (italic):* A team adds just a destructor to `Matrix`; nothing crashes, but the pipeline slows because every pass-by-value now copies 2 GB.

**Common mistake:** Defining only some of the five special functions. The compiler fills the gaps with defaults that either shallow-copy the pointer or silently fall back from move to copy — write zero or write all five.

### Visualization (canvas `c4`, 720×300)

Strip diagram: six cells for "how many of the five special functions you defined" (0 through 5), safe zones at the ends, bug zone in the middle.

- **Title (bold 15px, `#1a5276`, top center):** "Write Zero or Write Five — In Between Is the Bug Zone".
- **Subtitle line (12px `#6b7280`, centered at y=58):** "the five: destructor, copy ctor, copy =, move ctor, move =".
- **Cells:** six rounded boxes 88px wide, 44px tall, 8px radius, left edges at x = `[70, 174, 278, 382, 486, 590]`, top y=95, bold 16px centered digits "0"–"5". Cell 0 fill `rgba(0,131,0,0.12)` border 2px `#008300`; cells 1–4 fill `rgba(231,76,60,0.12)` border 2px `#e74c3c`; cell 5 fill/border green like cell 0.
- **Zone labels (bold 12px, under the cells near y=170):** green `#008300` under cell 0: "rule of zero — vector / unique_ptr own it"; red `#e74c3c` centered under cells 1–4: "defaults fill the gaps: shallow copies, moves fall back to copies"; green under cell 5: "rule of five — you own it fully". Stagger the three labels across y=170/195/170 so they do not collide.
- **Annotation (bold 13px orange `#d95926`, centered near y=265):** "declaring just a destructor silently turns every move into a copy".
- **Caption (12px `#444`, bottom right):** "zones schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 500-record log, two backups (1,000 records copied), and copy times are invented and labeled illustrative; the ledger counts news `[1, 3]` / deletes `[3, 3]` are exact for the described run, and the copy times 4 / 80 / 800 ms follow exactly from 10 MB / 200 MB / 2 GB at 2.5 GB/s.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
