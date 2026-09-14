# Monkey Patching

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Monkey Patching

**Subtitle:** Monkey patching changes a class or function in a running program — you fix the vendor's receipt code in memory without ever editing the vendor's files

## The Receipt Bug in Code You Can't Edit

**Tags:** `core idea` (blue), `runtime change` (green), `vendor code` (orange)

- **The shop** — a coffee shop's register runs on a vendor library, `pos_lib`, that totals receipts
- **The bug** — the library's `Order.total()` truncates the tax instead of rounding it, losing a penny
- **The wall** — the vendor's files are read-only, and the upstream fix is "scheduled next quarter"
- **The move** — at startup, one line reassigns the method: `Order.total = fixed_total`
- **The effect** — every order object, old or new, now runs the fixed code; no file was edited

*Example (italic):* At 7:00am the register boots, runs the one patch line, and every receipt that day totals correctly — `pos_lib.py` on disk is byte-for-byte untouched.

**Key point:** Monkey patching is replacing or adding attributes of a class or module at runtime — the change lives in the running program's memory, never in the source files.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the vendor file on disk (unchanged) vs the class object in memory (patched), each row boxes joined by arrows.

- **Title (bold 15px, `#1a5276`, top center):** "The File on Disk Never Changes — the Class in Memory Does".
- **Row 1 (y=95), label 12px `#444` at x=20:** "on disk"; blue `#2a78d6` rounded box at x=170 labeled "pos_lib.py — total() truncates" (12px), dashed 2px `#6b7280` (dash 4/3) arrow to a mute box at x=460 labeled "byte-for-byte unchanged".
- **Row 2 (y=205), label:** "in memory"; blue box at x=170 labeled "class Order (loaded 7:00am)"; solid 3px green `#008300` arrow with bold 12px green label "Order.total = fixed_total" to a green box at x=460 labeled "every live order uses the fix".
- **Box style:** 170–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(107,114,128,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "restart without the patch line and the bug comes straight back".

## One Penny on a $7.19 Order

**Tags:** `worked example` (blue), `hand-check` (green)

- **The order** — a cappuccino at $4.25 plus a bagel at $2.94 gives a subtotal of $7.19
- **The tax** — 8% of $7.19 is $0.5752, which should round up to $0.58
- **The bug** — the vendor code truncates $0.5752 down to $0.57, so the receipt says $7.76
- **The patch** — `fixed_total` rounds instead: tax $0.58, receipt total $7.77
- **The proof** — call `order.total()` before the patch: $7.76; after: $7.77 — same object, same call

*Example (italic):* The patch lands mid-morning; the very order sitting open on the register re-totals to $7.77 the next time `total()` is called on it.

**Key point:** The patch swaps what the method name points to on the class; existing objects pick up the new behavior because Python looks the method up on the class at call time.

### Visualization (canvas `c2`, 720×300)

Computation-flow diagram: one order's numbers flow left to right, then branch into the vendor path (truncate) and the patched path (round).

- **Title (bold 15px, `#1a5276`, top center):** "One Call, Two Answers: $0.5752 Tax, Truncated vs Rounded".
- **Start boxes:** blue `#2a78d6` rounded box at (x=40, y=130), 140×44, labeled "subtotal $7.19" (12px `#2c3e50`); solid 3px `#2c3e50` arrow to a blue box at (x=235, y=130), 150×44, labeled "× 8% = $0.5752".
- **Branch arrows from x≈390:** one 3px arrow up-right to a magenta `#d55181` box at (x=460, y=62), 200×44, fill `rgba(213,81,129,0.12)`, labeled "truncate → $0.57, total $7.76", with 12px magenta label "vendor total()" above it; one 3px arrow down-right to a green `#008300` box at (x=460, y=198), 200×44, fill `rgba(0,131,0,0.12)`, labeled "round → $0.58, total $7.77", with 12px green label "patched total()" below it.
- **Data (hardcoded literals):** items `["cappuccino", "bagel"]`, prices `[4.25, 2.94]`, subtotal `7.19`, rate `0.08`, raw tax `0.5752`, results `[7.76, 7.77]`.
- **Annotation (bold 13px green `#008300`, centered near y=285):** "same object, same call — one line changed the answer".
- **Caption (12px `#444`, bottom right):** "prices illustrative; the $0.5752 arithmetic is exact".

## Patches You Already Use Every Day

**Tags:** `where it's used` (blue), `testing` (green), `data tools` (orange)

- **Test fakes** — `mock.patch` swaps a real warehouse query for a canned 5-row answer in a test
- **Progress bars** — tqdm patches pandas to add `df.progress_apply()` with a live progress bar
- **Async rewiring** — gevent's `monkey.patch_all()` rewires sockets so old code runs async
- **Hotfixes** — a one-line patch bridges the months until the vendor ships the real fix
- **Instrumentation** — wrap a library function to log its arguments and timing without forking it

*Example (italic):* A test that hit the real warehouse took 40 seconds; with `query_db` patched to return 5 fixed rows it runs in 0.01 seconds.

**Key point:** Data scientists monkey patch constantly, often without noticing — every `mock.patch` in a test suite is a scoped, self-removing monkey patch.

### Visualization (canvas `c3`, 720×300)

Timeline diagram of a test run: the real function before and after, a shaded band where `mock.patch` holds the fake in place.

- **Title (bold 15px, `#1a5276`, top center):** "mock.patch: a Monkey Patch That Removes Itself".
- **Axis:** horizontal 2px `#999` timeline at y=200 from x=60 to x=660; 12px `#444` phase labels beneath it.
- **Data (hardcoded literals):** phases `["before", "inside", "after"]`, segment x-ranges `[[60,250],[250,470],[470,660]]`, call times in seconds `[40, 0.01, 40]`.
- **Shaded band:** aqua fill `rgba(25,158,112,0.15)` rectangle from x=250 to x=470, y=80 to y=200, dashed 2px `#199e70` border; bold 12px aqua `#199e70` label "with mock.patch('query_db')" centered at the band's top.
- **Segments:** before, 4px blue `#2a78d6` line along the axis with 12px blue label "real query_db() — 40 s"; inside, 4px aqua `#199e70` line with label "fake — 5 rows, 0.01 s"; after, 4px blue line with label "real again — 40 s".
- **Markers:** vertical dashed `#6b7280` (dash 4/3) lines at x=250 and x=470 with 12px `#6b7280` labels "patch on" / "patch off".
- **Annotation (bold 13px violet `#4a3aa7`, near x=360, y=55):** "scoped: the patch dies at the end of the with-block".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## The Change That Appears in No File

**Tags:** `common mistake` (red), `global effect` (orange)

- **Global reach** — the class is patched for the whole process; every import sees the change
- **Invisible diff** — the source on disk still shows the old behavior; reading the file misleads
- **Upgrade traps** — library v2.1 rewrites internals; the forgotten patch overwrites the new method
- **Patch collisions** — two libraries patching the same method: the last import wins, silently
- **Debug cost** — a teammate steps through the file, sees correct-looking code, and trusts it

*Example (italic):* The vendor ships the tax fix in v2.1, but the forgotten patch still overwrites `total()` with code written for v2.0's fields — receipts break on upgrade day.

**Common mistake:** Patching and forgetting. A monkey patch is a loan against the library's next release — keep it in one file, log it loudly at startup, and delete it when upstream ships the fix.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a forgotten patch colliding with the library's own fix (breaks) vs a managed patch that steps aside (smooth).

- **Title (bold 15px, `#1a5276`, top center):** "A Patch Leaves No Diff: Code That Isn't in Any File".
- **Row 1 (y=95), label 12px `#444` at x=20:** "forgotten patch"; blue `#2a78d6` rounded box at x=160 labeled "v2.1 ships the real fix" (12px), 3px arrow to a magenta `#d55181` box at x=430 labeled "old patch still overwrites total()" with bold 12px magenta "✗ breaks on upgrade day".
- **Row 2 (y=205), label:** "managed patch"; blue box at x=160 labeled "patches.py — logged at startup", 3px arrow to a green `#008300` box at x=430 labeled "version check: skip patch on v2.1" with bold 12px green "✓".
- **Box style:** 170–200px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(213,81,129,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "log it loudly, tie it to a ticket, delete it when upstream fixes".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all figures are the hardcoded literals above (no randomness); the prices `[4.25, 2.94]`, the 8% rate, and the test timings `[40, 0.01, 40]` are invented and labeled illustrative; the tax arithmetic is exact: 4.25 + 2.94 = 7.19, 7.19 × 0.08 = 0.5752, truncate → 0.57 (total 7.76), round → 0.58 (total 7.77).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
