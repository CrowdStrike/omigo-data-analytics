# Send & Sync (Rust)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Send & Sync (Rust)

**Subtitle:** Send marks a value as safe to hand to another thread and Sync marks it as safe to look at from two threads at once — so a data race stops being a rare 2 a.m. bug and becomes an ordinary compile error

## Two Baristas, One Tally Clicker

**Tags:** `core idea` (blue), `hand over vs share` (green), `type stamps` (orange)

- **The shop** — two baristas share one tally clicker that counts the cups sold today
- **Hand it over** — passing the clicker between baristas is fine, as long as one holds it at a time
- **Grab at once** — both baristas clicking in the same instant is where counts silently go wrong
- **Two stamps** — Rust marks every type: Send = safe to hand over, Sync = safe to use at once
- **Checked, not hoped** — code that ships an unstamped value to another thread will not compile

*Example (italic):* A clicker with a fragile paper tape (`Rc`) works fine for one barista, but hand it across the counter and the tape can tear — so Rust refuses to compile the handoff.

**Key point:** Send and Sync are permission stamps the compiler reads on every type — a data race becomes a type error at build time, not a mystery at runtime.

### Visualization (canvas `c1`, 720×300)

Two-panel diagram: the left panel shows one barista handing the clicker to another (Send — move across), the right panel shows two baristas using the same clicker at the same moment (Sync — share across), each panel labeled with its stamp.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways a Value Crosses Threads: Hand It Over vs Share It".
- **Layout:** vertical 1px `#e5e9ef` divider at x=360 from y=55 to y=265; left panel centered on x=185, right panel centered on x=545.
- **Left panel (Send):** bold 13px `#2a78d6` header "Send — hand it over" at (185, 75, centered); two 40px circles outlined 2px `#2a78d6` at (105, 160) and (265, 160) with 12px `#444` labels "barista A" / "barista B" below; 22×16 rounded clicker box filled `rgba(42,120,214,0.35)` mid-flight at (185, 150); 3px blue `#2a78d6` arrow from (130, 155) to (240, 155) with arrowhead; 12px `#2a78d6` caption "one owner at a time" at (185, 205, centered).
- **Right panel (Sync):** bold 13px `#199e70` header "Sync — use it at once" at (545, 75, centered); two 40px circles outlined 2px `#199e70` at (465, 160) and (625, 160), same barista labels; one 26×18 clicker box outlined 2px `#199e70`, filled `rgba(25,158,112,0.20)`, at (545, 150); two short 2px aqua arrows pointing inward from each circle to the box; 12px `#199e70` caption "two readers, same instant" at (545, 205, centered).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=245):** "Send = may move across threads; Sync = may be shared across threads".
- **Footer (12px `#6b7280`, centered near y=280):** "the compiler checks both stamps before the program is allowed to run".

## How a Count Goes Missing

**Tags:** `worked example` (blue), `read-add-write` (green), `compile error` (red)

- **The plan** — each barista will add 1000 cups to the tally; it should end at exactly 2000
- **One click, three steps** — the machine really does read, add 1, write back; not one atomic motion
- **The collision** — A reads 7, B reads 7, A writes 8, B writes 8: two cups sold, tally moved by one
- **In Rust** — sharing an `Rc<Cell<i32>>` tally fails: "cannot be sent between threads safely"
- **The fix** — `Arc<Mutex<i32>>`: Arc makes handing out copies safe, Mutex makes clicks take turns

*Example (italic):* Follow it by hand: tally at 7 → both baristas read 7 → both write back 8 — the tally says 8 although 9 cups were sold.

**Key point:** A data race is exactly this four-step overlap — and Rust rejects the program that could ever perform it, before it runs even once.

### Visualization (canvas `c2`, 720×300)

Timeline diagram of the lost-update interleaving: two barista lanes across four time steps, with the shared tally's value tracked underneath, showing two "+1" clicks producing a total of +1.

- **Title (bold 15px, `#1a5276`, top center):** "Two Clicks, One Count: the Lost-Update Interleaving".
- **Layout:** four time-step columns centered at x = 190, 330, 470, 610; 12px `#6b7280` column headers "step 1" … "step 4" at y=62; lane labels 12px `#444` at x=25: "barista A" (y=115), "barista B" (y=175), "shared tally" (y=240).
- **Lane A events:** rounded 120×34 boxes centered at (190, 110) "reads 7" and (470, 110) "writes 8", 2px `#2a78d6` border, fill `rgba(42,120,214,0.12)`, bold 12px `#2a78d6` text.
- **Lane B events:** rounded 120×34 boxes centered at (330, 170) "reads 7" and (610, 170) "writes 8", 2px `#d95926` border, fill `rgba(217,89,38,0.12)`, bold 12px `#d95926` text.
- **Tally track:** horizontal 2px `#999` line at y=240 from x=120 to x=680; bold 13px `#2c3e50` values under each step at y=262: "7", "7", "8", "8"; thin dashed `#6b7280` (dash 4/3) vertical connectors from each event box down to the track.
- **Annotation (bold 13px red `#e74c3c`, near x=610, y=210):** two lines: "should be 9 —" / "one cup lost".
- **Caption (12px `#444`, bottom left):** "illustrative — B's write lands on top of A's, erasing it".

## The Bug That Never Shows Up in Tests

**Tags:** `why it matters` (blue), `heisenbug` (red), `where it's used` (green)

- **In C-style threads** — the same racy counter compiles cleanly and fails only sometimes, at random
- **Flaky totals** — six racy runs came back 1731, 1462, 1998, 1554, 1876, 2000 — same code, six answers
- **Worst kind of bug** — it passes every test on the laptop and loses counts on the busy server
- **In Rust** — the racy version is a compile error; the `Arc<Mutex<i32>>` version returns 2000 every run
- **The trade** — one argument with the compiler instead of a career of unreproducible bug reports

*Example (italic):* A team chased the missing-cups bug for a week; it never reproduced in testing because the test machine ran the two baristas one after the other.

**Key point:** Send and Sync move data races from "sometimes, in production, silently" to "always, at compile time, loudly".

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: six unsynchronized run totals wobbling below the expected 2000, then three Mutex-protected runs landing exactly on it, with the expected line dashed across both groups.

- **Title (bold 15px, `#1a5276`, top center):** "Final Tally per Run: Racy Counter vs Mutex Counter (target 2000)".
- **Axes:** origin x=70, baseline y=250, plot width 590, plot height 180; y = final tally 0 to 2200 with 12px `#444` tick labels "0", "500", "1000", "1500", "2000" and light `#e5e9ef` gridlines; x = nine run slots, 12px `#444` labels "run 1" … "run 6" then "run 1" … "run 3".
- **Racy group (6 bars, left, 44px wide, 14px gaps starting x=95):** heights for totals `[1731, 1462, 1998, 1554, 1876, 2000]`, fill `rgba(217,89,38,0.55)`, 1px `#d95926` border; bold 11px `#d95926` value labels above each bar.
- **Mutex group (3 bars, right, same width, starting x=480):** totals `[2000, 2000, 2000]`, fill `rgba(0,131,0,0.45)`, 1px `#008300` border; bold 11px `#008300` value labels "2000" above each.
- **Group captions (bold 12px, centered under each group at y=285):** orange `#d95926` "unsynchronized (won't compile in Rust)"; green `#008300` "Arc<Mutex<i32>>".
- **Expected line:** horizontal dashed `#6b7280` (dash 4/3) line at tally 2000 across the plot; 12px `#6b7280` label "expected 2000" at its left end.
- **Annotation (bold 12px red `#e74c3c`, near x=200, y=95):** two lines: "worst run lost 538 cups (1462) —" / "and no error was ever printed".
- **Caption (12px `#444`, bottom right):** "run totals illustrative".

## Send Is Not Sync

**Tags:** `common mistake` (red), `type matrix` (orange)

- **Send** — the value may move to another thread for good; the clicker may change hands
- **Sync** — the value may be viewed from two threads at once; sharing a reference is safe
- **`Cell<i32>`** — Send but not Sync: fine to hand over whole, never safe to share and poke
- **`MutexGuard`** — Sync but not Send: the lock token must be released by the thread that took it
- **The mix-up** — Sync does not mean "mutate freely"; the Mutex is what makes writing take turns

*Example (italic):* `Mutex<i32>` is both Send and Sync not because racing is fine, but because its lock forces every writer to wait its turn.

**Common mistake:** Reading Sync as "thread-safe to mutate". Sync only promises that shared viewing is safe — safe shared mutation is the Mutex's job, layered on top.

### Visualization (canvas `c4`, 720×300)

Two-column stamp matrix: six everyday types down the left, Send and Sync columns with green checks and red crosses, highlighting the two off-diagonal surprises.

- **Title (bold 15px, `#1a5276`, top center):** "The Stamp Matrix: Who May Move, Who May Be Shared".
- **Layout:** row labels right-aligned 13px monospace `#2c3e50` at x=270; column headers bold 13px `#1a5276` "Send" at x=390 and "Sync" at x=530 (y=70); six rows at y = 100, 130, 160, 190, 220, 250; alternate rows get a full-width `#f8f9fa` band.
- **Rows and marks (bold 16px, ✓ green `#008300`, ✗ red `#e74c3c`), centered on the column x positions:**
  - `i32`: ✓ ✓
  - `Rc<i32>`: ✗ ✗
  - `Arc<i32>`: ✓ ✓
  - `Cell<i32>`: ✓ ✗
  - `MutexGuard<i32>`: ✗ ✓
  - `Arc<Mutex<i32>>`: ✓ ✓
- **Highlights:** 1.5px orange `#d95926` rounded rectangles around the `Cell<i32>` and `MutexGuard<i32>` rows (the two one-stamp types).
- **Annotation (bold 12px orange `#d95926`, right side near x=640, y=175, two lines):** "one stamp without" / "the other is normal".
- **Footer (12px `#6b7280`, centered near y=285):** "✓ = the compiler allows it; ✗ = the program is rejected".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; inline code in bullets uses a monospace span with `#f8f9fa` background; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red `#e74c3c` appears only for the lost-count and heisenbug alarms.
- **Data:** every number is hardcoded above (no randomness): the 7→8 interleaving values, the six racy run totals `[1731, 1462, 1998, 1554, 1876, 2000]`, the three Mutex totals `[2000, 2000, 2000]`, and the six-row stamp matrix; invented run totals keep their "illustrative" captions, and text numbers must match chart numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
