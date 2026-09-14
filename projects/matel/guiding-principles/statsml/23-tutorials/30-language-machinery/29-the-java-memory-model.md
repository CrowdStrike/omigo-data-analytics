# The Java Memory Model

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Java Memory Model

**Subtitle:** When two threads share a variable, the language — not the hardware — decides what one thread may see of the other's writes, and happens-before is that written contract

## The OPEN Sign and the Price Board

**Tags:** `core idea` (blue), `two threads` (green), `stale reads` (orange)

- **The bakery** — a price board and an OPEN sign: one thread writes them, another thread reads them
- **The plan** — the owner sets `price = 42`, then flips `open = true`; two ordinary shared variables
- **The customer** — waits until `open == true`, then reads `price`, expecting today's 42
- **The surprise** — the customer can see OPEN and still read 0; the two writes arrived out of order
- **Why** — compilers and CPUs reorder and cache writes; each thread may watch a different replay
- **The contract** — the Java memory model spells out exactly which reads must see which writes

*Example (italic):* The owner swears "I wrote 42 before flipping the sign" — and a customer reading plain variables can still, legally, see OPEN and read 0.

**Key point:** Without a happens-before edge between two threads, "I wrote it first" means nothing to the thread doing the reading.

### Visualization (canvas `c1`, 720×300)

Two-lane timeline of one legal racy execution: the owner's two writes on the top lane, the customer's two reads on the bottom lane, with the flag getting through while the price does not.

- **Title (bold 15px, `#1a5276`, top center):** "Two Threads, One Bakery: the Sign Outruns the Price".
- **Lanes:** two horizontal 2px `#e5e9ef` lines at y=115 (owner) and y=205 (customer), from x=130 to x=690; bold 13px `#1a5276` lane labels at x=20: "owner thread" above y=115, "customer thread" above y=205.
- **Owner events:** rounded boxes 150×34 centered on the lane at x-centers `[210, 390]`, 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.12)`, bold 12px `#1a5276` text: "price = 42", "open = true".
- **Customer events:** same box style at x-centers `[470, 640]` on the lower lane: "sees open == true" (blue style), then "reads price → 0" with 2px red `#e74c3c` border and fill `rgba(231,76,60,0.10)`, bold 12px `#e74c3c` text.
- **Program-order arrows:** 2px `#6b7280` solid arrows with small arrowheads along each lane between that lane's boxes.
- **Flag arrow:** 2px dashed (dash 6/4) `#008300` arrow from "open = true" down to "sees open == true", 11px `#008300` label beside it: "the flag got through".
- **Broken arrow:** 2px dashed (dash 6/4) `#e74c3c` arrow from "price = 42" down toward "reads price → 0" with a bold 13px red "✕" drawn on its midpoint, 11px `#e74c3c` label: "the 42 didn't".
- **Time axis:** thin `#999` arrow at y=262 from x=130 to x=690, 12px `#444` label "time →" at its right.
- **Annotation (bold 13px red `#e74c3c`, near x=470, y=70):** "no happens-before edge — OPEN arrived, 42 didn't".
- **Caption (12px `#444`, bottom right):** "illustrative — one legal execution with plain variables".

## Counting the Race: 1,000,000 Openings

**Tags:** `worked example` (blue), `volatile` (green)

- **The rerun** — open the shop 1,000,000 times with plain variables and tally what the customer read
- **Mostly fine** — in 999,827 runs the customer saw OPEN and read the fresh price, 42
- **The stale few** — in 173 runs the customer saw OPEN yet read 0; rare, real, and perfectly legal
- **One keyword** — declare `volatile boolean open`; writing it now happens-before every later read of it
- **The chain** — price=42 comes before the flag write, the flag write before the flag read: 42 must show
- **New tally** — 1,000,000 fresh reads and 0 stale ones; the 0 is a promise, not a lucky streak

*Example (italic):* 173 stale reads in a million is the kind of bug that passes every test on a laptop and pages you at peak traffic.

**Key point:** Making the flag `volatile` chains price=42 → open=true → sees open into one happens-before path, so stale reads drop from 173 to exactly 0 — by contract, not by chance.

### Visualization (canvas `c2`, 720×300)

Two-bar chart comparing stale-read counts out of 1,000,000 runs: plain flag vs volatile flag, with the volatile bar sitting at a guaranteed zero.

- **Title (bold 15px, `#1a5276`, top center):** "Stale Reads per 1,000,000 Openings".
- **Axes:** origin x=90, baseline y=245, plot width 560, plot height 180; y axis 0 to 200 stale reads, light `#e5e9ef` gridlines at 50, 100, 150, 200 with 12px `#444` labels; x axis has no scale, just two category slots.
- **Bar data (hardcoded):** values `[173, 0]` for categories `["plain boolean open", "volatile boolean open"]`.
- **Bar 1:** width 140 centered at x=260, height scaled to 173, fill `rgba(231,76,60,0.35)`, 2px `#e74c3c` border; bold 13px `#e74c3c` value label "173" above the bar top.
- **Bar 2:** width 140 centered at x=520, height 0 drawn as a 3px `#008300` flat segment on the baseline; bold 13px `#008300` value label "0" just above it.
- **Category labels:** 12px `#444` centered under each bar below the baseline.
- **Annotation (bold 13px green `#008300`, near x=470, y=120):** two lines: "0 is a guarantee," / "not luck".
- **Caption (12px `#444`, bottom right):** "counts illustrative — real stale-read rates vary by hardware".

## One Rulebook of Edges, Everywhere

**Tags:** `where it's used` (blue), `happens-before` (green), `synchronization` (orange)

- **The edges** — program order, volatile write → read, unlock → lock, thread `start()` and `join()`
- **Chaining** — edges are transitive: a path from a write to a read means the read must see the write
- **Locks too** — everything written before releasing a lock is visible after the next grab of that lock
- **Data race** — one thread writes, another reads, no edge between them: the model allows weird results
- **Real code** — lazy init, double-checked locking, and every concurrent library lean on these edges
- **Portable** — one contract on x86 and ARM alike; the JVM plants the hardware fences for you

*Example (italic):* A cache refresher builds a whole new map, then swaps one volatile reference — readers get the old map or the new map, never a half-built one.

**Key point:** You never reason about CPU caches or fences directly — you find, or build, a happens-before path from the write to the read.

### Visualization (canvas `c3`, 720×300)

Two-column edge diagram: the owner's two writes on the left, the customer's two reads on the right, with program-order arrows down each column, the volatile edge across, and the transitive guarantee drawn as a derived arrow.

- **Title (bold 15px, `#1a5276`, top center):** "The happens-before Chain That Delivers the 42".
- **Column headers (bold 13px `#1a5276`, y=58):** "owner thread" centered at x=220, "customer thread" centered at x=540.
- **Owner boxes:** rounded boxes 200×36, 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.12)`, bold 12px `#1a5276` text, centered at x=220 with y-centers `[100, 180]`: "price = 42", "open = true  (volatile)".
- **Customer boxes:** same size, centered at x=540 with y-centers `[140, 220]`: "reads open → true", "reads price → 42" — the last box with 2px green `#008300` border, fill `rgba(0,131,0,0.12)`, bold 12px `#008300` text.
- **Program-order arrows:** 2px `#2a78d6` solid arrows down each column between its two boxes, 11px `#6b7280` label "program order" beside each.
- **Volatile edge:** 3px `#008300` solid arrow from "open = true" to "reads open → true", bold 12px `#008300` label above it: "volatile write → read".
- **Transitive edge:** 3px dashed (dash 6/4) `#4a3aa7` arrow from "price = 42" curving to "reads price → 42", bold 12px `#4a3aa7` label along it: "guaranteed by transitivity".
- **Annotation (bold 12px `#1a5276`, centered near y=278):** "a path of edges from write to read means the read must see the write".

## The Confusion: volatile Is Not a Lock

**Tags:** `common mistake` (red), `atomicity` (orange)

- **Two promises** — visibility means "you see my write"; atomicity means "my update is one step"
- **volatile gives one** — later reads see the latest volatile write; that is visibility, nothing more
- **count++** — is really read, add 1, write back: three steps another thread can barge into
- **The tally** — two threads each add 10,000 to a volatile counter; expected 20,000, one run got 13,742
- **The fix** — `AtomicInteger` or `synchronized` makes read-add-write one indivisible step: 20,000 every run

*Example (italic):* Two clerks both read the counter at 500 and both write back 501 — one whole increment vanishes, volatile or not.

**Common mistake:** Reaching for `volatile` to fix `count++`. Volatile orders and publishes individual writes; compound read-modify-writes need an atomic class or a lock.

### Visualization (canvas `c4`, 720×300)

Three-bar chart of final counter values after two threads each add 10,000: the expected total, the volatile counter's shortfall, and the atomic counter hitting the mark.

- **Title (bold 15px, `#1a5276`, top center):** "Two Threads × 10,000 Increments: Where Did 6,258 Go?".
- **Axes:** origin x=90, baseline y=245, plot width 560, plot height 180; y axis 0 to 22,000, light `#e5e9ef` gridlines at 5,000, 10,000, 15,000, 20,000 with 12px `#444` labels "5k", "10k", "15k", "20k".
- **Bar data (hardcoded):** values `[20000, 13742, 20000]` for categories `["expected", "volatile count++", "AtomicInteger"]`.
- **Bars:** width 110 centered at x = `[220, 400, 580]`; bar 1 fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; bar 2 fill `rgba(231,76,60,0.35)` with 2px `#e74c3c` border; bar 3 fill `rgba(0,131,0,0.20)` with 2px `#008300` border.
- **Value labels:** bold 13px above each bar top in the bar's border color: "20,000", "13,742", "20,000".
- **Target line:** horizontal dashed (dash 4/3) `#6b7280` line across the plot at the 20,000 level.
- **Category labels:** 12px `#444` centered under each bar below the baseline.
- **Annotation (bold 13px red `#e74c3c`, near x=400, y=95):** two lines: "6,258 increments lost —" / "visible, but not atomic".
- **Caption (12px `#444`, bottom right):** "illustrative single run — losses vary run to run".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; inline code in a light-gray monospace chip; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all counts and bar values are the hardcoded literals above (no randomness); 999,827 + 173 = 1,000,000 and 20,000 − 13,742 = 6,258 must stay consistent between text and charts; invented run counts keep their "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
