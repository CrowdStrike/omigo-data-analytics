# System Calls & Kernel vs User Space

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** System Calls & Kernel vs User Space

**Subtitle:** A system call is how a program asks for anything — a file, a packet, a line on the screen — from the kernel, the only code allowed behind the counter

## The Counter You Can't Walk Behind

**Tags:** `core idea` (blue), `protection` (green), `the counter` (orange)

- **The café** — your program is a customer in a coffee shop; the espresso machine sits behind the counter
- **The rule** — customers never touch the machine; they place an order at the counter and wait
- **User space** — where ordinary programs run: your script, pandas, the browser, the shell
- **Kernel space** — where the operating system runs with full power over disk, memory, network, screen
- **The system call** — the only doorway: the program stops, asks the kernel, and gets a result back

*Example (italic):* When your script calls `open('sales.csv')`, it is a customer at the counter — only the kernel actually touches the disk.

**Key point:** A program cannot read a file, send a packet, or print a character by itself — every one of those is an order placed with the kernel through a system call.

### Visualization (canvas `c1`, 720×300)

Two-zone schematic: a user-space zone (café floor) and a kernel-space zone (behind the counter) separated by a dashed "counter" line, with a system-call arrow crossing over and a result arrow crossing back.

- **Title (bold 15px, `#1a5276`, top center):** "The Only Way Across the Counter Is a System Call".
- **User zone:** rounded rect x=40, y=60, width 290, height 185, fill `rgba(42,120,214,0.10)`, 2px `#2a78d6` border; bold 13px `#2a78d6` label "USER SPACE — the café floor" at its top; inside, three 12px `#2c3e50` boxes (fill `rgba(42,120,214,0.15)`, width 150, height 26) at y = 105, 145, 185 labeled "your script", "pandas", "the shell".
- **Kernel zone:** rounded rect x=400, y=60, width 290, height 185, fill `rgba(0,131,0,0.10)`, 2px `#008300` border; bold 13px `#008300` label "KERNEL SPACE — behind the counter"; inside, three boxes at the same y positions labeled "disk driver", "network stack", "memory manager".
- **Counter line:** vertical dashed `#6b7280` (dash 5/4) line at x=365 from y=52 to y=255, 12px `#6b7280` label "the counter" just above it.
- **Call arrow:** 3px `#d95926` arrow left-to-right crossing the counter at y=125, bold 12px `#d95926` label "system call: open / read / write" above it.
- **Return arrow:** 3px `#199e70` arrow right-to-left crossing at y=195, 12px `#199e70` label "result comes back" below it.
- **Annotation (bold 13px ink `#1a5276`, bottom center near y=280):** "no program touches hardware directly — it always asks".
- **Caption (12px `#444`, bottom right):** "schematic — no numeric data".

## Eight Trips to the Counter for One Small File

**Tags:** `worked example` (blue), `syscall trace` (green)

- **The program** — copy a 4,096-byte file to the screen, reading it 1,024 bytes at a time
- **open** — 1 call: the kernel checks permissions and hands back a file descriptor (a claim ticket)
- **read ×5** — four reads return 1,024 bytes each (4 × 1,024 = 4,096); a fifth returns 0: end of file
- **write** — 1 call sends the whole 4,096-byte buffer to the screen
- **close** — 1 call returns the ticket; total trips to the counter: 1 + 5 + 1 + 1 = 8

*Example (italic):* Run the copy under a syscall tracer and you count exactly 8 lines: one open, five reads, one write, one close.

**Key point:** Every syscall is a round trip — the CPU switches into kernel mode, does the work, and switches back — so this one small copy pays for 8 border crossings.

### Visualization (canvas `c2`, 720×300)

Square-wave mode timeline: a line that runs along the "user mode" level and dips down to the "kernel mode" level once for each of the 8 syscalls of the copy program.

- **Title (bold 15px, `#1a5276`, top center):** "One File Copy = 8 Trips into the Kernel".
- **Axes:** origin x=60, baseline y=245, plot width 600; two dashed `#e5e9ef` guide lines across the plot at y=110 and y=210 with 12px `#444` labels "user mode" (left, above y=110) and "kernel mode" (left, above y=210).
- **Mode line:** blue `#2a78d6` 3px square wave starting at (60, 110); it dips to y=210 for a 40px-wide well centered at each of x = `[90, 165, 240, 315, 390, 465, 540, 615]`, returning to y=110 between wells; each well filled `rgba(0,131,0,0.20)`.
- **Call labels (12px `#444`, centered under the baseline at y=265, one per well):** `["open", "read", "read", "read", "read", "read=0", "write", "close"]`.
- **Byte labels (11px `#6b7280`, inside the four data-read wells):** "1,024 B" under each of the reads at x = 165, 240, 315, 390.
- **Annotation (bold 13px orange `#d95926`, top right near y=70):** "8 switches down, 8 back up — for 4,096 bytes".
- **Caption (12px `#444`, bottom right):** "trace of the 4,096-byte copy; spacing schematic, counts exact".

## Why Your CSV Loader Cares About Buffer Size

**Tags:** `where it's used` (blue), `performance` (green), `buffering` (orange)

- **The CSV** — a 64 MB file read 1 KB at a time costs 65,536 read syscalls; 1 MB at a time costs 64
- **The math** — 67,108,864 ÷ 1,024 = 65,536 calls; 67,108,864 ÷ 1,048,576 = 64 calls, same bytes
- **sys time** — the `time` command splits a run into user (your code) and sys (kernel work for you)
- **The fix** — buffered readers batch tiny asks into big syscalls; pandas and file objects do this
- **The smell** — a "slow" loader with high sys time is often making millions of tiny syscalls

*Example (italic):* At roughly 1 µs of switching overhead per call, 65,536 reads waste ~66 ms while 64 reads waste ~0.06 ms — a 1,024× gap for the same bytes.

**Key point:** Crossing the user/kernel border is the expensive part, not the bytes — batch many small requests into a few big syscalls.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: number of read syscalls needed to pull the same 64 MB file through four different buffer sizes, log-feel bar widths.

- **Title (bold 15px, `#1a5276`, top center):** "Same 64 MB File, Four Very Different Syscall Bills".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "1 KB buffer — 65,536 calls": orange `#d95926` bar width 440, 12px orange label "~66 ms" at bar end
  - "4 KB buffer — 16,384 calls": yellow `#c98500` bar width 320, 11px label "~16 ms"
  - "64 KB buffer — 1,024 calls": blue `#2a78d6` bar width 180, 11px label "~1 ms"
  - "1 MB buffer — 64 calls": green `#008300` bar width 60, 11px label "~0.06 ms"
- **Bar style:** 16px tall, solid fills at 0.85 alpha (e.g. `rgba(217,89,38,0.85)`), count labels stay in the row label, time labels at bar ends.
- **Annotation (bold 13px green `#008300`, bottom right near y=255):** "1,024× fewer calls just by choosing a bigger buffer".
- **Caption (12px `#444`, bottom right):** "call counts exact (67,108,864 bytes ÷ buffer); ~1 µs per call illustrative; widths schematic".

## It Looks Like a Function Call — It Isn't

**Tags:** `common mistake` (red), `mode switch` (orange)

- **Looks familiar** — `read()` sits in your code like any function, so people assume it costs the same
- **The switch** — a syscall traps into the kernel: save registers, raise privilege, switch stacks, return
- **The cost** — a local function call is ~2 ns; a trivial syscall like `getpid()` is ~300 ns
- **Root confusion** — running as root does not put you in kernel mode; root programs still make syscalls
- **Not a place** — kernel space is a CPU privilege level, not a separate chip or a separate machine

*Example (italic):* A loop calling `getpid()` ten million times spends ~3 s just crossing the border; the same loop calling a local function takes ~0.02 s.

**Common mistake:** Treating a system call as a free function call. The syntax is identical; the price is a full privilege switch — hundreds of times the cost of a normal call.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart comparing the cost of one local function call against two syscalls, log-feel bar widths.

- **Title (bold 15px, `#1a5276`, top center):** "Same Syntax, Very Different Price per Call".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 80, 140, 200), each with a left-aligned 12px `#444` label at x=20:**
  - "local function call — ~2 ns": green `#008300` bar width 24
  - "getpid() syscall — ~300 ns": orange `#d95926` bar width 300
  - "read() of 1 KB — ~1,000 ns": violet `#4a3aa7` bar width 420
- **Bar style:** 16px tall, solid fills at 0.85 alpha, 11px `#444` nanosecond labels at bar ends.
- **Annotation (bold 13px magenta `#d55181`, bottom center near y=255):** "the border crossing alone costs ~150× the function call".
- **Caption (12px `#444`, bottom right):** "nanosecond costs illustrative (typical order of magnitude); widths schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 8-call trace (1 open + 5 reads + 1 write + 1 close for a 4,096-byte file in 1,024-byte chunks) and the syscall counts 65,536 / 16,384 / 1,024 / 64 are exact arithmetic (67,108,864 bytes ÷ buffer size); per-call costs (~1 µs switch overhead, ~2 ns function call, ~300 ns getpid, ~1,000 ns read) are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
