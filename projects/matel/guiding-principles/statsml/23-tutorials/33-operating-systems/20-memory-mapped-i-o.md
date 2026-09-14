# Memory-Mapped I/O

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Memory-Mapped I/O

**Subtitle:** The CPU talks to hardware by writing to special memory addresses — storing a byte at the right address doesn't save data, it starts a machine

## The Cubby That Starts the Roaster

**Tags:** `core idea` (blue), `addresses as commands` (green), `hardware` (orange)

- **The cubby wall** — a coffee shop has 256 numbered cubbies; staff drop cups and notes into them all day
- **Ordinary slots** — cubbies 0 through 239 just hold things: put a note in, the same note comes back out
- **The special slot** — cubby 240 is not storage; it is a chute wired straight into the roaster
- **The trick** — dropping an order slip into 240 starts the roaster; peeking into 241 shows its status light
- **The CPU's version** — some memory addresses are wired to device registers, not RAM chips
- **Same instruction** — the CPU uses its normal store instruction; the address decides who receives it

*Example (italic):* A barista "stores" the slip `dark roast, 2 kg` into cubby 240 and the roaster drum starts turning — no special verb, just the right slot.

**Key point:** Memory-mapped I/O carves device registers into the ordinary address space — a plain load or store to those addresses reads a sensor or commands hardware, so talking to a device looks exactly like touching memory.

### Visualization (canvas `c1`, 720×300)

Horizontal address-strip diagram of a 256-address space: a long bar where most of the strip is RAM and the last 16 addresses are device registers, with a store arrow landing on address 240.

- **Title (bold 15px, `#1a5276`, top center):** "One Address Space, Two Kinds of Slots: RAM Ends Where the Roaster Begins".
- **Strip geometry:** one rounded bar from x=60 to x=660 at y=140, 56px tall; addresses 0–255 map linearly, so RAM spans x=60 to x=622 and device registers span x=622 to x=660.
- **RAM segment (addresses 0–239):** fill `rgba(42,120,214,0.25)`, 2px `#2a78d6` border, bold 13px `#2a78d6` label "RAM — 240 ordinary cubbies" centered inside; 12px `#6b7280` tick labels "0" and "239" below the segment ends.
- **Device segment (addresses 240–255):** fill `rgba(217,89,38,0.25)`, 2px `#d95926` border; 12px `#6b7280` tick labels "240" and "255" below; bold 12px `#d95926` label "device registers" above the segment at y=110.
- **Store arrow:** 3px green `#008300` arrow from a rounded box at (x=430, y=45) labeled "store 72 → address 240" (12px `#2c3e50`, fill `rgba(0,131,0,0.12)`) down into the device segment at x=626.
- **Register callouts (12px `#444`, right side below strip):** "240 = serial data (the chute)" and "241 = serial status (the light)" at y=230 and y=248, x=430.
- **Annotation (bold 13px violet `#4a3aa7`, x=70, y=270):** "same store instruction — the address picks RAM or hardware".
- **Caption (12px `#444`, bottom right):** "256-address machine, illustrative".

## Sending "HI" Out a Serial Port by Hand

**Tags:** `worked example` (blue), `poll the status` (green)

- **The map** — addresses 0–239 are RAM; 240 is the serial data register, 241 is the status register
- **The rule** — status 1 means the port is ready; status 0 means it is still shipping the last byte
- **Step 1** — read 241, get 1 (ready), so store 72 (the code for "H") into 240; the wire starts sending
- **Step 2** — read 241, get 0; read again, 0; read a third time, 1 — the port is ready again
- **Step 3** — store 73 (the code for "I") into 240; "HI" is now on the wire
- **Hand-check** — 2 data writes plus 4 status reads = 6 bus operations to send 2 characters

*Example (italic):* The whole transmission is six ordinary memory operations: R241→1, W240=72, R241→0, R241→0, R241→1, W240=73.

**Key point:** A device driver is often just this loop — read the status address until it says ready, write the data address, repeat — and every step is a plain memory instruction you can replay by hand.

### Visualization (canvas `c2`, 720×300)

Timeline of the six bus operations as labeled boxes on a horizontal axis: status reads (blue when ready, yellow when busy) and data writes (green), each showing address and value.

- **Title (bold 15px, `#1a5276`, top center):** "Six Memory Operations Spell 'HI': Poll 241, Write 240".
- **Axis:** 2px `#999` baseline at y=245 from x=60 to x=660; 12px `#444` tick labels "op 1" … "op 6" centered under each box; y is categorical (no gridlines).
- **Boxes:** six rounded boxes 84px wide, 52px tall, centered at x = `[100, 196, 292, 388, 484, 580]`, y=160; two text lines each (12px `#2c3e50`): operations `["R 241", "W 240", "R 241", "R 241", "R 241", "W 240"]`, values `["→ 1 ready", "= 72 'H'", "→ 0 busy", "→ 0 busy", "→ 1 ready", "= 73 'I'"]`.
- **Box colors:** ready reads (ops 1, 5) fill `rgba(42,120,214,0.20)` with 2px `#2a78d6` border; busy reads (ops 3, 4) fill `rgba(201,133,0,0.18)` with 2px `#c98500` border; writes (ops 2, 6) fill `rgba(0,131,0,0.15)` with 2px `#008300` border.
- **Wire row:** 12px `#6b7280` label "on the wire:" at (x=60, y=95); bold 13px aqua `#199e70` "H" appearing above op 2 at y=95 and "HI" above op 6 at y=95.
- **Poll bracket:** thin dashed `#6b7280` bracket (dash 4/3) spanning ops 3–5 at y=125 with 12px `#6b7280` label "poll until ready".
- **Annotation (bold 13px green `#008300`, centered at y=280):** "no special I/O instruction anywhere — just loads and stores".
- **Caption (12px `#444`, bottom right):** "timings illustrative; 72/73 are the real codes for H/I".

## From Device Registers to 40 GB Files

**Tags:** `where it's used` (blue), `memory-mapped files` (green), `big data` (orange)

- **Embedded chips** — blinking an LED on a microcontroller is one store to a pin-control address
- **Screens** — a framebuffer maps every pixel to an address; drawing is writing bytes into it
- **Files too** — `mmap` extends the trick: the OS maps a file's bytes into your address space
- **Lazy loading** — a mapped page is fetched from disk only when your code first touches it
- **The math** — reading a 40 GB file fully at 0.5 GB/s takes 80 s; a memmap touches one 4 KB page
- **Data science** — numpy's `memmap` opens a 40 GB array on a 16 GB laptop because nothing loads up front

*Example (italic):* `np.memmap('features.dat')` on a 40 GB file returns in milliseconds and reads row 3 by pulling in a single 4 KB page, while `read()` would need 40 GB of RAM and about 80 seconds.

**Key point:** The same idea scales from a 1-byte LED register to a 40 GB dataset — map the thing into the address space, then plain reads and writes do the I/O, fetching only what is actually touched.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing "read whole file into RAM" vs "memory-map the file" for a 40 GB file on a 16 GB machine: RAM needed and time to first row.

- **Title (bold 15px, `#1a5276`, top center):** "40 GB File, 16 GB Laptop: Full Read vs Memory Map".
- **Axis:** labels left-aligned 12px `#444` at x=20, bars start at x=230, max width 420; pixel widths hardcoded (schematic, not to scale).
- **Rows (top to bottom at y = 70, 115, 185, 230):**
  - "full read — RAM needed 40 GB": red `#e74c3c` bar width 420 with a vertical dashed `#6b7280` line at width 168 labeled "16 GB machine limit" (12px `#6b7280`) — the bar crosses the limit
  - "full read — time to first row 80 s": yellow `#c98500` bar width 420, 11px `#444` label "80 s" at bar end
  - "memmap — RAM needed 4 KB (one page)": green `#008300` bar width 4, 11px `#444` label "4 KB" beside it
  - "memmap — time to first row ~0.1 ms": green `#008300` bar width 3, 11px `#444` label "~0.1 ms" beside it
- **Bar style:** 16px tall, fills solid at 0.85 alpha, 1px darker border of the same hue.
- **Annotation (bold 13px magenta `#d55181`, right side near y=265):** "map first, load only the pages you touch".
- **Caption (12px `#444`, bottom right):** "bar widths schematic; 80 s = 40 GB ÷ 0.5 GB/s, other numbers illustrative".

## The Status Read the Compiler Threw Away

**Tags:** `common mistake` (red), `volatile` (orange)

- **The trap** — to a compiler, address 241 looks like ordinary memory that nobody else changes
- **The "optimization"** — it hoists the read out of the poll loop: read 241 once, reuse the value forever
- **The hang** — the one read returns 0 (busy), the cached 0 never changes, the loop spins forever
- **The fix** — marking the pointer `volatile` tells the compiler every read must really happen
- **Reads have side effects too** — on some chips reading a register clears a flag, so extra reads also break things

*Example (italic):* The driver polls status 241 in a `while` loop; with optimization on, the compiled loop reads the register once, sees 0, and hangs the boot — adding `volatile` fixes it.

**Common mistake:** Treating a device register like normal memory. Hardware changes the value behind the compiler's back, so the compiler must be told (`volatile`) that every single load and store is a real, visible event.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the optimized poll loop reading status once and spinning forever vs the volatile loop re-reading until the value flips to 1.

- **Title (bold 15px, `#1a5276`, top center):** "Poll Loop vs the Optimizer: Why the Register Must Be volatile".
- **Row 1 (y=95), label 12px `#444` at x=20:** "without volatile"; blue `#2a78d6` rounded box at x=170 labeled "read 241 once → 0" (12px), 3px arrow to a red `#e74c3c` box at x=400 labeled "reuse cached 0 forever" with bold 12px red "✗ loop never exits" at x=590.
- **Row 2 (y=205), label:** "with volatile"; blue box at x=170 labeled "read 241 → 0", 3px arrow to a blue box at x=340 labeled "read again → 0", 3px arrow to a green `#008300` box at x=510 labeled "read again → 1" with bold 12px green "✓ exit loop" beneath it at y=245.
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Hardware note:** 12px `#6b7280` italic label at (x=170, y=150): "hardware flips 241 to 1 when ready — the cached copy never sees it".
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "the address is a wire to a device, not a slot the compiler owns".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 256-address map (RAM 0–239, data register 240, status register 241), the six-operation sequence (ops `["R 241","W 240","R 241","R 241","R 241","W 240"]`, values 1/72/0/0/1/73) and character codes 72='H', 73='I' are exact; the 40 GB / 16 GB / 0.5 GB/s / 80 s / 4 KB file numbers and the c3 bar pixel widths (420/420/4/3, limit line at 168) are invented and labeled illustrative/schematic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
