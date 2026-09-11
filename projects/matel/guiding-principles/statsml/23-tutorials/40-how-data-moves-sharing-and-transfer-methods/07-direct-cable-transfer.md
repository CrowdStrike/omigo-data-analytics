# Direct Cable Transfer

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Direct Cable Transfer

**Subtitle:** Two machines, one cable, no network — cross the transmit and receive wires and the files walk straight across

## Alice's Old Desktop, Bob's Laptop, and One Crossed Cable

**Tags:** `core idea` (blue), `no network` (green), `null modem` (orange)

- **The running example** — Alice's old desktop has no network card; her files must reach Bob's laptop
- **The serial port** — one pin transmits (TX), one receives (RX); it was built to talk to a modem
- **The null-modem trick** — cross the wires so each machine's TX lands on the other's RX
- **Three wires suffice** — transmit, receive, and ground; the cable itself is the whole network
- **Software on both ends** — a small program on each side sends and reassembles the bytes

*Example (italic):* Alice starts "send" on the desktop, Bob starts "receive" on the laptop, and the folder crawls across three wires.

**Key point:** Crossing transmit and receive is the whole trick — two machines talk directly, with no modem and no network anywhere.

### Visualization (canvas `c1`, 720×300)

Wiring schematic of a 3-wire null-modem link: two machine boxes with TX / RX / GND pins, transmit-to-receive wires crossing in the middle, ground running straight.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "A Null-Modem Link: Three Wires, Crossed on Purpose".
- **Boxes (rounded 8px, 160×130, y=85):** "Alice's desktop" / "(no network card)" at x=50, fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border; "Bob's laptop" at x=510, fill `rgba(25,158,112,0.12)`, 2px `#199e70` border. Labels bold 13px `#2c3e50`, centered near the box top (y=110 / y=128).
- **Pins:** 4px-radius dots on the inner edges at x=210 and x=510; TX at y=150, RX at y=185, GND at y=220. Pin labels 11px `#444`: "TX"/"RX"/"GND" just inside each box (right-aligned at x=204 for Alice, left-aligned at x=516 for Bob).
- **Wires:** Alice TX (210,150) → Bob RX (510,185), 3px `#2a78d6`, filled arrowhead at the Bob end; Bob TX (510,150) → Alice RX (210,185), 3px `#199e70`, arrowhead at the Alice end — the two lines cross mid-canvas. GND (210,220)–(510,220) straight, 2px `#6b7280`, 11px `#6b7280` label "shared ground" centered under it (y=238).
- **Annotation (bold 13px orange `#d95926`, centered, y=55):** "crossed on purpose — each transmit lands on the other's receive".
- **Caption (12px `#6b7280`, bottom center, y=285):** "schematic — a 3-wire null-modem serial link".

## Moving a 10 MB Folder: One Bit at a Time vs Eight Abreast

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Serial means single file** — bits leave one at a time down a single data wire
- **The spec rate** — a fast serial link runs at 115,200 bits per second (115.2 kbit/s)
- **Framing overhead** — each byte ships as 10 bits (start + 8 data + stop), so divide by 10, not 8
- **Serial time** — 115,200 ÷ 10 = 11,520 bytes/s; 10,000,000 ÷ 11,520 ≈ 870 s ≈ 14.5 min
- **Parallel means abreast** — 8 data wires carry a whole byte per step; ~70 KB/s on a basic port
- **Parallel time** — 10,000 KB ÷ 70 ≈ 143 s ≈ 2.4 min; enhanced EPP/ECP ports (~500 KB/s) ≈ 20 s

*Example (italic):* The same 10 MB folder is a lunch break over serial and under three minutes over a parallel cable.

**Key point:** Transfer time = size ÷ real per-byte rate — serial's framing spends 2 of every 10 bits before any data moves.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: time to move a 10 MB (10,000,000 byte) folder over the three port setups, on a shared seconds axis.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "Moving 10 MB: Serial vs Parallel Port".
- **Axis:** seconds 0–900 mapped to px x=230 (0 s) through x=680 (900 s); 1px `#999` baseline at y=245 and left axis; `#e5e9ef` vertical gridlines with 12px `#444` tick labels at 0 / 180 / 360 / 540 / 720 / 900; axis caption "seconds (180 s = 3 min)" 12px `#444` centered under the ticks.
- **Rows (bars 34px tall, centered on y=95 / 155 / 215, two-line 12px `#444` left labels at x=20):**
  - "Serial 115.2 kbit/s" / "~11.5 KB/s": blue `rgba(42,120,214,0.30)` bar to 870 s, 2px `#2a78d6` border, bold 13px `#2a78d6` value "870 s ≈ 14.5 min" drawn inside the bar's right end
  - "Parallel (basic)" / "~70 KB/s": aqua `rgba(25,158,112,0.30)` bar to 143 s, 2px `#199e70` border, bold 13px `#199e70` value "143 s ≈ 2.4 min" right of the bar end
  - "Parallel EPP/ECP" / "~500 KB/s": violet `rgba(74,58,167,0.30)` bar to 20 s, 2px `#4a3aa7` border, bold 13px `#4a3aa7` value "20 s" right of the bar end
- **Annotation (bold 13px orange `#d95926`, centered near x=470, y=190):** "8 wires abreast: 14.5 min → 2.4 min for the same folder".
- **Caption (12px `#6b7280`, bottom right):** "115,200 bit/s and 10-bit framing documented; sustained rates typical, illustrative".

## Target Disk Mode: One Laptop Boots as the Other's Disk

**Tags:** `where it's used` (blue), `rescue tool` (orange), `migration` (green)

- **Migration cables** — laplink-style software walked a whole PC, files and settings, over one cable
- **Target disk mode** — hold one key at power-on and the laptop starts as a bare external disk
- **No OS in the way** — the healthy machine reads the disk directly and copies what it needs
- **Rescue path** — works when the source machine's screen, OS, or network is dead
- **Modern descendant** — new phone and laptop setup copies everything over one USB-C cable
- **Same idea, faster wire** — ~70 KB/s parallel, ~30 MB/s FireWire, ~800 MB/s USB-C (illustrative)

*Example (italic):* Bob's laptop stalls at boot; in target disk mode its drive appears on Alice's machine like any plug-in disk, and the data walks off.

**Key point:** When the network is absent or the machine won't boot, a direct cable is still the recovery path — air-gapped labs and dead-machine rescues run on it.

### Visualization (canvas `c3`, 720×300)

Two-panel: left, a target-disk-mode sketch (laptop A boots as a bare disk, laptop B mounts it); right, log-scale bars of the one-cable idea's speed across three eras.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "One Laptop as the Other's Disk — the Idea Keeps Getting Faster".
- **Divider:** dashed 1px `#bdc3c7` vertical line at x=340 from y=40 to y=270.
- **Left panel:** box "laptop A" / "boots as bare disk" at x=25, y=100, 130×70, fill `rgba(74,58,167,0.15)`, 2px `#4a3aa7` border; cable line (155,135)–(200,135), 2.5px `#6b7280`; box "laptop B" / "sees a new drive" at x=200, y=100, 130×70, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; labels bold 12px `#2c3e50`, two lines. Under box A, 11px `#6b7280` two lines (y=192, y=207): "no OS running —" / "hold a key at power-on". Annotation bold 12px green `#008300` centered at x=178, y=240: "A's whole drive mounts like a plug-in disk".
- **Right panel (log-scale bars):** bars start at x=475, max extent x=690, 28px tall, centered on y=110 / 160 / 210; length proportional to log10(rate in KB/s) over the 0–6 range. Era labels 12px `#444`, right-aligned at x=465:
  - "1990s parallel": orange `rgba(217,89,38,0.30)` bar, log10(70)=1.85, 2px `#d95926` border, bold 12px `#d95926` value "~70 KB/s" right of the bar end
  - "2000s FireWire": aqua `rgba(25,158,112,0.30)` bar, log10(30,000)=4.48, 2px `#199e70` border, bold 12px `#199e70` value "~30 MB/s"
  - "today USB-C": violet `rgba(74,58,167,0.30)` bar, log10(800,000)=5.90, 2px `#4a3aa7` border, bold 12px `#4a3aa7` value "~800 MB/s"
- **Scale note (11px `#6b7280`, centered near x=530, y=248):** "bar length on a log scale".
- **Annotation (bold 12px orange `#d95926`, centered near x=530, y=70):** "~11,000× faster — same one-cable idea".
- **Caption (12px `#6b7280`, bottom center, y=288):** "rates typical for each era, illustrative".

## The Silent Cable: Straight-Through vs Null-Modem

**Tags:** `common mistake` (red), `debugging` (orange)

- **Identical plugs** — a straight-through and a null-modem cable look exactly the same outside
- **Straight-through** — wires TX to TX and RX to RX; both machines talk, neither listens
- **The symptom** — no error message, no data: each side waits forever on a silent receive pin
- **The fix** — the crossover belongs in the cable (or an adapter), not in the software
- **Modern echo** — early PC-to-PC Ethernet needed a crossover cable too; today's ports auto-swap

*Example (italic):* Alice's transfer sat at 0% for an hour; the software was fine — the cable wired each talker to the other's mouth, not its ear.

**Key point (Common mistake label):** Debugging the software first — a completely silent link usually means both sides transmit into deaf pins; check the cable's wiring.

### Visualization (canvas `c4`, 720×300)

Side-by-side wiring comparison: straight-through cable (TX–TX, RX–RX, fails) vs null-modem cable (TX–RX crossed, works), each as two mini machine boxes with two wires.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "Same Plug, Different Wiring: One Cable Is Silent, One Works".
- **Divider:** dashed 1px `#bdc3c7` vertical line at x=360 from y=40 to y=230.
- **Panel headings (bold 13px, centered, y=68):** "straight-through" in red `#e74c3c` at x=180; "null-modem" in green `#008300` at x=540.
- **Left panel (fails):** boxes 90×90 at x=30 and x=240, y=90, fill `#f5f6f8`, 2px `#6b7280` border, bold 12px `#2c3e50` labels "A" / "B" centered inside. Pins with 11px `#444` labels: TX at y=120, RX at y=160 on each box's inner edge. Wires 2.5px `#6b7280`: (120,120)–(240,120) and (120,160)–(240,160), straight across. Red `#e74c3c` bold 20px "✕" at (180,147). Label bold 12px `#e74c3c` centered at (180,215): "both talk, nobody listens".
- **Right panel (works):** boxes 90×90 at x=390 and x=600, y=90, same style. Wires: (480,120)–(600,160) in 2.5px `#2a78d6` and (480,160)–(600,120) in 2.5px `#199e70`, crossing mid-panel. Green `#008300` bold 20px "✓" at (540,147). Label bold 12px `#008300` centered at (540,215): "each TX feeds the other's RX".
- **Annotation (bold 13px orange `#d95926`, centered, y=252):** "a silent link is a wiring symptom — check the cable before the software".
- **Caption (12px `#6b7280`, bottom center, y=285):** "schematic — identical plugs, different internal wiring".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" in sections 1–3, "Common mistake:" in section 4).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` only for the genuine failure state (the straight-through panel in c4).
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). Documented facts: 115,200 bit/s is a standard serial rate; 8N1 framing is 10 bits per byte; null-modem wiring crosses TX and RX; parallel ports carry 8 data lines. Invented/typical figures (sustained ~70 KB/s and ~500 KB/s parallel rates, ~30 MB/s FireWire, ~800 MB/s USB-C) are labeled illustrative in each chart's caption. Arithmetic that must stay consistent between text and charts: 115,200 ÷ 10 = 11,520 bytes/s; 10,000,000 ÷ 11,520 ≈ 870 s ≈ 14.5 min; 10,000 KB ÷ 70 ≈ 143 s ≈ 2.4 min; 10,000 ÷ 500 = 20 s; 800,000 ÷ 70 ≈ 11,000×.
- This page has no links.
