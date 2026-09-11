# Infrared & Bluetooth

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Infrared & Bluetooth

**Subtitle:** Point-and-beam infrared gave way to paired short-range radios — how two devices agree to talk and trickle files between phones

## Two Phones Nose-to-Nose: Beaming a File on a Light Beam

**Tags:** `core idea` (blue), `IrDA beaming` (orange), `short range` (green)

- **The running example** — Alice beams a contact to Bob's phone, infrared ports aligned
- **A TV-remote beam** — IrDA is the same invisible light as a remote control, carrying a file
- **Line of sight** — the ports must face each other within about a meter; a hand blocks it
- **Move and it dies** — tilt either phone and the beam misses; the transfer aborts mid-file
- **Radio replaces light** — Bluetooth swaps the beam for a short-range radio; no aiming needed

*Example (italic):* At a 2002 lunch table, trading one ringtone meant two phones held nose-to-nose, dead still, for minutes.

**Key point:** Infrared is light, so it needs a clear straight path; Bluetooth is radio, so the two devices only need to be near each other.

### Visualization (canvas `c1`, 720×300)

Two-panel schematic: a light beam that needs aiming vs a radio that needs only proximity.

- **Title (bold 15px, `#1a5276`, top center):** "Light Needs a Path, Radio Needs Proximity (schematic)".
- **Divider:** dashed 1px `#bdc3c7` vertical line at x=360, y=40–282.
- **Left panel (IrDA):** two rounded phone rectangles (50×110, 2px `#2a78d6` border, `rgba(42,120,214,0.10)` fill) at x=55 and x=245, both at y=85; small filled `#d95926` squares (8×8) on the facing edges at y≈130 as IR ports; a translucent orange beam (`rgba(217,89,38,0.15)` filled triangle from the left port widening from ±4px to ±10px at the right port) with dashed 1.5px `#d95926` edge lines. Labels 12px `#444` centered under the panel: "IrDA: invisible light, ports face-to-face, ≤ ~1 m" (y=232). Red hazard note bold 12px `#e74c3c` (y=252, centered x=180): "tilt either phone → the beam misses, transfer aborts".
- **Right panel (Bluetooth):** one phone rectangle (46×95, 2px `#199e70` border, `rgba(25,158,112,0.10)` fill) at x=420, y=95; three concentric radio arcs (1.5px `#199e70`, radii 40/68/96, right half only) centered on the phone's right edge midpoint; a second phone rectangle at x=615, y=105 (40×80, 2px `#4a3aa7` border, `rgba(74,58,167,0.10)` fill) sitting inside the outer arc. Label 12px `#444` centered (y=232): "Bluetooth: ~10 m radio — no aiming, no line of sight". Green annotation bold 12px `#008300` (y=252, centered x=540): "works from a pocket".
- **Caption (12px `#6b7280`, bottom right):** "ranges typical, illustrative".

## Sending a 3 MB Song: One Division, Three Radios

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The song** — a 3 MB file is 3 × 8 = 24 megabits to move; time = bits ÷ link speed
- **IrDA (~115 kbit/s)** — 24,000 kbit ÷ 115 ≈ 209 s ≈ 3.5 min of holding perfectly still
- **Classic Bluetooth (~2 Mbit/s)** — 24 Mbit ÷ 2 ≈ 12 s, phones loose in two pockets
- **BLE's different goal** — tiny messages on a coin cell; a heart-rate reading is ~20 bytes
- **Units trap** — file sizes are bytes, link speeds are bits; multiply MB by 8 first

*Example (italic):* Alice's 3.5-minute statue act over infrared becomes a 12-second background job over Bluetooth.

**Key point:** The same division as any link — size ÷ speed — but the bytes-vs-bits unit mix-up is where most hand calculations go wrong.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: time to send the same 3 MB song over IrDA and classic Bluetooth, with a BLE row that deliberately has no bar.

- **Title (bold 15px, `#1a5276`, top center):** "The Same 3 MB Song, Three Radios".
- **Axis:** seconds 0–220 mapped to px x=195 (0 s) through x=680 (220 s); 1px `#999` baseline at y=238 with vertical `#e5e9ef` gridlines and 12px `#444` tick labels at 0 / 60 / 120 / 180 s; axis caption "seconds" 12px `#444` at y=272 centered on the axis.
- **Rows (bars 32px tall, two-line left labels 12px `#444` at x=20):**
  - y=92: "IrDA" / "~115 kbit/s" — orange bar (`rgba(217,89,38,0.30)` fill, 2px `#d95926` border) to 209 s; bold 13px `#d95926` value "209 s ≈ 3.5 min" drawn inside the bar's right end (bar nearly fills the axis)
  - y=150: "classic Bluetooth" / "~2 Mbit/s" — blue bar (`rgba(42,120,214,0.30)` fill, 2px `#2a78d6` border) to 12 s; bold 13px `#2a78d6` value "12 s" right of the bar end
  - y=208: "BLE" / "(modern, low energy)" — no bar; bold 12px `#4a3aa7` note starting at x=195: "different goal: 20-byte messages on a coin cell, not 3 MB files"
- **Annotation (bold 13px orange `#d95926`, centered near x=430, y=135):** "3.5 min of stillness → 12 s in a pocket".
- **Caption (12px `#6b7280`, bottom right):** "speeds typical, illustrative; 3 MB = 24,000 kilobits".

## Pairing: Agreeing on a Secret Once

**Tags:** `pairing` (blue), `mechanism` (orange)

- **The problem** — dozens of radios are in range; the two phones must agree who talks to whom
- **Discovery** — Bob's phone announces itself; Alice's phone lists every nearby name
- **The code check** — both screens show the same 6-digit number, 837 291; both confirm it matches
- **The shared secret** — confirming stores a link key on both phones; that key is the pairing
- **Once, not every time** — later connections reuse the stored key and just happen, silently

*Example (italic):* Alice pairs her earbuds once at the kitchen table; every morning after, they connect before her coat is on.

**Key point:** Pairing is a one-time handshake that leaves a stored shared secret on both devices — recognition afterwards is automatic and silent.

### Visualization (canvas `c3`, 720×300)

Left-to-right flow of the pairing handshake, with a loop box showing that every later connection skips it.

- **Title (bold 15px, `#1a5276`, top center):** "Pairing: One Handshake, Then Silence".
- **Top row — four rounded boxes (150×56, 8px radius, bold 11px two-line `#2c3e50` labels, centered on y=108), left to right at x = 20 / 200 / 380 / 560:**
  - "discover:" / "Bob's phone in the list" — blue fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border
  - "compare:" / "both show 837 291" — orange fill `rgba(217,89,38,0.14)`, 2px `#d95926` border
  - "confirm:" / "both tap yes" — orange fill `rgba(217,89,38,0.14)`, 2px `#d95926` border
  - "link key stored" / "on both phones" — green fill `rgba(0,131,0,0.12)`, 2px `#008300` border
- **Arrows:** solid 2.5px `#6b7280` arrows with filled arrowheads between consecutive boxes.
- **Loop box:** wide rounded box (360×44) centered at x=180–540, y=196, green fill `rgba(0,131,0,0.10)`, 2px `#008300` border, bold 12px `#008300` label "every later connection: automatic — no code, no menus"; a 2px `#6b7280` elbow arrow drops from the bottom of box 4 (x=635) down to y=218 and left into the loop box's right edge, with an arrowhead.
- **Annotation (bold 12px ink `#1a5276`, centered, y=58):** "the matching code proves both screens are in the same handshake".
- **Caption (12px `#6b7280`, bottom center, y=282):** "code 837 291 illustrative — yours will differ".

## Bluetooth Today: the Beam's Job Moved to Wi-Fi

**Tags:** `where it's used` (blue), `telemetry` (green), `common confusion` (red)

- **The role changed** — Bluetooth today mostly carries control and telemetry, not files
- **Around you now** — earbuds, watch, heart-rate strap, and tracker tags trickle tiny messages
- **Files moved on** — modern phone-to-phone sharing pairs over Bluetooth, then sends over Wi-Fi
- **Where you meet it** — wearable data arrives as a BLE trickle: tiny records, device clocks, gaps
- **Gaps mean range** — a missing hour in the log often means out of range, not no activity

*Example (italic):* A fitness study's dataset is millions of 20-byte heart-rate readings — and its hardest cleaning problem is the gaps.

**Common confusion:** "Sent over Bluetooth" on a modern phone usually means Bluetooth made the introduction and Wi-Fi carried the bytes — the name stayed, the job changed.

### Visualization (canvas `c4`, 720×300)

Hub schematic: Alice's phone with thin telemetry links to wearables on the left and a two-layer sharing link to Bob's phone on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Bluetooth Today: a Control & Telemetry Channel (schematic)".
- **Center hub:** rounded box (104×78) at x=308, y=108, `rgba(42,120,214,0.12)` fill, 2px `#2a78d6` border, bold 12px `#2c3e50` two-line label "Alice's" / "phone".
- **Left devices:** three rounded boxes (122×38, 1.5px `#199e70` border, `rgba(25,158,112,0.10)` fill, bold 11px labels) at x=30, y = 70 / 128 / 186: "earbuds", "watch", "heart-rate strap". Dashed 1.5px `#199e70` lines from each box's right edge to the hub's left edge; one shared aqua label bold 12px `#199e70` at (170, 245): "BLE: ~20 B messages, all day".
- **Right device:** rounded box (104×78) at x=580, y=108, `rgba(74,58,167,0.10)` fill, 2px `#4a3aa7` border, bold 12px two-line label "Bob's" / "phone".
- **Two links to Bob:** a thin dashed 1.5px `#2a78d6` line at y=128 labeled 11px `#2a78d6` above it "Bluetooth: introduction + pairing"; a thick 6px `#4a3aa7` arrow at y=168 with a filled arrowhead, labeled bold 12px `#4a3aa7` below it "Wi-Fi: the 3 MB song".
- **Annotation (bold 13px orange `#d95926`, centered, y=272):** "'sent over Bluetooth' — Bluetooth negotiated, Wi-Fi carried the bytes".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" or "Common confusion:").
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` reserved for the genuine hazard (the aborted-transfer note in c1).
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). Documented facts: IrDA SIR rate 115.2 kbit/s (rounded to ~115 in text); classic Bluetooth 2.0+EDR nominal 3 Mbit/s (practical ~2 Mbit/s used, labeled typical); numeric-comparison pairing with a 6-digit code is in the Bluetooth spec. Invented/typical figures (~1 m IR range, ~10 m Bluetooth range, ~20-byte heart-rate reading, code 837 291) are labeled illustrative in the charts. Arithmetic that must stay consistent between text and charts: 3 MB × 8 = 24 Mbit = 24,000 kbit; 24,000 ÷ 115 ≈ 209 s ≈ 3.5 min; 24 ÷ 2 ≈ 12 s.
- This page has no links.
