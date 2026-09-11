# Undersea Cables & the Fiber Backbone

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Undersea Cables &amp; the Fiber Backbone

**Subtitle:** Nearly all data between continents rides a few hundred fiber cables on the seabed — and one strand moves terabits by splitting light into colors

## A Garden Hose of Glass on the Ocean Floor

**Tags:** `core idea` (blue), `submarine cables` (green), `repeaters` (orange)

- **The route** — Alice's file to Bob in London rides a cable lying on the Atlantic floor
- **Garden-hose thick** — the cable is ~25 mm across; the data rides hair-thin glass inside
- **Mostly armor** — steel wire, copper, and plastic wrap a core of just a few fiber pairs
- **A few hundred** — a few hundred submarine cables carry nearly all data between continents
- **Repeaters** — amplifiers every ~60-80 km, powered by electricity fed down the cable from shore
- **Faults** — mostly fishing gear and anchors; a cable ship grapples the cable up and splices it

*Example (italic):* Alice's one-gigabyte file to Bob spends most of its trip inside glass thinner than a human hair.

**Key point:** The intercontinental internet is physical: a few hundred armored hoses of glass lying on the seabed.

### Visualization (canvas `c1`, 720×300)

Split panel (dashed `#bdc3c7` divider at x=330, y=40 to y=285): cable cross-section on the left, seabed side view with repeaters and a fault on the right.

- **Title (bold 15px, `P.ink`, top center, y=24):** "Inside the Cable, Along the Seabed".
- **Left — cross-section, concentric circles centered (165, 150):**
  - r=72 "plastic sheath": fill `tint(P.yellow, 0.20)`, stroke 2px `P.yellow`
  - r=56 "steel armor": fill `rgba(107,114,128,0.25)`, stroke 2px `#6b7280`
  - r=38 "copper (power)": fill `tint(P.orange, 0.35)`, stroke 2px `P.orange`
  - r=22 "core": fill `tint(P.blue, 0.15)`, stroke 1.5px `P.blue`
  - fibers: 6 small dots radius 2.5 filled `P.aqua` clustered inside the core circle
  - Labels 11px: "plastic sheath" (`P.yellow`, above at y=62), "steel armor" (`#6b7280`, y=104 with short leader line), "copper (power)" (`P.orange`, y=138 left side), bold "glass fibers — hair-thin" (`P.aqua`, below at y=248)
  - Caption (11px `#6b7280`, centered x=165, y=268): "cross-section, ~25 mm across (schematic)"
- **Right — seabed profile (x=355…695):**
  - sea surface: light wavy `P.blue` 1.5px line near y=75; seabed: solid 2px `#6b7280` line near y=205
  - shore box at left (x=358…396, y=52…76) stroked `P.ink`, bold 11px ink label "shore station" above, 11px `#6b7280` "feeds power" below the box
  - cable: 2.5px `P.ink` line along the seabed from the shore down to (690, 205)
  - repeaters: 5 dots radius 5 filled `P.violet` evenly spaced on the cable; bold 12px `P.violet` label "repeater every ~60-80 km" at y=232
  - fault: red `#e74c3c` X (two 2.5px strokes) on the cable near x=560 with bold 11px red label "fault: fishing gear / anchor" above at y=170
  - ship: small `P.orange` hull trapezoid on the surface above the fault, 11px `P.orange` label "cable ship grapples + splices" at y=52
- **Annotation (bold 13px `P.green`, centered x=525, y=282):** "the data rides hair-thin glass — the rest is armor and power".

## One Strand, Eighty Colors: DWDM

**Tags:** `DWDM` (blue), `wavelengths` (green), `dark fiber` (orange)

- **One strand** — a single glass fiber can carry many wavelengths of light at the same time
- **Colors as channels** — each wavelength is its own data channel, hundreds of gigabits each
- **DWDM** — dense wavelength-division multiplexing packs ~80 colors onto one strand
- **The math** — 80 colors × 200 Gb/s = 16 Tb/s on one fiber pair (illustrative modern figures)
- **Dark fiber** — extra strands laid but left unlit; lease one and light it years later
- **Upgrades** — capacity grows by swapping the shore equipment, not by laying new glass

*Example (italic):* At 16 Tb/s, one fiber pair could move Alice's 1 GB file about 2,000 times every second.

**Key point:** Terabits per strand come from splitting light into colors — the glass stays; the shore lasers get smarter.

### Visualization (canvas `c2`, 720×300)

Mux/demux schematic: six colored channels merge onto one strand and fan back out; a grey unlit strand below.

- **Title (bold 15px, `P.ink`, top center, y=24):** "One Strand Carries Many Colors at Once (DWDM)".
- **Input lanes (left):** 6 horizontal 3px lines from x=60 to x=168, y = 70 / 95 / 120 / 145 / 170 / 195; colors in order `P.blue, P.green, P.magenta, P.yellow, P.aqua, P.violet`; 11px same-hue labels left of each line: "λ1", "λ2", "λ3", "λ4", "λ5", "… λ80".
- **Mux box:** x=168…200, y=58…207, fill `#f8f9fa`, stroke 2px `P.ink`, bold 11px ink vertical-ish label "mux" centered.
- **Lit strand:** from x=200 to x=520 at y=132 — drawn as consecutive 16px dashes cycling through the six lane colors (lineWidth 5) to show all colors sharing one fiber; bold 12px `P.ink` label "one lit strand — all colors at once" centered above at y=112.
- **Demux box:** x=520…552, y=58…207, same style, label "demux".
- **Output lanes (right):** mirror of inputs, x=552 to x=660, same colors and y positions; 11px `#444` label "200 Gb/s each" right of the top lane at y=74.
- **Dark fiber:** dashed (8/6) 4px `#b8bec7` line from x=200 to x=520 at y=235; 12px `#6b7280` label centered at y=256: "dark fiber — laid, but not lit (leased and lit later)".
- **Annotation (bold 13px `P.green`, centered x=360, y=282):** "80 colors × 200 Gb/s ≈ 16 Tb/s on one fiber pair (illustrative)".
- **Caption (11px `#6b7280`, right-aligned x=705, y=44):** "6 colors drawn; real systems pack ~80+".

## The 28-Millisecond Floor: New York to London by Hand

**Tags:** `worked example` (blue), `latency floor` (orange)

- **The question** — how fast can any message possibly get from New York to London and back?
- **Light in glass** — light slows to ~200,000 km/s in fiber, about 2/3 of its vacuum speed
- **The path** — call the cable route 5,600 km, close to the direct great-circle distance
- **One way** — 5,600 km ÷ 200,000 km/s = 0.028 s, a 28 ms one-way floor from physics alone
- **Round trip** — request plus reply doubles it: 2 × 28 = 56 ms before any equipment delay
- **Reality** — real pings run ~70 ms; the ocean is most of it, and no code can remove it

*Example (italic):* Divide 5,600 by 200,000 on paper: 0.028 seconds — Bob's reply cannot reach Alice in under 56 ms.

**Key point:** Cross-ocean latency has a physics floor — 56 ms round trip New York–London; software only adds to it.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: one-way floor, round-trip floor, and a typical measured ping split into physics vs equipment.

- **Title (bold 15px, `P.ink`, top center, y=24):** "New York → London: 5,600 km ÷ 200,000 km/s = 28 ms".
- **Axis:** x scale 0–80 ms mapped to x=210…670; vertical `P.grid` gridlines at 0/20/40/60/80 from y=68 to y=240 with 12px `#444` tick labels at y=258; 2px `#999` vertical baseline at x=210.
- **Rows (bars 34px tall), right-aligned bold 13px `P.text` row labels at x=200:**
  - y=88 "one-way floor": `P.blue` bar 0→28, 12px blue value "28 ms" right of the bar
  - y=143 "round-trip floor": `P.green` bar 0→56, bold 12px green value "56 ms" right of the bar
  - y=198 "typical measured ping": stacked — `P.blue` segment 0→56 with bold 11px white inside label "physics 56", `P.orange` segment 56→70 with 11px orange label "+14 gear" above the segment at y=192; 12px `P.text` total "~70 ms" right of the bar
- **Bar style:** fills `tint(hue, 0.8)` with 1.5px same-hue borders.
- **Annotation (bold 13px `P.orange`, centered x=440, y=56):** "no software can beat 56 ms — it's the speed of light in glass".
- **Caption (12px `#444`, bottom right):** "route 5,600 km and ping ~70 ms illustrative; the division itself is exact".

## The Ocean in Your Query Time — and No, It Isn't Satellites

**Tags:** `where it's used` (blue), `CDNs` (green), `common mistake` (red)

- **Replication lag** — copying data between continents pays the physics floor on every trip
- **Remote reads** — "the data lives in another region" means each query crosses an ocean twice
- **Chatty protocols** — 20 request-reply turns to London is 20 × 56 ms: over a second of physics
- **CDNs and edges** — caches put copies near users because the floor cannot be optimized away
- **The mistake** — "the cloud is wireless": cables carry roughly 99% of intercontinental traffic
- **Wet glass** — between continents, the internet is overwhelmingly cables on the seabed

*Example (italic):* Bob's dashboard called a database one ocean away 30 times per load — 1.7 seconds of pure physics.

**Key point:** When cross-region calls are slow, suspect geography before code — then batch, cache, or replicate.

### Visualization (canvas `c4`, 720×300)

Split panel (dashed `#bdc3c7` divider at x=430, y=40 to y=285): round-trip times by distance on the left, cables-vs-satellites share on the right.

- **Title (bold 15px, `P.ink`, top center, y=24):** "Distance Sets the Round Trip; Cables Carry the Traffic".
- **Left — vertical bars, typical round trips (illustrative):**
  - baseline 2px `#999` at y=230; y scale 0–220 ms mapped to y=230…75; `P.grid` gridlines with 11px `#444` labels at 0 / 100 / 200 on the left at x=68
  - bars width 60 centered at x = 120 / 210 / 300 / 390: "same region" 2 ms (`P.aqua`), "NY–London" 70 ms (`P.blue`), "NY–Tokyo" 150 ms (`P.violet`), "NY–Sydney" 200 ms (`P.orange`)
  - fills `tint(hue, 0.8)`, 1.5px same-hue borders; bold 12px same-hue value labels ("2", "70", "150", "200") above each bar; 11px `P.text` category labels below the baseline at y=248
  - bold 12px `P.green` annotation centered x=250, y=270: "every request-reply turn pays this — cache near the user"
- **Right — share of intercontinental traffic (illustrative):**
  - bold 12px `P.ink` panel heading centered x=575, y=62: "who carries traffic between continents"
  - two vertical bars width 70, same baseline/scale style, y scale 0–110% mapped to y=230…80: "submarine cables" 99% (`P.blue`), "satellites" 1% (`P.orange`); bold 13px same-hue value labels "~99%" and "~1%" above the bars; 11px `P.text` labels below at y=248
  - bold 12px `P.magenta` annotation centered x=575, y=270: "'the cloud' crosses oceans as wet glass, not wireless"

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row (`.tag.blue/.green/.red/.orange`), `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Charts:** each canvas 720×300 logical, `devicePixelRatio`-scaled via a shared `setup(id)` helper (backing store sized to displayed width × dpr, redraw on debounced window resize), CSS `width:100%`; all data hardcoded literal values, no `Math.random()`; a `tint(hex, a)` helper produces translucent fills from palette hexes.
- **Palette:** `const P = {blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef'}`; navy ink for headings and axes; red only for the genuine fault marker in c1.
