# Broadband — DSL, Cable, Fiber

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Broadband — DSL, Cable, Fiber

**Subtitle:** The always-on line — the phone's copper pair, the TV's coax, and purpose-laid glass each carry data to the home, and upload lags download by design

## Three Wires Into Alice's House: Copper, Coax, Glass

**Tags:** `core idea` (blue), `physical media` (green), `always on` (orange)

- **The running example** — Alice's home office has run on all three: DSL, then cable, then fiber
- **Reused wires** — DSL rides phone copper, cable rides TV coax; only fiber was laid for data
- **Always on** — dial-up seized the voice band; DSL sits above it, so no dialing and no busy phone
- **Frequency split** — voice lives below 4 kHz; DSL puts data in bands from 25 kHz up to ~1.1 MHz
- **Distance matters** — high frequencies fade on long copper; DSL slows far from the exchange

*Example (italic):* Alice talks to Bob on the landline while a download runs — same wire, different frequencies, neither notices the other.

**Key point:** Broadband means moving data onto frequencies (or media) the legacy service doesn't use, so the line is always on.

### Visualization (canvas `c1`, 720×300)

Schematic band-plan bar of one copper pair: colored frequency segments for voice, guard, upstream, and downstream, with band edges labeled in kHz.

- **Title (bold 15px, `#1a5276`, top center):** "One Copper Pair, Split by Frequency (ADSL band plan)".
- **Annotation (bold 13px ink `#1a5276`, centered, y=60):** "same wire, different frequencies — the call and the download never touch".
- **Band bar:** one horizontal bar y=110 to y=185 built from four segments (schematic widths, not to scale):
  - voice, x 90–180: fill `rgba(0,131,0,0.18)`, 2px `#008300` border, inside label bold 13px `#008300` "voice (call)", range label above (12px `#444`, y=100) "0.3–3.4 kHz"
  - guard, x 180–235: fill `#e5e9ef`, no border, inside label 12px `#6b7280` "guard"
  - upstream, x 235–375: fill `rgba(217,89,38,0.25)`, 2px `#d95926` border, inside label bold 13px `#d95926` "upstream", range label above "25–138 kHz"
  - downstream, x 375–670: fill `rgba(42,120,214,0.22)`, 2px `#2a78d6` border, inside label bold 13px `#2a78d6` "downstream", range label above "138–1104 kHz"
- **Axis:** 1px `#999` baseline at y=185 from x=90 to x=670; boundary tick labels 12px `#444` at y=203: "0.3" (x=90), "3.4" (x=180), "25" (x=235), "138" (x=375), "1104" (x=670); axis caption 12px `#444` centered y=222: "kHz on the wire →".
- **Insight annotation (bold 13px orange `#d95926`, centered, y=250):** "downstream gets ~8× the spectrum of upstream — asymmetry is built into the band plan".
- **Caption (12px `#6b7280`, bottom center, y=285):** "schematic — band widths not to scale (ADSL, ITU G.992.1 edges)".

## Uploading 20 GB: Cable 500/20 vs Fiber 500/500

**Tags:** `worked example` (blue), `asymmetric speeds` (orange), `rule of thumb` (green)

- **The task** — Alice pushes a 20 GB dataset to a cloud bucket; 20 GB × 8 = 160,000 megabits
- **The formula** — transfer time = size in megabits ÷ line speed in Mbps; one division each way
- **Cable 500/20** — upload runs at 20 Mbps: 160,000 ÷ 20 = 8,000 s ≈ 2 h 13 min
- **Fiber 500/500** — upload runs at 500 Mbps: 160,000 ÷ 500 = 320 s ≈ 5.3 min
- **Same download** — both plans pull the file back in ~5.3 min; only the up direction differs

*Example (italic):* Both plans say "500 Mbps" on the bill; the number after the slash is the one Alice's upload feels.

**Key point:** Divide by the upload figure, not the headline — a 25× slower uplink means a 25× longer push.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: time to move 20 GB in each direction on each plan, shared minutes axis.

- **Title (bold 15px, `#1a5276`, top center):** "Moving 20 GB: the Slash Hides a 25× Gap".
- **Axis:** minutes 0–140 mapped to px x=250 (0) through x=680 (140); 1px `#999` baseline at y=255; `#e5e9ef` vertical gridlines with 12px `#444` tick labels at 0 / 30 / 60 / 90 / 120 min; axis caption "minutes" 12px `#444` centered at y=289.
- **Rows (bars 30px tall centered on row y, left-aligned 12px `#444` two-line labels at x=20):**
  - y=80: "cable 500/20 — download", blue fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, bar 5.3 min, bold 13px `#2a78d6` value "5.3 min" right of bar end
  - y=130: "cable 500/20 — upload", orange fill `rgba(217,89,38,0.30)`, 2px `#d95926` border, bar 133.3 min, bold 13px `#d95926` value "133 min ≈ 2 h 13 min" drawn inside the bar's right end
  - y=180: "fiber 500/500 — download", blue fill/border as above, bar 5.3 min, value "5.3 min"
  - y=230: "fiber 500/500 — upload", green fill `rgba(0,131,0,0.25)`, 2px `#008300` border, bar 5.3 min, bold 13px `#008300` value "5.3 min"
- **Annotation (bold 13px orange `#d95926`, centered near x=465, y=157 — in the gap between the cable-upload and fiber-download rows):** "same headline '500' — the upload direction differs 25×".
- **Caption (12px `#6b7280`, bottom right):** "idealized: line rate only, no protocol overhead".

## The Home Office Pushes: Upload Is the Working Speed

**Tags:** `where it's used` (blue), `home office` (green), `bottleneck` (orange)

- **Built for pull** — plans assume homes download: streaming and browsing barely upload at all
- **The reversal** — a home data office pushes: datasets, model checkpoints, backups, video calls
- **Checkpoint math** — a 5 GB checkpoint is 40,000 Mb; at 20 Mbps up that is 2,000 s ≈ 33 min
- **Calls compete** — a video call needs a steady ~4 Mbps up; a saturated uplink makes it stutter
- **Rule of thumb** — read the second number on the plan; that is the working speed of the office

*Example (italic):* Alice's nightly 5 GB checkpoint sync holds the 20 Mbps uplink for ~33 minutes — and her evening call stutters the whole time.

**Key point:** For anyone who ships data and models from home, the upload figure is the real speed of the office.

### Visualization (canvas `c3`, 720×300)

Diverging horizontal bar chart: one illustrative day of Alice's home-office traffic, download GB to the left (blue) and upload GB to the right (orange), per task.

- **Title (bold 15px, `#1a5276`, top center):** "One Day of Alice's Home-Office Traffic (illustrative)".
- **Center axis:** 1px `#999` vertical line at x=430 from y=80 to y=245; scale 20 px per GB both directions.
- **Column headers (bold 12px, y=72):** "download (GB)" in `#2a78d6` right-aligned at x=420; "upload (GB)" in `#d95926` left-aligned at x=440.
- **Rows (bars 24px tall centered on row y; task labels 12px `#444` left-aligned at x=20; hardcoded GB values; 12px value labels at bar ends in the bar's border color):**
  - y=92: "video call (all day)" — down 1.5, up 1.5
  - y=127: "dataset pull from cloud" — down 8, up 0.1
  - y=162: "checkpoint sync (nightly)" — down 0.1, up 5
  - y=197: "cloud backup" — down 0, up 3
  - y=232: "evening streaming" — down 6, up 0.1
  - Down bars: fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, drawn leftward from x=430. Up bars: fill `rgba(217,89,38,0.30)`, 2px `#d95926` border, drawn rightward from x=430. Zero/0.1 values draw a sliver with the value label only.
- **Annotation (bold 13px orange `#d95926`, left-aligned at x=545, two lines at y=160/178):** "the work traffic points" / "into the 20 Mbps lane".
- **Caption (12px `#6b7280`, bottom center, y=285):** "illustrative day — sizes rounded to make the shape visible".

## The 8 pm Mystery: Where Did My 500 Go?

**Tags:** `common mistake` (red), `shared pipe` (orange), `measurement` (green)

- **Shared pipe** — one coax segment serves the whole street; homes split the node's capacity
- **Rush hour** — evening streaming fills the shared segment, and every home sags together
- **DSL contrast** — each copper pair runs alone to the exchange; slower, but steady all day
- **Fiber contrast** — shared too, behind a splitter, but the pipe is so big the dip barely shows
- **Speed tests** — a midnight test sees the empty pipe; only an 8 pm test sees the evening speed

*Example (italic):* Alice's midnight speed test reads 480 Mbps; the same test at 8 pm reads 280 — the plan didn't change, the neighborhood did.

**Key point:** "Up to 500 Mbps" prices the empty pipe — measure at the hour you actually work.

### Visualization (canvas `c4`, 720×300)

Line chart of measured download speed by hour of day for the three lines at the same house, with the evening window shaded.

- **Title (bold 15px, `#1a5276`, top center):** "Same House, Hour by Hour (illustrative speed tests)".
- **Axes:** x = hours 0–22 mapped to px 70–560; y = 0–520 Mbps mapped to baseline y=245 up to y=60; 1px `#999` axis lines; `#e5e9ef` horizontal gridlines with 12px `#444` labels at 0 / 100 / 200 / 300 / 400 / 500; x ticks 12px `#444` at hours 0 / 4 / 8 / 12 / 16 / 20 with caption "hour of day" centered at y=289.
- **Evening shade:** `rgba(217,89,38,0.08)` rect from x(18) to x(22), y=60 to y=245.
- **Hours sampled (12 points):** 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22.
- **Cable line (orange `#d95926`, 3px, 3.5px dots):** [480, 485, 480, 470, 460, 450, 430, 400, 360, 300, 280, 380].
- **Fiber line (green `#008300`, 3px):** [495, 495, 494, 495, 493, 494, 492, 490, 488, 487, 486, 490].
- **DSL line (violet `#4a3aa7`, 3px):** [80, 80, 80, 79, 80, 80, 79, 78, 78, 77, 77, 79].
- **Annotation (bold 13px orange `#d95926`, right-aligned two lines ending at x=545, y=110/128):** "8 pm: the street is streaming —" / "cable drops to 280".
- **Legend (x=585, swatch rows at y=70/90/110, 12px `#2c3e50` labels):** green "fiber 500/500", orange "cable 500/20", violet "DSL 80/20".
- **Caption (12px `#6b7280`, bottom right):** "illustrative — one house, one week averaged".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" or "Common mistake:"; the section-4 callout uses "Common mistake:").
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. No red in any chart — no genuine error state on this page.
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). Documented facts: ADSL band edges 25 / 138 / 1104 kHz (ITU G.992.1) and voice band 0.3–3.4 kHz. Invented/typical figures (plan speeds as "typical", the traffic-day GB values, the hour-by-hour speed tests) are labeled illustrative in each chart's caption. Arithmetic that must stay consistent between text and charts: 20 GB × 8 = 160,000 Mb; 160,000 ÷ 20 = 8,000 s ≈ 133 min ≈ 2 h 13 min; 160,000 ÷ 500 = 320 s ≈ 5.3 min; 5 GB = 40,000 Mb ÷ 20 = 2,000 s ≈ 33 min; (1104 − 138) ÷ (138 − 25) ≈ 8.5, stated as "~8×" of usable spectrum — keep the "~"; cable curve midnight 480 vs 8 pm 280 matches the section-4 example line.
- This page has no links.
