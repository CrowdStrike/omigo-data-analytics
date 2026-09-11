# Optical Discs — CD, DVD, Blu-ray

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Optical Discs — CD, DVD, Blu-ray

**Subtitle:** A laser reads microscopic dents in spinning plastic — and one factory stamper turns a single master into millions of copies for cents apiece

## A Song as a Spiral of Microscopic Dents

**Tags:** `core idea` (blue), `how it works` (green)

- **One spiral** — all the data sits on a single track about 5 km long, wound from the center out
- **Pits and lands** — microscopic dents stamped in plastic under a mirror-thin aluminum layer
- **Laser readout** — a light beam bounces off the mirror; a pit edge reads as 1, a flat run as 0
- **Tiny scale** — one pit is about 0.5 µm wide, more than 100 times narrower than a human hair
- **No touching** — nothing contacts the surface, so a disc never wears out from being played

*Example (italic):* Alice's 700 MB music CD is one 5 km spiral of dents — her drive reads it with light alone, no needle.

**Key point:** The entire format is one trick: stamp microscopic bumps into cheap plastic, then read them back with a focused laser.

### Visualization (canvas `c1`, 720×300)

Two-part diagram: a side-view cross-section of a disc being read by a laser from below, then the track "unrolled" as a strip of dark pits and light lands with its bit readout.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "How a Laser Reads Dents as Bits".
- **Cross-section (x 80–640):** three stacked layers with 11px `#6b7280` labels to the right at x=650:
  - label/lacquer: y=50, height 12, fill `#e5e9ef`, label "label side"
  - aluminum mirror: y=62, height 10, fill `#b0bec5`, label "aluminum mirror"
  - clear plastic: y=72, height 55, fill `rgba(42,120,214,0.10)`, label "clear plastic, 1.2 mm"
- **Pits:** navy `#1a5276` rectangles 8px tall hanging below the aluminum line at y=72; x/width pairs: (150, 30), (260, 18), (300, 24), (430, 36), (520, 18).
- **Laser:** violet `#4a3aa7` 2.5px line from (300, 200) up to (312, 80) with a small arrowhead; a lighter reflected line `rgba(74,58,167,0.45)` from (312, 80) down to (324, 200). Bold 12px violet caption centered at (320, 216): "laser reads the reflection from below".
- **Unrolled track strip (y=237, height 20, x 80–640):** alternating segments, land fill `rgba(42,120,214,0.15)`, pit fill `#1a5276`; segment widths in px, starting with a land: [50, 35, 70, 25, 45, 60, 40, 35, 55, 45, 50, 50]. 11px `#6b7280` label "the track, unrolled" left-aligned at (80, 230).
- **Bit marks:** bold 12px magenta `#d55181` "1" centered above each internal segment boundary at y=228 — skip the label zone x<200 where "the track, unrolled" sits (draw 1s only for boundaries at x≥205).
- **Annotation (bold 13px magenta `#d55181`, centered at y=278):** "pit edge = 1, flat stretch = 0".
- **Caption (11px `#6b7280`, right-aligned at (640, 296)):** "schematic — not to scale".

## Backing Up Alice's 500 GB Laptop, Disc by Disc

**Tags:** `worked example` (green), `capacity ladder` (blue), `wavelength` (orange)

- **Capacity ladder** — CD 700 MB, DVD 4.7 GB, Blu-ray 25 GB — all on the same 12 cm plastic disc
- **The trick** — a shorter wavelength focuses to a smaller dot, so pits shrink and the spiral tightens
- **Wavelengths** — CD 780 nm infrared, DVD 650 nm red, Blu-ray 405 nm blue-violet (hence the name)
- **The division** — 500 ÷ 0.7 ≈ 715 CDs, 500 ÷ 4.7 ≈ 107 DVDs, 500 ÷ 25 = 20 Blu-rays
- **The stack** — 715 CDs is an 86 cm tower of plastic; 20 Blu-rays is a 2.4 cm pile
- **The hours** — at ~2.5 min per burned disc, the CD route is roughly 30 hours of swapping

*Example (italic):* Alice backs up her 500 GB laptop: a long weekend of CD swapping, or 20 Blu-rays before lunch.

**Key point:** Same disc, same size, ~35× the capacity — bought almost entirely by shrinking the laser's wavelength.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: discs needed to hold 500 GB, one bar per format, with capacity and laser wavelength under each bar.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "Discs Needed to Back Up 500 GB".
- **Annotation (bold 13px ink `#1a5276`, centered at y=44):** "same 12 cm disc — only the laser wavelength shrank".
- **Axes:** y axis from x=70, baseline y=235, plot height 165; y = disc count 0–750, gridlines `#e5e9ef` at 200/400/600 with 12px `#6b7280` labels; 12px `#444` rotated y-title "discs needed" at x=26.
- **Bars (width 110, centered at x = 190, 375, 560):** values `[715, 107, 20]`; fills blue `#2a78d6`, orange `#d95926`, violet `#4a3aa7`; bold 14px value labels in the bar color above each bar ("715", "107", "20").
- **X labels (two lines, centered under each bar):** bold 13px `#2c3e50` format name at y=255, then 12px `#6b7280` detail at y=273 — "CD" / "700 MB · 780 nm", "DVD" / "4.7 GB · 650 nm", "Blu-ray" / "25 GB · 405 nm".
- **Caption (11px `#6b7280`, right-aligned at (690, 296)):** "500 GB ÷ capacity, rounded up".

## Press Once, Stamp Millions

**Tags:** `why it matters` (blue), `economics` (orange)

- **Pressing** — a factory molds all the pits in one squeeze from a metal stamper, seconds per disc
- **One master** — cutting the glass master and stamper costs about $1,000 (illustrative), paid once
- **Cents each** — after the master, each pressed copy adds ~$0.05 in plastic and machine time
- **Break-even** — $1,000/N + $0.05 beats a ~$0.30 burned blank once N passes about 4,000 copies
- **Everywhere** — near-zero marginal cost is why trial discs flooded 1990s mailboxes and magazines
- **Data-science echo** — the same fixed-plus-marginal curve prices every build-once, serve-many system

*Example (italic):* At 1,000,000 copies the master adds a tenth of a cent per disc — the mailing envelope costs more.

**Key point:** Pressed discs made copying data essentially free per copy — the distribution economics that software and music ran on for two decades.

### Visualization (canvas `c3`, 720×300)

Log-log line chart: cost per copy versus number of copies; a pressing curve that falls with volume against a flat burning line, with the break-even point marked.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "Cost per Copy: Pressing vs Burning (illustrative)".
- **Axes:** origin x=70, baseline y=245, plot width 600, plot height 190. Both axes log10: x = copies from 100 (log 2) to 1,000,000 (log 6); y = $/copy from $0.02 to $20 (3 decades). Horizontal gridlines `#e5e9ef` at $0.10 / $1 / $10 with 12px `#6b7280` labels "$0.10", "$1", "$10"; x ticks 12px `#444` at "100", "1k", "10k", "100k", "1M". X-axis label 12px `#444` centered at y=280: "copies made".
- **Pressing curve (orange `#d95926`, 3px, with 4px dots):** points (N, $) = (100, 10.05), (300, 3.38), (1000, 1.05), (3000, 0.383), (10000, 0.15), (30000, 0.083), (100000, 0.06), (300000, 0.053), (1000000, 0.051) — i.e. $1,000/N + $0.05.
- **Burning line (green `#008300`, 3px, dashed 6/4):** flat at $0.30 across the plot; bold 12px green label "burned blanks: ~30¢ each, flat" above the line near the left.
- **Break-even:** vertical dashed `#6b7280` (4/4) line at N=4,000 from baseline to y=70; bold 12px ink label "break-even ≈ 4,000 copies" beside it.
- **Annotation (bold 13px orange `#d95926`, near the right end of the pressing curve):** "1M copies: ~5¢ each".
- **Legend (12px, top right):** orange square "pressed copy", green square "burned CD-R".
- **Caption (11px `#6b7280`, right-aligned at (690, 296)):** "all costs illustrative".

## Burning Is Not Pressing — and R Is Not RW

**Tags:** `common mistake` (red), `disc formats` (blue)

- **Pressed** — factory discs have real molded pits; a home drive cannot write one at all
- **CD-R** — the drive's laser darkens spots in a dye layer; write once, permanent, no undo
- **CD-RW** — a phase-change layer can be melted back to blank and rewritten about 1,000 times
- **Same to the reader** — all three play back as dark and light marks along one spiral
- **Not forever** — burned dye fades over the years ("disc rot"), so a burned backup is not an archive

*Example (italic):* Bob burns family photos to CD-R in 2004; twenty years later some discs read back with errors (illustrative).

**Key point:** "Burning" is a one-shot chemical write, not pressing — only RW discs can rewrite, and none of them is a permanent archive.

### Visualization (canvas `c4`, 720×300)

Three-lane schematic: the same stretch of track drawn for a pressed disc, a CD-R, and a CD-RW — identical mark pattern, different physics, different write budgets.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "Three Ways to Make a Dark Mark".
- **Lanes (track bar x 170–450, height 20, land fill `#eef2f7`, 1px `#c9d2dc` border), lane top y = 60, 135, 210.** Identical mark positions in every lane, x/width pairs: (185, 26), (230, 14), (275, 20), (330, 30), (395, 16).
- **Lane names (bold 13px ink `#1a5276`, left-aligned at x=24, vertically centered on each track):** "Pressed", "CD-R", "CD-RW"; beneath each name an 11px `#6b7280` sub-label: "molded pits", "burned dye", "phase-change".
- **Marks per lane:** Pressed = navy `#1a5276` rectangles; CD-R = orange `#d95926` rectangles; CD-RW = aqua `#199e70` rectangles.
- **Notes (12px `#2c3e50`, left-aligned at x=470, two lines per lane, bold count in the lane's mark color):** Pressed — "written at the factory" / "home writes: **0**"; CD-R — "laser cooks the dye dark" / "writes: **1**, permanent"; CD-RW — "melts back to blank" / "rewrites: **~1,000**".
- **Annotation (bold 13px ink `#1a5276`, centered at y=278):** "the drive can't tell — playback sees dark and light marks on one spiral".
- **Caption (11px `#6b7280`, right-aligned at (690, 296)):** "schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" in sections 1–3, "Common mistake:" wording kept as "Key point:" in section 4).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home/cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array, run once, and re-run on window resize (debounced 150ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Navy ink for titles and axes; red reserved for genuine error states (none on this page).
- **Data:** everything hardcoded, no randomness. Documented facts: capacities 700 MB / 4.7 GB / 25 GB, wavelengths 780 / 650 / 405 nm, disc thickness 1.2 mm, ~5 km spiral, ~0.5 µm pit width, disc diameter 12 cm, CD-RW rewrite order ~1,000. Derived: 500/0.7→715, 500/4.7→107, 500/25→20 (rounded up); 715 × 1.2 mm ≈ 86 cm; 20 × 1.2 mm = 2.4 cm; 715 × 2.5 min ≈ 30 h. Illustrative (labeled): $1,000 master, $0.05 pressed marginal, $0.30 blank, break-even 4,000, burn time 2.5 min, disc-rot anecdote. Pressing-curve points are exactly $1,000/N + $0.05.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
