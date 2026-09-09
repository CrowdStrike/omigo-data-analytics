# IP & Trade Secret Theft

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** IP & Trade Secret Theft

**Subtitle:** Years of research can leave as a single file — what gets stolen is the head start, not the hardware, and competing products arrive sooner than they should have.

**Intro callout (green-left-border box):** The stolen thing weighs nothing and is still on the owner's disk afterward — what leaves is the years a rival no longer has to spend.

## 1. The head start is the asset

A design file is small; the years behind it are not.

- **Fact:** designs, formulas, and processes fit in one file.
- **Fact:** a trained model compresses years of experiments too.
- **Mechanism:** the cost was the learning; the file is the receipt.
- **Mechanism:** a secret is worth the rediscovery time it saves.
- **Fact:** unlike a patent, a secret protects only while hidden.
- **Scene:** Alice's ten-year formula fits on one small drive.

**Key point (red-left-border box):** **Risk:** copying the file costs minutes; what it transfers is the decade of trial and error behind it.

### Visualization (canvas `c1`, 720×300)

Timeline bars: the originator's long, segmented R&D bar versus a rival's short bar that begins near the finish line, fed by a dashed "the file" arrow.

- **Title (bold 16px `#1b5e20`, top center):** "Nine years vs one file — the rival starts near the finish line".
- **Axis:** horizontal `#999` width-1 line from (140, 240) to (640, 240); tick marks (4px tall, `#999`) at x = 140, 290, 440, 590 with 12px `#999` centered labels at y=256: "year 0", "year 3", "year 6", "year 9".
- **Row labels** (bold 14px, left-aligned at x=40): "Originator" in `#1b5e20` at y=100; "Rival with the file" in `#b71c1c` at y=180.
- **Originator bar segments** (height 28 at y=82; fill = segment color at 0.12 alpha, stroke = segment color width 2; label bold 13px in segment color centered at y=100):
  | Label | color | x | width |
  |---|---|---|---|
  | research | #388e3c | 140 | 150 |
  | development | #1b5e20 | 290 | 150 |
  | refinement | #4527a0 | 440 | 150 |
- **Originator ship marker:** vertical `#00838f` width-2 line from (590, 70) to (590, 120); label "ships" bold 13px `#00838f` centered at (590, 64).
- **File transfer:** dashed (`[5,4]`) `#b71c1c` width-1.5 arrow with filled head from (565, 114) to (512, 158); label "the file" bold 13px `#b71c1c` left-aligned at (575, 142).
- **Rival bar:** single segment 110 wide, height 28 at x=490, y=162, color `#b71c1c` (same fill/stroke scheme); label "finish only" bold 13px `#b71c1c` centered at (545, 180).
- **Rival ship marker:** vertical `#b71c1c` width-2 line from (600, 150) to (600, 200); label "ships too" bold 13px `#b71c1c` centered at (600, 214).
- **Bottom line (bold 14px `#e65100`, centered, y=274):** "The short bar is not speed — the years were copied, not spent."
- **Caption (bottom center, 13px `#999`, y=290):** "Both products reach the market in the same window; only one paid for the road."

## 2. How it leaves and what follows

The copy is silent; the consequence is loud and arrives in the market.

- **Risk:** a departing researcher leaves with a personal copy.
- **Risk:** an intruder in the network copies the design vault.
- **Risk:** a supplier holding the drawings leaks them onward.
- **Seen:** a rival product appears missing the R&D years.
- **Mechanism:** with no R&D cost, the copy undercuts on price.
- **Fact:** once public, the secret's value drops for everyone.
- **Seen:** detection is slow — the rival's launch is the clue.

**Key point:** **Risk:** the theft is often discovered from the outside — a competing product that arrived years too early.

### Visualization (canvas `c2`, 720×300)

Flow diagram: three leak paths on the left converge on one design-file box in the center, then a single arrow leads to a rival product shipping sooner.

- **Title (bold 16px `#1b5e20`, top center):** "Three doors, one file — and a launch that comes too soon".
- **Source boxes** 180×46 at x=40 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at box y+19; sub-line 12px `#666` centered at box y+36):
  | Title | sub-line | color | y |
  |---|---|---|---|
  | Departing employee | personal copy on the way out | #e65100 | 52 |
  | Network intruder | copies the design vault | #b71c1c | 117 |
  | Supplier leak | shared drawings travel on | #4527a0 | 182 |
- **File box** 150×60 at x=300, y=110, color `#1b5e20`: title "Design file" bold 14px centered at (375, 133); sub-line 12px `#666` "years of work, one copy" at (375, 152).
- **Converging arrows** (width 1.5, filled triangular heads, each in its source box color): from (222, 75) to (298, 125); from (222, 140) to (298, 140); from (222, 205) to (298, 155).
- **Outcome arrow:** solid `#b71c1c` width-2 arrow from (452, 140) to (518, 140).
- **Outcome box** 160×60 at x=520, y=110, color `#b71c1c`: title "Rival product" bold 14px centered at (600, 133); sub-line 12px `#666` "ships years sooner" at (600, 152).
- **Bottom line (bold 14px `#e65100`, centered, y=265):** "Every path ends the same way: a launch missing its R&D years."
- **Caption (bottom center, 13px `#999`, y=285):** "The copy makes no noise; the competing product is usually the first signal."

## 3. What limits the damage

No single lock works; each defense removes a path or adds a trace.

- **Defense:** split the recipe across teams (need-to-know access).
- **Fact:** no one person can walk out with the whole design.
- **Defense:** unique marks per copy trace the leak (canary copies).
- **Defense:** exit interviews and device returns close that path.
- **Defense:** legal action can recover damages after the fact.
- **Risk:** courts award money, not the years of head start.
- **Scene:** Alice's team each holds one step of the formula.

**Key point:** **Win:** splitting, marking, and closing exits make the file harder to take and easier to trace — but a head start, once out, does not come back.

### Visualization (canvas `c3`, 720×300)

Defense table: one row per defense with a name, a short description, and a colored pill stating what the defense actually buys.

- **Title (bold 16px `#1b5e20`, top center):** "Four defenses — what each one actually buys".
- **Rows** at y = 58, 103, 148, 193 (defense name bold 14px in row color, left-aligned at x=40, baseline row y+14; description 12px `#666` left-aligned at x=40, baseline row y+30; status pill = rect at x=500, width 160, height 26 at row y+2, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (580, row y+19)):
  | Defense | description | pill text | color |
  |---|---|---|---|
  | Need-to-know access | recipe split across teams | slows the copy | #388e3c |
  | Canary copies | unique marks in each copy | traces the leak | #4527a0 |
  | Exit process | devices and access returned | closes a path | #00838f |
  | Legal action | damages claimed afterward | recovers money | #e65100 |
- **Bottom line (bold 14px `#1b5e20`, centered, y=255):** "Each row removes a path or adds a trace — none restores lost years."
- **Caption (bottom center, 13px `#999`, y=285):** "The head start, once copied, is spent; defenses protect the next one."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #388e3c`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also green-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1b5e20` green = mechanism/fact (Fact, Mechanism); `#00838f` teal = defense/win (Defense, Win); `#b71c1c` red = risk/loss (Risk, Seen); `#e65100` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1b5e20`; h2 1.3rem `#1b5e20`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #388e3c`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #b71c1c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each canvas is drawn by a named function (`drawC1`, `drawC2`, `drawC3`) and a `renderAll()` runs once at load plus on a debounced (150ms) window resize so canvases stay sharp. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary green `#1b5e20`, teal `#00838f`, red `#b71c1c`, orange `#e65100`, plus `#388e3c`, `#4527a0`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes. Each technical term (need-to-know access, canary copies) appears at most once, in parentheses. Fictional naming only (Alice; unnamed companies). No realistic credentials or secret contents anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
