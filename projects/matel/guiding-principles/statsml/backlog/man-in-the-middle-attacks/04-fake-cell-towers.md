# Fake Cell Towers

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Fake Cell Towers

**Subtitle:** A phone's loyalty goes to the loudest tower — a portable transmitter that outshouts the real one collects every phone in range without touching any of them.

**Intro callout (blue-left-border box):** Phones were designed to trust the network: for decades the tower checked the phone, but the phone never checked the tower. A device that exploits that one-way trust is called a cell-site simulator.

## 1. The setup: loudest tower wins

Phones choose towers by one rule: loudest wins.

- **Setup:** phones constantly rank nearby towers by strength.
- **Setup:** they camp on whichever signal comes in loudest.
- **Fact:** older standards checked in one direction only.
- **Fact:** the tower verified the phone, never the reverse.
- **Scene:** a portable box close by outshouts a distant tower.
- **Mechanism:** nearby and loud beats real and far, every time.

**Key point (red-left-border box):** **Fact:** the phone follows its normal rules — attaching to the impostor is not a malfunction.

### Visualization (canvas `c1`, 720×300)

Signal-strength race: a real tower far left arrives faint at the phone, a portable box near right arrives strong, and the phone attaches to the box.

- **Title (bold 13px `#1a5276`, top center, y=20):** "The phone ranks towers by loudness — the closest box wins".
- **Real tower (left, `#2980b9`, width-2 strokes):** lattice mast — lines (70,190)→(90,95) and (110,190)→(90,95), rungs (76,160)→(104,160) and (82,130)→(98,130); label "Real tower" bold 13px `#2980b9` centered (90,214); sub "far away" 10px `#666` (90,230).
- **Phone (center, `#1a5276`):** strokeRect (332,100,56,100) width 2; screen fillRect (338,110,44,70) in `#1a5276` at 0.08 alpha; label "Phone" bold 13px `#1a5276` centered (360,222); sub "camps on the loudest" 10px `#666` (360,238).
- **Portable box (right, `#e74c3c`):** rect (575,115,90,60), fill at 0.12 alpha, stroke width 2; antenna line (620,115)→(620,97) width 2 with a filled 3px-radius circle at (620,95); label "Portable box" bold 13px `#e74c3c` centered (620,200); sub "close by" 10px `#666` (620,216).
- **Signal-bar groups** (baseline y=118; five bars each, width 8, gap 4, heights 8/14/20/26/32 left to right):
  - Left group, bar x = 232, 244, 256, 268, 280: first 2 bars filled `#2980b9`, last 3 filled `#eee` with `#ccc` width-1 stroke; caption "from real tower: faint" 10px `#666` centered (256,136).
  - Right group, bar x = 436, 448, 460, 472, 484: all 5 filled `#e74c3c`; caption "from portable box: strong" 10px `#666` centered (460,136).
- **Connections:** dashed `#999` width-1.5 line (dash [5,4]) from (104,150) to (330,158), label "ignored" 10px `#999` centered (215,172); solid `#e74c3c` width-2.5 line from (573,148) to (390,155), label "attaches" bold 11px `#e74c3c` centered (480,172).
- **Bottom line (bold 12px `#e67e22`, centered, y=262):** "Nothing is hacked — the phone simply prefers the loudest voice."
- **Caption (11px `#999`, centered, y=287):** "Bar heights illustrative — closeness, not trickery, decides which signal wins."

## 2. The trick: collect, and optionally listen

Once phones attach, the box quietly takes attendance.

- **Mechanism:** the box reads each phone's subscriber number (IMSI).
- **Fact:** collecting that identifier is a documented capability.
- **Mechanism:** traffic is relayed onward, so calls still work.
- **Risk:** researchers have demonstrated pushes to older modes.
- **Risk:** older standards carry weaker scrambling (encryption).
- **Fact:** who was near this spot, and when, is itself the data.
- **History:** law-enforcement use is documented in several countries.

**Key point:** **Risk:** no phone is touched or broken — presence alone becomes a record.

### Visualization (canvas `c2`, 720×300)

Funnel/flow: many phones in a crowd feed into the box, the box logs identifier plus time, and traffic is relayed onward to the real network so nothing appears broken.

- **Title (bold 13px `#1a5276`, top center, y=20):** "One box in a crowd: collect, then pass it along".
- **Phone cluster (left, `#1a5276`):** six 16×26 rects, stroke width 1.5, fill at 0.08 alpha, at (48,78), (96,62), (60,130), (112,114), (48,178), (100,170); cluster label "phones in a crowd" bold 12px `#1a5276` centered (95,232).
- **Feed lines:** `#bbb` width-1 lines from each phone's right-edge midpoint — (64,91), (112,75), (76,143), (128,127), (64,191), (116,183) — all to (288,145).
- **Portable box (center, `#e74c3c`):** rect (290,105,150,80), fill at 0.12 alpha, stroke width 2; title "Portable box" bold 13px `#e74c3c` centered (365,131); sub-lines 10px `#666` centered: "logs each phone: who + when" (365,150) and "then passes traffic along" (365,166).
- **Log card (below box):** rect (285,200,160,46), fill `#f8f9fa`, stroke `#ccc` width 1; two left-aligned 10px `#666` lines at x=297: "ID-1 · seen 10:02" (y=218) and "ID-2 · seen 10:03" (y=234).
- **Real network (right, `#27ae60`):** rect (555,105,140,80), fill at 0.12 alpha, stroke width 2; title "Real network" bold 13px `#27ae60` centered (625,131); sub "calls and texts still work" 10px `#666` centered (625,150).
- **Relay arrow:** `#bbb` width-1.5 horizontal arrow with filled head from (444,145) to (553,145); label "relayed onward" 11px `#666` centered (498,132).
- **Bottom line (bold 12px `#e74c3c`, centered, y=266):** "Nothing looks broken — the phones never notice the extra stop."
- **Caption (11px `#999`, centered, y=288):** "IDs and times illustrative — being near this spot, at this time, is the data."

## 3. What stops it: making the phone check back

The fix is symmetry: the phone must question the tower too.

- **Defense:** newer generations added mutual verification.
- **Defense:** the phone now authenticates the network too.
- **Fact:** protections strengthened over generations, not at once.
- **Defense:** the newest standard hides the identifier up front.
- **Win:** that removes the easiest catch — the first message.
- **Defense:** some phones can switch off the oldest standard.
- **Trend:** community and research projects hunt odd towers.
- **Mechanism:** a tower that appears briefly and vanishes is a flag.

**Key point:** **Win:** the one-way trust is closing — but only for phones that refuse the old standards.

### Visualization (canvas `c3`, 720×300)

Generation timeline: 2G / 3G / 4G / 5G as four labeled boxes, each with a coarse one-idea summary of what changed, red at the start and green at the end.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Four generations: the phone learns to check back".
- **Boxes** 150×90 at y=88 (fill = box color at 0.12 alpha, stroke = box color width 2; generation title bold 15px in box color centered at y=116; two sub-lines 10px `#2c3e50` centered at y=142 and y=158; era label 10px `#999` centered at y=196 below the box):
  | Gen | color | x | sub-line 1 | sub-line 2 | era |
  |---|---|---|---|---|---|
  | 2G | #e74c3c | 30 | tower checks phone only | phone never checks back | 1990s |
  | 3G | #e67e22 | 205 | mutual check added | phone verifies network | 2000s |
  | 4G | #2980b9 | 380 | stronger checks | some gaps remained | 2010s |
  | 5G | #27ae60 | 555 | identifier concealed | in the first message | 2020s |
- **Arrows:** `#bbb` width-1.5 horizontal arrows with filled heads at y=133, from (184) to (203), from (359) to (378), and from (534) to (553).
- **Bottom line (bold 12px `#e74c3c`, centered, y=240):** "Old standards stay on for coverage — a pushed-down phone loses the newer checks."
- **Second bottom line (bold 12px `#27ae60`, centered, y=258):** "A phone that refuses the oldest standard closes the easiest door."
- **Caption (11px `#999`, centered, y=287):** "Coarse summary — protections strengthened over generations; details vary by network."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1a5276` blue = mechanism/fact (labels: Setup, Fact, Mechanism); `#27ae60` green = defense/win (Defense, Win); `#e74c3c` red = risk/loss (Risk); `#e67e22` orange = scene/context/history (Scene, History, Trend). Key-point boxes open with the same colored bold lead word (Fact, Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. All chart data is literal and hardcoded — no randomness, no dates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- **Content rule:** layman physical language throughout; each technical term appears at most once, in parentheses (cell-site simulator, IMSI, encryption). Hedge all unsourced claims ("researchers have demonstrated", "documented", "community and research projects"); invented numbers are labeled illustrative; no named companies, agencies, products, or real incidents.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
