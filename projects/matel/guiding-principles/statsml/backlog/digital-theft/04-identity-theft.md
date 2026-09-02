# Identity Theft

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Identity Theft

**Subtitle:** With enough personal facts — name, birth date, ID number, address history — a stranger becomes you on paper and opens new accounts in your name.

**Intro callout (blue-left-border box):** Nothing on your devices needs to be touched; the theft happens inside other people's databases and application forms.

## 1. Becoming you on paper

Institutions verify who you are by matching facts, not by seeing you.

- **Fact:** lenders check the facts on file, never a face.
- **Fact:** the keys: name, birth date, ID number, addresses.
- **Mechanism:** breaches spill exactly this bundle (data breach).
- **Mechanism:** a stranger holding the facts answers checks like you.
- **Fact:** a stolen card expires; stolen facts stay valid for years.
- **Scene:** Alice's records leak from a retail company's systems.

**Key point (red-left-border box):** **Risk:** whoever holds the fact bundle can pass as you on paper — the checklist has no face on it.

### Visualization (canvas `c1`, 720×300)

Gate schematic: the real person and a stranger with the same facts both pass one identity checklist.

- **Title (bold 16px `#5d4037`, top center):** "Two applicants, one checklist — the facts match either way".
- **Applicant boxes** 170×60 at y=50 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at y=74; sub-line 12px `#666` centered at y=94):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Alice (real person) | knows her own facts | #33691e | 50 |
  | Paper twin (stranger) | holds the same facts | #b71c1c | 500 |
- **Checklist box** 200×80 at x=260, y=130 in `#5d4037` (fill 0.12 alpha, stroke width 2): title "Identity checklist" bold 14px centered at (360, 150); check items 12px `#666` centered: "name matches" at (360, 168), "birth date matches" at (360, 183), "ID number matches" at (360, 198).
- **Arrows into the checklist** (width 2, filled heads): `#33691e` from (135, 112) to (280, 132); `#b71c1c` from (585, 112) to (440, 132).
- **Down arrow** `#5d4037` width 2 with filled head from (360, 212) to (360, 228).
- **Result pill:** rect 140×26 at x=290, y=230, `#0277bd` fill at 0.12 alpha, stroke `#0277bd` width 2; text "approved" bold 13px `#0277bd` centered at (360, 247).
- **Bottom line (bold 14px `#0277bd`, centered, y=268):** "Facts, not faces — whoever holds the facts passes the gate."
- **Caption (bottom center, 13px `#999`, y=286):** "The check compares answers against records; it never sees who is typing."

## 2. What the paper twin does

With the checks passed, the stranger's paperwork becomes your paperwork.

- **Risk:** new loans, cards, and phone plans open in your name.
- **Risk:** a tax refund is claimed before you file (refund fraud).
- **Mechanism:** statements route to an address the twin controls.
- **Risk:** missed payments land on your record (credit report).
- **Scene:** Bob first hears of the loan from a collector's call.
- **Fact:** discovery often takes months, sometimes longer.

**Key point:** **Risk:** you carry the debt trail until you prove the accounts were never yours.

### Visualization (canvas `c2`, 720×300)

Timeline: a new account opens at month 0, months pass quietly, and the victim finds out near month 8.

- **Title (bold 16px `#5d4037`, top center):** "Months of silence between the opening and the discovery".
- **Axis:** `#999` line width 1.5 from (60, 170) to (660, 170); small tick lines from y=165 to y=175 at x = 90, 290, 490, 630; tick labels 12px `#999` centered at y=190: "month 0" at x=90, "month 3" at x=290, "month 6" at x=490, "month 9" at x=630.
- **Event A (opening):** filled `#b71c1c` circle radius 5 at (90, 170); dashed `#b71c1c` vertical line width 1.5 from (90, 165) to (90, 110); label bold 13px `#b71c1c` centered at (150, 98): "new account opened"; sub-line 12px `#666` centered at (150, 114): "bills route to another address".
- **Quiet band:** rect x=120, y=150, width 380, height 40, `#0277bd` fill at 0.10 alpha; label bold 13px `#0277bd` centered at (310, 140): "quiet months — no signal reaches the real person".
- **Event B (discovery):** filled `#b71c1c` circle radius 5 at (560, 170); dashed `#b71c1c` vertical line width 1.5 from (560, 165) to (560, 110); label bold 13px `#b71c1c` centered at (545, 98): "a collector calls"; sub-line 12px `#666` centered at (545, 114): "first sign the victim sees".
- **Bottom line (bold 14px `#0277bd`, centered, y=255):** "The damage runs quietly until the first missed bill surfaces."
- **Caption (bottom center, 13px `#999`, y=285):** "Discovery usually arrives from outside — a call or a rejection, not an alert."

## 3. What limits the damage

The strongest defenses close the doors the stolen facts would otherwise open.

- **Defense:** a freeze stops new accounts opening (credit freeze).
- **Fact:** existing accounts keep working under a freeze.
- **Defense:** monitoring flags new lookups on your file (inquiry).
- **Defense:** an early fraud report shifts liability off the victim.
- **Defense:** leave ID numbers off forms that do not need them.
- **Win:** reported early, most losses fall on the lender.

**Key point:** **Win:** a freeze turns the stolen fact bundle into a key for a door that no longer opens.

### Visualization (canvas `c3`, 720×300)

Gate diagram: a freeze wall blocks new-account requests while activity on already-open accounts passes through an open lane.

- **Title (bold 16px `#5d4037`, top center):** "A credit freeze blocks the new, not the existing".
- **Freeze wall** in `#8d6e63` (fill 0.12 alpha, stroke width 2), drawn as two rects with a gap: top rect x=330, y=50, 60×80; bottom rect x=330, y=190, 60×50. Wall labels bold 13px `#8d6e63` centered: "credit" at (360, 85), "freeze" at (360, 100). Gap label 12px `#33691e` centered at (360, 145): "open lane".
- **Left boxes** 180×50 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at box y+21; sub-line 12px `#666` centered at box y+38):
  | Title | sub-line | color | x | y |
  |---|---|---|---|---|
  | New loan application | asks to open a fresh account | #b71c1c | 40 | 70 |
  | Your existing card | already-open account activity | #33691e | 40 | 150 |
- **Right box** 180×50 at x=500, y=150 in `#33691e`: title "Purchase goes through" centered at (590, 171); sub-line "nothing changes day to day" 12px `#666` centered at (590, 188).
- **Blocked arrow:** `#b71c1c` width-2 horizontal arrow with filled head from (230, 95) to (328, 95); label "blocked" bold 13px `#b71c1c` centered at (280, 82); dashed `#b71c1c` width-1.5 bounce arrow with filled head from (326, 102) to (255, 130); label "declined" 13px `#b71c1c` centered at (280, 145).
- **Pass-through arrow:** `#33691e` width-2 horizontal arrow with filled head from (230, 175) to (498, 175), crossing the wall gap.
- **Bottom line (bold 14px `#33691e`, centered, y=265):** "New doors stay shut; the ones already open keep working."
- **Caption (bottom center, 13px `#999`, y=285):** "The real person lifts the freeze briefly whenever new credit is actually wanted."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #8d6e63`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#5d4037` blue = mechanism/fact (Fact, Mechanism); `#33691e` green = defense/win (Defense, Win); `#b71c1c` red = risk/loss (Risk, Seen); `#0277bd` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#5d4037`; h2 1.3rem `#5d4037`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #8d6e63`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #b71c1c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each chart is a named `drawC1()`/`drawC2()`/`drawC3()` function; a `renderAll()` call runs them once and again on window resize (debounced 150ms) so canvases stay sharp after resize. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary blue `#5d4037`, green `#33691e`, red `#b71c1c`, orange `#0277bd`, plus `#8d6e63`, `#7b1fa2`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes ("most losses", not "all losses"). Each technical term (data breach, refund fraud, credit report, credit freeze, inquiry) appears at most once, in parentheses. Fictional people only (Alice, Bob); companies stay generic ("a retail company"). No realistic ID numbers or credentials anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
