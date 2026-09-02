# Source Code Leakage

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Source Code Leakage

**Subtitle:** A leaked repository hands over far more than a program — keys saved in files, unreleased plans, internal addresses, and a map of weaknesses that can be studied at leisure.

**Intro callout (blue-left-border box):** The code itself is often the least valuable thing in the copy; the notes, keys, and history that travel with it are what open doors.

## 1. How code escapes

One copy leaving is enough, and there are several doors.

- **Scene:** a private repository is one settings flip from public.
- **Mechanism:** one leaked access token opens the whole store.
- **Mechanism:** personal copies on laptops outlive access controls.
- **Mechanism:** linked personal copies (forks) outlive the original.
- **Fact:** the history holds every version ever committed.
- **Risk:** a key deleted yesterday still sits in old history.

**Key point (red-left-border box):** **Risk:** the copy carries the full history — removing a file today does not remove it from yesterday.

### Visualization (canvas `c1`, 720×300)

Convergence schematic: three escape paths on the left, all pointing at one "full history out" box on the right.

- **Title (bold 16px `#1a237e`, top center):** "Three doors, one outcome — the entire history leaves".
- **Escape-path boxes** 190×50 at x=40 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at box y+20; sub-line 12px `#666` centered at box y+37):
  | Title | sub-line | color | y |
  |---|---|---|---|
  | Settings flip | private turned public | #e65100 | 42 |
  | Leaked token | one credential, all access | #ad1457 | 116 |
  | Personal copy | laptop or fork, outside controls | #00838f | 190 |
- **Outcome box** 200×70 at x=470, y=110, color `#ad1457`: title "Full history out" bold 14px centered at (570, 138); sub-line 12px `#666` "every version ever committed" centered at (570, 158).
- **Arrows:** width-2 arrows with filled heads, each in its source box color: from (232, 67) to (466, 130); from (232, 141) to (466, 145); from (232, 215) to (466, 160).
- **Bottom line (bold 14px `#e65100`, centered, y=265):** "Different doors, identical outcome — one copy is a complete copy."
- **Caption (bottom center, 13px `#999`, y=285):** "The history travels with the copy; nothing committed is ever only in the past."

## 2. What a reader mines from it

The copy can be read slowly, completely, and without anyone noticing.

- **Risk:** keys saved in files unlock the live systems they guard.
- **Seen:** internal addresses map the non-public inside.
- **Seen:** unreleased features reveal plans before launch.
- **Mechanism:** the copy can be studied offline with unlimited time.
- **Fact:** reading the copy never trips the live system's alarms.
- **Risk:** comments and to-do notes flag the known weak spots.
- **Fact:** the code shows exactly how checks and defenses work.

**Key point:** **Risk:** the study happens against the copy, not the live system — no alarm watches a reader of stolen code.

### Visualization (canvas `c2`, 720×300)

Fan-out diagram: one leaked-repository box on the left, four mined items on the right, each with a severity pill.

- **Title (bold 16px `#1a237e`, top center):** "One copy, four kinds of value — each with its own severity".
- **Source box** 160×70 at x=40, y=115, color `#1a237e`: title "Leaked repository" bold 14px centered at (120, 143); sub-line 12px `#666` "readable at leisure" centered at (120, 163).
- **Mined-item boxes** 230×40 at x=330 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at box y+17; sub-line 12px `#666` centered at box y+32):
  | Title | sub-line | color | y |
  |---|---|---|---|
  | Saved keys | unlock live systems | #ad1457 | 48 |
  | Internal addresses | map of the inside | #e65100 | 103 |
  | Unreleased features | plans before launch | #e65100 | 158 |
  | Known weak spots | notes and unfinished fixes | #ad1457 | 213 |
- **Severity pills** 100×26 at x=590, at each item's y+7 (fill = item color at 0.12 alpha, stroke = item color width 2; pill text bold 13px in item color centered): "severe", "high", "high", "severe".
- **Arrows:** width-1.5 arrows with filled heads in `#999`, from (202, 150) to (328, 68); (202, 150) to (328, 123); (202, 150) to (328, 178); (202, 150) to (328, 233).
- **Bottom line (bold 14px `#e65100`, centered, y=265):** "The program is the least of it — the notes about it travel too."
- **Caption (bottom center, 13px `#999`, y=285):** "Offline study raises no alarms — the live system never sees the reader."

## 3. What limits the damage

Every layer here assumes the copy will eventually leak.

- **Defense:** scanners block commits that carry secrets.
- **Defense:** an exposed key is treated as leaked and swapped.
- **Defense:** secrets live in a separate locked store (vault).
- **Defense:** safety must not depend on the code staying secret.
- **Defense:** fewer people with full access means fewer copies.
- **Fact:** swapping keys turns a stolen key into a stale one.

**Key point:** **Win:** when secrets live outside the code and keys rotate fast, a leaked repository is an embarrassment — not a master key.

### Visualization (canvas `c3`, 720×300)

Defense ledger: one row per defense, each with a pill saying whether it blocks the loss or shrinks it.

- **Title (bold 16px `#1a237e`, top center):** "Four defenses — two block the loss, two shrink it".
- **Rows** at y = 50, 100, 150, 200 (filled circle radius 4 in row color at (52, row y+10); title bold 14px in row color left-aligned at (66, row y+14); sub-line 13px `#666` left-aligned at (66, row y+31); pill = rect 140×26 at x=520, row y+2, fill = row color at 0.12 alpha, stroke = row color width 2, pill text bold 13px in row color centered at (590, row y+19)):
  | Title | sub-line | pill text | color |
  |---|---|---|---|
  | Commit scanners | secrets never enter the history | blocks | #00796b |
  | Rotate on exposure | stolen keys go stale fast | shrinks | #303f9f |
  | Vault for secrets | the code holds no keys at all | blocks | #00796b |
  | No security by secrecy | reading the code is not breaking in | shrinks | #303f9f |
- **Bottom line (bold 14px `#1a237e`, centered, y=265):** "Each layer assumes the code will leak — and makes the leak survivable."
- **Caption (bottom center, 13px `#999`, y=285):** "None of these prevent the copy; they make the copy worth less."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #303f9f`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1a237e` blue = mechanism/fact (Fact, Mechanism); `#00796b` green = defense/win (Defense, Win); `#ad1457` red = risk/loss (Risk, Seen); `#e65100` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a237e`; h2 1.3rem `#1a237e`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #303f9f`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #ad1457`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each chart is a named `drawC1()`/`drawC2()`/`drawC3()` function; a `renderAll()` call runs them once and again on window resize (debounced 150ms) so canvases stay sharp after resize. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary blue `#1a237e`, green `#00796b`, red `#ad1457`, orange `#e65100`, plus `#303f9f`, `#00838f`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes. No operational how-to for finding or exploiting flaws — the page says only that a copy enables offline study. Each technical term (forks, vault) appears at most once, in parentheses. Fictional naming only (Alice/Bob, "a retail company" style); no realistic credential strings anywhere — say "a key saved in a file" style prose instead.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
