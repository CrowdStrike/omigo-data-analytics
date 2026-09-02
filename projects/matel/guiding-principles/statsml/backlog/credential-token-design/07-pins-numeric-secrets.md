# PINs & Numeric Secrets

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** PINs & Numeric Secrets

**Subtitle:** The deliberately tiny credential — a secret so small it is only safe inside a system that counts and limits guesses, which makes it the clearest case of security living outside the secret.

**Intro callout (blue-left-border box):** **Design [blue]:** A PIN is not a small password — unlike a password, which must survive offline guessing on its own, a PIN assumes it will never face offline guessing at all and delegates its entire security to rate-limiting, lockout, and hardware try-counters around it.

## 1. What a PIN is designed for

- **Design goal [blue]:** a tiny secret built for the numeric keypad.
- **Input surfaces [blue]:** cash machines, phone lines, door pads, lock screens.
- **Usability [green]:** enterable one-handed and in seconds.
- **Trade-off [blue]:** accept a minuscule secret, move security elsewhere.
- **Arithmetic [blue]:** ten thousand 4-digit values, roughly 13 bits.
- **Offline risk [red]:** any offline attacker exhausts the space instantly.
- **Survival condition [red]:** guesses must be counted and stopped.

**Key point (red-left-border box):** **Key question [red]:** never "how strong is the number" but "what happens on the wrong guess" — without an enforced try budget, a PIN is not a credential at all.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: raw guessing space in bits of entropy.

- **Title (bold 13px `#1a5276`, top center):** "Raw guessing space in bits of entropy (illustrative)".
- **Bars:** 28px tall, 18px gap, starting y=70; labels right-aligned 12px `#2c3e50` ending at x=190; track `#f0f0f0` 360px max (scaled so 60 bits = 360px); bar fill = row color at 0.6 alpha with 1px solid stroke in the row color; value text bold 11px `#2c3e50` after the bar.
- **Data (label, bits, color):**
  | Secret | bits | color |
  |---|---|---|
  | 4-digit PIN | 13 | #e74c3c |
  | 6-digit PIN | 20 | #e67e22 |
  | 8-digit PIN | 27 | #2980b9 |
  | Random 10-char password | 60 | #27ae60 |
- **Value labels:** "13 bits", "20 bits", "27 bits", "60 bits".
- **Caption (bottom center, 11px `#999`):** "Offline, a 4-digit space is exhausted instantly — the secret alone provides almost nothing."

## 2. Where the security actually lives

- **Hardware try-counter [green]:** card chips enforce the budget in silicon.
- **Permanent block [green]:** a few wrong entries and the chip blocks for good.
- **Reset path [blue]:** only an out-of-band reset unblocks the chip.
- **Tamper-resistant [green]:** attackers cannot reset the counter or read the PIN.
- **Server-side lockout [blue]:** the server counts failures for remote PINs.
- **Remote scope [blue]:** phone banking, centrally verified app PINs.
- **Response [blue]:** lock the account or force a step-up check.
- **Software weakness [red]:** only as good as discipline on every entry path.
- **Escalating delays [blue]:** each lock-screen failure waits longer.
- **Attacker cost [green]:** brute force stretches from minutes into years.
- **Owner cost [green]:** barely inconveniences two honest mistypes.

**Key point:** **Try budget [blue]:** the same four digits have completely different security per enforcement layer — the attacker's real budget is tries set by the layer, not combinations.

### Visualization (canvas `c2`, 720×300)

Fan-out diagram: one PIN box on the left feeding three enforcement-layer boxes on the right.

- **Title (bold 13px `#1a5276`, top center):** "Same secret, different enforcement layers".
- **Left box:** 140×50 centered on y=150 at x=30 (fill `#1a5276` at 0.12 alpha, stroke `#1a5276` width 2); label bold 13px `#1a5276` centered: "4-digit PIN"; below it 10px `#666` centered: "~13 bits".
- **Arrows:** `#bbb` width-1.5 lines from (170, 150) to (255, y) for each right box, with a small filled `#bbb` triangle head at the destination.
- **Right boxes:** 400×56 at x=260, centered on y=70/150/230 (fill = row color at 0.12 alpha, stroke = row color width 2; title bold 12px in row color at box top; description 11px `#2c3e50` below it):
  | Title | color | description |
  |---|---|---|
  | Hardware try-counter | #27ae60 | Card chip / secure element blocks itself after a few tries |
  | Server-side lockout | #2980b9 | Remote service locks or steps up after repeated failures |
  | Escalating delays | #e67e22 | Device makes each retry wait longer than the last |
- **Caption (bottom center, 11px `#999`):** "The attacker's budget is tries, not combinations — the enforcement layer sets the budget."

## 3. Human digit-choice skew

- **Heavy clustering [red]:** the chosen distribution is nothing like uniform.
- **Date patterns [red]:** birth years and other dates dominate picks.
- **Digit patterns [red]:** four identical digits, ascending or descending runs.
- **Keypad shapes [red]:** patterns traced on the keypad grid.
- **Entropy collapse [red]:** effective entropy sits far below the designed 13 bits.
- **Coverage skew [red]:** the top few percent covers a large user share.
- **Uniform assumption [blue]:** try-budget math silently assumes uniform choice.
- **Risk math [blue]:** three guesses in ten thousand reads as a 0.03% risk.
- **Skew multiplier [red]:** clustering boosts the odds on those same guesses.

**Key point:** **Distribution problem [red]:** a core theme of this repo — the model assumed a uniform distribution, the humans supplied a heavily skewed one, and every downstream risk number inherited the error; the entropy gap is a distribution problem, not a cryptography problem.

### Visualization (canvas `c3`, 720×320)

Line chart: cumulative share of users covered as PIN space is consumed most-popular-first, chosen vs uniform.

- **Title (bold 13px `#1a5276`, top center):** "Cumulative user coverage: chosen vs uniform PINs (illustrative)".
- **Axes:** origin x=90, baseline y=260, plot 560×200, stroke `#999` width 1.5; x label (11px `#666`, centered below): "share of PIN space, most-popular first →"; y label (rotated −90°, left of axis): "share of users covered →".
- **Uniform line:** `#999` width 1.5, dashed [5,4], straight from fraction (0, 0) to (1, 1); label 10px `#999` near (0.62, 0.55): "uniform choice".
- **Chosen-PIN curve:** `#e74c3c` width 2.5, solid polyline through fraction points [(0, 0), (0.01, 0.25), (0.05, 0.45), (0.10, 0.60), (0.20, 0.78), (0.50, 0.93), (1.0, 1.0)]; label bold 10px `#e74c3c` near (0.13, 0.72): "chosen PINs".
- Fractions map to pixels as x = 90 + fx·560, y = 260 − fy·200.
- **Callout (bold 11px `#e74c3c`, two lines, left-aligned at x=200, y=90/104):** "A tiny most-popular slice of the space" / "covers a large share of users".

## 4. Conventions, and current vs historical practice

- **Banking cards [orange]:** standardized on four digits almost everywhere.
- **Regional norm [orange]:** six digits in some regions and sectors.
- **SIM PINs [orange]:** default to four with longer values allowed.
- **Device unlock [orange]:** shifted from a 4-digit to a 6-digit default.
- **Digit-only inputs [blue]:** cash-machine keypads, door locks, phone lines.
- **Accessibility [blue]:** it too demands digit-only entry.
- **Persistence [orange]:** survives every prediction of its death.
- **Modern role [orange]:** device PINs now gate hardware-held keys.
- **Behind the PIN [blue]:** a secure element holds passkeys and payment credentials.
- **Strength inversion [blue]:** those credentials far outclass the PIN itself.
- **Hardware budget [green]:** the secure element enforces the try budget.

**Key point:** **Front door [orange]:** the humble PIN has quietly become the front door to the strongest credentials a person carries — acceptable only because the hardware behind it converts "13 skewed bits" into "a handful of tries against tamper-resistant silicon".

### Visualization (canvas `c4`, 720×300)

Dumbbell chart: historical vs current default PIN length by context.

- **Title (bold 13px `#1a5276`, top center):** "Default PIN length, historical vs current (illustrative)".
- **X axis:** line `#999` width 1.5 at y=250 from x=140 to x=680; ticks and labels (11px `#666`, centered at y=268) at digit values 3–8, mapping digit d → x = 140 + (d − 3) × 108; axis caption (11px `#666`, centered at x=410, y=288): "digits".
- **Rows** at y=70/115/160/205; row labels right-aligned 12px `#2c3e50` ending at x=125:
  | Context | historical | current | note (10px `#999`, left of x=560) |
  |---|---|---|---|
  | Banking card | 4 | 4 | 6 in some regions |
  | SIM PIN | 4 | 4 | longer allowed |
  | Device unlock | 4 | 6 | — |
  | Payment app PIN | 4 | 6 | — |
- **Marks:** historical value = 6px-radius dot `#999`; current value = 6px-radius dot `#1a5276`; when they differ, connect with a `#1a5276` width-2 line and draw a small filled `#1a5276` triangle head at the current dot; when equal, draw only the blue dot ringed by a `#999` width-1.5 circle of radius 9.
- **Legend (top right, from x=520 y=40):** gray dot + 10px `#666` "historical default"; blue dot + 10px `#666` "current default" 16px below.

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every text-column bullet is a labeled one-liner — `- **Label [color]:** phrase` in the md, where the phrase must fit on one line in the ~45% text column (roughly ≤55 characters). Never wrap a bullet; split long content into more labeled bullets instead. In HTML each bullet renders as `<li><span class="pt-label" style="color:COLOR">Label:</span> phrase</li>` (the `[color]` tag is dropped from visible text).
- **Label colors by meaning:** `#1a5276` blue = design/fact, `#27ae60` green = win/strength, `#e74c3c` red = flaw/risk, `#e67e22` orange = context/history/trend. The `[blue]`/`[green]`/`[red]`/`[orange]` tags in the md map to these hex values.
- **Intro / key-point boxes:** keep the callout boxes, but each opens with a bold colored lead word (same `pt-label` span and color scheme) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` font-weight 600 (color inline per label); ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×320, `c4` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width × `window.devicePixelRatio` and scales the context accordingly.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- **Content rule:** never print literal example PINs — name choice patterns in words only ("four identical digits", "birth years"), and keep every quantified chart title suffixed "(illustrative)".
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
