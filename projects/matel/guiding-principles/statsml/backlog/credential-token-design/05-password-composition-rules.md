# Password Composition Rules

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Password Composition Rules

**Subtitle:** How password rules evolved from storage-era length caps through mandatory-complexity checklists to the modern length-first reversal — and why human behavior, not combinatorics, drove every turn.

**Intro callout (blue-left-border box):** **The bet:** (blue) forced character classes strengthen human-chosen passwords. **The verdict:** (red) leaked corpora settled it the other way — length, breach screening, and no scheduled expiry won.

Bullet style: every bullet is one non-wrapping line — a bold colored label + a short phrase. Label colors by meaning: `#1a5276` blue = design/fact, `#27ae60` green = win/strength, `#e74c3c` red = flaw/risk, `#e67e22` orange = context/history/trend.

## 1. The historical arc

- **Era 1 — caps:** (orange) early systems capped passwords near eight characters.
- **Cause:** (blue) legacy hashing schemes consumed a fixed-size input.
- **Effect:** (blue) short passwords were a constraint, not a choice.
- **Era 2 — complexity:** (orange) uppercase, digit, and symbol became mandatory.
- **Rotation:** (orange) forced password changes roughly every 90 days.
- **Spread:** (orange) audit-friendly rules traveled via compliance checklists.
- **Speed:** (red) checklists spread faster than the evidence.
- **Era 3 — reversal:** (orange) modern national guidelines flipped to length-first.
- **Dropped:** (green) forced character classes and scheduled expiry.
- **Added:** (green) screening candidates against known-breached lists.
- **Rational then:** (blue) each era's policy fit its own constraints.

**Key point (red-left-border box):** **Inertia:** (red lead word) the complexity checklist outlived its evidence by two decades.

### Visualization (canvas `c1`, 720×300)

Three era boxes above a left-to-right timeline arrow.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Three eras of composition policy".
- **Boxes** 210×140 at y=45 (fill = era color at 0.12 alpha, stroke = era color width 2; title bold 13px in era color at y=68; items 11px `#2c3e50`, centered, 20px apart starting y=95):
  | Title | color | x | items |
  |---|---|---|---|
  | STORAGE-ERA CAPS | #e74c3c | 20 | Hard caps near 8 chars; Legacy hashes truncated input; Short by constraint |
  | MANDATORY COMPLEXITY | #e67e22 | 255 | Upper + digit + symbol; Forced 90-day rotation; Audit-friendly checklists |
  | LENGTH-FIRST REVERSAL | #27ae60 | 490 | Long passphrases favored; No forced classes or expiry; Breached-list screening |
- **Timeline:** `#999` width-2 line from x=30 to x=690 at y=235 with a filled arrowhead at the right end; a `#999` width-1 tick from each box's bottom-center (y=185) down to y=235.
- **Era labels** (11px `#666`, centered under each box at y=255): "mainframe era", "compliance era", "modern guidance".

## 2. Why the reversal happened

- **Flaw — substitutions:** (red) rules were satisfied by predictable transforms.
- **Pattern:** (blue) capital first letter, trailing digit, one common swap.
- **Result:** (red) realized entropy gain far below the theoretical.
- **Flaw — expiry:** (red) rotation produced trailing-counter increments.
- **Result:** (red) one expired password usually derives the current one.
- **Flaw — displacement:** (red) stricter rules pushed one password reused everywhere.
- **Also:** (red) users wrote their compliant passwords down.
- **Trade:** (red) theoretical on-site gain for concrete cross-site risk.
- **Net effect:** (red) rules moved predictability, never reduced it.

**Key point:** **Theme:** (blue lead word) human behavior beat the policy — measure the realized distribution, not the theoretical one.

### Visualization (canvas `c2`, 720×300)

Grouped vertical bar chart: theoretical vs realized entropy gain per rule.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Theoretical vs realized entropy gain per rule (illustrative)".
- **Axes:** origin x=70, baseline y=240, y-axis up to y=50; stroke `#999` width 1.5; y ticks at 0/5/10 bits (10px `#999` labels left of axis, light `#eee` gridlines across plot width 600).
- **Bars:** paired per group — theoretical `#1a5276`, realized `#e74c3c`; each bar 40px wide, 8px gap within the pair; fill at 0.6 alpha with 1px solid stroke of the same color; value labels bold 10px `#2c3e50` above each bar; scale = 190px plot height for 14 bits max.
- **Group centers** at x = 145, 295, 445, 595; group labels 11px `#2c3e50` centered at y=258.
- **Data (group, theoretical bits, realized bits):**
  | Rule | theoretical | realized |
  |---|---|---|
  | Require uppercase | 4.7 | 1.0 |
  | Require digit | 3.3 | 0.9 |
  | Require symbol | 5.0 | 1.2 |
  | All three | 13.0 | 2.5 |
- **Legend** (top-right, from x=520 at y=40, 12×12 swatches, 11px `#2c3e50` labels): `#1a5276` "theoretical bits", `#e74c3c` "realized bits (human-chosen)".
- **Caption (bottom center, 11px `#999`, y=290):** "Capital first letter, trailing digit, one common swap — attackers model the transforms."

## 3. Country and sector differences

- **Banking stacks:** (blue) short numeric PINs beside or instead of passwords.
- **Backstop:** (green) strict lockout and card possession carry the load.
- **Floors:** (orange) minimum lengths vary by jurisdiction and sector.
- **Consumer:** (orange) consumer services settle near the low end.
- **Regulated:** (orange) government and regulated sectors mandate longer floors.
- **Numeric-only:** (orange) feature-phone-era entry methods fixed a digit alphabet.
- **Persistence:** (red) numeric habits outlived the arrival of full keyboards.
- **Corporate lag:** (red) enterprise rules stayed strict after guidance reversed.
- **Cause:** (orange) compliance audits update slower than the research.
- **Nature:** (blue) differences are qualitative and regime-driven.

**Key point:** **Fragmentation:** (red lead word) one user faces a different rulebook per sector, which fuels reuse and workarounds.

### Visualization (canvas `c3`, 720×300)

Horizontal range bars: typical minimum-length floors by sector.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Typical minimum-length floors by sector (illustrative)".
- **Axis:** horizontal `#999` width-1.5 line at y=250 from x=230 to x=690; scale 0–16 characters (28.75px per character); ticks with 10px `#999` labels at 0, 4, 8, 12, 16; x label 11px `#666` centered at y=282: "minimum length floor (characters)".
- **Rows:** labels right-aligned 12px `#2c3e50` ending at x=215; range bar 18px tall from `lo` to `hi` on the axis scale (fill = row color at 0.6 alpha, 1px solid stroke); range text bold 10px `#2c3e50` after the bar ("lo–hi").
  | Sector | lo | hi | color | row center y |
  |---|---|---|---|---|
  | Feature-phone-era services (numeric-only) | 4 | 6 | #e74c3c | 60 |
  | Consumer web services | 6 | 8 | #e67e22 | 100 |
  | Banking logins (paired with numeric PIN) | 6 | 8 | #8e44ad | 140 |
  | Corporate / enterprise policies | 8 | 12 | #2980b9 | 180 |
  | Government / regulated sectors | 12 | 16 | #27ae60 | 220 |

## 4. Length vs composition math

- **Math:** (blue) entropy grows linearly with length.
- **But:** (blue) only logarithmically with alphabet size.
- **Per character:** (blue) each one multiplies the space by the whole alphabet.
- **Per class:** (blue) a new character class only nudges the multiplier.
- **Win:** (green) sixteen lowercase characters carry roughly 75 bits.
- **Compare:** (orange) eight full-keyboard characters carry roughly 52 bits.
- **Caveat:** (red) the uniform model still flatters complexity rules.
- **Reality:** (red) human "complex" strings cluster in a modeled subspace.
- **Wrong question:** (red) raw combinatorics is not guessability.
- **What matters:** (blue) only realized-distribution entropy counts.

**Key point:** **Guessability:** (red lead word) attackers search the human-chosen distribution in probability order.

### Visualization (canvas `c4`, 720×320)

Line chart: entropy in bits vs password length for three models.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Entropy growth: length beats alphabet (illustrative)".
- **Axes:** origin x=70, baseline y=260, plot 580 wide × 200 tall; stroke `#999` width 1.5; x ticks at lengths 0/4/8/12/16/20 (10px `#999`), y ticks at 0/35/70/105/140 bits; x label 11px `#666` centered at y=295: "password length (characters)"; y label rotated −90° at x=30: "entropy (bits)".
- **Scale:** x = 29px per character; y = 200px per 140 bits.
- **Lines (width 2, straight segments through points at lengths 0/4/8/12/16/20):**
  | Model | color | style | bits at 4/8/12/16/20 |
  |---|---|---|---|
  | Lowercase only (4.70 bits/char) | #27ae60 | solid | 18.8 / 37.6 / 56.4 / 75.2 / 94.0 |
  | Full keyboard (6.55 bits/char) | #e67e22 | solid | 26.2 / 52.4 / 78.6 / 104.8 / 131.0 |
  | Human-chosen "complex" strings | #e74c3c | dashed [6,4] | 10 / 20 / 27 / 32 / 35 |
- **Marker dots** (radius 4, filled in line color, label 10px `#2c3e50` beside dot): green dot at (16, 75.2) labeled "16 lowercase ≈ 75 bits"; orange dot at (8, 52.4) labeled "8 full-charset ≈ 52 bits".
- **Legend** (upper-left inside plot from x=85, y=70, 20px line swatches, 11px `#2c3e50`, 16px row spacing): the three model names in their line colors.

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is a single non-wrapping line — a bold colored label rendered as `<span class="pt-label" style="color:#…">Label:</span>` followed by a phrase of at most ~55 characters; no multi-sentence bullets. Label colors by meaning: `#1a5276` blue = design/fact, `#27ae60` green = win/strength, `#e74c3c` red = flaw/risk, `#e67e22` orange = context/history/trend. The `(color)` note after each md label gives the exact color. The intro callout and key-point boxes open with a bold lead word in the same color scheme (`<strong style="color:#…">Word:</strong>`) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` font-weight bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300, `c4` 720×320; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width × `window.devicePixelRatio` and calls `ctx.scale`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`, bar fill = color at 0.6 alpha (or `rgba(26,82,118,0.35)` for neutral bars).
- No example password strings anywhere — human patterns are described in words only. In regenerated HTML, any card/page links use `.html` extensions (this page has none).
