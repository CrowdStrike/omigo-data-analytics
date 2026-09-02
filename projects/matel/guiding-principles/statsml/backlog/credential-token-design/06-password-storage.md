# Password Storage Design

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Password Storage Design

**Subtitle:** From plaintext to memory-hard hashing — each design was forced by the attack that broke the one before, and the choice decides how a leaked database ages.

**Intro callout (blue-left-border box):** **Race:** [blue] password storage only starts to matter after the database leaks — the hash's per-guess cost is then all that stands between the leaked table and a cracked user base.

## 1. The evolution ladder

- **Plaintext:** [orange] earliest systems stored passwords as readable text
- **Exposure:** [red] any leak, backup, or admin read disclosed everything
- **Fast unsalted hash:** [orange] one-way hashing hid the stored text
- **Flaw:** [red] identical passwords produced identical hashes
- **Rainbow tables:** [red] precomputed lookups reversed human picks in bulk
- **Per-user salt:** [blue] a unique random value mixed into each user's hash
- **Win:** [green] every stored value distinct — precomputation dead
- **Residual flaw:** [red] the hash itself was still fast to compute
- **Throughput:** [red] billions of per-user guesses/s on commodity hardware
- **Memory-hard hash:** [green] each guess made costly in both time and memory
- **Tunable cost:** [blue] work factors let per-guess cost rise with hardware

**Key point (red-left-border box):** **Reactive:** [orange] every rung was adopted only after an attack broke the one below — defenders always one leak behind, never proactive.

### Visualization (canvas `c1`, 720×340)

Ascending staircase of four rung boxes, with red attack arrows between rungs.

- **Title (bold 13px `#1a5276`, top center, y=20):** "The storage evolution ladder".
- **Rung boxes** 220×62 (fill = rung color at 0.12 alpha, stroke = rung color width 2; title bold 12px in rung color 22px below box top; subtitle 10px `#666` centered 18px lower):
  | Rung | color | x | y (top) | subtitle |
  |---|---|---|---|---|
  | PLAINTEXT | #e74c3c | 30 | 255 | any reader sees every password |
  | FAST UNSALTED HASH | #e67e22 | 186 | 185 | same password, same hash |
  | PER-USER SALT | #2980b9 | 342 | 115 | precomputation defeated |
  | SLOW MEMORY-HARD HASH | #27ae60 | 498 | 45 | each guess costs time + memory |
- **Attack arrows:** `#e74c3c` width-1.5 line with filled arrowhead from each box's top-center to the next box's bottom-left area (e.g. from (140, 255) to (250, 247), from (296, 185) to (406, 177), from (452, 115) to (562, 107)); italic 10px `#e74c3c` label above each arrow's midpoint: "any read of the table", "precomputed rainbow tables", "GPU-speed guessing".

## 2. Offline vs online attack framing

- **Online guessing:** [blue] the attacker submits guesses to the live server
- **Server control:** [green] the server throttles, locks out, and alerts
- **Consequence:** [green] even weak passwords survive a few attempts/hour
- **Offline cracking:** [red] after a leak the attacker holds the hashes
- **Hardware speed:** [red] guessing runs on machines the attacker owns
- **No recourse:** [red] nothing the server does after the leak slows it
- **Design lever:** [blue] only the cost of one guess stays in defender hands
- **Slow hashing:** [green] memory-hard designs raise exactly that cost

**Key point:** **Pivot:** [red] storage design is irrelevant until the leak — then its slowness is the only barrier to mass cracking.

### Visualization (canvas `c2`, 720×300)

Horizontal log-scale bars: guess throughput per attack setting.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Guess throughput by attack setting (illustrative)".
- **Bars:** 26px tall; labels right-aligned 12px `#2c3e50` ending at x=240; bars start x=250; length = (log10(rate) + 2) × 35px (so one decade = 35px, zero at 10⁻² per second); fill = row color at 0.6 alpha with 1px solid stroke; rate text bold 11px `#2c3e50` after the bar.
  | Setting | rate label | log10 | color | row center y |
  |---|---|---|---|---|
  | Online, rate-limited server | ~10⁻² guesses/s | −2 (min bar 4px) | #27ae60 | 80 |
  | Offline vs slow memory-hard hash | ~10³ guesses/s | 3 | #e67e22 | 140 |
  | Offline vs fast unsalted hash | ~10¹⁰ guesses/s | 10 | #e74c3c | 200 |
- **Axis note (11px `#666`, left-aligned at x=250, y=240):** "log scale — each step is ×10".
- **Caption (bottom center, 11px `#999`, y=285):** "Twelve orders of magnitude separate a throttled login form from a leaked fast hash."

## 3. Storage design decides which composition rules matter

- **Slow-hash regime:** [blue] per-guess cost is so high that length dominates
- **Length wins:** [green] every extra character multiplies the attacker's bill
- **Classes lose:** [red] extra character classes add almost nothing
- **Fast-hash regime:** [red] throughput fells any human-chosen password
- **No rescue:** [red] no rule saves a pick from the predictable subspace
- **Storage-side half:** [orange] the other half of the composition-rules story
- **Stakes:** [blue] class rules only matter when the stored hash is slow

**Key point:** **Order:** [blue] user-facing rules are second-order — the first-order defense is the per-guess price the user never sees.

### Visualization (canvas `c3`, 720×300)

2×2 outcome matrix: storage design × password style.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Which leaked passwords get cracked (illustrative)".
- **Column headers** (bold 12px `#2c3e50`, centered over each column at y=58): "Fast unsalted hash" (x=305), "Slow memory-hard hash" (x=565).
- **Row labels** (12px `#2c3e50`, right-aligned ending x=170, vertically centered on each row): "Short complex string" (y=115), "Long passphrase" (y=210).
- **Cells** 250×85, columns at x=180 and x=440, rows at y=72 and y=167 (fill = cell color at 0.15 alpha, stroke = cell color width 1.5; verdict bold 12px in cell color centered):
  | Cell (row, col) | color | verdict |
  |---|---|---|
  | short + fast | #e74c3c | Cracked in minutes |
  | short + slow | #e67e22 | At risk — predictable subspace |
  | long + fast | #e67e22 | At risk — throughput still wins |
  | long + slow | #27ae60 | Survives the leak |
- **Caption (bottom center, 11px `#999`, y=285):** "Only the slow-hash column gives composition any stakes — and there, length wins."

## 4. Extra layers, current vs historical

- **Pepper:** [blue] a per-deployment secret mixed into every hash
- **Kept outside:** [blue] stored in a config store or hardware module
- **Win:** [green] a database-only leak can't even be guessed against
- **Hardware wrapping:** [blue] high-security sites hash inside a hardware module
- **Effect:** [green] cracking pinned to the defender's rate-limited device
- **Migration limit:** [red] hashes can't upgrade in place without the password
- **Rehash-on-login:** [blue] wrap or rehash at the next successful login
- **Retirement:** [orange] the legacy scheme is retired gradually
- **Table stakes:** [orange] salted slow hashing is now the baseline
- **Frontier:** [orange] tuning memory-hardness against evolving hardware
- **Credential stuffing:** [red] replayed leaks elsewhere bypass cracking entirely

**Key point:** **Limit:** [red] storage hardening cannot protect a password already leaked on another site — reuse defense is a separate problem.

### Visualization (canvas `c4`, 720×300)

Horizontal bar ranking: prevalence of storage defenses.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Prevalence of storage defenses (illustrative)".
- **Bars:** 22px tall, 10px gap, starting y=48; labels right-aligned 12px `#2c3e50` ending at x=230; track `#f0f0f0` 400px max from x=242; bar fill = row color at 0.6 alpha with 1px solid stroke; value bold 11px `#2c3e50` after the bar.
- **Data (label, value, color):**
  | Defense | value | color |
  |---|---|---|
  | Per-user salt | 95 | #27ae60 |
  | Slow adaptive hashing | 80 | #27ae60 |
  | Rehash-on-next-login migration | 55 | #2980b9 |
  | Memory-hard work-factor tuning | 45 | #2980b9 |
  | Pepper outside the database | 25 | #e67e22 |
  | Hardware-module wrapping | 8 | #8e44ad |
- **Caption (bottom center, 11px `#999`, y=285):** "Table stakes at the top; the current frontier at the bottom."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one line — a bold colored label naming the concept plus a phrase short enough (≤55 characters) not to wrap in the 45% text column. Long ideas are split into more labeled bullets, never longer ones. Markdown form: `- **Label:** [color] phrase`. HTML form: `<span class="pt-label" style="color:#hex">Label:</span> phrase`, with CSS `.pt-label { font-weight: 600; }`.
- **Label colors:** the `[color]` tag on each md bullet maps to `#1a5276` blue = design/fact, `#27ae60` green = win/strength, `#e74c3c` red = flaw/risk, `#e67e22` orange = context/history/trend. Drop the `[color]` tag in HTML — it becomes the span's inline color.
- **Callout/key-point style:** the `.intro` callout and each `.key-point` box are one short sentence led by the same bold colored `pt-label` lead word, colored by the same scheme.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×340, `c2` 720×300, `c3` 720×300, `c4` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width × `window.devicePixelRatio` and calls `ctx.scale`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`, bar fill = color at 0.6 alpha (or `rgba(26,82,118,0.35)` for neutral bars).
- No example password or hash strings anywhere — schemes and attacks are described in words only. In regenerated HTML, any card/page links use `.html` extensions (this page has none).
