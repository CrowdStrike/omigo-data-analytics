# Security Questions & KBA

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Security Questions & KBA

**Subtitle:** The knowledge factor that outsourced its secret to your biography — and what happened when biographies became searchable.

**Intro callout (blue-left-border box):** **Premise:** [blue] the design rests on answers being private, stable, and memorable — all three failed in the field.

## 1. The static Q&A design

A knowledge factor captured at enrollment and replayed at recovery.

- **Enrollment:** [blue] the user picks canonical questions and records answers
- **Recovery:** [blue] matching answers stand in for the password
- **Assumed private:** [blue] only you know the answers
- **Assumed stable:** [blue] the same answer holds years later
- **Assumed memorable:** [blue] no need to write answers down
- **Low entropy:** [red] a few common answers cover most users
- **Tiny space:** [red] the answer space is a fraction of a password's
- **Researchable:** [red] the questions ask facts people publish
- **Public secret:** [red] the answer often sits on a public profile
- **Shared:** [red] the same life facts unlock every site
- **Unrotatable:** [red] a leaked answer can never be changed
- **Fixed history:** [red] your biography does not rotate

### Visualization (canvas `c1`, 720×300)

Two-box contrast diagram: designed assumptions vs observed reality.

- **Title (bold 13px `#1a5276`, top center):** "Design assumptions vs observed reality".
- **Left box** 310×180 at x=30, y=50 (fill `#27ae60` at 0.12 alpha, stroke `#27ae60` width 2); header bold 13px `#27ae60` centered at box-center x=185, y=76: "Designed assumptions"; items 12px `#2c3e50` centered, first at y=108, 28px apart: "Answers are private", "Answers are stable over time", "Answers are easy to recall".
- **Arrow:** `#bbb` width-2 line from (340, 140) to (375, 140) with a filled `#bbb` triangle head at (382, 140).
- **Right box** 310×180 at x=385, y=50 (fill `#e74c3c` at 0.12 alpha, stroke `#e74c3c` width 2); header bold 13px `#e74c3c` centered at x=540, y=76: "Observed reality"; items 12px `#2c3e50` centered, first at y=108, 28px apart: "Answers are researchable or guessable", "Answers change or are misremembered", "The same facts are reused everywhere".
- **Caption (bottom center, 11px `#999`, y=280):** "Every assumption the design rests on fails in the field."

## 2. Failure analysis

- **Reused password:** [red] the same answer works on every site that asks
- **No reset:** [red] no reset button exists for a mother's maiden name
- **Social-media era:** [orange] the canonical questions became lookup-able
- **The canon:** [blue] first pet, mother's maiden name, first school
- **Published lives:** [orange] people publish the facts the questions ask
- **Attacker's job:** [red] shifted from guessing to reading
- **Honest answers:** [red] truth concentrates in a tiny answer space
- **Strictly worse:** [red] honesty loses to answering randomly
- **Random answers:** [red] restore entropy but guarantee forgetting
- **Self-defeating:** [red] recovery fails at the moment it exists for

**Key point (red-left-border box):** **Dilemma:** [red] honest answers are guessable and researchable, random answers are forgettable — no setting of the knob makes static Q&A work well.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: effective answer-space size in bits, honest answers vs a random secret.

- **Title (bold 13px `#1a5276`, top center):** "Effective answer space in bits (illustrative)".
- **Bars:** 24px tall, 14px gap, starting y=56; labels right-aligned 12px `#2c3e50` ending at x=210; track `#f0f0f0` 340px max (scaled so 60 bits = 340px); bar fill = row color at 0.6 alpha with 1px solid stroke in the row color; value text bold 11px `#2c3e50` after the bar.
- **Data (label, bits, color):**
  | Answer source | bits | color |
  |---|---|---|
  | Favorite color | 3 | #e74c3c |
  | First pet's name | 7 | #e74c3c |
  | First school | 9 | #e67e22 |
  | Mother's maiden name | 11 | #e67e22 |
  | Random machine-chosen answer | 60 | #27ae60 |
- **Value labels:** "~3 bits", "~7 bits", "~9 bits", "~11 bits", "~60 bits".
- **Caption (bottom center, 11px `#999`):** "Honest answers occupy a tiny space; a random answer is just a second password to forget."

## 3. Dynamic KBA (credit-file style)

Dynamic KBA generates its questions on the fly instead of storing them.

- **On-the-fly:** [blue] questions are built from third-party records
- **Record sources:** [blue] past addresses, loan figures, vehicles
- **Nothing enrolled:** [green] no stored answers leak from the service
- **Stronger bar:** [green] the attacker must hold a copy of your records
- **Higher cost:** [green] the job shifts from research to data acquisition
- **Broker errors:** [red] honest users fail on wrong or stale records
- **No telling apart:** [red] a confused owner looks like an attacker
- **Locale-limited:** [red] needs locales with deep consumer data files
- **Hard borders:** [red] coverage stops where the data files stop
- **Breach decay:** [red] every broker or credit-file leak weakens it
- **Knowledge shift:** [red] "only you know" becomes "the attacker knows"
- **Permanent:** [red] breached records never rotate back

**Key point (red-left-border box):** **Outsourced secret:** [red] every breach of the third party's database decays a strength the user cannot restore — the unrotatable-secret failure, one level removed.

### Visualization (canvas `c3`, 720×320)

Two-column comparison table drawn on canvas: static Q&A vs dynamic KBA across four dimensions.

- **Title (bold 13px `#1a5276`, top center):** "Static Q&A vs dynamic KBA".
- **Column headers** bold 13px centered at y=56: "Static Q&A" in `#e74c3c` at x=330, "Dynamic KBA" in `#2980b9` at x=560; each header underlined by a width-2 line in its color from ±70px around its center x, at y=64.
- **Row separator lines:** `#e0e0e0` width 1 from x=30 to x=660 at y=76/132/188/244.
- **Rows** (row label bold 12px `#666` left-aligned at x=30; cell text 11px `#2c3e50` centered at x=330 and x=560; each row's text baseline centered in its band, at y=104/160/216/272):
  | Row label | Static Q&A | Dynamic KBA |
  |---|---|---|
  | Attacker needs | public profiles and patience | a copy of your records |
  | Honest-user failure | forgotten or drifted answers | inaccurate broker data |
  | Coverage | works anywhere | locales with deep data files |
  | Degrades when | social media grows | broker databases leak |

## 4. Regional prevalence and decline

- **Once mandatory:** [orange] major webmail and banking providers required them
- **Standard path:** [orange] for years the default recovery route
- **Peak scale:** [orange] among the most widely deployed credentials ever
- **Still lingering:** [orange] some banking and government flows keep them
- **Regional:** [orange] some regions retain static Q&A or credit-file KBA
- **One layer:** [blue] now one gate among several, rarely the sole gate
- **The replacement:** [green] a code sent to a channel you already control
- **Possession proof:** [green] control of a device or mailbox
- **Why it won:** [green] stronger and easier than proving a memory
- **Modern guidance:** [orange] knowledge-based recovery is discouraged outright
- **New systems:** [orange] mostly never build it at all
- **Legacy tail:** [orange] the remaining installations are holdovers
- **Two-decade retreat:** [orange] the retirement has run for twenty years

**Illustrative Example (italic `.example` block):** Alice enrolled honest answers years ago and has not thought about them since; today the same facts appear across her public profiles, so her "secret" expired without her doing anything at all.

### Visualization (canvas `c4`, 720×300)

Two-line time chart: prevalence of recovery methods at major providers over time.

- **Title (bold 13px `#1a5276`, top center):** "Recovery methods at major providers over time (illustrative)".
- **Axes:** origin x=80, baseline y=250, plot 580×180 (top y=70), stroke `#999` width 1.5; x tick labels 11px `#666` centered at y=268 for years 2000/2005/2010/2015/2020/2025, mapping year t → x = 80 + (t − 2000) × 23.2; y label (rotated −90°, 11px `#666`, left of axis): "share of providers →".
- **Static Q&A line** `#e74c3c` width 2.5 through fraction points (year, share), y = 250 − share × 180:
  | year | share |
  |---|---|
  | 2000 | 0.55 |
  | 2005 | 0.80 |
  | 2008 | 0.90 |
  | 2012 | 0.75 |
  | 2016 | 0.50 |
  | 2020 | 0.28 |
  | 2025 | 0.15 |
- **Possession-based line** `#27ae60` width 2.5 through:
  | year | share |
  |---|---|
  | 2000 | 0.20 |
  | 2005 | 0.30 |
  | 2010 | 0.50 |
  | 2015 | 0.72 |
  | 2020 | 0.88 |
  | 2025 | 0.95 |
- **Line labels:** bold 11px in line color, left-aligned — "security questions" at (300, 100), "possession-based recovery (code to your channel)" at (250, 210).
- **Caption (bottom center, 11px `#999`, y=292):** "Knowledge-based recovery peaked, then was steadily retired in favor of possession proofs."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`/`.example`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one line — a bold colored label naming the concept plus a phrase short enough not to wrap in the 45% text column (roughly ≤55 characters). Never merge clauses into one bullet; split long content into more labeled bullets. Lead paragraphs are at most one short sentence.
- **Bullet label markup:** in HTML each bullet renders as `<li><span class="pt-label" style="color:COLOR">Label:</span> phrase</li>` with `.pt-label { font-weight: 600; }`. In this md, each bullet carries its color tag as `- **Label:** [color] phrase`; drop the `[color]` tag in the rendered HTML.
- **Label color scheme (by meaning):** `#1a5276` blue = design/fact, `#27ae60` green = win/strength, `#e74c3c` red = flaw/risk, `#e67e22` orange = context/history/trend.
- **Callout style:** the `.intro` and `.key-point` boxes each open with a bold colored lead word (same color scheme) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×320, `c4` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width × `window.devicePixelRatio` and scales the context accordingly.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- **Content rule:** never print example answers styled as credentials — name question categories and answer patterns in words only; keep every quantified chart title suffixed "(illustrative)"; people are Alice/Bob and providers are unnamed or "Vendor A"-style.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
