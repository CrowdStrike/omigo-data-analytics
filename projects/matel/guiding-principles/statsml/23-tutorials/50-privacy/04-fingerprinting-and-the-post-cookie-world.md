# Fingerprinting & the Post-Cookie World

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Fingerprinting & the Post-Cookie World

**Subtitle:** Your fonts and timezone are nearly unique — each browser attribute narrows the crowd, and enough small clues together point to exactly one person

## Fonts and a Timezone Walk Into a Crowd

**Tags:** `core idea` (blue), `entropy` (green), `tracking` (orange)

- **The crowd** — a website's visitor could be any of roughly 8,000,000,000 people on Earth
- **First clue** — a 2560×1440 screen is one of ~32 common setups: the crowd shrinks to ~250,000,000
- **Second clue** — timezone plus language list cuts by another ~32×, down to ~7,800,000 people
- **Third clue** — the exact set of installed fonts is rarer, ~1 in 1,024: now ~7,600 people remain
- **Fourth clue** — canvas rendering quirks (GPU + driver + OS) cut ~1,024× more: about 7 people left
- **The reveal** — add the GPU renderer string and the intersection is usually a single browser

*Example (italic):* No single clue identifies you — millions share your screen size — but the AND of five ordinary clues leaves a crowd of one.

**Key point:** A browser fingerprint is the combination of many low-information attributes; each one only narrows the crowd a little, but the narrowing multiplies, and published research (EFF's Panopticlick, 2010) found about 84% of sampled browsers were already unique.

### Visualization (canvas `c1`, 720×300)

Horizontal funnel bar chart: crowd size remaining after each attribute is observed, log-feel widths, one bar per step.

- **Title (bold 15px, `#1a5276`, top center):** "Each Attribute Narrows the Crowd: 8 Billion Down to 1".
- **Layout:** five bars at y = 60, 100, 140, 180, 220, each 22px tall, left edge x=230, max width 440; 12px `#444` row labels left-aligned at x=20: "everyone", "+ screen resolution", "+ timezone + language", "+ installed fonts", "+ canvas quirks + GPU".
- **Bar widths (hardcoded, log-feel, not a real log axis):** 440, 375, 305, 165, 8.
- **Bar fills:** first four `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge; last bar solid red `#e74c3c`.
- **Count labels (bold 12px `#1a5276` at each bar's right end):** "8,000,000,000", "250,000,000", "7,800,000", "7,600", "≈ 1".
- **Annotation (bold 13px red `#e74c3c`, near x=280, y=250):** "no single clue did it — the combination did".
- **Caption (12px `#444`, bottom right):** "per-attribute cuts illustrative; the multiplication is the point".

## Counting to 33 Bits

**Tags:** `worked example` (blue), `information theory` (green)

- **The target** — 2^33 = 8,589,934,592, so 33 yes/no questions suffice to single out one of 8 billion
- **The exact bit count** — log2(8,000,000,000) ≈ 32.9 bits; this number is exact math, not an estimate
- **Entropies add** — independent attributes contribute bits that sum: 5 + 5 = a 1-in-1,024 cut, not 1-in-64
- **The tally** — resolution ~5 bits, timezone+language ~5, fonts ~10, canvas ~10, GPU string ~4: total ~34
- **Hand-check** — 34 bits means 2^34 ≈ 17 billion buckets for 8 billion people: most buckets hold one person
- **The caveat** — attributes correlate (Mac users share fonts AND GPUs), so real totals run a bit lower

*Example (italic):* 5 + 5 + 10 + 10 + 4 = 34 bits collected from one page load — one bit past the ~33 needed to name a single human.

**Key point:** Identification is cheap in information terms: ~33 bits singles out anyone alive (exact), and ordinary browser attributes carrying 2-10 bits each (illustrative) sum past that line in a handful of reads.

### Visualization (canvas `c2`, 720×300)

Horizontal stacked bar: one bar of accumulated entropy built from five attribute segments, with a vertical threshold line at 33 bits.

- **Title (bold 15px, `#1a5276`, top center):** "Bits Add Up: Five Attributes Cross the 33-Bit Line".
- **Axis:** baseline y=245, 2px `#999`; x scale 0-36 bits mapped to x=60..660 (16.67 px/bit); tick labels 0, 8, 16, 24, 32 in 12px `#444`; gridlines `#e5e9ef` at each tick.
- **Stacked bar (y=120, 50px tall), segments left to right with widths in bits [5, 5, 10, 10, 4]:** resolution blue `#2a78d6`, timezone+language aqua `#199e70`, fonts violet `#4a3aa7`, canvas quirks orange `#d95926`, GPU string magenta `#d55181`; each segment labeled inside or above in bold 12px white/own color: "resolution 5", "tz+lang 5", "fonts 10", "canvas 10", "GPU 4".
- **Threshold line:** vertical dashed red `#e74c3c` (dash 5/4) at 33 bits from y=70 to y=245, bold 13px red label near top: "33 bits = 1 of 8.6 billion (exact: 2^33)".
- **Total marker (bold 13px `#1a5276` at the bar's right end, two stacked lines):** "total" / "≈ 34 bits".
- **Caption (12px `#444`, bottom right):** "threshold exact; per-attribute bits illustrative".

## Nothing to Delete: Tracking After Cookies

**Tags:** `where it's used` (blue), `post-cookie` (orange)

- **The cookie era** — a cookie is a stored ID: visible in settings, deletable, blockable by the browser
- **The fingerprint era** — nothing is stored; the ID is recomputed from your browser's behavior each visit
- **No delete button** — clearing storage changes nothing, because the fingerprint IS the observable config
- **The tracker's trade** — high-entropy attributes (fonts, canvas) drift as software updates; stable ones carry few bits
- **Linking across drift** — trackers match "34 bits, 30 of them unchanged" to re-identify after an update
- **Regulation's view** — EU and browser-vendor policies increasingly treat fingerprinting as tracking, consent required

*Example (italic):* A user clears cookies nightly; the site recomputes the same 34-bit fingerprint at breakfast and the profile continues uninterrupted.

**Key point:** Cookies are state you can delete; a fingerprint is a function of your machine, recomputable on every visit — the tracker's real problem is not storage but drift, trading stability against entropy.

### Visualization (canvas `c3`, 720×300)

Scatter plot of attributes on stability (x) vs entropy (y), showing the tracker's trade-off: the ideal top-right corner is nearly empty.

- **Title (bold 15px, `#1a5276`, top center):** "The Tracker's Trade-off: Stable Attributes Carry Few Bits".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = "typical days unchanged" 0 to 400 (ticks 0/100/200/300/400, 12px `#444`); y = entropy in bits 0 to 12, gridlines `#e5e9ef` at 4 and 8.
- **Points (7px filled circles, bold 12px labels beside each):** timezone (365, 3) blue `#2a78d6`; language list (365, 2) blue; screen resolution (300, 5) aqua `#199e70`; GPU string (200, 4) aqua; installed fonts (60, 10) orange `#d95926`; canvas quirks (45, 10) orange.
- **Quadrant shading:** faint `rgba(0,131,0,0.06)` rectangle over x>250, y>8 with bold 12px green `#008300` label "tracker's dream — mostly empty".
- **Annotation (bold 13px orange `#d95926`, near x=110, y=95):** "high-bit attributes drift with every update".
- **Caption (12px `#444`, bottom right):** "days and bits illustrative".

## The Spoofing Paradox

**Tags:** `common mistake` (red), `countermeasures` (orange)

- **The instinct** — install add-ons that block canvas reads and report a fake font list
- **The paradox** — a browser that refuses canvas reads and lists 6 fake fonts is itself a rare, trackable signature
- **The measure** — what protects you is your anonymity set: how many other people look exactly like you
- **The numbers** — default setup ~7 lookalikes; hand-tuned spoofing ~1 (you alone); uniform-mode browser ~500,000
- **The modern fix** — browsers converge on sameness: uniform reported values, rounded screen sizes, reduced APIs
- **The honest state** — it is an arms race; uniformity helps, new APIs leak new bits, regulation backstops the rest

*Example (italic):* A user installs a rare anti-canvas extension to hide, and its distinctive refusal pattern becomes the strongest identifying bit they broadcast.

**Common mistake:** Treating "block and spoof" as privacy. Standing out is the failure mode — you cannot lie your way into a crowd; you can only dress like one, which is why modern browsers make everyone answer alike.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: anonymity set size (people who share your exact fingerprint) for three configurations, log-feel widths — bigger bar = safer.

- **Title (bold 15px, `#1a5276`, top center):** "Anonymity Set: How Many People Look Exactly Like You?".
- **Layout:** three bars at y = 85, 145, 205, each 26px tall, left edge x=250, max width 420; 12px `#444` row labels left-aligned at x=20: "default browser", "hand-tuned spoofing add-ons", "uniform-mode privacy browser".
- **Bar widths (hardcoded, log-feel):** 130, 6, 420.
- **Bar fills:** default `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge; spoofed solid red `#e74c3c`; uniform solid green `#008300`.
- **Count labels (bold 12px, matching bar color, at each bar's right end):** "≈ 7 lookalikes", "1 — you alone", "≈ 500,000 lookalikes".
- **Annotation (bold 13px red `#e74c3c`, near x=300, y=155):** "spoofing made this user MORE distinctive".
- **Annotation (bold 13px green `#008300`, near x=380, y=250):** "sameness, not disguise, is the defense".
- **Caption (12px `#444`, bottom right):** "set sizes illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness). Exact facts: 2^33 = 8,589,934,592 and log2(8,000,000,000) ≈ 32.9 bits; Panopticlick (EFF, 2010) reported ~84% of sampled browsers unique. Everything else — per-attribute bit values (5/5/10/10/4), funnel crowd counts (250,000,000 / 7,800,000 / 7,600 / 7 / 1), stability days, and anonymity-set sizes (7 / 1 / 500,000) — is illustrative and labeled so in captions; funnel counts must stay consistent with the 5/5/10/10/4 bit tally.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
