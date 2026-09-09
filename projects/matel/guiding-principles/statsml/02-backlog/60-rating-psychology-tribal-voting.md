# Rating Psychology — Tribal Voting

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** Rating Psychology — Tribal Voting

**Subtitle:** July 2008: The Dark Knight opened, shot to #1 on the IMDb Top 250 above The Godfather, and within weeks was pushed back down.

**Intro callout:** When a published average becomes a target, votes turn into moves. The episode mixes two effects — ordinary opening-week self-selection and coordinated extreme voting — and separating them shows which part of the drop was actually an attack.

## 1. What Happened

Two things moved the rating, and they are worth keeping separate.

- **Opening surge** — the first raters of any release are the people who chose to see it immediately. Their ratings run high and arrive fast, then decay as a general audience catches up. This happens with or without a fight.
- **Coordinated voting** — the ranking flip was itself news, so 1/10 votes on Dark Knight and 10/10 on Godfather both spiked. Once the ranking is a target, votes are moves rather than opinions.

**Key point:** Most of the drop was the normal opening decay. Only the extra dip belongs to the fan war.

### Visualization (canvas `c1`, 720×320)

Line chart: schematic rating trajectories after release for a new release vs a long-established film, with a highlighted coordination window.

- **Title (bold 16px, `#1a5276`, top center):** "Rating After Release (schematic)".
- **Plot area:** x=76, y=48, width = canvas−150, height = canvas−110; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** y from 8.6 to 9.4, tick labels every 0.2 ("8.6, 8.8, 9.0, 9.2, 9.4", 12px `#5a6875`, right-aligned); x spans 0–20 weeks, axis label "Weeks since release" (13px `#4a5866`, centered below).
- **Coordination window:** shaded rectangle `rgba(230,126,34,0.18)` spanning weeks 1.5–4.5, full plot height.
- **New release curve:** v(t) = 8.75 + 0.55·e^(−t/3.2) − 0.13·e^(−((t−3)/1.5)²), drawn t=0→20 in 0.4 steps, stroke `#2980b9` 3px.
- **Established line:** near-flat line from (0, 9.21) to (20, 9.20), stroke `#e74c3c` 3px.
- **Flip marker:** 8px-radius circle outline in `#e67e22` (2.5px) at (week 1.7, 9.21) where the curves cross.
- **Labels (13px, left-aligned):** "#1 flips" in `#e67e22` beside the circle (at y≈9.36); "new release" in `#2980b9` at (week 7, 8.97); "long-established" in `#e74c3c` at (week 10, 9.26).

## 2. The Mean Is the Fragile Part

Coordinated votes pile up at 1 and 10, because those move the average most per vote cast.

- A block of 1/10 votes shifts the mean hard and leaves the median untouched.
- Near the top of a ranked list, films sit hundredths of a point apart — a shift too small to matter as an estimate still reorders the list.
- Votes still converge as they accumulate. They converge on the mobilised crowd's opinion, which is a different quantity than the one the number claims to report.

**Key point:** Publish the distribution, not just the average. A median or trimmed mean survives a coordinated minority; the arithmetic mean has no such floor.

### Visualization (canvas `c2`, 720×320)

Paired-bar histogram of vote distributions (ratings 1–10), honest/uncontested vs contested.

- **Title (bold 16px, `#1a5276`, top center):** "Where the Votes Land".
- **Data (percent per rating 1–10):**
  - uncontested: `[0.4, 0.3, 0.4, 0.6, 1.2, 2.6, 6.5, 17, 29, 42]`
  - contested: `[11, 1.6, 1.2, 1.4, 2.0, 3.4, 6.8, 15, 24, 33.6]`
- **Plot area:** x=66, y=68, width = canvas−120, height = canvas−132; scale max 44; L-shaped axes `#95a5a6` (1.4px).
- **Bars:** 10 slots; per slot two bars each 0.34·slot-width wide — left bar (uncontested) fill `rgba(39,174,96,0.50)` stroke `#27ae60` 1.4px; right bar (contested) fill `rgba(231,76,60,0.50)` stroke `#e74c3c`.
- **X labels:** "1"…"10" (13px `#4a5866`) under each slot; axis label "Rating given" centered below.
- **Legend (top-left inside plot):** green swatch + "uncontested", red swatch + "contested" (13px `#2c3e50`).
- **Annotation (13px `#e74c3c`, left-aligned near top of plot):** "median 9 either way — mean drops 8.9 → 7.7".

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 45% and `td.viz-col` 55%, both `vertical-align: top`, 12px padding. No index number in the h1.
- **Text blocks:** intro `<p>`, `<ul>` bullets (0.92rem) with `<strong>` lead-ins, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; secondary `#2980b9`; gray labels `#5a6875`/`#4a5866`, axes `#95a5a6`.
- **Canvas:** intrinsic 720×320; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), `ctx.scale` back to logical coordinates via a shared `setupCanvas(id)` helper.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
