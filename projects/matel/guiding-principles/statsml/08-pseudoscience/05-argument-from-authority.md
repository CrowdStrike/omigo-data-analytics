# Argument from Authority Disguised as Data

**Page type:** detail page (two-column obj-table layout: text left 40%, canvases right 60%, single section)
**HTML title tag:** Argument from Authority Disguised as Data — Pseudoscience in Data Analysis

**Subtitle:** Expert opinion treated as empirical evidence without context matching

## Section 1: "Expert/Successful Person Said X, Therefore X Is True"

- **CEO at conference:** "We do microservices and it transformed our engineering" — and 500 companies adopt them because a FAANG CEO said so. But what works at 10,000-engineer scale fails at a 15-person startup; the authority's context is invisible in the recommendation.
- **Famous researcher:** 200 published papers, so a new claim outside their expertise is accepted without scrutiny. Linus Pauling, Nobel laureate in chemistry, promoted megadose Vitamin C for cancer — wrong, yet his authority made people believe it for decades.
- **Data science influencer:** "Always use XGBoost," and 50,000 Kaggle users take it as gospel. But the influencer won competitions on data with specific characteristics; on your data, with a different distribution and signal-to-noise, linear regression might beat XGBoost.
- **"Big tech does it":** They use custom hardware, unlimited data, and PhD-level engineers, and can afford to over-engineer — you can't. Copying their architecture at your scale is cargo cult: same practice, catastrophically different context.

**Why it's pseudoscience:** Evidence from authority is not evidence from data — an expert's opinion is a hypothesis to test, not a conclusion to adopt, unless your constraints match theirs (they don't).

### Visualization (canvas `c1`, 720×300)

Text-panel comparison of big-tech context vs startup context (typographic layout, no chart marks).

- **Title (bold 17px, `#1a5276`, top center):** ""Big Tech Does It" Is Not Evidence It Works for YOU".
- **Body lines (17px `#333`, left-aligned at x=60):** "Big tech: 100K engineers, $100B revenue, unlimited compute, PhD-level teams" (y=60) and "Your startup: 8 engineers, $2M ARR, cloud credits running out, hiring is hard" (y=90).
- **Emphasis line (bold 17px `#e74c3c`, centered, y=125):** "Same practice (microservices, ML, custom infra) → catastrophically different outcomes".
- **Closing lines (17px `#555`, centered):** "Authority's context ≠ your context. Their success ≠ your evidence." (y=160) and "An expert's OPINION is a hypothesis to test, not a conclusion to adopt." (y=185).

### Visualization (canvas `c2`, 720×300)

Context-mismatch diagram: two context boxes connected through a shared central "practice" box by arrows.

- **Title (bold 17px Arial, `#1a5276`, top center):** "Context Mismatch".
- **Left box ("Their Context"):** at (30,35), 200×110, fill `rgba(39,174,96,0.1)`, stroke `#27ae60` width 2. Header bold 16px `#27ae60`: "Their Context". Body lines 16px `#2c3e50`, centered: "Big tech companies", "100K engineers", "$100B revenue", "Unlimited GPU".
- **Right box ("Your Context"):** at (w-230,35), 200×110, fill `rgba(231,76,60,0.1)`, stroke `#e74c3c` width 2. Header bold 16px `#e74c3c`: "Your Context". Body lines 16px `#2c3e50`, centered: "Startup", "8 engineers", "$2M funding", "cloud credits".
- **Middle box:** centered at (w/2-60,60), 120×40, fill `rgba(26,82,118,0.15)`, stroke `#1a5276` width 2; bold 18px `#1a5276` label: "Same Practice".
- **Arrows:** gray (`#7f8c8d`, width 1.5) lines with filled triangular heads: left box → middle box, middle box → right box.
- **Bottom annotation (bold 18px Arial, `#c0392b`, centered):** "Same practice, catastrophically different outcomes depending on context."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one full-width table with a single `<tr>`; left `<td>` (40%) holds `.obj-title` + `<ul>` bullets + trailing `<p>` paragraph, right `<td>` (60%, centered) holds both canvases (`c1` then `c2`) stacked.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Each chart drawn in its own IIFE. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, dark red `#c0392b`, grays `#666`/`#555`/`#333`/`#2c3e50`/`#7f8c8d`.
