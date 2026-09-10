# "Good things come to those who wait"

**Page type:** detail page (single saying-dissection card: why-believed line, flaw table, undefined-terms box, counterexamples box; no canvases)
**HTML title tag:** Good things come to those who wait — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Saying card

**Why-believed line:** Why people believe it: patience is sometimes rewarded, and the times it wasn't are forgotten.

### Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Selection on DV | Examines cases where waiting worked out; ignores cases where waiting meant missing the window entirely. |
| 2 | Ignores opportunity cost | Time spent waiting is time not spent acting. The comparison isn't wait vs act — it's wait vs best alternative use of that time. |
| 3 | Unfalsifiable | "Good things" and the timeframe are undefined. Wait long enough, and something good will happen by chance. How long invalidates it? |
| 4 | Survivorship | People who waited and got lucky tell the story. People who waited and got nothing don't. |
| 5 | Contradicts competing wisdom | Directly contradicts "strike while the iron is hot" and "the early bird catches the worm" — showing that proverbs are not a consistent system. |

### Undefined terms (orange callout)

**Undefined terms:** "good things" (what qualifies?), "wait" (how long? doing nothing? or preparing?), "come" (guaranteed? eventually? probably?)

### Counterexamples (green callout)

**Counterexamples:**

- First-mover advantage in markets — waiting means competitors capture the opportunity
- Compound interest — every year you wait to invest costs exponentially more
- Medical conditions — waiting to see a doctor often makes outcomes worse, not better

## Regeneration instructions

- **Template:** claim-dissection card layout (see `ui-templates/07-claim-dissection-cards.html`). Single page: h1 (the quoted saying, no index number), `.subtitle` paragraph, then one `.saying-card` containing in order: `.saying-why` line, `.flaw-table`, `.undefined-terms` box, `.counterexamples` box. Page ends at the counterexamples block. No canvases, no nav bar, no back/home links.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` — 0.88em, color `#666`, margin-bottom 14px.
- **Flaw table:** full width, border-collapse, 0.88em; `th` background `#f0f4f8`, padding 8px 12px, left-aligned, border `1px solid #e0e0e0`, color `#1a5276`; `td` padding 8px 12px, border `1px solid #e0e0e0`, vertical-align top; even rows background `#fafcfe`. Columns: # / Fallacy / How it hides.
- **Callout boxes:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em, with a `<ul>` (margin 4px 0 0 16px, li margin 3px 0).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with bottom border `2px solid #2980b9`; p 0.95em `#333`; `.subtitle` `#666` 1.0em; `strong` `#1a5276`; `.philosophy` style available (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but unused on this page.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Canvases (when present in this project) use `window.devicePixelRatio` scaling; this page has none. In regenerated HTML, any card links use `.html` extensions.
