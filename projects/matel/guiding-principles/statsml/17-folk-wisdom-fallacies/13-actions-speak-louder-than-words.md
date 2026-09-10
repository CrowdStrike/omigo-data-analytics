# "Actions speak louder than words"

**Page type:** detail page (single saying-dissection card: why-believed line, flaw table, undefined-terms box, counterexamples box; no canvases)
**HTML title tag:** Actions speak louder than words — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Saying card

**Why-believed line:** Why people believe it: observable behavior is often a better signal than stated intent. This is partially true — making it a more insidious fallacy.

### Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | False dichotomy | Frames actions and words as competing channels. In reality, communication IS an action with consequences — diplomacy, negotiation, teaching, therapy. |
| 2 | Context-blind universal | In many domains (law, contracts, leadership communication), words ARE the primary action. A CEO's public statement moves markets — that IS an action. |
| 3 | Ignores asymmetric observability | Actions are visible; motivations behind words are not. We discount words because we can't verify intent — but that's our measurement limitation, not a truth about relative value. |
| 4 | Survivorship in examples | Used when someone's words didn't match actions (broken promises). Never invoked when words DID match — creating a biased sample of when the saying is recalled. |

### Undefined terms (orange callout)

**Undefined terms:** "actions" (any action? which ones count?), "speak" (communicate? prove?), "louder" (more important? more truthful? more impactful?)

### Counterexamples (green callout)

**Counterexamples:**

- Diplomacy — words prevent wars that actions would escalate
- Therapy — words are the mechanism of healing; action alone doesn't resolve psychological pain
- Whistleblowing — words (testimony) are the action that creates accountability

## Regeneration instructions

- **Template:** claim-dissection card layout (see `ui-templates/07-claim-dissection-cards.html`). Single page: h1 (the quoted saying, no index number), `.subtitle` paragraph, then one `.saying-card` containing in order: `.saying-why` line, `.flaw-table`, `.undefined-terms` box, `.counterexamples` box. Page ends at the counterexamples block. No canvases, no nav bar, no back/home links.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` — 0.88em, color `#666`, margin-bottom 14px.
- **Flaw table:** full width, border-collapse, 0.88em; `th` background `#f0f4f8`, padding 8px 12px, left-aligned, border `1px solid #e0e0e0`, color `#1a5276`; `td` padding 8px 12px, border `1px solid #e0e0e0`, vertical-align top; even rows background `#fafcfe`. Columns: # / Fallacy / How it hides (this page has 4 flaw rows).
- **Callout boxes:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em, with a `<ul>` (margin 4px 0 0 16px, li margin 3px 0).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with bottom border `2px solid #2980b9`; p 0.95em `#333`; `.subtitle` `#666` 1.0em; `strong` `#1a5276`; `.philosophy` style available (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but unused on this page.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Canvases (when present in this project) use `window.devicePixelRatio` scaling; this page has none. In regenerated HTML, any card links use `.html` extensions.
