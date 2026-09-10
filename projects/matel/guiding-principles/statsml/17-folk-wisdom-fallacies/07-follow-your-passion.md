# "Follow your passion"

**Page type:** detail page (claim-dissection card: why-believed line, flaw table, undefined-terms callout, counterexamples callout, all inside one saying-card)
**HTML title tag:** Follow your passion — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Why people believe it

Why people believe it: successful people often are passionate about their work — but the causal direction may be reversed.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Survivorship bias | People passionate about painting who became successful painters are visible. The 10,000 who followed the same passion and are broke are not. |
| 2 | Reverse causation | People often become passionate about things they're good at (skill → passion), not the other way around. |
| 3 | Self-selection bias | Passionate people self-select into fields where passion is visible (arts, sports). In fields where it's not (accounting), the advice seems absurd. |
| 4 | Ignores market reality | Passion is supply-side. Demand determines whether it pays. Many passions have massive supply and tiny demand. |
| 5 | Single-factor | Ignores skill, timing, connections, market fit, financial runway. |

## Undefined terms (orange callout)

**Undefined terms:** "passion" (interest? obsession? any enjoyment?), "follow" (career? hobby? abandon everything else?)

## Counterexamples (green callout)

**Counterexamples:**

- Most professional athletes are passionate — most passionate athletes never go pro
- Cal Newport's research: passion often develops AFTER mastery, not before
- People who followed practical paths and discovered passion through competence

## Regeneration instructions

- **Template:** claim-dissection card layout (see `ui-templates/07-claim-dissection-cards.html`). Single page: h1 (quoted saying, no index number), `.subtitle` paragraph, then one `.saying-card` containing in order: `.saying-why` line, `.flaw-table`, `.undefined-terms` box, `.counterexamples` box. Page ends at the counterexamples block. No canvases on this page.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` — 0.88em, `#666`, margin-bottom 14px.
- **Flaw table:** `.flaw-table` — full width, border-collapse, 0.88em; th background `#f0f4f8`, padding 8px 12px, left-aligned, border `1px solid #e0e0e0`, color `#1a5276`; td padding 8px 12px, border `1px solid #e0e0e0`, vertical-align top; even rows `#fafcfe`. Columns: # / Fallacy / How it hides.
- **Callouts:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22` (orange), padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60` (green), padding 8px 14px, 0.88em, with a `<ul>` (margin 4px 0 0 16px, li margin 3px 0).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; p `#333` 0.95em; subtitle `#666` 1.0em; `strong` in `#1a5276`. A `.philosophy` style exists (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) though unused on this page. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas note:** this page has no canvases; if any are added, scale by `window.devicePixelRatio`. In regenerated HTML, any card links use `.html` extensions.
