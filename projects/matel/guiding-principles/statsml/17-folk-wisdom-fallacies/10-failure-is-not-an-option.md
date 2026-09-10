# "Failure is not an option"

**Page type:** detail page (claim-dissection card: why-believed line, flaw table, undefined-terms callout, counterexamples callout, all inside one saying-card)
**HTML title tag:** Failure is not an option — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Why people believe it

Why people believe it: it sounds decisive and strong. Associated with Apollo 13 (high-stakes context where it made partial sense).

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Survivorship bias | Quoted by people in contexts where they didn't fail. The phrase doesn't prevent failure; it just sounds good in retrospect. |
| 2 | Base rate neglect | In most endeavors, failure IS the statistically expected outcome. Declaring it "not an option" doesn't change the probability distribution. |
| 3 | Discourages experimentation | If failure isn't allowed, neither is learning. All innovation requires accepting failure as likely in early iterations. |
| 4 | Category error | Failure isn't an "option" you choose — it's an outcome you may get regardless of will or effort. |
| 5 | Unfalsifiable framing | If you succeed: "See! Failure wasn't an option." If you fail: "They didn't truly commit." No outcome disproves it. |

## Undefined terms (orange callout)

**Undefined terms:** "failure" (partial? total? temporary?), "option" (possibility? choice? acceptable outcome?)

## Counterexamples (green callout)

**Counterexamples:**

- SpaceX — "failure is an option here. If things are not failing, you are not innovating enough" (Musk)
- Scientific method — every experiment allows for negative results. Disallowing them = pseudoscience.
- Every iteration of every startup MVP — failure is expected and learned from

## Regeneration instructions

- **Template:** claim-dissection card layout (see `ui-templates/07-claim-dissection-cards.html`). Single page: h1 (quoted saying, no index number), `.subtitle` paragraph, then one `.saying-card` containing in order: `.saying-why` line, `.flaw-table`, `.undefined-terms` box, `.counterexamples` box. Page ends at the counterexamples block. No canvases on this page.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` — 0.88em, `#666`, margin-bottom 14px.
- **Flaw table:** `.flaw-table` — full width, border-collapse, 0.88em; th background `#f0f4f8`, padding 8px 12px, left-aligned, border `1px solid #e0e0e0`, color `#1a5276`; td padding 8px 12px, border `1px solid #e0e0e0`, vertical-align top; even rows `#fafcfe`. Columns: # / Fallacy / How it hides.
- **Callouts:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22` (orange), padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60` (green), padding 8px 14px, 0.88em, with a `<ul>` (margin 4px 0 0 16px, li margin 3px 0).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; p `#333` 0.95em; subtitle `#666` 1.0em; `strong` in `#1a5276`. A `.philosophy` style exists (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) though unused on this page. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas note:** this page has no canvases; if any are added, scale by `window.devicePixelRatio`. In regenerated HTML, any card links use `.html` extensions.
