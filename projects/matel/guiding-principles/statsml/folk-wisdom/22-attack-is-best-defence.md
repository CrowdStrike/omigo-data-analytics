# "Attack is the best form of defence"

**Page type:** detail page (claim-dissection card: single `.saying-card` containing why-believed line, flaw table, undefined-terms callout, counterexamples callout)
**HTML title tag:** Attack is the best form of defence — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected — why an observability asymmetry keeps this one alive.

## Why people believe it

Why people believe it: aggression produces visible, attributable wins. Successful defence produces nothing observable at all.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Unobservable counterfactual | Defence succeeds by producing non-events. Attack produces artefacts you can point at. The conclusion comes from what gets logged, not what works. |
| 2 | Per-round payoff vs ruin | Positive expected value per encounter, non-zero elimination risk per encounter. Win each of 20 encounters with probability 0.90 and survival is 0.90 to the 20th power — about 12%. "Wins 9 in 10" and "ends in ruin" describe the same strategy at different horizons. |
| 3 | Substitutes vs complements | Treated as one dial. They are separate capabilities competing for one budget. A goalkeeper is not a slower striker. |
| 4 | Deterrence ≠ aggression | Credible capability to retaliate does reduce attacks. Actually attacking is a different action. The proverb borrows one's evidence for the other. |
| 5 | Survivorship | Histories are written by winners, and aggressive winners tell better stories than cautious ones. |

## Undefined terms

**Undefined terms:** "attack" (pre-emptive strike? deterrent posture? litigation?), "defence" (prevention? detection? recovery?), "best" (highest payoff? lowest ruin risk? over what horizon?)

## Counterexamples

**Counterexamples:**

- Security: funding red-teaming while patching and backups go underfunded buys visibility into weaknesses, not the ability to survive them
- Sport: goal *differential* decides leagues — score more and concede more and you finish below a defensive side
- Litigation: aggressive filing invites counterclaims and discovery that expose the initiator

## Regeneration instructions

- **Template:** claim-dissection card style (see `ui-templates/07-claim-dissection-cards.html`). Single page: quoted-saying `<h1>`, `.subtitle` paragraph, then one `.saying-card` div containing (in order) `.saying-why` line, `.flaw-table`, `.undefined-terms` callout, `.counterexamples` callout. No canvases, no nav bar, no back/home links, no index number in the h1. Note "differential" in counterexample 2 is emphasized with `<em>`.
- **Flaw table:** `.flaw-table` — full width, collapsed borders, 0.88em; `th` background `#f0f4f8`, text `#1a5276`, `1px solid #e0e0e0` borders, padding 8px 12px; `td` same border/padding, top-aligned; even rows background `#fafcfe`. Columns: # / Fallacy / How it hides.
- **Callouts:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22` (orange), padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60` (green), padding 8px 14px, 0.88em, with a `<ul>` (margin 4px 0 0 16px, items 3px vertical margin).
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` 0.88em `#666`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; p 0.95em `#333`; subtitle `#666` 1.0em; `strong` in `#1a5276`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Canvases (when present on sibling pages) use `window.devicePixelRatio` scaling; this page has none. In regenerated HTML, any card links use `.html` extensions.
