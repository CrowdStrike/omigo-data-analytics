# Data Access Gatekeeping

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Data Access Gatekeeping — Common Bad Practices

**Subtitle:** Technical Sabotage — Control the credentials. Make access requests require "governance review" that takes weeks. Competitors' projects stall waiting for data.

## The Practice

- You control access to a key dataset (customer events, production logs, feature store). Another team's project needs it.
- Never say no (that's visible sabotage). Instead: "We need to go through the data governance process first." (+2 weeks)
- "Can you fill out this data access request form?" (+1 week). "Security needs to review." (+2 weeks). "Let me check with legal." (+2 weeks).
- Their project is blocked. Yours (which already has access) ships first.

## The Feature Store Variant

- You own the feature engineering pipeline. Competitor team requests a new feature be added.
- "That's a great idea! Let me add it to the backlog." Never prioritize it.
- Or: "Sure, I can add it in 6 weeks." Their model training is blocked. Your model ships using features you already built.

## The Schema Documentation Trap

- Dataset schema is complex and poorly documented (by design). Only you understand what fields mean.
- Competitor team gets access but can't use it effectively. "What does user_status=3 mean?" "Is event_time UTC or local?"
- You answer slowly or incompletely. They make mistakes, waste time debugging. You're months ahead.

## The Sampling "Help"

- Grant access but only to a 1% sample "to start." Full access requires another review (+3 weeks).
- Their exploratory analysis on sampled data is misleading. Rare events don't appear. Model trained on sample performs poorly.
- By the time they get full access, your model is in production.

**Why it persists:** Data governance and security reviews are LEGITIMATE processes. Using them as delay mechanisms is invisible as sabotage. "I'm just following the policy" is impossible to argue against.

**The tell:** Track time-to-access for teams that compete with the gatekeeper vs teams that don't. If competitor requests take 6 weeks and friendly requests take 2 days, it's strategic gatekeeping, not policy compliance.

**The calendar trap:** In data work, delays are fatal. Models require months of training data. Granting access in "6 weeks" when the project deadline is 8 weeks kills the project while looking cooperative.

### Visualization (canvas `c1`, 720×380)

Two-lane timeline comparison: gatekeeper's project vs competitor's project blocked by sequential review gates. Background `#f9f9f9`. Margins: left 30, right 20, top 40, bottom 30; two lanes 50px tall with a 20px gap.

- **Top lane label (bold 13px `#27ae60`, left-aligned):** "Gatekeeper project (already has data access):". Solid green line (`#27ae60`, width 3) covering the first 30% of the plot width, followed by bold 13px green text: "✓ Model shipped in 8 weeks".
- **Bottom lane label (bold 13px `#e74c3c`, left-aligned):** "Competitor project (needs data access):".
- **Delay gates (5 sequential segments on a 16-week scale):** "Access request form" +1wk, "Governance review" +2wk, "Security review" +2wk, "Legal check" +2wk, "Finally got access" +1wk. Each segment is a gray line (`#999`, width 2) along the lane center; at each gate end a small vertical marker box 16px wide, fill `rgba(231,76,60,0.1)`, stroke `#e74c3c` width 1; multi-line 9px red gate labels below the lane; "+Nwk" annotations in 9px `#999` above each segment midpoint.
- **After the last gate:** dashed gray line (dash 3/3) continuing to the right edge; right-aligned bold 12px red two-line annotation: "Got access at week 8." / "Not enough time to train model."
- **Bottom caption (italic 13px `#555`, centered):** `Data access delayed = project killed. Each gate is "legitimate." Together they're fatal.`

### Visualization (canvas `c2`, 720×320)

Strip plot (dot plot) of access-request turnaround times: every request to the same gatekeeper as one dot on a days-to-grant axis, in two lanes by requester team. Background `#f9f9f9`.

- **Title (bold 14px `#1a5276`, top center):** "Access-Request Turnaround, by Requester Team".
- **Layout:** margins left 170, right 30, top 45, bottom 70; x-scale 0–50 days over the plot width; two lanes centered at y=100 ("Allied teams") and y=195 ("Competing teams").
- **Axis:** horizontal baseline (`#999`, width 1) at y=250 with ticks every 10 days, 11px `#666` tick labels below; light vertical gridlines `#e5e5e5` at each tick spanning the lane band; axis label (11px `#666`, centered): "Days from request to access granted".
- **Deterministic dots (NO Math.random):**
  - Allied-team requests (15 dots), days-to-grant: `[1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 4, 4, 5, 6, 7]` — fill `rgba(39,174,96,0.75)`, stroke `#27ae60`, radius 6.
  - Competing-team requests (15 dots), days-to-grant: `[14, 18, 22, 26, 28, 30, 32, 33, 35, 36, 38, 40, 42, 44, 45]` — fill `rgba(231,76,60,0.75)`, stroke `#e74c3c`, radius 6.
  - Vertical jitter within each lane: `y = laneY + Math.sin(i * 2.399) * 13` (deterministic, index-based).
- **Lane labels:** bold 12px, right-aligned at x=158, in the lane color: "Allied teams" (`#27ae60`), "Competing teams" (`#e74c3c`).
- **Median markers:** vertical tick (lane color, width 2.5) from laneY−22 to laneY+22 at the lane median (allied 3 days, competing 33 days); bold 11px label above each tick: "median 3d" / "median 33d".
- **Insight annotation (bold 14px `#e74c3c`, centered near bottom):** "Competing teams wait 11× longer for the same dataset".
- **Bottom caption (italic 13px `#555`, centered):** "Same gatekeeper, same dataset, different treatment. Strategic delay, not policy."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title` headings + bullet lists + closing `<p><strong>` paragraphs, right `<td>` (60%, centered) holds the two canvases stacked.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276` (subsequent ones get inline `margin-top:14px`); `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display:block; margin:0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#555`/`#333`.
- **Links:** this page has no card links; in regenerated HTML any card links elsewhere use `.html` extensions.
