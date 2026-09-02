# Riding the Tide

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%; single row with multiple titled blocks and two stacked canvases)
**HTML title tag:** Riding the Tide — Common Bad Practices

**Subtitle:** Attribution Gaming — Company grows 30% organically. Claim your 2% contribution drove it.

## Section 1: The Practice

- Company in hypergrowth (30% YoY). Your team's contribution: marginal (maybe 2% if you're generous).
- "We drove 30% growth" is technically defensible — you DID contribute to the system that grew.
- Attribution is so diffuse that nobody can disprove your specific contribution's magnitude. Everyone claims the whole wave.

## Section 2: The Join Trick

- Join a winning team right before their big launch ships. Be present during success.
- "We shipped X" — where "we" includes you despite 3 weeks of contribution to a 12-month project. Proximity to success = credit.

## Section 3: The Macro Trick

- Your team's metric goes up 20%. Your work contributed maybe 5% of that. Market conditions, other teams' work, and organic growth caused 95%.
- But you present: "Our initiative drove +20%." Nobody can isolate your specific causal contribution because attribution is inherently ambiguous.

## Section 4: Variant — Tail-Coating Reports

- Put your team's work in the same quarterly report as the high-performing team. Present together. Association = shared credit.

**Why it persists:** In complex systems, causal attribution is genuinely difficult. Everyone claims a piece. Total claimed credit always exceeds 100% because NOBODY claims 2% — everyone claims the full outcome. And nobody can technically disprove it.

**The tell:** Sum all teams' claimed impact. If it exceeds 100% of actual growth (it always does), credit inflation is happening. Ask: "if YOUR team didn't exist this quarter, how much would the number change?" If the honest answer is "barely" — the claim is riding the tide.

### Visualization (canvas `c1`, 720×340)

Ocean-wave illustration: a large tide wave carrying four small boats, each claiming credit for the wave.

- **Background:** full-canvas light blue `#f0f8ff`.
- **Wave:** filled wave surface computed as `y = 60 + sin(x*0.008+0.5)*30 + sin(x*0.02)*10 + (x/w)*20` across the full width; area under it filled with a vertical gradient from `rgba(26,82,118,0.4)` (top) to `rgba(26,82,118,0.15)` (bottom); wave outline stroked `#1a5276` width 2.5.
- **Wave label (bold 16px `#1a5276`, centered at y≈200):** "30% organic growth — the tide".
- **Boats:** four small boats riding the wave surface at x = 120, 280, 440, 600 (Team A–D). Each boat: orange (`#e67e22`) trapezoid hull, thin dark (`#333`) mast, red (`#e74c3c`) triangular flag, and bold 17px red label above: '"We caused this!"'.
- **Caption (bottom center, italic 14px `#555`):** "Everyone claims to be the wind. Nobody is the ocean."

### Visualization (canvas `c2`, 720×300)

Two pie charts side by side: what actually drove growth vs the claim.

- **Background:** full-canvas `#f9f9f9`; pie radius 60; left pie centered at (0.3w, 0.45h), right pie at (0.7w, 0.45h).
- **Left pie — title (bold 16px `#1a5276`, above):** "What Actually Drove 30% Growth". Slices (starting at 12 o'clock, white 2px separators): Market conditions 40% `#1a5276`; Product-market fit 30% `#27ae60`; Other teams 20% `#e67e22`; Your team 10% `#e74c3c`. Legend below the pie, 10px color swatches + 16px `#333` labels with percentages: "Market conditions (40%)", "Product-market fit (30%)", "Other teams (20%)", "Your team (10%)".
- **Right pie — title (bold 16px `#e74c3c`, above):** "The Claim". Single full circle in `#e74c3c` with white 2px outline; white bold 17px two-line label on the pie: '"We drove' / '30% growth"'. Below the pie, 16px `#333` note: "Your team: 100%".
- **Caption (bottom center, italic 14px `#555`):** "In a rising tide, everyone claims to be the wind."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table with a single `<tr>`; left `<td>` (40%) holds four `.obj-title` blocks (The Practice, The Join Trick, The Macro Trick, Variant — Tail-Coating Reports — the latter three with `style="margin-top:14px;"`) each followed by its bullet list, then the two closing `<p>` paragraphs (**Why it persists** / **The tell** with `strong` lead-ins); right `<td>` (60%, centered) holds canvases `c1` and `c2` stacked.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; gray text `#666`/`#555`/`#333`; bar/wave fill family `rgba(26,82,118,0.35)`-style translucent blue.
- **Note:** in regenerated HTML, any card links use `.html` extensions (this page has none).
