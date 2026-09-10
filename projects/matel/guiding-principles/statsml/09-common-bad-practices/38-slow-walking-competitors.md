# Slow-Walking Competitors

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Slow-Walking Competitors — Common Bad Practices

**Subtitle:** Institutional Sabotage — Never say no. Just make yes take so long the window closes.

## The Practice

- Another team's project threatens your budget, relevance, or headcount. You never BLOCK it (that's visible and politically risky).
- Instead: "Can we get alignment on the data schema first?" (+3 weeks). "I have concerns about security — let's schedule a review." (+2 weeks). "Can we see the cost analysis?" (+2 weeks). "Marketing should weigh in." (+2 weeks).
- Death by process. You look collaborative. The project dies of delays.

## The Helper Trap

- Volunteer to "help" on their project — then become a bottleneck.
- Your review is always "almost done." Your feedback arrives the day before their deadline. Your "concerns" require meetings that can only be scheduled 3 weeks out.
- You look engaged and thorough. They're stuck.

## Variant — Standards Escalation

- "Before we can approve this, we need a design review. And a security review. And a privacy assessment. And legal sign-off."
- Each gate adds 2-4 weeks. The gates are all individually "reasonable." Collectively they're a kill mechanism.

## Variant — Information Requests

- "Can you provide: impact analysis, rollback plan, stakeholder list, risk assessment, compliance check?"
- Each document takes a week to produce. By the time everything's ready, the window of opportunity closed or leadership attention moved elsewhere.

**Why it persists:** Process-based delays are invisible as sabotage because they LOOK like diligence. "I just want to make sure we do this right" is impossible to argue against without looking reckless. The intent (kill through delay) hides behind the action (asking reasonable questions).

**The tell:** Track time-to-decision for projects that compete with the "helper's" work vs ones that don't. If they're only "thorough" about competitors' projects, it's strategic delay, not diligence.

### Visualization (canvas `c1`, 720×340)

Two-lane timeline comparison: direct path vs the same project subjected to "helpful" process gates. Background `#f9f9f9`. Margins: left 30, right 20, top 40, bottom 30; two lanes 50px tall with a 20px gap.

- **Top lane label (bold 14px `#27ae60`, left-aligned):** "Direct path: ship in 4 weeks". Solid green line (`#27ae60`, width 3) covering the first 25% of the plot width, ending in bold 13px green "✓ Ship".
- **Bottom lane label (bold 14px `#e74c3c`, left-aligned):** `Same project with "helpful" process:`.
- **Delay gates (5 sequential segments on a 16-week scale):** "Alignment meeting" +3wk, "Security review" +2wk, "Cost analysis" +2wk, "Marketing weigh-in" +2wk, "Legal sign-off" +2wk. Each segment is a gray line (`#999`, width 2) along the lane center; at each gate end a small vertical marker box 16px wide, fill `rgba(231,76,60,0.1)`, stroke `#e74c3c` width 1; multi-line 9px red gate labels below the lane; "+Nwk" annotations in 9px `#999` above each segment midpoint.
- **After the last gate:** dashed gray line (dash 3/3) continuing to the right edge; right-aligned bold 13px red two-line annotation: "Window closed." / "Project killed."
- **Bottom caption (italic 14px `#555`, centered):** `Murder by process. Each delay is "reasonable." Together they're fatal.`

### Visualization (canvas `c2`, 720×300)

Single stacked horizontal bar showing the cumulative cost of each "reasonable" request. Background `#f9f9f9`.

- **Title (bold 16px `#1a5276`, top center):** `Cost of Each "Reasonable" Request`.
- **Layout:** margins left 120, right 20; bar height 40; x-scale 15 weeks total over the bar width.
- **Segments (left to right):** "Original plan" 4wk `#27ae60`; "Alignment meeting" 3wk `#e67e22`; "Security review" 2wk `#e74c3c`; "Cost analysis" 2wk `#c0392b`; "Marketing sign-off" 2wk `#d35400`; "Legal" 2wk `#922b21`. Wide segments (>60px) get white bold 11px two-line inner labels: name + "(+Nw)"; narrow segments get the name rotated 90° in white bold 9px.
- **Bar border:** `#333`, width 1.5, around the whole stacked bar.
- **Deadline markers:** vertical dashed green line (`#27ae60`, dash 4/4, width 2) at week 4 with bold 12px green labels "original" / "deadline" above and "(4 wks)" below; vertical dashed red line (`#e74c3c`) at week 15 with bold 12px red labels "actual" / "finish" above and "(15 wks)" below.
- **Bottom caption (italic 14px `#555`, centered):** `Each gate: 2 weeks. Each is "reasonable." Together: lethal.`

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title` headings + bullet lists + closing `<p><strong>` paragraphs, right `<td>` (60%, centered) holds the two canvases stacked.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276` (subsequent ones get inline `margin-top:14px`); `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display:block; margin:0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, darker reds `#c0392b`/`#d35400`/`#922b21`, gray text `#666`/`#555`/`#333`.
- **Links:** this page has no card links; in regenerated HTML any card links elsewhere use `.html` extensions.
