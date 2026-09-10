# Strategic Incompetence

**Page type:** detail page (two-column obj-table layout: text left ~38%, two stacked canvases right ~62%, single row)
**HTML title tag:** Strategic Incompetence — Common Bad Practices

**Subtitle:** Institutional Sabotage — Do unwanted work badly enough that someone else takes it from you.

## The Practice

- Don't want to own on-call for a legacy system? Take your rotation but respond slowly, make mistakes, escalate unnecessarily, produce sloppy runbooks.
- Eventually someone says "maybe we should reassign this to someone more suited." You "tried" but it's "not your strength." The task migrates.
- Your plate is free for promotable work.

## The Selective Competence

- You are EXCELLENT at work you want (new ML models, greenfield features, visible launches).
- You are mysteriously terrible at work you don't want (documentation, on-call, mentoring juniors, legacy maintenance).
- If competence were random, you'd be bad at random things. If it's always bad on the UNGLAMOROUS work — it's strategic.

## Variant — Meeting Incompetence

- "I'm not good at facilitating meetings." Don't improve. Others take over.
- You now never run meetings, which frees 5+ hours/week for actual work.

## Variant — Hiring Incompetence

- "I'm not great at interviewing." Do a few bad interviews. Get removed from the interview panel.
- Free from interviewing obligations.

## The Compounding Trap

- Competence at ANYTHING gets rewarded with MORE of that thing. Good at on-call → more on-call rotations. Bad at on-call → reassigned to something else.
- The system penalizes competence at unglamorous work and rewards selective failure.

**Why it persists:** The performance system can't distinguish "can't do" from "won't do well." Both look the same from outside. And reassigning based on "strengths" is considered good management — so the gamer gets what they want while LOOKING like the manager made a smart decision.

**The tell:** Map the person's competence across all task types. If they're exceptional at high-visibility work and consistently poor at low-visibility work — the pattern is strategic, not natural. True incompetence would be RANDOM across task types.

### Visualization (canvas `c1`, 780×400)

Bimodal dot plot: one person's task-quality scores, split by whether they want to keep or shed the task. Background `#f9f9f9`. X scale is quality score 0–100 mapped across margins left 50, right 30; horizontal axis line at y=330.

- **Title (bold 16px `#1a5276`, centered, y=28):** "Person A: Quality Score on Every Task, One Quarter".
- **X-axis:** `#333` width 2 line at y=330 from x=50 to x=750; ticks at 0/25/50/75/100 (6px tick marks, 12px `#333` centered labels below); axis label 15px `#1a5276` centered at y=370: "Task quality score (0 = botched, 100 = excellent)".
- **Deterministic data (hardcoded, no randomness):** shared vertical jitter array `[-12, 8, -5, 12, -9, 4, -13, 10, 0, -7]` applied to each row's baseline.
  - Keep row scores: `[86, 88, 90, 91, 92, 93, 94, 95, 96, 97]` — green `#27ae60` filled dots r=7 around baseline y=150; row label bold 15px green, left-aligned at (50, 108): "Tasks Person A wants to keep (ML models, features, launches)".
  - Shed row scores: `[14, 18, 22, 25, 27, 30, 32, 35, 38, 42]` — red `#e74c3c` filled dots r=7 around baseline y=272; row label bold 15px red, left-aligned at (50, 238): "Tasks Person A wants to shed (on-call, docs, legacy, mentoring)".
- **Insight annotation (bold red `#e74c3c`):** double-headed horizontal arrow (width 3, filled triangular heads) at y=212 spanning the empty middle from score 46 to score 82; above it, two centered lines of bold 17px red text at score 64 (y=178 and y=198): "44-point empty middle — same person" / "Random incompetence scatters. Strategic incompetence splits."
- **Bottom caption (italic 15px `#555`, centered, y=390):** "Excellent on promotable work, ‘somehow’ failing on unglamorous work — the bimodal tell."

### Visualization (canvas `c2`, 780×360)

Conceptual scatter/line plot showing the competence penalty. Background `#f9f9f9`.

- **Title (bold 16px `#1a5276`, top center):** "The Competence Penalty".
- **Axes:** L-shaped axes in `#333` width 2; margins left 100, right 80, top 60, bottom 70.
- **X-axis label (16px `#1a5276`, centered):** "Competence at unglamorous work", with "(low)" near the left end and "(high)" near the right end.
- **Y-axis label (rotated vertical, 16px `#1a5276`):** "Amount of unglamorous work assigned", with "(low)" at the bottom and "(high)" at the top.
- **Trend:** upward straight line (`#1a5276`, width 3) from lower-left to upper-right of the plot area, with 5 filled blue dots (r=6) at fractions 0.15, 0.35, 0.55, 0.75, 0.9 along it.
- **Line labels (bold 17px `#1a5276`, right of midpoint, two lines):** "Good at on-call → get MORE on-call" / "Bad at on-call → reassigned to ML work".
- **Incentive arrow:** red (`#e74c3c`, width 3) horizontal arrow near the bottom pointing LEFT (toward low competence), with filled triangular head; bold 17px red label above it, centered: "← Incentive: be LESS competent at unglamorous tasks".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (38%) holds `.obj-title` headings + bullet lists + closing `<p><strong>` paragraphs, right `<td>` (62%, centered) holds the two canvases stacked.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276` (subsequent ones get inline `margin-top:14px`); `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display:block; margin:0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, star yellow `#f1c40f`, gray text `#666`/`#555`/`#333`.
- **Links:** this page has no card links; in regenerated HTML any card links elsewhere use `.html` extensions.
