# Social Media Priors

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** Social Media Priors

**Subtitle:** Likes mostly measure how many followers an account has. The content contributes the scatter, not the level.

**Intro callout:** Engagement counts arrive with a built-in prior: audience size sets the level, content only adds scatter around it. Raw counts and per-follower rates each hard-code a wrong exponent for the audience effect — the honest move is to estimate it.

## 1. Audience Sets the Level

Followers see a post by default and many engage out of habit. That baseline arrives before content quality has any effect, so raw counts across accounts of different sizes are comparing audiences.

- The fitted line is audience size. The vertical spread around it is the post.
- Only the deviation from the line says anything about the content.

**Key point:** Ranking by raw likes ranks by follower count with noise added.

### Visualization (canvas `c1`, 720×320)

Log-log scatter plot of likes vs followers with a fitted line and a deviation callout.

- **Title (bold 16px, `#1a5276`, top center):** "Likes vs Followers (log-log)".
- **Plot area:** x=80, y=46, width = canvas−140, height = canvas−108; L-shaped axes in `#95a5a6` (1.4px).
- **Scales:** x from log 2 to log 6, tick labels "10², 10³, 10⁴, 10⁵, 10⁶"; y from log 0 to log 4, tick labels "10⁰, 10¹, 10², 10³, 10⁴" (12px `#5a6875`).
- **Axis labels (13px `#4a5866`):** "Followers" centered below x-axis; "Likes" rotated −90° at x=22.
- **Scatter:** 130 points from a seeded LCG (seed 7): lx uniform in [2,6], ly = 0.75·lx + noise where noise = (sum of 3 uniforms − 1.5)·0.34; points outside [0,4] skipped; 3.5px dots filled `rgba(26,82,118,0.45)`.
- **Fit line:** y = 0.75·x drawn across the full x-range, stroke `#e74c3c` 3px, labeled "audience size" in 13px `#e74c3c` (left-aligned near x=4.2, 30px below the line).
- **Deviation marker:** at x=4.9, dashed (4/4) vertical green `#27ae60` segment (2.4px) from the line up 0.6 log-units, labeled "the post" in `#27ae60` at its top right.

## 2. Dividing by Followers Overshoots

The obvious fix is likes per follower. But likes grow *sublinearly* with audience — big accounts accumulate inactive followers and reach a smaller fraction of them.

- Fit `likes ≈ followers^b` and you get `b < 1`. The rate metric assumes `b = 1`.
- So engagement rate falls as accounts grow, and ranking by it just flips the bias toward small accounts.

**Key point:** Raw count assumes `b = 0`, rate assumes `b = 1`. Neither is neutral — estimate `b` instead of picking a side.

### Visualization (canvas `c2`, 720×320)

Curve chart: engagement rate (likes per follower) declining with account size vs the flat line the rate metric assumes.

- **Title (bold 16px, `#1a5276`, top center):** "Likes per Follower Falls as Accounts Grow".
- **Plot area:** x=80, y=50, width = canvas−160, height = canvas−112; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** x from log 2 to log 6, tick labels "10², 10³, 10⁴, 10⁵, 10⁶" (12px `#5a6875`); y unlabeled, rate(lx) = 10^(−0.25·lx) normalized so rate at x0 sits at 90% of plot height.
- **Axis labels (13px `#4a5866`):** "Followers" below; "Likes / followers" rotated −90° at x=22.
- **Observed curve:** rate(lx) drawn in 100 steps, stroke `#e74c3c` 3px, labeled "observed (b ≈ 0.75)" in 13px `#e74c3c` near x=2.9, 16px above the curve.
- **Assumed line:** horizontal dashed (6/5) green `#27ae60` line (2.4px) at 18% of the top rate, labeled "what the rate metric assumes (b = 1)" in `#27ae60`, 12px above it near x=3.2.

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 45% and `td.viz-col` 55%, both `vertical-align: top`, 12px padding. No index number in the h1.
- **Text blocks:** intro `<p>` (with `<em>` for "sublinearly"), `<ul>` bullets (0.92rem), inline `<code>` (background `#e8f0f8`, color `#1a5276`, 2px 5px padding, 3px radius) for formulas like `likes ≈ followers<sup>b</sup>` and `b = 1`, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; scatter fill `rgba(26,82,118,0.45)`; gray labels `#5a6875`/`#4a5866`, axes `#95a5a6`.
- **Canvas:** intrinsic 720×320; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), `ctx.scale` back to logical coordinates via a shared `setupCanvas(id)` helper; deterministic scatter via a seeded LCG.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
