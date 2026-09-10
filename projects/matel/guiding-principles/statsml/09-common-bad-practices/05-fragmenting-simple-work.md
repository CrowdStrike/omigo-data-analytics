# Fragmenting Simple Work

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Fragmenting Simple Work — Common Bad Practices

**Subtitle:** Manufactured Win — One improvement, five PRDs, five 'launches,' five impact claims.

## Section 1: The Practice

- One straightforward improvement (add a cache layer). Split into: (1) "Design caching architecture" (2) "Implement cache for Service A" (3) "Extend to Service B" (4) "Cache monitoring and observability" (5) "Cache hit rate optimization." Total value: one thing. Claimed launches: five.
- **Performance review version:** Each fragment gets its own impact statement. "Shipped 5 projects this half, each improving latency by 10-40%." Reality: one project that improved latency once.
- **Variant — document inflation:** One research finding split into 3 blog posts + 1 internal doc + 1 presentation. "5 knowledge-sharing artifacts produced this quarter!"
- **Variant — incremental rollout as launches:** Roll out to 10%, 25%, 50%, 100% — each percentage is a "launch" with its own metrics claim. Same code, four "launches."

**Why it persists:** Promo committees count LAUNCHES not impact-per-launch. More launches = more evidence of output. Combining them into one reduces the number of "stories" you can tell. Volume of narratives > magnitude of actual impact.

**The tell:** Could all these have been ONE project? If someone else could have done it as a single PR (or 2-3 PRs), it was fragmented for optics. Ask: "what's the total net impact of all 5?" — if it equals one thing, it was one thing.

### Visualization (canvas `c1`, 720×340)

Gantt chart: the real work is one short green bar; five staggered red "launch" bars stretch the story across ten weeks.

- **Title (bold 16px `#1a5276`, centered at y=22):** `One project, five "launches"`.
- **X-axis week grid:** weeks 0-10 mapped as `x = 30 + 66*week` (x from 30 to 690). Vertical gridlines `#e0e0e0` width 1 from y=40 to y=282 at every week; week tick labels "0".."10" in 12px `#666`, centered under each gridline at y=298.
- **Top lane (green, the actual work):** bar from week 0 to week 1, y=48, height 26; fill `rgba(39,174,96,0.35)`, stroke `#27ae60` width 2. Label to the right of the bar (left-aligned at bar end + 8px, bold 13px `#27ae60`, vertically centered in lane): "the actual cache layer (all the work)".
- **Five red launch lanes:** bars each spanning exactly 2 weeks, start weeks [0, 2, 4, 6, 8], lane tops y = 90 + i*38 (90, 128, 166, 204, 242), height 26; fill `rgba(231,76,60,0.15)`, stroke `#e74c3c` width 1.5. Inside each bar at left (x-start + 8, 12px `#333`): "PRD 1".."PRD 5".
- **Star markers:** at the right end of each red bar, a filled 5-point orange (`#e67e22`) star, outer radius 7, centered vertically in the lane; the word "launch" in 11px `#e67e22`, right-aligned 12px left of the star center.
- **Annotation (bold red 16px `#e74c3c`, centered at y=326, the only annotation):** `Work finished week 1. Launch count: 5.`

### Visualization (canvas `c2`, 720×300)

Paired bar groups: two engineers over one year — launch counts diverge wildly, total impact is identical.

- **Title (bold 16px `#1a5276`, centered at y=20):** "Same value, different launch counts".
- **Legend (one row):** blue swatch 14×14 at (140,34) + 12px `#333` text "Engineer A — one project" at x=160; orange swatch 14×14 at (390,34) + 12px `#333` text "Engineer B — five fragments" at x=410 (text baselines y=45). Engineer A bars `#1a5276`, Engineer B bars `#e67e22`, both filled at full color with no stroke.
- **Shared scale:** baseline axis line `#999` width 1 from x=90 to x=630 at y=230; 15px per unit (value 10 = 150px tall bar). Bar width 60.
- **Left group — "launches counted":** group center x=200; Engineer A bar at x=130 value 1 (height 15), Engineer B bar at x=210 value 5 (height 75). Group label "launches counted" 13px `#555`, centered at (200, 252).
- **Right group — "total impact delivered":** group center x=520; Engineer A bar at x=450 value 10 (height 150), Engineer B bar at x=530 value 10 (height 150) — identical bars. Group label "total impact delivered (units)" 13px `#555`, centered at (520, 252).
- **Value labels:** bold 13px in each bar's color, centered 6px above each bar top: "1", "5", "10", "10".
- **Annotation (bold red 16px `#e74c3c`, centered at y=286, the only annotation):** "Review counts the left chart. Value lives in the right one — identical."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` with left `<td>` (40%) holding `.obj-title` "The Practice" + bullets/paragraphs, right `<td>` (60%, centered) holding both canvases stacked (`c1` 720×340 above `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; p `#333` 0.95em; ul 0.9em `#333`, li margin 6px 0; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#555`/`#999`.
