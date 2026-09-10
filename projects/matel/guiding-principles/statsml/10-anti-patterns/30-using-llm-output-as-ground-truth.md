# Using LLM Output as Ground Truth

**Page type:** detail page (two card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Using LLM Output as Ground Truth

**Subtitle:** Output LOOKS authoritative (confident language, proper formatting) but may be completely fabricated

## Anti-Pattern

- "Claude said p-value should be 0.03" → used directly without verification
- LLMs hallucinate statistics, invent citations, confuse formulas
- The output looks authoritative: confident language, proper formatting
- No uncertainty markers — AI presents guesses with same confidence as facts

**Key-point box:** **Domain:** AI-assisted analysis, code generation, statistical interpretation

*Example: AI confidently states "Shapiro-Wilk W = 0.94, p = 0.031" — you use it in your report. The actual computation gives p = 0.23.*

### Visualization (canvas `c1`, 720×300)

Diagram: an authoritative-looking AI output box revealed as a hallucination by the actual computation below it.

- **Top box (AI output):** rect at (40, 20) size 640×100, fill `#f0f4f8`, stroke `#1a5276` width 2. Label "AI Output:" bold 14px `#1a5276` at (60, 45). Three lines of 13px monospace `#2c3e50`: "Shapiro-Wilk test: W = 0.941, p = 0.031" (60, 70); "Result: Significant at α = 0.05. Reject H₀ (non-normal)." (60, 90); "Recommendation: Use non-parametric alternative." (60, 110).
- **False-confidence mark:** "✓ Confident" bold 16px `#27ae60` at (580, 45).
- **Arrow:** red `#e74c3c` width 3 down from (360, 125) to (360, 155) with arrowhead.
- **Bottom box (reveal):** rect at (40, 160) size 640×90, fill `#fdf0f0`, stroke `#e74c3c` width 2. Label "ACTUAL Computation:" bold 14px `#e74c3c` at (60, 185). Two lines of 13px monospace `#2c3e50`: "Shapiro-Wilk test: W = 0.967, p = 0.230" (60, 210); "Result: NOT significant. FAIL to reject H₀ (data is normal)." (60, 230).
- **Big red X:** two `#e74c3c` strokes width 5 crossing over the right end of the bottom box, from (590, 170) to (650, 240) and (650, 170) to (590, 240).
- **Bottom labels (centered at x=360):** "HALLUCINATED STATISTIC" bold 15px `#e74c3c` at y=278; "AI invented the numbers — wrong conclusion, wrong recommendation" 12px at y=295.

## Design Pattern

- LLM output = hypothesis to verify, NEVER ground truth
- AI generates analysis → run the ACTUAL computation to verify
- AI suggests a test → check the assumptions yourself
- AI cites a paper → look it up
- Treat like a junior analyst's first draft: probably directionally right, possibly wrong in details

**Key-point box:** **Rule:** Every AI-generated number must be independently computed before use.

*Workflow: AI suggests "use Mann-Whitney U" → verify your data actually violates normality → run the test yourself → compare result to AI's claim.*

### Visualization (canvas `c2`, 720×300)

Flowchart: AI output treated as hypothesis, verified by actual computation, compared, then used or discarded.

- **Title (centered at w/2):** "Verify EVERY Claim" bold 15px `#1a5276` at y=30; subtitle "LLM output is a starting hypothesis — never the final answer" 12px `#666` at y=50.
- **Box 1:** rect (20, 125) size 150×50, fill `#f0f4f8`, stroke `#1a5276` width 2. Text centered at x=95: "AI Output" bold 12px `#1a5276`; "(hypothesis)" 11px `#666`.
- **Arrow 1:** dark `#2c3e50` width 2 from x=170 to x=210 at box mid-height, filled arrowhead.
- **Box 2:** rect (215, 125) size 170×50, fill `#e8f8f0`, stroke `#27ae60` width 2. Text bold 12px `#27ae60` centered at x=300, two lines: "Run ACTUAL" / "Computation".
- **Arrow 2:** `#2c3e50` width 2 from x=385 to x=430 at mid-height.
- **Decision diamond:** centered at (490, 150), half-width 45, half-height 30, fill `#fff8e8`, stroke `#e67e22` width 2, label "Compare" bold 12px `#e67e22`.
- **Green path (right):** `#27ae60` width 2 arrow from diamond right point to x=590; green box rect (595, 128) size 110×44, fill `#d4efdf`, stroke `#27ae60`. Text centered: "Match ✓" bold 13px `#27ae60`; "Use result" 11px.
- **Red path (down):** `#e74c3c` width 2 arrow from diamond bottom point down 40px; red box rect (435, 222) size 110×44, fill `#fdeaea`, stroke `#e74c3c`. Text centered at x=490: "Mismatch ✗" bold 13px `#e74c3c`; "Discard AI claim" 11px.

## Regeneration instructions

- **Layout:** two `.card-section` blocks ("Anti-Pattern", "Design Pattern"), each an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) followed by a `table.layout` with a single `<tr>`: left `td.text-col` (45%) holds a `<ul>`, a `.key-point` div, and a `.example` paragraph; right `td.viz-col` (55%) holds the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem. No nav bar, no back/home links.
- **Canvas:** `<canvas id="cN" height="300">` styled `width: 100%`, border `1px solid #e0e0e0`, radius 4px; drawn at intrinsic 720×300 and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; grays `#666`/`#555`/`#333`; dark text `#2c3e50`.
- Note: in regenerated HTML, any card links would use `.html` extensions.
