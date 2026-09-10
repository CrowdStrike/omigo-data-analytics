# Rephrasing Until AI Gives the Answer You Want

**Page type:** detail page (two card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Rephrasing Until AI Gives the Answer You Want

**Subtitle:** Tried multiple phrasings until AI confirmed your bias — you p-hacked the AI

## Anti-Pattern

- AI disagrees with your conclusion → rephrase with leading framing → AI agrees
- "Given that X is clearly related to Y, explain why..." — now AI confirms
- Multiple attempts until one confirms your bias — same as p-hacking
- You are training the AI to tell you what you want to hear

**Key-point box:** **Domain:** Prompt engineering for confirmation, iterative rephrasing

*Example: "Is caffeine intake related to exam scores?" → AI says "unlikely." You rephrase: "Given that caffeine clearly boosts cognition, explain the mechanism..." → AI obliges.*

### Visualization (canvas `c1`, 720×300)

Stacked attempt boxes showing escalating prompt bias until the AI agrees, with the final attempt flagged as cherry-picked.

- **Three attempt rows,** each a 620×60 box starting at (50, 20) with 18px vertical gap, stroke `#ccc` width 1:
  1. Label "Attempt 1 (neutral):" — prompt `"Is X related to Y?"` — response `AI: "Likely not, because..."` — mark "✗" in `#e74c3c`, box fill `#fdf8f8`.
  2. Label "Attempt 2 (leading):" — prompt `"Explain why X causes Y"` — response `AI: "Well, one could argue..."` — mark "~" in `#e67e22`, box fill `#fef9f3`.
  3. Label "Attempt 3 (very leading):" — prompt `"Given X clearly drives Y..."` — response `AI: "Yes! X drives Y because..."` — mark "✓" in `#27ae60`, box fill `#f3fdf7`.
- **Row text styling:** label bold 12px `#1a5276` at left; prompt 12px `#2c3e50` at x offset +180; response 12px `#555` on second line; mark bold 22px right-aligned in the row's color.
- **Highlight:** red `#e74c3c` dashed (6/4) rectangle width 3 drawn around attempt 3 (5px margin), with "← Cherry-picked!" bold 13px `#e74c3c` to its right.
- **Bottom labels (centered):** "P-HACKED THE AI" bold 15px `#e74c3c` at h−30; "Kept rephrasing until confirmation bias was satisfied" 12px `#666` at h−12.

## Design Pattern

- Ask ONCE with neutral framing
- If AI disagrees: that's MORE informative than agreement — investigate why
- First answer (before you inject bias) is the least contaminated
- AI disagreement = signal to check your assumption, not signal to rephrase
- Record the first response as your baseline — do not overwrite it

**Key-point box:** **Rule:** The first neutral-framed response is the most valuable. Disagreement is information, not failure.

*Workflow: Ask neutral question → AI disagrees → "Why does it disagree? What evidence contradicts my assumption?" → revise YOUR hypothesis, not the prompt.*

### Visualization (canvas `c2`, 720×300)

Flowchart: neutral prompt → AI disagrees → green "investigate" path vs crossed-out red "rephrase until agrees" path.

- **Neutral prompt box:** rect (40, 40) size 200×55, fill `#f0f4f8`, stroke `#1a5276` width 2. Centered at x=140: "Neutral Prompt" bold 12px `#1a5276`; `"Is X related to Y?"` 11px `#555`.
- **Arrow:** `#2c3e50` width 2 from (240, 67) to (295, 67) with filled arrowhead.
- **AI response box:** rect (300, 40) size 220×55, fill `#fef9f3`, stroke `#e67e22` width 2. Centered at x=410: "AI Response (disagrees)" bold 12px `#e67e22`; `"Unlikely — because..."` 11px `#555`.
- **Green path (down):** `#27ae60` width 3 arrow from (410, 95) to (410, 140). Green box rect (295, 145) size 230×55, fill `#d4efdf`, stroke `#27ae60` width 2. Centered at x=410: "Investigate WHY it disagrees" bold 12px `#27ae60`; "✓ Revise YOUR hypothesis" 11px `#555`.
- **Red path (right):** `#e74c3c` dashed (4/3) line width 2 from (520, 67) to (580, 67). Red box rect (560, 40) size 140×55, fill `#fdeaea`, stroke `#e74c3c` width 2, text 11px `#e74c3c` centered at x=630, two lines: "Rephrase until" / "agrees". Big red X width 4 crossed over the box, (565, 44)→(695, 91) and (695, 44)→(565, 91).
- **Bottom labels (centered):** "First answer = least contaminated" bold 14px `#1a5276` at h−40; "Disagreement = information" 13px `#27ae60` at h−20.

## Regeneration instructions

- **Layout:** two `.card-section` blocks ("Anti-Pattern", "Design Pattern"), each an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) followed by a `table.layout` with a single `<tr>`: left `td.text-col` (45%) holds a `<ul>`, a `.key-point` div, and a `.example` paragraph; right `td.viz-col` (55%) holds the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem. No nav bar, no back/home links.
- **Canvas:** `<canvas id="cN" height="300">` styled `width: 100%`, border `1px solid #e0e0e0`, radius 4px; drawn at intrinsic 720×300 and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; grays `#666`/`#555`/`#333`; dark text `#2c3e50`.
- Note: in regenerated HTML, any card links would use `.html` extensions.
