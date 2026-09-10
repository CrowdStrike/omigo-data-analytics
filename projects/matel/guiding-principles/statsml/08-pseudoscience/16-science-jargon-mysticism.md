# Science-Jargon Mysticism

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Science-Jargon Mysticism — Pseudoscience in Data Analysis

**Subtitle:** Complexity/Quantum/AI Washing: Borrowing Scientific Authority via Terminology

## Section 1: Complexity / Quantum / AI Washing — Borrowing Scientific Authority via Terminology

- **"Quantum" anything:** Quantum healing, quantum finance, quantum leadership — prestige borrowed from physics with zero connection to quantum mechanics. The word signals "too complex for you to question," which is the point of a thought-terminating label.
- **"AI-powered":** The product runs if-else logic or simple regex, and "our AI analyzes your data" means someone wrote a SQL query. Ask "how does it work?" and "it's AI!" ends the conversation.
- **"Proprietary algorithm":** Often means linear regression with 3 features. The label implies sophistication, prevents verification, and justifies premium pricing — opacity passing as authority.
- **"Data-driven":** Often means someone looked at a dashboard once, then chose the metric that confirmed a pre-existing preference. The word "data" confers scientific legitimacy on what is actually opinion with a graph.
- **"Synergy / paradigm shift / disruption":** Buzzwords that sound analytical but carry zero information. Strip them out and usually nothing remains.

**Why it's pseudoscience:** Real science invites scrutiny ("here's my method, replicate it") while jargon mysticism deflects it ("too complex to explain, trust me"). If they won't explain the mechanism in plain language, they either don't understand it or it doesn't exist.

### Visualization (canvas `c1`, 720×340)

Two-column text mapping diagram: buzzword vs what it really is.

- **Title (bold 17px, `#1a5276`, top center):** "Jargon Creates an Authority Barrier: \"Too Complex to Question\"".
- **Column headers (bold 17px, left-aligned):** "What they SAY:" in orange `#e67e22` at x=60, y=50; "What it IS:" in green `#27ae60` at x=420, y=50.
- **Rows** (5 rows, starting y=70, 32px row spacing; left cell is the quoted buzzword in `#e67e22` at x=60, right cell is "→  reality" in `#333` at x=420, both 17px):
  1. "AI-powered" → if-else statement
  2. "Proprietary algorithm" → linear regression
  3. "Quantum optimization" → random search
  4. "Data-driven decision" → looked at a dashboard once
  5. "Synergistic paradigm shift" → (contains zero information)
- **Bottom takeaway (bold 17px red `#e74c3c`, centered at h-8):** "If they won't explain it in plain language → they don't understand it or it doesn't exist."

### Visualization (canvas `c2`, 720×300)

Diagram: a large dark "complexity shield" rectangle hiding a tiny code box.

- **Title (bold 17px Arial, `#2c3e50`, top center):** "The Complexity Shield".
- **Shield:** filled rectangle `#2c3e50` at x=150, y=35, 200×140. Inside, centered bold 18px `#ecf0f1` labels stacked at 26px spacing: "AI-Powered", "Quantum ML", "Proprietary", "Synergy Engine", "Blockchain".
- **Real thing box:** rectangle at x=450, y=70, 180×50, fill `#f9f9f9`, stroke `#27ae60` width 2; inside, centered bold 16px monospace green `#27ae60` text: `if x > 5: return true`.
- **Shield caption (16px Arial red `#e74c3c`, centered under shield, two lines):** "This exists to PREVENT you" / "from asking what is behind it".
- **Box caption (16px Arial green `#27ae60`, centered under the box, two lines):** "This is what is" / "actually there".
- **Connector:** dashed gray `#95a5a6` line (dash 4/3, width 1) from the shield's right-middle to the box's left-middle, ending in a small filled gray arrowhead pointing at the box.

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title` + bullet list + closing "Why it's pseudoscience" paragraph, right `<td>` (60%, centered) holds two stacked canvases (`c1` 720×340, `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; p `#333` 0.95em; ul 0.9em `#333`, li margin 6px 0; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark slate `#2c3e50`, gray `#95a5a6`/`#666`/`#333`.
- In regenerated HTML, any card links use `.html` extensions.
