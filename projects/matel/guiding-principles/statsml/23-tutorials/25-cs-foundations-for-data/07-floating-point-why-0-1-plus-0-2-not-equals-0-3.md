# Floating Point: Why 0.1 + 0.2 ≠ 0.3

**Page type:** detail page (tutorial page: 4 card-sections, each an h2 + two-column layout table, text left 50%, canvas right 50%)
**HTML title tag:** Floating Point: Why 0.1 + 0.2 ≠ 0.3

**Subtitle:** Type 0.1 + 0.2 into Python or JavaScript and you get 0.30000000000000004 — computers write numbers in binary, and binary cannot write 0.1 exactly

## Try It: 0.1 + 0.2 on Any Computer

Tags: `core idea` (blue), `running example` (green)

- **The surprise** — Python and JS print 0.1 + 0.2 as 0.30000000000000004; R/SQL floats hide the gap
- **Not a bug** — every language follows the same standard for decimals (IEEE 754 doubles)
- **The reason** — computers store binary fractions, and 0.1 has no exact binary form
- **The analogy** — 1/3 in decimal is 0.3333… forever; 1/10 in binary is 0.00011001100… forever
- **The store** — the computer keeps the nearest 64-bit value, off by a hair

*Example:* 0.333 + 0.333 + 0.333 = 0.999 — cut 1/3 at three digits and thirds stop adding up to 1; binary does exactly this to 0.1.

**Key point:** The computer never had 0.1 — it had the closest binary number to 0.1, and the tiny gap surfaces in arithmetic.

### Visualization (canvas `c1`, 720×300)

Split-panel text diagram: decimal 1/3 on the left vs binary 1/10 on the right; vertical dashed gray divider (`#bdc3c7`, dash 4/3) at x=360.

- **Title (bold 15px, ink `#1a5276`, top center):** "Some fractions never end — in whichever base you write them".
- **Left panel (centered at x=180):** heading bold 13px blue `#2a78d6` "Decimal cannot write 1/3"; monospace 13px `#2c3e50` "1/3 = 0.3333333333…"; mute 12px "cut it at three digits:"; monospace "0.333" then "0.333 + 0.333 + 0.333"; orange `#d95926` bold 13px monospace "= 0.999  (not 1)".
- **Right panel (centered at x=540):** heading bold 13px violet `#4a3aa7` "Binary cannot write 1/10"; monospace 13px "0.1 = 0.0001100110011…"; mute 12px two lines "the pattern 0011 repeats forever;" / "a double keeps 53 binary digits:"; monospace 12px "stored 0.1 ="; orange bold 12px monospace "0.1000000000000000055511…"; mute 12px "(a hair above the real 0.1)".
- **Bottom line (ink bold 13px centered):** "same disease, different base — 0.1 is binary's 1/3".

## The Worked Example, Digit by Digit

Tags: `worked example` (green)

- **Stored 0.1** — actually 0.1000000000000000055511… (a hair high)
- **Stored 0.2** — actually 0.2000000000000000111022… (a hair high)
- **Their sum** — the two overshoots add up: 0.3000000000000000444…
- **Stored 0.3** — the nearest double to 0.3 is 0.2999999999999999889 (a hair low)
- **The verdict** — the sum and stored 0.3 differ, so `0.1 + 0.2 == 0.3` is False

*Example:* The gap is about 6 × 10⁻¹⁷ — invisible on a dashboard, fatal inside an == test.

**Key point:** Every decimal you type snaps to its nearest binary neighbor — and the sum snaps to a different neighbor than 0.3 does.

### Visualization (canvas `c2`, 720×300)

Zoomed number-line diagram around 0.3.

- **Title (bold 15px, `#1a5276`, top center):** "Zooming in around 0.3 (gaps magnified ~10¹⁶×)".
- **Header lines (monospace 12px, left-aligned at x=70):** "stored 0.1 = 0.1000000000000000055511…  (high)" and "stored 0.2 = 0.2000000000000000111022…  (high)".
- **Number line:** horizontal mute `#6b7280` line at y=190 from x=80 to x=640.
- **Marks:** dashed mute vertical line at center x=360 labeled below in mute 12px "true 0.3 (not representable)"; green `#008300` tick (width 2.5) at x=330 (offset from true 0.3 in the real ~1:4 ratio vs the sum) labeled bold 12px "what \"0.3\" stores" with monospace 11px value "0.2999999999999999889"; orange `#d95926` tick at x=490 labeled bold 12px "what 0.1 + 0.2 computes" with monospace 11px value "0.3000000000000000444".
- **Gap bracket:** magenta `#d55181` square bracket below the line spanning the two ticks, with bold 13px magenta caption centered: "two different nearest neighbors → == says False".

## A Million Small Adds Drift

Tags: `worked example` (green), `where it's used` (blue)

- **The experiment** — add 0.1 one million times; the true answer is 100,000
- **The result** — the computer gets 100000.00000133288, off by 1.3 × 10⁻⁶
- **Growth** — 10 adds are off by ~10⁻¹⁶; 100k by ~2×10⁻⁸; 1M by ~1.3×10⁻⁶
- **Everyday case** — summing a revenue column, a running total, averaging a million floats
- **Mitigation** — use `math.fsum` / numpy sums; they add in a smarter order

*Example:* Ten adds miss by 0.0000000000000001; a million adds miss by 0.0000013 — ten billion times bigger.

**Key point:** Each add contributes one rounding hair, and long-running sums collect them all — that is drift, not a data bug.

### Visualization (canvas `c3`, 720×300)

Log-log line chart: accumulated error vs number of additions (values hardcoded from an actual 64-bit double run).

- **Title (bold 15px, `#1a5276`, top center):** "Error of (0.1 added N times) vs N — both axes log scale".
- **Data (log10 N, log10 |error|), with monospace 11px point labels:** (1, −15.95) "1.1e-16", (2, −13.71) "2.0e-14", (3, −11.85) "1.4e-12", (4, −9.80) "1.6e-10", (5, −7.72) "1.9e-8", (6, −5.88) "1.3e-6".
- **Axes:** x from log10 = 1 to 6 with tick labels "10", "100", "1k", "10k", "100k", "1M" and caption "number of additions of 0.1"; y from −17 to −5 with labels "10^-16" through "10^-6" every 2 decades (right-aligned mute 12px) and horizontal grid-gray `#e5e9ef` gridlines. Padding: top 52, bottom 56, left 84, right 40. Axis lines mute.
- **Series:** blue `#2a78d6` line width 3 through the six points, 4.5px blue dots.
- **Annotation (orange `#d95926` bold 13px, right-aligned, top of plot):** "1M adds → 100000.0000013, not 100000".

## The Rules: Tolerances and Cents

Tags: `rule of thumb` (green), `common mistake` (orange)

- **Never ==** — equality tests on floats break silently; `0.1 + 0.2 == 0.3` is False
- **Use a tolerance** — `abs(a - b) < 1e-9`, or `math.isclose(a, b)`
- **Money in cents** — store $19.99 as the integer 1999; integers add exactly
- **Watch filters** — `WHERE price = 0.3` can miss rows that display as 0.3
- **Display lies** — print rounds to "0.3"; the stored value never was 0.3

*Example:* A filter `price == 0.3` returned 0 rows while the report showed thousands of $0.30 items.

**Key point:** Compare floats with a tolerance and keep money in integer cents — two rules that prevent most float bugs.

### Visualization (canvas `c4`, 720×300)

Split-panel code-result diagram; vertical dashed gray divider (`#bdc3c7`, dash 4/3) at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "The two fixes in practice".
- **Left panel** — heading bold 13px blue `#2a78d6` centered at x=180: "Compare with a tolerance". Monospace 12px lines with colored verdicts: "0.1 + 0.2 == 0.3" → bold magenta `#d55181` "→ False"; "diff = 5.6e-17" with mute 11px note "(a hair, far below any real tolerance)"; "abs(a - b) < 1e-9" → bold green `#008300` "→ True"; "math.isclose(a, b)" → bold green "→ True".
- **Right panel** — heading bold 13px aqua `#199e70` centered at x=540: "Keep money in integer cents". Monospace 12px lines: mute label "floats:", "0.10 + 0.20", bold magenta "= 0.30000000000000004"; mute label "integer cents:", "10 + 20", bold green "= 30  → $0.30 exactly".
- **Bottom line (ink bold 13px centered):** "integers add exactly — floats only approximate".

## Regeneration instructions

- **Template/layout:** tutorial detail page (per `tutorials/CLAUDE.md`, social-graph reference skeleton). Page = `<h1>` + `.subtitle` paragraph, then 4 `.card-section` blocks. Each `.card-section` has an `<h2>` (1.3rem, `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%), cell padding 12px, vertical-align top.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5 bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>` "Key point:" prefix.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg rgba(26,82,118,0.12) / `#1a5276`; green: bg rgba(39,174,96,0.15) / `#27ae60`; red: bg rgba(231,76,60,0.12) / `#e74c3c`; orange: bg rgba(230,126,34,0.15) / `#e67e22`.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; `<code>` in ui-monospace on `#f4f6f8`. No nav bar, no back/home links.
- **Canvases:** 720×300 intrinsic, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled via `window.devicePixelRatio` in a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- Grid cards elsewhere linking here use `.html` extensions in regenerated HTML.
