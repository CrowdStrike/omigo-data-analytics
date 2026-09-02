# Notable Languages

**Page type:** detail page (single two-column layout row: text/tags left 45%, canvas right 55%)
**HTML title tag:** Notable Languages

**Subtitle:** Each brought an idea. None held the mainstream.

## Notable Languages

Bullets:

- Assembly (1949) — direct CPU instructions, no abstraction
- COBOL (1959) — business logic in English-like prose, still runs banks
- Lisp (1958) — code is data, macros rewrite the language itself
- Perl (1987) — text processing Swiss army knife, write-only syntax
- D (2001) — better C++ without the baggage, never reached critical mass
- Lua (1993) — embedded scripting for games and configs
- Erlang (1986) — telecom-grade fault tolerance, hot code reload
- Haskell (1990) — pure functions, lazy evaluation, academic darling
- OCaml (1996) — ML family pragmatist, fast compiler, algebraic types

Key point (red-left-border callout):

Languages die when a simpler alternative solves 80% of the use case. They survive in niches where nothing else fits.

Tag pills:

- `historical` (blue)
- `academic` (blue)
- `niche` (orange)
- `embedded` (orange)
- `telecom` (blue)

### Visualization (canvas `timeline`, 720×220)

Horizontal timeline of language birth years with lollipop-style stems, alternating heights for label spacing.

- **Data (name, year), plotted in this order with a per-language color:**
  - Assembly, 1949 — `#1a5276`
  - Lisp, 1958 — `#27ae60`
  - COBOL, 1959 — `#e74c3c`
  - Erlang, 1986 — `#8e44ad`
  - Perl, 1987 — `#e67e22`
  - Haskell, 1990 — `#2980b9`
  - Lua, 1993 — `#16a085`
  - OCaml, 1996 — `#d35400`
  - D, 2001 — `#c0392b`
- **Axis:** horizontal line `#2c3e50` (width 2) at y=160, from x=50 (padLeft) to x=680 (720 − padRight 40); linear year scale 1945–2005 mapped across the axis width. Tick marks every 10 years from 1950 to 2000: 8px downward ticks with 10px `#666` centered year labels 20px below the axis.
- **Markers (per language):** vertical stem in the language color (width 1.5) from just above the axis up to the label; stem height alternates 70px (even index) / 95px (odd index). Filled 5px-radius circle in the language color at the base (y = axisY − 4). Language name in bold 11px in its color at the stem top; the year in 9px `#888` directly above the name.
- **Bottom annotation (10px `#95a5a6`, centered at (360, 200)):** "Birth years of notable programming languages (1949-2001)"

## Regeneration instructions

- **Template:** backlog detail page, kusto-style two-column layout. Single `table.layout` (width 100%, border-collapse collapse) with one `<tr>`: left `<td class="text-col">` (45%) holds bullets, `.key-point`, and `.tags`; right `<td class="viz-col">` (55%) holds the canvas. No index number in h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9` and 8px padding-bottom; `.subtitle` `#666` 0.95rem, 32px margin-bottom; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px.
- **Key point:** `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem, 12px margin-top.
- **Tags:** `.tags` flex-wrap with 8px gap, 20px margin-top; `.tag` pill padding 4px 10px, radius 4px, 0.78rem, weight 600. Colors: `.tag-green` background `#eafaf1` text `#1e8449`; `.tag-red` background `#fdedec` text `#c0392b`; `.tag-blue` background `#eaf2f8` text `#1a5276`; `.tag-orange` background `#fef9e7` text `#b7540c`.
- **Canvas:** intrinsic size 720×220, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; extended marker colors `#8e44ad`, `#2980b9`, `#16a085`, `#d35400`, `#c0392b`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
