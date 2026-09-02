# TOML

**Page type:** detail page (single card-section, two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** TOML

**Subtitle:** Explicit, minimal configuration format.

## Intro callout

**Core trade-off:** No surprises — every type is explicit and what you see is what you get — but deeply nested structures become verbose and awkward.

## How It Works

Tom's Obvious, Minimal Language — a 2013 reaction to YAML's implicit typing. Strings are always quoted, integers and floats are syntactically distinct, dates are first-class, indentation means nothing.

- **Schema:** None, but types are unambiguous from syntax
- **Layout:** Flat key-value pairs under `[table]` headers
- **Spec:** Small enough that parsers implement all of it
- **Best for:** Config where correctness beats expressiveness

**How it's parsed:** Type is determined by syntax alone, never by content: `"3.10"` is a string because of the quotes, `8080` is an integer because it has no quotes or decimal point, `1979-05-27T07:32:00Z` is a datetime because it matches the RFC 3339 grammar. There is no pattern-matching on values, so there is no Norway problem — the parser cannot surprise you because it never guesses. The price is verbosity: deep nesting repeats the full path in every table header (`[a.b.c.d]`).

**Key point (red-left-border callout):** **Design lesson:** TOML vs YAML is explicitness vs convenience. TOML chose "what you write is what you get" and accepted awkward deep hierarchies as the cost.

*Example: Cargo.toml (Rust), pyproject.toml (Python packaging), app config after a YAML-typing incident.*

### Visualization (canvas `c1`, 720×300)

Annotated code-listing diagram: a TOML source snippet with each line colored by inferred type, plus a right-hand explanation column and a dashed "nesting verbosity" panel below.

- **Title (bold 14px system-ui, `#1a5276`, centered at y=25):** "TOML — Type Comes From Syntax, Never From Content".
- **Code lines** start at x=50, y=62, line height 26px; code in 13px monospace (colored per line), explanation in 11px system-ui gray `#666` at x=360:
  1. `[server]` in `#1a5276` — "table header"
  2. `version = "3.10"` in `#27ae60` — "quotes ⇒ string — never float 3.1"
  3. `port = 8080` in `#e67e22` — "bare digits ⇒ integer"
  4. `ratio = 0.85` in `#e67e22` — "decimal point ⇒ float"
  5. `debug = false` in `#8e44ad` — "keyword ⇒ boolean"
  6. `started = 2024-03-15T10:00:00Z` in `#e74c3c` — "RFC 3339 syntax ⇒ first-class datetime"
- **Nesting verbosity panel:** dashed orange rectangle (`#e67e22`, dash 5/4, width 1.5, 620px wide, 62px tall) below the listing containing two 12px monospace lines in `#333`:
  - `[a.b.c.d]   # every deep table repeats the full path`
  - `[[server.routes]]   # arrays of tables — verbose but unambiguous`
  - plus an 11px system-ui orange (`#e67e22`) note: "the cost of explicitness: deep hierarchies get awkward"

## Regeneration instructions

- **Layout:** single `.card-section` with h2 "How It Works" (1.3rem `#1a5276`, 2px `#2980b9` bottom border), containing a `table.layout` (100% width, border-collapse) with one `<tr>`: left `td.text-col` (45%) holds paragraph, `<ul>` bullets, a `.key-point` div, and a `.example` paragraph; right `td.viz-col` (55%) holds the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro-callout` background `#f8f9fa`, left border 3px solid `#2980b9`, padding 10px 14px, 0.93rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem. `code` background `#f0f4f8`, padding 2px 6px, radius 3px. Canvas `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple accent.
- **Canvas:** intrinsic 720×300; use `window.devicePixelRatio` scaling (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper.
- In regenerated HTML, any card links use `.html` extensions.
