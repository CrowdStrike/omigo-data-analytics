# YAML

**Page type:** detail page (single card-section with two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** YAML

**Subtitle:** Configuration format readable by humans.

## Callout (intro)

**Core trade-off:** Beautiful readability and comments — but invisible indentation hazards and implicit type coercion create silent bugs that surface in production.

## How It Works

YAML Ain't Markup Language — indentation-based serialization designed for human readability: comments, multi-line strings, anchors/aliases for reuse. The format DevOps teams write by hand.

- **Schema:** None — and types are *guessed* from value patterns
- **Layout:** Nesting via indentation (spaces only, never tabs)
- **Spec:** 86 pages for YAML 1.2 — few parsers implement it fully
- **Best for:** Config humans read and review directly

**How it's parsed:** The parser infers structure from indentation depth and infers *types* from value patterns: unquoted `NO` matches the boolean pattern and becomes `false` (the Norway problem), `3.10` matches the float pattern and becomes `3.1`, `0o77` becomes octal 63. The value you wrote and the value the program receives can differ in type without any error — the fix is quoting everything ambiguous, which quietly concedes the readability argument.

**Failure mode:** Silent type coercion + invisible whitespace. One wrong space changes the structure; one unquoted country code changes a type. Both have caused real production outages. *(styled as key-point callout, red left border)*

*Example: Kubernetes manifests, GitHub Actions / GitLab CI, Docker Compose, Ansible playbooks, Helm charts.*

### Visualization (canvas `c1`, 720×300)

Wrote-vs-parsed comparison table (4 rows with arrows) plus an indentation-hazard snippet below.

- **Title (bold 14px, top center, `#1a5276`):** "YAML — What You Wrote vs What the Parser Made".
- **Coercion rows (starting x=50, y=60, 42px row height; each row: source in 13px monospace `#333`, arrow at x≈300, result in bold 13px monospace, reason in 11px system-ui `#666` at x=470; rows 1–3 tinted `rgba(231,76,60,0.06)` with red `#e74c3c` arrows/results, row 4 tinted `rgba(39,174,96,0.08)` with green `#27ae60` arrow/result):**
  1. `countries: [US, GB, NO]` → `NO → false` — "boolean pattern match — the Norway problem"
  2. `version: 3.10` → `3.10 → 3.1` — "float pattern match — trailing zero gone"
  3. `port: 0o77` → `0o77 → 63` — "octal pattern match"
  4. `version: "3.10"` → `"3.10" ✓` — "quoted — stays a string, no guessing"
- **Indentation hazard strip (below the rows, 13px monospace):**
  - `db:` in `#333`
  - `  host: prod` in `#333`
  - ` port: 5432` in `#e74c3c` (one space short), enclosed in a dashed red box (dash 3/3, 1.5px)
  - 11px red label to the right: "← one missing space: port is no longer inside db — structure silently changed".

## Regeneration instructions

- **Layout:** single-page detail doc: h1 with `2px solid #2980b9` bottom border, `.subtitle` paragraph, `.intro-callout` div, then one `.card-section` containing an h2 (also `2px solid #2980b9` bottom border) and a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) holding intro paragraph, `<ul>` bullets, "How it's parsed" paragraph, `.key-point` div, `.example` paragraph; right `<td class="viz-col">` (55%) holding the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `.intro-callout` background `#f8f9fa`, left border `3px solid #2980b9`, padding 10px 14px, 0.93rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `code` background `#f0f4f8`, 2px 6px padding, 3px radius. Canvas `width: 100%`, `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; row tints `rgba(231,76,60,0.06)` and `rgba(39,174,96,0.08)`.
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has none).
