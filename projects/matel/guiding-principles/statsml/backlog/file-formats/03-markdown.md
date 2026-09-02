# Markdown

**Page type:** detail page (single card-section with two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Markdown

**Subtitle:** Readable as plain text, renderable to anything.

## Callout (intro)

**Core trade-off:** Human convenience and universal readability — but no single standard exists (CommonMark, GFM, MDX, Pandoc all diverge).

## How It Works

Lightweight markup using punctuation (`#`, `*`, `-`, `[]`) to indicate formatting. Created by John Gruber in 2004, designed so the source is readable without rendering.

- **Schema:** None — prose with formatting conventions
- **Spec:** Fragmented — CommonMark, GFM, MDX, Pandoc all diverge
- **Property:** Diffable in version control, LLM-native
- **Best for:** Docs-as-code, READMEs, human+machine text

**How it's parsed:** A block pass splits the document into paragraphs, headings, lists, and code fences; an inline pass then resolves emphasis, links, and inline code within each block. Gruber's original description was ambiguous, so every implementation resolved the edge cases differently — CommonMark finally pinned down the block/inline algorithm in 2014, but GFM tables, footnotes, and MDX components remain extensions that only some renderers understand.

**Why it won:** The same source works for both audiences — humans read it raw in any editor, machines render it to HTML, PDF, DOCX, or slides. The cost: "valid Markdown" means different things to different renderers. *(styled as key-point callout, red left border)*

*Example: Every README on GitHub, wikis, Obsidian/Notion notes, API docs, LLM input and output.*

### Visualization (canvas `c1`, 720×300)

Side-by-side two-panel diagram: raw Markdown source on the left, rendered output on the right, connected by a green arrow.

- **Title (bold 14px, top center, `#1a5276`):** "Markdown — One Source, Two Readers".
- **Panels:** two 300×190 boxes starting at y=55, 60px gap between them, centered horizontally. Left panel background `#f8f9fa`, right panel background `#fff`; both with `#ccc` 1px border.
- **Left panel (source, 12px monospace, 20px line spacing, 12px left inset):** 8 lines with per-line colors:
  - `## Results` in `#1a5276`
  - (blank line)
  - `- **Bold item** with \`code\`` in `#333`
  - `- A [link](https://ex.com)` in `#333`
  - (blank line)
  - `| Col A | Col B |` in `#e67e22`
  - `|-------|-------|` in `#e67e22`
  - `| 1     | hello |` in `#e67e22`
- **Right panel (rendered mockup):**
  - Heading "Results" in bold 16px system-ui `#1a5276` with a light `#e0e0e0` horizontal rule under it.
  - Bullet line 1: "•  **Bold item** with" in `#333` 13px, followed by a `#f0f0f0` 38×16 rounded highlight containing `code` in 12px monospace `#c0392b`.
  - Bullet line 2: "•  A link" — link text in `#2980b9` with an underline stroke in `#2980b9`.
  - Mini rendered table 200×52 with `#bbb` borders, one row divider and one column divider, header row filled `#f8f9fa`; bold 12px header cells "Col A" / "Col B", body cells "1" / "hello" in 12px system-ui `#333`.
- **Arrow:** green (`#27ae60`, width 2) horizontal arrow with filled triangular head, from left panel edge to right panel edge at vertical center.
- **Panel labels (11px system-ui `#666`, centered under each panel):** "source — readable as-is in any editor" (left), "rendered — which renderer? GFM? CommonMark?" (right).
- **Bottom note (centered, `#e67e22`, 11px):** "orange = GFM table extension — not all renderers agree".

## Regeneration instructions

- **Layout:** single-page detail doc: h1 with `2px solid #2980b9` bottom border, `.subtitle` paragraph, `.intro-callout` div, then one `.card-section` containing an h2 (also `2px solid #2980b9` bottom border) and a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) holding intro paragraph, `<ul>` bullets, "How it's parsed" paragraph, `.key-point` div, `.example` paragraph; right `<td class="viz-col">` (55%) holding the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `.intro-callout` background `#f8f9fa`, left border `3px solid #2980b9`, padding 10px 14px, 0.93rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `code` background `#f0f4f8`, 2px 6px padding, 3px radius. Canvas `width: 100%`, `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has none).
