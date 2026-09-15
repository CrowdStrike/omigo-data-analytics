# [Page Title]

**Page type:** other — UI template file (claim-dissection cards: philosophy callout + 2-column TOC + repeated saying-cards with flaw tables and callouts; no canvases; placeholder content)
**HTML title tag:** Template 07 — Claim Dissection Cards

**Subtitle:** [One-line description of what this page dissects]

## Document structure (in order)

1. `<h1>` — "[Page Title]".
2. `.subtitle` paragraph.
3. Opening `.philosophy` callout.
4. `.toc` box: bold heading "Table of Contents" + ordered list in 2 CSS columns; each li is an anchor link to a section id (`#s01`, `#s02`, `#s03`), with HTML comment "Add more entries as needed".
5. Repeated entries (separated by `<!-- ============================================ -->` comments): `<h2 id="sNN">N. "[claim]"</h2>` followed by a `.saying-card` div containing `.saying-quote`, `.saying-why`, a `.flaw-table`, an `.undefined-terms` callout, and a `.counterexamples` callout.
6. A commented-out optional math callout block (HTML comment "Optional: math callout for quantitative demolition").
7. Closing `.philosophy` callout.

## Opening callout (`.philosophy`, verbatim)

**[Framing note]:** [Why this collection matters — the meta-pattern or teaching point across all entries.]

## Table of Contents (`.toc`, verbatim)

**Table of Contents**

1. [Entry 1 title] (link to `#s01`)
2. [Entry 2 title] (link to `#s02`)
3. [Entry 3 title] (link to `#s03`)

## Entry 1: 1. "[The claim being dissected]" (h2 id `s01`; placeholder text verbatim)

Saying card (`.saying-card`):

- Quote (`.saying-quote`, italic): "[The claim in quotes]"
- Why (`.saying-why`): Why people believe it: [1-2 sentences on the psychological mechanism]

Flaw table (`.flaw-table`, columns # / Fallacy / How it hides):

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | [Fallacy name] | [How this specific claim enables/hides this fallacy] |
| 2 | [Fallacy name] | [Explanation] |
| 3 | [Fallacy name] | [Explanation] |

(HTML comment after row 3: "Add more rows as needed")

- Undefined-terms callout (`.undefined-terms`, orange accent): **Undefined terms:** "[term]" ([what it could mean]), "[term]" ([what it could mean])
- Counterexamples callout (`.counterexamples`, green accent): **Counterexamples:**
  - [Concrete example that breaks the claim]
  - [Another counterexample]
  - [Another counterexample]

## Entry 2: 2. "[Next claim]" (h2 id `s02`; placeholder text verbatim)

Saying card:

- Quote: "[Next claim in quotes]"
- Why: Why people believe it: [explanation]

Flaw table:

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | [Fallacy name] | [Explanation] |
| 2 | [Fallacy name] | [Explanation] |

- Undefined-terms callout: **Undefined terms:** "[term]" ([ambiguity])
- Counterexamples callout: **Counterexamples:**
  - [Example]
  - [Example]

(HTML comment after entry 2: "Repeat pattern for each entry")

## Optional math callout (commented out in source; verbatim placeholder text)

A `.undefined-terms` div with inline style overrides `background:#f0f4f8; border-left-color:#1a5276;`:

**The math:**

[Setup — simple numbers that can be verified in head]

**Strategy A:** [description]
P = [calculation] = **[result]**

**Strategy B:** [description]
P = [calculation] = **[result]**

**[Conclusion — why the math demolishes the intuition]**

## Closing callout (`.philosophy`, verbatim)

**The meta-pattern:** [Summary observation that ties all entries together — what structural feature enables the fallacies across all claims.]

## Regeneration instructions

- **This is a UI template file** in `ui-templates/`, used as a starting point for claim/saying dissection pages (e.g. folk-wisdom style). All bracketed text is placeholder. No canvases and no script block in this template.
- **Layout:** single page: h1 + `.subtitle` + `.philosophy` framing callout + `.toc` (2-column ordered list of in-page anchor links) + repeated h2-anchored `.saying-card` entries + closing `.philosophy` callout. Entry separators are long `====` HTML comments.
- **Page CSS:** universal reset `* { margin:0; padding:0; box-sizing:border-box; }`; body `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`, background `#ffffff`, color `#2a2a2a`, padding 20px 10px, line-height 1.6. h1 1.8em `#1a5276`, margin-bottom 10px. h2 1.3em `#1a5276`, margin 50px 0 12px, border-bottom `2px solid #2980b9`, padding-bottom 8px. p margin 10px 0, `#333`, 0.95em. `.subtitle` `#666`, 1.0em, margin-bottom 30px. `strong` `#1a5276`.
- **Card CSS:** `.saying-card` background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-quote` 1.2em italic `#1a5276`, weight 600, margin-bottom 6px. `.saying-why` 0.88em `#666`, margin-bottom 14px.
- **Flaw table CSS:** `.flaw-table` width 100%, border-collapse collapse, margin 10px 0, 0.88em; th background `#f0f4f8`, padding 8px 12px, left-aligned, border `1px solid #e0e0e0`, `#1a5276`; td padding 8px 12px, border `1px solid #e0e0e0`, vertical-align top; even rows background `#fafcfe`.
- **Callout CSS:** `.undefined-terms` background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, margin 10px 0, 0.88em. `.counterexamples` background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, margin 10px 0, 0.88em; its ul margin 4px 0 0 16px, li margin 3px 0. `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, margin 20px 0, 0.9em.
- **TOC CSS:** `.toc` background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, margin 20px 0 30px, radius 4px; ol margin 10px 0 0 20px, 0.9em, `columns: 2`, li margin 4px 0, links `#2980b9` no underline.
- **Palette:** #1a5276 primary blue, #27ae60 green (counterexamples), #e74c3c red, #e67e22 orange (undefined terms), #2980b9 accent blue.
- **HTML comments to preserve:** "Add more entries as needed", `====` entry separators, "Add more rows as needed", "Repeat pattern for each entry", and the entire commented-out optional math callout block.
- **Canvases:** none in this template; if added, they use `window.devicePixelRatio` scaling per project convention.
- No nav bar, no back/home links. TOC links are in-page `#sNN` anchors only. In regenerated HTML any card links would use `.html` extensions (this page has none).
