# Tutorials — Simplest-Form Concept Tutorials

One concept per page, taught to a layman or newbie data scientist through ONE
concrete running example. Definition comes AFTER the example, never before.
These are tutorials, not pitfall pages — teach the concept itself; failure modes
get one section at most.

## Audience

Someone meeting the concept for the first time. They should come away able to
explain it to a colleague using the page's example. No prerequisites beyond
arithmetic unless the page states one in its subtitle.

## Reference Pages (read before writing)

| File | What to copy |
|------|--------------|
| `../most-powerful-signals/07-social-graph-and-connections.html` | Page skeleton, CSS, tag pills, one-line bullets, canvas style |
| `../real-world-distributions/08-advertising-and-adtech.html` | 3-col layout when a row needs two charts |
| `../recently-added-misc/tracking-data-collection-methods/06-session-replay.html` | Total content amount per page (do not exceed) |

## Topic Page Structure

- `<h1>` concept name (no index number), one-line `.subtitle` stating the core idea in plain words.
- 3–4 `.card-section` blocks, each `<h2>` + `table.layout` (50% text / 50% viz — the split is fixed;
  to shrink a chart use the canvas's `max-width`/`height`, never a narrower viz column):
  1. **The idea** — the concept introduced entirely through the running example
  2. **A worked example** — actual numbers, rows, or steps; the reader can redo it by hand
  3. **Why it matters** — where a data scientist meets it; what goes wrong without it
  4. *(optional)* **The common confusion** — the one thing people get wrong
- Section h2s should name the content, not the template (e.g. "Splitting the coin flips" not "A worked example" verbatim — the four roles above are roles, not literal headings).

## Left Column (text) — Keep It Short

- `.tags` row first: 2–4 colored pills (`core idea`, `worked example`, `rule of thumb`, `common mistake`, `where it's used`, or topic-specific).
- 5–8 bullets (5–6 for simple concepts, 8 only when genuinely dense), each ~90–100 chars
  including the `<b>bold term</b>` it opens with; up to ~105 is fine — see social-graph reference.
- At the 50/50 column split these run one line, sometimes two. Wrapping to a second line is
  acceptable; **never delete a fact or abbreviate jargon to avoid it.** Merge two facts into one
  ~105-char bullet rather than adding a 9th bullet, and don't split down to ~60-char stubs —
  over-splitting forces strained invented labels and shreds one idea across many lines.
- One italic `.example` line: a one-sentence concrete instance.
- One `.key-point` callout: the single takeaway of the section.
- No paragraph blocks. Plain words: "the average", not "the first moment".

## Right Column (viz) — Carry the Weight

The page should feel more visual than textual. The chart must show something the
text cannot — the example's actual data, a shape, a before/after, a flow.

- Canvas 720×300 logical, `devicePixelRatio` scaled, `width:100%` CSS.
- Use the 3-col layout (38/31/31, adtech reference) when one chart can't carry the row.
- **Large fonts**: chart titles bold 15–16px, axis/data labels 12–13px, annotation callouts bold 12–13px. Nothing below 11px.
- Generous white space: margins ≥50px left, ≥40px bottom; don't crowd annotations.
- Bold colored in-chart annotations stating the insight ("95% of ads below 4% CTR").
- Palette:

```js
const P = {
  blue: '#2a78d6', green: '#008300', magenta: '#d55181', yellow: '#c98500',
  aqua: '#199e70', orange: '#d95926', violet: '#4a3aa7',
  ink: '#1a5276', text: '#2c3e50', mute: '#6b7280', grid: '#e5e9ef'
};
```

Navy `#1a5276` is ink (headings, axes, callout borders). Red only for genuine
error/alarm states. Aim for hue variety within a chart.

## Data Integrity

- **Never `Math.random()`** — hardcode literal arrays. Deterministic pseudo-random
  (seeded, or `Math.sin(i*k)` jitter) is fine.
- Invented numbers get an "illustrative" label in the chart or caption.
- The worked example's numbers in the text MUST match the numbers in the chart.

## Grid Pages (category level)

- One grid file per category at `23-tutorials/NN-category-name.html`, topic pages in
  `23-tutorials/category-name/NN-topic-name.html` (2-digit zero-padded files).
- `.nav-grid` with **4 cards per row** (`repeat(4, 1fr)`; responsive fallbacks: 3 columns
  below 1400px, 2 below 1100px, 1 below 600px).
- Default is ONE flat `.nav-grid` for the whole category — the colored `.card-num`
  labels carry the grouping. Add `<h2>` sections (each with its own grid) only when
  the grouping is genuine: roughly 3+ cards per section and a clear theme that will
  keep accumulating cards. Never add an h2 that merely restates the card labels.
- Card: `.card-num` holds the SUBCATEGORY label (colored per subcategory),
  `<h3>` is "N. Topic Title" (unpadded N matching the file's number), one-line
  description, 2–4 `.topic-tag` pills.
- Card numbering runs 1..N across the whole category (not per subcategory).

## Subfolder Overrides

Some topic folders carry their own `CLAUDE.md` that layers extra visualization
conventions on top of this file (e.g. `common-howtos/CLAUDE.md`: pictorial
illustrated scenes, a distinct chart-palette theme per page). Read the folder's
`CLAUDE.md` before writing or editing any page inside it.

## Hard Rules

- No cross-page links except grid-card navigation. No back/home links.
- No item counts in summaries or cards.
- No `.nav` CSS or elements.
- Don't name real companies for undocumented behavior; generic examples preferred
  (a coffee shop's daily sales, a hospital's test results, an orders table).
- Every `<script>` must execute without throwing — verify canvas code carefully
  (variables in scope, no TypeScript syntax).
