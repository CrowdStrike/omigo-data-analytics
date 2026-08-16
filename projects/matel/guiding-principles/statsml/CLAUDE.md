# Project Instructions

## Critical: Challenge All Claims

When the user makes a statement, conclusion, or design decision, do NOT simply agree. Instead:

1. Verify that the claim is logically sound and scientifically valid
2. If it contradicts known statistics/ML theory, say so directly
3. If the reasoning has a gap or unstated assumption, point it out
4. If a simpler explanation or counterexample exists, present it
5. If the conclusion is correct, confirm it with the reasoning — not just "yes"

This applies especially to:
- Statistical assumptions and when they hold
- Claims about sample sizes, thresholds, distributions
- Design decisions that might introduce the same problems being solved (e.g., replacing magic numbers with different magic numbers)
- Overgeneralizations about ML algorithms

Disagreement is expected and preferred over false agreement. Be direct.

## Project Context

See `./STATSML.md` for core ideas, principles, architecture, and phases.

This is a statistical ML library (omigo-data-analytics-statsml) focused on verifying statistical preconditions before applying tests/models, multi-candidate parameter validation, and feature profiling pipelines.

## Conversation Style

- Wait for the user to guide the conversation direction
- Use prior knowledge rather than web search unless asked otherwise
- Keep responses concise and direct
- Use professional language that conveys the message but is not offensive.

## Brainstorming & Design Process

When brainstorming or designing a new concept:

1. Talk it through with examples first — don't jump to implementation
2. Take notes at high level, think about structure
3. Capture the core idea in a visual HTML doc (see layout rules below)
4. Sit on it — the user may want days to incubate before formalizing
5. Only then convert to a mathematical data model with confidence, coverage

## Documentation Style

Create docs that can be read and reviewed quickly. Keep them short and scannable:

- One canvas visualization + a few sentences per concept. No walls of text.
- Show only the latest/best classifier results (v3-full). Do not include historical v1/v2 numbers.
- Per feature: title, histogram, detected shape (one line), gap/valley (yes/no), 2-3 sentence summary, result. That's it.
- No extended feature lists, no categorical analysis sections, no supplementary output dumps.
- Tables over prose. Short rows over verbose paragraphs.
- If a section requires scrolling past 2 screens, it's too long — split or cut.

### HTML Design Doc Layout

When creating HTML docs, use templates from `docs/statsml/ui-templates/` as the starting point. The templates cover:

| Template | Use for |
|----------|---------|
| 01-landing-page | Top-level hub pages |
| 02-nav-grid | Section navigation |
| 03-toc-reference | Long reference docs with TOC |
| 04-two-col-catalog-badges | Catalog pages with status badges |
| 05-two-col-catalog-clean | Clean two-column catalogs |
| 06-sectioned-cards-callout | Card-based sections with callouts |

See `ui-templates/README.md` for full usage guide.

Additional canvas/chart rules:
- **Canvas sizing:** minimum 720px width, height 300-460px, use `width: 100%`
- **Grid galleries:** max 3 charts per row, minimum 200px height per chart
- **devicePixelRatio scaling:** always use `window.devicePixelRatio` for retina
- **Color palette:** #1a5276 (primary blue), #27ae60 (green), #e74c3c (red), #e67e22 (orange), rgba(26,82,118,0.35) (bar fill)


## Best Practices
Use predefined ui-templates to understand style, format, coloring, font etc scheme. Esp when multiple agents are created to write docs under a grid.


## TODO

- Remove dead `.nav` CSS rules from ~289 HTML files (the `<div class="nav">` elements are already gone, but the style blocks remain as unused code)
