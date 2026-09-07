# Project Instructions

## Critical: Challenge All Claims

When the user makes a statement, conclusion, or design decision, do NOT simply agree. Instead:

1. Verify that the claim is logically sound and scientifically valid
2. If it contradicts known statistics/ML theory, say so directly
3. If the reasoning has a gap or unstated assumption, point it out
4. If a simpler explanation or counterexample exists, present it
5. If the conclusion is correct, confirm it with the reasoning — not just "yes"
6. nefore creating a new html page or grid, check if there is something from backlog that already exists

This applies especially to:
- Statistical assumptions and when they hold
- Claims about sample sizes, thresholds, distributions
- Design decisions that might introduce the same problems being solved (e.g., replacing magic numbers with different magic numbers)
- Overgeneralizations about ML algorithms

Disagreement is expected and preferred over false agreement. Be direct.

## Critical: Mathematical Accuracy of Examples and Visualizations

Every number in every example, table, chart label, and caption must be **computed, not asserted**.
A plausible-looking number that does not follow from the stated data is a defect of the same
severity as broken code — the docs teach statistics, so a wrong figure teaches the wrong thing.

- **Compute before you write.** Derive correlations, p-values, rates, probabilities, and
  expected counts with an actual calculation. Never estimate a statistic by eye and label it.
- **Chart labels are computed at render time.** If a chart draws a fitted line, compute the fit
  from the plotted points in JS and print that value. Do not hardcode `r = −0.6` next to a
  hand-drawn line. (This exact defect existed in `03-berksons-paradox`: the label said −0.6, the
  seeded data gives −0.47.)
- **Seeded, never random.** Generated data uses a seeded PRNG so the figure, its labels, and the
  prose agree on every load. `Math.random()` in a chart makes the caption unverifiable.
- **Text, table, and chart must reconcile.** The same quantity stated in three places must match
  to the digit. If the prose says 113 hires, the chart must plot 113 and the table must total 113.
- **Arithmetic must close.** Row and column totals sum, percentages derive from the shown
  numerator and denominator, and subgroup counts add to the aggregate.
- **Check for degenerate setups.** A toy example whose parameters produce a probability of exactly
  0 or 1, or a divide-by-zero, illustrates nothing. Verify the construction is non-trivial before
  building a chart on it. (A two-disease OR-admission example needs a third admission route,
  or `P(B | not A, admitted)` is trivially 100%.)
- **Show the boundary honestly.** If an effect disappears under some condition, state the
  condition and verify the claim numerically rather than asserting the effect is universal.
- **Label unsourced figures.** Illustrative numbers are marked "Illustrative Example." Never
  present a constructed figure as a real-world measurement.

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
- Bullet points must not text-wrap: each bullet is a bold colored label + a short phrase that fits on one line at normal page width.
- Do NOT cut important information to achieve this — split it into more labeled bullets instead. Density comes from structure, not longer prose.
- Use a mix of colors, labels, fonts, and bold to convey information — colored bold labels name the concept, the phrase carries the fact.
- Show only the latest/best classifier results (v3-full). Do not include historical v1/v2 numbers.
- Per feature: title, histogram, detected shape (one line), gap/valley (yes/no), 2-3 sentence summary, result. That's it.
- No extended feature lists, no categorical analysis sections, no supplementary output dumps.
- Tables over prose. Short rows over verbose paragraphs.
- If a section requires scrolling past 2 screens, it's too long — split or cut.

### Markdown-First Page Workflow

- Every `.html` page must have a sibling `.md` page (same path and name, only the extension differs).
- Generate the `.md` page first, then use that `.md` as the source to generate the `.html`.
- When editing an existing `.html`, apply the same change to its sibling `.md` so the two never drift.

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
 - Use predefined ui-templates to understand style, format, coloring, font etc scheme. Esp when multiple agents are created to write docs under a grid.
 - When fanning doc-page generation out to subagents: one page per agent, at most 5 agents running in parallel. Each agent writes the `.md` spec first, then generates the `.html` from it.
 - Dont include count of items in summary or in the card. Because thats a lose dependent number that need to be updated everytime.
 - Dont run any Git commands. I will do all Git stuff on my own. If git history is needed to find some previous version etc. then ask.
 - Dont verify the rendering of html docs — no script checks (they are expensive), no browsers, screenshots, or render testing unless told to do so specifically.
 - dont put any cross reference links from one page to another except the index grid cards where it is for navigation. no back, home links kind of things either.
 - Some grid pages like backlog may have self reference links which is okay
 - Grid pages with cards should hae index number for each card. That index number should match the file index number in naming convention

## Folder-Level Instructions

Each subfolder has its own `CLAUDE.md` with folder-specific context and pending TODOs. See:

| Folder | Focus |
|--------|-------|
| `ab-testing/` | A/B testing pitfalls and methodology |
| `domains/` | Industry-specific data traps |
| `folk-wisdom/` | Popular sayings decomposed for hidden fallacies |
| `real-world-distributions/` | Surprising shapes from real data |
| `cognitive-biases/` | Human psychology failures in analysis |
| `metrics/` | Good/bad metrics, vanity metrics, reporting |
| `backlog/` | Unresolved topics and future directions |
| `interesting-problems-paradoxes/` | Classic puzzles and real-world analogues |
| `common-bad-practices/` | Organizational anti-patterns in data teams |

## Exclusions
Work only inside statsml directory. Dont go to parent or other outside directories (except /tmp) unless told so.

## Global TODO

- Remove dead `.nav` CSS rules from ~289 HTML files (the `<div class="nav">` elements are already gone, but the style blocks remain as unused code)

