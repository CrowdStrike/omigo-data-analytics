# Statistical Paradoxes — Folder Instructions

Each page isolates one failure mode where hidden structure breaks intuition.

## Template

Copy `23-tutorials/20-llms-and-generative-ai/08-positional-encoding.html` — CSS, `setup()`, `P` palette,
`.card-section`, tag pills, bullet form. `03-berksons-paradox` is the converted example here.
Deltas: columns are 50/50 and the canvas is centered.

Anything the template does not do, do not do.

## Page Shape

- `<h1>` = `Paradox Name: <layman punchline>` + one-line `.subtitle`. No index numbers anywhere.
- **Grid card summary = one line stating the SITUATION, not the resolution.** The reader has not
  read the page yet, so the summary must set up the puzzle from nothing: "You pick one of three
  doors; a host who knows what is behind them opens an empty one." Never open with the mechanism
  or the answer ("the survivors carry more weight…", "Easiest to see with 1,000 doors instead of
  3") — those name props and conclusions the reader has no way to picture. One sentence, ~65–90
  characters, no statistics.
- As many `.card-section` blocks as the paradox needs, one canvas each — three is often enough.
  Roles to draw from: the mechanism, a second domain, a real documented case, when it is safe.
  Name the content, don't label the role.
- Do not pad to a section count. If two sections would run the same computation twice, cut one.
- The boundary section is mandatory.
- Text column: `.tags` → 5–8 bullets → one italic `.example` → one `.key-point`. Each bullet is
  one non-wrapping line (~≤100 chars) and a complete thought. Simple concepts take 5.

## Language

Reader is smart, no statistics training.

- Lead with the puzzle, not the mechanism.
- No bare statistics in prose or headings — on a chart, only as a small gray parenthetical.
- Define or avoid: correlation coefficient, odds ratio, collider, conditioning, prior,
  posterior, p-value. No named techniques.
- Physical metaphors carry the mechanism — a door, a missing corner, a highlight reel.

## Canvas

- `width="720"` + height 300–340. Fonts: title bold 15px, labels 12px floor, big figure 19px,
  caption bold 13px.
- Space multi-line notes in pixels, not data units.
- No `Math.random()` — seeded LCG (seed 42) or literal arrays.
- Every printed statistic computed in the draw function from the plotted points.
- No tables on canvas. Every chart ends with a caption stating its takeaway.
- Palette roles: `green` honest view, `magenta` misleading view, `orange` the mechanism, `mute`
  unseen data, `ink` titles, `blue`/`aqua`/`violet`/`yellow` named groups.

## Chart Form

- Reversal → paired slope lines per subgroup + aggregate going the other way, bubble size = n.
- Base rate → unit grid or waffle.
- Filtering → full population gray, surviving subset highlighted, fitted line on each.
- Convergence/scale → curve with the intuition-breaking threshold marked.
- Information flow → before/after mass diagram with arrows.

## Content Bar

- Numbers reconcile with the chart to the digit. Unsourced constructions get "Illustrative
  Example".
- Alice/Bob for people, "Vendor A" for companies. No real companies in a critical framing.
- No military references.
- No cross-page links, no back/home links, no `.nav` CSS.

## Deduplication

Birthday and Monty Hall also live in `18-interesting-problems-paradoxes/`, which tells the story.
Here: the failure mode only.
