# Tracking Data Collection Methods

How data about people, places, and things actually gets collected — the mechanism, and what it does and does not measure.

## Audience

A layman meeting this tracking method for the first time. They should come away
with the core idea and one thing that surprises them. Not a survey of everything
true about the mechanism.

## Page Structure

2-column layout (`<table class="obj-table">`, 45%/55%). Three rows, these headings:

1. **What is it?**
2. **What does it collect?** — bullets, then the example payload (right column, under that row's canvas)
3. **Why is it collected?** — "Stated purpose" then "Additional consequence"

A page may deviate where the wording is wrong for its mechanism (the watermark
page asks "Why is it embedded?" / "How reliable is it?").

## Left Column: Keep It Short

Budget per row: **1–2 bullets per label, one callout.** A row over ~120 words is
too long. Cut the third example, the hedged aside, the mechanism detail that
does not change the reader's understanding.

- Bullets open with a bolded term and run one line.
- One callout, leading `<strong>Short label:</strong>`, stating one thing.
- Plain words. "Ring doorbell", not "IoT edge device". Short sentences, active voice.
- Do not restate a bullet in a callout, or a callout in the chart.

**What survives the cut:** the analytical point — selection effects, base rates,
unit-of-observation errors, aggregation traps. That is why these pages exist. State
it once, in the shortest form that still carries it.

**What goes:** tangents that argue against a claim the page never made; a second
worked example when one shows the shape; "designed for / reused for" blocks that
restate the two labels above them.

**Keep the hedges** (`usually`, `on some platforms`, `not established here`). They
mark unsourced claims — deleting one turns a hedge into an assertion.

## Right Column: Real Visualizations

**The chart must show something the text cannot.** A chart that renders the bullet
lists as text in two coloured boxes is not a visualization — the reader reads the
same sentences twice. That pattern was removed; do not reintroduce it.

Encode a quantity or a relationship: a signal read two ways, an error cone against
a boundary, a count growing with N, a flow between parties, a timeline. Reference
implementations: `30-phone-motion-sensors` (one trace, two readings),
`31-eye-tracking` (one error, two consequences), `27-biometric-templates-matching`
(wrong matches vs database size).

- **Canvas:** 720×320 logical, `devicePixelRatio` scaling. `setupCanvas` reads the
  element's own `width`/`height` attributes — hardcoding a size draws into part of
  the element and leaves mysterious whitespace.
- **Text must fit.** Do not fix overflow by shrinking type: reposition, wrap at a
  word boundary, or widen to 720px logical, in that order.
- **Palette** — declare once, use the tokens:

```js
const P = {
  blue: '#2a78d6', green: '#008300', magenta: '#d55181', yellow: '#c98500',
  aqua: '#199e70', orange: '#d95926', violet: '#4a3aa7',
  ink: '#1a5276', text: '#2c3e50', mute: '#6b7280', grid: '#e5e9ef'
};
```

  Seven series hues, validated for colour-vision separation. **Red is not in the
  rotation** — it is reserved for genuine alarm states. Navy `#1a5276` is ink only:
  headings, axes, callout borders. Aim for hue variety within a chart.

- **Body-copy CSS:**

```css
.key-point strong:first-child { color: #1a5276; }
.lede { font-size: 0.95em; margin: 0 0 10px 0; }
.lbl { display: inline-block; font-size: 0.7em; font-weight: 700; letter-spacing: 0.05em;
       text-transform: uppercase; padding: 2px 7px; border-radius: 3px; margin: 10px 0 2px 0; }
.lbl-purpose { background: #eaf2fb; color: #1a5276; }
.lbl-effect  { background: #fdf0e6; color: #a8501c; }
li b { color: #1a5276; font-weight: 600; }
```

## Tone

Tracking data is unsettling on its own; the writing does not need to add to it.
Describe the mechanism and let the reader draw the conclusion.

- **No attributed intent.** "ACR samples on-screen audio," not "your TV spies on you."
  Avoid spying, snooping, creepy, Orwellian.
- **No concealment framing.** "Stated purpose" / "Additional consequence", never
  "what they tell you / what they don't". Much of it is in the privacy policy.
- **No absolutes.** "Everything, every character" is alarming and usually false.
- **Say what it does NOT measure.** Often the most useful line — ACR observes a set,
  not a viewer; motion sensors measure a phone, not a person.
- **Applies inside `<script>` too.** Canvas labels are page copy.

## Data Integrity

- **Never `Math.random()` in a chart.** Generated bars under an asserted caption are
  fabricated evidence. Hardcode literal arrays.
- **Label invented numbers** "illustrative" or "schematic".
- **No unsourced specifics.** A date, dollar figure, or named incident stated as fact
  needs a source — otherwise describe the mechanism generically.
- **Do not compare incommensurable quantities.** A uniqueness proportion and a match
  accuracy in one bar chart implies a comparison that does not hold.
- **Do not run script-execution checks on charts by default — they are expensive.**
  Only verify by executing scripts when explicitly asked. If asked: `node --check`
  parses but does not run; a real harness must stub a canvas context, execute each
  `<script>`, exit nonzero on a throw, and be confirmed to fail on known-bad input
  first — a check that cannot fail is not a check.

## Example Payloads

One per page, in the "What does it collect?" row, right column, below the canvas.
Shows the shape of a record — field names, nesting, what is derived rather than
measured. Every page, no exceptions. Keep to 12–20 lines; shape, not completeness.

Required visible caption immediately above:

```html
<p class="payload-note">Sample payload — illustrative structure, not real captured data.</p>
```

A comment inside the block is not enough — a reader skimming the JSON will not see it.

Split fields into commented groups so epistemic status is visible:

```
  // ── documented in public API ──
  "transcript": "what's the weather",

  // ── inferred / plausible ──
  "confidence": 0.82,
```

If the mechanism has no public API, label the whole block inferred. Never present a
reconstruction as a captured record.

CSS: `.payload { background: #f8f9fa; border-left: 3px solid #1a5276; padding: 10px; font-family: ui-monospace, Menlo, monospace; font-size: 0.78em; overflow-x: auto; white-space: pre; line-height: 1.45; }`
`.payload-note { font-size: 0.82em; color: #666; margin: 12px 0 -6px 0; font-style: italic; }`

## Naming Rule: Real Companies

**Name a company only when the payload is genuinely documented** — published API,
spec, or open source (GA4 Measurement Protocol, Meta Conversions API, rrweb). If the
payload is reconstruction, use a broad category in the filename, `<title>`, `<h1>`
and index card: "Two-Sided Marketplace Events", not "Amazon & eBay".

A named company plus an invented schema reads as a leak of that company's internals.

Naming a product as an example of a category in prose is fine where the statement is
plainly true (Safari blocks third-party cookies by default). Attributing an
undocumented internal behaviour to a named company is not.

## Index

The hub is `../03-tracking-data-collection-methods.html` — one flat grid with a TOC.
Card numbers match each file's numeric prefix; files are numbered sequentially with
no gaps, so adding or removing a page means renumbering the rest in both places.

**When you renumber, check nothing else references the old prefixes.** Numeric
prefixes are the one unstable part of a filename, so cross-file references use the
slug alone — `CLAIMS-TO-SOURCE.md` had eleven stale numbers from a past renumber
before this rule existed.

## File Naming

`NN-kebab-case-name.html` (2-digit zero-padded prefix)

## Claims to Source Before Re-adding

See `CLAIMS-TO-SOURCE.md` — claims removed because they could not be verified.
Confirm before restoring any of them. Entries reference pages by slug, without the
numeric prefix. Add an entry whenever you strip an unsourced claim, and say what
would settle it; delete an entry only once its page is gone from the set.

