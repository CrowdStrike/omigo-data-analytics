# Evolution Folder — Working Notes

Last updated: 2026-08-22

This file records what exists, what the user asked for, what they rejected, and
what is still unresolved. Written so the thread can be picked up cold.

---

## 1. What exists in this folder

Seventeen per-strand pages, each covering one thread of computing history with a
canvas timeline plus an eras table plus a "what disappeared" block:

| File | Strand |
|------|--------|
| 01-programming-languages.html | Languages |
| 02-programming-paradigms.html | Paradigms |
| 03-data-footprint.html | Data scale |
| 04-data-analytics-sophistication.html | Analytics |
| 05-machine-learning.html | ML |
| 06-neural-networks.html | Neural nets |
| 07-system-architectures.html | Architecture |
| 08-coding-tools.html | Dev tools |
| 09-internet-communication.html | Internet |
| 10-compute-hardware.html | Processors |
| 11-cloud-saas.html | Delivery |
| 12-tech-stacks.html | Stacks |
| 13-open-source.html | Open source |
| 14-networking-hardware.html | Networking |
| 15-data-query-languages.html | Query languages |
| 16-big-tech-by-segment.html | Segments |
| 17-file-formats.html | File formats |

Parent pages, one level up:

- `23-evolution-of-computing.html` — the grid hub, cards 1–17 plus a draft card 18
- `24-evolution-single-page.html` — all 17 strands on one page, shared 1940–2026 scale
- `24-evolution-single-page-v2.html` — **draft**, cross-strand relationships
- `24-technology-causal-chains.html` — pre-existing, draws causal links explicitly

---

## 2. The user's objective

Stated across several messages, in their words where possible:

- **"every document here is a quick read to create wow effect, create interest"**
- **"minimal details, good viz to reduce text"**
- Storytelling aimed at a **layman**, not a technical reference
- Causality should be **implicit**: *"we dont have to draw that but it will become
  apparent automatically."* The GPU → deep learning example was given to
  illustrate this — put them adjacent on a timeline and the reader connects them
- Group **3–4 related strands per section** to show *"how things happen kind of
  together coz world was moving together business, hardware, software etc wise"*
- Grouping should be derived from **"correlated and causal big events"** first,
  then narrated — not narrated first
- Titles must be **factual, not editorializing**

---

## 3. Feedback given, in order

Chronological, since the direction shifted several times.

### On `17-file-formats.html`
- *"poor quality content"* — **NOT YET ADDRESSED.** The page was written before
  this feedback and has not been revised. See Open Items.

### On `24-evolution-single-page.html`
1. *"lets just sync evolution with 24-evolution-single-page.html"* — it had only
   14 sections for 17 pages, and sections 11–14 were numbered inconsistently
   with the files (Networking was #11 on the page, #14 as a file). **Fixed:** all
   17 sections present, numbering matches file names.
2. *"a better viz would be just a label with some viz to show when a specific tech
   arrived gaining popularity, most are still used to today so the end date is not
   adding much value"* + *"that way we dont need barchart and its label separately"*
   — **Fixed:** replaced bars-with-gutter-labels with name-at-arrival-year. The
   name IS the mark. Filled dot = fast adoption, hollow = gradual. Thin trailing
   line for duration, deliberately understated.
3. *"no need for thos arrows"* — **Fixed**, removed the ongoing-arrow glyph.
4. *"the 2 letter year code is ugly, confusing. need full 4 letter"* — **Fixed**,
   `'95` → `1995` on all axes. Applies to any future chart here.

### On `24-evolution-single-page-v2.html` — several rounds

**Round 1 (four analytical views): rejected.**
Built a term cloud sized by longevity, an arrivals histogram, a survival-by-decade
stacked bar, and a constraint band. Verdict: *"its ugly. very cluttered.
confusing."* Root cause was mixing four unrelated chart metaphors, each needing a
caveat paragraph to interpret.

**Round 2 (four overlap types): the current file, closest to accepted.**
Rebuilt as four sections, one consistent mark language, each showing a different
overlap shape. Specific feedback on it:
- *"the titles need upcadte. thos are criticizing. keep it factual"* — **Fixed.**
  "Gamers Paid for the AI Boom" → "Hardware Capability Arrived First, Software
  Followed"; "Nothing Replaced Anything" → "Query Languages and Formats
  Accumulated Rather Than Replaced"; "The Job Picked the Tool" → "How Languages
  Evolved With the Work They Served"; "The Five Years When Everything Moved" →
  "Every Strand Turned Over Between 2004 and 2011".
- *"1. Gamers Paid for the AI Boom is so misleading. I was referring to hardware
  advancements leading to software jumps. over history not just AI"* — **Fixed.**
  Section 1 was rescoped from a GPU/AI story to the general eighty-year pattern:
  transistors → compiled languages, microprocessor → personal computing, cheap
  memory → garbage collection, GPUs → deep learning, plus the two reversals
  (power wall → concurrency in languages; flash → log-structured stores).
- *"need full page width"* — **Fixed**, `.wrap` max-width removed.

**Round 3 (lean rewrite): rejected, reverted.**
Interpreted "quick read, minimal details" as a hard cut to three sections with a
huge number, a 4–7 row chart, and two lines of text each. Verdict: *"revert your
recnt chamgne. the latest one is worse."* Reverted to Round 2.

**Lesson from rounds 1 and 3:** the fix for clutter was *one consistent viz
metaphor*, not *less content*. Round 3 cut substance along with the clutter.
Density is acceptable when the marks are uniform and self-labelling.

---

## 4. Where v2 stands now

Four sections, each a deliberately different overlap shape, all sharing the
name-at-arrival-year mark language from `24-evolution-single-page.html`:

| # | Section | Overlap shown | Strands combined |
|---|---------|---------------|------------------|
| 1 | Hardware Capability Arrived First, Software Followed | Lag between capability and use | Processors, languages/paradigms, data scale |
| 2 | Query Languages and Formats Accumulated Rather Than Replaced | Accumulation — count the end-ticks | Query languages, file formats |
| 3 | How Languages Evolved With the Work They Served | Workload precedes tool | Paradigms, languages, data handling |
| 4 | Every Strand Turned Over Between 2004 and 2011 | Pure synchrony, 2003–2012 zoomed | Processors, delivery, dev tools, segments |

Reachable via card 18 on `23-evolution-of-computing.html`, styled with a dashed
border and a "draft" badge (`.card.draft`, `.badge-draft`).

---

## 5. How the section 4 claim was derived

Not asserted — computed. A sliding six-year window over every phase in all 17
strands, counting how many *distinct* strands had an arrival in that window:

| Window | Distinct strands moving |
|--------|------------------------|
| 1980–1985 | 13 of 17 |
| 1994–2000 | 14 of 17 |
| **2005–2010** | **17 of 17 — all of them** |
| 2017–2022 | 10, and falling |

Two things follow. First, 2005–2010 is the only window in eighty years where
every strand turned over. Second — and this is the counterintuitive bit worth
keeping — **the current AI period is narrower than 2005–2010**, concentrated in
models and data while languages, networking and architecture are quiet.

**Caveat that must stay on the page:** the clustering comes from this project's
own list of phases, so the exact count reflects the segmentation, not a discovery
about history. The concentration is robust to re-dating; the precision is not.

---

## 6. Corrections made to the user's framing

Recorded because the user's project instructions require challenging claims
rather than agreeing, and these were disagreements, not transcriptions.

1. **"Functional programming fitting big data" → functional *style*, retrofitted.**
   Big data pipelines were written in Java, Scala and Python, not Haskell or Lisp.
   What map/reduce actually caused was lambdas and immutable collections being
   added to imperative languages (Java 8, 2014). A functional-language era never
   happened; claiming one would be wrong.

2. **Python's simple syntax is an amplifier, not the initiating cause.** It won
   scientific computing first as glue over C and Fortran (NumPy, scikit-learn).
   Syntax then compounded: easy to teach → more developers → more libraries →
   more reason to teach. The user later refined this themselves as *"simple
   languge for plumbing, and then programming"*, which is now how the chart shows
   it — Python appears twice, 1998 as plumbing and 2015 as the language itself.

3. **Use cases do not fully determine grammar.** If they did, SQL would have
   stayed displaced after MapReduce. Instead Hive, Pig, Spark SQL and Trino were
   built to return to it. Pattern: a new workload reliably spawns a new grammar,
   which then either gets absorbed back into SQL or stays in its niche. Survival
   depends on the abstraction boundary, not on the workload.

4. **"Data lineage formats" ≠ table formats.** `17-file-formats.html` treats
   lineage as Delta/Iceberg/Hudi, which give *time travel*, not *provenance*.
   Lineage in the dbt/OpenLineage sense — which upstream produced which column —
   is a different concern and arguably belongs on a data-tooling page.

---

## 7. Open items

- **`17-file-formats.html` is still unrevised** after *"poor quality content"*.
  No specifics were given about what was wrong. Worth asking whether the problem
  is the prose density, the era groupings, the timeline, or the lineage
  conflation noted above before rewriting.
- **v2 section 3's top group is editorial.** "Systems too large for one person"
  is not a dated release. It is drawn grey and dotted with a note saying so, but
  it is a different kind of claim from every other row on the page. Cutting that
  group and letting the paradigm/language rows carry the story is the honest
  alternative.
- **Which v2 sections to keep** — the user said *"I will pick what I liked."*
  No selection made yet. All four are still in.
- **v2 is not linked from `index.html`**, only from the evolution grid. Intentional
  while it is a draft.
- **Global TODO from root CLAUDE.md still open:** dead `.nav` CSS rules in ~289
  HTML files, where the `<div class="nav">` elements are gone but the style
  blocks remain.

---

## 8. Mermaid — where it is used and why only there

Decided 2026-08-22. User asked *"try mermaid charts here?"*

**Used on `24-technology-causal-chains.html` only.** That page had 48 hand-typed
`&rarr;` arrows in monospace text. It is a dependency graph with no time axis,
which is exactly what mermaid does well, so the ten enabling chains are now real
node-and-edge diagrams.

**Deliberately NOT used on the timeline pages** (v2, the 17 strands,
24-evolution-single-page):

1. **Mermaid cannot place nodes on a time axis.** It auto-layouts by graph
   topology, so a 1996 node and a 2012 node get equal spacing. On those pages
   horizontal position *is* the year, and the 16-year GPU→deep-learning gap is
   the content. Using mermaid there would delete the point of the chart.
2. Those pages would lose the zero-dependency property for no gain.

### Implementation notes — three bugs hit, worth not repeating

- **Do not put diagram source in the DOM inside a hidden container.**
  `display:none` means mermaid cannot measure text and emits zero-size SVGs.
  Off-screen absolute positioning also failed (diagrams piled up overlapping).
  **Working approach:** diagram sources live in a JS array, rendered via
  `mermaid.render()` into empty `.mermaid-slot` divs. Raw source can never flash
  on screen, and measurement happens in a laid-out container.
- **Node IDs must be unique across the whole page.** All ten diagrams initially
  used `A`, `B`, `C`; mermaid keys internal state by id, so they merged into one
  giant overlapping graph. Now prefixed `n0A`, `n1A`, etc., and each render call
  gets a unique id (`mmd0`, `mmd1`, …).
- **`themeVariables` are ignored unless `theme: 'base'` is set.** Without it the
  default lavender palette wins. With it the nodes match the project blue
  (`#eef5fb` fill, `#2980b9` border, `#1a5276` text, `#a04000` arrows).

### The one cost, stated plainly

This is **the only external dependency in the whole repo** — every other page of
~290 renders offline with inline canvas. Mitigated by a fallback: the monospace
chain text stays in the DOM and is only hidden via `body.mermaid-ready`, which is
added after a successful render. If the CDN is blocked or the machine is offline,
the slots stay empty and the text arrows remain visible. Pinned to `mermaid@11`.

If this dependency is unwanted later, reverting means deleting the module script
and the two `.mermaid-slot` CSS rules — the text fallback is already there.

---

## 9. Conventions to preserve

Learned from feedback; violating these caused rework.

- **Four-digit years** on every axis. Never `'95`.
- **No arrows** as a chart glyph, and no cross-page navigation links — the only
  links are grid cards on the index pages.
- **Name at arrival year as the mark.** No separate bar plus gutter label to
  match up. End dates are understated because most things are still in use.
- **Card index numbers must match file index numbers** (card 17 → `17-*.html`).
- **No item counts** in summaries or cards — they go stale.
- **One viz metaphor per page.** This is the specific fix for "cluttered".
- **Full page width** for these cross-strand pages.
- Palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22`
  orange, `rgba(26,82,118,0.35)` bar fill. `devicePixelRatio` scaling always.
- **Do not run git commands** — the user handles all git themselves.
