# AI Code Feature Set Capacity Musical Chair

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 140. AI Code Feature Set Capacity Musical Chair

**Subtitle:** AI code assistants add features eagerly, but past a complexity threshold they can no longer keep every invariant consistent — each addition silently breaks something else and net progress oscillates (all scenarios and numbers below are illustrative, not measurements).

## Musical Chairs — Adding Feature N+1 Breaks Features N-1 and N-2

**Musical Chairs — Adding Feature N+1 Breaks Features N-1 and N-2**

AI coding tools are verbose and eager to add features. But there is a coherence ceiling — a point beyond which they cannot keep every feature consistent simultaneously. Illustrative scenario: a project sits at that ceiling with 100 features; asking for feature 101 gets it added, but the refactoring required silently breaks two existing features. Net working features stays flat while churn is high.

- The tool adds feature 101 correctly in isolation
- Refactoring to integrate it disrupts features 47 and 83 (subtle, no test failure)
- User doesn't notice until production — the AI never ran the full feature matrix
- Musical chairs: every time someone sits down, someone else stands up

### Visualization (canvas `c1`, 580×300)

Row of feature boxes with two flipped to broken by a new addition.

- **Title (bold 14px `#1a5276`, centered):** "Musical Chairs: Add 1, Break 2".
- **Feature boxes:** ten 48×35 rects at y=40 (x = 20 + i×54), fill `rgba(39,174,96,0.4)` and label F1…F10 in `#27ae60`; boxes F5 (index 4) and F8 (index 7) fill `rgba(231,76,60,0.4)` with red `#e74c3c` labels. Eleventh box "F11" / "(new)" in `rgba(41,128,185,0.4)` with `#2980b9` bold label.
- **Break arrows:** dashed (3/2) red lines, width 2, from F11's box down-row to F5 and F8 at y=78.
- **Red caption:** "Adding F11 broke F5 and F8 during refactoring".
- **Bottom bold gray (`#555`) lines:** "Before: 10 working. After: 9 working. Added 1, broke 2, fixed 1. Net: -1." and "Feature count oscillates around the ceiling. Never grows past it."

## Verbosity as False Confidence

**Verbosity as False Confidence**

AI tools generate 200 lines where 20 suffice. The volume LOOKS thorough. But verbose code has more surface area for bugs, is harder to review, and creates the illusion of completeness. The developer scans it, sees "lots of code," assumes it's handled. The bugs hide in the volume.

- 20 lines of human code: every line reviewed, every edge case visible
- 200 lines of AI code: reviewer's eyes glaze at line 50, bugs at line 180 survive
- More code = more maintenance burden passed to future developers

### Visualization (canvas `c2`, 580×300)

Two proportional blocks: small reviewed human code vs large AI code with hidden bug patches.

- **Title (bold 14px `#1a5276`):** "Verbosity: More Lines = More Hiding Places for Bugs".
- **Human block:** green rect `rgba(39,174,96,0.4)` at (50,40) 80×100, labels in `#27ae60`: "Human" / "20 lines" / "All reviewed".
- **AI block:** light red rect `rgba(231,76,60,0.2)` at (200,40) 350×100 with two darker bug rects `rgba(231,76,60,0.6)` at (480,100) 30×15 and (350,120) 25×12. Bold red labels: "AI: 200 lines. Reviewer glazes at line 50." / "Bugs hide at line 130 and 180." plus 11px note "← nobody reads past here".
- **Bottom bold gray:** "Volume ≠ correctness. Volume = surface area for unreviewed bugs."

## Context Window ≠ Coherence Window

**Context Window ≠ Coherence Window**

Model can "see" 100K tokens but can't maintain logical coherence across all of them simultaneously. It may reference a function correctly in line 50 but contradict that function's contract in line 4000. The context window is a READ window, not an UNDERSTAND window. Large codebases exceed coherence even when they fit in context.

- Early declarations contradicted by late implementations
- Type signatures that don't match actual return values 3000 lines later
- Duplicate utility functions with slightly different behavior
- Config values set correctly in one place, overridden incorrectly elsewhere

### Visualization (canvas `c3`, 580×300)

Two nested horizontal window bars: wide context window vs shorter coherence window with a contradiction zone.

- **Title (bold 14px `#1a5276`):** "Context Window (reads) ≠ Coherence Window (understands)".
- **Context bar:** blue-stroked (`#2980b9`, width 2) rect (30,40) 520×30, label inside: "Context window: 100K tokens (can SEE all code)".
- **Coherence bar:** green-stroked (`#27ae60`) rect (30,80) 200×30, label: "Coherence window: much smaller (actually CONSISTENT)".
- **Contradiction zone:** light red fill `rgba(231,76,60,0.2)` rect (230,80) 320×30, red label: "This zone: visible but contradictions accumulate".
- **Bottom bold gray lines (centered):** "Line 50: \"returns string.\" Line 4000: treats return as number. Both in context." and "Coherence decays with distance. Context doesn't."

## Regression Blindness — No Memory of What Used to Work

**Regression Blindness — No Memory of What Used to Work**

AI tool doesn't maintain a mental model of "what was working before." It sees current code and the request. It has no concept of "this refactoring broke feature X that was passing." Without test suites as the ground truth, the AI cannot distinguish improvements from regressions. It optimizes for the LOCAL request, not GLOBAL system health.

- Feature worked yesterday → AI refactors adjacent code → feature breaks silently
- No test = no detection. AI doesn't volunteer "by the way, I may have broken X"
- Accumulated regressions compound until the system is subtly wrong everywhere
- User trust in "the AI handled it" prevents manual verification

### Visualization (canvas `c4`, 580×300)

Declining line chart of fraction of features working across AI iterations.

- **Title (bold 14px `#1a5276`):** "No Memory of \"What Was Working Before\"".
- **Series (red `#e74c3c` line, width 2):** values `[1, 1, 1, 1, 1, 1, 1, 1, 0.8, 0.6, 0.7, 0.5, 0.6, 0.4, 0.5]` plotted at x = 40 + i×36, y = 150 − v×100.
- **Axis labels:** "Iteration (each = AI modification)" bottom center in `#333`; red y-label "% features working".
- **Bottom bold gray:** "Each fix is locally correct. Globally: working features decay over iterations."

## Eager Deletion / Over-Simplification

**Eager Deletion / Over-Simplification**

AI is asked to "clean up" or "simplify." It removes code that looks dead but is actually handling a rare edge case triggered once per month. The edge case code has no test (because it's rare). The AI sees: unused code path → delete. Production: monthly crash nobody connects to the "cleanup."

- Rare code paths look "dead" to statistical pattern matching
- "Simplify this" interpreted as "remove anything not obviously used"
- Defensive code (error handling, fallbacks) appears redundant until it's needed
- The deletion looks clean in review — the bug shows up 3 weeks later

### Visualization (canvas `c5`, 580×300)

Two code-path blocks, the rare one crossed out, with side annotations.

- **Title (bold 14px `#1a5276`):** "\"Clean Up\" = Delete Rare Edge Case Handling".
- **Main path:** green rect `rgba(39,174,96,0.3)` at (40,45) 200×40, label "Main path (99% of traffic)" in `#27ae60`.
- **Edge case:** red rect `rgba(231,76,60,0.3)` at (40,95) 200×40 with a red X (two crossing 2px lines corner-to-corner), label "Edge case (1% — looks \"dead\")" in `#e74c3c`.
- **Side annotations (11px `#555`, left aligned at x=270):** "AI: \"This code path appears unused → deleted\"" / "Reality: fires once/month on specific data pattern" / "3 weeks later: production crash nobody connects" / "to the \"cleanup\" that happened 3 weeks ago".
- **Bottom bold red (centered):** "Rare ≠ dead. The AI can't distinguish them without tests."

## Feature Interaction Blindness

**Feature Interaction Blindness**

AI builds Feature A perfectly. Builds Feature B perfectly. A and B interact in production in a way neither spec mentioned. The AI never cross-tested them because each was requested independently. The interaction (shared state, race condition, conflicting UI) only appears when both are live simultaneously.

- Each feature is correct in isolation — integration testing was never requested
- Shared resources (database connections, cache keys, global state) conflict
- UI layout breaks when two features render in the same viewport
- Permission models conflict — Feature A grants access Feature B should deny

### Visualization (canvas `c6`, 580×300)

Venn-style diagram: two feature circles with a conflict overlap.

- **Title (bold 14px `#1a5276`):** "Features Built Independently, Conflict When Combined".
- **Circles:** green `rgba(39,174,96,0.3)` circle center (180,100) radius 60 labeled "Feature A" / "✓ alone" in `#27ae60`; blue `rgba(41,128,185,0.3)` circle center (350,100) radius 60 labeled "Feature B" / "✓ alone" in `#2980b9`.
- **Overlap:** red circle `rgba(231,76,60,0.4)` center (265,100) radius 25 containing "💥".
- **Bottom bold gray (centered):** "Shared state, race condition, conflicting permissions — only appears when BOTH are live."

## Oscillating Solutions (Fix A Breaks B, Fix B Breaks A)

**Oscillating Solutions (Fix A Breaks B, Fix B Breaks A)**

Report bug in Feature A. AI fixes A by changing shared module. This breaks Feature B. Report B broken. AI fixes B by reverting shared module change + workaround. Workaround breaks A again. The AI doesn't remember the cycle — each fix is locally correct but globally oscillating. Net progress: zero.

- Tight coupling means any fix propagates side effects
- AI doesn't maintain history of "tried this, it broke that"
- Each conversation is fresh — no memory of the oscillation pattern
- After 3 cycles, the code is WORSE (accumulated workarounds) than before any fix

### Visualization (canvas `c7`, 580×300)

Square-wave oscillation line between "A works" and "B broken" states.

- **Title (bold 14px `#1a5276`):** "Fix A Breaks B → Fix B Breaks A → Infinite Loop".
- **Line (red `#e74c3c`, width 2):** 7 points alternating `[1,0,1,0,1,0,1]` at x = 80 + i×70, high state y=60, low state y=130.
- **State labels:** green "A works" at the three high points; red "B broken" (x≈115, 255) and "A broken" (x≈185, 325) at low points.
- **Bottom bold gray (centered):** "Net progress after 6 AI fixes: zero. Code is worse (accumulated workarounds)."

## Copy-Paste Divergence (Duplicated Logic Evolves Independently)

**Copy-Paste Divergence (Duplicated Logic Evolves Independently)**

AI generates similar code in 3 places instead of abstracting into a shared function. Each copy evolves independently as different features are modified. After 10 iterations, the 3 copies are subtly different — some have bug fixes the others don't. Which is "correct"? Nobody knows. The AI created technical debt by not abstracting.

- AI defaults to generating code inline rather than referencing existing utilities
- Fix applied to copy 1 never propagates to copies 2 and 3
- Over time: 3 different behaviors for what should be 1 behavior
- Refactoring into shared function is now risky — which copy is canonical?

### Visualization (canvas `c8`, 580×300)

Three code-copy boxes with diverging version labels.

- **Title (bold 14px `#1a5276`):** "Same Logic, 3 Copies, Diverging Over Time".
- **Boxes:** three 120×60 rects `rgba(41,128,185,0.2)` at y=50, centered near x=90/270/450, each titled in bold `#2980b9` with a `#555` status line: "Copy 1" / "v1.0 + fix A"; "Copy 2" / "v1.0 + fix B"; "Copy 3" / "v1.0 (no fixes)".
- **Bottom text (centered):** bold red: "After 10 iterations: 3 subtly different behaviors. Which is correct? Nobody knows."; gray `#555`: "AI generates inline instead of abstracting. Each copy evolves independently."

## Phantom Dependencies (Import It, Don't Check If It Exists)

**Phantom Dependencies (Import It, Don't Check If It Exists)**

AI generates code importing a package, function, or module that doesn't exist in this project. It looks correct syntactically. IDE might not flag it immediately. Fails at runtime, not compile time (in dynamic languages). AI hallucinated the dependency from training data — it exists in SOME codebase, not THIS one.

- Package names that exist on npm/pypi but aren't in your requirements
- Internal module paths that existed in the training data, not your project
- API methods from a different version of the library
- Runtime crash in production on first invocation of the hallucinated path

### Visualization (canvas `c9`, 580×300)

Two monospace import statements with red rejection annotations.

- **Title (bold 14px `#1a5276`):** "Import Statement Looks Correct — Package Doesn't Exist Here".
- **Code lines (13px monospace `#333`, left at x=60):** `import { processData } from "utils/transform"` and `import { validateSchema } from "data-validator"`.
- **Red bold annotations (at x=380):** "✗ exists in training data, not YOUR project" and "✗ npm package exists, not in your package.json".
- **Bottom text (centered):** bold gray: "Syntactically valid. Semantically hallucinated. Runtime crash on first call."; orange `#e67e22`: "Dynamic languages: no compile-time catch. Fails in production, not development."

## The Plateau — Diminishing Returns Beyond Complexity Threshold

**The Plateau — Diminishing Returns Beyond Complexity Threshold**

AI assistance has a sweet spot at simple-to-moderate complexity. Beyond some threshold — wherever it sits for a given model and codebase — every AI-generated addition requires human verification that takes LONGER than writing it yourself. The tool becomes net-negative at scale — generating plausible-looking code that's subtly wrong faster than you can verify it. You're now debugging AI output instead of building.

- Below threshold: AI accelerates — most generations are correct as-is.
- At threshold: AI is roughly neutral — enough generations need fixes to offset the speedup.
- Above threshold: AI decelerates. Every generation needs full review.
- The musical chairs effect: total working features oscillates, never grows past ceiling

### Visualization (canvas `c10`, 580×300)

Rise-plateau-decline productivity curve with shaded zones.

- **Title (bold 14px `#1a5276`):** "Productivity vs Complexity: The AI Ceiling".
- **Axes:** light gray `#ccc` L-shaped axes with margins left 50 / top 40 / right 30 / bottom 30; x-label "Project Complexity (features)" bottom center; rotated y-label "AI Productivity".
- **Curve (blue `#2980b9`, width 2), 100 points:** piecewise v = i×2.5 for i<40; 100 − (i−40)×1 for 40≤i<60; 80 − (i−60)×2 for i≥60; scaled to plot height /120.
- **Zones:** left 40% of plot shaded `rgba(39,174,96,0.1)` labeled "AI accelerates" in `#27ae60`; right 40% shaded `rgba(231,76,60,0.1)` labeled "AI decelerates" in `#e74c3c`; center label "Ceiling" in `#e67e22`.

## Regeneration instructions

- **Layout:** detail page. h1 + `.subtitle`, then one `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` div + an intro `<p>` + `<ul>` of bullets, right `<td>` (60%, centered) holds the canvas. No philosophy callout on this page. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; p 0.95em `#333`; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links.
- **Canvas:** intrinsic size 580×300 for all charts. This page's `setup(id)` uses an extra `vizScale = 1.3` on top of `window.devicePixelRatio`: backing store = w×1.3×dpr, CSS size = w×1.3 px, `ctx.scale(dpr×1.3, dpr×1.3)` — so charts render 1.3× larger than their declared size. Chart text is 11-14px -apple-system.
- **Palette:** primary blue `#1a5276` / `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#555`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
