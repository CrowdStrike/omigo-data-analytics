# Folk Wisdom Fallacies

Popular sayings and proverbs decomposed to reveal hidden logical fallacies, survivorship bias, and statistical errors.

## Purpose

The goal is **not** to conclude that proverbs are worthless — that reading is lazy and it stops the thinking. The goal is to train a repeatable thought process:

1. Find the grounding in a statement — what would have to be true for it to hold?
2. Generate examples and counterexamples deliberately, not just the confirming ones.
3. Assess how *strong* a claim is, separately from how confident it sounds.
4. Decide whether it fits **your** context — most claims are regime-dependent, and the regime is what gets omitted.

A claim that survives interrogation with its conditions stated is genuinely useful. One that survives only because it was never specific enough to test is not. The two feel identical until you run the check.

## Structure

| Page | Role |
|------|------|
| `../17-folk-wisdom-fallacies.html` | Nav-grid index (cards link here) |
| `02`–`26` | One saying each: title → why-believed → flaw table → undefined terms → counterexamples |
| `01-contradictory-proverb-pairs.html` | **Special card**, renders above the grid. Pairs of proverbs that contradict each other; each pair brackets a hidden conditioning variable. Carries the claim-assessment checklist. |

Every page ends at the counterexamples block. Cards `01` and `09` additionally close with a `.closing-note` on discounting presentation — rhyme and polished charts persuade independently of whether the underlying claim holds.

## Conventions

- Follow `ui-templates/07-claim-dissection-cards.html` for CSS and section order.
- **No canvas visualizations.** These pages are short reads; charts did not add value here and were removed.
- **Invented numbers must be labelled as illustrative** in the math-box. Several pages use synthetic figures to expose a mechanism; none should read as a measured rate.
- **No index numbers in `<h1>` or card titles** — the filename carries the order. Cross-reference by quoted saying, not by number.
- **Keep each page short** — roughly the length of `19-girls-grow-faster-than-boys.html`. A five-row flaw table, undefined terms, three or four real-world counterexamples. Not an essay.
- **No item counts** in prose ("thirteen pairs", "all ten questions") — they go stale on every edit.
- **No cross-links between cards.** Navigation is only from the index grid. A saying may appear both standalone and inside the contradictory-pairs page without them pointing at each other.
- **Plain language over fallacy names.** Describe the mechanism rather than labelling it ("the same numbers support opposite stories", not "Simpson's paradox").

## Note

The "slow and steady wins the race" page debunks that proverb. The root CLAUDE.md has a pending item to add *"Slow is steady, steady is fast"* as words-of-wisdom. Same phrase, opposite verdict — that item needs a deliberate framing of why some aphorisms hold up and others do not, or the two will contradict each other.

## Page Format (migrated)

Pages follow the three-file model — `NN-topic.txt.md` (verbatim text), `NN-topic.viz.md` (per-canvas regen briefs), `NN-topic.v2.html` (fenced page on shared `ui-templates/js/base.js`). Spec: `../ui-templates/FORMAT.md`.

- **Template default:** 07-claim-dissection (prose-only: pages have `txt.md` + `v2.html` only — NO `viz.md`, no shared js)
- Originals (`NN-topic.html`, old `NN-topic.md`) are kept untouched until the user's bulk review — never edit or delete them; all edits go to the three-file set per FORMAT.md's edit workflows.
