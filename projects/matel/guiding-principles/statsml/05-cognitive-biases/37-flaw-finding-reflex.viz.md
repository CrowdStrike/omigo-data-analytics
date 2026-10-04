# Flaw-Finding Reflex — Viz

**Page type:** detail page, card-section (template `06-sectioned-cards-callout`)
**HTML title tag:** Flaw-Finding Reflex — Cognitive Biases
**Template:** the card-section layout from `05-cognitive-biases/25-mere-exposure-effect.html`
**Source note wording:** the sibling `.txt.md` says the counts are "tallied at render time" and the figures "stated at render time"; the html `.src` notes say "tallied in the draw function" and "stated in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed or the construction invalidates that prose — re-read the computed values and update the text to match.

**Determinism:** no `Math.random()` and no generator. Pin positions, ring assignments and both reviewers' figures are fixed in the code, because the lesson is where the pins cluster and a draw would scatter them.

---

## 1. Where the First Ten Minutes Go

**Tag colors:** `core idea` violet, `attention` blue, `corner cases` magenta
**Hue family:** magenta at the rim against aqua at the centre with a violet caption

### canvas `c1` — 720×340

**A target, not a chart.** The design as concentric rings — main path at the centre, supporting parts around it, rare situations at the rim. Ten review comments land as pins, and almost all of them sit in the outer ring.

- **Data:** no PRNG. 3 rings: main path (centre), supporting parts (middle), rare situations (rim). 10 comments at fixed positions listed in the code — 7 in the rim ring, 2 in the middle, 1 in the centre.
- **Computed:** comments per ring **7 rim / 2 middle / 1 centre**, tallied from the pin list; share of the design's actual weight per ring stated as a separate fixed figure so the mismatch is visible.
- **Title (bold 15px `P.ink`, centered, y=21):** "Ten Review Comments, and Where They Landed"
- **Layout:** three concentric circles centred at `(w×0.36, 178)` with radii 46, 96 and 146. Centre filled `rgba(25,158,112,0.22)`, middle ring `rgba(255,255,255,1)`, rim ring `rgba(213,81,129,0.10)`, all stroked `P.grid`.
- **Ring labels** (12px, placed inside each band on the left): `P.aqua` "the main path", `P.mute` "supporting parts", `P.magenta` "rare situations".
- **Pins:** each comment a filled circle radius 5 with a 1.5px `#fff` outline, `P.magenta` in the rim ring, `P.mute` in the middle, `P.aqua` at the centre; positions fixed in the code, spread around the band rather than clustered.
- **Ring tallies** (right side, three blocks from y=96 on a 62px pitch): bold 19px in the ring hue with the comment count, then 12px `P.mute` naming the ring, then 12px `P.mute` with "where most of the work went" against the centre only.
- **The contrast line** (bold 12px `P.magenta`, right side, under the tallies): "7 of the 10 comments are about the rim", computed from the pin list.
- **Note line** (12px `P.mute`, centered, `h−28`): "the centre is the largest part of the design and the least discussed".
- **Caption (bold 13px `P.violet`, centered, `h−8`):** "Attention lands where a comment is easiest to make."

---

## 2. Approval Leaves No Trace, a Flaw Leaves a Comment

**Tag colors:** `the payoff` orange, `visible work` yellow, `nothing to show` red
**Hue family:** orange against aqua with a hard red bracket

### canvas `c2` — 720×340

**Two desks, not a chart.** Side by side: an hour of reading that leaves a single blank sticky note, and ten minutes at the rim that leaves a note everyone can read.

- **Data:** no PRNG. Two reviewers with fixed figures — 60 minutes and 10 minutes, 1 comment each, 0 quotable objections against 1.
- **Computed:** the effort ratio between the two panels drives the height of each panel's time bar, so the 6× difference is drawn from the two numbers rather than typed as a multiplier.
- **Title (bold 15px `P.ink`, centered, y=21):** "Same Document, Two Reviews, One Visible Contribution"
- **Layout:** two panels split at `w/2`, each with a time column on its left and a sticky note on its right, baseline y=250.
- **Time columns:** a vertical bar 34px wide rising from the baseline, height proportional to minutes spent — the careful reviewer's tall in `rgba(25,158,112,0.30)` stroked `P.aqua`, the edge-hunter's short in `rgba(217,89,38,0.45)` stroked `P.orange`. Each labelled bold 12px in its hue with "60 min" / "10 min" above.
- **Sticky notes:** 118×92 squares tilted 3 degrees, filled `#fdf6d8` stroked `rgba(201,133,0,0.6)` with a soft 1px shadow. The left note carries 12px `P.mute` "looks good to me" and nothing else; the right note carries three 12px `P.text` lines of a specific objection about a rare case, with a bold 12px `P.orange` header "corner case".
- **Panel headers** (bold 12px above each panel): `P.aqua` "READ THE DESIGN" and `P.orange` "WENT FOR THE EDGES".
- **Panel verdicts** (12px `P.mute` under each note): "reads as a rubber stamp" and "reads as a thorough review".
- **The asymmetry marker:** a 1.5px `#e74c3c` bracket spanning the two notes, labelled bold 12px `#e74c3c` "only one of these proves any reviewing happened".
- **Note line** (12px `P.mute`, centered, `h−28`): "the design itself was only actually read on the left".
- **Caption (bold 13px `P.orange`, centered, `h−8`):** "A flaw is the only review that leaves a receipt."

---

## Page-specific constraints

- **Canvas heights:** both 340, an unusual pairing — section 1 needs the rim radius of 146 plus the tally column, section 2 needs the sticky notes above the y=250 baseline.
- **Extra palette entry:** hard red `#e74c3c` is used on this page for the asymmetry bracket only, outside the shared `P`.
- **Neither visualization is a data-science chart.** Section 1 is a target with pins in it; section 2 is two desks with sticky notes. An earlier version of this material plotted a running accuracy score per check, which turned a behavioural observation into a measurement exercise and buried the point.
- **This page is about where attention goes first, not about whether the verdict is right.** The reviewer's criticism may be entirely correct. The bias is that the search starts at the rim and stops as soon as it succeeds, so the centre of the design goes unread. Do not add an accuracy comparison, a running score, or a "who was closer to the truth" panel — all of them re-frame the page as being about verdicts.
- **No exact science.** This is a common behavioural pattern, not a measured effect. Keep every figure round and illustrative — 10 comments, 3 rings, 60 minutes against 10 — and never present the split as a research finding. There is no citation on this page and none should be invented.
- **The reviewer is not the villain.** The reflex is rational given how reviewing is credited, and the criticism is often useful. The page fails if it reads as "reviewers are obstructive"; it should read as "approval leaves no evidence, so the incentive points outward".
- **Keep the incentive as the mechanism.** An earlier draft explained the reflex purely by effort — a flaw is cheaper to find than a design is to read. That is true and insufficient: the stronger reason is that a flaw is the only reviewing that leaves an artifact. Cost explains why it is easy; credit explains why it is done.
- **What separates this from card 35.** Scope Neglect is about a verdict formed on whichever slice of features a person happened to need. This page is about the reviewer's opening move and what they get for making it, with no verdict involved.
- **What separates this from card 01.** Confirmation Bias is unequal scrutiny depending on whether you liked the answer. Here the scrutiny is uniformly aimed at the edges regardless of what the reviewer wants to be true.
- **Keep this page at two sections.**
