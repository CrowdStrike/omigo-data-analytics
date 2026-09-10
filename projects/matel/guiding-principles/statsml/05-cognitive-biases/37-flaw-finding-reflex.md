# Flaw-Finding Reflex: The Reviewer Needs Something to Show

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** Flaw-Finding Reflex — Cognitive Biases

**Subtitle:** Handed something to review, most people go looking for what breaks before looking at what it does. Understanding the design produces nothing anyone can see, while one corner case produces a comment with your name on it — so attention goes where the visible credit is.

---

## Section 1 — Where the First Ten Minutes Go

**Tags:** `core idea` (violet), `attention` (blue), `corner cases` (magenta)

**Bullets:**
- **The document** — a design with a main path, a few supporting parts, and some rare situations
- **What it was written to explain** — the main path, which is where nearly all the work went
- **What the reviewer opens with** — a rare situation, hunting for the one the author did not cover
- **The first ten minutes** — spent on the edges of the design rather than its centre
- **Why it feels like reviewing** — it is effortful, specific, and it produces something to say
- **Reading the main path** — takes far longer and, if it is sound, produces nothing to say at all
- **So attention drains outward** — toward whatever might break, and away from whatever works
- **In a meeting this is faster** — the counter-argument arrives before the design is even finished
- **Nobody plans it** — the reviewer is not being difficult, they are going where the payoff is

**Key point:** The reflex is about where attention goes, not about whether the criticism is right. A corner case is a small, self-contained thing one person can hold in their head and win on. A design is large, slow to load, and offers no moment of victory — so the review starts at the edges and often never reaches the middle.

**Source note (`.src`):** Illustrative Example — one design and where a reviewer's first comments land; the counts are tallied in the draw function.

### Visualization — canvas `c1`, 720×340

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

## Section 2 — Approval Leaves No Trace, a Flaw Leaves a Comment

**Tags:** `the payoff` (orange), `visible work` (yellow), `nothing to show` (red)

**Bullets:**
- **Two reviewers, same document** — one reads the design carefully, one goes straight for the edges
- **The careful reviewer** — spends an hour, understands it, agrees with it, and writes "looks good"
- **What that leaves behind** — one line that is indistinguishable from never having opened the file
- **The edge-hunting reviewer** — spends ten minutes and files a specific, quotable objection
- **What that leaves behind** — a comment with their name on it, and a visible contribution to point at
- **How the two get read** — the second reviewer looks engaged and the first looks like a rubber stamp
- **So the reflex is rational** — finding a flaw is the only reviewing that produces evidence of itself
- **The part that is a bias** — the search stops once something is found, because the goal was met
- **What it costs the author** — the main path goes unread by anyone, and nobody notices that it did

**Key point:** Approving something correctly is invisible work. There is no artifact, no comment, no record that any thinking happened — so a reviewer with no criticism to offer looks identical to one who did nothing. The reflex is the cheapest way to prove you were there, and the cost is that the search ends the moment it succeeds.

**Source note (`.src`):** Illustrative Example — two reviewers on the same document, with time spent and what each leaves behind; the figures are stated in the draw function.

### Visualization — canvas `c2`, 720×340

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

## Regeneration instructions

- **Template:** the card-section layout from `05-cognitive-biases/25-mere-exposure-effect.html`. One `.card-section` per section, each an `<h2>` plus a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note.
- **Bullet form:** one line at 50% column width, 90–100 characters including the bold label.
- **Page CSS:** identical to `25-mere-exposure-effect.html`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Hue family per section:** 1 magenta at the rim against aqua at the centre with a violet caption, 2 orange against aqua with a hard red bracket.
- **Canvas:** intrinsic `width="720"`, both heights 340. `setup(id)` caches the logical size in `dataset`, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)`. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12px; body and axis labels 12px floor; big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` for the asymmetry bracket only.
- **Determinism:** no `Math.random()` and no generator. Pin positions, ring assignments and both reviewers' figures are fixed in the code, because the lesson is where the pins cluster and a draw would scatter them.
- **Neither visualization is a data-science chart.** Section 1 is a target with pins in it; section 2 is two desks with sticky notes. An earlier version of this material plotted a running accuracy score per check, which turned a behavioural observation into a measurement exercise and buried the point.
- **This page is about where attention goes first, not about whether the verdict is right.** The reviewer's criticism may be entirely correct. The bias is that the search starts at the rim and stops as soon as it succeeds, so the centre of the design goes unread. Do not add an accuracy comparison, a running score, or a "who was closer to the truth" panel — all of them re-frame the page as being about verdicts.
- **No exact science.** This is a common behavioural pattern, not a measured effect. Keep every figure round and illustrative — 10 comments, 3 rings, 60 minutes against 10 — and never present the split as a research finding. There is no citation on this page and none should be invented.
- **The reviewer is not the villain.** The reflex is rational given how reviewing is credited, and the criticism is often useful. The page fails if it reads as "reviewers are obstructive"; it should read as "approval leaves no evidence, so the incentive points outward".
- **Keep the incentive as the mechanism.** An earlier draft explained the reflex purely by effort — a flaw is cheaper to find than a design is to read. That is true and insufficient: the stronger reason is that a flaw is the only reviewing that leaves an artifact. Cost explains why it is easy; credit explains why it is done.
- **What separates this from card 35.** Scope Neglect is about a verdict formed on whichever slice of features a person happened to need. This page is about the reviewer's opening move and what they get for making it, with no verdict involved.
- **What separates this from card 01.** Confirmation Bias is unequal scrutiny depending on whether you liked the answer. Here the scrutiny is uniformly aimed at the edges regardless of what the reviewer wants to be true.
- **Keep this page at two sections.**
