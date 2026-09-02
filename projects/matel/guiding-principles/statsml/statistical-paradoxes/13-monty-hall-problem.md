# Monty Hall Problem: Who Removed the Option Decides Where Its Odds Go

**Page type:** paradox detail page — four `.card-section` blocks, 50/50 text/viz layout, canvases `c1`–`c4`
**HTML title tag:** Monty Hall Problem — Statistical Paradoxes

**Subtitle:** An eliminated option hands its odds to the survivors, and an eliminator who knew the answer hands them out unevenly

---

## Section 1 — Taking an Option Off the Table Does Not Destroy Its Odds

**Tags:** `core idea` (blue) · `redistribution` (green) · `who eliminated it` (orange)

**Bullets**

- **Freed weight** — an option that is ruled out cannot keep its share, so the survivors absorb all of it.
- **Three sealed boxes** — one prize, you claim one box, and each box starts out equally likely to hold it.
- **A knowing hand** — someone who can see inside opens an empty box, and is forbidden to open yours.
- **Your box is stuck** — nothing was checked about your claim, so its share stays exactly one in three.
- **The spared box** — it absorbs the freed share and ends at two in three, twice your box.
- **Uneven by design** — the flow is lopsided only because the opener steered around two boxes, not one.
- **The general rule** — freed weight lands on whatever the eliminator was not allowed to touch.
- **The wrong instinct** — "two left, so it's even" assumes both survivors were equally at risk of removal.

**Example:** Your box holds one third before and one third after; the spared box goes from one third to two thirds, so the freed one third landed entirely on it.

**Key point:** Elimination never destroys probability — it relocates it, and the destination is set by what the eliminator knew and was barred from removing.

**Source note:** Illustrative Example.

**Chart `c1` (720×320) — before/after mass diagram with a flow arrow**

- Title: "Ruling out an option moves its weight — it does not delete it"
- Top row: three equal blocks (Yours, Other, Ruled out), each height drawn from `1/3`, labelled with the computed share.
- Bottom row: Yours unchanged at the computed `1/3`; Other drawn at `1/3 + 1/3`; Ruled-out block dashed and empty at zero.
- Curved orange arrow from the ruled-out block to Other, labelled with the transferred mass computed as `before − after`.
- Big callout figure: the ratio `spared / yours`, computed (2.0×).
- Caption: "The freed weight went to the one box the opener was not allowed to open."

---

## Section 2 — Three Ways an Option Disappears, and Only One Rewrites the Ranking

**Tags:** `three kinds` (blue) · `blind vs informed` (green) · `ranking flip` (red) · `analyst check` (orange)

**Bullets**

- **Same starting odds** — three candidates carry 45, 35 and 20 out of 100, and the third one leaves in every case.
- **Blind removal** — a finalist withdraws or a variant dies in a failed deploy, with nobody consulting the answer.
- **Blind result** — survivors keep their relative standing and simply rescale to 56.2 and 43.8.
- **Evidence removal** — a test rules out a disease or debugging clears a subsystem, which is news about that option only.
- **Evidence result** — suspicion concentrates on both survivors in proportion, again 56.2 and 43.8, order intact.
- **Informed and constrained** — someone who knows the answer cuts one option but is barred from cutting yours.
- **Informed result** — yours falls to 39.1 while the spared option climbs to 60.9, so the ranking flips.
- **The tell** — a removal that reorders the survivors means the remover was steering, not sampling.

**Example:** Blind and evidence-based removal both leave your candidate ahead at 56.2 out of 100; an informed remover who was forbidden to touch you leaves you behind at 39.1.

**Key point:** Blind loss and honest evidence rescale the survivors without reordering them — only an informed, constrained eliminator can flip which survivor is favourite.

**Source note:** Illustrative Example.

**Chart `c2` (720×340) — three before/after bar groups, one per kind of elimination**

- Title: "Same three options, three ways the third one disappears"
- Three panels: BLIND, EVIDENCE, INFORMED + CONSTRAINED, each with a light "before" bar and a solid "after" bar for the two survivors.
- Before values hardcoded once as `[45, 35, 20]`; all after values computed in the draw function.
- Blind and evidence panels compute `p / (45 + 35)`; informed panel computes `45·0.5` and `35·1` renormalised.
- Each panel prints its two after values and a verdict line: "order kept" (green) twice, "order flipped" (magenta) once.
- Caption: "Only the informed, constrained remover changes which survivor is in front."

---

## Section 3 — Ten Thousand Letters Insisting the Odds Had Not Moved

**Tags:** `documented case` (blue) · `1990` (green) · `trained readers` (orange)

**Bullets**

- **The column** — Marilyn vos Savant gave the correct switching answer in Parade in September 1990.
- **The mailbag** — roughly ten thousand readers wrote in to tell her she was wrong.
- **The credentialled share** — about one thousand of those letters, one in ten, came from doctorate holders.
- **General readers** — she reported that 92 in 100 letters from the public said her answer was mistaken.
- **Academic readers** — among letters on university letterhead the figure was 65 in 100, still a clear majority.
- **The gap** — training cut the error rate by 27 points and still left most trained writers on the wrong side.
- **Erdos** — the mathematician Paul Erdos reportedly stayed unconvinced until he was shown a simulation.
- **What it documents** — misjudging where freed probability lands is a default reflex, not inexperience.

**Example:** 65 in 100 academic letters called the correct answer wrong, versus 92 in 100 from the general public — a 27-point improvement that still leaves the majority mistaken.

**Key point:** The reflex to split freed probability evenly survives graduate training, so the eliminator's knowledge has to be checked deliberately rather than sensed.

**Source note:** vos Savant, "Ask Marilyn," Parade, 9 September 1990, with follow-up columns 2 December 1990 and 17 February 1991; Erdos account from Vazsonyi's recollection.

**Chart `c3` (720×320) — grouped bars of disagreement rate by reader group**

- Title: "Share of letters saying the correct answer was wrong"
- Two groups (General readers, Academic letterhead), each with a magenta "said it was wrong" bar and a green "agreed" bar.
- Wrong shares hardcoded once as `[92, 65]`; agreed shares computed as `100 − wrong`.
- Gap bracket between the two magenta bars printing the computed difference (27 points).
- Small gray parenthetical: roughly 10,000 letters, about 1,000 from doctorate holders, computed as a share.
- Caption: "Training moved the error rate 27 points and still left the majority wrong."

---

## Section 4 — A Door That Falls Open by Accident Teaches You Nothing

**Tags:** `the boundary` (blue) · `even split` (green) · `accidental reveal` (orange)

**Bullets**

- **Drop the knowledge** — let the box be opened at random instead of by someone who can see inside.
- **Six equal branches** — three prize locations times two boxes that could pop open, each worth one sixth.
- **Two branches vanish** — in two of the six the prize itself is revealed, so those runs never reach the choice.
- **What is left** — two branches where staying wins and two where switching wins, from four surviving sixths.
- **The even split** — staying and switching are each exactly one half, so the advantage is gone.
- **Why it evens out** — a blind opener would have exposed the prize a third of the time, and those runs are missing.
- **The other constraint** — a knowing opener who was allowed to open your box also leaves you at one half.
- **The three questions** — ask who removed the option, what they could see, and what they were barred from removing.

**Example:** Of six equally likely branches, two are discarded because the prize showed, leaving two stay-wins and two switch-wins — one half each.

**Key point:** The advantage lives in the eliminator's constraint, not in the count of surviving options; a removal made without knowledge splits the freed weight evenly.

**Source note:** Illustrative Example.

**Chart `c4` (720×340) — branch-mass strips, accidental opener versus knowing opener**

- Title: "Where the six equally likely runs end up"
- Top strip (accidental opener): six equal segments — 2 stay-wins (blue), 2 switch-wins (green), 2 discarded because the prize showed (mute, dashed).
- Bottom strip (knowing opener): six segments — 2 stay-wins, 4 switch-wins, nothing discarded.
- Under each strip, computed renormalised shares: accidental prints `2/4` and `2/4`; knowing prints `2/6` and `4/6` as percentages.
- Big callout figure: the computed switch advantage in each case (1.0× versus 2.0×).
- Caption: "Remove the opener's knowledge and the freed weight splits evenly — no edge either way."
