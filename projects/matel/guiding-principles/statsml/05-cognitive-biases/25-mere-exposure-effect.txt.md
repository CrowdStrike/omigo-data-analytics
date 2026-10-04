# Mere-Exposure Effect: You Are Rating Your Own Retraining Bill

Change a tool someone uses every day and they will tell you it got worse. Show the same change to a stranger and they prefer it on the spot.

**The one construction behind every figure on this page.** A rater gives the redesign a whole-number
score out of ten, and the **old version scores 5** — the mark every panel on the page is read against.
A rater's score is **5 + gain − habit cost + noise**, rounded and held inside 0–10. **Gain** is how much
better the redesign genuinely is: **+1.2** for the good one, **−0.5** for the deliberately worse one in
section 4. **Habit cost** is **0.85 × log10(1 + prior visits)** — what it costs that rater to unlearn
their own fingers — scaled per rater by a factor between 0.75 and 1.25 that averages to one, and shrunk
over the weeks as they retrain. **Noise** is a bell of spread 1.5 points from a seeded generator. That is
the whole model: nothing on this page is asserted, and every figure follows from those four constants.

| Quantity | Value | How it checks out |
|---|---|---|
| The old version's own score | 5 | the mark every panel is read against |
| How much better the good redesign is | +1.2 | the worse one in section 4 is −0.5 |
| Habit cost for a daily user | 2.47 points | 0.85 × log10(1 + 800) = 0.85 × 2.903 |
| What a daily user is expected to score | 3.73 | 5 + 1.2 − 2.47 |
| What a newcomer is expected to score | 6.20 | 5 + 1.2 − 0 |
| The gap habit alone opens | 2.47 points | 6.20 − 3.73, the habit cost itself |
| Prior visits at which the rule flips | about 25 | 0.85 × log10(1 + 25) = 1.20, equal to the gain |
| Daily user on the worse redesign | 2.03 | 5 − 0.5 − 2.47 |
| Newcomer on the worse redesign | 4.50 | 5 − 0.5 − 0 |
| Habit cost still left in week 5 | 1.11 points | 2.47 × exp(−4/5); first week under the 1.2 gain |
| Habit cost still left in week 10 | 0.41 points | 2.47 × exp(−9/5), so a score of 5.79 |

Two reconciliations a reader should have. The rule's own flip point is **about 25 prior visits**, where
the habit cost equals the 1.2 gain exactly; section 2's chart prints **17** because it interpolates its
own sampled line, whose 25-visit group of 300 landed at 4.8 rather than 5.0. And small panels wobble
around the rule: section 1's forty-a-side average **3.3 and 6.5** against an expected 3.7 and 6.2, while
the panels of 300 and 400 in sections 2 to 4 all sit within 0.2 of it.

Section 5 adds a second rule, the stopwatch, because opinion is being checked against an outcome. A task
takes **42 × (1 − 0.09 + 0.22 × exp(−(week − 1) / 1.5))** seconds, so the old version's **42 seconds** is
the mark, the redesign settles at **38.2** seconds (42 × 0.91) once learned, and it first comes in under
42 in **week 3** — 42 × (0.91 + 0.22 × exp(−2/1.5)) = 40.7, against 43.0 in week 2.

## 1. Two Rater Groups, One Redesign, Opposite Verdicts

Tags: core idea | two groups | same design

- **The setup** — one redesign, one 0–10 score sheet, two rater groups scored independently
- **Group one** — forty people who used the old version every working day for two years
- **Group two** — forty people meeting either version for the first time this morning
- **The mark to beat** — the old version scores five out of ten from both groups alike
- **What the daily users said** — an average of 3.3, and 34 of the forty put it below the old
- **What the newcomers said** — an average of 6.5, and only 3 of the forty rated it down
- **What differs between the groups** — nothing but the hours already spent on the old version
- **What the daily users measured** — the cost of unlearning their own fingers, not the design

**Key point:** The two groups saw an identical design and split by more than three points. A verdict that moves that far on the rater's history is measuring the rater, not the thing being rated.

*Illustrative Example — eighty seeded score sheets; both averages and both counts are read back out of the plotted bars.*

## 2. The Score Falls as the Habit Grows

Tags: the dial | where it flips | habit only

- **One redesign, seven groups** — sorted only by how often each rater used the old version
- **Nobody in any group** — is told which version is newer or what anyone else scored
- **First-timers** — have no habit to protect, and they hand it 6.0 out of ten
- **Five prior visits** — 5.5, a mild preference, and the redesign is still the better one
- **A hundred prior visits** — 4.6, and the redesign has quietly become the worse one
- **Two thousand visits** — 3.3, a fall of 2.7 points with nothing changed about the design
- **Where the verdict turns** — around 17 prior visits, past which habit outweighs the gain
- **The dial** — the score is set by hours spent on the old version, not by the new one

**Key point:** Prior exposure alone walks the verdict from clearly better to clearly worse. Anywhere past the turning point, "I think this is worse" and "I have used the old one a lot" produce the same sentence.

*Illustrative Example — seven seeded panels of 300; every printed score and the turning point are computed at render time.*

## 3. Ten Weeks Later, Nobody Changed the Design

Tags: it wears off | the proof | launch week

- **The launch survey** — the daily users score the new version 3.6, well under the old five
- **Nothing shipped after that** — no fixes, no rollback, not one changed pixel for ten weeks
- **The same people, asked weekly** — the score climbs on its own as their hands relearn it
- **Week five** — level with the old version again, off the identical score sheet
- **Week ten** — 5.8, comfortably above the version they were defending at launch
- **The rise** — 2.2 points, bought entirely by practice rather than by any design work
- **What it proves** — the launch score was a bill for retraining, and the bill got paid off
- **What it prevents** — rolling back in week two on a number that was about to fix itself

**Key point:** A score that recovers while the design sits untouched was never a reading of the design. The recovery is the receipt: what the launch survey priced was the transition, and transitions end.

*Illustrative Example — ten seeded weekly surveys of the same 300 daily users; the launch score, the recovery week and the rise are all read off the drawn line.*

## 4. A Worse Redesign Draws the Same Boos

Tags: cuts both ways | no signal | the real problem

- **A second redesign** — genuinely worse than the old version, shown to the same two groups
- **Daily users, better redesign** — 72 in a hundred call it worse than what they had
- **Daily users, worse redesign** — 94 in a hundred, only 22 points away from the good one
- **Newcomers, better redesign** — 14 in a hundred call it worse
- **Newcomers, worse redesign** — 53 in a hundred, a 39-point gap that separates the two cases
- **Why the boos sound alike** — habit swamps quality, so both land as the same complaint
- **The practical problem** — a group that boos everything cannot say which one to keep
- **Where the signal went** — to the group with nothing to unlearn, whose answers still split

**Key point:** The bias is not that daily users dislike change — it is that they dislike improvement and damage by almost the same amount. Their reaction is honest and nearly useless, because it barely moves when quality does.

*Illustrative Example — four seeded panels of 400, one per group-and-redesign pair; every share and both gaps are counted at render time.*

## 5. Telling Worse Apart from Merely Different

Tags: the method | outcomes not opinions | let time pass

- **The question** — is this worse, or is it only different from what my hands expect
- **What opinion cannot settle** — both answers feel identical from the inside, at any exposure
- **The measurement that can** — time the same task on both versions and count the fumbles
- **The old version** — took 42 seconds a task, the mark the new one has to beat
- **Week one on the new one** — slower, because the users are still hunting for everything
- **Week three** — already quicker than the old version on the clock, for the same people
- **Their opinion in week three** — still below the old version, and it stays there in week four
- **The two-week window** — the clock says keep it, the group says bin it, and the clock is right
- **The method** — measure outcomes not opinions, and let a few weeks pass before you ask

**Key point:** Two things break the tie that a survey cannot. Measure what people accomplish rather than what they report, and wait long enough for retraining to finish. A dislike that survives both is about the design.

*Illustrative Example — ten seeded weeks of task timings and score sheets from the same 250 daily users; both crossing weeks and the width of the window are read off the drawn lines.*
