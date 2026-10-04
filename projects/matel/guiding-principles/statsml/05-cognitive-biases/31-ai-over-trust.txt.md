# AI Over-Trust: A Few Good Answers Are Not a Reference Check

Spot-checking a new colleague is sound reasoning, because a clean sample says something about the person. The same spot check on a generator says nothing about the answers you did not read.

## 1. The Same Clean Spot Check, Two Different Meanings

Tags: core idea | five checks | nothing learned

- **The batch** — 100 pieces of work arrive, you read 5 closely, and all 5 come back clean
- **Checking a person** — a person has one skill level, and every item in the batch came out of it
- **What the sample does** — 5 clean items are unlikely from a careless worker, so they rule that out
- **Before the check** — a new colleague could be anywhere from flawless to 30% wrong, so 14.3 bad
- **After 5 clean** — the same arithmetic now expects 10.8% wrong, so 10.3 bad in the 95 unread
- **The check earned** — 28% of the expected trouble removed, without opening the other 95
- **Checking a generator** — there is no skill level, only an error rate that each answer redraws
- **Before and after** — 11.4 bad items among the 95 unread, the identical number both times
- **Why it does not move** — a clean run happens 53% of the time at that rate, so it is unremarkable

**Key point:** With a person, "the first five were fine" is evidence about the sixth, because the sample and the rest share one cause. With a generator there is no shared cause to learn about, so the same clean five leave the estimate exactly where it started.

*Illustrative Example — the person's figures are an exact update on a flat prior over 0–30% error; the generator's are its fixed rate. Both are computed at render time.*

## 2. Twenty Quiet Rounds and the Checking Is Gone

Tags: the ratchet | trust grows | defects do not

- **Each answer** — has 8 things that could be wrong in it, each wrong 10% of the time on its own
- **So most answers** — carry at least one wrong thing 57% of the time, and 0.8 on average
- **Round one** — you are new to this and unsure, so you check 5 of the 8 things and find nothing
- **The rule everyone follows** — a clean round shortens the next check, a bad one resets it to 5
- **Rounds two to five** — clean, clean, clean, clean, and the check has fallen from 5 to 1
- **Rounds five onward** — 11 of the 20 rounds are checked one thing deep, an eighth of the surface
- **What was caught** — 2 of the 13 wrong things across the 20 rounds, so 11 went out unnoticed
- **Had the check held at 5** — 8 of the 13, four times as many, for a little over twice the reading
- **Why the streak felt earned** — checking 5 of 8 leaves a clean round 59% of the time by itself

**Key point:** Nothing about the generator changed across the 20 rounds — the error rate was fixed the whole way. What changed was the checking, and it was cut by the very streak of clean rounds that a fixed error rate produces on its own.

*Illustrative Example — 20 seeded rounds of 8 aspects at a fixed 10% error rate, run twice under the two checking rules; every count and depth is read off the rounds themselves.*
