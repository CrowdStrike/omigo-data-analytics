# Overconfidence Bias: Good Enough to Skip the Tests, and Shipping More Bugs

An experienced developer really does write cleaner code, decides it no longer needs testing, and ships more bugs than a beginner who tests. The skill is real; the conclusion drawn from it is not.

## 1. Three Times the Skill, More Bugs Out the Door

Tags: core idea | one release | skill is real

- **The release** — one batch of work, and the only question is how many bugs reach the customer
- **The beginner** — writes 300 bugs into it, which is three times as many as the experienced one
- **The experienced developer** — writes 100, genuinely three times cleaner, and that part is true
- **A test suite** — catches roughly 4 out of every 5 bugs that are actually in the code
- **Beginner who tests** — 300 written, 240 caught, and 60 get out to the customer
- **Experienced developer who tests** — 100 written, 80 caught, and 20 get out
- **Experienced developer who skips** — 100 written, none caught, and all 100 get out
- **So the best coder here** — ships more bugs than the weakest one, by skipping one step
- **What went wrong** — being better at writing was read as being good enough not to check

**Key point:** Writing well and checking your work are two separate steps, and skill only helps with the first. The experienced developer wins the part they are proud of and loses the part that decides what the customer sees.

*Illustrative Example — three cases on one release; the caught and shipped counts follow from the bug counts and the catch rate, computed at render time.*

## 2. Practice Runs Out. Checking Does Not.

Tags: why it feels right | practice has a limit | checking does not

- **Why the belief is not silly** — take any single piece of the work and it probably is fine
- **Roughly four pieces in five** — are clean when the experienced developer writes them
- **But a release has hundreds** — so plenty of pieces carry a bug even at that hit rate
- **The mistake** — something true about one piece gets applied to the whole release
- **Practice genuinely helps** — the bug count drops steeply over the first few years of work
- **Then it levels off** — the drop from year ten to year twenty is small, and after that tiny
- **So there is a limit** — untested work never gets much below about 50 bugs a release
- **A first-year developer who tests** — is already at about that level, in their first year
- **The difference** — practice runs out, while checking removes most of whatever is left

**Key point:** Practice and checking are not two routes to the same place. Practice lowers how many bugs you write and then flattens out; checking removes most of however many you wrote, whatever your level. That is why a lifetime of practice lands where a beginner with tests already is.

*Illustrative Example — an assumed practice curve that flattens rather than reaching zero; both lines and the level-off are computed at render time.*
