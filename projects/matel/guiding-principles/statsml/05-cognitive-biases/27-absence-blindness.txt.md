# Absence Blindness: The Baseline Goes Invisible

The thing that always works stops being noticed within a few weeks, and the work that keeps it working never shows up in any number anyone reports.

**The one rule behind every figure on this page.** A **level** is what was actually delivered in a week. The **level people silently expect** starts at **70** — what the service was before any of this — and each week creeps **35% of the way** toward whatever it has just received: **new expected = old expected + 0.35 × (delivered − old expected)**. A week **registers** at all only when delivered sits **4 or more** away from that expected level: above it as a noticed-good week, below it as a complaint. Sections 2 and 3 both run that two-line rule. Section 1 is the special case where the bar is a written threshold instead of a felt one.

| Section | What the weeks are | Which bar is applied |
|---|---|---|
| 1 | 52 weeks each: Team A one 260-minute stop in week 19, Team B 130 stops of 2 minutes | written — a single stop of 5 minutes or more gets an incident report |
| 2 | 24 weeks each: steady hands in 82 every week, wobbling cycles 88, 88, 70, 76, 84, 86 | felt — register only at 4 or more from the expected level |
| 3 | 48 weeks, four quarters aimed at 72 / 78 / 84 / 90 | both — a written bar at 72 and the same felt 4 |

Every quantity the bullets quote, and how it follows from those weeks:

| Quantity | Value | How it checks out |
|---|---|---|
| Minutes in a year | 525,600 | 365 × 24 × 60 |
| Team A minutes offline | 260 | one stop of 260 minutes |
| Team B minutes offline | 260 | 130 stops × 2 minutes each |
| Uptime, either team | 99.95% | (525,600 − 260) ÷ 525,600 |
| Incident reports | 1 and 0 | A's 260 clears the 5-minute bar; none of B's 2s do |
| Both 24-week averages | 82.0 | 24 × 82 = 1968; (88+88+70+76+84+86) × 4 = 1968; 1968 ÷ 24 |
| Steady team's noticed weeks | 3 | gaps 12.0, 7.8, 5.1 clear 4; week 4's is 3.3 and falling |
| Steady weeks registering nothing | 21 | 24 − 3 noticed − 0 complaints |
| Wobbling team's tally | 14 good, 6 bad | each climb reopens the gap past 4, each dip past −4 |
| The perverse ratio | 4.7× | 14 ÷ 3 |
| Quarter averages over 48 weeks | 71.4 up to 90.2 | mean of each quarter's 12 delivered levels |
| The rise | 18.8 points | 90.2 − 71.4 |
| Weeks under the written 72 | 7, 2, 1, 0 | the bar sits still while delivery climbs past it |
| Weeks 4 or more below expectation | 3, 3, 3, 3 | expectation climbs 70 to 86.7 behind delivery |
| Complaints logged all year | 12 | 3 + 3 + 3 + 3 |

Two of those cannot be redone with a pen. Team B's placement of its 130 stops and the 48 weekly levels come from a fixed seeded stream, so the **47 of 52 weeks touched**, the **12-minute worst week** and the four quarter averages are read off that draw. Team B's 260-minute total holds whatever the placement, because 130 stops of 2 minutes is 260 by construction. Everything else above closes by arithmetic.

## 1. The Service That Never Went Down

Tags: two teams | same uptime | nothing to point at

- **The setup** — Team A and Team B, a year each, one agreement, 260 minutes offline apiece
- **Team A, which broke once** — one 260-minute outage in week 19, then a clean run to December
- **Team B, which never broke** — 130 two-minute stops in 47 of 52 weeks, worst week 12 minutes
- **What the totals say** — 99.95% uptime each on a 525,600-minute year, equal to the decimal
- **The write-up rule** — one report per stop of 5 minutes or more, and B's longest ran 2 minutes
- **What that rule produced** — 1 report for Team A, 0 for the team that never stopped at all
- **What Team A can point at** — a report, a fix, a recovery, a named project on somebody's slide
- **What Team B can point at** — nothing, because a service that never stopped leaves no paper

**Key point:** Both teams lost the same 260 minutes and reported the same 99.95% uptime. Only one of them lost them in a shape the write-up rule could see — 1 report against 0 — and that shape, not the downtime, decided which team ended the year with something to show.

*Illustrative Example — 52 seeded weeks per team; both totals, both uptime figures and both report counts are counted off the plotted bars at render time.*

## 2. The Level Stops Being Felt

Tags: same average | felt vs delivered | recovery pays

- **Two teams, half a year** — 24 weeks each, both landing on an average quality of exactly 82
- **The steady team** — hands in 82 every single week, with no better week and no worse one
- **The wobbling team** — cycles 88, 88, 70, 76, 84, 86 four times over, averaging 82 as well
- **What people carry** — a level they silently expect, closing 35% of the gap to delivery each week
- **What registers at all** — only a week sitting 4 or more clear of the level they now expect
- **What happens to the steady team** — expectation reaches 82, and 21 of its 24 weeks go silent
- **What happens to the wobbling team** — every climb reopens the gap: 14 good weeks, 6 complaints
- **The perverse result** — 4.7× as many noticed-good weeks as the steady team, for equal work

**Key point:** A steady level is felt only while it is still new (the effect psychologists call hedonic adaptation). Once what people expect has climbed to 82 and met it, holding 82 forever registers as nothing at all — 21 of 24 weeks silent — while a team that dips and climbs back collects 14 noticed-good weeks on exactly the same average.

*Illustrative Example — two hardcoded 24-week series, chosen so both averages are exactly 82; every count, both averages and the ratio are computed at render time.*

## 3. What It Does to the Numbers

Tags: the measurement | fixed bar | sliding bar

- **The system** — genuinely improves over 48 weeks, a quarterly average of 71.4 rising to 90.2
- **The size of it** — a rise of 18.8 points, far too large to pass off as noise or a lucky quarter
- **Counted against a written bar** — weeks under 72 fall 7, 2, 1, 0 as the bar stays where it is
- **Counted against the felt bar** — a week counts only when it lands 4 or more below expectation
- **What that second count gives** — 3 complaints in every quarter, flat across an improving year
- **Why it stays flat** — expectation climbs from 70 to 86.7 behind delivery, so the gap persists
- **What the log therefore holds** — 12 complaints and no trace of the 18.8-point climb at all
- **What a well-run year looks like** — exactly like a year in which nothing whatsoever happened

**Key point:** If the only thing you log is a departure from what people currently expect, then the bar you are measuring against moves up whenever you improve. An 18.8-point rise cancels itself out of the count, leaving 3 complaints a quarter all year — a record indistinguishable from a system standing still.

*Illustrative Example — 48 seeded weeks (spread 4.5 around quarterly targets of 72 / 78 / 84 / 90); all eight quarterly counts, both quarter averages and the rise are computed at render time.*
