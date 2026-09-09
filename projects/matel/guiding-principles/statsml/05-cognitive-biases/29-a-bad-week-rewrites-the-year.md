# A Bad Week Rewrites the Year: When You Ask Decides What Happened

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-familiarity-feels-like-quality.html`)
**HTML title tag:** A Bad Week Rewrites the Year — Cognitive Biases

**Subtitle:** Ask someone about the past year a few days after something went wrong and you get a heavy year. Ask the same person in a calm stretch and the same year comes back light.

---

## Section 1 — One Year, Asked Twice

**Tags:** `core idea` (violet), `same year` (blue), `different answer` (magenta)

**Bullets:**
- **The year** — twelve things went wrong, on twelve dated weeks, and that list never changes
- **The first asking** — a few days after the twelfth one, while it is still fresh in mind
- **The second asking** — seven quiet weeks later, same person, same question about the same year
- **Asked while it stings** — 11 of the twelve come back, near enough the whole year
- **Asked in the calm** — 6 come back, and the other six cannot be brought to mind at all
- **What moved between the two** — only the date of the question; not one event was added or removed
- **Why the second answer is shorter** — nothing recent has pulled the older ones back up
- **What this breaks** — any answer collected right after trouble reads as a worse year than the year was

**Key point:** The same twelve months, from the same person, return 11 items or 6 depending on the week you ask. If the timing of the question moves the answer that far, the answer is partly a reading of the question's timing.

**Source note (`.src`):** Illustrative Example — one fixed list of twelve dated events shared by both askings; both counts are tallied from the plotted bars.

### Visualization — canvas `c1`, 720×360

A timeline strip of the twelve dated events across the top with the two asking moments marked on it, then two bar panels below — one per asking moment — each showing how strongly the same twelve memories come to mind, with the line for "close enough to mind to be named" drawn across both.

- **Shared construction (used by every chart on the page):** one fixed event list
  `EVENTS = [4, 9, 13, 18, 22, 26, 30, 35, 39, 43, 47, 51]`, the week number of each thing that went
  wrong in a 52-week year. Both panels, and every chart on the page, read this one array — the claim
  "same year, different answer" only holds if the underlying list is literally the same object.
- **How strongly a memory comes to mind:** event `i` starts at `V[i] = 0.70 + 0.30 × rng()` on its own
  week, seeded `lcg(42)`. Between refreshers it fades as `exp(−Δweeks / 10)`. A refresher at week `tj`
  lifts it back by `0.40 × exp(−(tj − E[i]) / 26)`, capped at its original `V[i]` — so a refresher
  restores an old memory less than a recent one. A memory is countable when its strength is `≥ 0.32`.
- **What is a refresher:** in this chart, every later bad event on the list. Nothing else is applied.
- **Two asking moments:** week **51.5**, a few days after the twelfth event, and week **58**, seven
  quiet weeks later. Same array, same rule, only `t` differs.
- **Computed:** strengths at week 51.5 are 0.27, 0.32, 0.38, 0.44, 0.50, 0.56, 0.67, 0.73, 0.79, 0.74,
  0.70, 0.90 → **11 of 12** countable. At week 58 they are 0.14, 0.17, 0.20, 0.23, 0.26, 0.29, 0.35,
  0.38, 0.41, 0.39, 0.36, 0.47 → **6 of 12**. Both counts are tallied in the draw function and printed
  from those variables.
- **Title (bold 15px `P.ink`, centered, y=21):** "One Year, Twelve Bad Weeks, Two Askings"
- **Timeline strip** (`y = 46`, x from `PX=54` to `w−150`, weeks 0–58): a 1.5px `P.mute` rule with a
  radius-4 `rgba(107,114,128,0.55)` dot per event, week numbers omitted to keep it clean, plus two
  markers — a bold 12px `P.magenta` "asked here" arrow at week 51.5 and a bold 12px `P.blue`
  "asked here" arrow at week 58. Left label bold 12px `P.ink` "THE YEAR AS IT HAPPENED".
- **Two bar panels** stacked below on the same event axis: upper (`TOPY=104`, `BASEY=196`) in magenta
  for the first asking, lower (`TOPY=230`, `BASEY=322`) in blue for the second. Twelve bars each,
  height proportional to strength on a fixed 0–1 scale. Bars at or above the line are solid
  (`rgba(213,81,129,0.55)` stroked `P.magenta`; `rgba(42,120,214,0.50)` stroked `P.blue`); bars below it
  drop to `rgba(107,114,128,0.20)` stroked `P.mute` so the six that vanish are visible as grey stubs.
- **The line:** a dashed (5/4) 1.5px `P.mute` horizontal at 0.32 in both panels, labelled 12px `P.mute`
  "close enough to mind to be named" once, in the upper panel.
- **Panel headers (bold 12px):** `P.magenta` "ASKED DAYS AFTER THE LAST ONE — 11 of 12 come back",
  `P.blue` "ASKED SEVEN QUIET WEEKS LATER — 6 of 12 come back". Both counts printed from the tally.
- **Right callout** at `w−140`: bold 13px `P.ink` "WHAT CHANGED", then bold 19px `P.mute` "nothing" over
  12px `P.mute` "in the year itself"; then bold 13px `P.ink` "THE ANSWER", bold 19px `P.magenta` "11"
  with 12px `P.mute` "right after" beside it and bold 19px `P.blue` "6" with "in the calm" beside it,
  and 12px `P.mute` "bad weeks reported" underneath. Both figures come from the tallies.
- **Axis note (12px `P.mute`, centered under the lower panel):** "the same twelve bad weeks, oldest at
  the left".
- **Caption (bold 13px `P.magenta`, centered, `h−9`):** "One list of twelve. The week you ask picks how much of it comes back."

---

## Section 2 — Each New One Drags the Old Ones Up

**Tags:** `the mechanism` (orange), `no new input needed` (yellow), `arrives in clusters` (blue)

**Bullets:**
- **The common belief** — bad memories are built to last, and good ones are the ones that slip away
- **What actually happens** — a new bad event acts as a reminder and pulls the similar old ones up
- **Being pulled up strengthens them** — each time one is brought to mind it comes back easier next time
- **With no reminders at all** — plain forgetting applies and only 1 of the twelve is still reachable
- **With each new event reminding** — 6 are reachable, from exactly the same twelve weeks
- **Going over it in your head** — does the same lifting, so the pile holds at 11 with no new events
- **Why that matters** — a grievance list can keep itself alive on nothing but attention
- **A side effect** — each new event drags old ones forward, so complaints arrive in bursts, not singly
- **What the bursts look like** — a worsening trend, while the rate of things going wrong has not moved

**Key point:** The pile is not held up by the badness of the memories. It is held up by how often something pulls them back into mind — a new event, or just going over the old ones. Remove the reminders and the ordinary rate of forgetting takes them.

**Source note (`.src`):** Illustrative Example — the same twelve dated events under three reminder rules; every plotted count is tallied at that week from the same shared list.

### Visualization — canvas `c2`, 720×380

Three lines of "how many of the twelve are reachable" against the week of the year, over the identical event list: no reminders, each new event reminding, and going over it in your head on top of that.

- **Same twelve events, same strength rule, same seed as section 1.** The only difference between the
  three lines is which weeks count as a refresher.
- **Line A — no reminders (`P.mute`, dashed):** the refresher list is empty, so each memory only fades.
  It rises as events happen and drifts back down; at week 58 it stands at **1 of 12**.
- **Line B — each new event reminds (`P.orange`):** refreshers are the later events on the list. It
  climbs in steps to a peak of **11** around week 47, holds 11 through week 51, then falls to **6** by
  week 58 as the quiet weeks pass.
- **Line C — plus going over it in your head (`P.violet`):** refreshers are the events plus every second
  week from week 51 onward, standing in for someone turning it over. No new events at all. It holds at
  **11 of 12** all the way to week 58.
- **The steps are the point:** line B jumps at each event week (weeks 9, 13, 18, 22, 26, 30, 35, 39, 43,
  47) — a new event does not add one memory, it lifts several, which is why complaints cluster.
- **Title (bold 15px `P.ink`, centered, y=21):** "How Many of the Twelve Are Reachable, Week by Week"
- **Axes:** plot box `PX=62`, right edge `w−176`, `TOPY=64`, `BASEY=300`. Y 0–12 memories with `P.grid`
  gridlines and 12px `P.mute` labels at 0, 3, 6, 9, 12; rotated 12px `P.mute` "bad weeks reachable".
  X weeks 0–58, ticks every 10 weeks in 12px `P.mute`, labelled "week of the year".
- **Event ticks:** a 6px `rgba(107,114,128,0.5)` tick on the baseline at each of the twelve event weeks,
  labelled once 12px `P.mute` "each tick is a week something went wrong".
- **Lines:** 3px each, drawn A then B then C so C sits on top. A `P.mute` dashed (6/4), B `P.orange`,
  C `P.violet`. End-of-line labels bold 12px in each hue, placed at the right edge: "no reminders — 1",
  "each new event reminds — 6", "plus going over it — 11", each figure printed from the series.
- **Asking markers:** thin dashed 1.5px `P.grid` verticals at weeks 51.5 and 58, labelled 12px `P.mute`
  "asked here" and "asked here", so this chart and section 1 read as one construction.
- **Right panel** at `w−166`: bold 13px `P.ink` "AT THE CALM ASKING", then bold 19px `P.mute` "1" over
  12px `P.mute` "with no reminders"; bold 19px `P.orange` "6" over "events reminding"; bold 19px
  `P.violet` "11" over "and going over it". Below that bold 13px `P.ink` "THE PEAK", bold 19px
  `P.orange` "11" over 12px `P.mute` "reached while the events kept coming" — the peak is taken as the
  maximum of the plotted series, not typed in.
- **Caption (bold 13px `P.orange`, centered, `h−9`):** "The pile does not last because it is bad. It lasts because something keeps lifting it."

---

## Section 3 — The List Goes, the Verdict Stays

**Tags:** `long run` (aqua), `the sting fades` (green), `the record does not` (red)

**Bullets:**
- **Follow the same person on** — no new incidents, just the ordinary passage of four more years
- **Naming the actual weeks** — 6 can be named at the calm asking, 4 a year on, 1 after two years
- **After three years** — none of the twelve can be placed or dated, and the list is simply gone
- **What stays behind** — the one-line conclusion drawn from them, which holds at four fifths and firms up
- **Why that is worse than a list** — a conclusion nobody can trace cannot be checked or argued down
- **What the other person faces** — a verdict with no instances attached, so there is nothing to rebut
- **The sting itself** — fades from about two thirds to near nothing, faster than the record of it does
- **So the folk claim is wrong** — what lasts is the record and the verdict, not the hurt of the event
- **The one exception** — for a truly traumatic event this ordinary fading fails, and that is its own topic
- **Vividness is not accuracy** — a memory recalled sharply and confidently can still be wrong on detail

**Key point:** Over years the incidents stop being nameable while the conclusion drawn from them stays put and firms up. The feeling fades fastest of the three — which is why "they are inconsiderate" can long outlive both the twelve events and any sting attached to them.

**Source note (`.src`):** Illustrative Example — the same twelve events carried forward four years; nameable counts, verdict strength and remaining sting are all computed in the draw function. The faster fade of the sting than of the record is the direction reported in the fading-affect literature (Walker & Skowronski); the vividness-without-accuracy result is Talarico & Rubin (2003).

### Visualization — canvas `c3`, 720×340

Bars for how many of the twelve can still be named, year by year, against two lines over the same years: the strength of the standing verdict, and how much of the original sting is left.

- **Starting point:** the week-58 strengths from section 1 — the calm asking, **6 of 12** nameable. This
  chart begins exactly where that one ends, so the two cannot disagree.
- **Nameable incidents:** each memory's week-58 strength decays over the following years on its own
  slow clock, `H[i] = 25 + 475 × rng()` weeks from a second seeded stream `lcg(7)`, with the same 0.32
  line for countable. Computed: **6, 4, 1, 0, 0** at years 0 through 4.
- **Verdict strength:** formed once from the whole pile as `1 − exp(−ΣV / 6)` = **81%**, then approaching
  certainty as `1 − (1 − v0) × exp(−0.25 y)` — it does not decay, because the conclusion gets restated
  rather than re-derived. Computed: **81%, 85%, 89%, 91%, 93%**.
- **Sting left:** the average of the original vividness values under a plain `exp(−Δweeks / 60)` fade,
  measured from each event's own week and expressed as a share of the pile's original total. Computed:
  **63%, 26%, 11%, 5%, 2%** — it drops far faster than the verdict, and faster than the nameable count.
- **Title (bold 15px `P.ink`, centered, y=21):** "Four Years On: What Can Be Named, What Is Still Believed"
- **Axes:** plot box `PX=64`, right edge `w−178`, `TOPY=62`, `BASEY=252`. Left axis 0–12 incidents in
  12px `P.mute`; right axis 0–100% in 12px `P.mute` printed outside the right edge. Five year slots
  labelled "at the asking", "+1 yr", "+2 yrs", "+3 yrs", "+4 yrs" in 12px `P.mute` beneath. Two header
  lines above the plot: bold 12px `P.aqua` "BARS — bad weeks they can still name" and 12px `P.mute`
  "LINES — read on the right-hand scale".
- **Bars:** one 46px bar per year, `rgba(25,158,112,0.45)` stroked `P.aqua`, height from the nameable
  count on the left axis, with the count in bold 19px `P.aqua` above it. A year with zero nameable
  incidents gets no bar and a bold 12px `P.mute` "none" at the baseline instead of a 1px sliver.
- **Verdict line:** 3px `#e74c3c` with radius-4.5 dots on the right axis, labelled bold 12px `#e74c3c`
  "the verdict — still held, firmer" with its end value printed.
- **Sting line:** 3px `P.green` dashed (6/4) with radius-4.5 dots on the right axis, labelled bold 12px
  `P.green` "the sting — mostly gone" with its end value printed.
- **The crossing worth seeing:** the sting line ends below every bar's worth of evidence while the
  verdict line ends at its highest point; a 12px `P.mute` note under the plot reads "the hurt went
  first, the list second, the conclusion not at all", with no figure asserted in it.
- **Right panel** at `w−168`: bold 13px `P.ink` "AFTER FOUR YEARS", then bold 19px `P.aqua` "0" over 12px
  `P.mute` "weeks they can name"; bold 19px `#e74c3c` "93%" over "certain of the verdict"; bold 19px
  `P.green` "2%" over "of the sting left".
- **Caption (bold 13px `#e74c3c`, centered, `h−9`):** "The conclusion outlives the evidence for it, which is what makes it unanswerable."

---

## Regeneration instructions

- **Template:** copy `05-cognitive-biases/25-familiarity-feels-like-quality.html` verbatim for the
  `<style>` block, the `setup()` canvas helper, the `lcg()` seeded PRNG, the `P` palette object, the
  `__charts` array and its debounced resize tail, the `table.layout` / `text-col` / `viz-col` 50/50
  structure, the `.tags` pill classes, `.key-point` and `.src`. Only the content differs.
- **Structure:** three `.card-section` blocks, each an `<h2>` plus a `table.layout` row of
  `td.text-col` (50%) then `td.viz-col` (50%). The split is fixed — a chart is shrunk with canvas
  `max-width` / height, never by narrowing the viz column.
- **Text column order:** `.tags` row of three pills → `<ul>` of 5–8 bullets (this page runs 8, 9 and 10
  where the mechanism needs the room) → one `.key-point` → one `.src`.
- **Bullet form:** `<li><b>Short bold label</b> — a phrase</li>`, around 90–100 characters total. A
  slight wrap is acceptable; deleting a fact to hit the length is not, so split instead.
- **Canvas:** intrinsic `width="720"` with heights 360, 380, 340. CSS `width: 100%`, capped at 720px by
  `setup()`. Draws registered in `__charts` and re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart headers bold 12–13px; axis and body labels 12px
  floor; callout figures bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`,
  `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`,
  `grid #e5e9ef`. Hard red `#e74c3c` only in section 3, where the verdict line is the failure being
  flagged. Hue family per section: 1 magenta against blue, 2 orange with a violet overlay, 3 aqua bars
  with a red and a green line.
- **One construction runs the whole page.** `EVENTS = [4, 9, 13, 18, 22, 26, 30, 35, 39, 43, 47, 51]`,
  `V[i] = 0.70 + 0.30 × rng()` on seed 42, fade `exp(−Δ/10)`, refresher lift `0.40 × exp(−age/26)`
  capped at `V[i]`, countable at `≥ 0.32`, askings at weeks 51.5 and 58. Section 1's two counts are
  the endpoints of section 2's lines, and section 3 starts from section 1's calm-asking strengths.
  Changing one constant moves every figure on the page, which is the point.
- **Determinism:** no `Math.random()` anywhere. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`),
  seed 42 for the vividness values and seed 7 for section 3's long fade clocks, one fresh stream per
  chart. The event weeks and the two asking weeks are hardcoded literals on purpose — the shape of the
  year and the timing of the questions are what the page teaches, so they must not wander with a draw.
  Every count, share and percentage printed on a canvas is computed inside the draw function from the
  plotted values and printed from that variable.
- **The mechanism must not be simplified.** The claim is not "bad memories last longer". It is that a
  new bad event acts as a reminder that pulls the similar old ones up, and being pulled up strengthens
  them again; with no reminder, ordinary forgetting applies. Going over it in your head counts as a
  reminder, which is why the pile can hold itself up on no new input. Over years the specific incidents
  become unnameable while the verdict drawn from them stays and hardens.
- **The correction is load-bearing.** The well-replicated finding is that the *sting* of a bad memory
  fades faster than the glow of a good one. This page must not contradict it: what lasts is the record
  that it happened and the verdict drawn from it, not the feeling. Trauma is named in one bullet as the
  exception where ordinary fading fails, and is not explained here.
- **Voice.** Plain physical language only. No technical term in any heading or bullet label. The
  fading-affect literature is named once, parenthetically, in section 3's `.src` and nowhere else.
  "Going over it in your head", never the clinical word for it.
- **Scope discipline.** This page is about *when* you ask and what gets dragged forward with the
  question. It must not drift into how bad and good events are weighted against each other — that is
  `26-one-bad-thing-outweighs-five-good`. It must not become about people with bad outcomes searching
  their history harder — that is `15-recall-bias`. It must not become uniform recency weighting — that
  is `10-recency-bias`.
