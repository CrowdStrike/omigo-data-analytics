# Monty Hall Problem

**Page type:** detail page (h2-sectioned two-column obj-table layout: text left 45%, canvas right 55%; philosophy callouts at top and bottom)
**HTML title tag:** Monty Hall Problem — Case Study

**Subtitle:** When someone who KNOWS the answer gives you information, that information is worth something — even when it feels like nothing changed.

## Callout (philosophy box, top)

**You might recognize this from the movie "21" (2008):** Kevin Spacey's character challenges a student with this problem in class. The student switches — and explains why. Most of the class (and most of the audience) can't believe the answer. It's one of the most counterintuitive results in probability. When this problem was published in a newspaper column in 1990, nearly 1,000 PhD holders wrote in to say the answer was wrong. It wasn't.

## 1. The Setup — From the Movie "21"

**Obj-title:** The Game Show (exactly as presented in the film)

Math box 1:

**You're on a game show.** There are 3 doors.

- Behind one door: a **brand new car** 🚗
- Behind the other two: **goats** 🐐🐐

You pick **Door 1**.

The host — who *knows* what's behind every door — opens **Door 3**, revealing a goat. 🐐

He turns to you and asks: **"Do you want to switch to Door 2?"**

Math box 2:

**The question:** Should you switch? Or does it not matter?

**Your gut says:** "Two doors left. Car is behind one. It's 50-50. Doesn't matter."

**The actual answer:** `Switch. You win 2/3 of the time by switching.`

In the movie, Ben Campbell (the student) immediately says "I'd switch." Professor Rosa asks why. Ben explains it. The class is confused. Let's make it clear.

### Visualization (canvas `canvas0`, 720×400)

Pictorial diagram: three game-show doors with states, plus explanatory text below.

- **Title (bold 14px `#1a5276`, top center):** "The Game Show — 3 Doors".
- **Doors:** three 120×180 rectangles, 50px gaps, centered horizontally, top at y=60:
  - Door 1 — state "picked": fill `rgba(41,128,185,0.15)`, border `#2980b9` 3px solid, yellow doorknob circle `#f4d03f` (r=7, right side, mid-height), "?" (36px `#333`) inside; label "Door 1" (bold 14px `#1a5276`) below; note "YOUR PICK" (bold 11px `#2980b9`) below that.
  - Door 2 — state "closed": fill `rgba(39,174,96,0.1)`, border `#27ae60` 3px solid, doorknob, "?" inside; label "Door 2"; note "Switch here?" (bold 11px `#27ae60`).
  - Door 3 — state "opened": fill `rgba(231,76,60,0.1)`, border `#e74c3c` 2px dashed (dash 6/4), no doorknob, "🐐" (36px) inside; label "Door 3"; note "HOST OPENS" (bold 11px `#e74c3c`).
- **Text below doors (centered):** 14px `#333`: "The host opens Door 3 — it's a goat. He always knows where the car is." then "He asks: \"Want to switch to Door 2?\"". Then bold 16px orange `#e67e22`: "Most people say \"doesn't matter — it's 50-50 now.\"" and bold 16px green `#27ae60`: "They're wrong. Switching wins 2 out of 3 times."

## 2. Make It Obvious: Use 1000 Doors

**Obj-title:** The Version That Kills All Doubt

Math box 1:

**Imagine 1,000 doors.** One has a car. 999 have goats.

You pick Door #472. Your chance of being right: `1 in 1,000`.

The host — who KNOWS where the car is — now opens **998 doors**, all showing goats. One door remains closed: Door #817.

It's now: your Door #472 vs Door #817.

**Should you switch?**

Math box 2:

**Of course you switch.**

Your original pick was 1/1000. That hasn't changed — you picked it blind.

The host then *carefully avoided* Door #817 while opening 998 others. Why did they skip that one? Because `they know the car is there`.

Door #817 carries the weight of all 999 other possibilities = `999/1000 chance`.

Now shrink it back to 3 doors. Same logic, just less dramatic:

- Your pick: 1/3 (picked blind)
- The remaining door after host reveals: 2/3 (concentrated from the other two)
- Switching wins 2/3 of the time. Always switch.

### Visualization (canvas `canvas1`, 720×400)

Two-stage diagram: strip of 70 tiny doors (representing 1,000) collapsing to two big doors.

- **Title (bold 14px `#1a5276`, top center):** "1,000 Doors → Host Opens 998 → Two Remain".
- **Top strip:** 70 small doors (8×50 px, 1px gap) starting at x=40, y=50. Door index 32 solid blue `#2980b9` (your pick), index 55 solid green `#27ae60` (remaining door); all others faded red `rgba(231,76,60,0.2)` with an X drawn in `rgba(231,76,60,0.5)`.
- **Strip labels (11px, centered):** "You" in `#2980b9` under door 32; "Last" in `#27ae60` under door 55; red `#e74c3c` line centered under strip: "← 998 doors opened (all goats) →".
- **Down arrow:** `#1a5276` 2px vertical arrow at page center around y≈140–170.
- **Bottom:** two big doors 100×140 at y≈190: left blue `#2980b9` fill, `#1a5276` 2px border, yellow doorknob `#f4d03f`, white text: "#472" (bold 16px), "YOUR PICK" (13px), "1/1000" (bold 20px); right green `#27ae60` fill, same border/knob, white text: "#817", "SURVIVED", "999/1000". Between them: "vs" (bold 24px `#1a5276`).
- **Bottom label (13px `#1a5276`, centered):** "Switch. Obviously."

## 3. Why It Feels Wrong

**Obj-title:** The Intuition Trap

Math box 1:

**Your brain says:**
"Two doors left. Car is behind one. 50-50."

**Why that's wrong:**
The two doors did NOT arrive at this moment equally. One was chosen blindly (yours). The other *survived a deliberate elimination process* by someone who knows the answer.

They are not equivalent. Their histories are completely different.

Math box 2:

**The key insight:** The host is NOT opening doors randomly. They are *forced* to avoid the car. This constraint makes their action informative.

If the host opened doors **randomly** (and might accidentally reveal the car), then switching wouldn't help. It's the host's KNOWLEDGE that creates the asymmetry.

Bullets:

- **What changed:** The host's knowledge leaked into the game through their action
- **What didn't change:** Your original pick is still 1/3 — no new information about YOUR door was revealed
- **The transfer:** The 2/3 that was spread across two doors concentrated into one door

### Visualization (canvas `canvas2`, 720×400)

Before/after probability-flow diagram with three doors per stage.

- **Title (bold 14px `#1a5276`, top center):** "Where Does the Probability Go?".
- **Stage 1 (label "BEFORE (you pick Door 1)", 12px `#666`, y≈55):** three 80×70 door boxes centered (x offsets −130, −40, +50 from center); Door 1 fill `rgba(41,128,185,0.3)`, Doors 2–3 fill `rgba(26,82,118,0.15)`; all bordered `#1a5276` 1.5px; each shows "1/3" (bold 16px `#1a5276`) and caption "Door 1"/"Door 2"/"Door 3" (11px).
- **Transition arrow:** vertical `#1a5276` 2px arrow between stages with red `#e74c3c` 12px label "Host opens Door 3 (goat)".
- **Stage 2 (label "AFTER (host opens Door 3)", 12px `#666`):**
  - Door 1: fill `rgba(41,128,185,0.3)`, border `#2980b9` 2px, "1/3" bold 16px `#2980b9`, caption "Your pick".
  - Door 2: fill `rgba(39,174,96,0.3)`, border `#27ae60` 2px, "2/3" bold 16px `#27ae60`, caption "SWITCH HERE".
  - Door 3: fill `rgba(231,76,60,0.15)`, border `#e74c3c` 2px, "0" bold 16px `#e74c3c`, caption "Opened (goat)".
- **Flow arrow:** dashed orange `#e67e22` (dash 5/3, 2px) quadratic curve from the top of Door 3 to the top of Door 2, labeled "1/3 transfers →" (11px orange).
- **Bottom explanation (13px `#1a5276`, centered, two lines):** "Your door stays at 1/3. The eliminated door's probability" / "flows to the remaining door → it becomes 2/3."

## 4. Where This Pattern Appears in Real Life

**Obj-title:** Information From Knowledgeable Actors

Math box 1:

**Hiring:**
You shortlist 3 candidates. A reference check eliminates one. The remaining two are NOT equal — the one who survived the check has higher expected quality, because the check was *informed*, not random.

Math box 2:

**Debugging:**
3 possible causes for a bug. You test one scenario and it's NOT the cause. The remaining two are NOT 50-50 — if your test was well-designed (targeted at the most likely cause), the untested options now carry different weights.

Math box 3:

**Due diligence:**
10 investment options. An expert eliminates 8 as bad fits. The remaining 2 aren't "equal random picks" — they survived expert scrutiny. Switching TO an expert-vetted option is usually better than sticking with your gut pick.

Bullets:

- **General rule:** When someone with knowledge eliminates options for you, the survivors carry more weight than your initial uninformed choice
- **Exception:** Only works if the eliminator has real knowledge. Random elimination teaches you nothing.

### Visualization (canvas `canvas3`, 720×400)

Two result blocks comparing STAY vs SWITCH over a simulated 1,000 games, with insight text below.

- **Title (bold 14px `#1a5276`, top center):** "Simulation: 1,000 Games Played".
- **STAY block (left):** 260×180 rectangle at y=60, fill `#fef5f5`, border `#e74c3c` 2px. Contents centered in red `#e74c3c`: "STAY Strategy" (bold 16px), "33%" (bold 48px); then 14px `#666`: "~333 wins out of 1000"; then 12px `#999`, two lines: "You win only when your" / "original blind pick was right".
- **SWITCH block (right):** 260×180 rectangle, fill `#f0fff4`, border `#27ae60` 2px. Contents centered in green `#27ae60`: "SWITCH Strategy" (bold 16px), "67%" (bold 48px); then 14px `#666`: "~667 wins out of 1000"; then 12px `#999`: "You win whenever your" / "original pick was WRONG (2/3)".
- **Insight text (centered below blocks):** 14px `#1a5276`: "The beautiful flip:"; then 13px `#333`, three lines: "STAY wins when your original pick was right (1/3 of the time)" / "SWITCH wins when your original pick was WRONG (2/3 of the time)" / "Since you're wrong 2/3 of the time picking blindly...switching wins 2/3."
- **1000-door comparison (12px `#666`, two lines):** "With 1000 doors: STAY = 0.1% win rate, SWITCH = 99.9% win rate." / "Same logic, just more obvious at scale."

## Callout (philosophy box, bottom)

**One sentence:** Your uninformed pick locks in at its original probability. When a knowledgeable actor eliminates alternatives, the remaining option inherits all the probability mass they didn't give to you. Switching captures that transferred probability.

## Regeneration instructions

- **Layout:** case-study detail page. h1, `.subtitle`, `.philosophy` callout, then per numbered section: `<h2>` (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by an `.obj-table` (full-width, one `<tr>`; left `<td>` 45% with `.obj-title` + `.math-box` blocks + optional paragraph/bullets, right `<td>` 55% centered holding the canvas). Closing `.philosophy` callout at the end. No nav bar, no back/home links.
- **Math boxes:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; inline `code` on `#eef2f7`, padding 2px 6px, radius 3px.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; obj-table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; ul 0.9em `#333`, margin `8px 0 8px 20px`.
- **Canvas:** four canvases (`canvas0`–`canvas3`), each intrinsic 720×400; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. All are drawn pictorial diagrams (doors, blocks, arrows), not data charts. Chart fonts `-apple-system, BlinkMacSystemFont, sans-serif`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, link blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, doorknob yellow `#f4d03f`, block backgrounds `#fef5f5`/`#f0fff4`, gray text `#666`/`#333`/`#999`.
