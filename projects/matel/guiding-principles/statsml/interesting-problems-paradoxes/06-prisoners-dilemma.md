# Prisoner's Dilemma

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Prisoner's Dilemma

**Subtitle:** Two people would both be better off cooperating. But rational self-interest leads both to betray. The result is worse for everyone — and neither player made a "mistake."

## Callout (philosophy box)

**Why this is fascinating:** It's not about prisoners. It's about any situation where two parties would benefit from cooperation, but each one has a private incentive to cheat. Price wars, arms races, doping in sports, overfishing — they're all the same game. And the troubling part: betrayal isn't a mistake. It's the rational choice. That's what makes it a dilemma.

## Section 1. The Story

**Obj-title:** The Setup — Simple Enough for Anyone

Math box 1:

**The situation:**

Two suspects are arrested. Police separate them into different rooms. Each is given the same offer:

- If you **betray** your partner and they stay **silent**: `you go free`, they get 10 years.
- If you both stay **silent**: you each get `1 year` (minor charge).
- If you both **betray**: you each get `5 years`.

You can't talk to each other. You don't know what they'll do. What do you choose?

Math box 2:

**Why it's tricky:**

The best COLLECTIVE outcome is both stay silent (1 year each = 2 years total).

But look at it from YOUR side:
- If partner stays silent → I betray = I go free (better than 1 year)
- If partner betrays → I betray = I get 5 years (better than 10 years)

`No matter what the other person does, betraying is better for ME.`

Both players think this way. Both betray. Both get 5 years.
Together they get 10 years — when they could have gotten 2.

### Visualization (canvas `canvas1`, 720×400)

2×2 payoff matrix diagram.

- **Title (bold 14px `#1a5276`, top center):** "The Payoff Matrix".
- **Matrix layout:** origin (160, 80), cells 180×100. Outer headers: "Player B" in bold `#2980b9` centered above; "Player A" in bold `#e67e22` rotated vertical at left. Column headers (13px `#2980b9`): "Stay Silent", "Betray". Row headers (13px `#e67e22`, right-aligned): "Stay Silent", "Betray".
- **Cells (each with fill, 2px colored border, "A: …" line in bold 14px `#e67e22` and "B: …" line in bold 14px `#2980b9`, optional bold 10px label in border color):**
  - Silent/Silent: A "1 year", B "1 year"; background `#f0fff4`, border `#27ae60`, label "BEST TOGETHER".
  - Silent/Betray: A "10 years", B "FREE"; background `#fef5f5`, border `#e74c3c`, no label.
  - Betray/Silent: A "FREE", B "10 years"; background `#fef5f5`, border `#e74c3c`, no label.
  - Betray/Betray: A "5 years", B "5 years"; background `#fff8f0`, border `#e67e22`, two-line label "BOTH BETRAY" / "(Nash Equilibrium)".
- **Bottom explanation (13px `#1a5276`, centered, two lines):** "Green = best for both. Orange = where they actually end up." / "The gap between those two IS the dilemma."

## Section 2. Why "Just Cooperate" Doesn't Work

**Obj-title:** The Rational Trap

Math box 1:

**The naive response:** "Obviously they should both stay silent!"

**The problem:** That requires TRUST. And trust is exploitable.

If I know you'll stay silent, my best move is to betray you (I go free). So trusting me is dangerous for you. And you know this. So you don't trust me. And I know you won't trust me. So I betray. And you betray.

`Mutual betrayal is the only stable outcome.`
Not because players are stupid or evil — because they're rational.

Math box 2:

**This is the Nash Equilibrium:**

Neither player can improve their outcome by changing strategy alone.
- If I switch from betray → silent (while they betray): I go from 5 years to 10 years. Worse.
- If they switch from betray → silent (while I betray): They go from 5 to 10. Worse.

Nobody can unilaterally improve. That's what makes it stable — and tragic.

Bullets:

- **The dilemma:** Individual rationality leads to collective irrationality
- **No player made an error.** Both chose their best available option. Yet the outcome is terrible for both.
- **The lesson:** Rational self-interest doesn't always produce good outcomes. System design matters.

### Visualization (canvas `canvas2`, 720×400)

Decision-analysis diagram: two scenario boxes plus a conclusion box.

- **Title (bold 14px `#1a5276`, top center):** "Your Decision (No Matter What They Do, Betray Is Better)".
- **Scenario box 1 (left, at x=60, y=60, 280×150):** background `#f0f4f8`, border `#2980b9` width 1.5. Header (bold 12px `#2980b9`, centered): "IF partner stays SILENT:". Body (12px `#333`, left-aligned): "I stay silent → 1 year" / "I betray → FREE ✓". Verdict (bold 12px `#27ae60`): "→ Betray is better".
- **Scenario box 2 (right, at x=380, y=60, 280×150):** background `#fef5f5`, border `#e74c3c` width 1.5. Header (bold 12px `#e74c3c`): "IF partner BETRAYS:". Body: "I stay silent → 10 years" / "I betray → 5 years ✓". Verdict (bold 12px `#27ae60`): "→ Betray is better".
- **Conclusion box (centered, 360×70, at y=240):** background `#fff8f0`, border `#e67e22` width 2. Bold 14px `#e67e22` "CONCLUSION:", then 13px lines "Betray is better in EVERY scenario." and "Both players reason the same way. Both betray."
- **Bottom line (12px `#e74c3c`, centered):** "Result: 5 years each (10 total) instead of 1 year each (2 total). Everyone loses."

## Section 3. Where This Happens Every Day

**Obj-title:** It's Not About Prisoners

Math box 1:

**Price wars:**
Two airlines on the same route. Both would profit at $400/ticket. But if one drops to $300, they steal all customers. So both drop. Both lose money. Neither can unilaterally raise prices (the other would steal their share).

Cooperate = keep prices high (both profit).
Betray = undercut (steal share but destroy margins).

Math box 2:

**Doping in sports:**
If nobody dopes → fair competition, everyone healthy.
If I dope and you don't → I win everything.
If we both dope → back to fair competition, but everyone's health is ruined.

Result: everyone dopes. Rational individually. Catastrophic collectively.

Math box 3:

**Climate agreements:**
If all countries cut emissions → everyone benefits.
If I pollute while others cut → I get cheap energy + clean air (from their cuts).
If everyone thinks this way → nobody cuts → everyone suffers.

Bullets:

- **Arms races:** Both sides spend billions on weapons neither wants to use. But unilateral disarmament = vulnerability.
- **Ad spending:** Coca-Cola and Pepsi spend $4B+/year on ads. If both stopped, they'd save money and keep the same market share. Neither can stop alone.
- **Overfishing:** Each boat catches as much as possible. Collective restraint would sustain the fishery. Individual restraint just means someone else catches your share.

### Visualization (canvas `canvas3`, 720×400)

Canvas-drawn 4-column comparison table of real-world examples.

- **Title (bold 14px `#1a5276`, top center):** "The Same Game, Everywhere".
- **Header row (bold 11px, centered):** "Domain" `#1a5276`, "If Both Cooperate" `#27ae60`, "Temptation to Betray" `#e67e22`, "Actual Result" `#e74c3c`. Header underline `#1a5276` width 1.5. Table starts at x=80, columns 160px wide, rows 80px tall; even rows have `#f8fafb` background band.
- **Rows (domain in bold 12px `#1a5276`; cell text 11px, two lines each, colored `#27ae60` / `#e67e22` / `#e74c3c` per column):**
  - Price Wars: "Both charge $400 / (both profit)"; "Undercut to $300 / (steal customers)"; "Race to bottom / (nobody profits)".
  - Doping: "Nobody dopes / (fair + healthy)"; "I dope alone / (I win everything)"; "Everyone dopes / (unfair + unhealthy)".
  - Ads: "Neither advertises / (save $2B each)"; "I advertise alone / (steal market share)"; "Both spend $4B / (same share as before)".
  - Fishing: "Limit catches / (sustainable)"; "I overfish / (big haul today)"; "All overfish / (collapse)".
- **Bottom insight (12px `#1a5276`, centered, two lines):** "In every case: individual rationality produces collective failure." / "The fix: regulation, contracts, reputation, or repeated games where trust can develop."

## Closing callout (philosophy box)

**How it's solved in practice:** Repeated interactions (you'll meet again → cooperation emerges), enforceable contracts (regulation forces cooperation), reputation systems (betray once, never trusted again), and mechanism design (change the payoff structure so betrayal isn't rational). The one-shot dilemma is unsolvable. The repeated game isn't — that's where tit-for-tat, trust, and institutions come from.

## Regeneration instructions

- **Layout:** detail page. h1, `.subtitle`, opening `.philosophy` callout, three numbered `h2` sections each holding a `.obj-table` (one `<tr>`: left `<td>` 45% with `.obj-title` + `.math-box` blocks + bullets, right `<td>` 55% centered canvas), closing `.philosophy` callout.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; p 0.95em `#333`; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; obj-table cells `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links.
- **Component styles:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `code` on `#eef2f7`, padding 2px 6px, radius 3px.
- **Canvases:** three canvases, each 720×400 intrinsic; shared `setupCanvas(id, w, h)` helper scales backing store by `window.devicePixelRatio`, and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
