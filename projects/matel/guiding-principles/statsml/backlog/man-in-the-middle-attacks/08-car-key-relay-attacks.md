# Car Key Relay Attacks

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Car Key Relay Attacks

**Subtitle:** Nothing is decoded and nothing is forged — two radios simply carry the key's short-range conversation across the distance it was never meant to travel.

**Intro callout (blue-left-border box):** Keyless entry answers one question: "is the key nearby?" The relay attack gives a true answer to the wrong question — the key IS answering, just not from nearby. A pure man-in-the-middle that only extends distance.

## 1. The setup: proximity as permission

Keyless cars unlock when the fob answers a short radio call.

- **Setup:** the car whispers a challenge; the fob replies.
- **Fact:** the fob answers on its own — no button press.
- **Fact:** it replies from a pocket or a hallway shelf.
- **Mechanism:** an answer heard = a key presumed at the car.
- **Fact:** the signal's short range IS the security.

**Key point (red-left-border box):** **Fact:** nothing measures distance — the weak signal is the only fence around the car.

### Visualization (canvas `c1`, 720×300)

Normal-operation diagram: car and fob together inside a small green range circle, challenge/answer arrows both ways.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Normal keyless entry: one short conversation".
- **Range ellipse:** dashed `#27ae60` width-1.5 ellipse centered (360, 150), radiusX 170, radiusY 95 (`setLineDash([6,4])`, reset after); label "short radio range" 11px `#27ae60` centered at (360, 45).
- **Boxes** height 55 at y=125 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 13px in box color centered at y=147; sub-line 10px `#666` centered at y=167):
  | Title | sub-line | color | x | width |
  |---|---|---|---|---|
  | Car | sends a challenge | #1a5276 | 220 | 110 |
  | Fob | answers by itself | #2980b9 | 400 | 100 |
- **Arrows:** `#bbb` width-1.5 horizontal arrows with filled triangular heads — challenge from (335, 140) to (395, 140); answer from (395, 165) back to (335, 165). Labels 10px `#666` centered: "challenge" at (365, 133), "answer" at (365, 158).
- **Bottom line (bold 12px `#e67e22`, centered, y=262):** "The car never measures distance — it only hears an answer."
- **Caption (bottom center, 11px `#999`, y=285):** "Short range is doing all the security work."

## 2. The trick: stretch the whisper

Two people with two plain radio boxes stretch that whisper.

- **Scene:** one box stands near the front door.
- **Scene:** the second box waits beside the car.
- **Mechanism:** box one picks up the fob's faint reply.
- **Mechanism:** box two replays it right at the car.
- **Fact:** the car's challenge rides the same bridge out.
- **Fact:** every message is genuine; only the distance lies.
- **History:** demonstrated by researchers; thefts reported.
- **Risk:** any "radio nearby = present" check can be relayed.
- **Risk:** contactless cards reportedly face the same trick.

**Key point:** **Risk:** nothing is decoded and nothing is forged — the attack only moves radio waves farther than they were meant to go.

### Visualization (canvas `c2`, 720×300)

Relay diagram: house with fob on the left, car on the right, two relay boxes bridging the gap; true versus assumed distance labeled below.

- **Title (bold 13px `#1a5276`, top center, y=20):** "The relay: every message genuine, only the distance is a lie".
- **Boxes** (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 13px in box color; sub-line 10px `#666`):
  | Title | sub-line | color | x | y | w | h | title y | sub y |
  |---|---|---|---|---|---|---|---|---|
  | House | fob on a shelf inside | #8e44ad | 30 | 90 | 130 | 80 | 118 | 138 |
  | Relay box 1 | by the front door | #e67e22 | 205 | 105 | 95 | 50 | 126 | 144 |
  | Relay box 2 | beside the car | #e67e22 | 425 | 105 | 95 | 50 | 126 | 144 |
  | Car | hears a nearby key | #1a5276 | 565 | 90 | 130 | 80 | 118 | 138 |
- **Fob marker:** small box inside the house — fill `#2980b9` at 0.12 alpha, stroke `#2980b9` width 1.5, rect (55, 145, 80, 18); label "Fob" bold 10px `#2980b9` centered at (95, 158).
- **Short arrows:** `#bbb` width-1.5 with filled heads — (160, 130) to (203, 130) and (522, 130) to (563, 130).
- **Carry line:** dashed `#e67e22` width-2 line from (302, 130) to (423, 130) (`setLineDash([7,5])`, reset after); label "genuine messages," 10px `#e67e22` centered at (362, 90) and "carried farther" at (362, 103).
- **True distance:** `#e74c3c` width-1 line from (95, 205) to (630, 205) with 6px vertical end ticks; label 11px `#e74c3c` centered at (362, 224): "true fob-to-car distance — across the street".
- **Assumed distance:** `#27ae60` width-1 line from (550, 248) to (630, 248) with 6px vertical end ticks; label 11px `#27ae60` centered at (455, 252), text "assumed distance: a few steps —" placed to the left of the ticks.
- **Caption (bottom center, 11px `#999`, y=285):** "Nothing decoded, nothing forged — two radios only stretch the conversation."

## 3. What stops it: measure the distance, not the answer

Defenses either silence the key or measure the distance.

- **Defense:** a metal pouch mutes the fob at home.
- **Defense:** some makers ship fobs that sleep when still.
- **Mechanism:** radio cannot be relayed faster than light.
- **Mechanism:** added distance shows up as added delay.
- **Defense:** time the round trip (ultra-wideband ranging).
- **Fact:** a relayed answer always arrives a bit late.
- **Defense:** a PIN-to-drive setting adds a second factor.

**Key point:** **Win:** timing the answer measures the distance itself — the one thing a relay cannot fake.

### Visualization (canvas `c3`, 720×300)

Timing diagram: two round-trip bars, direct versus relayed, with a cutoff line the relayed answer cannot beat.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Round-trip time of the answer (illustrative)".
- **Bars** 26px tall (labels right-aligned 11px `#2c3e50` ending at x=180, vertically centered on the bar; track `#f0f0f0` from x=195, 440px max; bar fill = row color at 0.6 alpha with 1px solid stroke in row color; end note bold 11px in row color 8px after the bar end):
  | Label | y | length | color | end note |
  |---|---|---|---|---|
  | direct answer | 95 | 170 | #27ae60 | on time |
  | relayed answer | 155 | 400 | #e74c3c | too late |
- **Cutoff line:** dashed `#666` width-1.5 vertical line at x=455 from y=70 to y=205 (`setLineDash([5,4])`, reset after); label 11px `#666` centered at (455, 58): "cutoff: farther than a parked key can be".
- **Axis note:** 11px `#999` centered at (360, 232): "round-trip time →".
- **Bottom line (bold 12px `#e67e22`, centered, y=258):** "Light speed cannot be beaten — the relay always adds delay."
- **Caption (bottom center, 11px `#999`, y=285):** "Illustrative timings — the check rejects any answer that arrives too late."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1a5276` blue = mechanism/fact (Setup, Fact, Mechanism); `#27ae60` green = defense/win (Defense, Win); `#e74c3c` red = risk/loss (Risk); `#e67e22` orange = scene/context/history (Scene, History). Key-point boxes open with the same colored bold lead word (Fact, Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. All chart data is hardcoded literals — no randomness, no dates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`, bar fill via row color at 0.6 alpha on `#f0f0f0` tracks.
- **Content rules:** layman physical language throughout; a technical term appears at most once, in parentheses (here: ultra-wideband ranging). No manufacturer names anywhere. Hedges kept for unsourced claims ("reported", "reportedly", "some makers"); invented numbers and timings labeled "illustrative".
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
