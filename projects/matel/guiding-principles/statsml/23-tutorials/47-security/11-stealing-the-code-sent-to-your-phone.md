# Stealing One-Time Codes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Stealing One-Time Codes

**Subtitle:** A one-time code is a bearer token — whoever holds it is let in, so its safety is a property of the channel that delivered it, not of how careful the user is

## What the Six Digits Actually Prove

**Tags:** `core idea` (blue), `bearer token` (orange), `possession vs identity` (green)

- **The check** — Alice's bank sends six digits; typing them back is the entire proof the login asks for
- **What it proves** — that whoever typed them held the code inside its short validity window, and nothing more
- **Bearer token** — like a paper cinema ticket: the holder is admitted, and the ticket never asks who is holding it
- **Not identity** — the code carries no name, no device fingerprint, and no record of who triggered the login
- **Short life** — a roughly 30-second window shrinks the opportunity, but a live theft finishes well inside it
- **Transferable** — anything a human can read aloud or retype can be moved to a place it was never meant to reach
- **The real question** — so it is not "is Alice careful" but "who else can come to hold this code at all"

*Example (italic):* The code's holder is standing at the bank's login page; the bank verifies that, then assumes the holder is Alice.

**Key point:** A one-time code verifies possession, not identity — which makes its security a property of the delivery channel rather than of the user's diligence.

### Visualization (canvas `c1`, 720×300)

Schematic: the code box on the left, one green "proves" box and one red "does not prove" box on the right, arrows from the code to each.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "One Code, One Fact Proved — and Three Left Open".
- **Code box:** violet rounded box at x=34, y=118, 168×64, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, 8px radius; centered lines — bold 13px `#4a3aa7` "the six digits" at y=144, 12px `#2c3e50` "valid ~30 seconds" at y=164.
- **Proves box:** green rounded box at x=300, y=58, 386×52, fill `rgba(0,131,0,0.10)`, 2px `#008300` border; bold 12px `#008300` "PROVES" left-aligned at (314, 78); 13px `#2c3e50` "someone holds the code right now" left-aligned at (314, 98).
- **Does-not-prove box:** red rounded box at x=300, y=142, 386×118, fill `rgba(231,76,60,0.09)`, 2px `#e74c3c` border; bold 12px `#e74c3c` "DOES NOT PROVE" left-aligned at (314, 164); three 13px `#2c3e50` lines left-aligned at x=322, y=188 / 212 / 236: "who that someone is", "which device it reached", "that the holder wanted this login".
- **Arrows:** 3px `#4a3aa7` line from (204, 138) to (292, 86) with an arrowhead, and 3px `#4a3aa7` line from (204, 162) to (292, 196) with an arrowhead (helper `arrowHead` at the line's angle).
- **Annotation (bold 13px violet `#4a3aa7`, left-aligned at (34, 214), wrapped over two lines at y=214 and y=232):** "whoever holds it" / "is treated as the owner".
- **Caption (12px `#444`, bottom right):** "illustrative example".

## Who Else Can Come to Hold the Code

**Tags:** `channel security` (blue), `SMS weakest` (red), `where it's used` (orange)

- **SMS** — a phone number is a reassignable account at a carrier, not a device welded to Alice's hand
- **Number takeover** — a port-out or reassignment moves the number, and the code then lands on another phone (SIM swap)
- **Nothing to notice** — that request targets the carrier's process, so Alice has no step to be careful about and never sees the code
- **Email codes** — a code mailed to an inbox inherits that inbox's entire security, which is itself often just a code
- **Authenticator app** — the digits are computed offline from a stored seed, so no carrier and no network ever carry them
- **Push approval** — nothing is retyped, but approval collapses to a single tap, and taps can be requested over and over
- **Hardware key or passkey** — the device signs a challenge, so no transmissible secret exists for anyone to obtain
- **The ladder** — counting the parties who can come to hold it: SMS 5, email 4, authenticator app 2, push 1, passkey 0

*Example (italic):* Alice's SMS code can be held by her, her carrier's systems, whoever moves the number, a lock-screen onlooker, or her synced laptop (party counts illustrative).

**Key point:** Rank channels by how many parties can obtain the secret, and the ordering falls out immediately — the only channels that resist theft are the ones with no transmissible secret at all.

### Visualization (canvas `c2`, 720×300)

Horizontal bar ladder: one row per delivery channel, bar length = number of distinct parties who can come to hold the code, with the parties named inside the bar.

- **Title (bold 15px, `#1a5276`, top center, y=22):** "How Many Parties Can Come to Hold the Code".
- **Geometry:** row labels 12px `#2c3e50` right-aligned ending at x=190; bars start at x=200, 94px per party (5 parties = 470px), bar height 26px; row tops at y = 56, 100, 144, 188, 232.
- **Rows (hardcoded literal array, no randomness):**
  | Channel | parties | bar px | parties named (11px, inside bar) | fill | border |
  |---|---|---|---|---|---|
  | SMS code | 5 | 470 | you · carrier systems · number takeover · lock screen · synced device | `rgba(231,76,60,0.20)` | `#e74c3c` |
  | emailed code | 4 | 376 | you · mail provider · inbox password holder · inbox's own recovery | `rgba(217,89,38,0.20)` | `#d95926` |
  | authenticator app | 2 | 188 | you · whoever unlocks the phone | `rgba(201,133,0,0.22)` | `#c98500` |
  | push approval | 1 | 94 | you (one tap) | `rgba(42,120,214,0.20)` | `#2a78d6` |
  | hardware key / passkey | 0 | 0 | — | — | `#008300` |
- **Inside-bar text:** 11px `#2c3e50`, left-aligned at bar start + 8px, vertically centered in the bar.
- **Count labels:** bold 13px in the row's border color, left-aligned at bar end + 10px, vertically centered: "5", "4", "2", "1"; the passkey row instead prints bold 13px `#008300` "0 — no transmissible secret exists" at x=210.
- **Annotation (bold 13px `#e74c3c`, left-aligned at (200, 290)):** "SMS: the parties are the carrier's, not the user's".
- **Caption (12px `#444`, top right just under the title, y=40):** "party counts illustrative".

## Nobody Guesses a Six-Digit Code

**Tags:** `worked example` (blue), `redo by hand` (green), `illustrative` (orange)

- **The space** — six digits means 10^6 = 1,000,000 possible values, from all zeros to all nines
- **The allowance** — assume the site permits 5 wrong attempts before locking, all inside one 30-second window
- **The guess odds** — 5 chances out of 1,000,000 is 5 ÷ 1,000,000 = 0.000005, which is 0.0005%, or 1 in 200,000
- **The theft odds** — obtaining the same code through its delivery channel is illustrated here at 1 in 5, or 0.2
- **The ratio** — 0.2 ÷ 0.000005 = 40,000, so the channel route is four orders of magnitude more likely
- **The honest conclusion** — the code's arithmetic is fine; the delivery is the part that fails
- **The wrong lever** — a seventh digit takes guessing to 1 in 2,000,000 and leaves the 1-in-5 route untouched

*Example (italic):* Lengthening the code divides an already-negligible number by ten, while the route attackers actually use does not move at all (rates illustrative).

**Key point:** The guessing baseline is 1 in 200,000 and the channel baseline is orders of magnitude higher, so hardening the code's length is not the lever — hardening its channel is.

### Visualization (canvas `c3`, 720×300)

Two horizontal bars on a log-scale probability axis: guessing the code versus obtaining it from the channel.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "Guessing the Code vs Obtaining It (log scale)".
- **Axis:** log10 probability from 10^-6 to 10^0, origin x=210, plot width 440, so one decade = 440/6 px; baseline 2px `#999` at y=232 from x=210 to x=650; gridlines 1px `#e5e9ef` at every decade from -6 to 0; 12px `#444` centered tick labels only at log10 = -6, -4, -2, 0 reading "1 in 1,000,000", "1 in 10,000", "1 in 100", "certain", placed at y=252; 12px `#444` axis caption "chance of the attacker obtaining the code" centered at (430, 276).
- **Data (computed in JS at render time, not hardcoded strings):** `guessP = 5 / 1000000` and `theftP = 0.2`; bar right edges from `x(p) = 210 + (Math.log10(p) + 6) * (440 / 6)`.
- **Guess bar:** top y=96, height 30, from x=210 to x(guessP) ≈ 261, fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border; 12px `#2c3e50` row label right-aligned at (198, 115) reading "guess it: 5 tries in 1,000,000"; bold 13px `#2a78d6` value label at bar end + 10px built at render time as `"1 in " + Math.round(1 / guessP).toLocaleString('en-US')`.
- **Theft bar:** top y=166, height 30, from x=210 to x(theftP) ≈ 599, fill `rgba(231,76,60,0.20)`, 2px `#e74c3c` border; 12px `#2c3e50` row label right-aligned at (198, 185) reading "obtain it from the channel"; bold 13px `#e74c3c` value label at bar end + 10px built at render time as `"1 in " + Math.round(1 / theftP)`.
- **Ratio annotation (bold 13px `#e74c3c`, centered at (430, 62)):** built at render time as `"the channel route is ~" + Math.round(theftP / guessP).toLocaleString('en-US') + "× more likely"`.
- **Caption (12px `#444`, bottom right):** "theft rate illustrative; guess rate computed".

## An Extra Gate, or an Alternative Door?

**Tags:** `common mistake` (red), `AND vs OR` (violet), `recovery paths` (green)

- **The belief** — adding a code is assumed to make the account twice as hard to enter: password AND code
- **AND is a gate** — two requirements in series means an attacker must satisfy both, so the bar genuinely rises
- **OR is a door** — if that same code can also reset the password, the code alone becomes a complete route in
- **Only downward** — an alternative path can never raise the bar; the account is as strong as its weakest route
- **Master key** — when recovery accepts an SMS code as sufficient proof, the phone number outranks the password
- **Same shape** — this is exactly the structure that makes "forgot password" flows deserve the login's scrutiny
- **The real fix** — remove SMS from the recovery paths; adding a second factor while leaving that door open is not enough
- **What to keep** — a code that only ever gates a login, and never authorizes a reset, stays an AND and keeps its value

*Example (italic):* Alice's 16-character password is irrelevant the moment her account will reset itself for anyone holding a code sent to her number.

**Common mistake:** Counting a one-time code as a second gate when it is also a recovery credential. If the code can reset the account, it is an OR branch, and an OR branch can only weaken what it is added to.

### Visualization (canvas `c4`, 720×300)

Two side-by-side logic diagrams: the intended AND (both factors required) and the actual OR (login path or recovery path).

- **Title (bold 15px, `#1a5276`, top center, y=24):** "Extra Gate (AND) or Alternative Door (OR)?".
- **Panel divider:** 1px `#e5e9ef` vertical line at x=364 from y=54 to y=250.
- **Left panel header (bold 13px `#008300`, centered at (190, 54)):** "as intended: AND".
- **Left panel boxes** (8px radius, 12px `#2c3e50` centered text): "password" at x=48, y=76, 118×38, fill `rgba(0,131,0,0.10)`, 2px `#008300`; "the code" at x=48, y=142, 118×38, same style; gate box "AND" at x=196, y=98, 66×58, fill `rgba(0,131,0,0.16)`, 2px `#008300`, bold 13px `#008300` centered text; "account" at x=176, y=200, 118×38, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`.
- **Left arrows (2px `#008300`, with arrowheads):** (166,95) → (192,118); (166,161) → (192,140); (229,156) → (229,196).
- **Right panel header (bold 13px `#d95926`, centered at (540, 54)):** "in practice: OR".
- **Right panel boxes:** "password + code" at x=390, y=76, 160×38, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`; "code alone (reset)" at x=390, y=142, 160×38, fill `rgba(231,76,60,0.14)`, 2px `#e74c3c`; gate box "OR" at x=584, y=98, 66×58, fill `rgba(217,89,38,0.16)`, 2px `#d95926`, bold 13px `#d95926` centered text; "account" at x=558, y=200, 118×38, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`.
- **Right arrows (with arrowheads):** 2px `#2a78d6` (550,95) → (580,118); 3px `#e74c3c` (550,161) → (580,140); 2px `#d95926` (617,156) → (617,196).
- **In-chart note (12px `#e74c3c`, centered at (470, 192)):** "the weaker branch decides".
- **Annotation (bold 13px violet `#4a3aa7`, centered at (360, 272)):** "an added path can only lower the bar — never raise it".
- **Caption (12px `#444`, bottom right, y=294):** "logic schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`, violet `rgba(74,58,167,0.12)`/`#4a3aa7`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers: `roundBoxTL` for rounded boxes, `arrowHead` for arrowheads.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is used only for the genuine alarm states (SMS delivery, the reset branch, the "does not prove" box).
- **Data:** no randomness anywhere. The party ladder `[5, 4, 2, 1, 0]` and the 1-in-5 channel-theft rate are invented and labeled illustrative. Everything about the guess baseline is computed in JS from `guessP = 5 / 1000000`: the "1 in 200,000" label, and the ratio `theftP / guessP = 40,000`. Text and chart must agree to the digit: 10^6 = 1,000,000; 5 ÷ 1,000,000 = 0.000005 = 0.0005% = 1 in 200,000; 0.2 ÷ 0.000005 = 40,000.
- **Framing:** defensive throughout. The page explains why a delivered code is a bearer token and how channels differ in exposure, so defenders can remove SMS from recovery paths; it gives no procedure for taking over a phone number and never prints a specimen code value. Fictional names only (Alice), no carrier or company names.
- **Scope boundaries (avoid overlap with sibling pages):** the phishing relay page, the know/have/are taxonomy, the push-fatigue drill, and the spoofed fraud-desk phone call are covered elsewhere and are deliberately out of scope here. This page's spine is channel exposure — who else can come to possess the code — plus the guess-versus-theft baseline and the AND/OR structure.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
