# Home Router DNS Hijacking

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Home Router DNS Hijacking

**Subtitle:** Every name you type gets looked up in a phonebook your router chooses — corrupt the phonebook and the right name delivers you to the wrong place.

**Intro callout (blue-left-border box):** The middleman does not touch your device or the bank's servers; it edits one setting on the box in the hallway closet, and every device in the house follows it.

## 1. The setup: names are looked up, not known

Typing a site name triggers a lookup before any trip.

- **Fact:** name goes in, a numeric address comes out (DNS).
- **Mechanism:** devices ask the service the router names.
- **Fact:** the router hands that choice to every device.
- **Fact:** nobody checks the phonebook's honesty by default.
- **Fact:** the answer that returns is simply trusted.

**Key point (red-left-border box):** **Mechanism:** one box in the hallway closet picks the phonebook for the whole house.

### Visualization (canvas `c1`, 720×300)

Honest lookup flow — laptop asks, router forwards, lookup service answers, browser follows the numeric address. All green/blue.

- **Title (bold 13px `#1a5276`, top center):** "An honest lookup: name in, numeric address out".
- **Question line (11px `#2980b9`, centered, y=48):** "\"where is my-bank.example?\"".
- **Boxes** 150×70 at y=70 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 13px in box color centered at y=100; sub-line 10px `#666` centered at y=120):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Laptop | types my-bank.example | #1a5276 | 40 |
  | Router | hands out the lookup choice | #2980b9 | 285 |
  | Lookup service | the phonebook (DNS) | #27ae60 | 530 |
- **Forward arrows:** `#2980b9` width-1.5 right-pointing arrows with filled heads at y=90, from (190,90) to (277,90) and from (435,90) to (522,90); 10px `#666` "asks" labels centered above at (233,82) and (478,82).
- **Return arrows:** `#27ae60` width-1.5 left-pointing arrows with filled heads at y=125, from (285,125) to (198,125) and from (530,125) to (443,125); 10px `#666` "answers" labels centered below at (233,140) and (478,140).
- **Answer line (11px `#27ae60`, centered, y=175):** "\"my-bank.example is at 203.0.113.7\" — a numeric address".
- **Bottom line (bold 12px `#1a5276`, centered, y=225):** "The browser goes wherever the answer points."
- **Second bottom line (bold 12px `#27ae60`, centered, y=245):** "Honest case: trusting the answer works fine."
- **Caption (bottom center, 11px `#999`, y=285):** "No device checks the phonebook's honesty — the answer is simply followed."

## 2. The trick: one setting, every device

The middleman never touches your laptop — only the router.

- **Scene:** the target is the box in the hallway closet.
- **History:** many routers shipped with default admin passwords.
- **History:** unpatched software (firmware) stayed common for years.
- **Mechanism:** one setting names which lookup service to use.
- **Risk:** change that field once, poison every lookup.
- **Risk:** every device in the house follows the router.
- **Mechanism:** the fake service answers most names honestly.
- **Lied:** a chosen few — banks, email — get false addresses.
- **Risk:** the fake page sits at the correct name.
- **Fact:** "check the URL" advice fails against this.

**Key point:** **Risk:** the address bar shows the right name while the page is the wrong machine.

### Visualization (canvas `c2`, 720×300)

Same flow as c1 but the router's lookup field is flagged red, and the fake service splits answers: most names honest (gray), the targeted name routed to the middleman (red). A browser address bar shows the correct name over the wrong destination.

- **Title (bold 13px `#1a5276`, top center):** "One changed setting poisons every lookup in the house".
- **Boxes** height 60 at y=55 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 13px in box color centered at y=81; sub-line 10px centered at y=99, `#666` unless noted):
  | Title | sub-line | color | x | width |
  |---|---|---|---|---|
  | Laptop | asks as usual | #1a5276 | 30 | 130 |
  | Router | lookup service: changed (bold 10px `#e74c3c`) | #e74c3c | 230 | 170 |
  | Fake lookup service | answers most names honestly | #8e44ad | 470 | 210 |
- **Forward arrows:** `#bbb` width-1.5 right-pointing arrows with filled heads at y=85, from (160,85) to (222,85) and from (400,85) to (462,85).
- **Answer path 1 (gray):** 11px `#999` label centered at (360,146): "most names → the honest numeric address"; `#999` width-1.5 left-pointing arrow at y=156 from (550,156) to (170,156).
- **Answer path 2 (red):** bold 11px `#e74c3c` label centered at (360,176): "my-bank.example → the middleman's address (203.0.113.66)"; `#e74c3c` width-1.5 left-pointing arrow at y=186 from (550,186) to (170,186).
- **Address bar:** 10px `#666` left-aligned label at (180,200): "browser address bar"; rectangle x=180 y=205 w=360 h=28, fill `#f8f9fa`, stroke `#666` width 1; bold 12px `#2c3e50` centered text at (360,223): "my-bank.example".
- **Red line (11px `#e74c3c`, centered, y=252):** "right name on screen — wrong machine behind it".
- **Caption (bottom center, 11px `#999`, y=285):** "\"Check the URL\" fails here — the URL is exactly right."

## 3. What stops it: the certificate backstop

One check runs after the lookup — and it is the whole game.

- **Defense:** the padlock system exists exactly for this case.
- **Fact:** the wrong machine lacks valid papers (certificate).
- **Fact:** barring separate compromise, forgery fails.
- **Defense:** the browser throws a full-screen warning instead.
- **Defense:** treat that warning as a stop sign, not an obstacle.
- **Defense:** change the router's admin password.
- **Defense:** update the router's software regularly.
- **Defense:** check which lookup service the router names.
- **Defense:** encrypted lookup (DNS over HTTPS) can skip the router.

**Key point:** **Defense:** clicking through the warning disarms the only alarm that fired.

### Visualization (canvas `c3`, 720×300)

Backstop diagram — wrong destination reached, site presents a certificate, browser compares name vs certificate, mismatch triggers a full-screen warning (red octagon motif).

- **Title (bold 13px `#1a5276`, top center):** "The certificate backstop: right name, wrong papers".
- **Boxes** height 60 at y=60 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 13px in box color centered at y=86; sub-line 10px `#666` centered at y=104):
  | Title | sub-line | color | x | width |
  |---|---|---|---|---|
  | Browser | expects my-bank.example | #1a5276 | 40 | 160 |
  | Middleman's machine | presents its certificate | #e74c3c | 280 | 170 |
  | Name check | name vs certificate | #2980b9 | 530 | 150 |
- **Forward arrows:** `#bbb` width-1.5 right-pointing arrows with filled heads at y=90, from (200,90) to (272,90) and from (450,90) to (522,90).
- **Vertical arrow:** `#bbb` width-1.5 line from (605,120) down to (605,155) with a small filled downward arrowhead ending at (605,160).
- **Octagon:** regular octagon centered at (605,197), radius 34, fill `#e74c3c`; white bold 13px "STOP" centered at (605,201).
- **Mismatch lines (centered at x=300):** 11px `#e74c3c` at y=185: "mismatch — no valid certificate for that name"; 11px `#2c3e50` at y=205: "the browser blocks the page with a full-screen warning".
- **Bottom line (bold 12px `#27ae60`, centered, y=255):** "Treat the warning as a stop sign, not an obstacle."
- **Caption (bottom center, 11px `#999`, y=285):** "The full-screen warning is the man-in-the-middle detector firing."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1a5276` blue = mechanism/fact (Setup, Fact, Mechanism); `#27ae60` green = defense/win (Defense, Win); `#e74c3c` red = risk/loss (Risk, Lied); `#e67e22` orange = scene/context/history (Scene, History, Trend). Key-point boxes open with the same colored bold lead word followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. All data is literal hardcoded coordinates — no Math.random, no Date.now.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- **Content rule:** layman language throughout — a technical term appears at most once, in parentheses; fictional domains only (`.example` style) and documentation-range numeric addresses (203.0.113.x); no named companies or router brands; no realistic passwords anywhere; hedges kept ("many", "historically", "barring separate compromise", "can").
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
