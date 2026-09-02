# Sensitive Email Theft

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Sensitive Email Theft

**Subtitle:** An inbox read quietly over weeks leaks negotiations, legal advice, deals, and personal history — the thief learns your next move before you make it.

**Intro callout (blue-left-border box):** Nothing is deleted and nothing breaks; the mailbox keeps working for its owner while a second reader keeps up with every thread.

## 1. The inbox is the archive

One mailbox holds years of decisions in a single searchable place.

- **Fact:** years of contracts and advice sit in one place.
- **Fact:** a single search surfaces any old deal term in seconds.
- **Mechanism:** email resets the passwords of most other accounts.
- **Fact:** drafts and threads read like a diary of next moves.
- **Scene:** Alice's legal thread names her walk-away number.
- **Risk:** executive and lawyer inboxes concentrate the most.

**Key point (berry-left-border box):** **Risk:** whoever reads the mailbox reads the archive — and holds the reset keys to most other accounts.

### Visualization (canvas `c1`, 720×300)

Inbox schematic: one large mailbox box containing four layered content rows, each with a sensitivity pill on the right.

- **Title (bold 16px `#01579b`, top center):** "One mailbox — the archive and the keyring in the same place".
- **Mailbox frame:** stroked `#01579b` rect at (60, 40), 600×195, lineWidth 2, fill `#01579b` at 0.04 alpha; header "Alice's mailbox — years, searchable" bold 14px `#01579b` centered at (360, 62).
- **Content rows** (row rect at x=80, width 560, height 30, fill = row color at 0.08 alpha, stroke = row color width 1.5; item text 14px `#2c3e50` left-aligned at x=95, baseline row y+20; pill = rect at x=480, width 145, height 22 at row y+4, fill = row color at 0.12 alpha, stroke = row color width 1.5, pill text bold 13px in row color centered at (552.5, row y+19)):
  | Item | pill text | color | y |
  |---|---|---|---|
  | Signed contracts and terms | sensitive | #ef6c00 | 76 |
  | Threads with the lawyers | privileged | #880e4f | 114 |
  | Offers and negotiating positions | market-moving | #880e4f | 152 |
  | Password reset messages | keys to accounts | #4e342e | 190 |
- **Bottom line (bold 14px `#ef6c00`, centered, y=262):** "One archive, one search box — and it all answers to one sign-in."
- **Caption (bottom center, 13px `#999`, y=285):** "The mailbox is both the diary and the keyring."

## 2. The quiet reader

Access changes nothing on the screen, so nothing looks wrong.

- **Mechanism:** one stolen password or one approval click opens it.
- **Mechanism:** a hidden rule mails copies out indefinitely (forwarding).
- **Fact:** a read-only intruder changes nothing visible.
- **Fact:** the owner's screen looks exactly as it always did.
- **Seen:** leaked threads surface later — deals, disputes, dumps.
- **Risk:** your position is known before you state it.
- **Risk:** a known next move reprices every negotiation.

**Key point:** **Risk:** the copy leaves silently with every new message — the mailbox itself shows no trace of being read.

### Visualization (canvas `c2`, 720×300)

Flow diagram: mailbox feeding a hidden rule feeding a second silent copy, with the owner's unchanged view below.

- **Title (bold 16px `#01579b`, top center):** "Every message goes two ways — only one of them is visible".
- **Top-row boxes** 160×55 at y=55 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at box y+22; sub-line 12px `#666` centered at box y+40):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Mailbox | every new message | #01579b | 50 |
  | Hidden rule | quietly copies each one | #ef6c00 | 280 |
  | Thief's copy | a second, silent inbox | #880e4f | 510 |
- **Top-row arrows:** solid `#880e4f` width-2 horizontal arrows with filled triangular heads from (212, 82) to (278, 82) and from (442, 82) to (508, 82).
- **Owner box** 160×55 at (50, 175), color `#558b2f`: title "Owner's screen" bold 14px centered at (130, 197); sub-line 12px `#666` "looks exactly the same" centered at (130, 215).
- **Unchanged link:** dashed `#999` width-1.5 vertical line from (130, 112) to (130, 173); label "no visible change" 13px `#999` left-aligned at (142, 148).
- **Bottom line (bold 14px `#ef6c00`, centered, y=262):** "Copies leave with every message — the original never moves."
- **Caption (bottom center, 13px `#999`, y=285):** "A reader who changes nothing gives the owner nothing to notice."

## 3. What limits the damage

Each habit below either blocks the reader or shrinks what is readable.

- **Defense:** the email account gets the strongest sign-in (two-step).
- **Defense:** a periodic check of mail rules and connected apps.
- **Defense:** new-device and new-place alerts reviewed, not dismissed.
- **Risk:** alerts dismissed by habit protect nothing.
- **Defense:** the most sensitive matters move off email entirely.
- **Defense:** old threads archived out of the live mailbox.
- **Fact:** a smaller live mailbox is a smaller window to read.

**Key point:** **Win:** a checked mailbox with a short memory leaks less — and a leak that does start is caught sooner.

### Visualization (canvas `c3`, 720×300)

Defense table: one row per habit, each with a blocks/shrinks pill on the right.

- **Title (bold 16px `#01579b`, top center):** "Five habits — one blocks the door, four shrink the window".
- **Rows** at y = 50, 90, 130, 170, 210 (item text 14px `#2c3e50` left-aligned at x=60, baseline row y+18; status pill = rect at x=500, width 140, height 26 at row y, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (570, row y+17)):
  | Item | pill text | color |
  |---|---|---|
  | Strongest sign-in guards the mailbox | blocks | #558b2f |
  | Regular check of rules and connected apps | shrinks | #0277bd |
  | New-device alerts reviewed, not dismissed | shrinks | #0277bd |
  | Sensitive matters moved to sealed channels | shrinks | #0277bd |
  | Old threads archived out of the live inbox | shrinks | #0277bd |
- **Bottom line (bold 14px `#01579b`, centered, y=262):** "The archive shrinks from years of history to a guarded short window."
- **Caption (bottom center, 13px `#999`, y=285):** "None of these needs new tools — only habits applied to one account."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #0277bd`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#01579b` blue = mechanism/fact (Fact, Mechanism); `#558b2f` green = defense/win (Defense, Win); `#880e4f` berry = risk/loss (Risk, Seen); `#ef6c00` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#01579b`; h2 1.3rem `#01579b`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #0277bd`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #880e4f`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each canvas is drawn by a named function (`drawC1`, `drawC2`, `drawC3`) and a debounced (150ms) window-resize listener calls `renderAll()` so canvases stay sharp. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary blue `#01579b`, green `#558b2f`, berry `#880e4f`, orange `#ef6c00`, plus `#0277bd`, `#4e342e`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes ("most other accounts", not "every account"). Each technical term (forwarding, two-step) appears at most once, in parentheses. Fictional naming only (Alice); no realistic credentials or account secrets anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
