# Authentication & MFA Mechanisms Survey

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Authentication & MFA Mechanisms Survey

**Subtitle:** A catalog of the ways systems verify "you are you" — every mechanism proves control of *something*, and its security is exactly the difficulty of stealing that something remotely.

**Intro callout (blue-left-border box):** Every authentication mechanism is a proof of control over one root of trust — a channel, a device, a physical object, or a memory. Its security is exactly the difficulty of stealing that root remotely, and the weakest accepted recovery path sets the real strength of the account.

## 1. What is it?

Authentication mechanisms reduce to three factor families: something you **know** (password, PIN, security question), something you **have** (phone, mailbox, hardware key, enrolled device), and something you **are** (fingerprint, face). MFA combines factors from different families.

Most "codes" are possession proofs delivered over a channel. The code itself is not the secret — control of the channel or device is. An email OTP proves mailbox access; an SMS OTP proves phone-number control; a TOTP proves a device holding a shared seed.

**Key point (red-left-border box):** The same numeric code means completely different things depending on channel. Six digits over SMS and six digits from an authenticator app look identical to the user but have very different threat models.

### Visualization (canvas `c1`, 720×300)

Three-box family diagram with an MFA bracket underneath.

- **Title (bold 13px `#1a5276`, top center):** "The three factor families".
- **Boxes** 200×150 at y=40 (fill = family color at 0.12 alpha, stroke = family color width 2; title bold 14px in family color; items 11px `#2c3e50`, centered, 20px apart):
  | Title | color | x | items |
  |---|---|---|---|
  | KNOW | #e74c3c | 30 | Password / PIN; Security questions |
  | HAVE | #1a5276 | 260 | Phone (SMS, push, TOTP); Mailbox (email OTP, link); Hardware key / smart card; Enrolled device (passkey, QR) |
  | ARE | #27ae60 | 490 | Fingerprint; Face / iris |
- **MFA bracket:** orange `#e67e22` width-2 bracket line spanning x=130 to x=590 at y≈225 with a center tick down from y=190; below it, bold 12px `#e67e22` centered: "MFA = factors from different families (code + code from the same channel is not MFA)".

## 2. The mechanism catalog

- **Email numeric code:** used as 1FA (passwordless login) or 2FA. Proves mailbox access — inherits all of email's weaknesses.
- **Magic link (unique URL):** same channel as email OTP but the code is a bearer token in the link. One click, no typing, same trust root.
- **SMS / voice-call OTP:** proves phone-number control. Vulnerable to SIM swap, SS7 interception, and forwarding scams.
- **Authenticator app (TOTP/HOTP):** shared seed + clock (or counter) generates codes offline. No delivery channel to intercept.
- **Push approval + number matching:** tap "approve" on an enrolled device; matching a displayed number defeats push-fatigue attacks.
- **Device code on TV screen:** OAuth device grant — the TV shows a short code, you confirm it on an already-authenticated phone/browser. Solves input on keyboard-less devices.
- **Bluetooth numeric comparison:** both devices display the same 6 digits, human confirms the match — proves physical proximity and defeats man-in-the-middle pairing.
- **QR cross-device login:** scan with an already-logged-in phone (WhatsApp Web pattern). Delegates trust from one session to another.
- **Hardware security keys (FIDO2/U2F):** challenge-response signed by a key that checks the site's origin. Phishing-resistant by construction.
- **Passkeys (WebAuthn):** FIDO key pairs synced across devices, unlocked by local biometric. Possession + inherence in one gesture.
- **Biometrics:** almost never sent to the server — they locally unlock a device-held key. The factor is really the device.
- **Recovery codes / grid cards:** pre-generated one-time codes on paper. Possession of a physical artifact.
- **Smart cards / client certificates:** cryptographic possession, common in government and enterprise.
- **Security questions:** knowledge factor whose answers are often public or guessable — the weakest entry in the catalog.

### Visualization (canvas `c2`, 720×460)

Horizontal bar ranking: resistance to remote attack by mechanism.

- **Title (bold 13px `#1a5276`, top center):** "Resistance to remote attack, by mechanism (illustrative)".
- **Bars:** 20px tall, 7px gap, starting y=36; labels right-aligned 12px `#2c3e50` ending at x=190; track `#f0f0f0` 360px max; bar fill = row color at 0.6 alpha with 1px solid stroke; value in bold 11px `#2c3e50` after the bar.
- **Data (label, value, color):**
  | Mechanism | value | color |
  |---|---|---|
  | Hardware key (FIDO2) | 97 | #27ae60 |
  | Passkey (WebAuthn) | 95 | #27ae60 |
  | Smart card / client cert | 92 | #27ae60 |
  | Bluetooth numeric compare | 88 | #27ae60 |
  | QR cross-device login | 78 | #2980b9 |
  | Device code (TV screen) | 74 | #2980b9 |
  | Push + number matching | 72 | #2980b9 |
  | Authenticator app (TOTP) | 60 | #e67e22 |
  | Push approval (plain) | 52 | #e67e22 |
  | Recovery / grid codes | 50 | #e67e22 |
  | Email OTP / magic link | 42 | #e67e22 |
  | SMS / voice OTP | 35 | #e74c3c |
  | Password alone | 15 | #e74c3c |
  | Security questions | 8 | #e74c3c |
- **Caption (bottom center, 11px `#999`):** "Typed codes cluster mid-table: all fall to a live phishing relay. Origin-bound crypto does not."

## 3. What does each mechanism actually prove?

Strip away the UX and every mechanism is a proof of control over one root of trust:

- **A channel:** email OTP, magic link, SMS, voice call. Security = security of the channel provider and its account-recovery path.
- **A device + stored secret:** TOTP, push approval, passkeys, QR login. Security = device theft/compromise resistance.
- **A physical object:** hardware key, smart card, grid card. Security = physical possession.
- **Physical proximity:** Bluetooth numeric comparison, NFC tap. Security = being in the same room.
- **A memory:** password, PIN, security question. Security = unguessability and non-reuse — historically poor.

**Key point:** Recovery paths collapse the hierarchy. If a phishable email resets your password, your account is only as strong as the email — regardless of how strong the primary MFA is. The weakest accepted path defines actual security.

### Visualization (canvas `c3`, 720×300)

Mapping diagram: five root-of-trust boxes each with an arrow to the mechanisms that prove it.

- **Title (bold 13px `#1a5276`, top center):** "What the mechanism actually proves".
- **Rows:** labeled boxes 170×34 at x=40, centered on y=55/105/155/205/255 (fill = color at 0.12 alpha, stroke = color width 1.5, label bold 12px in color); `#bbb` arrow to a left-aligned 12px `#2c3e50` mechanism list at x=272:
  | Root of trust | color | mechanisms |
  |---|---|---|
  | Channel control | #e74c3c | email OTP · magic link · SMS · voice |
  | Device + secret | #1a5276 | TOTP · push · passkey · QR login |
  | Physical object | #e67e22 | hardware key · smart card · grid card |
  | Proximity | #27ae60 | Bluetooth compare · NFC tap |
  | Memory | #8e44ad | password · PIN · security questions |

## 4. Failure modes & open questions

- **Phishing relays:** user types a real OTP into a fake site that forwards it live. Kills every typed code (SMS, email, TOTP alike). Only origin-bound crypto (FIDO2/passkeys) survives.
- **SIM swap:** attacker ports the number; SMS OTP now goes to them.
- **Push fatigue:** spam approval prompts until the user taps yes — the attack that made number matching necessary.
- **Channel loops:** email 2FA protecting the account that receives password resets is circular — one factor pretending to be two.
- **Verification-code social engineering:** "read me the code we just sent you" — the human becomes the relay.

**To explore:** code entropy vs expiry-window trade-offs (why 6 digits + 30s works), rate-limiting math on brute-forcing OTPs, and how account-recovery design dominates real-world account-takeover statistics.

**Key point:** Data angle: auth logs are a rich behavioral dataset — mechanism choice, failure rates, retry patterns, and abandonment funnels by factor type reveal both attack waves and UX friction.

### Visualization (canvas `c4`, 720×300)

Scatter plot: security vs user effort per mechanism.

- **Title (bold 13px `#1a5276`, top center):** "Security vs user effort (illustrative)".
- **Axes:** origin x=90, baseline y=260, plot 560×210, stroke `#999` width 1.5; x label (11px `#666`, centered below): "user effort →"; y label (rotated −90°, left of axis): "security →".
- **Points** (5px-radius dots, coordinates as fractions [effort, security] of plot; labels 10px `#2c3e50` to the right of each dot):
  | Label | effort | security | color |
  |---|---|---|---|
  | Password only | 0.30 | 0.10 | #e74c3c |
  | Security questions | 0.25 | 0.04 | #e74c3c |
  | SMS OTP | 0.45 | 0.30 | #e74c3c |
  | Email OTP | 0.50 | 0.35 | #e67e22 |
  | Magic link | 0.35 | 0.37 | #e67e22 |
  | TOTP app | 0.55 | 0.55 | #e67e22 |
  | Push + match | 0.35 | 0.62 | #2980b9 |
  | Device code (TV) | 0.60 | 0.66 | #2980b9 |
  | QR login | 0.28 | 0.68 | #2980b9 |
  | BT compare | 0.40 | 0.78 | #27ae60 |
  | Passkey | 0.15 | 0.88 | #27ae60 |
  | Hardware key | 0.50 | 0.94 | #27ae60 |
- **Callout (bold 11px `#27ae60`, near top of plot, two lines):** "Passkeys break the usual trade-off:" / "highest security, lowest effort".

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×460, `c3` 720×300, `c4` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
