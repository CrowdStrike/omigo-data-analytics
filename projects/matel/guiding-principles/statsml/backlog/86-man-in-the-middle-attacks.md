# Man-in-the-Middle Attacks — Across Sectors

**Page type:** grid page (nav-card grid, auto-fit columns min 300px, cards with a colored sector label, labeled bullet points and topic tags)
**HTML title tag:** Man-in-the-Middle Attacks — Across Sectors

**Subtitle:** One trick in many costumes: get between two parties who believe they are talking directly, then read or rewrite what passes through. Ten everyday versions, each from a different corner of life, told in plain language.

## Cards

Each card links to a detail page under `man-in-the-middle-attacks/`. Each card shows a colored uppercase sector label, a numbered title, three labeled bullets (bold colored label + short phrase, kept compact so lines do not wrap), and a row of topic tags. Bullet label colors: **Trick** `#1a5276`, **Everyday scene** `#e67e22`, **Defense** `#27ae60`.

### 1. Postal Mail Interception & Check Washing — POSTAL (`#8e44ad`) — [man-in-the-middle-attacks/01-postal-mail-interception.md](man-in-the-middle-attacks/01-postal-mail-interception.md)

- **Trick:** lift the envelope, wash the ink, rewrite the check
- **Everyday scene:** a mailed rent check cashed by a stranger
- **Defense:** gel ink, bank alerts, drop-off inside the post office

Topics: `check washing`, `mail theft`

### 2. ATM & Gas Pump Card Skimmers — BANKING (`#e74c3c`) — [man-in-the-middle-attacks/02-atm-card-skimmers.md](man-in-the-middle-attacks/02-atm-card-skimmers.md)

- **Trick:** a fake slot over the real one copies your card
- **Everyday scene:** a pinhole camera films the PIN you type
- **Defense:** chip cards answer with a one-time code, not a copy

Topics: `skimmers`, `card cloning`

### 3. Fake Public Wi-Fi Hotspots — PUBLIC SPACES (`#2980b9`) — [man-in-the-middle-attacks/03-fake-wifi-hotspots.md](man-in-the-middle-attacks/03-fake-wifi-hotspots.md)

- **Trick:** a nearby laptop poses as the free airport Wi-Fi
- **Everyday scene:** your phone auto-joins; all traffic detours
- **Defense:** the padlock still hides what's inside each page

Topics: `evil twin`, `public Wi-Fi`

### 4. Fake Cell Towers — TELECOM (`#1a5276`) — [man-in-the-middle-attacks/04-fake-cell-towers.md](man-in-the-middle-attacks/04-fake-cell-towers.md)

- **Trick:** a portable box outshouts the real tower nearby
- **Everyday scene:** phones in a crowd switch to it silently
- **Defense:** newer networks make phones check the tower's ID

Topics: `cell-site simulator`, `downgrade`

### 5. Invoice & Wire Fraud in Email Threads — REAL ESTATE (`#e67e22`) — [man-in-the-middle-attacks/05-invoice-wire-fraud.md](man-in-the-middle-attacks/05-invoice-wire-fraud.md)

- **Trick:** watch a hacked thread, then send "updated" details
- **Everyday scene:** a house down payment wired to a stranger
- **Defense:** verify new details by phone on a known number

Topics: `wire fraud`, `email compromise`

### 6. Man-in-the-Browser Banking Trojans — ONLINE BANKING (`#e74c3c`) — [man-in-the-middle-attacks/06-man-in-the-browser.md](man-in-the-middle-attacks/06-man-in-the-browser.md)

- **Trick:** malware rewrites the payee after you press send
- **Everyday scene:** the screen still shows the transfer you meant
- **Defense:** the phone app confirms amount and payee separately

Topics: `banking trojan`, `tampering`

### 7. QR Code Sticker Swaps — PAYMENTS (`#27ae60`) — [man-in-the-middle-attacks/07-qr-code-sticker-swaps.md](man-in-the-middle-attacks/07-qr-code-sticker-swaps.md)

- **Trick:** a fake QR sticker pasted over the real code
- **Everyday scene:** parking "payment" flows to a scam page
- **Defense:** read the web address before entering any card

Topics: `QR swap`, `payment fraud`

### 8. Car Key Relay Attacks — AUTOMOTIVE (`#1a5276`) — [man-in-the-middle-attacks/08-car-key-relay-attacks.md](man-in-the-middle-attacks/08-car-key-relay-attacks.md)

- **Trick:** two radios stretch the key's short-range signal
- **Everyday scene:** the car unlocks while the key sits indoors
- **Defense:** metal pouches; fobs that sleep when left still

Topics: `keyless entry`, `relay`

### 9. Home Router DNS Hijacking — HOME INTERNET (`#2980b9`) — [man-in-the-middle-attacks/09-home-router-dns-hijacking.md](man-in-the-middle-attacks/09-home-router-dns-hijacking.md)

- **Trick:** the router's phonebook points names at fake sites
- **Everyday scene:** the bank's address quietly resolves elsewhere
- **Defense:** change default passwords; heed certificate warnings

Topics: `DNS`, `router hijack`

### 10. Corporate HTTPS Inspection — WORKPLACE (`#8e44ad`) — [man-in-the-middle-attacks/10-corporate-https-inspection.md](man-in-the-middle-attacks/10-corporate-https-inspection.md)

- **Trick:** the office proxy decrypts, reads, and re-seals traffic
- **Everyday scene:** the padlock shows, but the proxy holds a key
- **Defense:** disclosed by policy — the lesson is what padlocks mean

Topics: `TLS inspection`, `proxies`

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap.
- **Links:** the headings above link to `.md` versions for navigation in markdown; in the regenerated HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num" style="color:SECTOR_COLOR">SECTOR</div>` (sector and hex given per heading above), `<h3>N. Title</h3>` (unpadded index number matching the file index), `<ul class="points">` with one `<li>` per labeled bullet — `<span class="pt-label" style="color:LABEL_COLOR">Label:</span>` followed by the phrase — and `<div class="topics">` with one `<span class="topic-tag">` per topic. Bullets stay short enough to avoid wrapping at normal card width.
- **Bullet label colors:** Trick `#1a5276`, Everyday scene `#e67e22`, Defense `#27ae60`.
- **Bullet style:** `ul.points` no list markers, padding-left 2px, margin 6px 0; `li` font-size 0.82em, color `#444`, line 1.45, 3px vertical gap; `.pt-label` weight 700 in its inline label color.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, box-shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#e67e22`, `translateY(-2px)`. `.card-num` 0.72em weight 700 uppercase, letter-spacing 0.5px; h3 `#1a3a4a` 1em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, `#666`.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#e67e22` (orange backlog accent); subtitle `#666` 1.05em. No nav bar, no back/home links. H1 carries no index number.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`.
- **Canvases:** none on this page; site-wide canvases use `window.devicePixelRatio` scaling.
