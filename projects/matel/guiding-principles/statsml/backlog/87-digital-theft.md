# Digital Theft — What Gets Stolen Online, and How

**Page type:** grid page (card grid in the `recently-added-misc/03-tracking-data-collection-methods` template style, 4 cards per row)
**HTML title tag:** Digital Theft — What Gets Stolen Online, and How

**Subtitle:** Passwords, identities, card numbers, health records, payroll files, customer databases, source code, trade secrets, strategy decks — the many forms of online theft, each told in plain language: what gets taken, how it happens, and what limits the damage.

## Cards

Each card links to a detail page under `digital-theft/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of topic tag pills.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | CREDENTIALS | Phishing & Password Theft | [digital-theft/01-phishing-credential-theft.md](digital-theft/01-phishing-credential-theft.md) | A convincing fake login page collects whatever you type — the thief signs in as you minutes later. | fake login pages, lookalike sites, stolen passwords |
| 2 | CREDENTIALS | Credential Harvesting & Replay | [digital-theft/02-credential-harvesting-replay.md](digital-theft/02-credential-harvesting-replay.md) | Malware collects saved passwords and session cookies in bulk — a stolen session gets replayed without ever logging in. | password-stealing malware, stolen sessions, no login needed |
| 3 | CREDENTIALS | Account Takeover & SIM Swap | [digital-theft/03-account-takeover-sim-swap.md](digital-theft/03-account-takeover-sim-swap.md) | Hijack the phone number or recovery email, and every "forgot my password" door swings open. | phone number hijack, recovery codes, reset abuse |
| 4 | PERSONAL | Identity Theft | [digital-theft/04-identity-theft.md](digital-theft/04-identity-theft.md) | With enough personal facts, a stranger becomes you on paper — new loans, cards, and tax refunds in your name. | accounts in your name, paper twin, credit freeze |
| 5 | PERSONAL | Health Records Theft | [digital-theft/05-health-records-theft.md](digital-theft/05-health-records-theft.md) | A medical file can't be reissued like a card — stolen health records fuel insurance fraud and expose what you never chose to share. | medical records, insurance fraud, facts you can't change |
| 6 | PERSONAL | Payroll & Background-Check Records | [digital-theft/06-payroll-background-check-records.md](digital-theft/06-payroll-background-check-records.md) | HR files hold salaries, background checks, and private disclosures — one stolen folder describes an entire workforce. | salary data, background checks, private disclosures |
| 7 | FINANCIAL | Payment Card Theft | [digital-theft/07-payment-card-theft.md](digital-theft/07-payment-card-theft.md) | Card numbers lifted from checkout pages are bundled, sold, and tested with small quiet charges. | checkout skimming, card testing, small test charges |
| 8 | FINANCIAL | Crypto Wallet Theft | [digital-theft/08-crypto-wallet-theft.md](digital-theft/08-crypto-wallet-theft.md) | Whoever learns the recovery phrase owns the money — and no bank exists to reverse the transfer. | recovery phrase, no undo button, fake support |
| 9 | CORPORATE | Customer Data Breach | [digital-theft/09-customer-data-breach.md](digital-theft/09-customer-data-breach.md) | One copied database exposes millions of people at once — the theft scales with the container, not the effort. | bulk copying, stolen databases, breach notices |
| 10 | CORPORATE | Insider Data Theft | [digital-theft/10-insider-data-theft.md](digital-theft/10-insider-data-theft.md) | A departing employee downloads the customer list on the way out — logged in, permitted, and gone. | departing employees, customer lists, download spikes |
| 11 | CORPORATE | Sensitive Email Theft | [digital-theft/11-sensitive-email-theft.md](digital-theft/11-sensitive-email-theft.md) | A quietly read inbox leaks negotiations, legal advice, and deals — the thief learns your next move before you make it. | read-only intruder, leaked threads, negotiations exposed |
| 12 | CORPORATE | IP & Trade Secret Theft | [digital-theft/12-ip-trade-secret-theft.md](digital-theft/12-ip-trade-secret-theft.md) | Years of research leaves as a single file — the head start is what gets stolen, and competing products arrive sooner. | designs and formulas, corporate spying, faster rivals |
| 13 | CORPORATE | Strategy & Roadmap Leaks | [digital-theft/13-strategy-roadmap-leaks.md](digital-theft/13-strategy-roadmap-leaks.md) | Release plans, investment moves, and strategy decks lose their value the moment a rival reads them. | product roadmaps, investment plans, next move exposed |
| 14 | CORPORATE | Ransomware & Data Extortion | [digital-theft/14-ransomware-data-extortion.md](digital-theft/14-ransomware-data-extortion.md) | Thieves copy the data before locking it — pay once for the key, and again to keep it off the internet. | steal then lock, double ransom, leak sites |
| 15 | ENGINEERING | Source Code Leakage | [digital-theft/15-source-code-leakage.md](digital-theft/15-source-code-leakage.md) | A leaked repository hands over keys saved in code, unreleased features, and a scannable map of weaknesses. | exposed repositories, keys in code, map of weaknesses |
| 16 | ENGINEERING | Accidental Data Leakage | [digital-theft/16-accidental-data-leakage.md](digital-theft/16-accidental-data-leakage.md) | No thief required — a storage bucket left open or an "anyone with the link" share publishes data by mistake. | open storage, anyone with the link, no hacking needed |
| 17 | PUBLIC DATA | Scraping & Data Harvesting | [digital-theft/17-scraping-data-harvesting.md](digital-theft/17-scraping-data-harvesting.md) | Each profile is public on its own; a million gathered into one file becomes a product nobody agreed to. | mass collection, public but gathered, resold profiles |
| 18 | CREDENTIALS | Stolen One-Time Codes | [digital-theft/18-stolen-one-time-codes.md](digital-theft/18-stolen-one-time-codes.md) | The thief's login triggers a real code to your phone — a convincing caller gets you to read it back. | fake bank calls, codes read aloud, second step hijacked |
| 19 | DEVICES | Lost & Discarded Devices | [digital-theft/19-lost-discarded-devices.md](digital-theft/19-lost-discarded-devices.md) | Data walks out on lost phones and USB sticks — and "deleted" files rise again from dumped or resold drives. | lost phones and USB sticks, dumped hard drives, deleted isn't erased |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** card-grid style of `recently-added-misc/03-tracking-data-collection-methods.html` (no TOC or philosophy box on this page). Single page: h1, `.subtitle` paragraph, then one `.grid` of `.card` anchors.
- **Layout:** `.grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, `margin: 14px 0 30px 0`; responsive: 3 columns below 1100px, 2 below 800px, 1 below 500px.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="card" href="...">` containing `<div class="card-label" style="color:CATEGORY_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), one `<p>` with the one-line description, then `<div class="topics">` with one `<span class="topic-tag">` per topic.
- **Category label colors:** CREDENTIALS `#1a5276`; PERSONAL `#8e44ad`; FINANCIAL `#e74c3c`; CORPORATE `#2980b9`; ENGINEERING `#e67e22`; PUBLIC DATA `#27ae60`; DEVICES `#6d4c41`.
- **Card style:** background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 16px, transition on box-shadow 0.2s; hover: `box-shadow 0 4px 12px rgba(0,0,0,0.1)`, border `#2980b9`. `.card-label` 0.72em weight 700 uppercase, 0.5px letter-spacing, 4px bottom margin; h3 `#1a5276` 1.0em with 6px bottom margin; description `p` `#555` 0.85em `margin: 0`. `.topic-tag` pills: background `#eef4f8`, border `1px solid #cdd`, radius 4px, padding 2px 6px, 0.7em, color `#555`; `.topics` is a flex row with 4px gap, wrap, 8px top margin.
- **Page style:** body `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`, white background, text `#2a2a2a`, `padding: 40px 20px`, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#1a5276` with 10px bottom margin (no border); subtitle `#666` 1.05em with 24px bottom margin. No nav bar, no back/home links. H1 carries no index number.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
