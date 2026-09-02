# Credential & Token Design

**Page type:** grid page (nav-card grid, auto-fit columns min 300px, cards with labeled bullet points and topic tags; no category labels — all cards share one topic)
**HTML title tag:** Credential & Token Design

**Subtitle:** How credential artifacts themselves are designed — format, embedded claims, entropy, lifetime, and revocability — from API tokens to recovery codes. The mechanisms that deliver or verify them are a separate story; this survey is about the artifact.

## Cards

Each card links to a detail page under `credential-token-design/`. Each card shows a numbered title, three labeled bullets (bold colored label + short phrase, kept compact so lines do not wrap), and a row of topic tags.

### 1. API Tokens & Keys — [credential-token-design/01-api-tokens-keys.md](credential-token-design/01-api-tokens-keys.md)

- **Format** (`#1a5276`): prefix + random core + checksum, shown once
- **Why prefixes** (`#e67e22`): secret scanners spot leaks in public code
- **Entropy** (`#27ae60`): length × alphabet — hex vs base62 trade-off

Topics: `opaque tokens`, `prefixes`, `entropy`

### 2. Structured Signed Tokens — [credential-token-design/02-structured-signed-tokens.md](credential-token-design/02-structured-signed-tokens.md)

- **Design** (`#1a5276`): claims + signature, verified with no lookup
- **Wins** (`#27ae60`): stateless, portable across services
- **Costs** (`#e74c3c`): size, readable claims, no true revocation

Topics: `JWT`, `claims`, `stateless`

### 3. Session Cookies & Anti-Replay Design — [credential-token-design/03-session-cookies-anti-replay.md](credential-token-design/03-session-cookies-anti-replay.md)

- **Three designs** (`#1a5276`): session ID, signed, encrypted
- **Anti-replay** (`#e67e22`): bind to hashed IP, device fingerprint
- **Trade-off** (`#e74c3c`): theft resistance vs mobile false logouts

Topics: `cookies`, `replay`, `binding`

### 4. Revocation & Lifetime Semantics — [credential-token-design/04-revocation-lifetime.md](credential-token-design/04-revocation-lifetime.md)

- **The split** (`#1a5276`): lookup dies instantly; signed lives to expiry
- **Denylist** (`#e74c3c`): revocation that re-adds the lookup
- **Compromise** (`#27ae60`): short access + rotating refresh tokens

Topics: `revocation`, `TTL`, `refresh tokens`

### 5. Password Composition Rules — [credential-token-design/05-password-composition-rules.md](credential-token-design/05-password-composition-rules.md)

- **The arc** (`#1a5276`): short caps → forced complexity → length-first
- **Why it reversed** (`#e74c3c`): humans made complexity predictable
- **Varies by** (`#e67e22`): country, regulator, and sector

Topics: `passwords`, `policy`, `history`

### 6. Password Storage Design — [credential-token-design/06-password-storage.md](credential-token-design/06-password-storage.md)

- **The ladder** (`#1a5276`): plaintext → salted → slow memory-hard
- **Framing** (`#e67e22`): design only matters after the database leaks
- **Consequence** (`#27ae60`): storage decides which rules matter

Topics: `hashing`, `salts`, `work factor`

### 7. PINs & Numeric Secrets — [credential-token-design/07-pins-numeric-secrets.md](credential-token-design/07-pins-numeric-secrets.md)

- **Entropy** (`#e74c3c`): ~13 bits — the system carries the security
- **Enforcement** (`#1a5276`): try-counters, lockout, secure elements
- **Human skew** (`#e67e22`): dates and patterns shrink it further

Topics: `PIN`, `rate limiting`, `entropy`

### 8. Security Questions & KBA — [credential-token-design/08-security-questions-kba.md](credential-token-design/08-security-questions-kba.md)

- **Static Q&A** (`#e74c3c`): public, low-entropy, unrotatable answers
- **Dynamic KBA** (`#1a5276`): credit-file questions, broker-dependent
- **Trend** (`#e67e22`): retired for possession-based recovery

Topics: `KBA`, `knowledge factors`, `decline`

### 9. Recovery Credentials — [credential-token-design/09-recovery-credentials.md](credential-token-design/09-recovery-credentials.md)

- **Catalog** (`#1a5276`): reset links, one-time codes, backup keys
- **Design flaw** (`#e74c3c`): secondary-email loops are circular
- **Principle** (`#e67e22`): weakest recovery path sets real strength

Topics: `reset links`, `recovery codes`, `backup keys`

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap.
- **Links:** the headings above link to `.md` versions for navigation in markdown; in the regenerated HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<h3>N. Title</h3>` (unpadded index number matching the file index), `<ul class="points">` with one `<li>` per labeled bullet — `<span class="pt-label" style="color:LABEL_COLOR">Label:</span>` followed by the phrase — and `<div class="topics">` with one `<span class="topic-tag">` per topic. No `.card-num` category label and no `<p>` description on this page; the bullets carry the content and each stays short enough to avoid wrapping at normal card width.
- **Bullet style:** `ul.points` no list markers, padding-left 2px, margin 6px 0; `li` font-size 0.82em, color `#444`, line 1.45, 3px vertical gap; `.pt-label` weight 700 in its inline label color.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, box-shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#e67e22`, `translateY(-2px)`. h3 `#1a3a4a` 1em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, `#666`.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#e67e22` (orange backlog accent); subtitle `#666` 1.05em. No nav bar, no back/home links. H1 carries no index number.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvases:** none on this page; site-wide canvases use `window.devicePixelRatio` scaling.
