# Credential & Token Design Survey — Design Spec

**Date:** 2026-08-27
**Status:** Approved design, pending implementation plan

## Goal

A drill-down survey of how credential artifacts themselves are designed — API tokens, signed tokens, session cookies, passwords, PINs, security questions, recovery credentials. For each: the technical design choices, pros and cons, prevalence, and how current practice differs from historical practice.

This is explicitly NOT about authentication mechanisms (how you prove identity — OTP delivery, passkeys, hardware keys). That angle is already covered by backlog page 69 (`backlog/69-authentication-mfa-mechanisms.md`). This survey covers the design of the credential artifact: its format, embedded claims, entropy, lifetime, and revocability.

## Structure & Files

Three layers, all md-first (author `.md`, generate `.html` from it; the two never drift):

1. **Backlog grid card 82** — added to `02-backlog.md` / `02-backlog.html`.
   - Category label: `SYSTEMS` (color `#27ae60`, matching survey cards 61, 63, 64, 69).
   - Title: `82. Credential & Token Design`.
   - Links to the sub-grid page below. Description: one-to-two sentences on the survey scope. Topic tags: `tokens`, `cookies`, `passwords`, `revocation`.
   - The existing grid intro note about placeholder cards 77–81 stays unchanged — card 82 has a real detail page, so it is not part of that note.

2. **Sub-grid page** — `backlog/82-credential-token-design.md` + `.html`.
   - A mini nav-grid: h1 **without** an index number (backlog detail-page convention), subtitle, then 9 cards in the standard nav-card grid style.
   - Card style: same nav-card as `02-backlog` (white card, gray border, hover lift) but without the colored category label — all 9 cards share one topic. Each card: `N. Title` in h3 where N matches the detail-page file index, one-to-two sentence description, topic tags. Single flat section — no category headings needed at 9 cards.
   - Card links point to `.md` siblings in the `.md` page and `.html` in the `.html` page.

3. **Detail pages** — new folder `backlog/credential-token-design/`, files `01-…` through `09-…` (`.md` + `.html` each).
   - Layout: backlog kusto-style two-column detail page (as codified in page 69's regeneration instructions): h1 + subtitle + blue intro callout, one `.lang-section` per numbered h2, each holding `table.layout` with `td.text-col` (~50%) and `td.viz-col` (~50%, one canvas).
   - Canvas rules: intrinsic width 720, height 300–460, `width: 100%`, devicePixelRatio-scaled backing store sized to displayed width (sharp-rendering pattern), standard palette (`#1a5276`, `#27ae60`, `#e74c3c`, `#e67e22`, `#2980b9`, `#8e44ad`).
   - Each page ends with regeneration instructions (layout, CSS, canvas specs), matching the established backlog detail-page md format.
   - 2-screen limit per page; typically 4 numbered sections per page.

## The 9 Detail Pages

Each page weaves four angles into its sections: technical design, pros/cons, prevalence, current vs historical.

### 01 — API tokens & keys (`01-api-tokens-keys.md`)
Opaque random tokens as the baseline design. Issuer-identifiable prefixes and why they exist (secret scanning: a recognizable prefix lets scanners and the issuer find leaked tokens in public code). Embedded checksums to reject typos/corruption offline. Entropy and encoding choices (hex vs base62/base64url; length vs alphabet trade-off). Historical arc: short static keys → long random tokens → prefixed, checksummed, scanner-friendly formats.

### 02 — Structured signed tokens (`02-structured-signed-tokens.md`)
JWT-family design: claims embedded in the artifact (subject, expiry, scopes), signed so the server can trust without lookup; signing vs encrypting (visible vs sealed claims). Pros: stateless verification, cross-service portability. Cons: size, everything-in-the-token leak surface, algorithm-confusion pitfalls (descriptive, not exploit detail), and the revocation problem (deferred to page 04). Prevalence: dominant in service-to-service and OAuth ecosystems; opaque tokens still preferred where central control matters.

### 03 — Session cookies & anti-replay (`03-session-cookies-anti-replay.md`)
Random session ID (server-side state) vs signed cookie (tamper-proof client state) vs encrypted cookie (sealed client state). Binding claims baked into the cookie to resist replay/theft: encrypted or hashed IP address, device fingerprint, user-agent binding — and the mobility/false-logout trade-off each binding creates. Rotation on privilege change (login, elevation). What each design actually buys against cookie theft.

### 04 — Revocation & lifetime semantics (`04-revocation-lifetime.md`)
The core split of the whole survey: **server-lookup tokens are revocable** (delete the row, the token is dead); **self-contained signed tokens are not** — they live until expiry unless a denylist exists, which reintroduces the per-request lookup the design tried to avoid. Short-lived access token + long-lived refresh token as the standard compromise; refresh rotation and reuse detection; TTL design (why short expiry is the real revocation story). Table: credential type → revocable? → how → latency of revocation.

### 05 — Password composition rules (`05-password-composition-rules.md`)
Historical arc: early short-length limits (storage-era constraints) → the mandatory-complexity era (uppercase + digit + symbol, forced 90-day expiry) → the modern reversal (length-first, no forced composition, no scheduled expiry, screen against known-breached lists). Why the reversal happened: composition rules produced predictable substitutions; forced expiry produced incremental suffixes. Country/regulator differences: banking rules, numeric-only conventions in some regions, minimum-length floors varying by jurisdiction and sector. Charts marked "illustrative".

### 06 — Password storage design (`06-password-storage.md`)
Evolution: plaintext → fast unsalted hashes → salted hashes → deliberately slow, memory-hard hashing (work-factor designs). Why storage design determines which composition rules even matter: against a slow hash, length dominates; against a fast unsalted hash, nothing composition does saves you. Pepper/HSM as the server-side secret layer. Offline vs online attack as the framing.

### 07 — PINs & numeric secrets (`07-pins-numeric-secrets.md`)
4 vs 6 digits; regional and sector conventions (banking cards, SIM PINs, device PINs). The key point: a 4-digit PIN has ~13 bits of entropy — the security lives in rate-limiting, lockout, and hardware-backed try-counters, not the secret itself. Human digit-choice skew (birthdays, patterns) shrinks effective entropy further — a natural data/distribution angle for this repo.

### 08 — Security questions & KBA (`08-security-questions-kba.md`)
Static Q&A design: why it fails (answers are low-entropy, publicly researchable, unchangeable once leaked). Dynamic knowledge-based auth (credit-file-style generated questions): stronger but data-broker-dependent and locale-limited. Regional prevalence differences and the steady decline of both in favor of possession-based recovery. Historical: once mandatory at major providers, now largely retired.

### 09 — Recovery credentials (`09-recovery-credentials.md`)
Reset links as short-lived single-use bearer tokens (design parameters: TTL, single-use enforcement, session invalidation on use). One-time recovery codes (pre-generated, hashed at rest like passwords). Backup keys / printed secrets. The closing point of the survey: the weakest accepted recovery path sets the account's real strength — recovery credentials are credentials and deserve the same design scrutiny.

## Content Constraints

- **No realistic credential strings** (established repo convention): token anatomy is shown as schematic labeled boxes — e.g. a diagram with segments labeled "prefix", "random part (base62)", "checksum" — never plausible-looking secrets, never `key=value` credential syntax. Scanner false-positive footnote where a page discusses token formats.
- **Fictional naming:** Alice/Bob for people, "Vendor A"-style for companies; no real-company anecdotes; unsourced stories labeled "Illustrative Example".
- **Prior knowledge only, no web search.** Prevalence and historical claims framed qualitatively; any quantified chart labeled "(illustrative)" in its title, matching page 69's practice.
- Text/viz balance: bullets are 1–2 full sentences (no fragments); one canvas per numbered section.
- No cross-links between pages except grid-card navigation (no links to/from page 69, no back/home links).
- No item counts in card descriptions or summaries.

## Error Handling / Verification

- Md and html siblings generated together; html generated from the md spec.
- Verification: at most one quick script sanity check on the generated html (no browsers/screenshots).

## Out of Scope

- Authentication mechanisms (OTP, passkeys, hardware keys, biometrics) — covered by backlog 69.
- Exploit walkthroughs or attack tooling — failure modes are described at the threat-model level only.
- OAuth protocol flows (grants, redirects) — only the artifacts (access/refresh tokens) are in scope.
