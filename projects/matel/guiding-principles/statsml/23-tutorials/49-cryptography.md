# Cryptography

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Cryptography

**Subtitle:** How secrets stay secret on open networks — keys that lock and unlock, signatures that prove identity, proofs that reveal nothing, and the limits a future machine would impose.

## Cards

Each card links to a topic page under `cryptography/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | CRYPTO BUILDING BLOCKS | Symmetric vs Asymmetric Encryption | [49-cryptography/01-symmetric-vs-asymmetric-encryption.md](49-cryptography/01-symmetric-vs-asymmetric-encryption.md) | One shared key that both locks and unlocks, or a key pair where anyone can lock but only you can unlock. | shared key, key pair, lock vs unlock |
| 2 | CRYPTO BUILDING BLOCKS | RSA in Miniature | [49-cryptography/02-rsa-in-miniature.md](49-cryptography/02-rsa-in-miniature.md) | Encrypt a number by hand with two tiny primes — the whole RSA machine fits on a napkin when n = 33. | tiny primes, by hand, public key |
| 3 | CRYPTO BUILDING BLOCKS | Diffie-Hellman | [49-cryptography/03-diffie-hellman.md](49-cryptography/03-diffie-hellman.md) | Two people shout messages across a crowded room and still end up with a shared secret nobody else can figure out. | key exchange, shared secret, public channel |
| 4 | CRYPTO BUILDING BLOCKS | Digital Signatures & Certificates | [49-cryptography/04-digital-signatures-and-certificates.md](49-cryptography/04-digital-signatures-and-certificates.md) | Sign with the private key, verify with the public key, then follow a chain of signatures up to a root your browser already trusts. | signing, trust chain, HTTPS |
| 5 | ADVANCED PROTOCOLS | Zero-Knowledge Proofs | [49-cryptography/05-zero-knowledge-proofs.md](49-cryptography/05-zero-knowledge-proofs.md) | Convince someone you know a secret — a password, an age, a solution — without revealing one bit of the secret itself. | prove without telling, cave door, verification |
| 6 | ADVANCED PROTOCOLS | Homomorphic Encryption | [49-cryptography/06-homomorphic-encryption.md](49-cryptography/06-homomorphic-encryption.md) | Compute on data that stays encrypted the whole time — the cloud multiplies sealed numbers and never sees a single one. | compute on ciphertext, sealed glovebox, encrypted in use |
| 7 | CRYPTO LIMITS | What a Quantum Computer Would Actually Break | [49-cryptography/07-what-a-quantum-computer-would-actually-break.md](49-cryptography/07-what-a-quantum-computer-would-actually-break.md) | Not all encryption — the asymmetric handshake falls to Shor while a longer symmetric key survives Grover. | what breaks, what survives, Shor vs Grover |
| 8 | CRYPTO LIMITS | Harvest Now, Decrypt Later | [49-cryptography/08-harvest-now-decrypt-later.md](49-cryptography/08-harvest-now-decrypt-later.md) | Traffic recorded today can be opened years later, so the question is how long your data must stay secret. | recorded today, secrecy lifetime, migration lead time |

## Regeneration instructions

- **Template:** tutorials category grid page (per `tutorials/CLAUDE.md`). Body: h1 (1.8em `#2980b9`), `.subtitle` (`#666` 1.05em), then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Grid:** `repeat(4, 1fr)`, 16px gap; responsive fallbacks: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Card:** white `.nav-card` (1px `#d8d8d8` border, 10px radius, 20px padding, subtle shadow, hover lifts 2px with `#2980b9` border); `.card-num` = uppercase subcategory label (0.75em bold); `<h3>` = "N. Title" (unpadded N matching the topic file's index); one-line `<p>` description (0.85em `#555`); `.topics` row of `.topic-tag` pills (0.7em, `#f0f0f0` background, 1px `#ccc` border).
- **Category label colors (set by inline script keyed on `.card-num` text):** CRYPTO BUILDING BLOCKS `#2980b9`, ADVANCED PROTOCOLS `#27ae60`.
- **Page CSS:** body system-ui sans-serif, background `#f5f5f0`, text `#2a2a2a`, 40px padding, line-height 1.6. No nav bar, no back/home links, no item counts.
- In regenerated HTML, card hrefs use `.html` extensions (`cryptography/NN-slug.html`); this spec links the `.md` siblings.
