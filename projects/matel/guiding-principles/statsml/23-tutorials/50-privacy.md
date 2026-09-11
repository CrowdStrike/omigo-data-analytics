# Privacy

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Privacy

**Subtitle:** Why removing names doesn't make a dataset anonymous — and how tracking, fingerprinting, and privacy regulation actually work.

## Cards

Each card links to a topic page under `privacy/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | PRIVACY-PRESERVING DATA | Differential Privacy | [50-privacy/01-differential-privacy.md](50-privacy/01-differential-privacy.md) | Publish the true aggregate while exposing no individual — the released number looks (almost) the same whether or not your row was in the dataset. | added noise, privacy budget, aggregates |
| 2 | PRIVACY-PRESERVING DATA | k-Anonymity & Re-identification | [50-privacy/02-k-anonymity-and-re-identification.md](50-privacy/02-k-anonymity-and-re-identification.md) | Removing names does not anonymize a dataset — a few ordinary columns combine into a fingerprint, and k-anonymity is the first formal defense. | quasi-identifiers, re-identification, crowd of k |
| 3 | TRACKING & REGULATION | How Web Tracking Works | [50-privacy/03-how-web-tracking-works.md](50-privacy/03-how-web-tracking-works.md) | Third-party cookies, invisible pixels, and redirect chains — the plain mechanics that turn page loads into cross-site behavioral profiles. | third-party cookies, tracking pixels, cross-site profiles |
| 4 | TRACKING & REGULATION | Fingerprinting & the Post-Cookie World | [50-privacy/04-fingerprinting-and-the-post-cookie-world.md](50-privacy/04-fingerprinting-and-the-post-cookie-world.md) | Your fonts and timezone are nearly unique — each browser attribute narrows the crowd, and enough small clues point to exactly one person. | browser attributes, uniqueness, no cookies needed |
| 5 | TRACKING & REGULATION | Consent & Privacy Regulation | [50-privacy/05-consent-and-privacy-regulation.md](50-privacy/05-consent-and-privacy-regulation.md) | GDPR and CCPA turned privacy into engineering requirements — how the legal concepts map onto system design. | GDPR, CCPA, consent, data rights |

## Regeneration instructions

- **Template:** tutorials category grid page (per `tutorials/CLAUDE.md`). Body: h1 (1.8em `#2980b9`), `.subtitle` (`#666` 1.05em), then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Grid:** `repeat(4, 1fr)`, 16px gap; responsive fallbacks: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Card:** white `.nav-card` (1px `#d8d8d8` border, 10px radius, 20px padding, subtle shadow, hover lifts 2px with `#2980b9` border); `.card-num` = uppercase subcategory label (0.75em bold); `<h3>` = "N. Title" (unpadded N matching the topic file's index); one-line `<p>` description (0.85em `#555`); `.topics` row of `.topic-tag` pills (0.7em, `#f0f0f0` background, 1px `#ccc` border).
- **Category label colors (set by inline script keyed on `.card-num` text):** PRIVACY-PRESERVING DATA `#27ae60`, TRACKING & REGULATION `#8e44ad`.
- **Page CSS:** body system-ui sans-serif, background `#f5f5f0`, text `#2a2a2a`, 40px padding, line-height 1.6. No nav bar, no back/home links, no item counts.
- In regenerated HTML, card hrefs use `.html` extensions (`privacy/NN-slug.html`); this spec links the `.md` siblings.
