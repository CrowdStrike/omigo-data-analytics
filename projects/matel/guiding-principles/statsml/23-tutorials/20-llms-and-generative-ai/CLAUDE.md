# LLMs & Generative AI — Folder Instructions

## Tone rules for the terms-of-service / data-use pages

Applies to cards 49-55 (output-use limits, chats-and-training, leakage, retention, open weights,
model hubs, deployment) and any new card covering terms, data use, retention, or restrictions.

- **Possibility wording, not assertion.** Any claim about what a provider does with data —
  training on chats, retention windows, deletion timing, output-use restrictions, review
  practices — is written as what terms **can** or **may** include, or what is **commonly seen**.
  Never as what happens. Numbers follow the same rule: "commonly published as around 30 days",
  "a stated window" — never "the vendor keeps it 30 days". Land each thread on "the actual terms
  decide" once per section, not per bullet.
- **Technical mechanics stay assertive.** VRAM arithmetic, how memorization works, what a hub
  listing contains, how deletion propagates through separate stores — these are facts. Only
  vendor-policy and contract claims get the "can" treatment.
- **No named-actor scenarios.** No Alice/Bob storylines on these pages — this overrides the
  project's usual fictional-naming convention here. Use plain mechanism statements or neutral
  technical cases with no protagonist.
- **No paranoia.** Not warnings or threat briefings. No "risk zone", no alarm-register canvas
  annotations, no framing providers as adversaries or readers as being caught out. Section
  titles are descriptive, not ominous.
- **Plain register, not legal.** No statute or article citations, no terms of art. Avoid the word
  "litigation" — use "a preservation hold" or "a legal preservation obligation". Keep the
  existing "not legal advice" / "illustrative" captions.
- **The through-line.** It is common to assume how these things work — that output is yours to
  use freely, that chats are private, that deleting removes data, that downloaded weights are
  unrestricted — and the terms of service can decide otherwise. Stated as a neutral observation.
- **Structure:** 3 `## ` sections and 3 canvases (c1-c3) per page; max 8 bullets per section,
  9 only where a section genuinely needs it. Merge related bullets rather than deleting a fact.

## Pending TODO

- **Cards 49-55 were tone-corrected in a minimal pass, not regenerated.** The batch was written
  by parallel subagents from an earlier brief, then edited down (4 sections → 3, bullet trims)
  and tone-corrected under a token budget. A full regeneration of these seven pages from their
  `.md` specs, applying every rule above uniformly, is still outstanding. Specifically unverified
  across the batch:
  - **md/html drift** — the pairs were edited repeatedly; text may not agree line for line.
    Note that `*Example (italic):*` in these specs is a *rendering directive*, not page text, and
    must not appear as a visible prefix in the html.
  - **Cross-page figure consistency** — the same quantities are cited on more than one page and
    were authored independently: VRAM arithmetic (51, 52, 55), retention window days (50, 53, 54),
    per-token and per-hour cost figures (51, 55). These have not been reconciled to the digit.
    **Known instance:** cards 51 and 55 use a +25% VRAM overhead factor (8B → 20 GB) while card 52
    uses +20% (8B → 19.2 GB). Each closes arithmetically and labels its own assumption, so neither
    is wrong in isolation, but a reader crossing pages sees the same model needing two figures.
    Fix by picking one factor, or by making 52 state its assumption as explicitly as 55 does.
  - **Card 53 retains the most alarm framing in the batch** — its title ("Leaking Private Data
    Through the Wrong Setting"), subtitle ("wrong door"), and several section-2 bullets
    ("No signal", "Invisible by construction", "Long lag") still read as a threat briefing.
    Renaming the page means renaming the file and its grid card in `20-llms-and-generative-ai.md`
    and `.html`, so it was left alone pending a decision.
  - **Residual alarm framing and named-actor examples** — the corrective pass targeted the most
    visible instances only; it did not sweep every bullet and canvas annotation.
  - **Two content additions may be incomplete:** the privacy/data-exposure dimension on card 55
    (local = no egress and no terms governing content; own cloud GPUs = your own boundary;
    hosted/managed = defined by the contract) and the retention-driver bullets on card 54
    (purpose sets the clock; who decides retention differs by plan — vendor decides on a consumer
    plan, customer instructs on a business one; sensitive data gets tighter handling; some
    obligations force longer keeping; pasting a third party's data makes it your responsibility).
