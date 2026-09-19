# Tutorial Foundations (101) Pillars — Design

**Date:** 2026-08-27
**Status:** Approved (pilot scope)
**Pilot:** Game Theory (`tutorials/55-game-theory.html`)

## Problem

Many tutorial categories jump straight to interesting example topics (Nash
equilibrium, auctions, Shapley values) but have no page answering "what *is*
this topic, what are its major pillars, how is it practiced." A newcomer landing
on the Game Theory grid has no 101 entry point.

## The Pattern

The section is named **101 Intro** and pillar filenames carry a `101-intro`
marker (`NN-101-intro-<slug>`) so the set is recognizable as a sequenced group
that moves together.

The 101 Intro is an **ordered teaching sequence** (a curriculum spine), unlike
the rest of the grid which is à la carte. Two admission tests govern cards:

- **Spine card (101 Intro):** necessity — a newcomer can't follow the rest of
  the grid without it. Order matters; examples are constructed minimal ones.
- **Concept card (other sections):** resonance — the concept has a relatable
  real-life story where it visibly plays out. Order doesn't matter; a concept
  without a good story waits until one is found.

**Scaling ladder:** concept too big for a bullet → its own section; section too
big for a page → an adjacent pillar card; spine beyond ~10 cards → tree-like
hierarchy (the pillar or category becomes its own grid one level down, with its
own 101 Intro). 5–10 cards is the expected band; <5 is fine (don't pad), >10
means re-cut the category or demote cards. Depth stays shallow (rarely more
than two levels below the hub).

**Editorial bar:** curation over coverage. A newbie should get a map (the
spine), hooks (wow examples), and redo-by-hand depth on core concepts —
completeness is a non-goal.

**Authoring order:** write the `.md` spec first, generate the `.html` from it.

Each tutorials category grid gets a **101 Intro** subcategory section — a
normal `h2` + `.nav-grid`, placed **first** in the grid, with its own
`card-num` label color. Each card is one **pillar** of the topic itself:

- what it is (vocabulary, the core model)
- its major types / branches / steps
- how problems in it get solved
- how it's practiced in real work

### Rules

1. **Content-driven pillar count.** No fixed number, no padding to a quota.
   As many cards as the topic needs.
2. **One standard detail page per pillar.** Existing tutorial template applies
   unchanged: tags row, one-line bullets, italic example, key-point callout,
   canvas per section, 3–4 `.card-section` blocks, session-replay page-length
   cap. If a pillar cannot fit one page, split it into multiple pillar cards —
   never a longer page.
3. **Self-contained pages.** Each pillar readable alone; no dependence on
   reading sibling pages first. No cross-page links (grid cards only).
4. **Duplication by design.** A pillar may also exist as a full topic card
   elsewhere (e.g., feature engineering as a step inside "ML application
   workflow" and as its own topic). Each occurrence teaches from its own
   context and perspective. No deduplication, no cross-links.
5. **Practice pillar is standard.** Every Foundations set includes a
   "how it's practiced" card grounding the topic in real work.
6. **Foundations come first, existing files renumber.** Card numbering runs
   1..N across the whole category; foundations occupy the leading numbers and
   existing topic files (html + md siblings) renumber to follow. Grid page
   (html + md) updates card numbers and links in the same change.

## Pilot: Game Theory

`55-game-theory.html` gains a Foundations section with 5 pillars:

| # | File | Pillar | Covers |
|---|------|--------|--------|
| 1 | `game-theory/01-101-intro-what-is-a-game` | What Is a Game | Players, strategies, payoffs — turning a situation into a payoff matrix |
| 2 | `game-theory/02-101-intro-types-of-games` | Types of Games | Zero-sum vs non-zero-sum, simultaneous vs sequential, one-shot vs repeated |
| 3 | `game-theory/03-101-intro-solving-a-game` | Solving a Game | Dominant strategies, best response, elimination — what "solution" means |
| 4 | `game-theory/04-101-intro-sequential-games-backward-induction` | Sequential Games & Backward Induction | Game trees, reasoning from the last move backward |
| 5 | `game-theory/05-101-intro-game-theory-in-practice` | Game Theory in Practice | Pricing, ad auctions, mechanism design, multi-agent ML, negotiation |

Existing pages renumber (html + md pairs):

| Old | New | Card # |
|-----|-----|--------|
| `01-nash-equilibrium` | `06-nash-equilibrium` | 6 |
| `02-prisoners-dilemma-repeated-games` | `07-prisoners-dilemma-repeated-games` | 7 |
| `03-auctions` | `08-auctions` | 8 |
| `04-shapley-values` | `09-shapley-values` | 9 |
| `05-voting-paradoxes-arrows-theorem` | `10-voting-paradoxes-arrows-theorem` | 10 |

Grid changes in `55-game-theory.html` / `.md`:

- New first section `<h2>101 Intro</h2>` with its own `.nav-grid`, cards 1–5,
  `card-num` label "101 INTRO" with its own color (distinct from the existing
  labels; `#8e44ad` violet — orange `#e67e22` was taken mid-flight by a
  concurrently added "Information in Games" section holding card 11,
  Monty Hall in Real Life).
- Existing "Strategic Play" and "Cooperation & Collective Choice" sections keep
  their labels/colors; card h3 numbers and hrefs update to 6–10.

Content overlap with existing cards (pillar 3 vs Nash Equilibrium, pillar 5 vs
Auctions) is intentional: pillars teach the general machinery, existing cards
go deep on one celebrated result.

## Out of Scope (future rounds)

- Rolling the pattern out to other categories. Each category gets audited
  first for whether it actually lacks a 101 (some, like Math Foundations, are
  already the 101), then built in batches copying the pilot's pattern.
- Any change to the tutorial detail-page template itself.
