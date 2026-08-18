# Statistical ML Guiding Principles (Work In Progress)

## What This Is

Guiding principles and reference material for designing statistical and ML-based systems — drawn from years of working with big data systems and informed by active
areas of research in statistics, ML pipelines, and feature engineering.

## Note
A lot of examples are generated with assistance from AI coding tools like Claude Code.

These docs aim to:
- Explore design approaches for precondition verification, feature profiling, and distribution analysis
- Catalog common pitfalls across ML pipelines, A/B testing, and domain-specific analysis
- Capture best practices that are easy to forget under production pressure

## Structure

Open `index.html` for the visual hub. The top-level sections are:

| # | Section | Theme |
|---|---------|-------|
| 1 | Concepts | Theoretical foundations — statistics, ML, AI, engineering, distributed systems |
| 2 | Backlog | Unresolved questions and future directions |
| 3 | Brainstorm | Design explorations — profiling, shape detection, separation |
| 4 | Domain Pitfalls | Industry-specific data traps (finance, health, e-commerce, …) |
| 5 | Cognitive Biases | Human psychology failures in analysis |
| 6 | ML Pipeline Pitfalls | Leakage, drift, training-serving skew |
| 7 | A/B Testing Pitfalls | Peeking, underpowered tests, contamination |
| 8 | Pseudoscience | Cargo-cult statistics, unfalsifiable claims |
| 9 | Common Bad Practices | Organizational anti-patterns in data teams |
| 10 | Anti-Patterns | Anti-pattern → design pattern pairs |
| 11 | Terminology Overloading | When everyday language collides with formal meaning |
| 12 | Statistical Tests & Metrics | Preconditions, validity, and alternatives |
| 13 | Statistical Paradoxes | Simpson's, base rate, Berkson's, survivorship |
| 14 | Metrics Design | Good/bad metrics, reporting, granularity |
| 15 | ML Algorithm Assumptions | What each algorithm requires from data |
| 16 | Correlation vs Causation | Correlation, causation, co-occurrence |
| 17 | Folk Wisdom Fallacies | Popular sayings decomposed for hidden fallacies |
| 18 | Interesting Problems & Paradoxes | Birthday paradox, Monty Hall, Nash equilibrium, … |
| 19 | Applied Game Theory & Behavioral Design | Pricing, engagement, auctions |
| 20 | Real-World Distribution Gallery | Surprising shapes from e-commerce, betting, crypto, retail |

## Note on Originality

Many ideas here borrow from or build on well-known concepts in statistics and ML. The value is in the curation, organization, and how they connect — not in claiming novelty.

## License

Part of the [omigo-data-analytics](https://github.com/CrowdStrike/omigo-data-analytics) project.
