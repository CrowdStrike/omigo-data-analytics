# Claims to Source Before Re-adding

Removed from the tracking pages because they could not be verified. Confirm before restoring.
Split out of `CLAUDE.md` to keep that file to rules.

Pages are referenced by **slug without the numeric prefix** — prefixes are renumbered
whenever a page is added or removed, and every reference here went stale the last time
that happened. Match on the slug, not the number.

- `voice-assistant-wake-words`: 2019 reporting on human review of assistant recordings — which outlet covered which vendor, and opt-in vs opt-out per vendor afterwards (Amazon believed to be opt-out, not opt-in). Also whether deleting audio deletes the derived transcript, which has changed over time.
- `smart-tv-content-recognition`: Vizio/FTC settlement — year, amount, and what the order actually required. Whether the New Jersey AG was a joint party.
- `stock-trading-order-flow`: payment-for-order-flow revenue by year, and any disclosed market-maker-specific figure. A previous "$2.6 billion in a single year" exceeded the named broker's total revenue and was removed as not credible. The page has since been de-named under the real-companies rule — the broker and the wholesale market maker are now described by role, and the charts no longer render "Front-run trades" / "trades ahead", which contradicted the page's own prose stating that front-running is prohibited and the mechanism is internalisation. Restore names only alongside a documented figure.
- `share-link-tracking`: the actual share-parameter names per platform (`fbclid` is well documented; others were removed as unverified).
- `aggregation-granularity-proxy`: reframed from a collection mechanism to an aggregation-granularity entry. The page now argues that coarsening to a household clears a non-PII bar while leaving a group small enough to act as a person proxy. The k-anonymity framing is sound in principle; the claim that average household size is a small single-digit number is stated without a source (census data would confirm it) and is deliberately written without a figure.
- `aggregation-granularity-proxy`: IP-based and address-based household targeting are marked "documented as a targeting capability." Believed correct — these are openly sold product categories — but no source was checked. Confirm, or downgrade the label to inferred.
- `aggregation-granularity-proxy`: the page states that covert microphone use for ad targeting is "not established." There is academic work that looked for audio exfiltration and did not find it; cite it, or soften to "no source checked either way."
- `payment-transaction-tracking`: the claim that a small number of transactions uniquely identifies most people traces to published re-identification work. The page now states the concept without a figure and credits "the re-identification literature." Find the paper and the actual numbers before restoring any percentage.
- `loyalty-cards-store-apps`: the retailer pregnancy-prediction anecdote. Widely repeated, unverified — removed from prose and from the chart that rendered it.
- `chat-messaging-metadata`: the "we kill people based on metadata" quotation and the attribution of metadata sharing between two named messaging products. Both removed as unsourced.
- `clipboard-access`: the 2020 clipboard-read incident and the "53 apps" figure. Removed. Note the iOS 14 paste banner itself is well documented and can stay.
- `ai-chatbot-conversations`: the statement that training defaults "differ by product and by plan" is believed true of the consumer/business split but no provider's terms were checked.
- `store-wifi-bluetooth-beacons`: foot-traffic data resale to investors is an openly sold category; the stronger claim that funds use it to predict earnings ahead of announcement was removed as unverified.
- `phone-permission-requests`: call-log metadata access is platform-dependent — materially restricted on iOS and gated behind a special-use declaration on Android. Currently hedged to "on some platforms"; deserves a proper check.
- `home-security-cameras`: the payload implies enrolled-face matching and audio capture as vendor features. Neither is sourced, and availability varies by vendor and region. Prose is hedged; the payload is not.
- `smart-home-mapping`: a robot-vacuum maker's reported consideration of selling floor-plan data, and the subsequent acquisition. Removed along with the company names.
- `robotics-warehouse-tracking`: automated termination from productivity metrics without human review. Removed; the page now argues from the schema and states explicitly that the internal behaviour is not established here. Same treatment applied to per-rider price discrimination in `rideshare-delivery-apps`.
- `search-history`: onward sale of search logs to insurers or employer background-check services. Removed as unverified.
- `dna-genetic-data`: subpoena access to relatives' genetic data. Plausible and widely reported, but no source checked.
- `phone-motion-sensors`: browsers moved device-motion access behind a permission request. The direction is correct; the specific year was removed as unsourced.
- `connected-cars`: driver-monitoring cameras are described as usually emitting a derived attention state rather than storing video. Believed correct as a general design pattern, not verified per vendor, and deliberately phrased with "usually".
- `eye-tracking`: the C2 heatmap array encodes an "eye-level shelf wins" pattern. Labelled illustrative, but the array itself still carries the claim. Consider flattening it or finding a source.
