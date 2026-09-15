# Bank-to-Bank Rails

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Bank-to-Bank Rails

**Subtitle:** ACH, SEPA, UPI, and Pix move money straight from one bank account to another — no card network in the middle, and the new rails do it in seconds

## Moving $500 of Rent Without a Card Network

**Tags:** `core idea` (blue), `account to account` (green), `no card network` (orange)

- **The rent** — you owe your landlord $500 on the first of the month, both of you have bank accounts
- **The card path** — a card payment routes through a card network and takes a percentage cut on the way
- **The rail path** — a bank rail (ACH, SEPA, UPI, Pix) moves the $500 account-to-account directly
- **The price gap** — on a typical 2.9% + 30¢ card rate the cut is $14.80; ACH charges a flat few cents
- **Two verbs** — rails support push (you send money out) and, on ACH, pull (the landlord debits you)

*Example (italic):* The same $500 rent costs $14.80 in fees on a typical card rate but about $0.25 over ACH — roughly 60× cheaper for one hop between two bank accounts.

**Key point:** Bank rails connect accounts directly through the banking system, skipping the card network and its percentage fee — which is why payroll, rent, and bills ride rails, not cards.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the $500 rent traveling the card path (with a network in the middle and a fee bite) vs the bank-rail path (account to account, flat fee), boxes and arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways to Move $500: Through a Card Network vs Straight Between Banks".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "card"; blue `#2a78d6` rounded box at x=110 labeled "your bank" (12px), 3px arrow to an orange `#d95926` box at x=310 labeled "card network", 3px arrow to a blue box at x=520 labeled "landlord's bank"; bold 13px red `#e74c3c` label above the middle box: "fee $14.80 (2.9% + 30¢)".
- **Row 2 (boxes centered on y=210), label:** "bank rail"; blue box at x=110 "your bank", 3px arrow to a green `#008300` box at x=310 labeled "rail: ACH / SEPA / UPI / Pix", 3px arrow to a blue box at x=520 "landlord's bank"; bold 13px green `#008300` label above the middle box: "fee ~$0.25 flat".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "same $500, same two accounts — the rail decides the fee".
- **Caption (12px `#444`, bottom right):** "card 2.9% + 30¢ and ACH 25¢ are illustrative typical rates; $14.80 is exact math on that rate".

## Rent Day: Batch Windows vs Five Seconds

**Tags:** `worked example` (blue), `batch vs real-time` (green)

- **The clock** — you send the $500 at 5:00pm Monday; the two rails now take very different roads
- **ACH batches** — ACH banks collect payments into files and submit them in scheduled windows
- **The wait** — Monday 5pm misses the day's cutoff; the file goes in Tuesday's 9am window
- **The landing** — the network settles the batch overnight; the landlord sees $500 Wednesday 9am
- **The instant rail** — a UPI- or Pix-style transfer clears and settles at 5:00:05pm Monday
- **Hand-check** — Monday 5pm to Wednesday 9am is 40 hours; 40 hours vs 5 seconds is ~29,000×

*Example (italic):* Both payments start Monday 5:00pm — the instant-rail rent is spendable by 5:00:05pm, the ACH rent arrives Wednesday at 9:00am, 40 hours later.

**Key point:** ACH is a batch system — money waits for the next processing window and settles 1–2 business days later; a real-time rail clears each payment individually in seconds, 24/7.

### Visualization (canvas `c2`, 720×300)

Two-lane timeline from Monday 5pm to Wednesday 9am: the instant-rail payment landing immediately vs the ACH payment stepping through cutoff, batch window, and settlement.

- **Title (bold 15px, `#1a5276`, top center):** "The Same $500 Rent on Two Rails: 5 Seconds vs 40 Hours".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = elapsed hours 0 to 40 mapped at 15px/hour, 12px `#444` tick labels at "Mon 5pm" (x=60), "Tue 9am" (x=300), "Tue 11pm" (x=510), "Wed 9am" (x=660); vertical gridlines `#e5e9ef` at those ticks.
- **Instant lane (y=110), 12px `#444` label "UPI / Pix style" at x=62 above the lane:** green `#008300` dot at x=60 ("sent 5:00pm"), green dot at x=62 with bold 12px green label "final 5:00:05pm"; 3px green line between them.
- **ACH lane (y=180), 12px `#444` label "ACH" at x=62 above the lane:** blue `#2a78d6` 3px line from x=60 to x=660 with dots at x=60 ("sent 5:00pm"), x=300 ("Tue 9am batch window"), x=510 ("settles overnight"), x=660 (bold 12px blue "lands Wed 9am"); dot labels 11–12px `#444` alternating above/below the lane.
- **Annotation (bold 13px green `#008300`, near x=150, y=80):** "instant rail: done before you pocket your phone".
- **Annotation (bold 13px blue `#2a78d6`, near x=430, y=215):** "ACH: waits for the next window".
- **Caption (12px `#444`, bottom right):** "cutoff and window times illustrative; 1–2 business day ACH timing as documented".

## The Real-Time Wave: UPI, Pix, and FedNow

**Tags:** `where it's used` (blue), `real-time rails` (green), `adoption` (orange)

- **UPI (India, 2016)** — instant transfers addressed by phone number or ID, free for consumers
- **The scale** — UPI crossed roughly 10 billion payments per month in August 2023
- **Pix (Brazil, 2020)** — launched November 2020, free for individuals, billions of payments monthly
- **FedNow (US, 2023)** — the Federal Reserve's instant rail launched July 2023, joining ACH and wires
- **SEPA (Europe)** — the euro area's ACH-equivalent, plus SEPA Instant for 24/7 transfers in seconds
- **The reshaping** — near-zero fees and instant settlement pull payments off cards onto these rails

*Example (italic):* By 2023 UPI alone moved more payments per month (~10 billion) than many national card systems handle — seven years after launching in 2016.

**Key point:** The real-time wave — UPI (2016), Pix (2020), FedNow (2023) — made bank rails instant, always-on, and near-free, so the cheapest rail is increasingly also the fastest one.

### Visualization (canvas `c3`, 720×300)

Line chart of UPI monthly transaction volume 2017–2023 with dashed markers for the Pix and FedNow launches, showing the explosive real-time adoption curve.

- **Title (bold 15px, `#1a5276`, top center):** "UPI Monthly Payments: 0.1B to 10B in Seven Years — and Other Rails Follow".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = years 2017 to 2023 with 12px `#444` tick labels each year (100px apart); y = billions of payments per month 0 to 10, gridlines `#e5e9ef` at 2.5/5/7.5 with 12px `#444` labels "2.5B", "5B", "7.5B".
- **UPI line:** green `#008300` 3px line through years `[2017, 2019, 2020, 2021, 2022, 2023]`, monthly billions `[0.1, 1, 2, 4, 7, 10]`, 4px radius green dots at each point.
- **Pix marker:** vertical dashed `#6b7280` (dash 4/3) line at x for late 2020, bold 12px orange `#d95926` label "Pix launches (Nov 2020)" near its top.
- **FedNow marker:** vertical dashed `#6b7280` line at x for mid-2023, bold 12px violet `#4a3aa7` label "FedNow launches (Jul 2023)" near its top.
- **Annotation (bold 13px green `#008300`, near x=2022, y=75):** "~10B payments/month by Aug 2023".
- **Caption (12px `#444`, bottom right):** "UPI milestones publicly reported by NPCI, rounded; launch dates as documented".

## Instant Isn't Just Fast ACH

**Tags:** `common mistake` (red), `finality` (orange)

- **The confusion** — treating a real-time rail as "ACH but quicker"; the rails differ in kind, not speed
- **Pull can bounce** — an ACH debit that "landed" can return as unpaid days later (R01, 2 banking days)
- **The long tail** — a consumer can dispute an unauthorized ACH debit for up to 60 calendar days
- **Final is final** — UPI/Pix/FedNow settle irrevocably; any pull needs the payer's explicit approval
- **The mistake** — shipping goods when ACH funds "appear", then losing both goods and money to a return

*Example (italic):* A landlord who marks rent paid the moment an ACH debit posts can see the $500 vanish two days later on an insufficient-funds return — an instant-rail $500 has no NSF-style return; reversal takes a fraud process.

**Common mistake:** Confusing "the money showed up" with "the money is mine". ACH pulls are provisional and reversible for days; real-time rails trade ACH-style pulls for instant, irrevocable finality.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: an ACH pull that posts and then returns (money claws back) vs an instant push that is final in seconds, shown as payment-state boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Arrived Is Not Final: ACH Pull vs Instant Push".
- **Row 1 (boxes centered on y=100), label 12px `#444` at x=20:** "ACH pull"; blue `#2a78d6` rounded box at x=120 labeled "landlord debits $500" (12px), 3px arrow to a blue box at x=330 labeled "posts in 2 days (provisional)", 3px arrow to a red `#e74c3c` box at x=545 labeled "R01 return — $500 clawed back" with bold 12px red "✗ up to 60 days at risk".
- **Row 2 (boxes centered on y=210), label:** "instant push"; blue box at x=120 "you push $500", 3px arrow to a green `#008300` box at x=330 labeled "settles in seconds", then arrow to a green box at x=545 labeled "final — irrevocable" with bold 12px green "✓".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "speed and finality are different promises — instant rails make both, ACH pulls make neither".
- **Caption (12px `#444`, bottom right):** "R01 2-banking-day and 60-day unauthorized-return windows as documented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and positions above (no randomness); the $500 rent, 2.9% + 30¢ card rate, 25¢ ACH fee, and the Monday-5pm cutoff/window times are invented and labeled illustrative ($14.80 is exact math on the illustrative rate); UPI monthly-volume milestones `[0.1, 1, 2, 4, 7, 10]` billions for 2017–2023 are rounded publicly reported figures; launch dates (UPI 2016, Pix Nov 2020, FedNow Jul 2023), ACH 1–2 business day settlement, and the R01 2-banking-day / 60-calendar-day return windows are documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
