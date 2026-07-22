# Token summary EDA v2

Version `token_summary_eda_v2_common`. Timestamps are UTC epoch milliseconds.

The common seven-day view contains `token_mint`, `logical_date`, creation timestamp,
duration, event count, unique-wallet count, observed migration, one-minute events,
backend/schema provenance, and availability flags. Null means unavailable/not computed;
it is never replaced by zero. The enriched RFC view is the original 19–24 April RFC
schema and is explicitly six-day only.

Day 18 maps directly for common counters and one-minute activity. Detailed windows,
censoring and wallet detail are `NOT_COMPUTED` and exposed through availability flags.
