# RFC-011 — Analytical Ranking Interface

RFC-011 evaluates the twelve RFC-010 candidates on their final out-of-sample
prediction partitions for 2026-04-25 and 2026-04-26. It supports deterministic
top-k, percentile and score-threshold selections, score/label metrics, exports
and a local Streamlit reader.

The declared level is `ANALYTICAL_RANKING_ONLY`. It contains no price,
execution, capital, fee, slippage, PnL, return or drawdown field. Scores and
labels are not evidence of profitability.

RFC-013 owns price reconstruction, execution assumptions and financial replay.
It may consume RFC-011 selections but cannot change their historical scores.

The two days remain separate. No strategy is automatically optimized on them;
the UI only exposes descriptive comparisons. Limitations include the short RFC
corpus, scarce migrations, only two out-of-sample days, and unvalidated price
semantics.
