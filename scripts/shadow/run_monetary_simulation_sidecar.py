#!/usr/bin/env python3
"""Read-only RFC-014 fictitious monetary simulation sidecar.

The EUR stake is a normalized unit.  Price ratios are quoted in the same
PumpAPI-native quote unit; this script performs no SOL/EUR conversion and has
no network, wallet, transaction, or live-runtime dependency.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "src"))
from atlaspump.monetary_simulation import (  # noqa: E402
    COST_SCENARIOS,
    consensus_ids,
    first_price_at_or_after,
    maximum_drawdown,
    top_prediction_ids,
    valid_price,
    valuation,
    virtual_quantity,
)

HORIZONS = {"30s": 30, "60s": 60, "2m": 120, "5m": 300, "10m": 600, "15m": 900, "30m": 1800, "60m": 3600}
SCORES = {"CATBOOST": "catboost_score", "XGBOOST": "xgboost_score", "MLP": "mlp_score"}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iter_raw(path: Path):
    with path.open("rb") as handle, zstd.ZstdDecompressor().stream_reader(handle) as reader:
        pending = b""
        while block := reader.read(1024 * 1024):
            pending += block
            lines = pending.split(b"\n")
            pending = lines.pop()
            for line in lines:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def event_time(raw: dict) -> datetime | None:
    value = raw.get("timestamp")
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        return None
    return datetime.fromtimestamp(value / 1000 if value > 100_000_000_000 else value, UTC)


def price_index(paths: list[Path], mints: set[str]) -> tuple[dict[str, list[tuple[datetime, float]]], Counter]:
    index: dict[str, list[tuple[datetime, float]]] = {mint: [] for mint in mints}
    audit: Counter = Counter()
    for path in paths:
        for raw in iter_raw(path):
            audit["records_scanned"] += 1
            mint = raw.get("mint")
            if mint not in mints:
                continue
            audit["records_for_selected_mints"] += 1
            pool = str(raw.get("pool", "")).lower()
            price = raw.get("price")
            if pool not in {"pump", "pump-amm"}:
                audit["unsupported_pool_price"] += 1
                continue
            timestamp = event_time(raw)
            if not valid_price(price) or timestamp is None:
                audit["invalid_direct_price"] += 1
                continue
            index[str(mint)].append((timestamp, float(price)))
            audit["direct_price_points"] += 1
    for points in index.values():
        points.sort(key=lambda item: item[0])
    return index, audit


def memberships(predictions: pd.DataFrame) -> dict[str, set[str]]:
    rows = predictions[["prediction_id", *SCORES.values()]].to_dict("records")
    result: dict[str, set[str]] = {"ALL_VALIDATED": set(predictions.prediction_id.astype(str))}
    for label, score in SCORES.items():
        for fraction, pct in ((0.01, "1_PERCENT"), (0.05, "5_PERCENT"), (0.10, "10_PERCENT")):
            result[f"{label}_TOP_{pct}"] = top_prediction_ids(rows, score, fraction)
    for fraction, pct in ((0.01, "1_PERCENT"), (0.05, "5_PERCENT")):
        sets = [top_prediction_ids(rows, score, fraction) for score in SCORES.values()]
        result[f"CONSENSUS_2_OF_3_TOP_{pct}"] = consensus_ids(sets, 2)
        result[f"CONSENSUS_3_OF_3_TOP_{pct}"] = consensus_ids(sets, 3)
    return result


def performance(rows: pd.DataFrame) -> pd.DataFrame:
    output = []
    for keys, group in rows.groupby(["portfolio", "horizon", "cost_scenario", "valuation_policy"], dropna=False):
        selected = len(group)
        valued = group[group.valuation_status == "VALUED"]
        net = group.net_pnl_eur.astype(float)
        losses = net[net < 0]
        gains = net[net > 0]
        drawdown, drawdown_pct = maximum_drawdown(group.sort_values("exit_time").net_pnl_eur.astype(float).tolist())
        output.append({
            "portfolio": keys[0], "horizon": keys[1], "cost_scenario": keys[2], "valuation_policy": keys[3],
            "positions_selected": selected, "positions_valued": len(valued),
            "positions_censored": int((group.valuation_status == "NO_EXIT_PRICE").sum()),
            "positions_no_liquidity": int((group.valuation_status == "NO_EXIT_PRICE").sum()),
            "capital_committed_eur": float(selected), "final_value_eur": float(group.net_exit_value_eur.sum()),
            "net_pnl_eur": float(net.sum()), "return_on_committed_capital": float(net.sum() / selected) if selected else 0.0,
            "win_count": int((net > 0).sum()), "loss_count": int((net < 0).sum()), "flat_count": int((net == 0).sum()),
            "win_rate": float((net > 0).mean()) if selected else 0.0, "average_return": float(group.net_return.mean()),
            "median_return": float(group.net_return.median()), "p10_return": float(group.net_return.quantile(.1)),
            "p25_return": float(group.net_return.quantile(.25)), "p75_return": float(group.net_return.quantile(.75)),
            "p90_return": float(group.net_return.quantile(.9)), "best_position_return": float(group.net_return.max()),
            "worst_position_return": float(group.net_return.min()), "gross_profit_eur": float(gains.sum()),
            "gross_loss_eur": float(losses.sum()), "profit_factor": (float(gains.sum() / abs(losses.sum())) if len(losses) else "INFINITY"),
            "maximum_drawdown_eur": drawdown, "maximum_drawdown_percent": drawdown_pct,
            "maximum_simultaneous_capital_eur": None, "maximum_simultaneous_positions": None,
        })
    return pd.DataFrame(output)


def main(args: argparse.Namespace) -> int:
    run, out = args.source_run, args.output_run
    for directory in ("config", "state", "positions", "valuations", "reports", "logs", "manifests", "commands", "diagnostics"):
        (out / directory).mkdir(parents=True, exist_ok=True)
    features_path, predictions_path, outcomes_path = run / "features/features.parquet", run / "predictions/predictions.parquet", run / "outcomes/outcomes_multi_horizon.parquet"
    features, predictions, outcomes = (pq.read_table(path).to_pandas() for path in (features_path, predictions_path, outcomes_path))
    if "tensorflow_score" in predictions and "mlp_score" not in predictions:
        predictions["mlp_score"] = predictions["tensorflow_score"]
    if predictions.prediction_id.duplicated().any():
        raise RuntimeError("duplicate prediction_id")
    frame = predictions.merge(features[["token_mint", "feature_cutoff"]], on=["token_mint", "feature_cutoff"], validate="one_to_one")
    frame = frame.merge(outcomes[["token_mint", "creation_time", "migration_timestamp", "observation_end", "coverage_status", "censored"]], on=["token_mint", "creation_time"], validate="one_to_one")
    member_sets = memberships(frame)
    price_paths = sorted((args.archive_root / "archives").glob("*.jsonl.zst")) + [args.boundary_archive]
    prices, audit = price_index(price_paths, set(frame.token_mint.astype(str)))
    positions, valuation_rows = [], []
    for row in frame.to_dict("records"):
        mint, prediction_id = str(row["token_mint"]), str(row["prediction_id"])
        entry_time = pd.Timestamp(row["feature_cutoff"]).to_pydatetime()
        points = prices[mint]
        entry = first_price_at_or_after(points, entry_time, args.forward_tolerance_seconds)
        base = {"simulation_id": out.name, "source_run_id": run.name, "prediction_id": prediction_id, "feature_id": prediction_id, "mint": mint, "creation_time": row["creation_time"], "entry_time": entry_time, "virtual_stake_eur": 1.0, "catboost_score": row["catboost_score"], "xgboost_score": row["xgboost_score"], "mlp_score": row["mlp_score"], "consensus_memberships": json.dumps(sorted(name for name, ids in member_sets.items() if prediction_id in ids)), "migration_time": row["migration_timestamp"], "migrated": bool(pd.notna(row["migration_timestamp"])), "coverage_end_time": row["observation_end"]}
        if entry is None:
            base.update({"entry_price": None, "entry_price_source": "DIRECT_PUMPAPI_PRICE", "virtual_quantity": None, "position_status": "INVALID_ENTRY_PRICE"})
            positions.append(base)
            continue
        base.update({"entry_price": entry[1], "entry_price_source": "DIRECT_PUMPAPI_PRICE", "virtual_quantity": virtual_quantity(1.0, entry[1]), "position_status": "OPEN"})
        for label, seconds in HORIZONS.items():
            exit_point = first_price_at_or_after(points, entry_time + timedelta(seconds=seconds), args.forward_tolerance_seconds)
            base[f"exit_time_{label}"] = exit_point[0] if exit_point else None
            base[f"exit_price_{label}"] = exit_point[1] if exit_point else None
            base[f"exit_status_{label}"] = "VALUED" if exit_point else "NO_EXIT_PRICE"
        positions.append(base)
        exit_point = first_price_at_or_after(points, entry_time + timedelta(minutes=5), args.forward_tolerance_seconds)
        for portfolio, ids in member_sets.items():
            if prediction_id not in ids:
                continue
            for scenario, cost in COST_SCENARIOS.items():
                for policy in ("CENSORED", "PRUDENT_ZERO_ON_MISSING"):
                    if exit_point:
                        values = valuation(1.0, entry[1], exit_point[1], cost); status = "VALUED"; exit_time, exit_price = exit_point
                    elif policy == "CENSORED":
                        continue
                    else:
                        values = {"gross_exit_value_eur": 0.0, "net_exit_value_eur": 0.0, "gross_pnl_eur": -1.0, "net_pnl_eur": -1.0, "gross_return": -1.0, "net_return": -1.0}; status = "NO_EXIT_PRICE"; exit_time = exit_price = None
                    valuation_rows.append({"simulation_id": out.name, "prediction_id": prediction_id, "mint": mint, "portfolio": portfolio, "horizon": "5m", "cost_scenario": scenario, "valuation_policy": policy, "entry_time": entry_time, "exit_time": exit_time, "entry_price": entry[1], "exit_price": exit_price, "virtual_stake_eur": 1.0, "valuation_status": status, **values})
    positions_df, values_df = pd.DataFrame(positions), pd.DataFrame(valuation_rows)
    pq.write_table(pa.Table.from_pandas(positions_df, preserve_index=False), out / "positions/virtual_positions.parquet", compression="zstd")
    with (out / "positions/virtual_positions.jsonl").open("w", encoding="utf-8") as handle:
        for record in positions:
            handle.write(json.dumps(record, default=str, separators=(",", ":")) + "\n")
    pq.write_table(pa.Table.from_pandas(values_df, preserve_index=False), out / "valuations/position_valuations.parquet", compression="zstd")
    perf = performance(values_df); perf.to_csv(out / "reports/portfolio_performance.csv", index=False)
    all_realistic = values_df[(values_df.portfolio == "ALL_VALIDATED") & (values_df.cost_scenario == "REALISTIC") & (values_df.valuation_policy == "CENSORED")].sort_values("exit_time")
    all_realistic = all_realistic.assign(cumulative_pnl_eur=all_realistic.net_pnl_eur.cumsum(), cumulative_capital_committed_eur=range(1, len(all_realistic)+1), active_capital_eur=1.0)
    all_realistic[["exit_time", "cumulative_pnl_eur", "cumulative_capital_committed_eur", "active_capital_eur"]].to_csv(out / "reports/equity_curve.csv", index=False)
    pq.write_table(
        pa.Table.from_pandas(
            all_realistic[["exit_time", "cumulative_pnl_eur", "cumulative_capital_committed_eur", "active_capital_eur"]],
            preserve_index=False,
        ),
        out / "reports/equity_curve.parquet",
        compression="zstd",
    )
    deciles=[]; score_perf={}
    valued = values_df[(values_df.portfolio == "ALL_VALIDATED") & (values_df.cost_scenario == "GROSS") & (values_df.valuation_policy == "CENSORED")]
    joined = frame.merge(valued[["prediction_id", "net_return"]], on="prediction_id")
    for label, score in SCORES.items():
        joined["decile"] = pd.qcut(joined[score].rank(method="first"), 10, labels=False) + 1
        grouped = joined.groupby("decile", observed=True).agg(count=("prediction_id","size"), mean_return=("net_return","mean"), win_rate=("net_return",lambda x:(x>0).mean()), pnl=("net_return","sum")).reset_index()
        for record in grouped.to_dict("records"): deciles.append({"model":label, **record})
        score_perf[label] = {"spearman_score_return": float(joined[[score,"net_return"]].corr(method="spearman").iloc[0,1])}
    pd.DataFrame(deciles).to_csv(out / "reports/score_deciles.csv", index=False)
    (out / "reports/score_performance.json").write_text(json.dumps(score_perf, indent=2) + "\n")
    coverage = {"positions_total":len(positions_df), "entry_valued":int(positions_df.entry_price.notna().sum()), "entry_missing":int(positions_df.entry_price.isna().sum()), "t5_valued":int((positions_df.exit_status_5m == "VALUED").sum()), "t5_missing":int((positions_df.exit_status_5m == "NO_EXIT_PRICE").sum())}
    (out / "reports/data_coverage.json").write_text(json.dumps(coverage, indent=2) + "\n")
    price_report = {"selected_source":"price", "quote_asset":"PumpAPI quote unit; relative ratio only, no SOL/EUR conversion", "event_types_available":"buy,sell,create,migrate where observed", "records":dict(audit), "entry_forward_tolerance_seconds":args.forward_tolerance_seconds, "known_limitations":"No assertion of economic price semantics beyond consistent relative quote ratios."}
    (out / "diagnostics/price_source_audit.json").write_text(json.dumps(price_report, indent=2) + "\n")
    (out / "reports/price_source_audit.json").write_text(json.dumps(price_report, indent=2) + "\n")
    summary = {"simulation_only":True,"real_money":False,"capital_policy":"unlimited","virtual_stake_eur":1.0,"source_run":str(run),"source_snapshot":args.source_snapshot,"price_methodology":"direct PumpAPI price ratio", "cost_scenarios":COST_SCENARIOS,"coverage_status":coverage,"lookahead_protection":"entry/exit use first price at or after target only","positions":len(positions_df),"valuations":len(values_df)}
    (out / "reports/simulation_summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    manifest = {**summary,"source_sha256s":{path.name:sha256(path) for path in (features_path,predictions_path,outcomes_path)},"artifacts": ["positions/virtual_positions.parquet","valuations/position_valuations.parquet","reports/portfolio_performance.csv"]}
    (out / "manifests/simulation_manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n")
    files=sorted(path for path in out.rglob("*") if path.is_file() and path.name != "artifact_sha256s.txt")
    (out / "artifact_sha256s.txt").write_text("".join(f"{sha256(path)}  {path.relative_to(out)}\n" for path in files))
    return 0


if __name__ == "__main__":
    parser=argparse.ArgumentParser(); parser.add_argument("--source-run",type=Path,required=True); parser.add_argument("--output-run",type=Path,required=True); parser.add_argument("--archive-root",type=Path,required=True); parser.add_argument("--boundary-archive",type=Path,required=True); parser.add_argument("--source-snapshot",required=True); parser.add_argument("--forward-tolerance-seconds",type=int,default=10); raise SystemExit(main(parser.parse_args()))
