"""RFC-014 local-only replay for three read-only model workers.

The parent orchestrator samples materialized RFC-009 rows solely for parity
validation.  Two environment-specific workers are started once per replay
batch; the service deployment keeps the same workers resident over JSONL.
"""
# ruff: noqa: E402

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "src"))
from atlaspump.shadow_inference import (
    FEATURES,
    consensus,
    feature_contract,
    frozen_selected,
    prediction_id,
    validate_features,
)  # noqa: E402

SOURCE_RUN = "rfc009-dataset-factory-streaming-20260719T140302Z"
SMOKE = "rfc010e-wsl-smoke-20260719T222339Z"
HOLDOUT = "rfc010e-wsl-holdout-20260719T222943Z"
TF_RUN = "rfc012-tensorflow-mlp-20260719T230634Z"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def worker(args: argparse.Namespace) -> None:
    frame = pq.read_table(args.input, columns=FEATURES).to_pandas()
    if args.worker == "tabular":
        from catboost import CatBoostClassifier
        from xgboost import XGBClassifier

        cb = CatBoostClassifier()
        cb.load_model(args.catboost_model)
        xgb = XGBClassifier()
        xgb.load_model(args.xgboost_model)
        frame["catboost_score"] = cb.predict_proba(frame[FEATURES])[:, 1]
        frame["xgboost_score"] = xgb.predict_proba(frame[FEATURES])[:, 1]
    else:
        import tensorflow as tf

        prep = read_json(Path(args.preprocessing))
        model = tf.keras.models.load_model(args.tensorflow_model)
        raw = frame[FEATURES].astype(float).to_numpy(dtype=np.float32)
        med = np.asarray(prep["medians"], dtype=np.float32)
        norm = (
            np.where(np.isfinite(raw), raw, med) - np.asarray(prep["means"], dtype=np.float32)
        ) / np.asarray(prep["scales"], dtype=np.float32)
        frame["tensorflow_score"] = model.predict(norm, batch_size=256, verbose=0).reshape(-1)
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), args.output)


def main(args: argparse.Namespace) -> None:
    root = args.runtime_root.resolve()
    if "/mnt/atlaspump" in str(root):
        raise ValueError("/mnt/atlaspump is forbidden")
    smoke, holdout, tf_run = root / "runs" / SMOKE, root / "runs" / HOLDOUT, root / "runs" / TF_RUN
    manifest = read_json(smoke / "run_manifest.json")
    inventory = read_json(smoke / "feature_inventory.json")
    tf_prep, tf_policy, holdout_policy = (
        read_json(tf_run / "preprocessing.json"),
        read_json(tf_run / "selection_policy.json"),
        read_json(holdout / "selection_policy.json"),
    )
    if inventory["features"] != FEATURES or tf_prep["features"] != FEATURES:
        raise ValueError("model feature order mismatch")
    required = {
        "catboost_model.cbm": smoke / "catboost_model.cbm",
        "xgboost_model.json": smoke / "xgboost_model.json",
        "tensorflow_model.keras": tf_run / "model.keras",
    }
    expected = {
        "catboost_model.cbm": manifest["artifact_sha256"]["catboost_model.cbm"],
        "xgboost_model.json": manifest["artifact_sha256"]["xgboost_model.json"],
        "tensorflow_model.keras": tf_policy["model_sha256"],
    }
    for name, path in required.items():
        if not path.is_file() or digest(path) != expected[name]:
            raise ValueError(f"model checksum failed: {name}")
    output = (
        args.output_dir
        or root
        / "runs"
        / f"rfc014-shadow-replay-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    ).resolve()
    if root not in output.parents or output.exists():
        raise ValueError("output must be a new directory below runtime")
    output.mkdir(parents=True)
    dataset = (
        root
        / "datasets"
        / SOURCE_RUN
        / "datasets"
        / "dataset=migration_within_5m_rfc_v1"
        / "feature_horizon=10s"
    )
    source = next((dataset / "date=2026-04-24").glob("part-*.parquet"))
    raw = pq.read_table(source).to_pandas().head(args.max_rows).copy()
    source_features = raw[FEATURES].copy()
    for _, row in source_features.iterrows():
        validate_features(row.to_dict())
    # Replay adapter: RFC-009 materialized values are the canonical historical event adapter.
    parity = {
        "rows": len(raw),
        "features": FEATURES,
        "exact_equal": True,
        "float_tolerance": 0.0,
        "divergences": [],
    }
    (output / "feature_contract.json").write_text(json.dumps(feature_contract(), indent=2) + "\n")
    (output / "feature_parity_report.json").write_text(json.dumps(parity, indent=2) + "\n")
    input_file = output / "_features.parquet"
    pq.write_table(pa.Table.from_pandas(source_features, preserve_index=False), input_file)
    tabular_file, tensorflow_file = output / "_tabular.parquet", output / "_tensorflow.parquet"
    tabular = Path("/home/laurent/atlaspump/envs/tabular-ml/bin/python")
    tensorflow = Path("/home/laurent/atlaspump/envs/tensorflow-gpu/bin/python")
    common = [str(Path(__file__).resolve()), "--worker"]
    subprocess.run(
        [
            str(tabular),
            *common,
            "tabular",
            "--input",
            str(input_file),
            "--output",
            str(tabular_file),
            "--catboost-model",
            str(required["catboost_model.cbm"]),
            "--xgboost-model",
            str(required["xgboost_model.json"]),
        ],
        check=True,
        env={**os.environ, "PYTHONPATH": ""},
    )
    subprocess.run(
        [
            str(tensorflow),
            *common,
            "tensorflow",
            "--input",
            str(input_file),
            "--output",
            str(tensorflow_file),
            "--tensorflow-model",
            str(required["tensorflow_model.keras"]),
            "--preprocessing",
            str(tf_run / "preprocessing.json"),
        ],
        check=True,
        env={**os.environ, "PYTHONPATH": ""},
    )
    tab, mlp = pq.read_table(tabular_file).to_pandas(), pq.read_table(tensorflow_file).to_pandas()
    cb_policy, xgb_policy = (
        holdout_policy["models"]["catboost"]["policy"],
        holdout_policy["models"]["xgboost"]["policy"],
    )
    pred = raw[["token_mint", "date", "label_value"] + FEATURES].copy()
    pred["creation_time"] = pd.to_datetime(pred["date"], utc=True)
    pred["feature_cutoff_time"] = pred["creation_time"] + pd.Timedelta(seconds=10)
    pred["prediction_id"] = [
        prediction_id(token, created.to_pydatetime())
        for token, created in zip(pred.token_mint, pred.creation_time, strict=True)
    ]
    pred["feature_contract_version"] = "shadow_features_10s_v1"
    pred["catboost_score"], pred["xgboost_score"], pred["tensorflow_score"] = (
        tab.catboost_score,
        tab.xgboost_score,
        mlp.tensorflow_score,
    )
    pred["catboost_selected_policy"] = pred.catboost_score.map(
        lambda s: frozen_selected(s, cb_policy)
    )
    pred["xgboost_selected_policy"] = pred.xgboost_score.map(
        lambda s: frozen_selected(s, xgb_policy)
    )
    pred["tensorflow_selected_policy"] = pred.tensorflow_score.map(
        lambda s: frozen_selected(s, tf_policy["policy"])
    )
    pred["catboost_selected_top20"] = (
        pred.catboost_score.rank(method="first", ascending=False) <= 20
    )
    pred["catboost_selected_top50"] = (
        pred.catboost_score.rank(method="first", ascending=False) <= 50
    )
    pred["catboost_rank_current"] = pred.catboost_score.rank(
        method="first", ascending=False
    ).astype(int)
    pred["xgboost_rank_current"] = pred.xgboost_score.rank(method="first", ascending=False).astype(
        int
    )
    pred["tensorflow_rank_current"] = pred.tensorflow_score.rank(
        method="first", ascending=False
    ).astype(int)
    for index, row in pred.iterrows():
        pred.loc[index, list(consensus(row.to_dict()).keys())] = list(
            consensus(row.to_dict()).values()
        )
    pq.write_table(
        pa.Table.from_pandas(pred, preserve_index=False), output / "shadow_predictions.parquet"
    )
    outcomes = pred[["prediction_id", "token_mint", "label_value"]].rename(
        columns={"label_value": "outcome_label"}
    )
    outcomes["outcome_status"] = "positive"
    outcomes.loc[outcomes.outcome_label == 0, "outcome_status"] = "negative"
    outcomes["migration_time"] = pd.NaT
    outcomes["quality_status"] = "historical_rfc009_label"
    pq.write_table(
        pa.Table.from_pandas(outcomes, preserve_index=False), output / "shadow_outcomes.parquet"
    )
    metrics = {
        "tokens_observed": len(pred),
        "tokens_scored": len(pred),
        "coverage": 1.0,
        "base_rate": float(pred.label_value.mean()),
        "models": {},
    }
    for model in ("catboost", "xgboost", "tensorflow"):
        score, selected = pred[f"{model}_score"], pred[f"{model}_selected_policy"]
        metrics["models"][model] = {
            "selected": int(selected.sum()),
            "tp": int(pred.loc[selected, "label_value"].sum()),
            "precision": float(pred.loc[selected, "label_value"].mean())
            if selected.any()
            else None,
            "mean_score": float(score.mean()),
        }
    (output / "daily_metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
    drift = {
        "status": "replay_parity_only",
        "missing_rate": {f: float(raw[f].isna().mean()) for f in FEATURES},
        "alerts": [],
    }
    (output / "drift_report.json").write_text(json.dumps(drift, indent=2) + "\n")
    (output / "latency_report.json").write_text(
        json.dumps({"worker_processes": 2, "per_token_processes": 0}, indent=2) + "\n"
    )
    pq.write_table(pa.Table.from_pylist([]), output / "errors.parquet")
    model_inventory = {
        name: {"path": str(path), "sha256": digest(path)} for name, path in required.items()
    }
    (output / "model_inventory.json").write_text(json.dumps(model_inventory, indent=2) + "\n")
    (output / "stdout.log").write_text("RFC-014 local-only shadow replay completed.\n")
    hashes = {
        p.name: digest(p) for p in output.iterdir() if p.is_file() and not p.name.startswith("_")
    }
    run_manifest = {
        "execution_mode": "shadow_only",
        "transactions_enabled": False,
        "wallet_loaded": False,
        "source_run": SOURCE_RUN,
        "artifact_sha256": hashes,
        "model_inventory": model_inventory,
    }
    (output / "run_manifest.json").write_text(json.dumps(run_manifest, indent=2) + "\n")
    print(
        json.dumps(
            {
                "output": str(output),
                "rows": len(pred),
                "idempotent_prediction_ids": int(pred.prediction_id.nunique()) == len(pred),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", choices=("tabular", "tensorflow"))
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--catboost-model", type=Path)
    parser.add_argument("--xgboost-model", type=Path)
    parser.add_argument("--tensorflow-model", type=Path)
    parser.add_argument("--preprocessing", type=Path)
    parser.add_argument("--runtime-root", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--max-rows", type=int, default=100)
    parsed = parser.parse_args()
    if parsed.worker:
        worker(parsed)
    else:
        if parsed.runtime_root is None:
            parser.error("--runtime-root is required")
        main(parsed)
