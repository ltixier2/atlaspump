#!/usr/bin/env python3
"""Long-lived, read-only model worker for RFC-014 shadow streaming."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "src"))

from atlaspump.shadow_inference import FEATURES  # noqa: E402


def main(args: argparse.Namespace) -> None:
    if args.kind == "tabular":
        from catboost import CatBoostClassifier
        from xgboost import XGBClassifier

        catboost = CatBoostClassifier()
        catboost.load_model(args.catboost_model)
        xgboost = XGBClassifier()
        xgboost.load_model(args.xgboost_model)
    else:
        import tensorflow as tf

        gpus = tf.config.list_physical_devices("GPU")
        print(
            json.dumps(
                {
                    "tensorflow_python": sys.executable,
                    "tensorflow_version": tf.__version__,
                    "visible_gpus": [gpu.name for gpu in gpus],
                    "tensorflow_device": args.tensorflow_device,
                    "model": args.tensorflow_model,
                }
            ),
            file=sys.stderr,
            flush=True,
        )
        if args.tensorflow_device == "gpu" and not gpus:
            raise RuntimeError("--tensorflow-device gpu requires visible /GPU:0")
        preprocessing = json.loads(Path(args.preprocessing).read_text(encoding="utf-8"))
        model = tf.keras.models.load_model(args.tensorflow_model)
        means = np.asarray(preprocessing["means"], dtype=np.float32)
        scales = np.asarray(preprocessing["scales"], dtype=np.float32)
        medians = np.asarray(preprocessing["medians"], dtype=np.float32)
    for line in sys.stdin:
        request = json.loads(line)
        values = np.asarray(
            [[row[name] for name in FEATURES] for row in request["rows"]], dtype=float
        )
        if args.kind == "tabular":
            response = {
                "catboost_score": catboost.predict_proba(values)[:, 1].tolist(),
                "xgboost_score": xgboost.predict_proba(values)[:, 1].tolist(),
            }
        else:
            normalized = (np.where(np.isfinite(values), values, medians) - means) / scales
            response = {
                "tensorflow_score": model.predict(normalized, batch_size=256, verbose=0)
                .reshape(-1)
                .tolist()
            }
        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=("tabular", "tensorflow"), required=True)
    parser.add_argument("--catboost-model")
    parser.add_argument("--xgboost-model")
    parser.add_argument("--tensorflow-model")
    parser.add_argument("--preprocessing")
    parser.add_argument("--tensorflow-device", choices=("auto", "gpu", "cpu"), default="auto")
    main(parser.parse_args())
