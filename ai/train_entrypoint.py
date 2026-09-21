#!/usr/bin/env python
"""Script entry point for GPU training, for the `air` CLI and `ai_runtime_task`.

Notebook 32 is the interactive path and this is the headless one. Both call
`src.crfs.train_gpu.train`, so the training code has exactly one definition.

What this does NOT do is build the training set. That needs Spark and a feature store
client; this process has neither, by design. Notebook 32 exports the point-in-time
training set to a UC volume once, and every GPU iteration after that reads Parquet --
which is also what makes the GPU task cheap to re-run while tuning.

    air run --file ai/train.yaml -p <profile> --watch
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crfs import train_gpu as TG  # noqa: E402

NON_FEATURES = {"engaged", "sample_weight", "ts", "event_ts", "viewer_id", "rail_id",
                "request_epoch_s", "device", "locale", "rail_position", "was_viewport"}
CATEGORICAL = ["device", "locale"]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--parquet", required=True,
                   help="UC volume path the training set was exported to")
    ap.add_argument("--label", default="engaged")
    ap.add_argument("--weight-col", default="sample_weight")
    ap.add_argument("--epochs", type=int, default=8)
    ap.add_argument("--batch-size", type=int, default=4096)
    ap.add_argument("--lr", type=float, default=1e-3)
    ap.add_argument("--hidden", default="256,128")
    ap.add_argument("--checkpoint-dir", default=None)
    ap.add_argument("--out", default=None,
                   help="where to write the metrics summary; defaults to stdout only")
    args = ap.parse_args(argv)

    head = TG.load_frame(args.parquet).head(200)
    feature_cols = [c for c in head.columns if c not in NON_FEATURES]
    if not feature_cols:
        raise SystemExit(
            f"no feature columns in {args.parquet}. Its columns are {list(head.columns)} "
            "-- has notebook 32 exported the training set to this path?")
    print(f"{len(feature_cols)} feature columns, {len(CATEGORICAL)} categorical")

    result = TG.train(
        parquet_path=args.parquet,
        feature_cols=feature_cols,
        categorical=CATEGORICAL,
        label=args.label,
        weight_col=args.weight_col,
        epochs=args.epochs,
        batch_size=args.batch_size,
        lr=args.lr,
        hidden=tuple(int(h) for h in args.hidden.split(",") if h.strip()),
        checkpoint_dir=args.checkpoint_dir,
    )
    summary = TG.summary(result)
    print(summary)
    if args.out:
        with open(TG._local(args.out), "w") as fh:
            fh.write(summary)
    # Registration deliberately stays in the notebook: `fe.log_model` needs the
    # TrainingSet object to attach the feature spec, and that object only exists where
    # the training set was built. A model logged from here would serve without
    # automatic feature lookup, which is the one property this repo is about.
    return json.loads(summary)


if __name__ == "__main__":
    main()
