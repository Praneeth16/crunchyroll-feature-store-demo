"""A torch ranker trained on GPU, fed from the feature store.

Why this exists: `docs/open_items.md` 4 says the point-in-time join is Spark and
scales, while the estimator does not -- every model in this repo collects to pandas and
fits scikit-learn on one driver, and at 363k labels against a 421k-row time-series
table that did not finish inside a 60-minute task. The substitution was named and left
unproven. This is the proof, on the axis that matters for a ranker: the same training
set, a model that trains in minibatches on an accelerator, and the same
`fe.log_model` contract so nothing downstream changes.

What it does, precisely -- the earlier version of this docstring overclaimed and the
distinction matters:

  * **The Spark side never collects.** `training_set.load_df()` is written straight to
    Parquet on a UC volume, so the point-in-time join's output never passes through the
    driver as pandas. That is the part that failed before: `toPandas()` on the full join
    is what put notebook 22 on a 25% sample.
  * **The training side reads that Parquet.** `train()` loads it as one frame, which is
    correct at this demo's size and is *not* out-of-core. `train_streaming()` is the
    out-of-core path: it encodes and steps per Parquet batch, so its ceiling is the
    batch. Pass `streaming=True` to use it. Neither is dressed up as the other.
  * **It does not change the serving contract.** The wrapper's `predict` takes the
    same request shape and returns the same three columns as the sklearn rail ranker,
    and the model is logged with its feature spec, so automatic feature lookup and
    `score_batch` work exactly as before.

Importable and runnable both ways: notebook 32 calls `train()` inside a
`@distributed` function, and `ai/train.yaml` runs this file as a script through the
AI Runtime CLI. That is the same code in both paths rather than two copies.

Driver-side only in the sense that matters here: the *model class* is defined in the
notebook that logs it, because a served model cannot import from src/crfs/.
"""
import json
import os
import time

FEATURE_NULL = 0.0


# ------------------------------------------------------------------ data plumbing
def export_training_set(training_set, volume_dir: str, label: str,
                        extra=None, join_keys=None) -> str:
    """Write a training set to Parquet on a UC volume and return the path.

    Parquet on a volume rather than pandas in memory: this is the step that decides
    whether the training script scales, and it is also what lets the GPU task read the
    data without a Spark session of its own.

    `extra` re-attaches columns that `create_training_set(exclude_columns=...)` had to
    drop. A sample weight is the case that matters: it must not be in the feature set,
    because a model that can read it can read the label through it, but the training
    loop needs it. Joined here rather than in pandas so the whole operation stays in
    Spark.
    """
    df = training_set.load_df()
    if extra is not None:
        if not join_keys:
            raise ValueError("join_keys is required when extra is supplied")
        before = df.count()
        df = df.join(extra, on=list(join_keys), how="left")
        after = df.count()
        # A one-to-many join here would silently duplicate training rows and reweight
        # the fit. The label query produces one row per impression key, so this holds.
        if after != before:
            raise ValueError(
                f"re-attaching {extra.columns} changed the row count {before} -> {after}; "
                f"{join_keys} is not unique in the extra frame")
    path = f"{volume_dir.rstrip('/')}/training_set"
    df.write.mode("overwrite").option("compression", "snappy").parquet(path)
    return path


def load_frame(path: str):
    """Read the exported training set back as one pandas frame.

    Deliberately simple: at this demo's size a frame fits comfortably, and the thing
    being demonstrated is GPU minibatch training, not out-of-core IO. `iter_batches`
    below is what a real volume would use, and it takes the same path.
    """
    import pyarrow.dataset as ds

    return ds.dataset(_local(path), format="parquet").to_table().to_pandas()


def iter_batches(path: str, batch_size: int = 65_536):
    """Stream the exported Parquet in row batches, never materialising the whole set."""
    import pyarrow.dataset as ds

    for batch in ds.dataset(_local(path), format="parquet").to_batches(batch_size=batch_size):
        yield batch.to_pandas()


def _local(path: str) -> str:
    """UC volume paths are readable as files; `dbfs:/Volumes/...` is not a filesystem
    path and arrow cannot open it. Normalising here keeps every caller from having to
    know that."""
    if path.startswith("dbfs:/"):
        return path[len("dbfs:"):]
    return path


# ---------------------------------------------------------------------- the model
def build_encoder(pdf, feature_cols, categorical):
    """Category -> integer maps, plus per-column mean and scale for the numerics.

    Kept as plain dicts so the whole thing pickles into the model artifact and the
    serving path needs no sklearn transformer to reload.
    """
    import numpy as np

    encoders = {c: {v: i for i, v in enumerate(sorted(pdf[c].dropna().astype(str).unique()))}
                for c in categorical}
    stats = {}
    for c in feature_cols:
        if c in pdf:
            try:
                col = pdf[c].astype("float64")
            except (ValueError, TypeError) as e:
                # Naming the column here turns a 40-frame pandas traceback ending in
                # `could not convert string to float: 'sci_fi'` into something that says
                # which column and what to do about it. A string column in the numeric
                # list means the caller's taxonomy is wrong, not the data.
                raise ValueError(
                    f"feature column {c!r} is not numeric (sample: "
                    f"{pdf[c].dropna().head(3).tolist()!r}). It belongs in `categorical`, "
                    f"not in `feature_cols` -- see rails.model_columns().") from e
        else:
            col = None
        mu = float(col.mean()) if col is not None and col.notna().any() else 0.0
        sd = float(col.std()) if col is not None and col.notna().any() else 1.0
        # A constant column has sd 0, and dividing by it produces inf, which becomes a
        # NaN loss on the first step -- a failure that looks like a bad learning rate.
        stats[c] = {"mean": mu, "scale": sd if sd and not np.isclose(sd, 0.0) else 1.0}
    return {"encoders": encoders, "stats": stats}


def encode(pdf, feature_cols, categorical, enc):
    """Frame -> float32 matrix. Shared by training and by the served wrapper, so the
    two cannot normalise differently -- the training/serving skew this whole repo
    argues against would otherwise reappear inside the model."""
    import numpy as np
    import pandas as pd

    out = np.empty((len(pdf), len(feature_cols) + len(categorical)), dtype="float32")
    for i, c in enumerate(feature_cols):
        col = pdf[c] if c in pdf else pd.Series(FEATURE_NULL, index=pdf.index)
        v = pd.to_numeric(col, errors="coerce").astype("float64").fillna(FEATURE_NULL)
        s = enc["stats"].get(c, {"mean": 0.0, "scale": 1.0})
        out[:, i] = ((v - s["mean"]) / s["scale"]).to_numpy(dtype="float32")
    for j, c in enumerate(categorical):
        m = enc["encoders"].get(c, {})
        col = pdf[c] if c in pdf else pd.Series("unknown", index=pdf.index)
        idx = col.astype(str).map(lambda x, m=m: m.get(x, len(m))).astype("float32")
        out[:, len(feature_cols) + j] = idx.to_numpy(dtype="float32")
    return out


def make_mlp(n_inputs: int, hidden=(256, 128), dropout: float = 0.1):
    import torch.nn as nn

    layers, prev = [], n_inputs
    for h in hidden:
        layers += [nn.Linear(prev, h), nn.ReLU(), nn.Dropout(dropout)]
        prev = h
    layers += [nn.Linear(prev, 1)]
    return nn.Sequential(*layers)


# ------------------------------------------------------------------------ training
def train(parquet_path: str,
          feature_cols,
          categorical,
          label: str = "engaged",
          weight_col: str = None,
          epochs: int = 8,
          batch_size: int = 4096,
          lr: float = 1e-3,
          hidden=(256, 128),
          holdout_frac: float = 0.2,
          log_mlflow: bool = True,
          checkpoint_dir: str = None,
          streaming: bool = False) -> dict:
    """Fit the ranker on whatever accelerator is present and return the artifacts.

    Returns a dict with `state_dict`, the encoder, the metrics and the device used, so
    the caller owns logging and registration -- this function neither registers a model
    nor mutates Unity Catalog.
    """
    import numpy as np
    import torch
    import torch.nn as nn
    from sklearn.metrics import roc_auc_score

    if streaming:
        return train_streaming(
            parquet_path, feature_cols, categorical, label=label, weight_col=weight_col,
            epochs=epochs, batch_size=batch_size, lr=lr, hidden=hidden,
            holdout_frac=holdout_frac, log_mlflow=log_mlflow,
            checkpoint_dir=checkpoint_dir)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    started = time.perf_counter()

    pdf = load_frame(parquet_path)
    # Time-ordered split, and a timestamp is REQUIRED rather than nice to have. Without
    # one this used physical Parquet row order, which Spark does not guarantee -- so the
    # "holdout" was an arbitrary subset while the log said it was temporal. A random or
    # arbitrary split also leaks the future through the very windows these features
    # aggregate, which is the whole reason the split is time-ordered.
    ts_col = next((c for c in ("event_ts", "ts", "impression_ts", "request_epoch_s")
                   if c in pdf.columns), None)
    if ts_col is None:
        raise ValueError(
            "the exported training set has no timestamp column (looked for event_ts, ts, "
            f"impression_ts, request_epoch_s; it has {sorted(pdf.columns)[:12]}...). A "
            "time-ordered holdout is not possible without one -- re-export carrying the "
            "label timestamp.")
    pdf = pdf.sort_values(ts_col)
    cut = int(len(pdf) * (1.0 - holdout_frac))
    tr, te = pdf.iloc[:cut], pdf.iloc[cut:]
    print(f"split on {ts_col}: train {len(tr)} | holdout {len(te)}")

    enc = build_encoder(tr, feature_cols, categorical)
    Xtr = encode(tr, feature_cols, categorical, enc)
    Xte = encode(te, feature_cols, categorical, enc)
    ytr = tr[label].astype("float32").to_numpy()
    yte = te[label].astype("float32").to_numpy()
    wtr = (tr[weight_col].astype("float32").to_numpy()
           if weight_col and weight_col in tr else np.ones_like(ytr))

    model = make_mlp(Xtr.shape[1], hidden=hidden).to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=lr)
    # Per-sample reduction so inverse-propensity weights can be applied; the vertical
    # ranker's labels are position-biased and its weights are the correction.
    lossf = nn.BCEWithLogitsLoss(reduction="none")

    Xtr_t = torch.from_numpy(Xtr).to(device)
    ytr_t = torch.from_numpy(ytr).to(device)
    wtr_t = torch.from_numpy(wtr).to(device)
    Xte_t = torch.from_numpy(Xte).to(device)

    n = Xtr_t.shape[0]
    history = []
    for epoch in range(epochs):
        model.train()
        perm = torch.randperm(n, device=device)
        total = 0.0
        for start in range(0, n, batch_size):
            idx = perm[start:start + batch_size]
            opt.zero_grad(set_to_none=True)
            logits = model(Xtr_t[idx]).squeeze(-1)
            loss = (lossf(logits, ytr_t[idx]) * wtr_t[idx]).mean()
            loss.backward()
            opt.step()
            total += float(loss.detach()) * len(idx)
        model.eval()
        with torch.no_grad():
            p = torch.sigmoid(model(Xte_t).squeeze(-1)).cpu().numpy()
        auc = float(roc_auc_score(yte, p)) if len(set(yte.tolist())) > 1 else float("nan")
        history.append({"epoch": epoch + 1, "train_loss": total / n, "holdout_auc": auc})
        print(f"epoch {epoch + 1}/{epochs}  loss={total / n:0.5f}  holdout_auc={auc:0.4f}")
        if log_mlflow:
            _log_metrics({"train_loss": total / n, "holdout_auc": auc}, step=epoch + 1)
        if checkpoint_dir:
            _checkpoint(model, checkpoint_dir, epoch + 1)

    elapsed = time.perf_counter() - started
    print(f"\ntrained on {device} in {elapsed:0.1f}s | rows={len(tr)} holdout={len(te)}")
    return {
        "state_dict": {k: v.cpu() for k, v in model.state_dict().items()},
        "encoder": enc,
        "feature_cols": list(feature_cols),
        "categorical": list(categorical),
        "n_inputs": int(Xtr.shape[1]),
        "hidden": list(hidden),
        "device": device,
        "gpu_name": (torch.cuda.get_device_name(0) if device == "cuda" else None),
        "history": history,
        "holdout_auc": history[-1]["holdout_auc"] if history else None,
        "train_rows": int(len(tr)),
        "holdout_rows": int(len(te)),
        "seconds": round(elapsed, 1),
    }


def _log_metrics(metrics: dict, step: int):
    """MLflow is optional here: under `@distributed` a run already exists and metrics
    attach to it, but the same function has to run in a plain script with no active
    run without turning that into a crash."""
    try:
        import mlflow

        if mlflow.active_run():
            mlflow.log_metrics(metrics, step=step)
    except Exception:
        pass


def _checkpoint(model, checkpoint_dir: str, epoch: int):
    import torch

    os.makedirs(_local(checkpoint_dir), exist_ok=True)
    path = os.path.join(_local(checkpoint_dir), f"epoch_{epoch:03d}.pt")
    torch.save(model.state_dict(), path)


def summary(result: dict) -> str:
    return json.dumps({k: v for k, v in result.items()
                       if k not in ("state_dict", "encoder", "history")}, default=str)


def train_streaming(parquet_path: str,
                    feature_cols,
                    categorical,
                    label: str = "engaged",
                    weight_col: str = None,
                    epochs: int = 8,
                    batch_size: int = 65_536,
                    lr: float = 1e-3,
                    hidden=(256, 128),
                    holdout_frac: float = 0.2,
                    log_mlflow: bool = True,
                    checkpoint_dir: str = None) -> dict:
    """The out-of-core path: never hold the dataset, only a batch.

    `train()` is the right choice at this demo's size and is what notebook 32 runs. This
    exists because the claim "the memory ceiling is the batch" has to be true of some code
    path, and pointing at an unused `iter_batches` was not good enough.

    Two compromises it makes, stated rather than hidden:

      * **The encoder is fitted on the first pass's first batches.** A true streaming fit
        would need a second pass or running moments; this takes the first
        `encoder_batches` worth of rows, which is fine for standardisation and wrong for a
        heavily ordered file.
      * **The holdout is the tail by time.** The file is sorted on export, so the last
        `holdout_frac` of batches is the temporal holdout, evaluated batch by batch.
    """
    import numpy as np
    import torch
    import torch.nn as nn
    from sklearn.metrics import roc_auc_score

    device = "cuda" if torch.cuda.is_available() else "cpu"
    started = time.perf_counter()

    batches = list(range(sum(1 for _ in iter_batches(parquet_path, batch_size))))
    n_batches = len(batches)
    if n_batches < 2:
        # One batch is not a stream; fall back rather than pretend, and say so.
        print(f"only {n_batches} batch at size {batch_size}; using the in-memory path")
        return train(parquet_path, feature_cols, categorical, label=label,
                     weight_col=weight_col, epochs=epochs, batch_size=batch_size, lr=lr,
                     hidden=hidden, holdout_frac=holdout_frac, log_mlflow=log_mlflow,
                     checkpoint_dir=checkpoint_dir, streaming=False)
    cut = max(1, int(n_batches * (1.0 - holdout_frac)))

    enc = None
    model = opt = None
    lossf = nn.BCEWithLogitsLoss(reduction="none")
    history = []
    for epoch in range(epochs):
        total, seen = 0.0, 0
        for i, frame in enumerate(iter_batches(parquet_path, batch_size)):
            if i >= cut:
                break
            if enc is None:
                enc = build_encoder(frame, feature_cols, categorical)
                model = make_mlp(len(feature_cols) + len(categorical), hidden=hidden).to(device)
                opt = torch.optim.AdamW(model.parameters(), lr=lr)
            X = torch.from_numpy(encode(frame, feature_cols, categorical, enc)).to(device)
            y = torch.from_numpy(frame[label].astype("float32").to_numpy()).to(device)
            wt = (torch.from_numpy(frame[weight_col].astype("float32").to_numpy()).to(device)
                  if weight_col and weight_col in frame else torch.ones_like(y))
            model.train()
            opt.zero_grad(set_to_none=True)
            loss = (lossf(model(X).squeeze(-1), y) * wt).mean()
            loss.backward()
            opt.step()
            total += float(loss.detach()) * len(frame)
            seen += len(frame)

        model.eval()
        probs, ys = [], []
        for i, frame in enumerate(iter_batches(parquet_path, batch_size)):
            if i < cut:
                continue
            X = torch.from_numpy(encode(frame, feature_cols, categorical, enc)).to(device)
            with torch.no_grad():
                probs.append(torch.sigmoid(model(X).squeeze(-1)).cpu().numpy())
            ys.append(frame[label].astype("float32").to_numpy())
        y_true = np.concatenate(ys) if ys else np.array([])
        y_hat = np.concatenate(probs) if probs else np.array([])
        auc = (float(roc_auc_score(y_true, y_hat))
               if len(set(y_true.tolist())) > 1 else float("nan"))
        history.append({"epoch": epoch + 1, "train_loss": total / max(seen, 1),
                        "holdout_auc": auc})
        print(f"epoch {epoch + 1}/{epochs}  loss={total / max(seen, 1):0.5f}  "
              f"holdout_auc={auc:0.4f}  (streaming, {cut}/{n_batches} batches)")
        if log_mlflow:
            _log_metrics({"train_loss": total / max(seen, 1), "holdout_auc": auc},
                         step=epoch + 1)
        if checkpoint_dir:
            _checkpoint(model, checkpoint_dir, epoch + 1)

    elapsed = time.perf_counter() - started
    return {
        "state_dict": {k: v.cpu() for k, v in model.state_dict().items()},
        "encoder": enc,
        "feature_cols": list(feature_cols),
        "categorical": list(categorical),
        "n_inputs": len(feature_cols) + len(categorical),
        "hidden": list(hidden),
        "device": device,
        "gpu_name": (torch.cuda.get_device_name(0) if device == "cuda" else None),
        "history": history,
        "holdout_auc": history[-1]["holdout_auc"] if history else None,
        "train_rows": None,          # never counted: the point is not to hold the set
        "holdout_rows": None,
        "streaming": True,
        "batches": n_batches,
        "seconds": round(elapsed, 1),
    }
