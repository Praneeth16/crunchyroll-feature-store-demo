# GPU training on AI Runtime, fed by the feature store

> *Job for GPU based training. AI Runtime for serverless GPU.*

Three ways to get a GPU, one training module behind all of them, and the feature-store
contract unchanged in every case.

`make gpu-train` runs it. `make probe` first if the workspace is new — AI Runtime is
**Public Preview** and regional.

## Why this exists in a feature-store repo

[`open_items.md`](open_items.md) §4 names the one place this POC does not scale: the
point-in-time join is Spark and scales, the **estimator does not**. Every other model
here collects to pandas and fits scikit-learn on one driver, and at 363k labels against
a 421k-row time-series table that did not finish inside a 60-minute task, twice — which
is why the rail ranker trains on a 25% session sample.

`notebooks/30_advanced/32_gpu_train.py` substitutes the estimator and **nothing else**:
the same `fe.create_training_set` with the same four `FeatureLookup`s and five
`FeatureFunction`s, the same IPS weights, the same `fe.log_model` contract, the same
request shape and output columns. Choosing an estimator stops being an architectural
decision, which is the actual point.

## What this workspace has

Measured by `notebooks/30_advanced/29b_gpu_probe.py`, not assumed:

| | |
|---|---|
| accelerator for `GPU_1xA10` | **NVIDIA A10G, 23 GB**, driver 580.126.16 |
| torch | **2.7.1+cu126**, CUDA 12.6, one visible device |
| `serverless_gpu.distributed` | importable |
| region | us-east-1 |

Accelerator enums: `GPU_1xA10`, `GPU_1xH100`, `GPU_8xH100` — the per-node GPU count is
encoded in the name, and `accelerator_count` must be a multiple of it.

**Regions.** Serverless GPU is documented for AWS us-west-2, us-west-1, us-east-1,
us-east-2, ca-central-1 and sa-east-1. Crunchyroll's own region, **GCP us-west1, is not
on that list** — the same gap as Lakebase. Treat this track as roadmap for them and as
working today on an AWS workspace.

## 1 · A notebook, interactively

Compute selector → **Serverless** → **Accelerator** → `1xA10` → Attach. The Standard
environment excludes `torch` to keep the image small (`%pip install torch`, or use the
Databricks AI environment which ships it).

```python
import torch
print(torch.cuda.get_device_name(0))
```

## 2 · A job task in the bundle — the one this repo uses

Two fields, and **both** are required; the first without the second fails the deploy
with `An environment is required for serverless task … when compute is set on task level`:

```yaml
      environments:
        - environment_key: gpu
          spec:
            environment_version: "4"        # supersedes the deprecated `client`
            dependencies:
              - torch
              - databricks-feature-engineering
              - scipy
      tasks:
        - task_key: gpu_train
          compute:
            hardware_accelerator: GPU_1xA10
          environment_key: gpu
          notebook_task:
            notebook_path: ../notebooks/30_advanced/32_gpu_train.py
          timeout_seconds: 7200
```

The accelerator is requested at **task start**, so an unavailable preview or an
exhausted GPU pool fails on the run page with a platform message rather than inside the
notebook. A `PENDING` task is usually the pool, not a bug — the probe run waited several
minutes for its A10.

### The multi-node shape

`ai_runtime_task` is a first-class DABs task type for multi-node GPU work. It runs a
**script at a workspace path** on every node rather than a notebook:

```yaml
        - task_key: gpu_train_multinode
          ai_runtime_task:
            experiment: /Users/<you>/crunchyroll_rail_ranker_gpu
            deployments:
              - name: trainer
                command_path: /Workspace/.../files/ai/train.sh
                compute:
                  accelerator_type: GPU_8xH100
                  accelerator_count: 8      # a multiple of the per-node count
```

Exactly one deployment is supported in the current preview, so driver/worker splits are
not yet available. This repo does not use it: one A10 trains this model in minutes, and
shipping a multi-node config that nobody has run would be the kind of unverified claim
the rest of the repo avoids.

## 3 · The `air` CLI, from a laptop

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv tool install --force databricks-air --python 3.12
air --version

databricks auth login --host https://<workspace> --profile <profile>
air run --file ai/train.yaml -p <profile> --watch
air list runs --limit 10
air logs <run-id>
air cancel <run-id>
```

[`ai/train.yaml`](../ai/train.yaml) declares `compute.accelerator_type`,
`num_accelerators`, the pip dependencies, a `code_source.snapshot` of `src/` and `ai/`,
and `timeout_minutes`. It runs [`ai/train_entrypoint.py`](../ai/train_entrypoint.py),
which calls the same `src/crfs/train_gpu.train` the notebook does.

## How the data reaches the GPU

This is the part that decides whether the path scales, and it is why the work is split
in two:

1. **Serverless, with Spark** — `fe.create_training_set(...)` does the point-in-time
   as-of join, and `TG.export_training_set` writes the result to Parquet on a UC volume
   (`/Volumes/<cat>/<schema>/crfs_ops/gpu_training/training_set`). The excluded
   `sample_weight` is re-attached here, with a row-count check, because a weight must
   not be a feature but the loop needs it.
2. **GPU task, no Spark** — reads that Parquet in row batches with `pyarrow.dataset` and
   trains in minibatches. The memory ceiling is the batch, not the dataset.

Consequences worth knowing: the expensive join runs once and every training iteration
after it is cheap; the GPU process needs no Spark session, which is what lets the same
module run under the `air` CLI; and a `dbfs:/Volumes/...` path is not a filesystem path,
so `TG._local()` normalises it before arrow opens it.

## The serving contract does not change

```python
fe.log_model(model=GpuRailRanker(), flavor=mlflow.pyfunc,
             training_set=training_set,            # <- the feature spec travels
             registered_model_name=MODEL, artifacts=…, input_example=…)
```

* Trained on GPU, **served on CPU** — a 60-feature MLP over tens of rows does not need
  an accelerator at inference, and a CPU endpoint is cheaper and already characterised
  by the benchmark in notebook 25.
* The wrapper returns `rail_id`, `engagement_probability`, `rail_rank`, ranked within
  `viewer_id` — identical to the sklearn rail ranker, so it is a drop-in for the same
  endpoint and the same app.
* It is registered, tagged with its definition fingerprint
  ([feature_versioning.md](feature_versioning.md)) and aliased `@challenger`. Promotion
  and deployment stay separate steps.
* Notebook 32 §7 scores the same rows through both models with `fe.score_batch` and
  reports the rank correlation. Neither call supplies a feature value.

## Cost

There is **no budget cap** on serverless GPU beyond the task timeout, so:

* the job pins `GPU_1xA10` — the cheapest accelerator, and one is enough here;
* `timeout_seconds: 7200` is a deliberate backstop, not a default;
* `air` has `timeout_minutes` and `usage_policy_name` for the same job;
* `make gpu-train` is **opt-in** — `make up` and `setup.sh` do not run it unless asked,
  so the default demo cost profile is unchanged;
* checkpoints go to the ops volume per epoch, so a killed run is resumable rather than
  repaid.

## Related

* [`../src/crfs/train_gpu.py`](../src/crfs/train_gpu.py) — the training module
* [`open_items.md`](open_items.md) §4 — the gap this closes
* [`cost_and_sizing.md`](cost_and_sizing.md) — the rest of the cost picture
