# Databricks notebook source
# MAGIC %md
# MAGIC # 29b · Is serverless GPU (AI Runtime) available to this job?
# MAGIC
# MAGIC Companion to notebook 29, for the other preview this track needs. It reports
# MAGIC what the task was actually given — accelerator, driver, CUDA, visible devices —
# MAGIC and costs one short task on the smallest accelerator.
# MAGIC
# MAGIC The job that runs this asks for a GPU with a task-level `compute` block:
# MAGIC
# MAGIC ```yaml
# MAGIC - task_key: gpu_probe
# MAGIC   compute:
# MAGIC     hardware_accelerator: GPU_1xA10
# MAGIC   notebook_task:
# MAGIC     notebook_path: ../notebooks/30_advanced/29b_gpu_probe.py
# MAGIC ```
# MAGIC
# MAGIC If the accelerator is not available the task fails at **start**, before this
# MAGIC notebook runs, and the run page carries the reason. If it starts and `cuda` is
# MAGIC absent, the preview is on but the task did not get a device — which is a
# MAGIC different problem with a different fix, and that is why both are checked.
# COMMAND ----------
import json
import os
import platform
import subprocess
import sys

findings = {"python": platform.python_version()}
# COMMAND ----------
# MAGIC %md
# MAGIC ## What the platform handed this task
# COMMAND ----------
# nvidia-smi is the platform's answer, independent of any Python package being
# installed -- a missing torch would otherwise look identical to a missing GPU.
try:
    smi = subprocess.run(
        ["nvidia-smi", "--query-gpu=name,memory.total,driver_version",
         "--format=csv,noheader"],
        capture_output=True, text=True, timeout=60)
    out = (smi.stdout or smi.stderr).strip()
    print("nvidia-smi:", out or "(no output)")
    findings["nvidia_smi"] = out
except FileNotFoundError:
    findings["nvidia_smi"] = "nvidia-smi not on PATH -- this task has no GPU"
    print(findings["nvidia_smi"])
except Exception as e:
    findings["nvidia_smi"] = f"{type(e).__name__}: {e}"
    print(findings["nvidia_smi"])

for var in ["CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES", "RANK", "LOCAL_RANK",
            "WORLD_SIZE", "MASTER_ADDR"]:
    if var in os.environ:
        print(f"  {var}={os.environ[var]}")
        findings[var] = os.environ[var]
# COMMAND ----------
# MAGIC %md
# MAGIC ## Torch, and whether it sees the device
# MAGIC
# MAGIC The Standard serverless environment excludes `torch` so the image stays small;
# MAGIC the Databricks AI environment ships it. Installing here rather than assuming
# MAGIC means this probe answers the same question in either.
# COMMAND ----------
try:
    import torch
except ModuleNotFoundError:
    print("torch not present; installing")
    subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "torch"], check=False)
    try:
        import torch
    except ModuleNotFoundError:
        torch = None

findings["torch"] = torch.__version__ if torch else "not installable in this environment"

if torch is not None:
    print("torch:", torch.__version__)
    avail = torch.cuda.is_available()
    findings["cuda_available"] = bool(avail)
    print("cuda available:", avail)
    if avail:
        findings["device_name"] = torch.cuda.get_device_name(0)
        findings["device_count"] = torch.cuda.device_count()
        findings["cuda_version"] = torch.version.cuda
        print("device:", findings["device_name"], "| count:", findings["device_count"],
              "| cuda:", findings["cuda_version"])
        # A real op, not just a capability flag: a device that reports available but
        # cannot run a kernel is a failure worth surfacing here rather than in training.
        x = torch.randn(2048, 2048, device="cuda")
        y = (x @ x).sum().item()
        print("matmul on device ok, checksum:", round(y, 3))
        findings["matmul"] = "ok"
# COMMAND ----------
# MAGIC %md
# MAGIC ## Is the `serverless_gpu` distributed API importable?
# MAGIC
# MAGIC `@distributed` is what notebook 32 uses to run a training function across the
# MAGIC GPUs of a node. It ships with the AI Runtime environment rather than from PyPI,
# MAGIC so its absence means the environment, not the accelerator.
# COMMAND ----------
try:
    from serverless_gpu import distributed  # noqa: F401
    findings["serverless_gpu"] = "importable"
    print("serverless_gpu.distributed importable")
except Exception as e:
    findings["serverless_gpu"] = f"{type(e).__name__}: {e}"
    print("serverless_gpu not importable:", findings["serverless_gpu"])
# COMMAND ----------
ready = bool(findings.get("cuda_available")) and findings.get("matmul") == "ok"
print("\nGPU usable for training here:", ready)
dbutils.notebook.exit(json.dumps({"gpu_ready": ready, **findings}, default=str))
