#!/usr/bin/env bash
# Sample: submit a job to a Ray cluster (e.g. KubeRay).
# Set RAY_ADDRESS or pass --address. Credentials (DATABRICKS_*) must be set in the job environment.

set -euo pipefail

RAY_ADDRESS="${RAY_ADDRESS:-http://kuberay-1.dev.meesho.int}"

# 1) Minimal Ray cluster check (no Daft)
echo "=== Ray cluster resources (no Daft) ==="
ray job submit --address "$RAY_ADDRESS" -- python -c "
import ray
ray.init()
print(ray.cluster_resources())
"

# 2) Optional: run daft-sql-adapter on the cluster
# Ensure the cluster has this package (and DATABRICKS_* set, e.g. via runtime-env or image).
#
# ray job submit --address "$RAY_ADDRESS" -- \
#   daft-sql-adapter --sql "SELECT 1 AS x" --tables "" --output /tmp/out.arrow --metadata /tmp/meta.json
