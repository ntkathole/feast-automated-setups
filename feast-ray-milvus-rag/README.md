# Feast RAG with Ray + Milvus + PostgreSQL (KubeRay)

Automated setup for deploying a **RAG (Retrieval-Augmented Generation)** pipeline on Kubernetes/OpenShift using the Feast Operator, backed by:

- **Ray** as the offline store and compute engine (distributed processing via KubeRay + codeflare-sdk)
- **Milvus** as the online store (vector similarity search)
- **PostgreSQL** as the registry
- **HuggingFace SQuAD** dataset read via `RaySource`

## Architecture

```
                    ┌─────────────────────────────────┐
                    │   HuggingFace SQuAD Dataset      │
                    │   (via RaySource reader)          │
                    └──────────┬──────────────────────┘
                               │
                    ┌──────────▼──────────────────────┐
                    │   KubeRay Cluster                │
                    │   (head + 1 worker)              │
                    │   created via codeflare-sdk      │
                    │   distributed embedding gen      │
                    │   sentence-transformers/          │
                    │   all-MiniLM-L6-v2 (384-dim)     │
                    └──────────┬──────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
    ┌─────────▼────────┐ ┌────▼──────┐ ┌──────▼─────────┐
    │   Ray             │ │ PostgreSQL│ │   Milvus        │
    │   (offline store) │ │ (registry)│ │   (online store)│
    │   data processing │ │ SQL-based │ │   vector search │
    └──────────────────┘ └───────────┘ └────────────────┘
```

## Prerequisites

- `kubectl` (or `oc` for OpenShift) configured and connected to your cluster
- The **Feast Operator** CRD installed, or use the `-o` flag to install it automatically
- The **KubeRay operator** installed, or use the `--kuberay-install` flag to install it
- A Git repository containing the feature definitions (see [Feature Repository](#feature-repository))

## Directory Structure

```
feast-ray-milvus-rag/
├── setup.sh                       # Deploy everything
├── teardown.sh                    # Remove everything cleanly
├── create_ray_cluster.py          # Create/delete Ray cluster via codeflare-sdk
├── Dockerfile.feature-server      # Custom feature server image (feast[ray] + patches)
├── ray_initializer.py             # Patched ray_initializer.py for the custom image
├── rag_demo.ipynb                 # Jupyter notebook for the RAG workflow demo
├── templates/
│   ├── postgres.yaml              # PostgreSQL Secret + Deployment + Service
│   ├── milvus.yaml                # Milvus (standalone + etcd + MinIO) Deployment + Service
│   ├── ray-batch-engine-cm.yaml   # ConfigMap for Feast batch engine → Ray (with runtime_env)
│   └── feast.yaml                 # Feast Secrets + FeatureStore CR (with batchEngine ref)
├── feature_repo/                  # Feature definitions (host in your own Git repo)
│   ├── feature_store.yaml         # Local testing reference config
│   ├── rag_features.py            # Entity, RaySource, BatchFeatureView, FeatureService
│   └── test_workflow.py           # Demo: query similar passages via Feast + Milvus
├── generated/                     # (auto-created) Rendered manifests with your namespace
└── README.md
```

## Feature Repository

The `feature_repo/` directory contains the Feast feature definitions for the RAG pipeline. These must be hosted in a Git repository that the Feast Operator can clone.

### What the Feature Repo Contains

| File | Purpose |
|------|---------|
| `rag_features.py` | Defines a `RaySource` reading HuggingFace SQuAD, a `BatchFeatureView` with a Ray-native UDF for distributed embedding generation, and a `FeatureService` for retrieval |
| `feature_store.yaml` | Feast project configuration (offline/online/registry) - used for local testing; the operator overrides this via the CR |
| `test_workflow.py` | Demo script that generates query embeddings and searches for similar passages via `retrieve_online_documents_v2` |

### Key Design Choices

- **RaySource with HuggingFace reader**: Uses `reader_type="huggingface"` to load `rajpurkar/squad` directly via `ray.data.from_huggingface()` - no local data files needed
- **Ray-native BatchFeatureView**: The `PassageEmbeddingProcessor` class is a Ray actor that loads the embedding model once per worker and processes batches efficiently
- **sentence-transformers/all-MiniLM-L6-v2**: 384-dimensional embeddings with cosine similarity, suitable for passage retrieval
- **Ray offline store**: Uses `RayOfflineStore` to natively handle `RaySource` data loading and processing
- **PostgreSQL registry**: SQL-based metadata storage for feature definitions

### Hosting the Feature Repo

Push the `feature_repo/` contents to a Git repository:

```bash
cd feature_repo
git init
git add .
git commit -m "Feast RAG feature definitions"
git remote add origin https://github.com/yourorg/feast-rag-features.git
git push -u origin main
```

## Quick Start

```bash
# Deploy everything into a new namespace, installing both operators
./setup.sh -n feast-rag -c -o --kuberay-install \
  -g https://github.com/yourorg/feast-rag-features.git -r main

# Deploy into the default "feast-rag" namespace (must already exist)
./setup.sh -g https://github.com/yourorg/feast-rag-features.git

# Use 'oc' instead of 'kubectl' (OpenShift)
KUBECTL_CMD=oc ./setup.sh -n feast-rag -c -o --kuberay-install \
  -g https://github.com/yourorg/feast-rag-features.git
```

## Setup Options

| Flag | Description |
|------|-------------|
| `-n, --namespace NAME` | Target namespace (default: `feast-rag`) |
| `-c, --create-namespace` | Create the namespace if it doesn't exist |
| `-o, --operator-install` | Install the Feast Operator from `dist/install.yaml` |
| `--kuberay-install` | Install the KubeRay operator from `opendatahub-io/kuberay` |
| `--kuberay-version VER` | KubeRay version to install (default: `v1.3.0`) |
| `-g, --git-url URL` | Git repo URL containing the feature_repo (required) |
| `-r, --git-ref REF` | Git branch/tag/commit to checkout (default: `main`) |
| `--skip-datastores` | Skip Milvus/PostgreSQL deployment |
| `--skip-ray` | Skip RayCluster deployment |
| `--skip-feast` | Skip FeatureStore CR deployment (deploy only infra) |
| `--skip-apply` | Skip running `feast apply` after deployment |
| `--wait SECONDS` | Timeout for pod readiness checks (default: 180) |
| `--apply-timeout SECS` | Timeout for Feast deployment readiness and apply Job completion (default: 600) |

## Teardown Options

```bash
# Remove everything including namespace and operators
./teardown.sh -n feast-rag --delete-namespace -o --kuberay-uninstall

# Remove only the FeatureStore, keep infra
./teardown.sh -n feast-rag --skip-datastores --skip-ray
```

| Flag | Description |
|------|-------------|
| `-n, --namespace NAME` | Target namespace (default: `feast-rag`) |
| `-o, --operator-uninstall` | Also remove the Feast Operator |
| `--kuberay-uninstall` | Also remove the KubeRay operator |
| `--delete-namespace` | Delete the namespace after cleanup |
| `--skip-datastores` | Keep Milvus and PostgreSQL running |
| `--skip-ray` | Keep the RayCluster running |
| `--skip-feast` | Keep the FeatureStore CR |

## Deployment Flow

1. **Prerequisites check** - verifies `kubectl` connectivity and required parameters
2. **Template rendering** - replaces `__NAMESPACE__`, `__FEATURE_REPO_URL__`, and `__FEATURE_REPO_REF__` in all YAML templates
3. **Namespace setup** - creates or validates the target namespace
4. **Feast Operator install** (optional) - applies `infra/feast-operator/dist/install.yaml`
5. **KubeRay operator install** (optional) - installs from `opendatahub-io/kuberay` via kustomize
6. **Datastore deployment** - deploys PostgreSQL and Milvus (standalone with embedded etcd), waits for readiness
7. **Ray cluster deployment** - creates Ray cluster via codeflare-sdk (1 head + 1 worker) and the batch engine `ConfigMap`, waits for all Ray pods
8. **FeatureStore deployment** - applies secrets and FeatureStore CR (with `batchEngine` referencing the Ray ConfigMap), polls until Ready
9. **Feast apply** - triggers a one-off Job from the operator-created CronJob to register feature definitions

## Ray Cluster Details

The Ray cluster is created programmatically via **codeflare-sdk** (see `create_ray_cluster.py`):

| Component | Replicas | Image | Resources |
|-----------|----------|-------|-----------|
| Head node | 1 | `quay.io/modh/ray:2.54.1-py312-cu128` | 1 CPU, 3-6Gi memory |
| Workers | 1 | `quay.io/modh/ray:2.54.1-py312-cu128` | 1 CPU, 2-4Gi memory |

The Feast Operator connects to Ray via a `ConfigMap` (`feast-ray-batch-engine`) that configures `batch_engine.type: ray.engine` with `use_kuberay: true` and `kuberay_conf` pointing to the cluster. A `runtime_env` in `kuberay_conf` installs `datasets` and `sentence-transformers` on the Ray workers at connection time.

## Post-Deployment: Materialization

After the FeatureStore CR is ready and `feast apply` has run, materialize the embeddings:

```bash
# Materialize SQuAD passage embeddings (Ray will generate embeddings distributedly)
kubectl exec deploy/feast-rag-demo -n feast-rag -- \
  feast materialize 2020-01-01T00:00:00 2026-12-31T23:59:59

# Run the demo to query similar passages
kubectl exec deploy/feast-rag-demo -n feast-rag -- \
  python test_workflow.py
```

## Customization

### Using a Different HuggingFace Dataset

Edit `feature_repo/rag_features.py` and change the `RaySource`:

```python
squad_source = RaySource(
    name="nq_passages",
    reader_type="huggingface",
    reader_options={
        "dataset_name": "google-research-datasets/natural_questions",
        "split": "train",
    },
    timestamp_field="event_timestamp",
)
```

### Using a Different Embedding Model

Update `EMBED_MODEL_ID` and `EMBEDDING_DIM` in `rag_features.py`, and update the `embedding_dim` in `templates/feast.yaml` (the Milvus secret).

### Scaling the Ray Cluster

Use `create_ray_cluster.py` to adjust worker count and resources:

```bash
python create_ray_cluster.py --workers 4 --worker-memory 4 --worker-memory-limit 8
```

### GPU-Accelerated Embeddings

Add GPU resources via codeflare-sdk and update the batch engine ConfigMap:

```yaml
# In ray-batch-engine-cm.yaml
config: |
  type: ray.engine
  num_gpus: 1
```

### Using OpenShift

```bash
export KUBECTL_CMD=oc
./setup.sh -n feast-rag -c -o --kuberay-install \
  -g https://github.com/yourorg/feast-rag-features.git
```

## Comparison with feast-postgres-redis

| Aspect | feast-postgres-redis | feast-ray-milvus-rag |
|--------|---------------------|--------------------------|
| Online store | Redis | Milvus (vector search) |
| Offline store | DuckDB (file) | Ray |
| Registry | PostgreSQL (SQL) | PostgreSQL (SQL) |
| Compute engine | Default (local) | Ray via KubeRay (codeflare-sdk) |
| Data source | Git-hosted parquet | HuggingFace via RaySource |
| Use case | Credit scoring | RAG document retrieval |
| Embedding support | No | Yes (384-dim, cosine) |
| Infra components | 2 (Postgres, Redis) | 4 (Postgres, Milvus, Ray head, Ray workers) |

## Dependencies (for local feature repo development)

```bash
pip install feast[ray] sentence-transformers pymilvus psycopg2-binary datasets codeflare-sdk
```
