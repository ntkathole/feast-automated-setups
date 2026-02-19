# Automated Feast Operator Deployment

Automated setup for deploying a Feast FeatureStore instance on Kubernetes/OpenShift using the Feast Operator, backed by Redis (online store) and PostgreSQL (registry).

## Prerequisites

- `kubectl` (or `oc` for OpenShift) configured and connected to your cluster
- The Feast Operator CRD installed, or use the `-o` flag to install it automatically

## Directory Structure

```
feast-postgres-redis/
├── setup.sh            # Deploy datastores + FeatureStore
├── teardown.sh         # Remove everything cleanly
├── templates/
│   ├── redis.yaml      # Redis Deployment + Service template
│   ├── postgres.yaml   # PostgreSQL Secret + Deployment + Service template
│   └── feast.yaml      # Feast data-store Secret + FeatureStore CR template
├── generated/          # (auto-created) Rendered manifests with your namespace
└── README.md
```

## Quick Start

```bash
# Deploy into a new namespace "my-feast", installing the operator too
./setup.sh -n my-feast -c -o

# Deploy into the default "feast" namespace (must already exist)
./setup.sh

# Use 'oc' instead of 'kubectl' (OpenShift)
KUBECTL_CMD=oc ./setup.sh -n my-feast -c -o
```

## Setup Options

| Flag | Description |
|------|-------------|
| `-n, --namespace NAME` | Target namespace (default: `feast`) |
| `-c, --create-namespace` | Create the namespace if it doesn't exist |
| `-o, --operator-install` | Install the Feast Operator from `dist/install.yaml` |
| `--skip-datastores` | Skip Redis/PostgreSQL deployment |
| `--skip-feast` | Skip FeatureStore CR deployment (deploy only datastores) |
| `--skip-apply` | Skip running `feast apply` after deployment |
| `--wait SECONDS` | Timeout for pod readiness checks (default: 120) |
| `--apply-timeout SECS` | Timeout waiting for the Feast deployment before running apply (default: 300) |

## Teardown Options

```bash
# Remove everything and delete the namespace
./teardown.sh -n my-feast --delete-namespace -o

# Remove only the FeatureStore, keep datastores
./teardown.sh -n my-feast --skip-datastores
```

| Flag | Description |
|------|-------------|
| `-n, --namespace NAME` | Target namespace (default: `feast`) |
| `-o, --operator-uninstall` | Also remove the Feast Operator |
| `--delete-namespace` | Delete the namespace after cleanup |
| `--skip-datastores` | Keep Redis and PostgreSQL running |
| `--skip-feast` | Keep the FeatureStore CR |

## Deployment Flow

1. **Prerequisites check** — verifies `kubectl` connectivity
2. **Template rendering** — replaces `__NAMESPACE__` in all YAML templates and writes to `generated/`
3. **Namespace setup** — creates or validates the target namespace
4. **Operator install** (optional) — applies `infra/feast-operator/dist/install.yaml`
5. **Datastore deployment** — applies PostgreSQL and Redis manifests, waits for pods to be ready
6. **FeatureStore deployment** — applies the Feast Secret and FeatureStore CR, waits for all pods to be Ready
7. **Feast apply** — waits for the Feast deployment to be available, then execs `feast apply` directly in the pod

## Customization

### Using a different FeatureStore configuration

Edit `templates/feast.yaml` to change:
- `feastProject` name
- Git repository URL and ref for `feastProjectDir`
- Online/offline store types and persistence settings
- Resource requests/limits

Keep `__NAMESPACE__` as the placeholder — the setup script replaces it at render time.

### Using OpenShift

Set the `KUBECTL_CMD` environment variable:

```bash
export KUBECTL_CMD=oc
./setup.sh -n my-feast -c -o
```

### Custom PostgreSQL credentials

Edit the `postgres-secret` in `templates/postgres.yaml` to change the database name, user, or password. The connection string in `templates/feast.yaml` references these via `${POSTGRES_USER}`, `${POSTGRES_PASSWORD}`, and `${POSTGRES_DB}` environment variables injected from the same secret.
