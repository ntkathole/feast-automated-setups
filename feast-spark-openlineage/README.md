# Feast + SparkApplication compute engine + OpenLineage consumer (RHOAI/ODH)

## What this deploys

| Piece | How |
|-------|-----|
| Feature definitions | `feature_repo/` (postgres driver template) via `feastProjectDir.git` |
| FeatureStore | `overlays/featurestore.yaml` |
| Batch engine | `spark_application` (Kubeflow Spark Operator CRDs) |
| Lineage | Feast OpenLineage **consumer** on registry REST (`/v1/lineage`) |
| Images | `…:develop-openlineage-spark-parentlink` (Feast + OL + pyspark + **materialize→Spark parentRun**); `…:spark-application-0.65-ol-listener` (Spark 4 + openlineage-spark JAR) |
| Spark → Feast OL | `spark_conf` listener → registry-rest `/v1/lineage`; parent facet links Spark jobs under `materialize_<project>` |

## Prerequisites

1. ODH/RHOAI with `feastoperator: Managed` and `sparkoperator: Managed`
2. Namespace `feast` with Postgres + Redis and secrets `postgres-secret`, `feast-data-stores` (include `postgres`, `redis`, `sql` keys)
3. Cluster can pull the quay.io/nkathole images

## Apply

```bash
# Seed driver offline table once
kubectl -n feast run seed-driver --rm -it --restart=Never \
  --image=quay.io/nkathole/feature-server:develop-openlineage-spark \
  --env-from=secret/postgres-secret \
  --env=PG_HOST=postgres.feast.svc.cluster.local \
  --command -- python - <<'PY'
# or copy seed_driver_data.py into the pod
PY

kubectl apply -f overlays/featurestore.yaml
```

Trigger materialization (creates SparkApplication CRs):

```bash
kubectl create job --from=cronjob/feast-lineage-demo-periodic <name> -n feast
# or: kubectl exec into a feast pod and run feast materialize-incremental
```

View lineage in the Feast UI OpenLineage graph (not Marquez).

## Git source for the operator

```yaml
feastProjectDir:
  git:
    url: https://github.com/ntkathole/feast-automated-setups.git
    ref: main
    featureRepoPath: feast-spark-openlineage/feature_repo
```
