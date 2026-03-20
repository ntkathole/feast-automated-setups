#!/bin/bash
#===============================================================================
# Launch All Benchmark Jobs in Parallel
# Creates jobs for: 4 stores × 3 refs = 12 jobs (each runs both scenarios)
#===============================================================================

set -e

NAMESPACE="feast-benchmark"
REGISTRY="image-registry.openshift-image-registry.svc:5000/feast-benchmark/feast-benchmark"
REFS=("v0.59.0" "v0.60.0" "v0.61.0")
STORES=("sqlite" "redis" "postgres" "dynamodb")
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JOBS_DIR="$SCRIPT_DIR/k8s/jobs"
K8S_DIR="$SCRIPT_DIR/k8s"

echo ""
echo "=============================================="
echo "Setting up benchmark infrastructure"
echo "=============================================="

# Apply all infrastructure via top-level kustomization
# This includes: namespace, PVC, ConfigMaps (benchmark-config, benchmark-script), stores (redis, postgres)
echo "Applying infrastructure via Kustomize..."
if command -v kustomize &>/dev/null; then
    kustomize build "$K8S_DIR" | oc apply -f -
else
    oc kustomize "$K8S_DIR" | oc apply -f -
fi

# Wait for stores to be ready
echo "Waiting for Redis..."
oc rollout status deployment/redis -n $NAMESPACE --timeout=60s || true
echo "Waiting for Postgres..."
oc rollout status deployment/postgres -n $NAMESPACE --timeout=60s || true

# Clean up existing jobs
echo "Cleaning up existing jobs..."
oc delete jobs -l app=feast-benchmark -n $NAMESPACE 2>/dev/null || true
sleep 2

echo ""
echo "=============================================="
echo "Launching ALL Benchmark Jobs"
echo "=============================================="
echo "Refs: ${REFS[*]}"
echo "Stores: ${STORES[*]}"
echo "Scenarios: entity_scaling, feature_scaling"
echo ""

JOB_COUNT=0

for REF in "${REFS[@]}"; do
    echo "--- Creating jobs for ref: $REF ---"
    for STORE in "${STORES[@]}"; do
        JOB_NAME="bench-${REF//./-}-${STORE}"
        JOB_TEMPLATE="$JOBS_DIR/${STORE}-job.yaml"
        
        if [[ ! -f "$JOB_TEMPLATE" ]]; then
            echo "Warning: Template not found: $JOB_TEMPLATE"
            continue
        fi
        
        # Create temporary modified job file
        TEMP_JOB=$(mktemp)
        
        # Modify job: change name, image tag, and FEAST_REF value
        cat "$JOB_TEMPLATE" | \
            sed "s|name: feast-benchmark-${STORE}|name: ${JOB_NAME}|g" | \
            sed "s|:latest|:${REF}|g" | \
            sed 's|value: "master"|value: "'"${REF}"'"|g' > "$TEMP_JOB"
        
        # Verify the FEAST_REF was set correctly
        if ! grep -q "value: \"${REF}\"" "$TEMP_JOB"; then
            echo "ERROR: Failed to set FEAST_REF=${REF} in job template"
            cat "$TEMP_JOB" | grep -A1 FEAST_REF || true
            rm -f "$TEMP_JOB"
            continue
        fi
        
        # Apply the modified job
        oc apply -f "$TEMP_JOB" -n $NAMESPACE
        rm -f "$TEMP_JOB"
        
        echo "  Created: $JOB_NAME"
        ((JOB_COUNT++))
    done
done

echo ""
echo "=============================================="
echo "Created $JOB_COUNT benchmark jobs"
echo "=============================================="
echo ""
echo "Monitor progress:"
echo "  watch 'oc get pods -l app=feast-benchmark -n $NAMESPACE'"
echo ""
echo "View logs for a specific job:"
echo "  oc logs -f job/bench-v0-60-0-sqlite -n $NAMESPACE"
echo ""
echo "Wait for all to complete:"
echo "  oc wait --for=condition=complete job -l app=feast-benchmark -n $NAMESPACE --timeout=30m"
echo ""
