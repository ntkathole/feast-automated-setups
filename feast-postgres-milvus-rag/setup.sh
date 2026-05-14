#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_DIR="${SCRIPT_DIR}/templates"
GENERATED_DIR="${SCRIPT_DIR}/generated"
OPERATOR_DIR="${SCRIPT_DIR}/../../infra/feast-operator"

DEFAULT_NAMESPACE="feast-rag"
DEFAULT_KUBERAY_VERSION="v1.3.0"
KUBECTL_CMD="${KUBECTL_CMD:-kubectl}"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Deploy a RAG pipeline using the Feast Operator with:
  - PostgreSQL  as registry (SQL) and offline store
  - Milvus      as online store (vector search)
  - Ray cluster as compute engine (distributed embedding generation via KubeRay)
  - HuggingFace SQuAD dataset read via RaySource

Options:
  -n, --namespace NAME       Kubernetes namespace to deploy into (default: ${DEFAULT_NAMESPACE})
  -c, --create-namespace     Create the namespace if it doesn't exist
  -o, --operator-install     Also install the Feast Operator (from dist/install.yaml)
      --kuberay-install      Also install the KubeRay operator
      --kuberay-version VER  KubeRay version to install (default: ${DEFAULT_KUBERAY_VERSION})
  -g, --git-url URL          Git repo URL containing the feature_repo (required)
  -r, --git-ref REF          Git branch/tag/commit to checkout (default: main)
      --skip-datastores      Skip deploying Milvus and PostgreSQL (if already running)
      --skip-ray             Skip deploying the RayCluster (if already running)
      --skip-feast           Skip deploying the FeatureStore CR (deploy only infra)
      --skip-apply           Skip running 'feast apply' after deployment
      --wait SECONDS         Seconds to wait for pod readiness (default: 180)
      --apply-timeout SECS   Seconds to wait for Feast deployment and apply Job (default: 600)
  -h, --help                 Show this help message

Examples:
  # Deploy everything into a new namespace with feature repo from Git
  $(basename "$0") -n feast-rag -c -o --kuberay-install \\
    -g https://github.com/yourorg/feast-rag-features.git -r main

  # Deploy only datastores + Ray cluster
  $(basename "$0") -n feast-rag --skip-feast

  # Deploy without running feast apply automatically
  $(basename "$0") -n feast-rag -g https://github.com/yourorg/feast-rag-features.git --skip-apply
EOF
    exit 0
}

NAMESPACE="${DEFAULT_NAMESPACE}"
CREATE_NS=false
INSTALL_OPERATOR=false
INSTALL_KUBERAY=false
KUBERAY_VERSION="${DEFAULT_KUBERAY_VERSION}"
SKIP_DATASTORES=false
SKIP_RAY=false
SKIP_FEAST=false
SKIP_APPLY=false
WAIT_TIMEOUT=180
APPLY_TIMEOUT=600
FEATURE_REPO_URL=""
FEATURE_REPO_REF="main"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace)       NAMESPACE="$2"; shift 2 ;;
        -c|--create-namespace) CREATE_NS=true; shift ;;
        -o|--operator-install) INSTALL_OPERATOR=true; shift ;;
        --kuberay-install)    INSTALL_KUBERAY=true; shift ;;
        --kuberay-version)    KUBERAY_VERSION="$2"; shift 2 ;;
        -g|--git-url)         FEATURE_REPO_URL="$2"; shift 2 ;;
        -r|--git-ref)         FEATURE_REPO_REF="$2"; shift 2 ;;
        --skip-datastores)    SKIP_DATASTORES=true; shift ;;
        --skip-ray)           SKIP_RAY=true; shift ;;
        --skip-feast)         SKIP_FEAST=true; shift ;;
        --skip-apply)         SKIP_APPLY=true; shift ;;
        --wait)               WAIT_TIMEOUT="$2"; shift 2 ;;
        --apply-timeout)      APPLY_TIMEOUT="$2"; shift 2 ;;
        -h|--help)            usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

info()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
ok()    { echo -e "\033[1;32m[OK]\033[0m    $*"; }
warn()  { echo -e "\033[1;33m[WARN]\033[0m  $*"; }
error() { echo -e "\033[1;31m[ERROR]\033[0m $*"; exit 1; }

check_prerequisites() {
    if ! command -v "${KUBECTL_CMD}" &>/dev/null; then
        error "${KUBECTL_CMD} is not installed or not in PATH"
    fi

    if ! ${KUBECTL_CMD} cluster-info &>/dev/null; then
        error "Cannot connect to Kubernetes cluster. Check your kubeconfig."
    fi
    ok "Connected to Kubernetes cluster"

    if ! ${SKIP_FEAST} && [[ -z "${FEATURE_REPO_URL}" ]]; then
        error "Git repo URL is required. Use -g/--git-url to specify the feature repo URL."
    fi
}

render_templates() {
    info "Rendering templates for namespace '${NAMESPACE}'..."
    mkdir -p "${GENERATED_DIR}"
    for tmpl in "${TEMPLATE_DIR}"/*.yaml; do
        filename="$(basename "${tmpl}")"
        sed -e "s/__NAMESPACE__/${NAMESPACE}/g" \
            -e "s|__FEATURE_REPO_URL__|${FEATURE_REPO_URL}|g" \
            -e "s/__FEATURE_REPO_REF__/${FEATURE_REPO_REF}/g" \
            "${tmpl}" > "${GENERATED_DIR}/${filename}"
    done
    ok "Generated manifests written to ${GENERATED_DIR}/"
}

setup_namespace() {
    if ${KUBECTL_CMD} get namespace "${NAMESPACE}" &>/dev/null; then
        ok "Namespace '${NAMESPACE}' already exists"
    elif ${CREATE_NS}; then
        info "Creating namespace '${NAMESPACE}'..."
        ${KUBECTL_CMD} create namespace "${NAMESPACE}"
        ok "Namespace '${NAMESPACE}' created"
    else
        error "Namespace '${NAMESPACE}' does not exist. Use -c/--create-namespace to create it."
    fi
}

install_operator() {
    if ! ${INSTALL_OPERATOR}; then
        return 0
    fi

    local install_yaml="${OPERATOR_DIR}/dist/install.yaml"
    if [[ ! -f "${install_yaml}" ]]; then
        warn "Operator install manifest not found at ${install_yaml}"
        info "Attempting to build it with 'make build-installer'..."
        if [[ -f "${OPERATOR_DIR}/Makefile" ]]; then
            make -C "${OPERATOR_DIR}" build-installer
        else
            error "Cannot find operator Makefile at ${OPERATOR_DIR}/Makefile"
        fi
    fi

    info "Installing Feast Operator..."
    ${KUBECTL_CMD} apply -f "${install_yaml}"
    ok "Feast Operator installed"

    info "Waiting for operator deployment to be ready..."
    ${KUBECTL_CMD} wait --for=condition=available deployment \
        -l control-plane=controller-manager \
        -n feast-operator-system \
        --timeout="${WAIT_TIMEOUT}s" 2>/dev/null || \
        warn "Operator readiness check timed out; it may still be starting up."
}

install_kuberay() {
    if ! ${INSTALL_KUBERAY}; then
        return 0
    fi

    info "Installing KubeRay operator (${KUBERAY_VERSION})..."
    ${KUBECTL_CMD} create -k \
        "github.com/opendatahub-io/kuberay/ray-operator/config/default?ref=${KUBERAY_VERSION}" \
        2>/dev/null || \
        info "KubeRay operator resources may already exist, applying updates..."

    info "Waiting for KubeRay operator to be ready..."
    ${KUBECTL_CMD} wait --timeout="${WAIT_TIMEOUT}s" \
        --for=condition=Available=true deployment kuberay-operator \
        2>/dev/null || \
        warn "KubeRay operator readiness check timed out; it may still be starting up."
    ok "KubeRay operator installed"
}

deploy_datastores() {
    if ${SKIP_DATASTORES}; then
        info "Skipping datastore deployment (--skip-datastores)"
        return 0
    fi

    info "Deploying PostgreSQL..."
    ${KUBECTL_CMD} apply -f "${GENERATED_DIR}/postgres.yaml"
    ok "PostgreSQL manifests applied"

    info "Deploying Milvus..."
    ${KUBECTL_CMD} apply -f "${GENERATED_DIR}/milvus.yaml"
    ok "Milvus manifests applied"

    info "Waiting for PostgreSQL pod to be ready (timeout: ${WAIT_TIMEOUT}s)..."
    ${KUBECTL_CMD} wait --for=condition=ready pod \
        -l app=postgres -n "${NAMESPACE}" \
        --timeout="${WAIT_TIMEOUT}s" 2>/dev/null || \
        warn "PostgreSQL readiness check timed out"

    info "Waiting for Milvus pod to be ready (timeout: ${WAIT_TIMEOUT}s)..."
    ${KUBECTL_CMD} wait --for=condition=ready pod \
        -l app=milvus -n "${NAMESPACE}" \
        --timeout="${WAIT_TIMEOUT}s" 2>/dev/null || \
        warn "Milvus readiness check timed out"

    ok "Datastores are ready"
}

deploy_ray_cluster() {
    if ${SKIP_RAY}; then
        info "Skipping RayCluster deployment (--skip-ray)"
        return 0
    fi

    if ! ${KUBECTL_CMD} api-resources --api-group=ray.io 2>/dev/null | grep -q RayCluster; then
        warn "RayCluster CRD not found. Install KubeRay operator first (--kuberay-install)."
        warn "Skipping Ray cluster deployment."
        return 0
    fi

    info "Deploying RayCluster..."
    ${KUBECTL_CMD} apply -f "${GENERATED_DIR}/raycluster.yaml"
    ok "RayCluster CR applied"

    info "Deploying Ray batch engine ConfigMap..."
    ${KUBECTL_CMD} apply -f "${GENERATED_DIR}/ray-batch-engine-cm.yaml"
    ok "Ray batch engine ConfigMap applied"

    info "Waiting for Ray head pod to be ready (timeout: ${WAIT_TIMEOUT}s)..."
    ${KUBECTL_CMD} wait --for=condition=ready pod \
        -l ray.io/group=headgroup -n "${NAMESPACE}" \
        --timeout="${WAIT_TIMEOUT}s" 2>/dev/null || \
        warn "Ray head readiness check timed out"

    info "Waiting for Ray worker pods to be ready (timeout: ${WAIT_TIMEOUT}s)..."
    ${KUBECTL_CMD} wait --for=condition=ready pod \
        -l ray.io/group=workergroup -n "${NAMESPACE}" \
        --timeout="${WAIT_TIMEOUT}s" 2>/dev/null || \
        warn "Ray worker readiness check timed out"

    ok "Ray cluster is ready"
}

get_featurestore_name() {
    grep -A3 'kind: FeatureStore' "${GENERATED_DIR}/feast.yaml" | grep 'name:' | head -1 | awk '{print $2}'
}

deploy_feast() {
    if ${SKIP_FEAST}; then
        info "Skipping FeatureStore deployment (--skip-feast)"
        return 0
    fi

    local fs_name
    fs_name=$(get_featurestore_name)

    info "Deploying Feast FeatureStore CR '${fs_name}' (RAG with Milvus + PostgreSQL + Ray)..."
    ${KUBECTL_CMD} apply -f "${GENERATED_DIR}/feast.yaml"
    ok "FeatureStore CR applied"

    info "Waiting for FeatureStore CR '${fs_name}' to be ready (this may take a few minutes)..."
    local cr_ready=false
    for i in $(seq 1 30); do
        local phase
        phase=$(${KUBECTL_CMD} get featurestore "${fs_name}" -n "${NAMESPACE}" \
            -o jsonpath='{.status.phase}' 2>/dev/null) || true

        if [[ "${phase}" == "Ready" ]]; then
            cr_ready=true
            break
        elif [[ "${phase}" == "Failed" ]]; then
            warn "FeatureStore CR is in Failed state. Check:"
            warn "  ${KUBECTL_CMD} describe featurestore ${fs_name} -n ${NAMESPACE}"
            return 1
        fi
        info "  FeatureStore phase: ${phase:-Pending} (attempt ${i}/30)"
        sleep 10
    done

    if ${cr_ready}; then
        ok "FeatureStore CR '${fs_name}' is Ready"
    else
        warn "FeatureStore CR did not reach Ready state within timeout. Check with:"
        warn "  ${KUBECTL_CMD} get featurestore ${fs_name} -n ${NAMESPACE}"
        warn "  ${KUBECTL_CMD} describe featurestore ${fs_name} -n ${NAMESPACE}"
    fi
}

run_feast_apply() {
    if ${SKIP_FEAST} || ${SKIP_APPLY}; then
        info "Skipping feast apply (--skip-feast or --skip-apply)"
        return 0
    fi

    local fs_name
    fs_name=$(get_featurestore_name)
    local deploy_name="feast-${fs_name}"
    local feast_label="feast.dev/name=${fs_name}"

    info "Looking for the feast-apply CronJob..."
    local cronjob_name=""
    cronjob_name=$(${KUBECTL_CMD} get cronjobs -n "${NAMESPACE}" \
        -l "${feast_label}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true

    if [[ -z "${cronjob_name}" ]]; then
        warn "No feast-apply CronJob found. You may need to trigger 'feast apply' manually."
        warn "  ${KUBECTL_CMD} exec deploy/${deploy_name} -n ${NAMESPACE} -- bash -c 'feast apply'"
        return 0
    fi

    ok "Found CronJob: ${cronjob_name}"

    local job_name="feast-apply-$(date +%s)"
    info "Creating one-off Job '${job_name}' from CronJob '${cronjob_name}'..."
    ${KUBECTL_CMD} create job "${job_name}" \
        --from="cronjob/${cronjob_name}" \
        -n "${NAMESPACE}"

    info "Waiting for feast-apply Job to complete (timeout: ${APPLY_TIMEOUT}s)..."
    if ${KUBECTL_CMD} wait --for=condition=complete "job/${job_name}" \
        -n "${NAMESPACE}" \
        --timeout="${APPLY_TIMEOUT}s" 2>/dev/null; then
        ok "feast apply completed successfully"
    else
        warn "feast-apply Job did not complete within ${APPLY_TIMEOUT}s"
        warn "Check Job status:"
        warn "  ${KUBECTL_CMD} get job ${job_name} -n ${NAMESPACE}"
        warn "  ${KUBECTL_CMD} logs job/${job_name} -n ${NAMESPACE}"
    fi
}

print_summary() {
    echo ""
    echo "=============================================="
    echo "  Feast RAG Deployment Summary"
    echo "=============================================="
    echo "  Namespace:      ${NAMESPACE}"
    echo "  Datastores:     $(${SKIP_DATASTORES} && echo 'skipped' || echo 'deployed (PostgreSQL + Milvus)')"
    echo "  Ray Cluster:    $(${SKIP_RAY} && echo 'skipped' || echo 'deployed (KubeRay)')"
    echo "  Feast CR:       $(${SKIP_FEAST} && echo 'skipped' || echo 'deployed')"
    echo "  Feast Operator: $(${INSTALL_OPERATOR} && echo 'installed' || echo 'skipped')"
    echo "  KubeRay:        $(${INSTALL_KUBERAY} && echo 'installed' || echo 'skipped')"
    echo "  Feast Apply:    $(${SKIP_APPLY} || ${SKIP_FEAST} && echo 'skipped' || echo 'triggered')"
    echo "  Online Store:   Milvus (vector search)"
    echo "  Offline Store:  PostgreSQL"
    echo "  Registry:       PostgreSQL (SQL)"
    echo "  Batch Engine:   Ray (via KubeRay cluster)"
    echo "  Feature Repo:   ${FEATURE_REPO_URL:-N/A} (ref: ${FEATURE_REPO_REF})"
    echo "=============================================="
    echo ""
    echo "Useful commands:"
    echo "  ${KUBECTL_CMD} get pods -n ${NAMESPACE}"
    echo "  ${KUBECTL_CMD} get featurestore -n ${NAMESPACE}"
    echo "  ${KUBECTL_CMD} get raycluster -n ${NAMESPACE}"
    echo "  ${KUBECTL_CMD} logs -n ${NAMESPACE} -l feast.dev/name"
    echo ""
    echo "Next steps:"
    echo "  1. Materialize embeddings (Ray will generate embeddings distributedly):"
    echo "     ${KUBECTL_CMD} exec deploy/feast-rag-demo -n ${NAMESPACE} -- \\"
    echo "       feast materialize 2020-01-01T00:00:00 2026-12-31T23:59:59"
    echo "  2. Run the test workflow:"
    echo "     ${KUBECTL_CMD} exec deploy/feast-rag-demo -n ${NAMESPACE} -- \\"
    echo "       python test_workflow.py"
    echo ""
}

main() {
    info "Starting Feast RAG automated setup..."
    echo ""

    check_prerequisites
    render_templates
    setup_namespace
    install_operator
    install_kuberay
    deploy_datastores
    deploy_ray_cluster
    deploy_feast
    run_feast_apply
    print_summary

    ok "Setup complete!"
}

main
