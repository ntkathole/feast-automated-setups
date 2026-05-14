#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GENERATED_DIR="${SCRIPT_DIR}/generated"
OPERATOR_DIR="${SCRIPT_DIR}/../../infra/feast-operator"

DEFAULT_NAMESPACE="feast-rag"
KUBECTL_CMD="${KUBECTL_CMD:-kubectl}"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Remove Feast FeatureStore (RAG), Ray cluster, datastores, and optionally the operators and namespace.

Options:
  -n, --namespace NAME       Kubernetes namespace to clean up (default: ${DEFAULT_NAMESPACE})
  -o, --operator-uninstall   Also uninstall the Feast Operator
      --kuberay-uninstall    Also uninstall the KubeRay operator
      --delete-namespace     Delete the namespace after removing resources
      --skip-datastores      Keep Milvus and PostgreSQL running
      --skip-ray             Keep the RayCluster running
      --skip-feast           Keep the FeatureStore CR
  -h, --help                 Show this help message

Examples:
  # Remove everything including namespace and operators
  $(basename "$0") -n feast-rag --delete-namespace -o --kuberay-uninstall

  # Remove only the FeatureStore CR, keep infra
  $(basename "$0") -n feast-rag --skip-datastores --skip-ray
EOF
    exit 0
}

NAMESPACE="${DEFAULT_NAMESPACE}"
UNINSTALL_OPERATOR=false
UNINSTALL_KUBERAY=false
DELETE_NS=false
SKIP_DATASTORES=false
SKIP_RAY=false
SKIP_FEAST=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace)         NAMESPACE="$2"; shift 2 ;;
        -o|--operator-uninstall) UNINSTALL_OPERATOR=true; shift ;;
        --kuberay-uninstall)    UNINSTALL_KUBERAY=true; shift ;;
        --delete-namespace)     DELETE_NS=true; shift ;;
        --skip-datastores)      SKIP_DATASTORES=true; shift ;;
        --skip-ray)             SKIP_RAY=true; shift ;;
        --skip-feast)           SKIP_FEAST=true; shift ;;
        -h|--help)              usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

info()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
ok()    { echo -e "\033[1;32m[OK]\033[0m    $*"; }
warn()  { echo -e "\033[1;33m[WARN]\033[0m  $*"; }

remove_feast() {
    if ${SKIP_FEAST}; then
        info "Skipping FeatureStore removal (--skip-feast)"
        return 0
    fi

    local feast_yaml="${GENERATED_DIR}/feast.yaml"
    if [[ -f "${feast_yaml}" ]]; then
        info "Removing FeatureStore CR and secrets..."
        ${KUBECTL_CMD} delete -f "${feast_yaml}" --ignore-not-found
        ok "FeatureStore CR removed"
    else
        info "Removing FeatureStore CR by name..."
        ${KUBECTL_CMD} delete featurestore rag-demo -n "${NAMESPACE}" --ignore-not-found
        ${KUBECTL_CMD} delete secret feast-data-stores -n "${NAMESPACE}" --ignore-not-found
        ok "FeatureStore resources removed"
    fi

    info "Waiting for operator-managed pods to terminate..."
    for i in $(seq 1 6); do
        if ! ${KUBECTL_CMD} get pods -n "${NAMESPACE}" -l feast.dev/name 2>/dev/null | grep -q .; then
            break
        fi
        sleep 5
    done
}

remove_ray_cluster() {
    if ${SKIP_RAY}; then
        info "Skipping RayCluster removal (--skip-ray)"
        return 0
    fi

    local raycluster_yaml="${GENERATED_DIR}/raycluster.yaml"
    if [[ -f "${raycluster_yaml}" ]]; then
        info "Removing RayCluster..."
        ${KUBECTL_CMD} delete -f "${raycluster_yaml}" --ignore-not-found
    else
        info "Removing RayCluster by name..."
        ${KUBECTL_CMD} delete raycluster feast-ray -n "${NAMESPACE}" --ignore-not-found
    fi

    local cm_yaml="${GENERATED_DIR}/ray-batch-engine-cm.yaml"
    if [[ -f "${cm_yaml}" ]]; then
        ${KUBECTL_CMD} delete -f "${cm_yaml}" --ignore-not-found
    else
        ${KUBECTL_CMD} delete configmap feast-ray-batch-engine -n "${NAMESPACE}" --ignore-not-found
    fi

    info "Waiting for Ray pods to terminate..."
    for i in $(seq 1 6); do
        if ! ${KUBECTL_CMD} get pods -n "${NAMESPACE}" -l app=feast-ray 2>/dev/null | grep -q .; then
            break
        fi
        sleep 5
    done
    ok "Ray cluster removed"
}

remove_datastores() {
    if ${SKIP_DATASTORES}; then
        info "Skipping datastore removal (--skip-datastores)"
        return 0
    fi

    for resource in milvus postgres; do
        local yaml="${GENERATED_DIR}/${resource}.yaml"
        if [[ -f "${yaml}" ]]; then
            info "Removing ${resource}..."
            ${KUBECTL_CMD} delete -f "${yaml}" --ignore-not-found
        else
            info "Removing ${resource} by label..."
            ${KUBECTL_CMD} delete deployment "${resource}" -n "${NAMESPACE}" --ignore-not-found
            ${KUBECTL_CMD} delete service "${resource}" -n "${NAMESPACE}" --ignore-not-found
        fi
    done
    ${KUBECTL_CMD} delete secret postgres-secret -n "${NAMESPACE}" --ignore-not-found
    ok "Datastores removed"
}

uninstall_operator() {
    if ! ${UNINSTALL_OPERATOR}; then
        return 0
    fi

    local install_yaml="${OPERATOR_DIR}/dist/install.yaml"
    if [[ -f "${install_yaml}" ]]; then
        info "Uninstalling Feast Operator..."
        ${KUBECTL_CMD} delete -f "${install_yaml}" --ignore-not-found
        ok "Feast Operator uninstalled"
    else
        warn "Operator install manifest not found at ${install_yaml}; skipping operator removal."
    fi
}

uninstall_kuberay() {
    if ! ${UNINSTALL_KUBERAY}; then
        return 0
    fi

    info "Uninstalling KubeRay operator..."
    ${KUBECTL_CMD} delete deployment kuberay-operator --ignore-not-found 2>/dev/null || true
    ok "KubeRay operator uninstalled"
}

delete_namespace() {
    if ! ${DELETE_NS}; then
        return 0
    fi

    info "Deleting namespace '${NAMESPACE}'..."
    ${KUBECTL_CMD} delete namespace "${NAMESPACE}" --ignore-not-found
    ok "Namespace '${NAMESPACE}' deleted"
}

cleanup_generated() {
    if [[ -d "${GENERATED_DIR}" ]]; then
        info "Cleaning up generated manifests..."
        rm -rf "${GENERATED_DIR}"
        ok "Generated directory removed"
    fi
}

main() {
    info "Starting Feast RAG teardown..."
    echo ""

    remove_feast
    remove_ray_cluster
    remove_datastores
    uninstall_operator
    uninstall_kuberay
    delete_namespace
    cleanup_generated

    echo ""
    ok "Teardown complete!"
}

main
