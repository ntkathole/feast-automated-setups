#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GENERATED_DIR="${SCRIPT_DIR}/generated"
OPERATOR_DIR="${SCRIPT_DIR}/../../infra/feast-operator"

DEFAULT_NAMESPACE="feast"
KUBECTL_CMD="${KUBECTL_CMD:-kubectl}"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Remove Feast FeatureStore, datastores, and optionally the operator and namespace.

Options:
  -n, --namespace NAME       Kubernetes namespace to clean up (default: ${DEFAULT_NAMESPACE})
  -o, --operator-uninstall   Also uninstall the Feast Operator
      --delete-namespace     Delete the namespace after removing resources
      --skip-datastores      Keep Redis and PostgreSQL running
      --skip-feast           Keep the FeatureStore CR
  -h, --help                 Show this help message

Examples:
  # Remove everything including namespace
  $(basename "$0") -n my-feast --delete-namespace -o

  # Remove only the FeatureStore CR
  $(basename "$0") -n feast --skip-datastores
EOF
    exit 0
}

NAMESPACE="${DEFAULT_NAMESPACE}"
UNINSTALL_OPERATOR=false
DELETE_NS=false
SKIP_DATASTORES=false
SKIP_FEAST=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace)         NAMESPACE="$2"; shift 2 ;;
        -o|--operator-uninstall) UNINSTALL_OPERATOR=true; shift ;;
        --delete-namespace)     DELETE_NS=true; shift ;;
        --skip-datastores)      SKIP_DATASTORES=true; shift ;;
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
        ${KUBECTL_CMD} delete featurestore example -n "${NAMESPACE}" --ignore-not-found
        ${KUBECTL_CMD} delete secret feast-data-stores -n "${NAMESPACE}" --ignore-not-found
        ok "FeatureStore resources removed"
    fi

    info "Waiting for operator-managed pods to terminate..."
    for i in $(seq 1 6); do
        if ! ${KUBECTL_CMD} get pods -n "${NAMESPACE}" -l app.kubernetes.io/managed-by=feast-operator 2>/dev/null | grep -q .; then
            break
        fi
        sleep 5
    done
}

remove_datastores() {
    if ${SKIP_DATASTORES}; then
        info "Skipping datastore removal (--skip-datastores)"
        return 0
    fi

    for resource in redis postgres; do
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
    info "Starting Feast teardown..."
    echo ""

    remove_feast
    remove_datastores
    uninstall_operator
    delete_namespace
    cleanup_generated

    echo ""
    ok "Teardown complete!"
}

main
