#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_DIR="${SCRIPT_DIR}/templates"
GENERATED_DIR="${SCRIPT_DIR}/generated"
OPERATOR_DIR="${SCRIPT_DIR}/../../infra/feast-operator"

DEFAULT_NAMESPACE="feast"
KUBECTL_CMD="${KUBECTL_CMD:-kubectl}"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Deploy Redis, PostgreSQL, and a Feast FeatureStore instance via the Feast Operator.

Options:
  -n, --namespace NAME       Kubernetes namespace to deploy into (default: ${DEFAULT_NAMESPACE})
  -c, --create-namespace     Create the namespace if it doesn't exist
  -o, --operator-install     Also install the Feast Operator (from dist/install.yaml)
      --skip-datastores      Skip deploying Redis and PostgreSQL (if already running)
      --skip-feast           Skip deploying the FeatureStore CR (deploy only datastores)
      --wait SECONDS         Seconds to wait for datastore pods to be ready (default: 120)
  -h, --help                 Show this help message

Examples:
  # Deploy everything into a new namespace "my-feast"
  $(basename "$0") -n my-feast -c -o

  # Deploy only datastores into an existing namespace
  $(basename "$0") -n feast --skip-feast

  # Deploy only the FeatureStore CR (datastores already running)
  $(basename "$0") -n feast --skip-datastores
EOF
    exit 0
}

NAMESPACE="${DEFAULT_NAMESPACE}"
CREATE_NS=false
INSTALL_OPERATOR=false
SKIP_DATASTORES=false
SKIP_FEAST=false
WAIT_TIMEOUT=120

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace)       NAMESPACE="$2"; shift 2 ;;
        -c|--create-namespace) CREATE_NS=true; shift ;;
        -o|--operator-install) INSTALL_OPERATOR=true; shift ;;
        --skip-datastores)    SKIP_DATASTORES=true; shift ;;
        --skip-feast)         SKIP_FEAST=true; shift ;;
        --wait)               WAIT_TIMEOUT="$2"; shift 2 ;;
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
}

render_templates() {
    info "Rendering templates for namespace '${NAMESPACE}'..."
    mkdir -p "${GENERATED_DIR}"
    for tmpl in "${TEMPLATE_DIR}"/*.yaml; do
        filename="$(basename "${tmpl}")"
        sed "s/__NAMESPACE__/${NAMESPACE}/g" "${tmpl}" > "${GENERATED_DIR}/${filename}"
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

deploy_datastores() {
    if ${SKIP_DATASTORES}; then
        info "Skipping datastore deployment (--skip-datastores)"
        return 0
    fi

    info "Deploying PostgreSQL..."
    ${KUBECTL_CMD} apply -f "${GENERATED_DIR}/postgres.yaml"
    ok "PostgreSQL manifests applied"

    info "Deploying Redis..."
    ${KUBECTL_CMD} apply -f "${GENERATED_DIR}/redis.yaml"
    ok "Redis manifests applied"

    info "Waiting for PostgreSQL pod to be ready (timeout: ${WAIT_TIMEOUT}s)..."
    ${KUBECTL_CMD} wait --for=condition=ready pod \
        -l app=postgres -n "${NAMESPACE}" \
        --timeout="${WAIT_TIMEOUT}s" 2>/dev/null || \
        warn "PostgreSQL readiness check timed out"

    info "Waiting for Redis pod to be ready (timeout: ${WAIT_TIMEOUT}s)..."
    ${KUBECTL_CMD} wait --for=condition=ready pod \
        -l app=redis -n "${NAMESPACE}" \
        --timeout="${WAIT_TIMEOUT}s" 2>/dev/null || \
        warn "Redis readiness check timed out"

    ok "Datastores are ready"
}

deploy_feast() {
    if ${SKIP_FEAST}; then
        info "Skipping FeatureStore deployment (--skip-feast)"
        return 0
    fi

    info "Deploying Feast FeatureStore CR..."
    ${KUBECTL_CMD} apply -f "${GENERATED_DIR}/feast.yaml"
    ok "FeatureStore CR applied"

    info "Waiting for FeatureStore pods to start (this may take a few minutes)..."
    sleep 10

    local feast_ready=false
    for i in $(seq 1 12); do
        if ${KUBECTL_CMD} get pods -n "${NAMESPACE}" -l app.kubernetes.io/managed-by=feast-operator 2>/dev/null | grep -q "Running"; then
            feast_ready=true
            break
        fi
        info "  Checking FeatureStore pods... (attempt ${i}/12)"
        sleep 10
    done

    if ${feast_ready}; then
        ok "FeatureStore pods are running"
    else
        warn "FeatureStore pods may not be fully ready yet. Check with:"
        warn "  ${KUBECTL_CMD} get pods -n ${NAMESPACE}"
    fi
}

print_summary() {
    echo ""
    echo "=============================================="
    echo "  Feast Deployment Summary"
    echo "=============================================="
    echo "  Namespace:  ${NAMESPACE}"
    echo "  Datastores: $(${SKIP_DATASTORES} && echo 'skipped' || echo 'deployed')"
    echo "  Feast CR:   $(${SKIP_FEAST} && echo 'skipped' || echo 'deployed')"
    echo "  Operator:   $(${INSTALL_OPERATOR} && echo 'installed' || echo 'skipped')"
    echo "=============================================="
    echo ""
    echo "Useful commands:"
    echo "  ${KUBECTL_CMD} get pods -n ${NAMESPACE}"
    echo "  ${KUBECTL_CMD} get featurestore -n ${NAMESPACE}"
    echo "  ${KUBECTL_CMD} logs -n ${NAMESPACE} -l app.kubernetes.io/managed-by=feast-operator"
    echo ""
}

main() {
    info "Starting Feast automated setup..."
    echo ""

    check_prerequisites
    render_templates
    setup_namespace
    install_operator
    deploy_datastores
    deploy_feast
    print_summary

    ok "Setup complete!"
}

main
