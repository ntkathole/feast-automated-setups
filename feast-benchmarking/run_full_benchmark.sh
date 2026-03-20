#!/bin/bash
#===============================================================================
# Feast Online Store Benchmark - Full Cycle Automation
#===============================================================================
#
# DESCRIPTION:
#   Single source of truth for running end-to-end performance evaluation
#   across all online stores (SQLite, Redis, PostgreSQL, DynamoDB).
#   Supports comparing multiple Feast versions/branches.
#   Benchmarks run SEQUENTIALLY by default to avoid resource contention.
#   Use --parallel for faster (but less reliable) parallel execution.
#
# USAGE:
#   ./run_full_benchmark.sh [OPTIONS]
#
# OPTIONS:
#   --config <file>       Config file path (default: benchmark.config.yaml)
#   --compare             Compare all refs defined in config (parallel builds)
#   --refs <list>         Specific refs to run (comma-separated, e.g. "baseline,optimized")
#   --feast-git-ref <ref> Feast git reference (branch/tag/commit) - triggers rebuild
#   --feast-git-url <url> Feast git repository URL
#   --base-image <image>  Pre-built base image for fast builds (optional)
#   --stores <list>       Stores to benchmark (default: from config)
#   --namespace <ns>      Kubernetes namespace (default: from config)
#   --features <n>        Number of features (default: from config)
#   --entities <list>     Entity counts to test (default: from config)
#   --iterations <n>      Iterations per test (default: from config)
#   --warmup <n>          Warmup iterations (default: from config)
#   --timeout <s>         Job timeout in seconds (default: from config)
#   --output-dir <dir>    Results output directory (default: from config)
#   --skip-build          Skip image build even if git ref specified
#   --skip-k8s            Skip K8s jobs, run locally only (SQLite)
#   --skip-charts         Skip chart generation
#   --stage <stage>       Run specific stage only: build, benchmark, charts, or all (default: all)
#   --scenario <name>     Benchmark scenario: entity_scaling, feature_scaling, or all (default: entity_scaling)
#   --cleanup             Clean up all resources in the namespace (jobs, deployments, PVC, secrets, etc.)
#   --parallel            Run benchmarks in parallel (default: sequential for reliable results)
#   --aws-key <key>       AWS Access Key ID (for DynamoDB)
#   --aws-secret <secret> AWS Secret Access Key (for DynamoDB)
#   --aws-region <region> AWS Region (default: eu-west-1)
#   --dry-run             Show what would be done without executing
#   --verbose             Enable verbose output
#   --help                Show this help message
#
# SCENARIOS:
#   entity_scaling   - Fixed 200 features, vary entities (1, 10, 50, 100, 200)
#   feature_scaling  - Fixed 50 entities, vary features (5, 25, 50, 100, 150, 200)
#   all              - Run both scenarios
#
# STAGES:
#   build      - Build Docker images for each feast reference (parallel)
#   benchmark  - Run K8s benchmark jobs (sequential by default, --parallel for faster)
#   charts     - Generate charts from existing results
#   all        - Run all stages (default)
#
# CONFIG FILE:
#   The config file (benchmark.config.yaml) contains all default settings
#   including feast references, database connections, and benchmark parameters.
#   Command-line arguments override config file settings.
#
# PREREQUISITES:
#   - oc/kubectl CLI configured with cluster access
#   - Python 3.11+ with venv
#   - Namespace with Redis, Postgres pods running
#   - AWS credentials secret (for DynamoDB)
#
# EXAMPLES:
#   # Run default ref from config
#   ./run_full_benchmark.sh
#
#   # Compare all refs defined in config (builds images in parallel)
#   ./run_full_benchmark.sh --compare
#
#   # Compare specific refs only
#   ./run_full_benchmark.sh --refs "baseline,optimized"
#
#   # Run with custom feast branch (triggers rebuild)
#   ./run_full_benchmark.sh --feast-git-ref perf/my-optimization
#
#   # Run only Redis and Postgres
#   ./run_full_benchmark.sh --stores "redis postgres"
#
#   # Dry run to see commands
#   ./run_full_benchmark.sh --dry-run --verbose
#
#   # Run only build stage (create images)
#   ./run_full_benchmark.sh --compare --stage build
#
#   # Run only benchmark stage (sequential - reliable results)
#   ./run_full_benchmark.sh --compare --stage benchmark
#
#   # Run benchmarks in parallel (faster but may have resource contention)
#   ./run_full_benchmark.sh --compare --stage benchmark --parallel
#
#   # Generate charts from existing results (no K8s needed)
#   ./run_full_benchmark.sh --compare --stage charts
#
# OUTPUT STRUCTURE:
#   results/
#   ├── {ref_name}/           # Per-reference results
#   │   ├── sqlite/
#   │   ├── redis/
#   │   ├── postgres/
#   │   ├── dynamodb/
#   │   └── charts/
#   └── comparison/           # Cross-ref comparison (--compare mode)
#       └── charts/
#
#===============================================================================

set -euo pipefail

#-------------------------------------------------------------------------------
# Configuration Defaults (overridden by config file, then CLI args)
#-------------------------------------------------------------------------------
CONFIG_FILE=""
COMPARE_MODE=false
REFS_TO_RUN=""
FEAST_GIT_REF=""
FEAST_GIT_URL=""
STORES=""
NAMESPACE=""
FEATURES=""
ENTITIES=""
ITERATIONS=""
WARMUP=""
TIMEOUT=""
OUTPUT_DIR=""
SKIP_BUILD=false
SKIP_K8S=false
SKIP_CHARTS=false
DRY_RUN=false
VERBOSE=false
STAGE="all"  # all, build, benchmark, charts, cleanup
SCENARIO=""  # entity_scaling, feature_scaling, all (empty = use default from config)
CHARTS_OUTPUT=""  # Set during execution
BASE_IMAGE=""  # Optional: pre-built base image for fast builds (e.g., quay.io/user/feast-benchmark-base:latest)
CLEANUP=false  # Cleanup all resources
PARALLEL_MODE=false  # Run refs in parallel (default: sequential for reliable results)
AWS_ACCESS_KEY_ID=""  # AWS credentials for DynamoDB
AWS_SECRET_ACCESS_KEY=""
AWS_REGION="eu-west-1"

# Multi-ref support
CURRENT_REF=""
CURRENT_REF_OUTPUT=""

# Config JSON (initialized empty, populated by load_config)
CONFIG_JSON="{}"

# Script directory (for relative paths)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOBS_DIR="${SCRIPT_DIR}/k8s/jobs"
BUILD_DIR="${SCRIPT_DIR}/k8s/build"

# Kubernetes CLI (oc or kubectl)
K8S_CLI="oc"
if ! command -v oc &>/dev/null; then
    K8S_CLI="kubectl"
fi

#-------------------------------------------------------------------------------
# Colors and Logging
#-------------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log_info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
log_success() { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*"; }
log_verbose() { [[ "$VERBOSE" == "true" ]] && echo -e "${CYAN}[DEBUG]${NC} $*" || true; }

log_header() {
    echo ""
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  $*${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
}

log_section() {
    echo ""
    echo -e "${BLUE}───────────────────────────────────────────────────────────────${NC}"
    echo -e "${BLUE}  $*${NC}"
    echo -e "${BLUE}───────────────────────────────────────────────────────────────${NC}"
}

#-------------------------------------------------------------------------------
# Help
#-------------------------------------------------------------------------------
show_help() {
    head -60 "$0" | grep -E "^#" | sed 's/^#//' | sed 's/^!/#!/'
    exit 0
}

#-------------------------------------------------------------------------------
# Argument Parsing
#-------------------------------------------------------------------------------
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --config)        CONFIG_FILE="$2"; shift 2 ;;
            --compare)       COMPARE_MODE=true; shift ;;
            --refs)          REFS_TO_RUN="$2"; shift 2 ;;
            --feast-git-ref) FEAST_GIT_REF="$2"; shift 2 ;;
            --feast-git-url) FEAST_GIT_URL="$2"; shift 2 ;;
            --base-image)    BASE_IMAGE="$2"; shift 2 ;;
            --stores)        STORES="$2"; shift 2 ;;
            --namespace)     NAMESPACE="$2"; shift 2 ;;
            --features)      FEATURES="$2"; shift 2 ;;
            --entities)      ENTITIES="$2"; shift 2 ;;
            --iterations)    ITERATIONS="$2"; shift 2 ;;
            --warmup)        WARMUP="$2"; shift 2 ;;
            --timeout)       TIMEOUT="$2"; shift 2 ;;
            --output-dir)    OUTPUT_DIR="$2"; shift 2 ;;
            --skip-build)    SKIP_BUILD=true; shift ;;
            --skip-k8s)      SKIP_K8S=true; shift ;;
            --skip-charts)   SKIP_CHARTS=true; shift ;;
            --stage)         STAGE="$2"; shift 2 ;;
            --scenario)      SCENARIO="$2"; shift 2 ;;
            --dry-run)       DRY_RUN=true; shift ;;
            --verbose)       VERBOSE=true; shift ;;
            --cleanup)       CLEANUP=true; shift ;;
            --parallel)      PARALLEL_MODE=true; shift ;;
            --aws-key)       AWS_ACCESS_KEY_ID="$2"; shift 2 ;;
            --aws-secret)    AWS_SECRET_ACCESS_KEY="$2"; shift 2 ;;
            --aws-region)    AWS_REGION="$2"; shift 2 ;;
            --help|-h)       show_help ;;
            *)               log_error "Unknown option: $1"; exit 1 ;;
        esac
    done
}

#-------------------------------------------------------------------------------
# Config File Loading
#-------------------------------------------------------------------------------
load_config() {
    local config_path="$1"
    
    if [[ ! -f "$config_path" ]]; then
        log_warn "Config file not found: $config_path (using defaults)"
        CONFIG_JSON="{}"
        return 0
    fi
    
    log_info "Loading config from: $config_path"
    
    # Use venv Python if available (has PyYAML), otherwise system python3
    local PYTHON_CMD="python3"
    if [[ -f "${SCRIPT_DIR}/.venv/bin/python" ]]; then
        PYTHON_CMD="${SCRIPT_DIR}/.venv/bin/python"
    fi
    
    # Parse YAML config using Python (handles complex YAML safely)
    CONFIG_JSON=$($PYTHON_CMD << PYTHON_EOF
import yaml
import json
import sys

try:
    with open('$config_path', 'r') as f:
        config = yaml.safe_load(f)
    print(json.dumps(config))
except Exception as e:
    print(json.dumps({"error": str(e)}), file=sys.stderr)
    sys.exit(1)
PYTHON_EOF
)

    # Extract common settings
    if [[ -z "$NAMESPACE" ]]; then
        NAMESPACE=$(echo "$CONFIG_JSON" | python3 -c "import sys,json; c=json.load(sys.stdin); print(c.get('kubernetes',{}).get('namespace','feast-benchmark'))" 2>/dev/null || echo "feast-benchmark")
    fi
    if [[ -z "$FEATURES" ]]; then
        FEATURES=$(echo "$CONFIG_JSON" | python3 -c "import sys,json; c=json.load(sys.stdin); print(c.get('benchmark',{}).get('features',200))" 2>/dev/null || echo "200")
    fi
    if [[ -z "$ENTITIES" ]]; then
        ENTITIES=$(echo "$CONFIG_JSON" | python3 -c "import sys,json; c=json.load(sys.stdin); print(' '.join(map(str,c.get('benchmark',{}).get('entities',[1,10,50,100,200]))))" 2>/dev/null || echo "1 10 50 100 200")
    fi
    if [[ -z "$ITERATIONS" ]]; then
        ITERATIONS=$(echo "$CONFIG_JSON" | python3 -c "import sys,json; c=json.load(sys.stdin); print(c.get('benchmark',{}).get('iterations',300))" 2>/dev/null || echo "300")
    fi
    if [[ -z "$WARMUP" ]]; then
        WARMUP=$(echo "$CONFIG_JSON" | python3 -c "import sys,json; c=json.load(sys.stdin); print(c.get('benchmark',{}).get('warmup',20))" 2>/dev/null || echo "20")
    fi
    if [[ -z "$TIMEOUT" ]]; then
        TIMEOUT=$(echo "$CONFIG_JSON" | python3 -c "import sys,json; c=json.load(sys.stdin); print(c.get('kubernetes',{}).get('job_timeout',1800))" 2>/dev/null || echo "1800")
    fi
    if [[ -z "$OUTPUT_DIR" ]]; then
        OUTPUT_DIR=$(echo "$CONFIG_JSON" | python3 -c "import sys,json; c=json.load(sys.stdin); print(c.get('output',{}).get('results_dir','results'))" 2>/dev/null || echo "results")
    fi
    if [[ -z "$STORES" ]]; then
        STORES=$(echo "$CONFIG_JSON" | python3 -c "
import sys, json
c = json.load(sys.stdin)
stores = c.get('stores', {})
enabled = [s for s in ['sqlite','redis','postgres','dynamodb'] if stores.get(s,{}).get('enabled',True)]
print(' '.join(enabled))
" 2>/dev/null || echo "sqlite redis postgres dynamodb")
    fi
    
    log_verbose "Config loaded: NAMESPACE=$NAMESPACE, STORES=$STORES"
}

# Get list of all references from config
get_all_refs() {
    echo "$CONFIG_JSON" | python3 -c "
import sys, json
c = json.load(sys.stdin)
refs = c.get('references', {})
print(' '.join(refs.keys()))
" 2>/dev/null || echo ""
}

# Get default reference name
get_default_ref() {
    echo "$CONFIG_JSON" | python3 -c "
import sys, json
c = json.load(sys.stdin)
print(c.get('default_ref', 'default'))
" 2>/dev/null || echo "default"
}

# Get reference details (source, git_url, git_ref, version)
get_ref_config() {
    local ref_name="$1"
    local key="$2"
    
    echo "$CONFIG_JSON" | python3 -c "
import sys, json
c = json.load(sys.stdin)
ref = c.get('references', {}).get('$ref_name', {})
print(ref.get('$key', ''))
" 2>/dev/null || echo ""
}

# Get DAX endpoint from config
get_dax_endpoint() {
    if [[ -n "${CONFIG_JSON:-}" ]]; then
        echo "$CONFIG_JSON" | python3 -c "
import sys, json
c = json.load(sys.stdin)
dax = c.get('stores',{}).get('dynamodb',{}).get('dax',{})
if dax.get('enabled', False):
    print(dax.get('endpoint', ''))
else:
    print('')
" 2>/dev/null || echo ""
    else
        echo ""
    fi
}

# Get store-specific config value
get_store_config() {
    local store="$1"
    local key="$2"
    local default="$3"
    
    if [[ -n "${CONFIG_JSON:-}" ]]; then
        echo "$CONFIG_JSON" | python3 -c "
import sys, json
c = json.load(sys.stdin)
val = c.get('stores',{}).get('$store',{}).get('$key')
print(val if val is not None else '$default')
" 2>/dev/null || echo "$default"
    else
        echo "$default"
    fi
}

# Get default scenario from config
get_default_scenario() {
    echo "$CONFIG_JSON" | python3 -c "
import sys, json
c = json.load(sys.stdin)
print(c.get('default_scenario', 'entity_scaling'))
" 2>/dev/null || echo "entity_scaling"
}

# Get scenario config value
get_scenario_config() {
    local scenario="$1"
    local key="$2"
    
    echo "$CONFIG_JSON" | python3 -c "
import sys, json
c = json.load(sys.stdin)
scenario = c.get('scenarios', {}).get('$scenario', {})
val = scenario.get('$key')
if isinstance(val, list):
    print(' '.join(map(str, val)))
else:
    print(val if val is not None else '')
" 2>/dev/null || echo ""
}

# Get list of all scenarios
get_all_scenarios() {
    echo "$CONFIG_JSON" | python3 -c "
import sys, json
c = json.load(sys.stdin)
scenarios = c.get('scenarios', {})
print(' '.join(scenarios.keys()))
" 2>/dev/null || echo "entity_scaling"
}

#-------------------------------------------------------------------------------
# Utility Functions
#-------------------------------------------------------------------------------
run_cmd() {
    local cmd="$*"
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${YELLOW}[DRY-RUN]${NC} $cmd"
        return 0
    fi
    log_verbose "Executing: $cmd"
    eval "$cmd"
}

check_prerequisites() {
    log_section "Checking Prerequisites"
    
    # Check K8s CLI
    if ! command -v "$K8S_CLI" &>/dev/null; then
        log_error "Neither 'oc' nor 'kubectl' found. Please install one."
        exit 1
    fi
    log_success "K8s CLI: $K8S_CLI"
    
    # Check Python
    if ! command -v python3 &>/dev/null; then
        log_error "python3 not found"
        exit 1
    fi
    log_success "Python: $(python3 --version)"
    
    # Check cluster connection
    if ! $K8S_CLI cluster-info &>/dev/null; then
        log_error "Not connected to Kubernetes cluster"
        exit 1
    fi
    log_success "Cluster: connected"
    
    # Check namespace - create infrastructure if missing
    if ! $K8S_CLI get namespace "$NAMESPACE" &>/dev/null; then
        log_info "Namespace '$NAMESPACE' not found, deploying infrastructure..."
        deploy_infrastructure
    else
        log_success "Namespace: $NAMESPACE"
        
        # Check if PVC exists, deploy if missing
        if ! $K8S_CLI get pvc benchmark-results -n "$NAMESPACE" &>/dev/null; then
            log_info "PVC not found, deploying infrastructure..."
            deploy_infrastructure
        fi
    fi
    
    # Check required pods
    for pod_prefix in redis postgres; do
        if [[ "$STORES" == *"$pod_prefix"* ]] || [[ "$pod_prefix" == "redis" && "$STORES" == *"dynamodb"* ]]; then
            if ! $K8S_CLI get pods -n "$NAMESPACE" -l app="$pod_prefix" --no-headers 2>/dev/null | grep -q Running; then
                log_warn "$pod_prefix pod not running (may be optional)"
            else
                log_success "$pod_prefix pod: running"
            fi
        fi
    done
    
    # Check AWS credentials for DynamoDB
    if [[ "$STORES" == *"dynamodb"* ]]; then
        # If credentials provided via CLI, create the secret
        if [[ -n "$AWS_ACCESS_KEY_ID" ]] && [[ -n "$AWS_SECRET_ACCESS_KEY" ]]; then
            log_info "Creating AWS credentials secret from CLI arguments..."
            run_cmd "$K8S_CLI create secret generic aws-credentials -n $NAMESPACE \
                --from-literal=AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
                --from-literal=AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
                --from-literal=AWS_DEFAULT_REGION=$AWS_REGION \
                --dry-run=client -o yaml | $K8S_CLI apply -f -"
            log_success "AWS credentials: created"
        elif ! $K8S_CLI get secret aws-credentials -n "$NAMESPACE" &>/dev/null; then
            log_error "AWS credentials secret not found (required for DynamoDB)"
            log_error "Provide via CLI: --aws-key <key> --aws-secret <secret>"
            log_error "Or create manually: oc create secret generic aws-credentials -n $NAMESPACE \\"
            log_error "    --from-literal=AWS_ACCESS_KEY_ID=<key> \\"
            log_error "    --from-literal=AWS_SECRET_ACCESS_KEY=<secret> \\"
            log_error "    --from-literal=AWS_DEFAULT_REGION=eu-west-1"
            exit 1
        else
            log_success "AWS credentials: found"
        fi
    fi
}

deploy_infrastructure() {
    log_section "Deploying Infrastructure"
    
    local k8s_dir="${SCRIPT_DIR}/k8s"
    
    # Generate benchmark-config ConfigMap from YAML config
    sync_config_to_k8s
    
    # Check if kustomize is available
    if command -v kustomize &>/dev/null; then
        log_info "Deploying with kustomize..."
        run_cmd "kustomize build $k8s_dir | $K8S_CLI apply -f -"
    else
        log_info "Deploying with $K8S_CLI apply -k..."
        run_cmd "$K8S_CLI apply -k $k8s_dir"
    fi
    
    # Wait for deployments to be ready
    log_info "Waiting for Redis deployment..."
    run_cmd "$K8S_CLI rollout status deployment/redis -n $NAMESPACE --timeout=120s" || log_warn "Redis deployment not ready"
    
    log_info "Waiting for PostgreSQL deployment..."
    run_cmd "$K8S_CLI rollout status deployment/postgres -n $NAMESPACE --timeout=120s" || log_warn "PostgreSQL deployment not ready"
    
    log_success "Infrastructure deployed"
}

sync_config_to_k8s() {
    log_info "Syncing config from benchmark.config.yaml to K8s ConfigMap..."
    
    local env_file="${SCRIPT_DIR}/k8s/base/benchmark.env"
    
    # Generate benchmark.env from YAML config
    cat > "$env_file" << EOF
# ===========================================
# AUTO-GENERATED from benchmark.config.yaml
# Do not edit directly - edit benchmark.config.yaml instead
# ===========================================

# Benchmark Scenarios
SCENARIOS=entity_scaling,feature_scaling

# Entity Scaling: Fixed features, vary entities
FIXED_FEATURES=${FEATURES:-200}
ENTITY_COUNTS=${ENTITIES// /,}

# Feature Scaling: Fixed entities, vary features  
FIXED_ENTITIES=50
FEATURE_COUNTS=5,25,50,100,150,200

# Legacy vars (kept for backward compatibility)
FEATURES=${FEATURES:-200}
ENTITIES=${ENTITIES// /,}

# Iterations and warmup
ITERATIONS=${ITERATIONS:-300}
WARMUP=${WARMUP:-20}

# SLA targets
SLA_P99_MS=60
SLA_THROUGHPUT_RPH=3000000

# Store endpoints (internal cluster DNS)
REDIS_HOST=redis.${NAMESPACE}.svc.cluster.local
REDIS_PORT=6379
POSTGRES_HOST=postgres.${NAMESPACE}.svc.cluster.local
POSTGRES_PORT=5432
POSTGRES_DATABASE=feast
EOF

    log_verbose "Generated $env_file"
}

cleanup_all_resources() {
    log_header "Cleanup: Removing All Resources"
    
    log_section "Deleting Benchmark Jobs"
    run_cmd "$K8S_CLI delete jobs --all -n $NAMESPACE 2>/dev/null || true"
    
    log_section "Deleting Deployments"
    run_cmd "$K8S_CLI delete deployment redis postgres -n $NAMESPACE 2>/dev/null || true"
    
    log_section "Deleting Services"
    run_cmd "$K8S_CLI delete service redis postgres -n $NAMESPACE 2>/dev/null || true"
    
    log_section "Deleting PVC"
    run_cmd "$K8S_CLI delete pvc benchmark-results -n $NAMESPACE 2>/dev/null || true"
    
    log_section "Deleting ConfigMaps"
    run_cmd "$K8S_CLI delete configmap benchmark-config benchmark-script -n $NAMESPACE 2>/dev/null || true"
    
    log_section "Deleting Secrets"
    run_cmd "$K8S_CLI delete secret aws-credentials postgres-secret -n $NAMESPACE 2>/dev/null || true"
    
    log_section "Deleting Build Resources"
    run_cmd "$K8S_CLI delete buildconfig --all -n $NAMESPACE 2>/dev/null || true"
    run_cmd "$K8S_CLI delete imagestream --all -n $NAMESPACE 2>/dev/null || true"
    run_cmd "$K8S_CLI delete builds --all -n $NAMESPACE 2>/dev/null || true"
    
    log_section "Verifying Cleanup"
    local remaining
    remaining=$($K8S_CLI get all -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l | tr -d ' ')
    
    if [[ "$remaining" -eq 0 ]]; then
        log_success "All resources cleaned up successfully"
    else
        log_warn "Some resources may remain:"
        $K8S_CLI get all -n "$NAMESPACE" 2>/dev/null || true
    fi
    
    echo ""
    log_info "To delete the namespace entirely: $K8S_CLI delete namespace $NAMESPACE"
}

setup_local_env() {
    log_section "Setting Up Local Environment"
    
    cd "$SCRIPT_DIR"
    
    if [[ ! -d ".venv" ]]; then
        log_info "Creating Python virtual environment..."
        run_cmd "python3 -m venv .venv"
    fi
    
    log_info "Installing dependencies..."
    run_cmd "./.venv/bin/pip install -q feast matplotlib numpy pandas pyyaml"
    
    log_success "Local environment ready"
}

#-------------------------------------------------------------------------------
# Image Build
#-------------------------------------------------------------------------------

# Global array to track async builds: "build_name:image_tag"
declare -a ASYNC_BUILDS=()

ensure_build_resources() {
    # Check build resources exist
    if [[ ! -f "${BUILD_DIR}/imagestream.yaml" ]] || [[ ! -f "${BUILD_DIR}/buildconfig.yaml" ]]; then
        log_error "Build resources not found in ${BUILD_DIR}"
        exit 1
    fi
    
    # Ensure ImageStream and BuildConfig exist for feast-benchmark
    if ! $K8S_CLI get imagestream feast-benchmark -n "$NAMESPACE" &>/dev/null; then
        log_info "Creating ImageStream..."
        run_cmd "$K8S_CLI apply -f ${BUILD_DIR}/imagestream.yaml -n $NAMESPACE"
    fi
    
    if ! $K8S_CLI get buildconfig feast-benchmark -n "$NAMESPACE" &>/dev/null; then
        log_info "Creating BuildConfig..."
        run_cmd "$K8S_CLI apply -f ${BUILD_DIR}/buildconfig.yaml -n $NAMESPACE"
    fi
    
    # Ensure base image resources exist
    if [[ -f "${BUILD_DIR}/imagestream-base.yaml" ]]; then
        if ! $K8S_CLI get imagestream feast-benchmark-base -n "$NAMESPACE" &>/dev/null; then
            log_info "Creating Base ImageStream..."
            run_cmd "$K8S_CLI apply -f ${BUILD_DIR}/imagestream-base.yaml -n $NAMESPACE"
        fi
    fi
    
    if [[ -f "${BUILD_DIR}/buildconfig-base.yaml" ]]; then
        if ! $K8S_CLI get buildconfig feast-benchmark-base -n "$NAMESPACE" &>/dev/null; then
            log_info "Creating Base BuildConfig..."
            run_cmd "$K8S_CLI apply -f ${BUILD_DIR}/buildconfig-base.yaml -n $NAMESPACE"
        fi
    fi
}

# Build base image (one-time, contains all dependencies except feast)
build_base_image() {
    log_section "Building Base Image"
    
    # Check if base image already exists with valid tag
    local base_tag
    base_tag=$($K8S_CLI get istag feast-benchmark-base:latest -n "$NAMESPACE" -o jsonpath='{.tag.name}' 2>/dev/null || echo "")
    
    if [[ "$base_tag" == "latest" ]]; then
        log_info "Base image already exists, skipping build"
        return 0
    fi
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${YELLOW}[DRY-RUN]${NC} Would build base image"
        return 0
    fi
    
    log_info "Building base image (one-time, ~3-5 minutes)..."
    log_info "This image contains all dependencies except Feast itself"
    
    # Create minimal build directory
    local build_dir="/tmp/feast_build_base"
    rm -rf "$build_dir"
    mkdir -p "$build_dir"
    
    # Copy required files
    cp "$SCRIPT_DIR"/Dockerfile.base "$build_dir/"
    cp "$SCRIPT_DIR"/scripts/*.py "$build_dir/" 2>/dev/null || true
    cp "$SCRIPT_DIR"/requirements.txt "$build_dir/" 2>/dev/null || true
    
    # Run the build
    if $K8S_CLI start-build feast-benchmark-base -n "$NAMESPACE" \
        --from-dir="$build_dir" \
        --follow; then
        log_success "Base image built successfully"
    else
        log_error "Base image build failed"
        rm -rf "$build_dir"
        exit 1
    fi
    
    rm -rf "$build_dir"
}

# Start a build asynchronously (no --follow)
start_build_async() {
    local git_ref="$1"
    local git_url="${2:-https://github.com/feast-dev/feast.git}"
    local image_tag="${3:-latest}"
    local feast_extras="${4:-}"  # Optional reference-specific extras
    
    # Use default extras if not specified
    if [[ -z "$feast_extras" ]]; then
        feast_extras=$(echo "$CONFIG_JSON" | python3 -c "import sys,json; c=json.load(sys.stdin); print(c.get('feast_extras','redis,aws,postgres'))" 2>/dev/null || echo "redis,aws,postgres")
    fi
    
    log_info "[$image_tag] Starting async build: $git_ref (extras: $feast_extras)"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${YELLOW}[DRY-RUN]${NC} Would start async build: $image_tag ($git_ref) with extras: $feast_extras"
        ASYNC_BUILDS+=("dry-run-${image_tag}:${image_tag}")
        return 0
    fi
    
    # OpenShift binary builds don't support --build-arg, so we patch the Dockerfile
    local dockerfile="${SCRIPT_DIR}/Dockerfile"
    local dockerfile_backup="${SCRIPT_DIR}/Dockerfile.backup.${image_tag}"
    
    # Backup original Dockerfile
    cp "$dockerfile" "$dockerfile_backup"
    
    # Patch ARG defaults in Dockerfile (including extras)
    if [[ -n "$BASE_IMAGE" ]]; then
        sed -i.tmp \
            -e "s|^ARG BASE_IMAGE=.*|ARG BASE_IMAGE=${BASE_IMAGE}|" \
            -e "s|^ARG SKIP_DEPS=.*|ARG SKIP_DEPS=\"true\"|" \
            -e "s|^ARG FEAST_GIT_URL=.*|ARG FEAST_GIT_URL=\"${git_url}\"|" \
            -e "s|^ARG FEAST_GIT_REF=.*|ARG FEAST_GIT_REF=\"${git_ref}\"|" \
            -e "s|^ARG FEAST_EXTRAS=.*|ARG FEAST_EXTRAS=\"${feast_extras}\"|" \
            "$dockerfile"
    else
        sed -i.tmp \
            -e "s|^ARG FEAST_GIT_URL=.*|ARG FEAST_GIT_URL=\"${git_url}\"|" \
            -e "s|^ARG FEAST_GIT_REF=.*|ARG FEAST_GIT_REF=\"${git_ref}\"|" \
            -e "s|^ARG FEAST_EXTRAS=.*|ARG FEAST_EXTRAS=\"${feast_extras}\"|" \
            "$dockerfile"
    fi
    rm -f "${dockerfile}.tmp"
    
    # Create a minimal build directory (avoids uploading .venv, results, etc.)
    local safe_tag=$(echo "$image_tag" | tr '/' '_')
    local build_dir="/tmp/feast_build_${safe_tag}"
    rm -rf "$build_dir"
    mkdir -p "$build_dir"
    
    # Copy only necessary files
    cp "$SCRIPT_DIR"/scripts/*.py "$build_dir/" 2>/dev/null || true
    cp "$SCRIPT_DIR"/requirements.txt "$build_dir/" 2>/dev/null || true
    cp "$dockerfile" "$build_dir/Dockerfile"  # Use patched Dockerfile
    
    # Restore original Dockerfile immediately
    mv "$dockerfile_backup" "$dockerfile"
    
    # Start build WITHOUT --follow (async), capture build name
    local build_output
    build_output=$($K8S_CLI start-build feast-benchmark -n "$NAMESPACE" \
        --from-dir="$build_dir" \
        -o name 2>&1)
    
    # Cleanup build directory
    rm -rf "$build_dir"
    
    # Extract build name from output (output includes upload progress + "build.build.openshift.io/feast-benchmark-XX")
    local build_name
    build_name=$(echo "$build_output" | grep -o 'feast-benchmark-[0-9]*' | tail -1)
    
    if [[ -z "$build_name" ]]; then
        log_error "Failed to start build for $image_tag: $build_output"
        return 1
    fi
    
    log_success "[$image_tag] Build started: $build_name"
    ASYNC_BUILDS+=("${build_name}:${image_tag}")
}

# Wait for all async builds to complete
wait_for_all_builds() {
    if [[ ${#ASYNC_BUILDS[@]} -eq 0 ]]; then
        log_warn "No builds to wait for"
        return 0
    fi
    
    log_section "Waiting for ${#ASYNC_BUILDS[@]} Parallel Builds"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${YELLOW}[DRY-RUN]${NC} Would wait for builds: ${ASYNC_BUILDS[*]}"
        return 0
    fi
    
    local all_complete=false
    local timeout=900  # 15 minutes max
    local elapsed=0
    local interval=15
    
    while [[ "$all_complete" != "true" ]] && [[ $elapsed -lt $timeout ]]; do
        all_complete=true
        local status_line=""
        
        for entry in "${ASYNC_BUILDS[@]}"; do
            local build_name="${entry%%:*}"
            local image_tag="${entry##*:}"
            
            local phase
            phase=$($K8S_CLI get build "$build_name" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null)
            
            case "$phase" in
                Complete)
                    status_line="$status_line [$image_tag:done]"
                    ;;
                Failed|Error|Cancelled)
                    log_error "Build $build_name ($image_tag) failed: $phase"
                    log_info "Logs: $K8S_CLI logs build/$build_name -n $NAMESPACE"
                    return 1
                    ;;
                *)
                    status_line="$status_line [$image_tag:$phase]"
                    all_complete=false
                    ;;
            esac
        done
        
        if [[ "$all_complete" != "true" ]]; then
            echo -ne "\r[${elapsed}s]$status_line          "
            sleep $interval
            elapsed=$((elapsed + interval))
        fi
    done
    
    echo ""  # newline after progress
    
    if [[ $elapsed -ge $timeout ]]; then
        log_error "Build timeout after ${timeout}s"
        return 1
    fi
    
    # Tag all completed builds
    for entry in "${ASYNC_BUILDS[@]}"; do
        local build_name="${entry%%:*}"
        local image_tag="${entry##*:}"
        
        if [[ "$image_tag" != "latest" ]]; then
            log_info "Tagging image as: $image_tag"
            $K8S_CLI tag "feast-benchmark:latest" "feast-benchmark:${image_tag}" -n "$NAMESPACE" 2>/dev/null || true
        fi
    done
    
    log_success "All ${#ASYNC_BUILDS[@]} builds completed"
}

# Original synchronous build (for single-ref mode)
build_feast_image() {
    local git_ref="$1"
    local git_url="${2:-https://github.com/feast-dev/feast.git}"
    local image_tag="${3:-latest}"
    local feast_extras="${4:-}"  # Optional reference-specific extras
    
    # Use default extras if not specified
    if [[ -z "$feast_extras" ]]; then
        feast_extras=$(echo "$CONFIG_JSON" | python3 -c "import sys,json; c=json.load(sys.stdin); print(c.get('feast_extras','redis,aws,postgres'))" 2>/dev/null || echo "redis,aws,postgres")
    fi
    
    log_section "Building Feast Image: $image_tag"
    log_info "Git URL: $git_url"
    log_info "Git Ref: $git_ref"
    log_info "Tag:     $image_tag"
    log_info "Extras:  $feast_extras"
    
    ensure_build_resources
    
    # Log build mode
    if [[ -n "$BASE_IMAGE" ]]; then
        log_info "Starting feast build (~30 seconds with base image)..."
    else
        log_info "Starting full feast build (~3-5 minutes)..."
    fi
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${YELLOW}[DRY-RUN]${NC} Would start build with FEAST_GIT_REF=$git_ref FEAST_GIT_URL=$git_url TAG=$image_tag EXTRAS=$feast_extras"
        return 0
    fi
    
    # OpenShift binary builds don't support --build-arg, so we patch the Dockerfile
    local dockerfile="${SCRIPT_DIR}/Dockerfile"
    local dockerfile_backup="${SCRIPT_DIR}/Dockerfile.backup"
    
    # Backup original Dockerfile
    cp "$dockerfile" "$dockerfile_backup"
    
    # Patch ARG defaults in Dockerfile (including extras)
    log_info "Patching Dockerfile for: $git_ref"
    
    # Check if using base image for fast builds
    if [[ -n "$BASE_IMAGE" ]]; then
        log_info "Using base image: $BASE_IMAGE (fast build mode)"
        sed -i.tmp \
            -e "s|^ARG BASE_IMAGE=.*|ARG BASE_IMAGE=${BASE_IMAGE}|" \
            -e "s|^ARG SKIP_DEPS=.*|ARG SKIP_DEPS=\"true\"|" \
            -e "s|^ARG FEAST_GIT_URL=.*|ARG FEAST_GIT_URL=\"${git_url}\"|" \
            -e "s|^ARG FEAST_GIT_REF=.*|ARG FEAST_GIT_REF=\"${git_ref}\"|" \
            -e "s|^ARG FEAST_EXTRAS=.*|ARG FEAST_EXTRAS=\"${feast_extras}\"|" \
            "$dockerfile"
    else
        log_info "Full build mode (all dependencies)"
        sed -i.tmp \
            -e "s|^ARG FEAST_GIT_URL=.*|ARG FEAST_GIT_URL=\"${git_url}\"|" \
            -e "s|^ARG FEAST_GIT_REF=.*|ARG FEAST_GIT_REF=\"${git_ref}\"|" \
            -e "s|^ARG FEAST_EXTRAS=.*|ARG FEAST_EXTRAS=\"${feast_extras}\"|" \
            "$dockerfile"
    fi
    rm -f "${dockerfile}.tmp"
    
    # Create a minimal build directory (avoids uploading .venv, results, etc.)
    local safe_tag=$(echo "$image_tag" | tr '/' '_')
    local build_dir="/tmp/feast_build_${safe_tag}"
    rm -rf "$build_dir"
    mkdir -p "$build_dir"
    
    # Copy only necessary files
    cp "$SCRIPT_DIR"/scripts/*.py "$build_dir/" 2>/dev/null || true
    cp "$SCRIPT_DIR"/requirements.txt "$build_dir/" 2>/dev/null || true
    cp "$dockerfile" "$build_dir/Dockerfile"  # Use patched Dockerfile
    
    # Restore original Dockerfile immediately
    mv "$dockerfile_backup" "$dockerfile"
    
    # Run the build
    local build_success=false
    if $K8S_CLI start-build feast-benchmark -n "$NAMESPACE" \
        --from-dir="$build_dir" \
        --follow; then
        build_success=true
    fi
    
    # Cleanup build directory
    rm -rf "$build_dir"
    
    if [[ "$build_success" != "true" ]]; then
        log_error "Build failed"
        exit 1
    fi
    
    # Tag the image for this ref
    if [[ "$image_tag" != "latest" ]]; then
        log_info "Tagging image as: $image_tag"
        $K8S_CLI tag "feast-benchmark:latest" "feast-benchmark:${image_tag}" -n "$NAMESPACE" 2>/dev/null || true
    fi
    
    # Verify build succeeded
    local latest_build
    latest_build=$($K8S_CLI get builds -n "$NAMESPACE" -l buildconfig=feast-benchmark --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null)
    
    local build_status
    build_status=$($K8S_CLI get build "$latest_build" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null)
    
    if [[ "$build_status" == "Complete" ]]; then
        log_success "Build completed successfully: $latest_build"
    else
        log_error "Build failed with status: $build_status"
        log_info "Check build logs: $K8S_CLI logs build/$latest_build -n $NAMESPACE"
        exit 1
    fi
}

#-------------------------------------------------------------------------------
# K8s Job Management
#-------------------------------------------------------------------------------
delete_existing_jobs() {
    local store="$1"
    log_verbose "Deleting existing job for $store..."
    run_cmd "$K8S_CLI delete job feast-benchmark-${store} -n $NAMESPACE --ignore-not-found 2>/dev/null || true"
}

create_job() {
    local store="$1"
    local image_tag="${2:-latest}"
    local job_file="${JOBS_DIR}/${store}-job.yaml"
    
    if [[ ! -f "$job_file" ]]; then
        log_error "Job file not found: $job_file"
        return 1
    fi
    
    log_info "Creating job for $store (image tag: $image_tag)..."
    
    # If using non-latest tag, patch the job with the correct image tag
    if [[ "$image_tag" != "latest" ]]; then
        # Create job with patched image tag
        local image_url="image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/feast-benchmark:${image_tag}"
        run_cmd "cat $job_file | sed 's|feast-benchmark:latest|feast-benchmark:${image_tag}|g' | $K8S_CLI create -f - -n $NAMESPACE"
    else
        run_cmd "$K8S_CLI create -f $job_file -n $NAMESPACE"
    fi
}

wait_for_job() {
    local store="$1"
    log_info "Waiting for $store job to complete (timeout: ${TIMEOUT}s)..."
    run_cmd "$K8S_CLI wait --for=condition=complete job/feast-benchmark-${store} -n $NAMESPACE --timeout=${TIMEOUT}s"
}

get_job_logs() {
    local store="$1"
    log_verbose "Getting logs for $store..."
    $K8S_CLI logs -n "$NAMESPACE" -l store="$store" --tail=50 2>/dev/null || true
}

#-------------------------------------------------------------------------------
# Results Collection
#-------------------------------------------------------------------------------
create_results_reader() {
    log_info "Creating results reader pod..."
    run_cmd "$K8S_CLI delete pod results-reader -n $NAMESPACE --ignore-not-found 2>/dev/null || true"
    sleep 2
    
    run_cmd "$K8S_CLI run results-reader -n $NAMESPACE --image=busybox --restart=Never \
        --overrides='{\"spec\":{\"containers\":[{\"name\":\"results-reader\",\"image\":\"busybox\",\"command\":[\"sleep\",\"3600\"],\"volumeMounts\":[{\"name\":\"results\",\"mountPath\":\"/results\"}]}],\"volumes\":[{\"name\":\"results\",\"persistentVolumeClaim\":{\"claimName\":\"benchmark-results\"}}]}}'"
    
    log_info "Waiting for results reader pod..."
    run_cmd "$K8S_CLI wait --for=condition=ready pod/results-reader -n $NAMESPACE --timeout=60s"
}

fetch_results() {
    local store="$1"
    local output_base="${2:-${SCRIPT_DIR}/${OUTPUT_DIR}}"
    
    log_info "Fetching results for $store..."
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${YELLOW}[DRY-RUN]${NC} Would fetch /results/${store}/ to $output_base/${store}/"
        return 0
    fi
    
    # Fetch results for each scenario (entity_scaling and feature_scaling)
    for scenario in entity_scaling feature_scaling; do
        local output_dir="${output_base}/${store}/${scenario}"
        local output_file="${output_dir}/benchmark_results.json"
        mkdir -p "$output_dir"
        
        # Check if results exist for this scenario
        if $K8S_CLI exec results-reader -n "$NAMESPACE" -- test -f "/results/${store}/${scenario}/benchmark_results.json" 2>/dev/null; then
            $K8S_CLI exec results-reader -n "$NAMESPACE" -- cat "/results/${store}/${scenario}/benchmark_results.json" > "$output_file" 2>/dev/null
            if [[ -s "$output_file" ]]; then
                log_success "Saved: $output_file"
            fi
        else
            log_verbose "No ${scenario} results found for $store"
        fi
    done
}

cleanup_results_reader() {
    log_info "Cleaning up results reader pod..."
    run_cmd "$K8S_CLI delete pod results-reader -n $NAMESPACE --ignore-not-found 2>/dev/null || true"
}

#-------------------------------------------------------------------------------
# Local Benchmark (SQLite)
#-------------------------------------------------------------------------------
run_local_benchmark() {
    local store="$1"
    local scenario="${2:-entity_scaling}"
    local ref_name="${3:-${CURRENT_REF:-default}}"
    
    # Get store-specific iterations/warmup from config (with defaults)
    local store_iterations=$(get_store_config "$store" "iterations" "$ITERATIONS")
    local store_warmup=$(get_store_config "$store" "warmup" "$WARMUP")
    
    # Handle "all" scenario - run both
    if [[ "$scenario" == "all" ]]; then
        log_info "Running both scenarios for $store..."
        run_local_benchmark "$store" "entity_scaling" "$ref_name"
        run_local_benchmark "$store" "feature_scaling" "$ref_name"
        return
    fi
    
    log_info "Running local benchmark for $store (scenario: $scenario)..."
    log_info "Store $store: $store_iterations iterations, $store_warmup warmup"
    
    # Output path: results/{ref}/{store}_{scenario}/
    local output_path="${SCRIPT_DIR}/${OUTPUT_DIR}/${ref_name}/${store}_${scenario}"
    
    if [[ "$scenario" == "feature_scaling" ]]; then
        # Feature scaling: fixed entities, vary features
        local fixed_entities=$(get_scenario_config "feature_scaling" "entities")
        local feature_counts=$(get_scenario_config "feature_scaling" "features")
        
        fixed_entities="${fixed_entities:-50}"
        feature_counts="${feature_counts:-5 25 50 100 150 200}"
        
        log_info "Feature scaling: $fixed_entities entities, features: $feature_counts"
        
        run_cmd "./.venv/bin/python scripts/unified_benchmark.py \
            --store $store \
            --entities $fixed_entities \
            --features $feature_counts \
            --iterations $store_iterations \
            --warmup $store_warmup \
            --profile \
            --scenario feature_scaling \
            --output $output_path"
    else
        # Entity scaling (default): fixed features, vary entities
        local entities_arg=$(echo "$ENTITIES" | tr ' ' ' ')
        
        run_cmd "./.venv/bin/python scripts/unified_benchmark.py \
            --store $store \
            --features $FEATURES \
            --entities $entities_arg \
            --iterations $store_iterations \
            --warmup $store_warmup \
            --profile \
            --scenario entity_scaling \
            --output $output_path"
    fi
}

#-------------------------------------------------------------------------------
# Chart Generation
#-------------------------------------------------------------------------------
generate_charts() {
    local output_base="${1:-${SCRIPT_DIR}/${OUTPUT_DIR}}"
    local charts_dir="${output_base}/charts"
    
    log_section "Generating Charts"
    
    local dirs=""
    local names=""
    
    for store in $STORES; do
        local result_dir="${output_base}/${store}"
        if [[ -f "${result_dir}/benchmark_results.json" ]]; then
            dirs="$dirs $result_dir"
            names="$names $store"
        else
            log_warn "No results for $store, skipping in charts"
        fi
    done
    
    if [[ -z "$dirs" ]]; then
        log_error "No results found for chart generation"
        return 1
    fi
    
    mkdir -p "$charts_dir"
    
    run_cmd "./.venv/bin/python scripts/generate_charts.py \
        --dirs $dirs \
        --names $names \
        --output $charts_dir"
    
    log_success "Charts saved to: $charts_dir"
}

#-------------------------------------------------------------------------------
# Summary Report
#-------------------------------------------------------------------------------
print_summary() {
    log_header "Benchmark Summary"
    
    echo ""
    echo "Configuration:"
    echo "  Config:      $CONFIG_FILE"
    if [[ -n "$FEAST_GIT_REF" ]]; then
        echo "  Feast:       ${FEAST_GIT_URL:-https://github.com/feast-dev/feast.git}@$FEAST_GIT_REF"
    fi
    echo "  Features:    $FEATURES"
    echo "  Entities:    $ENTITIES"
    echo "  Iterations:  $ITERATIONS"
    echo "  Warmup:      $WARMUP"
    echo "  Stores:      $STORES"
    echo ""
    
    # Show results for each reference
    local refs_to_show
    if [[ -n "${REFS_TO_RUN:-}" ]]; then
        # Split comma-separated refs
        refs_to_show="${REFS_TO_RUN//,/ }"
    elif [[ -n "${FEAST_GIT_REF:-}" ]]; then
        refs_to_show="$FEAST_GIT_REF"
    else
        refs_to_show="master"
    fi
    
    for ref in $refs_to_show; do
        echo "Results for $ref (p99 latency @ 50 entities, 200 features):"
        echo "┌──────────┬──────────┬────────┐"
        echo "│  Store   │  p99(ms) │  SLA   │"
        echo "├──────────┼──────────┼────────┤"
        
        for store in $STORES; do
            # Check version-specific path first, then legacy path
            local result_file="${SCRIPT_DIR}/${OUTPUT_DIR}/${ref}/${store}/entity_scaling/benchmark_results.json"
            if [[ ! -f "$result_file" ]]; then
                result_file="${SCRIPT_DIR}/${OUTPUT_DIR}/${store}/entity_scaling/benchmark_results.json"
            fi
            if [[ ! -f "$result_file" ]]; then
                result_file="${SCRIPT_DIR}/${OUTPUT_DIR}/${store}/benchmark_results.json"
            fi
            
            if [[ -f "$result_file" ]]; then
                local p99=$(python3 -c "
import json
with open('$result_file') as f:
    data = json.load(f)
latency = data.get('latency', [])
for r in latency:
    if r.get('num_entities') == 50:
        print(f\"{r.get('p99', 0):.1f}\")
        break
else:
    print('N/A')
" 2>/dev/null || echo "N/A")
                
                local sla_status="FAIL"
                if [[ "$p99" != "N/A" ]] && (( $(echo "$p99 < 60" | bc -l 2>/dev/null || echo 0) )); then
                    sla_status="PASS"
                fi
                
                printf "│ %-8s │ %8s │ %-6s │\n" "$store" "$p99" "$sla_status"
            else
                printf "│ %-8s │ %8s │ %-6s │\n" "$store" "N/A" "N/A"
            fi
        done
        
        echo "└──────────┴──────────┴────────┘"
        echo ""
    done
    if [[ -n "$CHARTS_OUTPUT" ]]; then
        echo "Charts:  $CHARTS_OUTPUT"
    fi
    echo "Results: ${SCRIPT_DIR}/${OUTPUT_DIR}/"
}

#-------------------------------------------------------------------------------
# Run benchmark for a single reference
#-------------------------------------------------------------------------------
run_single_ref() {
    local ref_name="$1"
    local ref_source="$2"
    local ref_git_url="$3"
    local ref_git_ref="$4"
    
    CURRENT_REF="$ref_name"
    CURRENT_REF_OUTPUT="${SCRIPT_DIR}/${OUTPUT_DIR}/${ref_name}"
    CHARTS_OUTPUT="${CURRENT_REF_OUTPUT}/charts"
    
    log_header "Benchmarking: $ref_name"
    echo ""
    echo "  Source:     $ref_source"
    if [[ "$ref_source" == "git" ]]; then
        echo "  Git URL:    $ref_git_url"
        echo "  Git Ref:    $ref_git_ref"
    fi
    echo "  Output:     $CURRENT_REF_OUTPUT"
    echo ""
    
    # Build image for this ref (with unique tag)
    local image_tag="${ref_name}"
    if [[ "$ref_source" == "git" ]] && [[ "$SKIP_BUILD" != "true" ]]; then
        build_feast_image "$ref_git_ref" "$ref_git_url" "$image_tag"
    fi
    
    # Track which stores to fetch from K8s
    local k8s_stores=""
    
    # Run benchmarks for each store
    for store in $STORES; do
        log_section "[$ref_name] Benchmarking: $store"
        
        if [[ "$SKIP_K8S" == "true" && "$store" != "sqlite" ]]; then
            log_warn "Skipping $store (--skip-k8s enabled)"
            continue
        fi
        
        case $store in
            sqlite)
                if [[ "$SKIP_K8S" == "true" ]]; then
                    run_local_benchmark "$store"
                else
                    delete_existing_jobs "$store"
                    create_job "$store" "$image_tag"
                    k8s_stores="$k8s_stores $store"
                fi
                ;;
            redis|postgres|dynamodb)
                delete_existing_jobs "$store"
                create_job "$store" "$image_tag"
                k8s_stores="$k8s_stores $store"
                ;;
            *)
                log_error "Unknown store: $store"
                ;;
        esac
    done
    
    # Wait for all K8s jobs
    if [[ -n "$k8s_stores" ]]; then
        log_section "[$ref_name] Waiting for K8s Jobs"
        for store in $k8s_stores; do
            wait_for_job "$store" || log_warn "Job $store may have failed"
        done
        
        # Fetch results to ref-specific directory
        log_section "[$ref_name] Collecting Results"
        create_results_reader
        for store in $k8s_stores; do
            fetch_results "$store" "$CURRENT_REF_OUTPUT"
        done
        cleanup_results_reader
    fi
    
    # Generate charts for this ref
    if [[ "$SKIP_CHARTS" != "true" ]]; then
        generate_charts "$CURRENT_REF_OUTPUT"
    fi
    
    log_success "[$ref_name] Complete"
}

#-------------------------------------------------------------------------------
# Generate comparison charts across multiple refs
#-------------------------------------------------------------------------------
generate_comparison_charts() {
    local refs="$1"
    
    log_section "Generating Comparison Charts"
    
    local comparison_dir="${SCRIPT_DIR}/${OUTPUT_DIR}/comparison/charts"
    mkdir -p "$comparison_dir"
    
    # Check that we have results for at least 2 refs
    local ref_count=0
    for ref in $refs; do
        local ref_dir="${SCRIPT_DIR}/${OUTPUT_DIR}/${ref}"
        if [[ -d "$ref_dir" ]]; then
            ref_count=$((ref_count + 1))
        fi
    done
    
    if [[ "$ref_count" -lt 2 ]]; then
        log_warn "Need at least 2 refs with results for comparison"
        return 1
    fi
    
    # Use cross-reference comparison mode
    run_cmd "./.venv/bin/python scripts/generate_charts.py \
        --compare-refs \
        --results-base ${SCRIPT_DIR}/${OUTPUT_DIR} \
        --output $comparison_dir"
    
    log_success "Comparison charts saved to: $comparison_dir"
}

#-------------------------------------------------------------------------------
# Stage: Build - Create Docker images for each ref
#-------------------------------------------------------------------------------
run_stage_build() {
    local refs="$1"
    
    log_header "Stage: BUILD"
    
    ensure_build_resources
    
    # Log build mode
    if [[ -n "$BASE_IMAGE" ]]; then
        log_info "Fast build mode: using base image $BASE_IMAGE"
    else
        log_info "Full build mode: building all dependencies from scratch"
    fi
    
    log_section "Starting Parallel Builds"
    ASYNC_BUILDS=()  # Reset
    
    for ref in $refs; do
        local ref_source=$(get_ref_config "$ref" "source")
        local ref_git_url=$(get_ref_config "$ref" "git_url")
        local ref_git_ref=$(get_ref_config "$ref" "git_ref")
        local ref_extras=$(get_ref_config "$ref" "extras")  # Reference-specific extras
        
        # If ref not in config, use it as a git tag directly with default/provided URL
        if [[ -z "$ref_source" ]]; then
            ref_source="git"
            ref_git_url="${FEAST_GIT_URL:-https://github.com/feast-dev/feast.git}"
            ref_git_ref="$ref"
            log_info "Reference '$ref' not in config, using as git tag: $ref_git_url@$ref"
        fi
        
        if [[ "$ref_source" == "git" ]]; then
            start_build_async "$ref_git_ref" "$ref_git_url" "$ref" "$ref_extras"
            sleep 2  # Small delay to avoid race conditions on Dockerfile
        fi
    done
    
    # Wait for all builds to complete and tag them
    wait_for_all_builds || { log_error "Build phase failed"; exit 1; }
    
    # Tag images from build digests
    log_section "Tagging Images"
    for ref in $refs; do
        local latest_build
        latest_build=$($K8S_CLI get builds -n "$NAMESPACE" -l buildconfig=feast-benchmark \
            --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null)
        
        if [[ -n "$latest_build" ]]; then
            local digest
            digest=$($K8S_CLI get build "$latest_build" -n "$NAMESPACE" \
                -o jsonpath='{.status.output.to.imageDigest}' 2>/dev/null)
            if [[ -n "$digest" ]]; then
                log_info "Tagging feast-benchmark:$ref"
                $K8S_CLI tag "feast-benchmark@$digest" "feast-benchmark:$ref" -n "$NAMESPACE" 2>/dev/null || true
            fi
        fi
    done
    
    log_success "Build stage complete"
}

#-------------------------------------------------------------------------------
# Stage: Benchmark - Sequential Mode (DEFAULT - avoids resource contention)
#-------------------------------------------------------------------------------
run_stage_benchmark_sequential() {
    local refs="$1"
    
    log_header "Stage: BENCHMARK (Sequential Mode - One ref at a time)"
    log_info "Running benchmarks sequentially to avoid resource contention"
    echo ""
    
    local ref_count=0
    local total_refs=$(echo "$refs" | wc -w | tr -d ' ')
    
    for ref in $refs; do
        ((ref_count++))
        log_section "[$ref_count/$total_refs] Benchmarking: $ref"
        
        # Step 1: Clean up existing jobs for this ref
        log_info "Cleaning up existing jobs for $ref..."
        run_cmd "$K8S_CLI delete jobs -l app=feast-benchmark -n $NAMESPACE --ignore-not-found 2>/dev/null || true"
        sleep 2
        
        # Step 2: Launch jobs for THIS ref only
        local job_count=0
        local ref_jobs=""
        
        for store in $STORES; do
            local job_name="bench-${ref//./-}-${store}"
            local job_file="${JOBS_DIR}/${store}-job.yaml"
            
            if [[ ! -f "$job_file" ]]; then
                log_warn "Job file not found: $job_file"
                continue
            fi
            
            # Check if this ref uses DAX
            local use_dax=$(get_ref_config "$ref" "use_dax")
            local dax_endpoint=$(get_dax_endpoint)
            
            # Create job with modified name, image tag, and env vars
            local temp_job=$(mktemp)
            
            if [[ "$use_dax" == "true" || "$use_dax" == "True" ]] && [[ -n "$dax_endpoint" ]] && [[ "$store" == "dynamodb" ]]; then
                log_info "  DAX enabled for $ref (endpoint: $dax_endpoint)"
                cat "$job_file" | \
                    sed "s|name: feast-benchmark-${store}|name: ${job_name}|g" | \
                    sed "s|:latest|:${ref}|g" | \
                    sed "/env:/a\\
            - name: FEAST_REF\\
              value: \"${ref}\"\\
            - name: USE_DAX\\
              value: \"true\"\\
            - name: DAX_ENDPOINT\\
              value: \"${dax_endpoint}\"" > "$temp_job"
            else
                cat "$job_file" | \
                    sed "s|name: feast-benchmark-${store}|name: ${job_name}|g" | \
                    sed "s|:latest|:${ref}|g" | \
                    sed "/env:/a\\
            - name: FEAST_REF\\
              value: \"${ref}\"" > "$temp_job"
            fi
            
            run_cmd "$K8S_CLI apply -f $temp_job -n $NAMESPACE"
            rm -f "$temp_job"
            
            log_verbose "  Created: $job_name"
            ref_jobs="$ref_jobs $job_name"
            ((job_count++))
        done
        
        log_info "Launched $job_count jobs for $ref"
        
        # Step 3: Wait for all jobs for THIS ref to complete
        log_info "Waiting for $ref jobs to complete..."
        echo "  Monitor: $K8S_CLI get pods -l app=feast-benchmark -w"
        
        local failed_jobs=""
        for job_name in $ref_jobs; do
            log_verbose "  Waiting for $job_name..."
            if ! $K8S_CLI wait --for=condition=complete "job/${job_name}" -n $NAMESPACE --timeout=${TIMEOUT}s 2>/dev/null; then
                log_warn "  Job $job_name may have failed or timed out"
                failed_jobs="$failed_jobs $job_name"
            else
                log_verbose "  $job_name completed"
            fi
        done
        
        if [[ -n "$failed_jobs" ]]; then
            log_warn "Failed jobs for $ref:$failed_jobs"
        fi
        
        # Step 4: Collect results for THIS ref immediately
        log_info "Collecting results for $ref..."
        create_results_reader
        
        local ref_output="${SCRIPT_DIR}/${OUTPUT_DIR}/${ref}"
        for store in $STORES; do
            local pvc_path="/results/${ref}/${store}"
            local local_path="${ref_output}/${store}"
            mkdir -p "$local_path"
            
            for scenario in entity_scaling feature_scaling; do
                mkdir -p "${local_path}/${scenario}"
                $K8S_CLI exec results-reader -n $NAMESPACE -- \
                    cat "${pvc_path}/${scenario}/benchmark_results.json" 2>/dev/null \
                    > "${local_path}/${scenario}/benchmark_results.json" 2>/dev/null || true
            done
            
            if [[ -f "${local_path}/entity_scaling/benchmark_results.json" ]]; then
                cp "${local_path}/entity_scaling/benchmark_results.json" "${local_path}/benchmark_results.json"
            fi
        done
        
        cleanup_results_reader
        
        log_success "Completed: $ref"
        echo ""
        
        # Brief pause between refs to let shared resources settle
        if [[ $ref_count -lt $total_refs ]]; then
            log_info "Pausing 5s before next reference..."
            sleep 5
        fi
    done
    
    log_success "Sequential benchmark complete - All $total_refs refs processed"
}

#-------------------------------------------------------------------------------
# Stage: Benchmark - Parallel Mode (faster but may have resource contention)
#-------------------------------------------------------------------------------
run_stage_benchmark_parallel() {
    local refs="$1"
    
    log_header "Stage: BENCHMARK (Parallel Mode)"
    log_warn "Parallel mode may cause resource contention - use --parallel only for fast iteration"
    
    # Step 1: Clean up all existing benchmark jobs
    log_section "Cleaning up existing jobs"
    run_cmd "$K8S_CLI delete jobs -l app=feast-benchmark -n $NAMESPACE --ignore-not-found 2>/dev/null || true"
    sleep 2
    
    # Step 2: Launch ALL jobs for ALL refs and ALL stores in parallel
    log_section "Launching all benchmark jobs in parallel"
    local job_count=0
    local all_jobs=""
    
    for ref in $refs; do
        local ref_source=$(get_ref_config "$ref" "source")
        
        # If ref not in config, treat it as a valid ref anyway (image should exist from build stage)
        if [[ -z "$ref_source" ]]; then
            log_info "Reference '$ref' not in config, assuming image exists"
        fi
        
        log_info "Creating jobs for ref: $ref"
        
        # Check if this ref uses DAX
        local use_dax=$(get_ref_config "$ref" "use_dax")
        local dax_endpoint=$(get_dax_endpoint)
        
        for store in $STORES; do
            local job_name="bench-${ref//./-}-${store}"
            local job_file="${JOBS_DIR}/${store}-job.yaml"
            
            if [[ ! -f "$job_file" ]]; then
                log_warn "Job file not found: $job_file"
                continue
            fi
            
            # Create job with modified name, image tag, and env vars
            local temp_job=$(mktemp)
            
            if [[ "$use_dax" == "true" || "$use_dax" == "True" ]] && [[ -n "$dax_endpoint" ]] && [[ "$store" == "dynamodb" ]]; then
                log_info "  DAX enabled for $ref (endpoint: $dax_endpoint)"
                cat "$job_file" | \
                    sed "s|name: feast-benchmark-${store}|name: ${job_name}|g" | \
                    sed "s|:latest|:${ref}|g" | \
                    sed "/env:/a\\
            - name: FEAST_REF\\
              value: \"${ref}\"\\
            - name: USE_DAX\\
              value: \"true\"\\
            - name: DAX_ENDPOINT\\
              value: \"${dax_endpoint}\"" > "$temp_job"
            else
                cat "$job_file" | \
                    sed "s|name: feast-benchmark-${store}|name: ${job_name}|g" | \
                    sed "s|:latest|:${ref}|g" | \
                    sed "/env:/a\\
            - name: FEAST_REF\\
              value: \"${ref}\"" > "$temp_job"
            fi
            
            run_cmd "$K8S_CLI apply -f $temp_job -n $NAMESPACE"
            rm -f "$temp_job"
            
            log_verbose "  Created: $job_name"
            all_jobs="$all_jobs $job_name"
            ((job_count++))
        done
    done
    
    echo ""
    log_success "Launched $job_count benchmark jobs"
    echo ""
    
    # Step 3: Wait for ALL jobs to complete
    log_section "Waiting for all jobs to complete (timeout: ${TIMEOUT}s)"
    echo "Monitor progress: $K8S_CLI get pods -l app=feast-benchmark -w"
    echo ""
    
    local failed_jobs=""
    for job_name in $all_jobs; do
        log_verbose "Waiting for $job_name..."
        if ! $K8S_CLI wait --for=condition=complete "job/${job_name}" -n $NAMESPACE --timeout=${TIMEOUT}s 2>/dev/null; then
            log_warn "Job $job_name may have failed or timed out"
            failed_jobs="$failed_jobs $job_name"
        fi
    done
    
    if [[ -n "$failed_jobs" ]]; then
        log_warn "Some jobs failed:$failed_jobs"
    fi
    
    # Step 4: Fetch results from PVC for each ref
    log_section "Collecting results from PVC"
    create_results_reader
    
    for ref in $refs; do
        local ref_output="${SCRIPT_DIR}/${OUTPUT_DIR}/${ref}"
        log_info "Fetching results for ref: $ref -> $ref_output"
        
        for store in $STORES; do
            # Results are stored at /results/{ref}/{store}/ on PVC
            local pvc_path="/results/${ref}/${store}"
            local local_path="${ref_output}/${store}"
            mkdir -p "$local_path"
            
            # Copy entity_scaling and feature_scaling results
            for scenario in entity_scaling feature_scaling; do
                run_cmd "$K8S_CLI exec results-reader -n $NAMESPACE -- \
                    cat ${pvc_path}/${scenario}/benchmark_results.json 2>/dev/null \
                    > ${local_path}/${scenario}/benchmark_results.json || true" || true
                mkdir -p "${local_path}/${scenario}"
                $K8S_CLI exec results-reader -n $NAMESPACE -- \
                    cat "${pvc_path}/${scenario}/benchmark_results.json" 2>/dev/null \
                    > "${local_path}/${scenario}/benchmark_results.json" 2>/dev/null || true
            done
            
            # Also create combined benchmark_results.json for backward compatibility
            if [[ -f "${local_path}/entity_scaling/benchmark_results.json" ]]; then
                cp "${local_path}/entity_scaling/benchmark_results.json" "${local_path}/benchmark_results.json"
            fi
        done
    done
    
    cleanup_results_reader
    
    log_success "Benchmark stage complete - Results collected for: $refs"
}

#-------------------------------------------------------------------------------
# Stage: Benchmark - Wrapper (dispatches to sequential or parallel)
#-------------------------------------------------------------------------------
run_stage_benchmark() {
    local refs="$1"
    
    if [[ "$PARALLEL_MODE" == "true" ]]; then
        run_stage_benchmark_parallel "$refs"
    else
        run_stage_benchmark_sequential "$refs"
    fi
}

#-------------------------------------------------------------------------------
# Stage: Charts - Generate charts from results
#-------------------------------------------------------------------------------
run_stage_charts() {
    local refs="$1"
    
    log_header "Stage: CHARTS"
    
    # Generate per-ref charts
    for ref in $refs; do
        local ref_dir="${SCRIPT_DIR}/${OUTPUT_DIR}/${ref}"
        local charts_dir="${ref_dir}/charts"
        
        if [[ ! -d "$ref_dir" ]]; then
            log_warn "No results directory for $ref, skipping"
            continue
        fi
        
        log_section "Generating Charts: $ref"
        mkdir -p "$charts_dir"
        
        # Build dirs and names for stores
        local store_dirs=""
        local store_names=""
        for store in $STORES; do
            local store_dir="${ref_dir}/${store}"
            if [[ -d "$store_dir" ]] && [[ -f "${store_dir}/benchmark_results.json" ]]; then
                store_dirs="$store_dirs $store_dir"
                store_names="$store_names $store"
            fi
        done
        
        if [[ -n "$store_dirs" ]]; then
            run_cmd "./.venv/bin/python scripts/generate_charts.py \
                --dirs $store_dirs \
                --names $store_names \
                --output $charts_dir"
            log_success "Charts saved to: $charts_dir"
        else
            log_warn "No results found for $ref"
        fi
    done
    
    # Generate comparison charts if multiple refs
    local ref_count=$(echo "$refs" | wc -w | tr -d ' ')
    if [[ "$ref_count" -gt 1 ]]; then
        generate_comparison_charts "$refs"
    fi
    
    log_success "Charts stage complete"
}

#-------------------------------------------------------------------------------
# Main Execution
#-------------------------------------------------------------------------------
main() {
    parse_args "$@"
    
    # Handle cleanup early (doesn't need full config)
    if [[ "$CLEANUP" == "true" ]]; then
        # Load minimal config for namespace
        if [[ -z "$CONFIG_FILE" ]]; then
            CONFIG_FILE="${SCRIPT_DIR}/benchmark.config.yaml"
        fi
        load_config "$CONFIG_FILE"
        
        # Use default namespace if not set
        NAMESPACE="${NAMESPACE:-feast-benchmark}"
        
        cleanup_all_resources
        exit 0
    fi
    
    # Validate stage
    case "$STAGE" in
        all|build|benchmark|charts) ;;
        *) log_error "Invalid stage: $STAGE (must be: all, build, benchmark, charts)"; exit 1 ;;
    esac
    
    # Validate scenario
    if [[ -n "$SCENARIO" ]]; then
        case "$SCENARIO" in
            entity_scaling|feature_scaling|all) ;;
            *) log_error "Invalid scenario: $SCENARIO (must be: entity_scaling, feature_scaling, all)"; exit 1 ;;
        esac
    fi
    
    # Load config file (defaults to benchmark.config.yaml)
    if [[ -z "$CONFIG_FILE" ]]; then
        CONFIG_FILE="${SCRIPT_DIR}/benchmark.config.yaml"
    fi
    load_config "$CONFIG_FILE"
    
    # Determine scenario to run
    if [[ -z "$SCENARIO" ]]; then
        SCENARIO=$(get_default_scenario)
    fi
    
    log_header "Feast Online Store Benchmark"
    echo ""
    echo "  Config:     $CONFIG_FILE"
    echo "  Stage:      $STAGE"
    echo "  Scenario:   $SCENARIO"
    echo "  Mode:       $(if [[ "$COMPARE_MODE" == "true" ]]; then echo "Compare"; else echo "Single"; fi)"
    echo "  Stores:     $STORES"
    echo "  Features:   $FEATURES"
    echo "  Entities:   $ENTITIES"
    echo "  Iterations: $ITERATIONS"
    echo "  Namespace:  $NAMESPACE"
    echo "  Dry Run:    $DRY_RUN"
    echo ""
    
    # Charts stage doesn't need K8s prerequisites
    if [[ "$STAGE" != "charts" ]]; then
        check_prerequisites
    fi
    setup_local_env
    
    # Determine which refs to run
    local refs_to_benchmark=""
    
    if [[ "$COMPARE_MODE" == "true" ]] || [[ -n "$REFS_TO_RUN" ]]; then
        # Multi-ref mode
        if [[ -n "$REFS_TO_RUN" ]]; then
            refs_to_benchmark=$(echo "$REFS_TO_RUN" | tr ',' ' ')
        else
            refs_to_benchmark=$(get_all_refs)
        fi
        
        if [[ -z "$refs_to_benchmark" ]]; then
            log_error "No references found in config. Add references section to benchmark.config.yaml"
            exit 1
        fi
        
        echo "  References: $refs_to_benchmark"
        echo ""
        
        # Run requested stage(s)
        case "$STAGE" in
            all)
                run_stage_build "$refs_to_benchmark"
                run_stage_benchmark "$refs_to_benchmark"
                run_stage_charts "$refs_to_benchmark"
                ;;
            build)
                run_stage_build "$refs_to_benchmark"
                ;;
            benchmark)
                run_stage_benchmark "$refs_to_benchmark"
                ;;
            charts)
                run_stage_charts "$refs_to_benchmark"
                ;;
        esac
        
    else
        # Single-ref mode (original behavior)
        local ref_name=""
        local ref_source=""
        local ref_git_url=""
        local ref_git_ref=""
        
        if [[ -n "$FEAST_GIT_REF" ]]; then
            ref_name="custom"
            ref_source="git"
            ref_git_url="${FEAST_GIT_URL:-https://github.com/feast-dev/feast.git}"
            ref_git_ref="$FEAST_GIT_REF"
        else
            ref_name=$(get_default_ref)
            ref_source=$(get_ref_config "$ref_name" "source")
            ref_git_url=$(get_ref_config "$ref_name" "git_url")
            ref_git_ref=$(get_ref_config "$ref_name" "git_ref")
            
            if [[ -z "$ref_source" ]]; then
                ref_name="default"
                ref_source="pypi"
            fi
        fi
        
        refs_to_benchmark="$ref_name"
        
        case "$STAGE" in
            all)
                run_single_ref "$ref_name" "$ref_source" "$ref_git_url" "$ref_git_ref"
                ;;
            build)
                if [[ "$ref_source" == "git" ]]; then
                    build_feast_image "$ref_git_ref" "$ref_git_url" "$ref_name"
                else
                    log_warn "Build stage skipped (source is not git)"
                fi
                ;;
            benchmark)
                SKIP_BUILD=true
                SKIP_CHARTS=true
                run_single_ref "$ref_name" "$ref_source" "$ref_git_url" "$ref_git_ref"
                ;;
            charts)
                run_stage_charts "$ref_name"
                ;;
        esac
    fi
    
    # Print summary (skip for build-only stage)
    if [[ "$STAGE" != "build" ]]; then
        print_summary
    fi
    
    log_header "Complete: $STAGE"
    echo ""
    echo "Results: ${SCRIPT_DIR}/${OUTPUT_DIR}/"
    if [[ "$COMPARE_MODE" == "true" ]] || [[ -n "$REFS_TO_RUN" ]]; then
        echo "Comparison: ${SCRIPT_DIR}/${OUTPUT_DIR}/comparison/charts/"
    fi
}

# Run main
main "$@"
