#!/bin/bash
#===============================================================================
# DAX Cluster Setup Automation
#===============================================================================
# Automatically provisions a DAX cluster for Feast benchmarking.
#
# USAGE:
#   ./scripts/setup_dax.sh [OPTIONS]
#
# OPTIONS:
#   --create          Create DAX cluster (default)
#   --delete          Delete DAX cluster and cleanup resources
#   --status          Check DAX cluster status
#   --wait            Wait for cluster to be available
#   --cluster-name    DAX cluster name (default: feast-dax)
#   --node-type       DAX node type (default: dax.t3.small)
#   --nodes           Number of nodes (default: 1)
#   --help            Show this help
#
# EXAMPLES:
#   ./scripts/setup_dax.sh --create
#   ./scripts/setup_dax.sh --create --node-type dax.t3.medium --nodes 3
#   ./scripts/setup_dax.sh --status
#   ./scripts/setup_dax.sh --delete
#
#===============================================================================

set -euo pipefail

# Defaults
ACTION="create"
CLUSTER_NAME="feast-dax"
NODE_TYPE="dax.t3.small"
NODE_COUNT=1
SUBNET_GROUP_NAME="feast-dax-subnets"
SECURITY_GROUP_NAME="feast-dax-sg"
IAM_ROLE_NAME="FeastDAXServiceRole"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

#-------------------------------------------------------------------------------
# Parse Arguments
#-------------------------------------------------------------------------------
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --create)       ACTION="create"; shift ;;
            --delete)       ACTION="delete"; shift ;;
            --status)       ACTION="status"; shift ;;
            --wait)         ACTION="wait"; shift ;;
            --cluster-name) CLUSTER_NAME="$2"; shift 2 ;;
            --node-type)    NODE_TYPE="$2"; shift 2 ;;
            --nodes)        NODE_COUNT="$2"; shift 2 ;;
            --help|-h)      show_help; exit 0 ;;
            *)              log_error "Unknown option: $1"; exit 1 ;;
        esac
    done
}

show_help() {
    head -30 "$0" | grep -E "^#" | sed 's/^#//' | sed 's/^!/#!/'
}

#-------------------------------------------------------------------------------
# Gather Cluster Info
#-------------------------------------------------------------------------------
gather_cluster_info() {
    log_info "Gathering OpenShift cluster information..."
    
    # Check if oc is available
    if ! command -v oc &>/dev/null; then
        log_error "oc CLI not found. Please install and configure it."
        exit 1
    fi
    
    # Check if aws is available
    if ! command -v aws &>/dev/null; then
        log_error "AWS CLI not found. Please install and configure it."
        exit 1
    fi
    
    # Get region
    REGION=$(oc get infrastructure cluster -o jsonpath='{.status.platformStatus.aws.region}' 2>/dev/null)
    if [[ -z "$REGION" ]]; then
        log_error "Could not determine cluster region. Is the cluster running on AWS?"
        exit 1
    fi
    log_info "Region: $REGION"
    
    # Get cluster name
    OCP_CLUSTER_NAME=$(oc get infrastructure cluster -o jsonpath='{.status.infrastructureName}' 2>/dev/null)
    log_info "OpenShift Cluster: $OCP_CLUSTER_NAME"
    
    # Get VPC ID
    VPC_ID=$(aws ec2 describe-vpcs --region "$REGION" \
        --filters "Name=tag:Name,Values=*${OCP_CLUSTER_NAME}*" \
        --query 'Vpcs[0].VpcId' --output text 2>/dev/null)
    
    if [[ -z "$VPC_ID" || "$VPC_ID" == "None" ]]; then
        log_error "Could not find VPC for cluster $OCP_CLUSTER_NAME"
        exit 1
    fi
    log_info "VPC: $VPC_ID"
    
    # Get subnets
    SUBNET_IDS=$(aws ec2 describe-subnets --region "$REGION" \
        --filters "Name=vpc-id,Values=$VPC_ID" \
        --query 'Subnets[*].SubnetId' --output text 2>/dev/null)
    log_info "Subnets: $SUBNET_IDS"
    
    # Get worker node security group
    NODE_SG=$(aws ec2 describe-security-groups --region "$REGION" \
        --filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=*node*" \
        --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
    
    if [[ -z "$NODE_SG" || "$NODE_SG" == "None" ]]; then
        # Try alternate pattern
        NODE_SG=$(aws ec2 describe-security-groups --region "$REGION" \
            --filters "Name=vpc-id,Values=$VPC_ID" \
            --query 'SecurityGroups[?contains(GroupName, `node`)].GroupId | [0]' --output text 2>/dev/null)
    fi
    log_info "Worker Node SG: $NODE_SG"
    
    # Get AWS account ID
    ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text 2>/dev/null)
    log_info "AWS Account: $ACCOUNT_ID"
}

#-------------------------------------------------------------------------------
# Create DAX Cluster
#-------------------------------------------------------------------------------
create_dax() {
    gather_cluster_info
    
    echo ""
    log_info "Creating DAX cluster: $CLUSTER_NAME"
    echo "  Node Type: $NODE_TYPE"
    echo "  Node Count: $NODE_COUNT"
    echo ""
    
    # Step 1: Create Security Group
    log_info "Step 1/5: Creating security group..."
    DAX_SG=$(aws ec2 describe-security-groups --region "$REGION" \
        --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" "Name=vpc-id,Values=$VPC_ID" \
        --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
    
    if [[ -z "$DAX_SG" || "$DAX_SG" == "None" ]]; then
        DAX_SG=$(aws ec2 create-security-group --region "$REGION" \
            --group-name "$SECURITY_GROUP_NAME" \
            --description "Security group for Feast DAX cluster" \
            --vpc-id "$VPC_ID" \
            --query 'GroupId' --output text 2>/dev/null)
        log_success "Created security group: $DAX_SG"
        
        # Add inbound rules
        aws ec2 authorize-security-group-ingress --region "$REGION" \
            --group-id "$DAX_SG" --protocol tcp --port 8111 --source-group "$NODE_SG" 2>/dev/null || true
        aws ec2 authorize-security-group-ingress --region "$REGION" \
            --group-id "$DAX_SG" --protocol tcp --port 9111 --source-group "$NODE_SG" 2>/dev/null || true
    else
        log_info "Security group already exists: $DAX_SG"
    fi
    
    # Step 2: Create IAM Role
    log_info "Step 2/5: Creating IAM role..."
    if ! aws iam get-role --role-name "$IAM_ROLE_NAME" &>/dev/null; then
        cat > /tmp/dax-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "dax.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
        aws iam create-role \
            --role-name "$IAM_ROLE_NAME" \
            --assume-role-policy-document file:///tmp/dax-trust-policy.json \
            --description "Service role for Feast DAX cluster" >/dev/null
        
        aws iam attach-role-policy \
            --role-name "$IAM_ROLE_NAME" \
            --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess
        
        rm -f /tmp/dax-trust-policy.json
        log_success "Created IAM role: $IAM_ROLE_NAME"
        
        # Wait for role to propagate
        log_info "Waiting for IAM role to propagate..."
        sleep 10
    else
        log_info "IAM role already exists: $IAM_ROLE_NAME"
    fi
    
    ROLE_ARN=$(aws iam get-role --role-name "$IAM_ROLE_NAME" --query 'Role.Arn' --output text)
    
    # Step 3: Create Subnet Group
    log_info "Step 3/5: Creating subnet group..."
    if ! aws dax describe-subnet-groups --subnet-group-names "$SUBNET_GROUP_NAME" --region "$REGION" &>/dev/null; then
        aws dax create-subnet-group \
            --subnet-group-name "$SUBNET_GROUP_NAME" \
            --subnet-ids $SUBNET_IDS \
            --region "$REGION" >/dev/null
        log_success "Created subnet group: $SUBNET_GROUP_NAME"
    else
        log_info "Subnet group already exists: $SUBNET_GROUP_NAME"
    fi
    
    # Step 4: Create DAX Cluster
    log_info "Step 4/5: Creating DAX cluster (this takes 10-15 minutes)..."
    if ! aws dax describe-clusters --cluster-names "$CLUSTER_NAME" --region "$REGION" &>/dev/null; then
        aws dax create-cluster \
            --cluster-name "$CLUSTER_NAME" \
            --node-type "$NODE_TYPE" \
            --replication-factor "$NODE_COUNT" \
            --iam-role-arn "$ROLE_ARN" \
            --subnet-group "$SUBNET_GROUP_NAME" \
            --security-group-ids "$DAX_SG" \
            --region "$REGION" >/dev/null
        log_success "DAX cluster creation initiated: $CLUSTER_NAME"
    else
        log_info "DAX cluster already exists: $CLUSTER_NAME"
    fi
    
    # Step 5: Wait for cluster
    log_info "Step 5/5: Waiting for cluster to be available..."
    wait_for_cluster
    
    # Get endpoint
    echo ""
    log_success "DAX cluster is ready!"
    show_status
}

#-------------------------------------------------------------------------------
# Wait for Cluster
#-------------------------------------------------------------------------------
wait_for_cluster() {
    local max_attempts=60
    local attempt=0
    
    while [[ $attempt -lt $max_attempts ]]; do
        STATUS=$(aws dax describe-clusters --cluster-names "$CLUSTER_NAME" --region "$REGION" \
            --query 'Clusters[0].Status' --output text 2>/dev/null)
        
        if [[ "$STATUS" == "available" ]]; then
            return 0
        elif [[ "$STATUS" == "creating" ]]; then
            echo -ne "\r  Status: $STATUS (attempt $((attempt+1))/$max_attempts, waiting 30s)..."
            sleep 30
            ((attempt++))
        else
            log_error "Unexpected status: $STATUS"
            return 1
        fi
    done
    
    log_error "Timeout waiting for cluster to be available"
    return 1
}

#-------------------------------------------------------------------------------
# Show Status
#-------------------------------------------------------------------------------
show_status() {
    if [[ -z "${REGION:-}" ]]; then
        REGION=$(oc get infrastructure cluster -o jsonpath='{.status.platformStatus.aws.region}' 2>/dev/null)
    fi
    
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  DAX Cluster Status: $CLUSTER_NAME"
    echo "═══════════════════════════════════════════════════════════════"
    
    CLUSTER_INFO=$(aws dax describe-clusters --cluster-names "$CLUSTER_NAME" --region "$REGION" \
        --query 'Clusters[0]' --output json 2>/dev/null)
    
    if [[ -z "$CLUSTER_INFO" || "$CLUSTER_INFO" == "null" ]]; then
        log_warn "Cluster not found: $CLUSTER_NAME"
        return 1
    fi
    
    STATUS=$(echo "$CLUSTER_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin).get('Status','unknown'))")
    NODES=$(echo "$CLUSTER_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin).get('TotalNodes',0))")
    ACTIVE=$(echo "$CLUSTER_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ActiveNodes',0))")
    NODE_TYPE=$(echo "$CLUSTER_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin).get('NodeType','unknown'))")
    
    echo "  Status:      $STATUS"
    echo "  Node Type:   $NODE_TYPE"
    echo "  Nodes:       $ACTIVE / $NODES active"
    
    if [[ "$STATUS" == "available" ]]; then
        ENDPOINT=$(echo "$CLUSTER_INFO" | python3 -c "import sys,json; e=json.load(sys.stdin).get('ClusterDiscoveryEndpoint',{}); print(f\"dax://{e.get('Address','')}:{e.get('Port',8111)}\")")
        echo ""
        echo "  Endpoint:    $ENDPOINT"
        echo ""
        echo "  Feast Config (once DAX support is added):"
        echo "  ─────────────────────────────────────────"
        echo "  online_store:"
        echo "    type: dynamodb"
        echo "    region: \"$REGION\""
        echo "    use_dax: true"
        echo "    dax_endpoint: \"$ENDPOINT\""
    fi
    echo "═══════════════════════════════════════════════════════════════"
}

#-------------------------------------------------------------------------------
# Delete DAX Cluster
#-------------------------------------------------------------------------------
delete_dax() {
    if [[ -z "${REGION:-}" ]]; then
        REGION=$(oc get infrastructure cluster -o jsonpath='{.status.platformStatus.aws.region}' 2>/dev/null)
    fi
    
    if [[ -z "${VPC_ID:-}" ]]; then
        OCP_CLUSTER_NAME=$(oc get infrastructure cluster -o jsonpath='{.status.infrastructureName}' 2>/dev/null)
        VPC_ID=$(aws ec2 describe-vpcs --region "$REGION" \
            --filters "Name=tag:Name,Values=*${OCP_CLUSTER_NAME}*" \
            --query 'Vpcs[0].VpcId' --output text 2>/dev/null)
    fi
    
    echo ""
    log_warn "This will delete the DAX cluster and all associated resources."
    read -p "Are you sure? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        log_info "Cancelled."
        return 0
    fi
    
    # Delete DAX cluster
    log_info "Deleting DAX cluster: $CLUSTER_NAME..."
    if aws dax describe-clusters --cluster-names "$CLUSTER_NAME" --region "$REGION" &>/dev/null; then
        aws dax delete-cluster --cluster-name "$CLUSTER_NAME" --region "$REGION" >/dev/null
        log_info "Waiting for cluster deletion (5-10 minutes)..."
        
        while aws dax describe-clusters --cluster-names "$CLUSTER_NAME" --region "$REGION" &>/dev/null; do
            echo -ne "\r  Deleting..."
            sleep 30
        done
        echo ""
        log_success "Cluster deleted"
    else
        log_info "Cluster not found, skipping"
    fi
    
    # Delete subnet group
    log_info "Deleting subnet group: $SUBNET_GROUP_NAME..."
    aws dax delete-subnet-group --subnet-group-name "$SUBNET_GROUP_NAME" --region "$REGION" 2>/dev/null || true
    
    # Delete security group
    log_info "Deleting security group: $SECURITY_GROUP_NAME..."
    DAX_SG=$(aws ec2 describe-security-groups --region "$REGION" \
        --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" "Name=vpc-id,Values=$VPC_ID" \
        --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
    if [[ -n "$DAX_SG" && "$DAX_SG" != "None" ]]; then
        aws ec2 delete-security-group --group-id "$DAX_SG" --region "$REGION" 2>/dev/null || true
    fi
    
    # Delete IAM role
    log_info "Deleting IAM role: $IAM_ROLE_NAME..."
    aws iam detach-role-policy --role-name "$IAM_ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess 2>/dev/null || true
    aws iam delete-role --role-name "$IAM_ROLE_NAME" 2>/dev/null || true
    
    log_success "Cleanup complete!"
}

#-------------------------------------------------------------------------------
# Main
#-------------------------------------------------------------------------------
main() {
    parse_args "$@"
    
    case "$ACTION" in
        create) create_dax ;;
        delete) delete_dax ;;
        status) show_status ;;
        wait)   
            if [[ -z "${REGION:-}" ]]; then
                REGION=$(oc get infrastructure cluster -o jsonpath='{.status.platformStatus.aws.region}' 2>/dev/null)
            fi
            wait_for_cluster && show_status
            ;;
        *) log_error "Unknown action: $ACTION"; exit 1 ;;
    esac
}

main "$@"
