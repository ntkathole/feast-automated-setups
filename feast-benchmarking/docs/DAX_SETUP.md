# DynamoDB Accelerator (DAX) Setup Guide

This guide explains how to set up a DAX cluster for Feast benchmarking.

## Overview

DAX is an in-memory caching layer for DynamoDB that provides sub-millisecond read latency for cached data.

**Actual Performance Results (from benchmarking):**

| Metric | DynamoDB Direct | With DAX | Improvement |
|--------|-----------------|----------|-------------|
| Raw GetItem (1 key) | 4.0ms | 1.2ms | **70%** |
| Raw BatchGetItem (5 keys) | 7.5ms | 4.9ms | **34%** |
| Raw BatchGetItem (50 keys) | 46.5ms | 45.5ms | **2%** |
| Feast e2e (50 entities) | 72.8ms | ~75ms | **<5%** |

> **Important:** DAX provides significant raw database improvement, but Feast's SDK overhead (93%+ of latency) masks the benefit. See [DAX_BENCHMARK_FINDINGS.md](DAX_BENCHMARK_FINDINGS.md) for details.

## Quick Setup (Automated)

Use the automated setup script:

```bash
# Create DAX cluster
./scripts/setup_dax.sh --create

# Check status
./scripts/setup_dax.sh --status

# Delete cluster and cleanup
./scripts/setup_dax.sh --delete
```

## Prerequisites

- AWS CLI configured with appropriate permissions
- OpenShift/Kubernetes cluster running in AWS
- Access to create IAM roles, security groups, and DAX resources

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     AWS VPC (same region)                    │
│  ┌─────────────────┐         ┌─────────────────────────┐    │
│  │ OpenShift Cluster│         │    DAX Cluster          │    │
│  │ (feast-benchmark)│ ──────► │ Port 8111/9111          │    │
│  │                  │         │                         │    │
│  └─────────────────┘         └──────────┬──────────────┘    │
│                                          │                   │
│                               ┌──────────▼──────────────┐   │
│                               │      DynamoDB           │   │
│                               │   (feast_benchmark)     │   │
│                               └─────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Setup Instructions

### Step 1: Gather Cluster Information

```bash
# Get cluster region
REGION=$(oc get infrastructure cluster -o jsonpath='{.status.platformStatus.aws.region}')
echo "Region: $REGION"

# Get cluster name
CLUSTER_NAME=$(oc get infrastructure cluster -o jsonpath='{.status.infrastructureName}')
echo "Cluster: $CLUSTER_NAME"

# Get VPC ID
VPC_ID=$(aws ec2 describe-vpcs --region $REGION \
    --filters "Name=tag:Name,Values=*${CLUSTER_NAME}*" \
    --query 'Vpcs[0].VpcId' --output text)
echo "VPC: $VPC_ID"

# Get subnets
aws ec2 describe-subnets --region $REGION \
    --filters "Name=vpc-id,Values=$VPC_ID" \
    --query 'Subnets[*].[SubnetId,AvailabilityZone]' --output table

# Get worker node security group
NODE_SG=$(aws ec2 describe-security-groups --region $REGION \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=*node*" \
    --query 'SecurityGroups[0].GroupId' --output text)
echo "Node SG: $NODE_SG"
```

### Step 2: Create DAX Security Group

```bash
# Create security group for DAX
DAX_SG=$(aws ec2 create-security-group --region $REGION \
    --group-name "feast-dax-sg" \
    --description "Security group for Feast DAX cluster" \
    --vpc-id $VPC_ID \
    --query 'GroupId' --output text)
echo "DAX SG: $DAX_SG"

# Allow inbound on port 8111 (unencrypted) from worker nodes
aws ec2 authorize-security-group-ingress --region $REGION \
    --group-id $DAX_SG \
    --protocol tcp \
    --port 8111 \
    --source-group $NODE_SG

# Allow inbound on port 9111 (encrypted) from worker nodes
aws ec2 authorize-security-group-ingress --region $REGION \
    --group-id $DAX_SG \
    --protocol tcp \
    --port 9111 \
    --source-group $NODE_SG
```

### Step 3: Create IAM Role for DAX

```bash
# Create trust policy
cat > /tmp/dax-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "dax.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create the role
aws iam create-role \
    --role-name FeastDAXServiceRole \
    --assume-role-policy-document file:///tmp/dax-trust-policy.json \
    --description "Service role for Feast DAX cluster"

# Attach DynamoDB access policy
aws iam attach-role-policy \
    --role-name FeastDAXServiceRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess

# Get the role ARN
ROLE_ARN=$(aws iam get-role --role-name FeastDAXServiceRole --query 'Role.Arn' --output text)
echo "Role ARN: $ROLE_ARN"
```

### Step 4: Create DAX Subnet Group

```bash
# Replace with your subnet IDs from Step 1
SUBNET_IDS="subnet-xxx subnet-yyy"

aws dax create-subnet-group \
    --subnet-group-name feast-dax-subnets \
    --subnet-ids $SUBNET_IDS \
    --region $REGION
```

### Step 5: Create DAX Cluster

```bash
# Create cluster (takes 10-15 minutes)
aws dax create-cluster \
    --cluster-name feast-dax \
    --node-type dax.t3.small \
    --replication-factor 1 \
    --iam-role-arn $ROLE_ARN \
    --subnet-group feast-dax-subnets \
    --security-group-ids $DAX_SG \
    --region $REGION

# Check status
aws dax describe-clusters --cluster-names feast-dax --region $REGION \
    --query 'Clusters[0].[Status,ClusterDiscoveryEndpoint.Address]' --output text
```

**Node Type Options:**

| Type | vCPU | Memory | Use Case | Cost/hour |
|------|------|--------|----------|-----------|
| dax.t3.small | 2 | 2 GB | Testing | ~$0.04 |
| dax.t3.medium | 2 | 4 GB | Light prod | ~$0.08 |
| dax.r5.large | 2 | 16 GB | Production | ~$0.26 |

### Step 6: Get DAX Endpoint

Once status is `available`:

```bash
# Get endpoint
aws dax describe-clusters --cluster-names feast-dax --region $REGION \
    --query 'Clusters[0].ClusterDiscoveryEndpoint' --output json
```

Output:
```json
{
    "Address": "feast-dax.xxxxx.dax-clusters.eu-west-1.amazonaws.com",
    "Port": 8111,
    "URL": "dax://feast-dax.xxxxx.dax-clusters.eu-west-1.amazonaws.com:8111"
}
```

## Feast Configuration

DAX support is implemented in the `add-dax-client-support` branch: [abhijeet-dhumal/feast](https://github.com/abhijeet-dhumal/feast/tree/add-dax-client-support)

### feature_store.yaml

```yaml
project: my_project
provider: local
online_store:
  type: dynamodb
  region: "eu-west-1"
  use_dax: true
  dax_endpoint: "dax://feast-dax.xxxxx.dax-clusters.eu-west-1.amazonaws.com:8111"
  batch_size: 100
```

### Installation

```bash
# Install Feast with DAX support
pip install "feast[aws,dax] @ git+https://github.com/abhijeet-dhumal/feast.git@add-dax-client-support"
```

### Requirements

- `amazon-dax-client>=2.0.0` (automatically installed with `feast[dax]`)
- DAX cluster must be in the same VPC as your application

## Cleanup

To delete the DAX cluster and associated resources:

```bash
# Delete DAX cluster (takes 5-10 minutes)
aws dax delete-cluster --cluster-name feast-dax --region $REGION

# Wait for deletion, then delete subnet group
aws dax delete-subnet-group --subnet-group-name feast-dax-subnets --region $REGION

# Delete security group
aws ec2 delete-security-group --group-id $DAX_SG --region $REGION

# Delete IAM role (detach policy first)
aws iam detach-role-policy \
    --role-name FeastDAXServiceRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess
aws iam delete-role --role-name FeastDAXServiceRole
```

## Troubleshooting

### Cannot connect to DAX from pods

1. Verify security group allows traffic from worker nodes:
   ```bash
   aws ec2 describe-security-groups --group-ids $DAX_SG --region $REGION
   ```

2. Verify pods are in the same VPC as DAX cluster

3. Check DAX cluster status is `available`:
   ```bash
   aws dax describe-clusters --cluster-names feast-dax --region $REGION
   ```

### DAX cluster creation fails

1. Ensure subnets are in different AZs (for multi-node clusters)
2. Verify IAM role has correct trust policy
3. Check subnet group has valid subnets

### High latency despite DAX

1. Verify cache hits (first request is always a miss)
2. Check if data exists in DynamoDB table
3. Ensure using correct endpoint URL format (`dax://` not `https://`)

## Cost Estimation

| Configuration | Monthly Cost |
|---------------|--------------|
| 1x dax.t3.small (testing) | ~$30 |
| 1x dax.t3.medium (light prod) | ~$60 |
| 3x dax.r5.large (HA prod) | ~$570 |

## References

- [AWS DAX Documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.html)
- [DAX Client for Python](https://github.com/aws/aws-dax-client)
- [Feast DynamoDB Online Store](https://docs.feast.dev/reference/online-stores/dynamodb)
- [DAX Implementation Branch](https://github.com/abhijeet-dhumal/feast/tree/add-dax-client-support)
- [DAX Benchmark Findings](DAX_BENCHMARK_FINDINGS.md)

---

*Last updated: March 20, 2026*
