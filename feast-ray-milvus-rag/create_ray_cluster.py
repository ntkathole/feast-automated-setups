#!/usr/bin/env python3
"""
Create a Ray cluster using codeflare-sdk for the Feast RAG pipeline.

Usage:
    python create_ray_cluster.py --name feast-ray --namespace feast-rag
    python create_ray_cluster.py --name feast-ray --namespace feast-rag --workers 4
    python create_ray_cluster.py --name feast-ray --namespace feast-rag --down
"""

import argparse
import sys


def create_cluster(args):
    from codeflare_sdk import Cluster, ClusterConfiguration

    cluster = Cluster(ClusterConfiguration(
        name=args.name,
        namespace=args.namespace,
        image=args.image,
        num_workers=args.workers,
        head_cpu_requests=args.head_cpu_requests,
        head_cpu_limits=args.head_cpu_limits,
        head_memory_requests=args.head_memory,
        head_memory_limits=args.head_memory_limit,
        worker_cpu_requests=args.worker_cpu_requests,
        worker_cpu_limits=args.worker_cpu_limits,
        worker_memory_requests=args.worker_memory,
        worker_memory_limits=args.worker_memory_limit,
    ))
    print(f"Creating Ray cluster '{args.name}' in namespace '{args.namespace}'...")
    cluster.up()

    print("Waiting for cluster to be ready...")
    cluster.wait_ready()

    print(f"Ray cluster ready: {cluster.cluster_uri()}")
    cluster.details()
    return cluster


def delete_cluster(args):
    from codeflare_sdk import get_cluster

    print(f"Deleting Ray cluster '{args.name}' from namespace '{args.namespace}'...")
    cluster = get_cluster(cluster_name=args.name, namespace=args.namespace)
    if cluster is None:
        print(f"Cluster '{args.name}' not found in namespace '{args.namespace}'")
        return
    cluster.down()
    print(f"Ray cluster '{args.name}' deleted.")


def main():
    parser = argparse.ArgumentParser(description="Manage Ray cluster via codeflare-sdk")
    parser.add_argument("--name", default="feast-ray", help="Cluster name")
    parser.add_argument("--namespace", default="feast-rag", help="Kubernetes namespace")
    parser.add_argument("--image", default="quay.io/modh/ray:2.54.1-py312-cu128",
                        help="Ray image")
    parser.add_argument("--workers", type=int, default=1, help="Number of workers")
    parser.add_argument("--head-cpu-requests", type=int, default=1)
    parser.add_argument("--head-cpu-limits", type=int, default=1)
    parser.add_argument("--head-memory", type=int, default=3, help="Head memory requests (Gi)")
    parser.add_argument("--head-memory-limit", type=int, default=6, help="Head memory limits (Gi)")
    parser.add_argument("--worker-cpu-requests", type=int, default=1)
    parser.add_argument("--worker-cpu-limits", type=int, default=1)
    parser.add_argument("--worker-memory", type=int, default=2, help="Worker memory requests (Gi)")
    parser.add_argument("--worker-memory-limit", type=int, default=4, help="Worker memory limits (Gi)")
    parser.add_argument("--down", action="store_true", help="Delete the cluster instead of creating it")
    args = parser.parse_args()

    try:
        if args.down:
            delete_cluster(args)
        else:
            create_cluster(args)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
