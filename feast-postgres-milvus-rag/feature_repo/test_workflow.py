"""
RAG Pipeline Demo - Query similar passages from SQuAD via Feast + Milvus

Prerequisites:
    1. feast apply
    2. feast materialize 2020-01-01T00:00:00 2026-12-31T23:59:59
    3. python test_workflow.py

Demonstrates:
    - Ray-powered distributed embedding generation (via materialization)
    - Milvus vector similarity search via Feast retrieve_online_documents_v2
    - End-to-end RAG retrieval pipeline
"""

import sys
from pathlib import Path


def main():
    sys.path.append(str(Path(__file__).parent))

    try:
        from sentence_transformers import SentenceTransformer

        from feast import FeatureStore
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install feast[ray] sentence-transformers pymilvus")
        sys.exit(1)
    store = FeatureStore(repo_path=".")

    feature_views = store.list_feature_views()
    print(f"Feature views: {len(feature_views)}")
    for fv in feature_views:
        print(f"  - {fv.name} ({type(fv).__name__})")

    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    queries = [
        "What is the capital of France?",
        "How does photosynthesis work in plants?",
        "Who wrote the theory of relativity?",
    ]

    for query in queries:
        print(f"\nQuery: '{query}'")
        query_embedding = model.encode([query], normalize_embeddings=True)[0].tolist()

        results = store.retrieve_online_documents_v2(
            features=[
                "passage_embeddings:embedding",
                "passage_embeddings:title",
                "passage_embeddings:context",
            ],
            query=query_embedding,
            top_k=3,
        )

        df = results.to_df()
        if not df.empty:
            print(f"  Top {len(df)} results:")
            for i, row in df.iterrows():
                title = row.get("title", "N/A")
                context = row.get("context", "")
                snippet = context[:120] + "..." if len(context) > 120 else context
                distance = row.get("distance", "N/A")
                print(f"    {i+1}. [{title}] (score: {distance})")
                print(f"       {snippet}")
        else:
            print("  No results found. Ensure materialization has been run.")

    print("\nWhat was demonstrated:")
    print("  - HuggingFace SQuAD dataset read via RaySource")
    print("  - Ray-distributed embedding generation")
    print("  - Milvus vector storage and retrieval")
    print("  - Semantic similarity search via Feast API")


if __name__ == "__main__":
    main()
