"""
RAG Feature Definitions - HuggingFace Dataset with Ray Compute Engine

Uses RaySource to read a HuggingFace dataset (SQuAD) and a Ray-native
BatchFeatureView to generate embeddings with sentence-transformers for
vector similarity search via Milvus.

Stack:
  - Data source:   HuggingFace 'rajpurkar/squad' via RaySource
  - Compute:       Ray batch engine (distributed embedding generation)
  - Online store:  Milvus (vector search)
  - Offline store: PostgreSQL
  - Registry:      PostgreSQL (SQL)
"""

from datetime import timedelta

import pandas as pd

from feast import BatchFeatureView, Entity, FeatureService, Field, ValueType
from feast.infra.offline_stores.contrib.ray_offline_store.ray_source import RaySource
from feast.types import Array, Float32, Int64, String

EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

# ---------------------------------------------------------------------------
# Entity
# ---------------------------------------------------------------------------
passage = Entity(
    name="passage",
    description="Unique passage/context for RAG retrieval",
    value_type=ValueType.STRING,
    join_keys=["passage_id"],
)

# ---------------------------------------------------------------------------
# Data Source - HuggingFace SQuAD dataset read via Ray
# ---------------------------------------------------------------------------
squad_source = RaySource(
    name="squad_passages",
    reader_type="huggingface",
    reader_options={
        "dataset_name": "rajpurkar/squad",
        "split": "train",
    },
    timestamp_field="event_timestamp",
    description="SQuAD Wikipedia passages loaded via ray.data.from_huggingface",
)


# ---------------------------------------------------------------------------
# Ray-native UDF for distributed embedding generation
# ---------------------------------------------------------------------------
class PassageEmbeddingProcessor:
    """
    Generates embeddings for SQuAD context passages.
    Model is loaded once per Ray worker and reused across batches.
    """

    def __init__(self):
        import torch
        from sentence_transformers import SentenceTransformer

        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(EMBED_MODEL_ID, device=device)

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        batch = batch.copy()

        batch["passage_id"] = [f"squad_{i}" for i in range(len(batch))]

        if "context" in batch.columns:
            texts = batch["context"].fillna("").tolist()
            embeddings = self.model.encode(
                texts,
                show_progress_bar=False,
                batch_size=min(128, max(32, len(texts))),
                normalize_embeddings=True,
                convert_to_numpy=True,
            )
            batch["embedding"] = embeddings.tolist()
        else:
            import numpy as np

            batch["embedding"] = [
                (np.random.randn(EMBEDDING_DIM) / EMBEDDING_DIM).tolist()
                for _ in range(len(batch))
            ]

        batch["embedding_model"] = EMBED_MODEL_ID

        if "title" not in batch.columns:
            batch["title"] = ""
        if "context" not in batch.columns:
            batch["context"] = ""

        batch["event_timestamp"] = pd.Timestamp.now(tz="UTC")

        return batch


def generate_passage_embeddings(ds):
    """Ray-native UDF: distributed embedding generation over SQuAD passages."""
    num_blocks = ds.num_blocks()
    max_workers = 4
    if num_blocks < max_workers:
        ds = ds.repartition(max_workers)

    return ds.map_batches(
        PassageEmbeddingProcessor,
        batch_format="pandas",
        concurrency=max_workers,
        batch_size=512,
    )


# ---------------------------------------------------------------------------
# BatchFeatureView - embeddings with Ray UDF
# ---------------------------------------------------------------------------
passage_embeddings_view = BatchFeatureView(
    name="passage_embeddings",
    entities=[passage],
    mode="ray",
    ttl=timedelta(days=365),
    schema=[
        Field(name="passage_id", dtype=String),
        Field(name="title", dtype=String),
        Field(name="context", dtype=String),
        Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=EMBEDDING_DIM,
            vector_search_metric="COSINE",
        ),
        Field(name="embedding_model", dtype=String),
    ],
    source=squad_source,
    udf=generate_passage_embeddings,
    online=True,
    tags={
        "team": "ml_platform",
        "use_case": "rag",
        "dataset": "squad",
        "transformation_mode": "ray_native",
    },
)

# ---------------------------------------------------------------------------
# Feature Services
# ---------------------------------------------------------------------------
rag_retrieval_service = FeatureService(
    name="rag_retrieval",
    features=[passage_embeddings_view],
    tags={
        "use_case": "rag_retrieval",
        "compute_engine": "ray",
        "online_store": "milvus",
    },
)
