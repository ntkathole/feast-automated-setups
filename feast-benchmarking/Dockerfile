# Feast Benchmark Image
#===============================================================================
# Two modes:
#   1. FULL BUILD (default): Install all deps + feast from scratch
#   2. FAST BUILD: Use pre-built base image, only install feast
#
# FULL BUILD (default):
#   ./run_full_benchmark.sh --feast-git-ref=master
#   # OR manually:
#   docker build --build-arg FEAST_GIT_REF=master -t feast-benchmark:master .
#
# FAST BUILD (with base image):
#   # First build base image once:
#   docker build -f Dockerfile.base -t feast-benchmark-base:latest .
#   # Then use it:
#   docker build --build-arg BASE_IMAGE=feast-benchmark-base:latest \
#                --build-arg FEAST_GIT_REF=master -t feast-benchmark:master .
#
# OpenShift with base image:
#   --build-arg BASE_IMAGE=image-registry.openshift-image-registry.svc:5000/feast-benchmark/feast-benchmark-base:latest
#===============================================================================

# Base image: default is python:3.11-slim (full build), override for fast build
ARG BASE_IMAGE=python:3.11-slim
FROM ${BASE_IMAGE}

LABEL maintainer="feast-benchmark"
LABEL description="Feast online store benchmark suite"

# Build arguments for feast installation
ARG FEAST_SOURCE="pypi"
ARG FEAST_GIT_URL="https://github.com/feast-dev/feast.git"
ARG FEAST_GIT_REF="master"
ARG FEAST_EXTRAS="redis,aws,postgres"
ARG FEAST_VERSION=">=0.40.0"

# Flag to skip deps (set to "true" when using base image with deps pre-installed)
ARG SKIP_DEPS="false"

# Store as env vars for runtime reference
ENV FEAST_SOURCE=${FEAST_SOURCE}
ENV FEAST_GIT_URL=${FEAST_GIT_URL}
ENV FEAST_GIT_REF=${FEAST_GIT_REF}

WORKDIR /app

# Install system dependencies (only if not using base image)
RUN if [ "$SKIP_DEPS" = "false" ]; then \
        apt-get update && apt-get install -y --no-install-recommends \
            git \
            curl \
        && rm -rf /var/lib/apt/lists/*; \
    fi

# Install Python dependencies (only if not using base image)
RUN if [ "$SKIP_DEPS" = "false" ]; then \
        pip install --no-cache-dir \
            'click>=7.0.0,<9.0.0' \
            'colorama>=0.3.9,<1' \
            'dill~=0.3.0' \
            'protobuf>=4.24.0' \
            'Jinja2>=2,<4' \
            'jsonschema' \
            'mmh3' \
            'numpy>=1.24,<3' \
            'pandas>=1.4.3,<3' \
            'pyarrow>=12.0.0' \
            'pydantic>=2.0' \
            'pygments>=2.12.0,<3' \
            'PyYAML>=5.4.0,<7' \
            'requests' \
            'SQLAlchemy>1' \
            'tabulate>=0.8.0,<1' \
            'tenacity>=7,<9' \
            'toml>=0.10.0,<1' \
            'tqdm>=4,<5' \
            'typeguard>=4.0.0' \
            'fastapi>=0.68.0' \
            'uvicorn>=0.22.0' \
            'gunicorn' \
            'dask[dataframe]>=2023.0' \
            'prometheus_client' \
            'psutil' \
            'bigtree>=0.19.2' \
            'pyjwt' \
            'orjson>=3.9.0' \
            'fsspec' \
            'aiobotocore>=2.5.0' \
            'boto3>=1.34.0' \
            'botocore>=1.34.0' \
            'psycopg[binary,pool]>=3.0.0' \
            'redis>=4.2.2,<5' \
            'hiredis>=2.0.0,<3' \
            'matplotlib>=3.5.0' \
            'amazon-dax-client>=2.0.0,<3'; \
    fi

# Install feast
RUN if [ "$FEAST_SOURCE" = "git" ]; then \
        echo "Installing feast from git: ${FEAST_GIT_URL}@${FEAST_GIT_REF}"; \
        if [ "$SKIP_DEPS" = "true" ]; then \
            pip install --no-cache-dir --no-deps \
                "feast[${FEAST_EXTRAS}] @ git+${FEAST_GIT_URL}@${FEAST_GIT_REF}"; \
        else \
            pip install --no-cache-dir \
                "feast[${FEAST_EXTRAS}] @ git+${FEAST_GIT_URL}@${FEAST_GIT_REF}"; \
        fi; \
    else \
        echo "Installing feast from PyPI: ${FEAST_VERSION}"; \
        if [ "$SKIP_DEPS" = "true" ]; then \
            pip install --no-cache-dir --no-deps \
                "feast[${FEAST_EXTRAS}]${FEAST_VERSION}"; \
        else \
            pip install --no-cache-dir \
                "feast[${FEAST_EXTRAS}]${FEAST_VERSION}"; \
        fi; \
    fi

# Verify feast installation
RUN python -c "import feast; print(f'Feast {feast.__version__} installed successfully')"

# Copy benchmark scripts
COPY scripts/unified_benchmark.py .
COPY scripts/generate_charts.py .

# Create results directory
RUN mkdir -p /results

# Default command
ENTRYPOINT ["python", "unified_benchmark.py"]
CMD ["--help"]
