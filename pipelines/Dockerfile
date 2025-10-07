FROM ghcr.io/osgeo/gdal:ubuntu-small-3.9.3

ENV USR_LOCAL_BIN=/usr/local/bin
ENV VENV_DIR=/app/pipelines/.venv
ENV PYTHON_VERSION="3.13"
ENV UV_VERSION="0.8.19"
ENV PATH=${USR_LOCAL_BIN}:${PATH} \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=${VENV_DIR} \
    UV_UNMANAGED_INSTALL=${USR_LOCAL_BIN}

# Update package lists
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gcc build-essential \
    && rm -rf /var/lib/apt/lists

RUN curl -LsSf https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-installer.sh | sh

RUN useradd -m -s /bin/bash appuser
RUN mkdir -p /app/pipelines
RUN chown appuser:appuser /app/pipelines

USER appuser
COPY --chown=appuser pyproject.toml uv.lock /app/pipelines/
WORKDIR /app/pipelines

# Create a virtual environment with uv inside the container
RUN uv venv ${VENV_DIR} --python ${PYTHON_VERSION} --seed --system-site-packages

## Verify GDAL and core Python package installation
RUN . ${VENV_DIR}/bin/activate && \
    uv sync --locked --no-install-project --no-dev
ENV PATH=${VENV_DIR}/bin:${USR_LOCAL_BIN}:${PATH}
ENV VIRTUAL_ENV=/app/pipelines/.venv

COPY --chown=appuser . /app/pipelines

ENV PYTHONPATH=/app
