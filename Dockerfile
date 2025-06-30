FROM ghcr.io/osgeo/gdal:ubuntu-small-3.9.3

ENV USR_LOCAL_BIN=/usr/local/bin
ENV VENV_DIR=/app/.venv
ENV PYTHON_VERSION="3.13"
ENV PATH=${USR_LOCAL_BIN}:${PATH} \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=${VENV_DIR} \
    UV_UNMANAGED_INSTALL=${USR_LOCAL_BIN}

# Update package lists
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gcc build-essential \
    && rm -rf /var/lib/apt/lists

# TODO: Restore the non-root user stuff

# RUN useradd -m -s /bin/bash appuser
# RUN chown -R appuser:appuser /app

# USER appuser
# COPY --chown=appuser . .
# RUN chown -R appuser:appuser /app

COPY . /app
WORKDIR /app

# Create a virtual environment with uv inside the container
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && uv venv ${VENV_DIR} --python ${PYTHON_VERSION} --seed --system-site-packages

## Verify GDAL and core Python package installation
RUN . ${VENV_DIR}/bin/activate \
    && uv sync --locked --no-install-project --no-dev

ENV PATH=${VENV_DIR}/bin:${USR_LOCAL_BIN}:${PATH}