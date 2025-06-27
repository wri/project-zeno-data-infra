FROM ghcr.io/osgeo/gdal:ubuntu-small-latest

ENV VENV_DIR=/app/.venv
ENV PYTHON_VERSION="3.12"

# Update package lists and install Python 3.12
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists

# # Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && install -m 0755 /root/.local/bin/uv /usr/local/bin/uv

ENV PATH="/root/.local/bin:$PATH"

RUN useradd -m -s /bin/bash appuser

WORKDIR /app
RUN chown -R appuser:appuser /app

USER appuser

COPY --chown=appuser . .

RUN chown -R appuser:appuser /app

RUN uv venv ${VENV_DIR} --python ${PYTHON_VERSION}
RUN uv pip sync pyproject.toml

RUN . /app/.venv/bin/activate

# Verify GDAL and core Python package installation
RUN python -c "from osgeo import gdal; print(f'GDAL version: {gdal.__version__}')" \
    && python -c "import numpy, xarray; print('Core packages imported successfully')"

# Default command
CMD ["/bin/bash"]
