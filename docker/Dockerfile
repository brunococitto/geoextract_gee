# Use GDAL 3.11 image so gdal==3.11.0 Python package builds against matching libgdal
# required by geocif, python 3.12 slim bookworm image has gdal 3.6.2
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.11.4

# OSGeo image already has GDAL 3.11 and Python 3.12. Do NOT apt install gdal.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential curl git ca-certificates python3.12-venv python3.12-dev libhdf4-dev gfortran && \
    rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh > /tmp/uv-installer.sh && sh /tmp/uv-installer.sh && rm /tmp/uv-installer.sh

ENV PATH="/root/.local/bin:/root/.cargo/bin:${PATH}"
ENV UV_CACHE_DIR=/opt/.uv-cache
ENV PYTHONNOUSERSITE=1

WORKDIR /app

# OSGeo image marks system Python as externally managed; use a venv.
RUN python3 -m venv /opt/geo-stack/.venv
ENV PATH="/opt/geo-stack/.venv/bin:${PATH}"

COPY requirements.txt /app/requirements.txt

RUN uv pip install --upgrade pip setuptools wheel cython && \
    uv pip install --index-strategy unsafe-best-match -r /app/requirements.txt && \
    rm -rf /opt/.uv-cache

COPY requirements-git.txt /app/requirements-git.txt
RUN uv pip install -r /app/requirements-git.txt

COPY verify_install.py /app/verify_install.py
RUN python /app/verify_install.py

CMD ["/bin/bash"]