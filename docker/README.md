# Docker Environment

This directory contains the necessary files to build an isolated Docker environment for the geospatial stack. 

## 1. Build the Container

To build the Docker image locally, run the following command from within this `docker/` directory (you can change the image tag `geo-stack:local` to whatever you prefer, just make sure to use the same tag in the following commands):

```bash
docker build -t geo-stack:local .
```

## 2. Check the Installation

You can optionally verify that all dependencies and packages are correctly installed inside the newly built image:

```bash
docker run --rm geo-stack:local python /app/verify_install.py
```

## 3. Run the Container

To start an interactive bash session inside the container, mapping a local folder into the container's workspace, run:

```bash
docker run --rm -it \
  -w /workspace \
  -v "/path/to/local/folder":/workspace \
  geo-stack:local \
  bash
```

> [!NOTE]
> The Docker image is configured to automatically install the latest stable version of `geoextract_gee` from its remote GitHub repository during the build. However, if you are actively developing it locally and want to test your local code edits, mapping the folder at runtime (via `-v`) is the best approach. Once inside the container's bash session, simply override the built-in version by installing your local workspace in editable mode:
> ```bash
> pip install -e /workspace
> ```
