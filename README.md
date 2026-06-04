# geoextract_gee

Google Earth Engine backend extension for `geoprepare` (geoextract). 

This package provides an optional Google Earth Engine (GEE) execution path for Earth Observation (EO) data extraction. It orchestrates task combinations, processes them using GEE, polls the tasks, and downloads the resulting CSVs from Google Cloud Storage (GCS).

## Prerequisites

- **Google Earth Engine Account**: Ensure you have access and a project set up.
- **Google Cloud Storage**: A GCS bucket is required to export the results before downloading.
- **Environment**: It is recommended to refer to [installer](https://github.com/ritviksahajpal/installer) for setting up the environment required for `geocif` and `geoprepare`.

## Installation

Since `geoextract_gee` is an extension of `geoprepare`, it requires `geoprepare` to be installed. You can install `geoextract_gee` in editable mode:

```bash
pip install -e /path/to/geoextract_gee
```
*(If you are running inside the provided Docker container, this may already be mounted at `/workspace/geoextract_gee`)*

## Docker Environment

If you prefer to run the package inside an isolated container, this repository includes its own fully self-contained Docker setup. 

Please refer to the [Docker Setup Instructions](docker/README.md) inside the `docker/` directory for details on how to build, verify, and run the environment.

## Authentication

If no specific secret or service account is provided in your configuration, the script will automatically trigger the standard Earth Engine authentication flow (`ee.Authenticate()`). Make sure to follow the prompted instructions if running interactively for the first time.

## Configuration

To use the GEE backend, you must enable it in your `geoprepare` configuration file and provide the GEE-specific parameters in the `[DEFAULT]` section:

```ini
[DEFAULT]
# General config
use_gee = True
redo = False

# GEE specific config
gee_project = your-gcp-project-name
gee_bucket = your-gcs-bucket-name
gee_audit_stats = True
```

## Architecture

1.  **Task Submission**: For each region and variable combination, a GEE task is created. The extraction avoids multiprocessing and instead submits tasks to the EE queue, respecting queue limits.
2.  **Polling**: The process tracks the submitted tasks and polls Earth Engine until all tasks are marked as `COMPLETED` or `FAILED`.
3.  **Download and Format**: Once tasks are complete, the resulting CSVs are downloaded concurrently from GCS and formatted to match the structure expected by `geomerge`.
