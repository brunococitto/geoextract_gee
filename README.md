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

## Supported Datasets

The GEE backend currently supports extraction for the following `geoprepare` datasets:

| Dataset | Description | Notes |
|---------|-------------|-------|
| **AEF** (`aef`) | Alpha Earth Fraction | Extracted as a multi-year static average. The GEE dataset has different scaling and quantization compared to the raw files from Source Cooperative. |
| **CHIRPS** (`chirps`) | Precipitation data | |
| **CPC** (`cpc_*`) | Temperature fallbacks | Supports `cpc_tmax`, `cpc_tmin`. |
| **ESI** (`esi`) | Evaporative Stress Index | ESI data in GEE often has a processing lag. The pipeline extracts the last available data for the given period. Supports `esi_4wk` and `esi_12wk`. |
| **NDVI** (`ndvi`) | Vegetation index (MODIS) | Because the GEE pipeline pulls daily observations rather than the 8-day composite blocks used by the legacy `tiles` backend, the timing and number of missing observations at the start or end of a time-series will differ. `geomerge` naturally handles linear interpolation of these gaps across year boundaries. |
| **NSIDC** (`nsidc`) | Snow cover data (MOD10A1) | Supports `nsidc_surface` and  `nsidc_rootzone`. |
| **VIIRS** (`viirs`) | High-res VIIRS NDVI data | |

## Architecture

1.  **Task Submission**: The pipeline extracts data using one of two modes, controlled by `gee_parallel_regions` in the configuration:
    - **Sequential Mode** (`gee_parallel_regions = False`): For each region and variable combination, a separate GEE task is created. 
    - **Parallel Batch Mode** (`gee_parallel_regions = True`): All regions within a country are bundled into a single `ee.FeatureCollection` and processed simultaneously by GEE via `reduceRegion` mapping. This submits a single "bulk" GEE task per year/variable, reducing the task queue size and may also avoid quota limits.
      1. **Note:** For variables requiring heavy reductions (e.g., `nsidc` converting 3-hourly images to daily means), Parallel Batch Mode may cause GEE tasks to take more time to run, time out or run out of memory. In case of error, using Sequential Mode (`gee_parallel_regions = False`) is recommended for these variables.
      2. **Note:** In Parallel Batch Mode, incremental updates for the current year do not generate partial files like Sequential Mode. Instead, the pipeline automatically re-extracts the entire year and overwrites the local CSVs to ensure seamless data coverage without complex local file merging and different time windows required by each variable.
2.  **Polling**: The process tracks the submitted tasks and polls Earth Engine until all tasks are marked as `COMPLETED` or `FAILED`. To prevent unexpected billing, polling monitors EECU usage and cancels runaway tasks exceeding defined limits.
3.  **Download and Format**: Once tasks are complete, the resulting CSVs are downloaded concurrently from Google Cloud Storage using `transfer_manager`. If Parallel Batch Mode was used, the bulk CSV is automatically parsed and split locally into the individual region CSVs expected by `geomerge`.
