"""
extract_EO_gee.py - GEE-backed EO extraction per admin region.

Mirrors the role of ``geoprepare.extract.extract_EO`` for the geoextract_gee extension:
``run`` orchestrates combinations; ``process_gee`` handles one combo.
"""

from __future__ import annotations

import os
import importlib
from pathlib import Path

from tqdm import tqdm
import arrow as ar
import geopandas as gpd
import ee

from geoprepare import utils as geo_utils
from geoprepare.extract.extract_EO import (
    get_admin_fields,
    validate_scale
)

from .datasets import common

def prepare_output_directory_gee(params, country: str, scale: str, crop: str, var: str) -> Path:
    """
    Create (if needed) and return the output directory for GEE CSV staging.
    """
    threshold = params.parser.getboolean(country, "threshold")
    limit = geo_utils.crop_mask_limit(params, country, threshold)
    dir_crop_inputs = Path(f"crop_t{limit}") if threshold else Path(f"crop_p{limit}")

    use_gee_subdir = params.parser.getboolean("DEFAULT", "use_gee_subdir", fallback=False)
    dir_output = params.dir_output
    if use_gee_subdir:
        dir_output = dir_output / "gee"
    dir_output = dir_output / dir_crop_inputs / country / scale / crop / var
    os.makedirs(dir_output, exist_ok=True)
    return dir_output

def get_dataset_handler(var: str):
    """Dynamically import the dataset handler module."""
    if var in ['cpc_tmax', 'cpc_tmin']:
        module_name = 'cpc'
    elif 'nsidc' in var:
        module_name = 'nsidc'
    else:
        module_name = var
        
    try:
        return importlib.import_module(f"geoextract_gee.datasets.{module_name}")
    except ImportError:
        return None

def process_gee_var(
    row,
    limit: int,
    var: str,
    params,
    year: int,
    country: str,
    region: str,
    region_id: str,
    crop: str,
    scale: str,
    afi_file: str,
    handler,
    crs,
    existing_tasks: dict,
):
    """
    Submits a GEE export task for a single region.
    """
    # 1. Convert shapely geometry to EE geometry (WGS84)
    from shapely.geometry import mapping
    import geopandas as gpd
    
    if crs is None:
        params.logger.warning(f"Shapefile CRS is missing. Assuming EPSG:4326 for region {region}.")
        crs = "EPSG:4326"

    geom_wgs84 = gpd.GeoSeries([row.geometry], crs=crs).to_crs("EPSG:4326").iloc[0]
    geom_ee = common.ee.Geometry(mapping(geom_wgs84))

    # 2. Read GEE settings from config
    export_bucket = params.parser.get("DEFAULT", "gee_bucket")
    include_audit = params.parser.getboolean("DEFAULT", "gee_audit_stats")
    
    project_name = params.project_name
    export_prefix = f"gee_extract/{project_name}/{country}/{scale}/{var}/{year}/{region_id}_{region}_{year}_{var}_{crop}"

    # Dates: nov to july for each season
    # so 2025 will be from nov 2025 to july 2026
    # we should read this from crop calendar
    # add a flag and get start/end from calendar at region level
    date_from = f"{year}-11-01"
    date_to = f"{year+1}-08-01" # Exclusive end date in EE
    
    # Get the corresponding EE asset for the cropmask
    cropmask_asset = params.cropmask_map.get(str(afi_file))

    config = common.TaskConfig(
        country=country,
        crop=crop,
        scale=scale,
        var=var,
        year=year,
        region_label=region,
        region_id=region_id,
        geometry_ee=geom_ee,
        date_from=date_from,
        date_to=date_to,
        export_bucket=export_bucket,
        export_prefix=export_prefix,
        cropmask_asset=cropmask_asset,
        include_audit=include_audit
    )

    # 3. Check if task already exists and is RUNNING or COMPLETED
    if config.task_desc in existing_tasks:
        state = existing_tasks[config.task_desc]
        if state in ['RUNNING', 'READY', 'PENDING']:
            params.logger.info(f"Skipping task {config.task_desc} - already {state}")
            params.tracked_task_descs.append(config.task_desc)
            return
        elif state in ['COMPLETED', 'SUCCEEDED']:
            params.logger.info(f"Skipping task {config.task_desc} - already {state}")
            # Already done, no need to poll, but we will need it for downloading later.
            # We can track it or assume it's just ready in GCS.
            return

    # 4. Throttle to respect GEE's 3000 task queue limit
    import time
    if params.active_task_count >= 2900:
        params.logger.info(f"Approaching GEE queue limit ({params.active_task_count} tasks). Pausing submission...")
        while params.active_task_count >= 2500:
            time.sleep(60)
            ops = ee.data.listOperations()
            params.active_task_count = len([
                op for op in ops 
                if op.get('metadata', {}).get('state') in ['PENDING', 'RUNNING']
            ])
        params.logger.info(f"Queue drained to {params.active_task_count}. Resuming submission...")

    # 5. Create and start task
    try:
        task = handler.create_task(config, limit)
        task.start()
        params.active_task_count += 1
        params.tracked_task_descs.append(config.task_desc)
        params.logger.info(f"Submitted GEE task: {task.status()['description']} (ID: {task.id})")
    except Exception as e:
        params.logger.error(f"Failed to submit GEE task for {region}: {e}")

def process_gee(val):
    """
    One ``build_combinations`` item:
    ``(params, country, crop, scale, var, year, afi_file, df_country)``.
    """
    (
        params,
        country,
        crop,
        scale,
        var,
        year,
        _afi_file,
        df_country,
    ) = val

    if var in ['chirps_gefs']:
        raise ValueError(f"Dataset: {var} is not available in Google Earth Engine. Please use regular backend.")

    validate_scale(scale)
    combo_id = (country, crop, var, year)

    if var == "aef":
        params.logger.warning('AEF not implemented yet in GEE backend')
        return combo_id

    handler = get_dataset_handler(var)
    if not handler:
        params.logger.warning(f"No GEE handler found for variable: {var}")
        return combo_id

    dir_output = prepare_output_directory_gee(params, country, scale, crop, var)
    admin_name, admin_id = get_admin_fields(scale)

    threshold = params.parser.getboolean(country, "threshold")
    limit = geo_utils.crop_mask_limit(params, country, threshold)

    params.logger.debug(
        f"GEE process combo: country={country} crop={crop} scale={scale} var={var} "
        f"year={year} dir_output={dir_output} admin={admin_name}/{admin_id} "
        f"rows={len(df_country)}"
    )

    # Fetch existing tasks to skip duplicates
    # This queries GEE for recent tasks (usually limited to the last few days/weeks)
    # to avoid starting a new task if one is already processing or completed.
    import ee
    existing_tasks = {}
    
    if not hasattr(params, 'active_task_count'):
        params.active_task_count = 0
        
    for op in ee.data.listOperations():
        metadata = op.get('metadata', {})
        desc = metadata.get('description')
        state = metadata.get('state')
        # Keep only the newest state. If the newest run FAILED, we must record it as FAILED 
        # so we don't accidentally fall back to tracking an older COMPLETED run!
        if desc and desc not in existing_tasks:
            existing_tasks[desc] = state
            if state in ['PENDING', 'RUNNING', 'READY']:
                params.active_task_count += 1

    for _, row in df_country.iterrows():
        if not row[admin_name]:
            continue
            
        # Hardcoded filter for testing
        if row[admin_name].lower() not in ['nuristan', 'hirat']:
            continue

        region = row[admin_name].lower().replace(" ", "_")
        region_id = row[admin_id]

        # Check if local CSV already exists (Skip if previously downloaded)
        csv_name = f"{region_id}_{region}_{year}_{var}_{crop}.csv"
        path_output = dir_output / csv_name
        if path_output.exists():
            params.logger.debug(f"Skipping {region} - {csv_name} already exists locally.")
            continue

        process_gee_var(
            row,
            limit,
            var,
            params,
            year,
            country,
            region,
            region_id,
            crop,
            scale,
            _afi_file,
            handler,
            df_country.crs,
            existing_tasks,
        )


    return combo_id

def poll_gee_tasks(params):
    """
    Polls Earth Engine until all tracked tasks finish.
    """
    import time
    import ee
    
    start_time = time.time()
    
    if not hasattr(params, "tracked_task_descs") or not params.tracked_task_descs:
        params.logger.info("No active tasks to monitor. Skipping polling.")
        return

    params.logger.info(f"Monitoring {len(params.tracked_task_descs)} active tasks...")
    
    while True:
        operations = ee.data.listOperations()
        current_ops = {}
        for op in operations:
            metadata = op.get('metadata', {})
            desc = metadata.get('description')
            # The API returns newest operations first. By only adding to current_ops if it doesn't
            # exist, we guarantee we track the newest run of a task, ignoring old failed ones!
            if desc and desc not in current_ops:
                current_ops[desc] = metadata
        
        active_count = 0
        completed_count = 0
        failed_count = 0
        total_eecu_seconds = 0.0
        
        for desc in params.tracked_task_descs:
            metadata = current_ops.get(desc, {})
            state = metadata.get('state', 'UNKNOWN')
            
            # Extract EECU usage (it updates in real-time for running tasks!)
            eecu = float(metadata.get('batchEecuUsageSeconds', 0.0))
            total_eecu_seconds += eecu
            
            # Safety Kill-Switch: Prevent billing explosion on runaway tasks
            if state in ['RUNNING', 'PENDING'] and eecu > 3600:
                op_name = metadata.get('name')
                if op_name:
                    params.logger.warning(
                        f"SAFETY ALERT: Task '{desc}' exceeded 3600 EECU-seconds ({eecu:.2f})! "
                        f"Cancelling to prevent billing explosion."
                    )
                    try:
                        ee.data.cancelOperation(op_name)
                    except Exception as e:
                        params.logger.error(f"Failed to cancel task {desc}: {e}")
            
            if state in ['READY', 'RUNNING', 'PENDING']:
                active_count += 1
            elif state in ['COMPLETED', 'SUCCEEDED']:
                completed_count += 1
            elif state in ['FAILED', 'CANCELLED']:
                failed_count += 1

        params.logger.info(
            f"GEE Tasks - Active: {active_count}, "
            f"Completed: {completed_count}, Failed: {failed_count} | "
            f"Total EECU-seconds used: {total_eecu_seconds:.2f}"
        )

        if active_count == 0:
            params.logger.info("All tracked tasks have finished processing.")
            break
            
        # Optional: Add a timeout to prevent infinite polling (e.g. 24 hours max)
        if (time.time() - start_time) > (24 * 3600):
            params.logger.error("Maximum polling time (24 hours) reached! Aborting poll.")
            break
            
        # Wait 2 minutes before polling again
        time.sleep(120)

import pandas as pd

def format_gee_csv(path_output, country, region, region_id, year, var):
    """Formats a raw GEE CSV to match the geomerge structure."""
    if not path_output.exists():
        return
        
    df = pd.read_csv(path_output)
    
    # Skip if already formatted
    if 'doy' in df.columns and f"{var}_mean" in df.columns:
        return
        
    df['country'] = country
    df['region'] = region
    df['region_id'] = region_id
    df['year'] = year
    df[var] = df['stats_mean']
    if 'date' in df.columns:
        df['doy'] = pd.to_datetime(df['date']).dt.dayofyear

    # Dynamically rename columns based on the variable
    rename_map = {
        'stats_mean': f'{var}_mean',
        'stats_min': f'{var}_min',
        'stats_max': f'{var}_max',
        'stats_median': f'{var}_median',
        'stats_sum': f'{var}_sum',
        'stats_std': f'{var}_std',
        'counts_total': 'total_pixels',
        'counts_valid_data': 'valid_data',
        'counts_valid_data_after_masking': 'valid_data_after_masking',
        'counts_weight_sum': 'weight_sum',
        'counts_weight_sum_used': 'weight_sum_used'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # We keep all other columns (like 'date', 'region_label', etc.)
    df.to_csv(path_output, index=False)

def download_gee_csvs(params, combinations):
    """
    Downloads the completed CSVs from Google Cloud Storage into the local output directories concurrently.
    """
    params.logger.info("Downloading completed GEE CSVs from Google Cloud Storage concurrently...")
    from google.cloud import storage
    from google.cloud.storage import transfer_manager
    
    bucket_name = params.parser.get("DEFAULT", "gee_bucket")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    total_download_count = 0
    
    for combo in combinations:
        params_obj, country, crop, scale, var, year, afi_file, df_country = combo
        
        dir_output = prepare_output_directory_gee(params, country, scale, crop, var)
        params.logger.info(dir_output)
        admin_name, admin_id = get_admin_fields(scale)
        project_name = params.project_name
        
        # The GCS prefix for this combination
        prefix = f"gee_extract/{project_name}/{country}/{scale}/{var}/{year}/"
        
        # 1. Fetch all existing blobs in this prefix
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        # 2. Filter out blobs that already exist locally
        blobs_to_download = []
        for blob in blobs:
            if not blob.name.endswith(".csv"):
                continue
                
            csv_name = blob.name.split("/")[-1]
            path_output = dir_output / csv_name
            
            if not path_output.exists():
                params.logger.warning(f'{path_output} already exists')
                # BUGFIX: We only append the csv_name here because transfer_manager
                # will automatically prepend the blob_name_prefix to fetch it from GCS!
                blobs_to_download.append(csv_name)
                
        # 3. Download concurrently
        if blobs_to_download:
            params.logger.info(f"Downloading {len(blobs_to_download)} new CSVs for {country} {crop} {var}...")
            
            # blob_name_prefix strips the GCS path so files go straight into dir_output
            results = transfer_manager.download_many_to_path(
                bucket,
                blobs_to_download,
                destination_directory=str(dir_output),
                blob_name_prefix=prefix,
                max_workers=8,
                worker_type=transfer_manager.THREAD
            )
            
            for name, result in zip(blobs_to_download, results):
                if isinstance(result, Exception):
                    params.logger.error(f"Failed to download {name}: {result}")
                else:
                    total_download_count += 1
                    
        # 4. Format the downloaded CSVs to match geomerge expectations
        for _, row in df_country.iterrows():
            if not row[admin_name]:
                continue
            if row[admin_name].lower() not in ['nuristan', 'hirat']:
                continue
                
            region = row[admin_name].lower().replace(" ", "_")
            region_id = row[admin_id]
            csv_name = f"{region_id}_{region}_{year}_{var}_{crop}.csv"
            path_output = dir_output / csv_name
            
            try:
                format_gee_csv(path_output, country, region, region_id, year, var)
            except Exception as e:
                params.logger.error(f"Failed to format CSV {path_output}: {e}")
                    
    params.logger.info(f"Successfully downloaded and formatted {total_download_count} new CSVs from GCS.")
    params.logger.info("GeoExtract GEE Pipeline Complete!")


def run(obj):
    """Main entry for the GEE extraction pipeline."""
    params = obj
    params.logger.info("Running GeoExtract with GEE")

    # Initialize Earth Engine once
    common.init_ee(params)
    
    params.tracked_task_descs = []

    from geoprepare.extract.extract_EO import build_combinations
    combinations = build_combinations(obj)

    # Map local paths to EE assets (hashing, uploading, and ingesting automatically)
    from .asset_manager import sync_cropmasks
    unique_cropmasks = set(combo[6] for combo in combinations)
    cropmask_map = sync_cropmasks(params, unique_cropmasks)
    
    # Store the map in params for easy access in handlers
    params.cropmask_map = cropmask_map

    for combo in tqdm(combinations, desc="GeoExtract GEE", unit="combo"):
        process_gee(combo)
        
    poll_gee_tasks(params)
    
    # Download everything from GCS
    download_gee_csvs(params, combinations)