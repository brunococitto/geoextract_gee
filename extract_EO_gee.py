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
from shapely.geometry import mapping

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
    elif var in ['nsidc_surface', 'nsidc_rootzone']:
        module_name = 'nsidc'
    elif var in ['esi_4wk', 'esi_12wk']:
        module_name = 'esi'
    elif var == 'viirs':
        module_name = 'viirs'
    elif var == 'aef':
        module_name = 'aef'
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
    max_date: str|None = None,
    bulk_fc: ee.FeatureCollection|None = None,
):
    """
    Submits a GEE export task for a single region (or bulk regions).
    """
    # 1. Convert shapely geometry to EE geometry (WGS84)
    if bulk_fc is not None:
        geom_ee = bulk_fc
    else:
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
    
    if var == 'aef':
        # AEF gets passed year=0 from extract_EO, so we must use the global config years.
        # This will be passed to aef.py which calculates the average over this period.
        date_from = f"2017-01-01"
        # Use current year to ensure we capture the latest AEF range
        current_year = ar.now().year
        date_to = f"{current_year}-01-02"
    else:
        if max_date:
            date_from = max_date
            suffix = max_date.replace("-", "")[4:]
            export_prefix = f"gee_extract/{project_name}/{country}/{scale}/{var}/{year}/{region_id}_{region}_{year}_{var}_{crop}_u{suffix}"
        else:
            date_from = f"{year}-01-01"
        date_to = f"{year+1}-01-01" # Exclusive end date in EE
    
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
            tqdm.write(f"INFO: Skipping task {config.task_desc} - already {state}")
            params.tracked_task_descs.append(config.task_desc)
            return
        elif state in ['COMPLETED', 'SUCCEEDED']:
            if not getattr(params, 'redo', False):
                tqdm.write(f"INFO: Skipping task {config.task_desc} - already {state}")
                # Already done, no need to poll, but we will need it for downloading later.
                return
            else:
                tqdm.write(f"INFO: Task {config.task_desc} already {state}, but redo is True. Re-submitting...")

    # 4. Throttle to respect GEE's 3000 task queue limit
    import time
    if params.active_task_count >= 2900:
        params.logger.info(f"Approaching GEE queue limit ({params.active_task_count} tasks). Pausing submission...")
        throttle_start = time.time()
        while params.active_task_count >= 2500:
            if time.time() - throttle_start > 45 * 60:
                params.logger.error("Throttling timeout (45 minutes) reached! GEE queue is stuck. Aborting submission to prevent infinite hang.")
                raise TimeoutError("GEE queue failed to drain within 45 minutes.")
                
            time.sleep(60)
            ops = ee.data.listOperations()
            params.active_task_count = len([
                op for op in ops 
                if op.get('metadata', {}).get('state') in ['PENDING', 'RUNNING']
            ])
        params.logger.info(f"Queue drained to {params.active_task_count}. Resuming submission...")

    # 5. Create and start task with retry logic
    from tenacity import Retrying, stop_after_attempt, wait_exponential
    
    try:
        for attempt in Retrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=5, min=5, max=30),
            reraise=True
        ):
            with attempt:
                if attempt.retry_state.attempt_number > 1:
                    params.logger.warning(f"Retrying GEE task submission for {region} (attempt {attempt.retry_state.attempt_number}/3)...")
                    
                task = handler.create_task(config, limit)
                task.start()
                
                params.active_task_count += 1
                existing_tasks[config.task_desc] = 'PENDING'
                params.tracked_task_descs.append(config.task_desc)
                tqdm.write(f"INFO: Submitted GEE task: {task.status()['description']} (ID: {task.id})")
                
    except Exception as e:
        params.logger.error(f"Failed to submit GEE task for {region} after 3 attempts: {e}")

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

    handler = get_dataset_handler(var)
    if not handler:
        params.logger.warning(f"No GEE handler found for variable: {var}")
        return combo_id

    dir_output = prepare_output_directory_gee(params, country, scale, crop, var)
    admin_name, admin_id = get_admin_fields(scale)

    threshold = params.parser.getboolean(country, "threshold")
    limit = geo_utils.crop_mask_limit(params, country, threshold)

    tqdm.write(
        f"GEE process combo: country={country} crop={crop} scale={scale} var={var} "
        f"year={year} dir_output={dir_output} admin={admin_name}/{admin_id} "
        f"rows={len(df_country)}"
    )

    # Fetch existing tasks to skip duplicates
    # We now fetch this once in run() to avoid an expensive network call per combination
    existing_tasks = getattr(params, 'existing_tasks', {})
    gee_parallel_regions = params.parser.getboolean("DEFAULT", "gee_parallel_regions", fallback=False)

    current_year = ar.now().year
    
    if gee_parallel_regions:
        features = []
        for _, row in df_country.iterrows():
            if not row[admin_name]:
                continue
            region = str(row[admin_name]).lower().replace(" ", "_").replace("/", "_")
            region_id = str(row[admin_id])
            
            csv_name = f"{region_id}_{region}_{year}_{var}_{crop}.csv"
            path_output = dir_output / csv_name
            empty_path = dir_output / f"_empty_{csv_name}.skip"
            
            # Check if this specific region already exists locally
            if not getattr(params, 'redo', False) and (path_output.exists() or empty_path.exists()):
                # If we're updating current year, we don't support parallel updates yet, 
                # so we just re-run the whole year for all regions, or we could fallback to sequential.
                # For simplicity, we assume bulk processing extracts the full year.
                # Skip adding this feature if it's fully downloaded and we aren't updating it
                if year == current_year and var != 'aef' and path_output.exists():
                    pass # We will re-extract current year entirely for this region to get updates
                else:
                    continue
                    
            crs = df_country.crs if df_country.crs else "EPSG:4326"
            geom_wgs84 = gpd.GeoSeries([row.geometry], crs=crs).to_crs("EPSG:4326").iloc[0]
            
            feat = ee.Feature(ee.Geometry(mapping(geom_wgs84)), {
                "region_label": region,
                "region_id": region_id
            })
            features.append(feat)
            
        if not features:
            params.logger.debug(f"All regions skipped for {country} {var} {year} {crop}")
            return combo_id
            
        fc = ee.FeatureCollection(features)
        
        process_gee_var(
            row=None,
            limit=limit,
            var=var,
            params=params,
            year=year,
            country=country,
            region="all_regions",
            region_id="bulk",
            crop=crop,
            scale=scale,
            afi_file=_afi_file,
            handler=handler,
            crs="EPSG:4326",
            existing_tasks=existing_tasks,
            max_date=None,
            bulk_fc=fc
        )
        return combo_id

    for _, row in df_country.iterrows():
        if not row[admin_name]:
            continue

        region = str(row[admin_name]).lower().replace(" ", "_").replace("/", "_")
        region_id = str(row[admin_id])

        # Check if local CSV already exists (Skip if previously downloaded)
        csv_name = f"{region_id}_{region}_{year}_{var}_{crop}.csv"
        path_output = dir_output / csv_name
        empty_path = dir_output / f"_empty_{csv_name}.skip"
        
        max_date = None
        if not getattr(params, 'redo', False) and (path_output.exists() or empty_path.exists()):
            if year == current_year and path_output.exists() and var != 'aef':
                try:
                    import pandas as pd
                    df_existing = pd.read_csv(path_output)
                    if 'date' in df_existing.columns:
                        val_col = f"{var}_mean" if f"{var}_mean" in df_existing.columns else 'stats_mean'
                        if val_col in df_existing.columns:
                            valid_df = df_existing.dropna(subset=[val_col])
                        else:
                            valid_df = df_existing
                            
                        if not valid_df.empty:
                            max_date = pd.to_datetime(valid_df['date']).max().strftime('%Y-%m-%d')
                except Exception as e:
                    params.logger.warning(f"Failed to read current year ({year}) existing CSV to find max date: {e}")
                    
                if max_date:
                    params.logger.debug(f"Found existing current year CSV for {region}. Will extract from {max_date} onwards.")
                else:
                    params.logger.debug(f"Skipping {region} - {csv_name} (or empty marker) already exists locally.")
                    continue
            else:
                params.logger.debug(f"Skipping {region} - {csv_name} (or empty marker) already exists locally.")
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
            max_date,
        )


    return combo_id

def poll_gee_tasks(params):
    """
    Polls Earth Engine until all tracked tasks finish.
    """
    import time
    
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
            # exist, we guarantee we track the newest run of a task, ignoring old failed ones
            if desc and desc not in current_ops:
                current_ops[desc] = metadata
        
        active_count = 0
        completed_count = 0
        failed_count = 0
        total_eecu_seconds = 0.0
        
        for desc in params.tracked_task_descs:
            metadata = current_ops.get(desc, {})
            state = metadata.get('state', 'UNKNOWN')
            
            # Extract EECU usage (it updates in real-time for running tasks)
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

        eecu_h_pricing = params.parser.getfloat("DEFAULT", "gee_eecu_h_pricing", fallback=0.4)
        cost = total_eecu_seconds / 60 / 60 * eecu_h_pricing

        params.logger.info(
            f"GEE Tasks - Active: {active_count}, "
            f"Completed: {completed_count}, Failed: {failed_count} | "
            f"Total EECU-seconds used: {total_eecu_seconds:.2f} (Cost: ${cost:.2f})"
        )

        if active_count == 0:
            params.logger.info("All tracked tasks have finished processing.")
            break
            
        # Add a timeout to prevent infinite polling
        # we can set this as a parameter in config
        if (time.time() - start_time) > (24 * 3600):
            params.logger.error("Maximum polling time (24 hours) reached! Aborting poll.")
            break
            
        # Wait 2 minutes before polling again
        time.sleep(30)

import pandas as pd

def format_gee_csv(path_output, country, region, region_id, lat, lon, year, var):
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
    df['lat'] = lat
    df['lon'] = lon
    
    # AEF does not have year/doy dependency in geomerge, it's a static 64-band embedding.
    # We just need to ensure the columns exist exactly as-is.
    if var == 'aef':
        # Ensure we maintain the standard column order for the static metadata
        aef_cols = [col for col in df.columns if col.startswith('aef_')]
        cols = ['country', 'region', 'region_id', 'lat', 'lon'] + aef_cols
        df = df[cols]
        df.to_csv(path_output, index=False)
        return
        
    df['year'] = year
    df[var] = df['stats_mean']
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        
        # Pad missing DOYs to ensure 365/366 rows for geomerge
        all_dates = pd.date_range(start=f"{year}-01-01", end=f"{year}-12-31", freq='D')
        df = df.set_index('date').reindex(all_dates).rename_axis('date').reset_index()
        
        df['doy'] = df['date'].dt.dayofyear
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        # Fill static columns for padded rows
        static_cols = ['country', 'region', 'region_id', 'lat', 'lon', 'year']
        for col in static_cols:
            if col in df.columns:
                df[col] = df[col].ffill().bfill()

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
    
    # Ensure correct column ordering while keeping all other columns
    base_cols = ['country', 'region', 'region_id', 'lat', 'lon', 'year', 'doy', var]
    other_cols = [col for col in df.columns if col not in base_cols]
    
    df = df[base_cols + other_cols]
    df.to_csv(path_output, index=False)

def download_gee_csvs(params, combinations):
    """
    Downloads the completed CSVs from Google Cloud Storage into the local output directories concurrently.
    """
    params.logger.info("Downloading completed GEE CSVs from Google Cloud Storage concurrently...")
    from google.cloud import storage
    from google.cloud.storage import transfer_manager
    from google.cloud.storage.retry import DEFAULT_RETRY
    import collections
    import re
    from pathlib import Path
    
    bucket_name = params.parser.get("DEFAULT", "gee_bucket")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    project_name = params.project_name
    
    # 1. Find all unique country/scale pairs
    country_scale_pairs = set()
    for combo in combinations:
        _, country, _, scale, _, _, _, _ = combo
        country_scale_pairs.add((country, scale))
        
    total_download_count = 0
    
    # Process each country/scale super-group
    for country, scale in country_scale_pairs:
        prefix = f"gee_extract/{project_name}/{country}/{scale}/"
        params.logger.info(f"Fetching blob list for {country}/{scale} from GCS...")
        
        # 2. Make one API call per country to get all blobs
        all_blobs = list(bucket.list_blobs(prefix=prefix))
        
        # 3. Group blobs by base CSV name for lookup
        blob_groups = collections.defaultdict(list)
        for b in all_blobs:
            if not b.name.endswith(".csv"):
                continue
            fname = b.name.split("/")[-1]
            base_name = re.sub(r'_u\d{4}\.csv$', '', fname)
            if base_name.endswith('.csv'):
                base_name = base_name[:-4]
            blob_groups[base_name].append(b)
            
        blobs_to_download = [] # List of tuples: (blob_object, str(local_path), csv_name)
        format_queue = [] # List of combos to format later
        split_queue = [] # List of bulk CSVs to split
        
        # 4. Match combinations to blobs
        for combo in combinations:
            params_obj, c_country, crop, c_scale, var, year, afi_file, df_country = combo
            if c_country != country or c_scale != scale:
                continue
                
            dir_output = prepare_output_directory_gee(params, country, scale, crop, var)
            admin_name, admin_id = get_admin_fields(scale)
            
            gee_parallel_regions = params.parser.getboolean("DEFAULT", "gee_parallel_regions", fallback=False)
            
            if gee_parallel_regions:
                bulk_csv_name = f"bulk_all_regions_{year}_{var}_{crop}"
                matching_blobs = blob_groups.get(bulk_csv_name, [])
                
                bulk_csv_filename = f"{bulk_csv_name}.csv"
                path_output = dir_output / bulk_csv_filename
                
                current_year = ar.now().year
                needs_download = False
                if str(year) == str(current_year):
                    needs_download = True
                else:
                    for _, row in df_country.iterrows():
                        if not row[admin_name]: continue
                        region_label = str(row[admin_name]).lower().replace(" ", "_").replace("/", "_")
                        region_id = str(row[admin_id])
                        csv_name = f"{region_id}_{region_label}_{year}_{var}_{crop}.csv"
                        if getattr(params, 'redo', False) or (not (dir_output / csv_name).exists() and not (dir_output / f"_empty_{csv_name}.skip").exists()):
                            needs_download = True
                            break
                        
                if needs_download:
                    for blob in matching_blobs:
                        csv_name = blob.name.split("/")[-1]
                        download_dest = dir_output / csv_name
                        blobs_to_download.append((blob, str(download_dest), csv_name))
                    if matching_blobs:
                        split_queue.append((path_output, df_country, admin_name, admin_id, year, var, crop, dir_output))
                        
                # Add to format_queue for all regions
                for _, row in df_country.iterrows():
                    if not row[admin_name]: continue
                    region_label = str(row[admin_name]).lower().replace(" ", "_").replace("/", "_")
                    region_id = str(row[admin_id])
                    format_queue.append((dir_output / f"{region_id}_{region_label}_{year}_{var}_{crop}.csv", country, region_label, region_id, 
                                         round(row.geometry.centroid.y, 6), 
                                         round(row.geometry.centroid.x, 6), 
                                         year, var, []))
            else:
                for _, row in df_country.iterrows():
                    if not row[admin_name]:
                        continue
                    region_label = str(row[admin_name]).lower().replace(" ", "_").replace("/", "_")
                    region_id = str(row[admin_id])
                    
                    base_csv_name = f"{region_id}_{region_label}_{year}_{var}_{crop}"
                    matching_blobs = blob_groups.get(base_csv_name, [])
                    
                    base_csv_filename = f"{base_csv_name}.csv"
                    path_output = dir_output / base_csv_filename
                    empty_path = dir_output / f"_empty_{base_csv_filename}.skip"
                    
                    update_files_to_merge = []
                    
                    for blob in matching_blobs:
                        csv_name = blob.name.split("/")[-1]
                        is_update = bool(re.search(r'_u\d{4}\.csv$', csv_name))
                        download_dest = dir_output / csv_name
                        
                        if not is_update:
                            if not getattr(params, 'redo', False) and (path_output.exists() or empty_path.exists()):
                                continue
                        else:
                            if download_dest.exists():
                                update_files_to_merge.append(download_dest)
                                continue
                                
                        blobs_to_download.append((blob, str(download_dest), csv_name))
                        
                        if is_update:
                            update_files_to_merge.append(download_dest)
                            
                    format_queue.append((path_output, country, region_label, region_id, 
                                            round(row.geometry.centroid.y, 6), 
                                            round(row.geometry.centroid.x, 6), 
                                            year, var, update_files_to_merge))
                                    
        # 5. Concurrently download everything using transfer_manager.download_many
        if blobs_to_download:
            params.logger.info(f"Downloading {len(blobs_to_download)} CSVs concurrently for {country} {scale}...")
            
            # create pairs of (blob, file_path_string)
            blob_file_pairs = [(b_obj, dest) for b_obj, dest, _ in blobs_to_download]
            
            results = transfer_manager.download_many(
                blob_file_pairs,
                max_workers=16,
                worker_type=transfer_manager.THREAD,
                download_kwargs={"retry": DEFAULT_RETRY}
            )
            
            # Process results
            for (b_obj, dest, csv_name), result in zip(blobs_to_download, results):
                if isinstance(result, Exception):
                    params.logger.error(f"Failed to download {csv_name}: {result}")
                else:
                    dest_path = Path(dest)
                    if dest_path.exists():
                        with open(dest_path, 'r') as f:
                            lines = [next(f, None) for _ in range(2)]
                        if lines[1] is None or not lines[1].strip():
                            params.logger.warning(f"Downloaded CSV is empty: {csv_name}. Renaming with _empty_ prefix and .skip suffix.")
                            empty_path = dest_path.parent / f"_empty_{csv_name}.skip"
                            dest_path.rename(empty_path)
                        else:
                            total_download_count += 1
                            
        # 5.5 Split bulk CSVs locally
        for sq in split_queue:
            path_output, df_country, admin_name, admin_id, sq_year, sq_var, sq_crop, dir_output = sq
            if path_output.exists():
                try:
                    import pandas as pd
                    df_bulk = pd.read_csv(path_output)
                    for _, row in df_country.iterrows():
                        if not row[admin_name]: continue
                        region_label = str(row[admin_name]).lower().replace(" ", "_").replace("/", "_")
                        region_id = str(row[admin_id])
                        
                        if 'region_id' in df_bulk.columns:
                            df_region = df_bulk[df_bulk['region_id'].astype(str) == str(region_id)]
                        else:
                            df_region = df_bulk[df_bulk['region_label'].astype(str) == str(region_label)]
                            
                        target_csv = dir_output / f"{region_id}_{region_label}_{sq_year}_{sq_var}_{sq_crop}.csv"
                        if df_region.empty:
                            empty_path = dir_output / f"_empty_{region_id}_{region_label}_{sq_year}_{sq_var}_{sq_crop}.csv.skip"
                            with open(empty_path, 'w') as f:
                                f.write("date,region_label\\n")
                        else:
                            df_region.to_csv(target_csv, index=False)
                            
                    # Clean up the bulk CSV to save space
                    path_output.unlink()
                except Exception as e:
                    params.logger.error(f"Failed to split bulk CSV {path_output}: {e}")
                    
        # 6. Format and merge updates locally
        for fq in format_queue:
            path_output, f_country, region, region_id, lat, lon, f_year, f_var, update_files = fq
            
            if path_output.exists():
                try:
                    format_gee_csv(path_output, f_country, region, region_id, lat, lon, f_year, f_var)
                except Exception as e:
                    params.logger.error(f"Failed to format base CSV {path_output}: {e}")
                    
            if update_files and path_output.exists():
                import pandas as pd
                try:
                    base_df = pd.read_csv(path_output)
                    dfs = [base_df]
                    for uf in update_files:
                        uf_path = Path(uf)
                        if uf_path.exists():
                            format_gee_csv(uf_path, f_country, region, region_id, lat, lon, f_year, f_var)
                            udf = pd.read_csv(uf_path)
                            if not udf.empty:
                                dfs.append(udf)
                                
                    merged_df = pd.concat(dfs, ignore_index=True)
                    if 'date' in merged_df.columns:
                        merged_df = merged_df.drop_duplicates(subset=['date'], keep='last')
                        merged_df = merged_df.sort_values(by='date')
                        
                    merged_df.to_csv(path_output, index=False)
                    for uf in update_files:
                        uf_path = Path(uf)
                        if uf_path.exists():
                            uf_path.unlink()
                except Exception as e:
                    params.logger.error(f"Failed to merge update files for {path_output}: {e}")

    params.logger.info(f"Successfully downloaded and formatted {total_download_count} new CSVs from GCS.")
    params.logger.info("GeoExtract GEE Pipeline Complete!")

def run(obj):
    """Main entry for the GEE extraction pipeline."""
    params = obj
    params.logger.info("Running GeoExtract with GEE")

    # Initialize Earth Engine once
    common.init_ee(params)
    
    params.redo = params.parser.getboolean("DEFAULT", "redo", fallback=False)
    if params.redo:
        params.logger.warning("redo is set to True in config. All datasets will be re-processed on Earth Engine, which may result in increased EECU costs.")
    
    params.tracked_task_descs = []

    from geoprepare.extract.extract_EO import build_combinations
    combinations = build_combinations(obj)

    # Map local paths to EE assets (hashing, uploading, and ingesting automatically)
    from .asset_manager import sync_cropmasks
    unique_cropmasks = set(combo[6] for combo in combinations)
    cropmask_map = sync_cropmasks(params, unique_cropmasks)
    
    # Store the map in params for easy access in handlers
    params.cropmask_map = cropmask_map

    # Fetch existing tasks once to avoid network overhead per combination
    params.existing_tasks = {}
    params.active_task_count = 0
    for op in ee.data.listOperations():
        metadata = op.get('metadata', {})
        desc = metadata.get('description')
        state = metadata.get('state')
        # Keep only the newest state. If the newest run FAILED, we must record it as FAILED 
        # so we don't accidentally fall back to tracking an older COMPLETED run
        if desc and desc not in params.existing_tasks:
            params.existing_tasks[desc] = state
            if state in ['PENDING', 'RUNNING', 'READY']:
                params.active_task_count += 1

    for combo in tqdm(combinations, desc="GeoExtract GEE", unit="combo"):
        process_gee(combo)
        
    poll_gee_tasks(params)
    
    # Download everything from GCS
    download_gee_csvs(params, combinations)