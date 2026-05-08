from __future__ import annotations

import ee
from .common import TaskConfig

# --- Dataset Constants ---
AEF_COLLECTION_ID = "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL"

def _aef_stats_feature(
    avg_img: ee.Image,
    config: TaskConfig,
    mask_threshold_percent: float,
) -> ee.Feature:
    """
    One row per region representing the multi-year average of 
    the 64 AEF embedding bands over the crop-masked area.
    """
    avg_img = ee.Image(avg_img)

    afi_thresh = ee.Number(float(mask_threshold_percent * 100))
    w_raw = ee.Image(config.cropmask_asset).float().clip(config.geometry_r)
    w = w_raw.updateMask(w_raw.gt(afi_thresh))

    # Mask the AEF image to the crop area
    p = avg_img.float().clip(config.geometry_r)
    pf = p.updateMask(w.mask())

    reducer_common_parms = {
        "geometry": config.geometry_r,
        "crs": config.reduce_crs,
        "scale": config.reduce_scale,
        "maxPixels": 1e13,
        "tileScale": 4,
        "bestEffort": True
    }

    # Returns a dict like {"A00": 0.123, "A01": 0.456, ...}
    d_mean = pf.reduceRegion(
        reducer=ee.Reducer.mean(),
        **reducer_common_parms
    )
    
    props = {"region_label": config.region_label}
    
    # We rename the keys from A00...A63 to aef_1...aef_64 directly in the feature
    for i in range(64):
        gee_band = f"A{str(i).zfill(2)}"
        target_col = f"aef_{i+1}"
        props[target_col] = d_mean.getNumber(gee_band)

    return ee.Feature(None, ee.Dictionary(props))


def create_task(config: TaskConfig, mask_threshold_percent: float) -> ee.batch.Task:
    """Creates a GEE Export task for AEF (AlphaEarth Foundations) 64-band data."""
    
    # AEF has data from 2018 onwards
    start_year = int(config.date_from.split('-')[0])
    end_year = int(config.date_to.split('-')[0])
    
    start_year = max(2018, start_year)
    end_year = min(2025, end_year - 1)
    
    # If the user requested a year completely outside AEF bounds (e.g., 2026-2027),
    # we fall back to the global 2018-2025 average so geomerge still gets the columns.
    # This mimics extract_EO using the cached aef_avg.tif for future years
    if start_year > end_year:
        start_year = 2018
        end_year = 2025
        
    # Annual images are at 01-01T00:00:00Z, 
    ic = ee.ImageCollection(AEF_COLLECTION_ID).filterDate(f"{start_year}-01-01", f"{end_year}-01-02")
    
    avg_image = ic.mean()

    # ensure projection equals dataset projection to decrease reducers cost
    aef_proj = ee.Projection(
        ee.Algorithms.If(
            ic.size().gt(0), 
            ic.first().select("A00").projection(), 
            ee.Projection('EPSG:4326')
        )
    )
    
    config.geometry_r = config.geometry_ee.transform(aef_proj, ee.ErrorMargin(1))
    config.reduce_crs = aef_proj.crs()
    
    # Force scale to 5600m (0.05 degrees) to match local equivalents
    # and leverage GEE Image Pyramids for the 64-band reduction
    config.reduce_scale = 5600

    # We only want one row per region
    # So wrap the single average image in a single-feature FeatureCollection.
    feat = _aef_stats_feature(avg_image, config, mask_threshold_percent)
    col = ee.FeatureCollection([feat])

    # just region_label and 64 AEF bands
    selectors = ["region_label"] + [f"aef_{i+1}" for i in range(64)]

    task_desc = config.task_desc
    
    return ee.batch.Export.table.toCloudStorage(
        collection=col,
        description=task_desc[:100],
        bucket=config.export_bucket,
        fileNamePrefix=config.export_prefix,
        fileFormat="CSV",
        selectors=selectors,
    )
