from __future__ import annotations

import ee
from .common import TaskConfig

# --- Dataset Constants ---
VIIRS_COLLECTION_ID = "NASA/VIIRS/002/VNP09H1"

def _viirs_weekly_stats_feature(
    img: ee.Image,
    config: TaskConfig,
    mask_threshold_percent: float,
) -> ee.Feature:
    """
    One row per 8-day composite, aligned with geoextract geom_extract + arr_stats for VIIRS.
    """
    img = ee.Image(img)

    # 1. QA Masking
    # SurfReflect_State_500m bit 0-1: 00=clear, 01=cloudy, 10=mixed, 11=not set
    qa = img.select("SurfReflect_State_500m")
    cloud_state = qa.bitwiseAnd(3)
    # Allow 0 (Clear) or 3 (Not set, assumed clear)
    img = img.updateMask(cloud_state.eq(0).Or(cloud_state.eq(3)))

    # 2. Calculate NDVI
    # Band I1 (Red), Band I2 (NIR)
    # NDVI = (NIR - Red) / (NIR + Red)
    ndvi_raw = img.normalizedDifference([
        "SurfReflect_I2", 
        "SurfReflect_I1"
    ])
    
    # geomerge reverses "Mark's Scaling" ((NDVI * 200) + 50)
    # discuss it with ritvick
    ndvi = ndvi_raw.multiply(200).add(50).int16().rename("viirs")

    afi_thresh = ee.Number(float(mask_threshold_percent * 100))
    w_raw = ee.Image(config.cropmask_asset).float().clip(config.geometry_r)
    w = w_raw.updateMask(w_raw.gt(afi_thresh))

    p = (
        ndvi.float()
        .clip(config.geometry_r)
    )
    
    pf = p.updateMask(w.mask())

    reducer_common_parms = {
        "geometry": config.geometry_r,
        "crs": config.reduce_crs,
        "scale": config.reduce_scale,
        "maxPixels": 1e13,
        "tileScale": 4,
    }

    # Common stats: mean, min, max, median, count (valid after mask)
    pf_and_w = pf.addBands(w.rename("weight"))
    d_mean = pf_and_w.reduceRegion(
        reducer=ee.Reducer.mean().splitWeights(),
        **reducer_common_parms
    )
    
    # Combined min, max, median, and unweighted count
    d_stats = pf.reduceRegion(
        reducer=ee.Reducer.minMax()
            .combine(ee.Reducer.median().unweighted(), sharedInputs=True)
            .combine(ee.Reducer.count().unweighted(), sharedInputs=True),
        **reducer_common_parms
    )

    valid_after = d_stats.getNumber("viirs_count")
    mean_raw = d_mean.getNumber("mean")
    
    props = {
        "date": img.date().format("YYYY-MM-dd"),
        "region_label": config.region_label,
        "stats_mean": mean_raw,
        "stats_min": d_stats.get("viirs_min"),
        "stats_max": d_stats.get("viirs_max"),
        "stats_median": d_stats.get("viirs_median"),
        "stats_count": valid_after,
    }

    if config.include_audit:
        stack_audit = ee.Image.cat([
            ee.Image.constant(1).clip(config.geometry_r).rename("total_w"),
            ee.Image.constant(1).updateMask(p.mask()).rename("valid_p_w"),
            w.rename("w_all"),
            w.updateMask(pf.mask()).rename("w_used"),
            pf.pow(2).multiply(w).rename("p2w"),
        ]).float()

        d_audit = stack_audit.reduceRegion(
            reducer=ee.Reducer.sum().unweighted(),
            **reducer_common_parms
        )

        w_used_sum = d_audit.getNumber("w_used")
        sum_p2w = d_audit.getNumber("p2w")
        
        # Weighted Std Dev
        var_raw = ee.Number(
            ee.Algorithms.If(
                w_used_sum.gt(0),
                sum_p2w.divide(w_used_sum).subtract(mean_raw.pow(2)),
                0,
            )
        )
        std_mm = ee.Number(
            ee.Algorithms.If(
                w_used_sum.lte(0),
                None,
                ee.Algorithms.If(valid_after.gt(1), var_raw.max(0).sqrt(), 0),
            )
        )

        props.update({
            "counts_total": d_audit.getNumber("total_w"),
            "counts_valid_data": d_audit.getNumber("valid_p_w"),
            "counts_valid_data_after_masking": valid_after,
            "counts_weight_sum": d_audit.getNumber("w_all"),
            "counts_weight_sum_used": w_used_sum,
            "stats_std": std_mm,
        })

    return ee.Feature(None, ee.Dictionary(props))


def create_task(config: TaskConfig, mask_threshold_percent: float) -> ee.batch.Task:
    """Creates a GEE Export task for VIIRS NDVI data."""
    ic = (
        ee.ImageCollection(VIIRS_COLLECTION_ID)
        .filterDate(config.date_from, config.date_to)
        .filterBounds(config.geometry_ee)
    )

    # ensure projection equals dataset projection to decrease reducers cost
    viirs_proj = ee.Projection(
        ee.Algorithms.If(
            ic.size().gt(0), 
            ic.first().select("SurfReflect_I1").projection(), 
            ee.Projection('EPSG:4326')
        )
    )
    
    config.geometry_r = config.geometry_ee.transform(viirs_proj, ee.ErrorMargin(1))
    config.reduce_crs = viirs_proj.crs()
    # Force scale to 5600m (0.05 degrees) instead of nominalScale (500m).
    # matches resolution used locally, and instructs GEE to 
    # use Image Pyramids to reduce the cost
    config.reduce_scale = 5600

    col = ic.map(lambda im: _viirs_weekly_stats_feature(im, config, mask_threshold_percent))

    selectors = ["date", "region_label", "stats_mean", "stats_min", "stats_max", "stats_median", "stats_count"]
    if config.include_audit:
        selectors += [
            "counts_total", "counts_valid_data", "counts_valid_data_after_masking",
            "counts_weight_sum", "counts_weight_sum_used", "stats_std"
        ]

    task_desc = config.task_desc
    
    return ee.batch.Export.table.toCloudStorage(
        collection=col,
        description=task_desc[:100],
        bucket=config.export_bucket,
        fileNamePrefix=config.export_prefix,
        fileFormat="CSV",
        selectors=selectors,
    )
