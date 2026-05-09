from __future__ import annotations

import ee
from .common import TaskConfig

# --- Dataset Constants ---
CHIRPS_COLLECTION_ID = "UCSB-CHC/CHIRPS/V3/DAILY_SAT"

def _chirps_daily_stats_feature(
    img: ee.Image,
    config: TaskConfig,
    mask_threshold_percent: float,
) -> ee.Feature:
    """
    One row per day, aligned with geoextract geom_extract + arr_stats (chirps + AFI).
    """
    img = ee.Image(img)

    afi_thresh = ee.Number(float(mask_threshold_percent * 100))
    w_raw = ee.Image(config.cropmask_asset).float().clip(config.geometry_r)
    w = w_raw.updateMask(w_raw.gt(afi_thresh))

    p = (
        img.select("precipitation")
        .float()
        .multiply(100)
        .int16()
        .divide(100)
        .clip(config.geometry_r) # check all touched vs centroid
    )
    pf = p.updateMask(p.gte(0)).updateMask(w.mask())

    reducer_common_parms = {
        "geometry": config.geometry_r,
        "crs": config.reduce_crs,
        "scale": config.reduce_scale,
        "maxPixels": 1e9,
        "tileScale": 4,
        "bestEffort": True,
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

    valid_after = d_stats.getNumber("precipitation_count")
    mean_raw = d_mean.getNumber("mean")
    
    props = {
        "date": img.date().format("YYYY-MM-dd"),
        "region_label": config.region_label,
        "stats_mean": mean_raw,
        "stats_min": d_stats.get("precipitation_min"),
        "stats_max": d_stats.get("precipitation_max"),
        "stats_median": d_stats.get("precipitation_median"),
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
        
        # Weighted Std Dev logic
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
    """Creates a GEE Export task for CHIRPS data."""
    ic = (
        ee.ImageCollection(CHIRPS_COLLECTION_ID)
        .filterDate(config.date_from, config.date_to)
        .filterBounds(config.geometry_ee)
    )

    # ensure projection equals dataset projection to decrease reducers cost
    chirps_proj = ee.Projection(
        ee.Algorithms.If(
            ic.size().gt(0), 
            ic.first().select("precipitation").projection(), 
            ee.Projection('EPSG:4326')
        )
    )
    
    config.geometry_r = config.geometry_ee.transform(chirps_proj, ee.ErrorMargin(1))
    config.reduce_crs = chirps_proj.crs()
    config.reduce_scale = chirps_proj.nominalScale()

    col = ic.map(lambda im: _chirps_daily_stats_feature(im, config, mask_threshold_percent))

    selectors = ["date", "region_label", "stats_mean", "stats_min", "stats_max", "stats_median", "stats_count"]
    if config.include_audit:
        selectors += [
            "counts_total", "counts_valid_data", "counts_valid_data_after_masking",
            "counts_weight_sum", "counts_weight_sum_used", "stats_std"
        ]

    # GEE task description only allows letters, numbers, -, and _
    # task_desc is now centralized in TaskConfig
    task_desc = config.task_desc
    
    return ee.batch.Export.table.toCloudStorage(
        collection=col,
        description=task_desc[:100],
        bucket=config.export_bucket,
        fileNamePrefix=config.export_prefix,
        fileFormat="CSV",
        selectors=selectors,
    )