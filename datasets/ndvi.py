from __future__ import annotations

import ee
from .common import TaskConfig

# --- Dataset Constants ---
NDVI_COLLECTION_ID = "MODIS/061/MOD09CMG"

def _ndvi_daily_stats_feature_batch(
    img: ee.Image,
    config: TaskConfig,
    mask_threshold_percent: float,
) -> ee.FeatureCollection:
    """
    One row per day per region, aligned with geoextract geom_extract + arr_stats for NDVI.
    Uses strict QA for high-quality daily observations.
    """
    img = ee.Image(img)

    # 1. QA Masking similar to octvi ranking logic
    state_qa = img.select("Coarse_Resolution_State_QA")
    
    # Snow (Bits 12 or 15)
    snow = state_qa.bitwiseAnd(4096).gt(0).Or(state_qa.bitwiseAnd(32768).gt(0))
    
    # High Aerosol (Bits 6 and 7 == 11) -> 192 in decimal
    high_aerosol = state_qa.bitwiseAnd(192).eq(192)
    
    # Cloud Shadow (Bit 2)
    shadow = state_qa.bitwiseAnd(4).gt(0)
    
    # Internal Cloud Algorithm (Bit 10)
    cloud_int = state_qa.bitwiseAnd(1024).gt(0)
    
    # Combine masks: keep pixels where NONE of these bad conditions are true
    bad_pixels = snow.Or(high_aerosol).Or(shadow).Or(cloud_int)
    img = img.updateMask(bad_pixels.Not())

    # 2. Calculate NDVI
    # Band 1 (Red), Band 2 (NIR)
    # NDVI = (NIR - Red) / (NIR + Red)
    ndvi_raw = img.normalizedDifference([
        "Coarse_Resolution_Surface_Reflectance_Band_2", 
        "Coarse_Resolution_Surface_Reflectance_Band_1"
    ])
    
    # geomerge.py explicitly reverses "Mark's Scaling" ((NDVI * 200) + 50) on the local side.
    ndvi = ndvi_raw.multiply(200).add(50).int16().rename("ndvi")

    afi_thresh = ee.Number(float(mask_threshold_percent * 100))
    fc_bounds = config.geometry_r.geometry().bounds()
    w_raw = ee.Image(config.cropmask_asset).float().clip(fc_bounds)
    w = w_raw.updateMask(w_raw.gt(afi_thresh))

    p = (
        ndvi.float()
        .clip(fc_bounds)
    )
    
    pf = p.updateMask(w.mask())
    pf_and_w = pf.addBands(w.rename("weight"))

    if config.include_audit:
        stack_audit = ee.Image.cat([
            ee.Image.constant(1).clip(fc_bounds).rename("total_w"),
            ee.Image.constant(1).updateMask(p.mask()).rename("valid_p_w"),
            w.rename("w_all"),
            w.updateMask(pf.mask()).rename("w_used"),
            pf.pow(2).multiply(w).rename("p2w"),
        ]).float()
    else:
        stack_audit = None

    reducer_common_parms = {
        "crs": config.reduce_crs,
        "scale": config.reduce_scale,
        "maxPixels": 1e9,
        "tileScale": 4,
        "bestEffort": True,
    }

    def _process_region(feat):
        geom = feat.geometry()

        d_mean = pf_and_w.reduceRegion(
            reducer=ee.Reducer.mean().splitWeights(),
            geometry=geom,
            **reducer_common_parms
        )
        
        d_total_mask = w.rename("weight").reduceRegion(
            reducer=ee.Reducer.count().unweighted(),
            geometry=geom,
            **reducer_common_parms
        )
        total_mask_pixels = ee.Number(d_total_mask.get("weight"))
        
        d_stats = pf.reduceRegion(
            reducer=ee.Reducer.minMax()
                .combine(ee.Reducer.median().unweighted(), sharedInputs=True)
                .combine(ee.Reducer.count().unweighted(), sharedInputs=True),
            geometry=geom,
            **reducer_common_parms
        )

        valid_after = ee.Number(d_stats.get("ndvi_count"))
        
        mean_raw = d_mean.get("mean")
        
        props = ee.Dictionary({
            "date": img.date().format("YYYY-MM-dd"),
            "region_label": feat.get("region_label"),
            "region_id": feat.get("region_id"),
            "stats_mean": mean_raw,
            "stats_min": d_stats.get("ndvi_min"),
            "stats_max": d_stats.get("ndvi_max"),
            "stats_median": d_stats.get("ndvi_median"),
            "stats_count": valid_after,
            "total_mask_pixels": total_mask_pixels,
        })
        
        if config.include_audit:
            d_audit = stack_audit.reduceRegion(
                reducer=ee.Reducer.sum().unweighted(),
                geometry=geom,
                **reducer_common_parms
            )

            w_used_sum = d_audit.getNumber("w_used")
            sum_p2w = d_audit.getNumber("p2w")
            
            mean_num = ee.Number(mean_raw)
            var_raw = ee.Number(
                ee.Algorithms.If(
                    w_used_sum.gt(0),
                    sum_p2w.divide(w_used_sum).subtract(mean_num.pow(2)),
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

            props = props.combine(ee.Dictionary({
                "counts_total": d_audit.getNumber("total_w"),
                "counts_valid_data": d_audit.getNumber("valid_p_w"),
                "counts_valid_data_after_masking": valid_after,
                "counts_weight_sum": d_audit.getNumber("w_all"),
                "counts_weight_sum_used": w_used_sum,
                "stats_std": std_mm,
            }))

        return ee.Feature(None, props)

    return ee.FeatureCollection(config.geometry_r).map(_process_region)


def create_task(config: TaskConfig, mask_threshold_percent: float) -> ee.batch.Task:
    """Creates a GEE Export task for NDVI data."""
    ic = (
        ee.ImageCollection(NDVI_COLLECTION_ID)
        .filterDate(config.date_from, config.date_to)
        .filterBounds(config.geometry_ee)
    )

    # ensure projection equals dataset projection to decrease reducers cost
    ndvi_proj = ee.Projection(
        ee.Algorithms.If(
            ic.size().gt(0), 
            ic.first().select("Coarse_Resolution_Surface_Reflectance_Band_1").projection(), 
            ee.Projection('EPSG:4326')
        )
    )
    
    config.geometry_r = config.geometry_ee.transform(ndvi_proj, ee.ErrorMargin(1))
    config.reduce_crs = ndvi_proj.crs()
    config.reduce_scale = ndvi_proj.nominalScale()

    col = ic.map(lambda im: _ndvi_daily_stats_feature_batch(im, config, mask_threshold_percent)).flatten()

    selectors = ["date", "region_label", "stats_mean", "stats_min", "stats_max", "stats_median", "stats_count", "total_mask_pixels"]
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
