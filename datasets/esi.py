from __future__ import annotations

import ee
from .common import TaskConfig

def _esi_weekly_stats_feature_batch(
    img: ee.Image,
    config: TaskConfig,
    mask_threshold_percent: float,
) -> ee.FeatureCollection:
    """
    One row per week per region, aligned with geoextract geom_extract + arr_stats for ESI.
    """
    img = ee.Image(img)
    
    # config.var will be 'esi_4wk' or 'esi_12wk'. 
    # But in the EE the band is simply named 'ESI' in both collections
    band_name = "ESI"

    afi_thresh = ee.Number(float(mask_threshold_percent * 100))
    fc_bounds = config.geometry_r.geometry().bounds()
    w_raw = ee.Image(config.cropmask_asset).float().clip(fc_bounds)
    w = w_raw.updateMask(w_raw.gt(afi_thresh))

    p = (
        img.select(band_name)
        .float()
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

        # Common stats: mean, min, max, median, count (valid after mask)
        d_mean = pf_and_w.reduceRegion(
            reducer=ee.Reducer.mean().splitWeights(),
            geometry=geom,
            **reducer_common_parms
        )
        
        # Combined min, max, median, and unweighted count
        d_stats = pf.reduceRegion(
            reducer=ee.Reducer.minMax()
                .combine(ee.Reducer.median().unweighted(), sharedInputs=True)
                .combine(ee.Reducer.count().unweighted(), sharedInputs=True),
            geometry=geom,
            **reducer_common_parms
        )

        valid_after = d_stats.getNumber(f"{band_name}_count")
        mean_raw = d_mean.getNumber("mean")
        
        props = ee.Dictionary({
            "date": img.date().format("YYYY-MM-dd"),
            "region_label": feat.get("region_label"),
            "region_id": feat.get("region_id"),
            "stats_mean": mean_raw,
            "stats_min": d_stats.get(f"{band_name}_min"),
            "stats_max": d_stats.get(f"{band_name}_max"),
            "stats_median": d_stats.get(f"{band_name}_median"),
            "stats_count": valid_after,
        })

        if config.include_audit:
            d_audit = stack_audit.reduceRegion(
                reducer=ee.Reducer.sum().unweighted(),
                geometry=geom,
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
    """Creates a GEE Export task for ESI (Evaporative Stress Index) data."""
    suffix = config.var.split('_')[1]
    collection_id = f"projects/climate-engine/esi/{suffix}"
    band_name = "ESI"
    
    ic = (
        ee.ImageCollection(collection_id)
        .filterDate(config.date_from, config.date_to)
        .filterBounds(config.geometry_ee)
    )

    # ensure projection equals dataset projection to decrease reducers cost
    esi_proj = ee.Projection(
        ee.Algorithms.If(
            ic.size().gt(0), 
            ic.first().select(band_name).projection(), 
            ee.Projection('EPSG:4326')
        )
    )
    
    config.geometry_r = config.geometry_ee.transform(esi_proj, ee.ErrorMargin(1))
    config.reduce_crs = esi_proj.crs()
    config.reduce_scale = esi_proj.nominalScale()

    col = ic.map(lambda im: _esi_weekly_stats_feature_batch(im, config, mask_threshold_percent)).flatten()

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
