from __future__ import annotations

import ee
from .common import TaskConfig

# --- Dataset Constants ---
NSIDC_COLLECTION_ID = "NASA/SMAP/SPL4SMGP/008"

def _nsidc_daily_stats_feature_batch(
    img: ee.Image,
    config: TaskConfig,
    mask_threshold_percent: float,
) -> ee.FeatureCollection:
    """
    One row per day per region, aligned with geoextract geom_extract + arr_stats for NSIDC.
    """
    img = ee.Image(img)
    
    # Geoprepare uses 'nsidc_surface' but GEE band is 'sm_surface'
    band_name = "sm_surface" if "surface" in config.var else "sm_rootzone"

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
    """Creates a GEE Export task for NSIDC (SMAP L4) data."""
    ic = (
        ee.ImageCollection(NSIDC_COLLECTION_ID)
        .filterDate(config.date_from, config.date_to)
        .filterBounds(config.geometry_ee)
    )

    band_name = "sm_surface" if "surface" in config.var else "sm_rootzone"

    # ensure projection equals dataset projection to decrease reducers cost
    nsidc_proj = ee.Projection(
        ee.Algorithms.If(
            ic.size().gt(0), 
            ic.first().select(band_name).projection(), 
            ee.Projection('EPSG:4326')
        )
    )
    
    config.geometry_r = config.geometry_ee.transform(nsidc_proj, ee.ErrorMargin(1))
    config.reduce_crs = nsidc_proj.crs()
    config.reduce_scale = nsidc_proj.nominalScale()

    # NSIDC SMAP L4 is 3-hourly (8 images per day). 
    # We MUST average them to daily to match geoprepare expectations.
    def make_daily(day_offset):
        start = ee.Date(config.date_from).advance(day_offset, 'day')
        end = start.advance(1, 'day')
        
        # Average the 3-hourly images for this day
        daily_mean = ic.filterDate(start, end).select(band_name).mean()
        
        # Set system:time_start so img.date() works, and band_count to track empty images
        return daily_mean.set({
            'system:time_start': start.millis(),
            'band_count': daily_mean.bandNames().size()
        })

    n_days = ee.Date(config.date_to).difference(ee.Date(config.date_from), 'day')
    daily_ic = ee.ImageCollection(
        ee.List.sequence(0, n_days.subtract(1)).map(make_daily)
    )
    
    # Drop days with no data (e.g. sensor outages or future dates) before applying selectors
    daily_ic = daily_ic.filter(ee.Filter.gt('band_count', 0))

    col = daily_ic.map(lambda im: _nsidc_daily_stats_feature_batch(im, config, mask_threshold_percent)).flatten()

    # We need to filter out features where the count is 0 (e.g. days with no data)
    col = col.filter(ee.Filter.gt("stats_count", 0))

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
