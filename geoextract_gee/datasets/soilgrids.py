from __future__ import annotations

import ee
from .common import TaskConfig

def create_task(config: TaskConfig, mask_threshold_percent: float) -> ee.batch.Task:
    """Creates a GEE Export task for SoilGrids static variables."""
    depth_cm = getattr(config, 'depth_cm', 30)
    
    # Define weights based on standard SoilGrids layers
    if depth_cm == 30:
        weights = [5/30, 10/30, 15/30]
    elif depth_cm == 60:
        weights = [5/60, 10/60, 15/60, 30/60]
    elif depth_cm == 100:
        weights = [5/100, 10/100, 15/100, 30/100, 40/100]
    else:
        # Default to 30cm fallback
        weights = [5/30, 10/30, 15/30]
        
    depth_suffixes = ["0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm"]
    num_bands = len(weights)
    suffixes = depth_suffixes[:num_bands]
    
    vars_to_pull = ['sand', 'clay', 'soc', 'bdod']
    out_images = []
    
    for v in vars_to_pull:
        # Load the ISRIC asset
        img = ee.Image(f"projects/soilgrids-isric/{v}_mean")
        
        # Calculate the depth-weighted mean
        weighted_sum = None
        for suffix, w in zip(suffixes, weights):
            b_name = f"{v}_{suffix}_mean"
            b = img.select(b_name).multiply(w)
            if weighted_sum is None:
                weighted_sum = b
            else:
                weighted_sum = weighted_sum.add(b)
                
        # Rename to match exactly what geocif expects: "soil_sand", "soil_clay", etc.
        out_images.append(weighted_sum.rename(f"soil_{v}").float())
        
    # Combine into a single image stack
    final_img = ee.Image.cat(out_images)
    
    # Apply crop mask filtering
    afi_thresh = ee.Number(float(mask_threshold_percent * 100))
    fc_bounds = config.geometry_ee.bounds(1)
    
    w_raw = ee.Image(config.cropmask_asset).float().clip(fc_bounds)
    w = w_raw.updateMask(w_raw.gt(afi_thresh))
    
    final_masked = final_img.updateMask(w.mask())
    
    def process_region(feat):
        geom = feat.geometry()
        
        reducer_common_parms = {
            "crs": "EPSG:4326",
            "scale": 250, # Natively 250m grid
            "maxPixels": 1e9,
            "tileScale": 4,
            "bestEffort": True,
        }
        
        # Calculate mean over the masked region
        d_mean = final_masked.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom,
            **reducer_common_parms
        )
        
        # Create output row
        props = ee.Dictionary({
            "date": ee.String(config.date_from).slice(0, 10), # Typically YYYY-MM-DD
            "region_label": feat.get("region_label"),
            "soil_sand": d_mean.get("soil_sand"),
            "soil_clay": d_mean.get("soil_clay"),
            "soil_soc": d_mean.get("soil_soc"),
            "soil_bdod": d_mean.get("soil_bdod"),
        })
        
        return ee.Feature(None, props)
        
    col = ee.FeatureCollection(config.geometry_ee).map(process_region)
    
    selectors = ["date", "region_label", "soil_sand", "soil_clay", "soil_soc", "soil_bdod"]
    
    return ee.batch.Export.table.toCloudStorage(
        collection=col,
        description=config.task_desc[:100],
        bucket=config.export_bucket,
        fileNamePrefix=config.export_prefix,
        fileFormat="CSV",
        selectors=selectors,
    )
