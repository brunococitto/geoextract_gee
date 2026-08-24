# Future Improvements & Ideas

## Hybrid GEE / Local Extraction Pipeline
**Idea**: When running with `use_gee = True`, automatically route variables that lack a Google Earth Engine handler (e.g., `chirts_era5_tmax`, `etref`) to the local tiles backend (`geoprepare.extract.extract_EO`).

### Current Challenge
Right now, if `extract_EO_gee.py` encounters a variable it doesn't support, it simply skips it. While it's easy to add a dynamic Python fallback (e.g., `getattr(extract_EO, f"process_{var}")`), this fails in practice due to **data availability**. 
The local extraction functions expect the raw `.tif` tiles to already exist in the `GEO/intermed/` directory. However, when the pipeline is configured for GEE, the `geodownload` module is entirely skipped. Thus, the local fallback extraction attempts to run, finds no `.tif` tiles, and fails.

### Proposed Solution
To build a true hybrid system:
1. Modify the entrypoint (`run_geoextract.py`) to parse the `eo_model` list from the config before anything else.
2. Check each variable against the available GEE handlers and split the list into "GEE Variables" and "Local Variables".
3. Trigger the `geodownload` pipeline **only** for the "Local Variables" to fetch the necessary `.tif` files into `GEO/intermed/`.
4. Run the extraction loop: GEE variables get dispatched to the asynchronous `extract_EO_gee.py`, while Local Variables get routed to the synchronous `extract_EO.py`.


