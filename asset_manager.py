import hashlib
import time
import uuid
from pathlib import Path

import ee
from google.cloud import storage

def get_file_md5(file_path: Path) -> str:
    """Calculates the MD5 hash of a local file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def check_asset_exists_and_matches(asset_id: str, local_md5: str) -> bool:
    """Checks if an EE asset exists and has a matching local_md5 property."""
    try:
        asset = ee.data.getAsset(asset_id)
        asset_md5 = asset.get('properties', {}).get('local_md5')
        return asset_md5 == local_md5
    except ee.ee_exception.EEException:
        # Asset doesn't exist or we don't have access
        return False

def upload_to_gcs(local_path: Path, bucket_name: str, logger) -> str:
    """Uploads a local file to GCS and returns the gs:// URI."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    # Put them in a staging folder so they don't clutter the export directory
    dest_blob_name = f"gee_staging/{local_path.name}"
    blob = bucket.blob(dest_blob_name)
    
    logger.info(f"Uploading {local_path.name} to gs://{bucket_name}/{dest_blob_name}...")
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{dest_blob_name}"

def start_ingestion(gcs_uri: str, asset_id: str, local_md5: str):
    """Triggers an Earth Engine ingestion task from a GCS URI."""
    manifest = {
        "name": asset_id,
        "tilesets": [
            {"id": "tileset_1", "sources": [{"uris": [gcs_uri]}]}
        ],
        "properties": {
            "local_md5": local_md5
        }
    }
    request_id = str(uuid.uuid4())
    ee.data.startIngestion(request_id, manifest)

def sync_cropmasks(params, unique_paths: set) -> dict:
    """
    Synchronizes local TIFF cropmasks with Earth Engine.
    Returns a dict mapping the local Path string to the final GEE Asset ID.
    """
    params.logger.info(f"Synchronizing {len(unique_paths)} unique cropmasks with Earth Engine...")
    
    project_name = params.parser.get("DEFAULT", "gee_project")
    bucket_name = params.parser.get("DEFAULT", "gee_bucket")
    
    # 1. Ensure the parent folder exists in GEE
    folder_id = f"projects/{project_name}/assets/geoextract"
    try:
        ee.data.createAsset({'type': 'FOLDER'}, folder_id)
        params.logger.info(f"Created new GEE folder: {folder_id}")
    except ee.ee_exception.EEException:
        pass # Already exists
        
    cropmask_map = {}
    pending_ingestions = []
    
    # 2. Check and stage files
    for path in unique_paths:
        path_obj = Path(path)
        asset_name = path_obj.stem # Removes the path and .tif extension
        asset_id = f"{folder_id}/{asset_name}"
        cropmask_map[str(path)] = asset_id
        
        if not path_obj.exists():
            params.logger.warning(f"Cropmask file not found locally: {path_obj}")
            continue
            
        md5 = get_file_md5(path_obj)
        
        # Check if it already exists in GEE and matches our hash
        if check_asset_exists_and_matches(asset_id, md5):
            params.logger.debug(f"Asset {asset_name} already up to date in GEE.")
            continue
            
        # It needs to be uploaded
        gcs_uri = upload_to_gcs(path_obj, bucket_name, params.logger)
        
        params.logger.info(f"Triggering ingestion for {asset_name}...")
        start_ingestion(gcs_uri, asset_id, md5)
        pending_ingestions.append(asset_id)
        
    # 3. Block and wait for ingestions to finish
    if pending_ingestions:
        params.logger.info(f"Waiting for {len(pending_ingestions)} cropmasks to finish ingesting...")
        
        while True:
            ops = ee.data.listOperations()
            active_ingestions = 0
            has_failures = False
            
            for op in ops:
                metadata = op.get('metadata', {})
                state = metadata.get('state')
                
                # Check if it's an image ingestion task
                if metadata.get('type') == 'INGEST_IMAGE':
                    if state in ['PENDING', 'RUNNING']:
                        active_ingestions += 1
                    elif state == 'FAILED':
                        # Optionally check if it's one of ours, but usually safe to log
                        params.logger.error(f"An ingestion failed: {op.get('name')}")
                        has_failures = True
                        
            if active_ingestions == 0:
                params.logger.info("All cropmask ingestions have completed.")
                break
                
            params.logger.info(f"{active_ingestions} assets still ingesting. Waiting 30s...")
            time.sleep(30)
            
    return cropmask_map
