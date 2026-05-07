from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import ee
from google.oauth2 import service_account

import os

def init_ee(params) -> None:
    # Initialize Earth Engine using settings from config or defaults.
    
    # Path relative to metadata folder as specified in config
    gee_key = params.parser.get("DEFAULT", "gee_key")
    secret_path = params.dir_metadata / gee_key
    
    project_id = params.parser.get("DEFAULT", "gee_project")

    if not secret_path.is_file():
        raise RuntimeError(f"GEE secret key not found at {secret_path}")

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(secret_path)

    info = json.loads(secret_path.read_text(encoding="utf-8"))
    credentials = service_account.Credentials.from_service_account_info(
        info,
        # We need cloud platform for Google Cloud Storage export
        scopes=["https://www.googleapis.com/auth/earthengine", "https://www.googleapis.com/auth/cloud-platform"],
    )
    ee.Initialize(credentials=credentials, project=project_id)
    params.logger.info(f"Earth Engine initialized with project: {project_id}")

class TaskConfig:
    def __init__(
        self,
        country: str,
        crop: str,
        scale: str,
        var: str,
        year: int,
        region_label: str,
        region_id: str,
        geometry_ee: ee.Geometry,
        date_from: str,
        date_to: str,
        export_bucket: str,
        export_prefix: str,
        cropmask_asset: str,
        include_audit: bool = True,
    ):
        self.country = country
        self.crop = crop
        self.scale = scale
        self.var = var
        self.year = year
        self.region_label = region_label
        self.region_id = region_id
        self.geometry_ee = geometry_ee
        self.date_from = date_from
        self.date_to = date_to
        self.export_bucket = export_bucket
        self.export_prefix = export_prefix
        self.cropmask_asset = cropmask_asset
        self.include_audit = include_audit
        
        self.prj_name = self.export_prefix.split('/')[1] if self.export_prefix.startswith('gee_extract/') else 'unknown'
        self.task_desc = f"{self.prj_name}_{self.country}_{self.var}_{self.year}_{self.region_label}_{self.crop}"

        # Internal placeholders for projection-aware reduction
        self.geometry_r = None
        self.reduce_crs = None
        self.reduce_scale = None
