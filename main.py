import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional

# Import your custom logic
from looker_to_unified_schema import transform_looker_to_unified, LookerClient
from domo_client import DomoClient
from domo_adapter import DomoAdapter
from dataset_resolver import StaticDatasetResolver

app = FastAPI(title="Looker to Domo Migration API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# --- Request Models ---

class LookerAuthRequest(BaseModel):
    looker_url: str
    looker_client_id: str
    looker_client_secret: str

class MigrationRequest(BaseModel):
    looker_url: str
    looker_id: str
    looker_client_id: str
    looker_client_secret: str
    domo_page_id: str
    domo_cookie: str
    domo_csrf: str
    dataset_mapping: Dict[str, str]

# --- Endpoints ---

@app.post("/looker/dashboards")
async def list_dashboards(req: LookerAuthRequest):
    """Page 1 -> Page 2: Fetches all dashboards from Looker."""
    try:
        client = LookerClient(req.looker_client_id, req.looker_client_secret, req.looker_url)
        if not client.token:
            raise HTTPException(status_code=401, detail="Looker Authentication Failed")

        url = f"{req.looker_url.rstrip('/')}/api/4.0/dashboards"
        headers = {'Authorization': f'token {client.token}'}
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        
        return [{"id": str(d["id"]), "title": d["title"]} for d in response.json()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/looker/preview")
async def preview_dashboard(req: Dict[str, str]):
    """Page 2 -> Page 3: Fetches specific dashboard metadata and detected views."""
    try:
        unified_schema = transform_looker_to_unified(
            dashboard_id=req['looker_id'],
            client_id=req['looker_client_id'],
            client_secret=req['looker_client_secret'],
            base_url=req['looker_url']
        )
        views = [ds['name'] for ds in unified_schema.get('datasets', [])]
        return {
            "dashboard_name": unified_schema['source']['dashboardName'],
            "views": views
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/migrate")
async def run_migration(req: MigrationRequest):
    """Page 3: Executes the migration cards to Domo."""
    try:
        unified_schema = transform_looker_to_unified(
            dashboard_id=req.looker_id,
            client_id=req.looker_client_id,
            client_secret=req.looker_client_secret,
            base_url=req.looker_url
        )

        domo_headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "x-requested-with": "XMLHttpRequest",
            "x-csrf-token": req.domo_csrf,
            "Cookie": req.domo_cookie
        }
        domo_client = DomoClient(base_url="https://gwcteq-partner.domo.com", headers=domo_headers)

        dynamic_resolver_map = {}
        for ds in unified_schema.get("datasets", []):
            internal_id = ds["id"]
            view_name = ds["name"]
            domo_uuid = req.dataset_mapping.get(view_name)
            if domo_uuid:
                dynamic_resolver_map[internal_id] = domo_uuid

        if not dynamic_resolver_map:
            raise Exception("No valid dataset mappings were found.")

        resolver = StaticDatasetResolver(dynamic_resolver_map)
        adapter = DomoAdapter(domo_client, resolver, column_mapping={})
        results = adapter.deploy_dashboard(unified_schema, req.domo_page_id)

        return {
            "status": "success",
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

