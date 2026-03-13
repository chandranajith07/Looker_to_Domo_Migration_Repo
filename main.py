import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional

# Import your custom logic (Keep these files as they are)
from looker_to_unified_schema import transform_looker_to_unified, LookerClient
from domo_adapter import DomoAdapter
from dataset_resolver import StaticDatasetResolver

app = FastAPI(title="Looker to Domo Migration Payload Generator")

# Enable CORS for Domo environment
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# --- Helper Class: Config Collector ---
# This class "tricks" the DomoAdapter into giving us the JSON 
# instead of actually trying to send it to Domo.
class ConfigCollector:
    def __init__(self):
        self.configs = []

    def create_card(self, page_id, payload):
        """Instead of calling an API, we just save the payload to a list."""
        self.configs.append(payload)
        return {"id": "pending_deployment"}

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
    dataset_mapping: Dict[str, str]
    selected_visual_ids: Optional[List[str]] = None
    # NOTE: domo_cookie and domo_csrf are REMOVED as they are no longer needed.

# --- Endpoints ---

@app.post("/looker/dashboards")
async def list_dashboards(req: LookerAuthRequest):
    """Fetches all dashboards from Looker for the dropdown list."""
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
    """Provides the data for the table on Page 3 (Titles, Types, Calcs)."""
    try:
        unified_schema = transform_looker_to_unified(
            dashboard_id=req['looker_id'],
            client_id=req['looker_client_id'],
            client_secret=req['looker_client_secret'],
            base_url=req['looker_url']
        )
        return {
            "dashboard_name": unified_schema['source']['dashboardName'],
            "visuals": unified_schema['pages'][0]['visuals'], 
            "looker_datasets": unified_schema["datasets"],
            "calculatedFields": unified_schema.get("calculatedFields", [])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get-migration-payloads")
async def get_migration_payloads(req: MigrationRequest):
    """
    NEW ENDPOINT: 
    Converts Looker charts into Domo JSON 'configs'.
    Does not call Domo. Returns configs to the React Frontend.
    """
    try:
        # 1. Generate Unified Schema from Looker
        unified_schema = transform_looker_to_unified(
            dashboard_id=req.looker_id,
            client_id=req.looker_client_id,
            client_secret=req.looker_client_secret,
            base_url=req.looker_url
        )

        # 2. Filter visuals to only the CHECKED boxes in the UI
        if req.selected_visual_ids is not None:
            selected_set = set(req.selected_visual_ids)
            for page in unified_schema.get("pages", []):
                page["visuals"] = [
                    v for v in page.get("visuals", [])
                    if v.get("id") in selected_set
                ]

        # 3. Setup Dataset Resolver (maps Looker views to Domo UUIDs)
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

        # 4. Generate the Card JSON Payloads
        # We use ConfigCollector instead of DomoClient to capture the JSON
        collector = ConfigCollector()
        adapter = DomoAdapter(collector, resolver, column_mapping={})
        
        # This will populate collector.configs
        adapter.deploy_dashboard(unified_schema, req.domo_page_id)

        # 5. Return the configs to the Frontend
        return {
            "status": "success",
            "domo_page_id": req.domo_page_id,
            "card_configs": collector.configs
        }

    except Exception as e:
        print(f"Error generating payloads: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    # Make sure port 8000 is open on Render
    uvicorn.run(app, host="0.0.0.0", port=8000)
