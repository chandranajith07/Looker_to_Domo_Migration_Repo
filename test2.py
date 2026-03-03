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

# Enable CORS so React (port 3000) can talk to Python (port 8000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
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
    # Mapping of Looker View Name -> Domo Dataset ID
    # Example: {"rpt_supply_finus_deals": "3ac9801f-82e3-44da-a222-5ac175eea8ed"}
    dataset_mapping: Dict[str, str]

# --- Endpoints ---

@app.post("/looker/dashboards")
async def list_dashboards(req: LookerAuthRequest):
    """
    Page 1 -> Page 2 Transition:
    Authenticates with Looker and returns a list of all available dashboards.
    """
    try:
        # Initialize Looker Client to get token
        client = LookerClient(req.looker_client_id, req.looker_client_secret, req.looker_url)
        if not client.token:
            raise HTTPException(status_code=401, detail="Looker Authentication Failed")

        # Fetch Dashboards from Looker API
        url = f"{req.looker_url.rstrip('/')}/api/4.0/dashboards"
        headers = {'Authorization': f'token {client.token}'}
        
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        
        dashboards = response.json()
        print(dashboards)
        
        # Return simplified list for the UI table
        return [{"id": str(d["id"]), "title": d["title"]} for d in dashboards]

    except Exception as e:
        print(f"❌ Error fetching dashboards: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/migrate")
async def run_migration(req: MigrationRequest):
    """
    Page 3: Executes the actual migration.
    1. Converts Looker Dashboard to Unified Schema.
    2. Resolves Dataset IDs dynamically.
    3. Deploys cards to Domo.
    """
    try:
        print(f"🚀 Starting migration for Looker Dashboard ID: {req.looker_id}")

        # 1. Convert Looker -> Unified Schema (In-Memory)
        unified_schema = transform_looker_to_unified(
            dashboard_id=req.looker_id,
            client_id=req.looker_client_id,
            client_secret=req.looker_client_secret,
            base_url=req.looker_url
        )

        # 2. Setup Domo Client
        domo_headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "x-requested-with": "XMLHttpRequest",
            "x-csrf-token": req.domo_csrf,
            "Cookie": req.domo_cookie
        }
        # Note: You can also make the domo base url dynamic if needed
        domo_client = DomoClient(base_url="https://gwcteq-partner.domo.com", headers=domo_headers)

        # 3. Dynamic Dataset Resolution
        # Looker generates 'dataset_1', 'dataset_2' etc internally.
        # We must map these internal IDs to the real Domo IDs provided in req.dataset_mapping
        dynamic_resolver_map = {}
        
        for ds in unified_schema.get("datasets", []):
            internal_id = ds["id"]    # e.g., "dataset_1"
            view_name = ds["name"]    # e.g., "rpt_supply_finus_deals"
            
            # Map based on the View Name provided by user in the React form
            domo_uuid = req.dataset_mapping.get(view_name)
            
            if domo_uuid:
                dynamic_resolver_map[internal_id] = domo_uuid
                print(f"🔗 Linked View '{view_name}' to Domo UUID: {domo_uuid}")
            else:
                print(f"⚠️ Warning: No mapping provided for Looker View '{view_name}'")

        if not dynamic_resolver_map:
            raise Exception("No valid dataset mappings were found. Migration cannot proceed.")

        resolver = StaticDatasetResolver(dynamic_resolver_map)

        # 4. Deploy to Domo using the Adapter
        adapter = DomoAdapter(domo_client, resolver, column_mapping={})
        results = adapter.deploy_dashboard(
            unified_schema=unified_schema,
            page_id=req.domo_page_id
        )

        success_count = sum(1 for r in results if r["status"] == "SUCCESS")
        print(f"✅ Migration finished. {success_count}/{len(results)} visuals deployed.")

        return {
            "status": "success",
            "dashboard_name": unified_schema["source"]["dashboardName"],
            "total_visuals": len(results),
            "successful": success_count,
            "results": results
        }

    except Exception as e:
        print(f"❌ Migration Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    # Start the server
    print("🔥 Starting API Server on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)