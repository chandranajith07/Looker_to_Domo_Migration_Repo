import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Any, List

from looker_to_unified_schema import transform_looker_to_unified, LookerClient
from domo_adapter import DomoAdapter
from dataset_resolver import StaticDatasetResolver

app = FastAPI(title="Looker to Domo Payload API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

class LookerAuthRequest(BaseModel):
    looker_url: str
    looker_client_id: str
    looker_client_secret: str

class PayloadRequest(BaseModel):
    looker_url: str
    looker_id: str
    looker_client_id: str
    looker_client_secret: str
    dataset_mapping: Dict[str, str]

@app.options("/{full_path:path}")
async def preflight_handler(request: Request, full_path: str):
    response = JSONResponse(content="OK")
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response

@app.post("/looker/dashboards")
async def list_dashboards(req: LookerAuthRequest):
    try:
        client = LookerClient(req.looker_client_id, req.looker_client_secret, req.looker_url)
        url = f"{req.looker_url.rstrip('/')}/api/4.0/dashboards"
        res = requests.get(url, headers={'Authorization': f'token {client.token}'}, timeout=60)
        return [{"id": str(d["id"]), "title": d["title"]} for d in res.json()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/looker/preview")
async def preview_dashboard(req: Dict[str, Any]):
    try:
        unified = transform_looker_to_unified(req['looker_id'], req['looker_client_id'], req['looker_client_secret'], req['looker_url'])
        return {"dashboard_name": unified['source']['dashboardName'], "visuals": unified['pages'][0]['visuals'], "looker_datasets": unified['datasets']}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/prepare-payloads")
async def prepare_payloads(req: PayloadRequest):
    """Generates Domo JSON payloads for React to deploy via browser session."""
    try:
        unified = transform_looker_to_unified(req.looker_id, req.looker_client_id, req.looker_client_secret, req.looker_url)
        
        # Bridge the View Names to the provided Domo IDs
        dynamic_map = {}
        for ds in unified['datasets']:
            domo_uuid = req.dataset_mapping.get(ds['name'])
            if domo_uuid: dynamic_map[ds['id']] = domo_uuid
        
        resolver = StaticDatasetResolver(dynamic_map)
        adapter = DomoAdapter(domo_client=None, dataset_resolver=resolver)
        
        payloads = adapter.get_all_payloads(unified)
        return {"status": "success", "payloads": payloads}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
