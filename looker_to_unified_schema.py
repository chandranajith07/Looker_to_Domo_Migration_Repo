import json
import requests
import traceback
from typing import Dict, Any, Optional, List

class LookerClient:
    def __init__(self, client_id: str, client_secret: str, base_url: str):
        self.session = requests.Session()
        self.base_url = base_url.rstrip('/')
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = self._get_token()
        self.explore_cache = {} 

    def _get_token(self):
        """Authenticates with Looker API and returns an access token."""
        auth_url = f"{self.base_url}/api/4.0/login"
        payload = {'client_id': self.client_id, 'client_secret': self.client_secret}
        try:
            res = self.session.post(auth_url, data=payload, timeout=10)
            res.raise_for_status()
            return res.json().get('access_token')
        except Exception as e:
            print(f"❌ Looker API Authentication Failed: {e}")
            return None

    def _normalize_agg(self, looker_agg: str) -> str:
        """Maps Looker technical aggregations to standard BI aggregations."""
        mapping = {
            "SUM_DISTINCT": "SUM",
            "AVERAGE_DISTINCT": "AVERAGE",
            "COUNT_DISTINCT": "COUNT_DISTINCT",
            "LIST": "COUNT",
            "YESNO": "SUM"
        }
        upper_agg = looker_agg.upper() if looker_agg else "SUM"
        return mapping.get(upper_agg, upper_agg)

    def get_dashboard(self, dashboard_id: str) -> dict:
        """Fetches dashboard metadata from Looker."""
        url = f"{self.base_url}/api/4.0/dashboards/{dashboard_id}"
        headers = {'Authorization': f'token {self.token}'}
        res = self.session.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        return res.json()

    def get_explore_fields(self, model: str, view: str) -> dict:
        """Fetches field definitions for a specific LookML Explore."""
        key = f"{model}:{view}"
        if key in self.explore_cache: return self.explore_cache[key]

        url = f"{self.base_url}/api/4.0/lookml_models/{model}/explores/{view}"
        headers = {'Authorization': f'token {self.token}'}
        try:
            res = self.session.get(url, headers=headers, timeout=15)
            if res.status_code != 200: return {}
            
            data = res.json().get('fields', {})
            field_map = {}

            # Process Measures
            for m in data.get('measures', []):
                clean_agg = self._normalize_agg(m.get('type', 'SUM'))
                field_map[m['name']] = {"type": "MEASURE", "agg": clean_agg, "dt": "NUMERIC"}
                field_map[m['name'].split('.')[-1]] = field_map[m['name']]
            
            # Process Dimensions
            for d in data.get('dimensions', []):
                dt = "DATE" if d.get('is_timeframe') or "date" in d['name'].lower() else "STRING"
                field_map[d['name']] = {"type": "DIMENSION", "agg": None, "dt": dt}
                field_map[d['name'].split('.')[-1]] = field_map[d['name']]

            self.explore_cache[key] = field_map
            return field_map
        except:
            return {}

def get_safe_query(element: dict) -> Optional[dict]:
    """Safely extracts the query object from a Looker dashboard element."""
    q = element.get("query")
    if isinstance(q, dict): return q
    rm = element.get("result_maker")
    if isinstance(rm, dict):
        rm_q = rm.get("query")
        if isinstance(rm_q, dict): return rm_q
    return None

def transform_looker_to_unified(dashboard_id: str, client_id: str, client_secret: str, base_url: str):
    """
    Main function to convert a Looker Dashboard into the Unified BI Schema.
    Now accepts dynamic base_url, client_id, and client_secret.
    """
    api = LookerClient(client_id, client_secret, base_url)
    looker_data = api.get_dashboard(dashboard_id)
    
    unified = {
        "schemaVersion": "1.3",
        "source": {
            "tool": "Looker",
            "dashboardId": str(looker_data.get("id")),
            "dashboardName": looker_data.get("title"),
            "folder": looker_data.get("folder", {}).get("name", "Unknown") if isinstance(looker_data.get("folder"), dict) else "Unknown",
            "createdAt": looker_data.get("created_at")
        },
        "datasets": [],
        "calculatedFields": [],
        "pages": []
    }

    view_to_ds_id = {}
    visuals = []

    for element in looker_data.get("dashboard_elements", []):
        # Handle Text Tiles
        if element.get("type") == "text":
            visuals.append({
                "id": str(element.get("id")), 
                "type": "TEXT", 
                "title": "Note", 
                "content": element.get("body_text", "")
            })
            continue

        query = get_safe_query(element)
        if not query: continue

        # --- Extract Limit and Sort Logic ---
        looker_limit = query.get("limit")
        looker_sorts = query.get("sorts", [])
        
        # Determine sort order (Looker format: ["field_name desc"])
        sort_order = "DESCENDING"
        if looker_sorts and " asc" in looker_sorts[0].lower():
            sort_order = "ASCENDING"

        model, view = query.get("model"), query.get("view")
        field_defs = api.get_explore_fields(model, view)

        # Dataset Management
        if view not in view_to_ds_id:
            ds_id = f"dataset_{len(view_to_ds_id) + 1}"
            view_to_ds_id[view] = ds_id
            unified["datasets"].append({"id": ds_id, "name": view, "sourceArn": None})

        # Field Categorization
        m_list, d_list = [], []
        for f_name in query.get("fields", []):
            info = field_defs.get(f_name)
            clean_col = f_name.split(".")[-1]
            if info and info["type"] == "MEASURE":
                m_list.append({"column": clean_col, "aggregation": info["agg"], "dataType": "NUMERIC"})
            else:
                dt = info["dt"] if info else "STRING"
                d_list.append({"column": clean_col, "dataType": dt})

        # Chart Type Mapping
        vis_type = str(query.get("vis_config", {}).get("type", "")).lower()
        chart_type = "TABLE"
        if any(x in vis_type for x in ["grid", "table"]): chart_type = "TABLE"
        elif "line" in vis_type: chart_type = "LINE"
        elif "area" in vis_type: chart_type = "AREA"
        elif "pie" in vis_type: chart_type = "PIE"
        elif any(x in vis_type for x in ["bar", "column"]):
            chart_type = "STACKED_BAR" if query.get("vis_config", {}).get("stacking") else "BAR"
        elif "scatter" in vis_type: chart_type = "SCATTER"
        elif any(x in vis_type for x in ["single_value", "multiple_value"]): chart_type = "KPI"

        # Build Visual Object
        viz = {
            "id": str(element.get("id")),
            "type": chart_type,
            "title": element.get("title") or "Untitled",
            "datasetRef": view_to_ds_id[view],
            "limit": int(looker_limit) if looker_limit else None,
            "sortOrder": sort_order
        }

        # Structure data based on chart type
        if chart_type == "TABLE":
            viz["columns"] = [{"field": d["column"], "type": "DIMENSION", "dataType": d["dataType"]} for d in d_list] + \
                             [{"field": m["column"], "type": "MEASURE", "aggregation": m["aggregation"], "dataType": m["dataType"]} for m in m_list]
            viz["config"] = {"showTotals": True, "conditionalFormatting": False, "pagination": {"enabled": True, "pageSize": 500}}
        elif chart_type in ["BAR", "STACKED_BAR"]:
            viz["x"] = [d["column"] for d in d_list[:1]]
            viz["stack"] = [d["column"] for d in d_list[1:]]
            viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"]} for m in m_list]
        elif chart_type in ["LINE", "AREA"]:
            viz["x"] = [{"column": d["column"], "timeGrain": "DAY"} for d in d_list[:1]]
            viz["stack"] = []
            viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"]} for m in m_list]
            viz["axes"] = {"x": {"title": "Date"}, "y": {"title": "Value"}}
        elif chart_type == "PIE":
            viz["categories"] = [d["column"] for d in d_list[:1]]
            viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"]} for m in m_list[:1]]
        elif chart_type == "SCATTER":
            viz["categories"] = [d["column"] for d in d_list]
            for i, m in enumerate(m_list[:2]):
                m["mapping"] = "XAXIS" if i == 0 else "YAXIS"
            viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"], "mapping": m.get("mapping")} for m in m_list]
            viz["config"] = {"showLegend": False}
        elif chart_type == "KPI":
            viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"]} for m in m_list[:1]]

        visuals.append(viz)

    # Wrap up result
    unified["pages"].append({"id": f"page_{dashboard_id}", "name": "Main Dashboard", "visuals": visuals})
    return unified