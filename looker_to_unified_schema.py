# import json
# import requests
# import traceback
# from typing import Dict, Any, Optional, List

# class LookerClient:
#     def __init__(self, client_id: str, client_secret: str, base_url: str):
#         self.session = requests.Session()
#         self.base_url = base_url.rstrip('/')
#         self.client_id = client_id
#         self.client_secret = client_secret
#         self.token = self._get_token()
#         self.explore_cache = {} 

#     def _get_token(self):
#         """Authenticates with Looker API and returns an access token."""
#         auth_url = f"{self.base_url}/api/4.0/login"
#         payload = {'client_id': self.client_id, 'client_secret': self.client_secret}
#         try:
#             res = self.session.post(auth_url, data=payload, timeout=10)
#             res.raise_for_status()
#             return res.json().get('access_token')
#         except Exception as e:
#             print(f"❌ Looker API Authentication Failed: {e}")
#             return None

#     def _normalize_agg(self, looker_agg: str) -> str:
#         """Maps Looker technical aggregations to standard BI aggregations."""
#         mapping = {
#             "SUM_DISTINCT": "SUM",
#             "AVERAGE_DISTINCT": "AVERAGE",
#             "COUNT_DISTINCT": "COUNT_DISTINCT",
#             "LIST": "COUNT",
#             "YESNO": "SUM"
#         }
#         upper_agg = looker_agg.upper() if looker_agg else "SUM"
#         return mapping.get(upper_agg, upper_agg)

#     def get_dashboard(self, dashboard_id: str) -> dict:
#         """Fetches dashboard metadata from Looker."""
#         url = f"{self.base_url}/api/4.0/dashboards/{dashboard_id}"
#         headers = {'Authorization': f'token {self.token}'}
#         res = self.session.get(url, headers=headers, timeout=15)
#         res.raise_for_status()
#         return res.json()

#     def get_explore_fields(self, model: str, view: str) -> dict:
#         """Fetches field definitions for a specific LookML Explore."""
#         key = f"{model}:{view}"
#         if key in self.explore_cache: return self.explore_cache[key]

#         url = f"{self.base_url}/api/4.0/lookml_models/{model}/explores/{view}"
#         headers = {'Authorization': f'token {self.token}'}
#         try:
#             res = self.session.get(url, headers=headers, timeout=15)
#             if res.status_code != 200: return {}
            
#             data = res.json().get('fields', {})
#             field_map = {}

#             # Process Measures
#             for m in data.get('measures', []):
#                 clean_agg = self._normalize_agg(m.get('type', 'SUM'))
#                 field_map[m['name']] = {"type": "MEASURE", "agg": clean_agg, "dt": "NUMERIC"}
#                 field_map[m['name'].split('.')[-1]] = field_map[m['name']]
            
#             # Process Dimensions
#             for d in data.get('dimensions', []):
#                 dt = "DATE" if d.get('is_timeframe') or "date" in d['name'].lower() else "STRING"
#                 field_map[d['name']] = {"type": "DIMENSION", "agg": None, "dt": dt}
#                 field_map[d['name'].split('.')[-1]] = field_map[d['name']]

#             self.explore_cache[key] = field_map
#             return field_map
#         except:
#             return {}

# def get_safe_query(element: dict) -> Optional[dict]:
#     """Safely extracts the query object from a Looker dashboard element."""
#     q = element.get("query")
#     if isinstance(q, dict): return q
#     rm = element.get("result_maker")
#     if isinstance(rm, dict):
#         rm_q = rm.get("query")
#         if isinstance(rm_q, dict): return rm_q
#     return None

# def transform_looker_to_unified(dashboard_id: str, client_id: str, client_secret: str, base_url: str):
#     """
#     Main function to convert a Looker Dashboard into the Unified BI Schema.
#     Now accepts dynamic base_url, client_id, and client_secret.
#     """
#     api = LookerClient(client_id, client_secret, base_url)
#     looker_data = api.get_dashboard(dashboard_id)
    
#     unified = {
#         "schemaVersion": "1.3",
#         "source": {
#             "tool": "Looker",
#             "dashboardId": str(looker_data.get("id")),
#             "dashboardName": looker_data.get("title"),
#             "folder": looker_data.get("folder", {}).get("name", "Unknown") if isinstance(looker_data.get("folder"), dict) else "Unknown",
#             "createdAt": looker_data.get("created_at")
#         },
#         "datasets": [],
#         "calculatedFields": [],
#         "pages": []
#     }

#     view_to_ds_id = {}
#     visuals = []

#     for element in looker_data.get("dashboard_elements", []):
#         # Handle Text Tiles
#         if element.get("type") == "text":
#             visuals.append({
#                 "id": str(element.get("id")), 
#                 "type": "TEXT", 
#                 "title": "Note", 
#                 "content": element.get("body_text", "")
#             })
#             continue

#         query = get_safe_query(element)
#         if not query: continue

#         # --- Extract Limit and Sort Logic ---
#         looker_limit = query.get("limit")
#         looker_sorts = query.get("sorts", [])
        
#         # Determine sort order (Looker format: ["field_name desc"])
#         sort_order = "DESCENDING"
#         if looker_sorts and " asc" in looker_sorts[0].lower():
#             sort_order = "ASCENDING"

#         model, view = query.get("model"), query.get("view")
#         field_defs = api.get_explore_fields(model, view)

#         # Dataset Management
#         if view not in view_to_ds_id:
#             ds_id = f"dataset_{len(view_to_ds_id) + 1}"
#             view_to_ds_id[view] = ds_id
#             unified["datasets"].append({"id": ds_id, "name": view, "sourceArn": None})

#         # Field Categorization
#         m_list, d_list = [], []
#         for f_name in query.get("fields", []):
#             info = field_defs.get(f_name)
#             clean_col = f_name.split(".")[-1]
#             if info and info["type"] == "MEASURE":
#                 m_list.append({"column": clean_col, "aggregation": info["agg"], "dataType": "NUMERIC"})
#             else:
#                 dt = info["dt"] if info else "STRING"
#                 d_list.append({"column": clean_col, "dataType": dt})

#         # Chart Type Mapping
#         vis_type = str(query.get("vis_config", {}).get("type", "")).lower()
#         chart_type = "TABLE"
#         if any(x in vis_type for x in ["grid", "table"]): chart_type = "TABLE"
#         elif "line" in vis_type: chart_type = "LINE"
#         elif "area" in vis_type: chart_type = "AREA"
#         elif "pie" in vis_type: chart_type = "PIE"
#         elif any(x in vis_type for x in ["bar", "column"]):
#             chart_type = "STACKED_BAR" if query.get("vis_config", {}).get("stacking") else "BAR"
#         elif "scatter" in vis_type: chart_type = "SCATTER"
#         elif any(x in vis_type for x in ["single_value", "multiple_value"]): chart_type = "KPI"

#         # Build Visual Object
#         viz = {
#             "id": str(element.get("id")),
#             "type": chart_type,
#             "title": element.get("title") or "Untitled",
#             "datasetRef": view_to_ds_id[view],
#             "limit": int(looker_limit) if looker_limit else None,
#             "sortOrder": sort_order
#         }

#         # Structure data based on chart type
#         if chart_type == "TABLE":
#             viz["columns"] = [{"field": d["column"], "type": "DIMENSION", "dataType": d["dataType"]} for d in d_list] + \
#                              [{"field": m["column"], "type": "MEASURE", "aggregation": m["aggregation"], "dataType": m["dataType"]} for m in m_list]
#             viz["config"] = {"showTotals": True, "conditionalFormatting": False, "pagination": {"enabled": True, "pageSize": 500}}
#         elif chart_type in ["BAR", "STACKED_BAR"]:
#             viz["x"] = [d["column"] for d in d_list[:1]]
#             viz["stack"] = [d["column"] for d in d_list[1:]]
#             viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"]} for m in m_list]
#         elif chart_type in ["LINE", "AREA"]:
#             viz["x"] = [{"column": d["column"], "timeGrain": "DAY"} for d in d_list[:1]]
#             viz["stack"] = []
#             viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"]} for m in m_list]
#             viz["axes"] = {"x": {"title": "Date"}, "y": {"title": "Value"}}
#         elif chart_type == "PIE":
#             viz["categories"] = [d["column"] for d in d_list[:1]]
#             viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"]} for m in m_list[:1]]
#         elif chart_type == "SCATTER":
#             viz["categories"] = [d["column"] for d in d_list]
#             for i, m in enumerate(m_list[:2]):
#                 m["mapping"] = "XAXIS" if i == 0 else "YAXIS"
#             viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"], "mapping": m.get("mapping")} for m in m_list]
#             viz["config"] = {"showLegend": False}
#         elif chart_type == "KPI":
#             viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"]} for m in m_list[:1]]

#         visuals.append(viz)

#     # Wrap up result
#     unified["pages"].append({"id": f"page_{dashboard_id}", "name": "Main Dashboard", "visuals": visuals})
#     return unified


# ---------------------

# import json
# import requests
# import traceback
# from typing import Dict, Any, Optional, List

# from calc_field_translator import parse_dynamic_fields


# class LookerClient:
#     def __init__(self, client_id: str, client_secret: str, base_url: str):
#         self.session = requests.Session()
#         self.base_url = base_url.rstrip('/')
#         self.client_id = client_id
#         self.client_secret = client_secret
#         self.token = self._get_token()
#         self.explore_cache = {}

#     def _get_token(self):
#         auth_url = f"{self.base_url}/api/4.0/login"
#         payload = {'client_id': self.client_id, 'client_secret': self.client_secret}
#         try:
#             res = self.session.post(auth_url, data=payload, timeout=10)
#             res.raise_for_status()
#             return res.json().get('access_token')
#         except Exception as e:
#             print(f"❌ Looker API Authentication Failed: {e}")
#             return None

#     def _normalize_agg(self, looker_agg: str) -> str:
#         mapping = {
#             "SUM_DISTINCT":     "SUM",
#             "AVERAGE_DISTINCT": "AVERAGE",
#             "COUNT_DISTINCT":   "COUNT_DISTINCT",
#             "LIST":             "COUNT",
#             "YESNO":            "SUM",
#         }
#         upper_agg = looker_agg.upper() if looker_agg else "SUM"
#         return mapping.get(upper_agg, upper_agg)

#     def get_dashboard(self, dashboard_id: str) -> dict:
#         url = f"{self.base_url}/api/4.0/dashboards/{dashboard_id}"
#         headers = {'Authorization': f'token {self.token}'}
#         res = self.session.get(url, headers=headers, timeout=15)
#         res.raise_for_status()
#         return res.json()

#     def get_explore_fields(self, model: str, view: str) -> dict:
#         key = f"{model}:{view}"
#         if key in self.explore_cache:
#             return self.explore_cache[key]
#         url = f"{self.base_url}/api/4.0/lookml_models/{model}/explores/{view}"
#         headers = {'Authorization': f'token {self.token}'}
#         try:
#             res = self.session.get(url, headers=headers, timeout=15)
#             if res.status_code != 200:
#                 return {}
#             data = res.json().get('fields', {})
#             field_map = {}
#             for m in data.get('measures', []):
#                 clean_agg = self._normalize_agg(m.get('type', 'SUM'))
#                 field_map[m['name']] = {"type": "MEASURE", "agg": clean_agg, "dt": "NUMERIC"}
#                 field_map[m['name'].split('.')[-1]] = field_map[m['name']]
#             for d in data.get('dimensions', []):
#                 dt = "DATE" if d.get('is_timeframe') or "date" in d['name'].lower() else "STRING"
#                 field_map[d['name']] = {"type": "DIMENSION", "agg": None, "dt": dt}
#                 field_map[d['name'].split('.')[-1]] = field_map[d['name']]
#             self.explore_cache[key] = field_map
#             return field_map
#         except Exception:
#             return {}


# def get_safe_query(element: dict) -> Optional[dict]:
#     q = element.get("query")
#     if isinstance(q, dict):
#         return q
#     rm = element.get("result_maker")
#     if isinstance(rm, dict):
#         rm_q = rm.get("query")
#         if isinstance(rm_q, dict):
#             return rm_q
#     return None


# # ---------------------------------------------------------------------------
# # Calculated-field helpers
# # ---------------------------------------------------------------------------

# def _extract_calc_fields_from_query(query: dict, column_mapping: dict = None) -> list[dict]:
#     """
#     Parse the query's dynamic_fields and return unified calculatedField dicts.

#     Each entry:
#         {
#           "name":        str,   # canonical field name referenced in query.fields
#           "label":       str,
#           "expression":  str,   # original Looker expression (for audit trail)
#           "beast_mode":  str,   # ready-to-use Domo Beast Mode SQL
#           "is_disabled": bool,
#           "category":    str,   # "measure" | "table_calculation"
#         }
#     """
#     raw_df = query.get("dynamic_fields")
#     if not raw_df:
#         return []

#     parsed = parse_dynamic_fields(raw_df, column_mapping or {})
#     results = []
#     for cf in parsed:
#         # Reconstruct the original expression string for audit purposes
#         if cf["category"] == "table_calculation":
#             # Try to recover original expression from raw list
#             if isinstance(raw_df, str):
#                 try:
#                     raw_list = json.loads(raw_df)
#                 except Exception:
#                     raw_list = []
#             else:
#                 raw_list = raw_df if isinstance(raw_df, list) else []
#             original_expr = next(
#                 (f.get("expression", "") for f in raw_list
#                  if f.get("table_calculation") == cf["name"] or f.get("label") == cf["label"]),
#                 ""
#             )
#         else:
#             original_expr = cf.get("beast_mode", "")   # measure: beast_mode IS the expression

#         results.append({
#             "name":        cf["name"],
#             "label":       cf["label"],
#             "expression":  original_expr,
#             "beast_mode":  cf["beast_mode"],
#             "is_disabled": cf["is_disabled"],
#             "category":    cf["category"],
#         })
#     return results


# def _build_calc_field_set(calc_fields: list[dict]) -> dict:
#     """Return a name → calc_field dict for quick lookups."""
#     return {cf["name"]: cf for cf in calc_fields}


# # ---------------------------------------------------------------------------
# # Main transform
# # ---------------------------------------------------------------------------

# def transform_looker_to_unified(
#     dashboard_id: str,
#     client_id: str,
#     client_secret: str,
#     base_url: str,
#     column_mapping: dict = None,
# ) -> dict:
#     """
#     Convert a Looker Dashboard into the Unified BI Schema.
#     Now extracts dynamic_fields (calculated fields) and translates them to
#     Domo Beast Mode SQL, stored in unified["calculatedFields"].
#     """
#     column_mapping = column_mapping or {}
#     api = LookerClient(client_id, client_secret, base_url)
#     looker_data = api.get_dashboard(dashboard_id)

#     unified = {
#         "schemaVersion": "1.3",
#         "source": {
#             "tool":          "Looker",
#             "dashboardId":   str(looker_data.get("id")),
#             "dashboardName": looker_data.get("title"),
#             "folder":        (looker_data.get("folder") or {}).get("name", "Unknown"),
#             "createdAt":     looker_data.get("created_at"),
#         },
#         "datasets":         [],
#         "calculatedFields": [],   # ← populated below
#         "pages":            [],
#     }

#     view_to_ds_id  = {}
#     visuals        = []
#     # Global dedup for calc fields across all tiles
#     all_calc_fields: dict[str, dict] = {}

#     for element in looker_data.get("dashboard_elements", []):
#         # Text tiles
#         if element.get("type") == "text":
#             visuals.append({
#                 "id":      str(element.get("id")),
#                 "type":    "TEXT",
#                 "title":   "Note",
#                 "content": element.get("body_text", ""),
#             })
#             continue

#         query = get_safe_query(element)
#         if not query:
#             continue

#         # ----- Extract limit / sort -----
#         looker_limit = query.get("limit")
#         looker_sorts = query.get("sorts", [])
#         sort_order   = "DESCENDING"
#         if looker_sorts and " asc" in looker_sorts[0].lower():
#             sort_order = "ASCENDING"

#         model, view = query.get("model"), query.get("view")
#         field_defs  = api.get_explore_fields(model, view)

#         # ----- Calculated fields from this tile's dynamic_fields -----
#         tile_calc_fields = _extract_calc_fields_from_query(query, column_mapping)
#         calc_field_set   = _build_calc_field_set(tile_calc_fields)

#         element_id = str(element.get("id", "?"))
#         print(f"\n🔍 Element {element_id} | view={view}")
#         print(f"   query.fields       : {query.get('fields', [])}")
#         print(f"   dynamic_fields raw : {str(query.get('dynamic_fields', ''))[:120]}")
#         print(f"   calc_field_set keys: {list(calc_field_set.keys())}")

#         # Merge into global map (dedup by name, last write wins)
#         for cf in tile_calc_fields:
#             all_calc_fields[cf["name"]] = cf

#         # ----- Dataset management -----
#         if view not in view_to_ds_id:
#             ds_id = f"dataset_{len(view_to_ds_id) + 1}"
#             view_to_ds_id[view] = ds_id
#             unified["datasets"].append({"id": ds_id, "name": view, "sourceArn": None})

#         # ----- Field categorisation -----
#         m_list, d_list = [], []
#         for f_name in query.get("fields", []):
#             bare = f_name.split(".")[-1]

#             # Check both bare name AND full name against calc_field_set
#             # (Looker dynamic fields have no view prefix in query.fields)
#             calc_key = bare if bare in calc_field_set else (f_name if f_name in calc_field_set else None)

#             # Is it a dynamic (calculated) field?
#             if calc_key:
#                 cf = calc_field_set[calc_key]
#                 print(f"   ✅ Calc field matched: {f_name} → {calc_key} | disabled={cf['is_disabled']} | beast_mode={cf['beast_mode']}")
#                 if not cf["is_disabled"]:
#                     m_list.append({
#                         "column":      calc_key,
#                         "aggregation": "SUM",   # beast mode handles the real logic
#                         "dataType":    "NUMERIC",
#                         "is_calc":     True,
#                     })
#                 continue

#             info = field_defs.get(f_name) or field_defs.get(bare)
#             if info and info["type"] == "MEASURE":
#                 m_list.append({
#                     "column":      bare,
#                     "aggregation": info["agg"],
#                     "dataType":    "NUMERIC",
#                     "is_calc":     False,
#                 })
#             else:
#                 dt = info["dt"] if info else "STRING"
#                 d_list.append({"column": bare, "dataType": dt})

#         # ----- Chart type mapping -----
#         vis_type   = str(query.get("vis_config", {}).get("type", "")).lower()
#         chart_type = "TABLE"
#         if any(x in vis_type for x in ["grid", "table"]):
#             chart_type = "TABLE"
#         elif "line" in vis_type:
#             chart_type = "LINE"
#         elif "area" in vis_type:
#             chart_type = "AREA"
#         elif "pie" in vis_type:
#             chart_type = "PIE"
#         elif any(x in vis_type for x in ["bar", "column"]):
#             chart_type = "STACKED_BAR" if query.get("vis_config", {}).get("stacking") else "BAR"
#         elif "scatter" in vis_type:
#             chart_type = "SCATTER"
#         elif any(x in vis_type for x in ["single_value", "multiple_value"]):
#             chart_type = "KPI"

#         # ----- Build visual object -----
#         viz = {
#             "id":         str(element.get("id")),
#             "type":       chart_type,
#             "title":      element.get("title") or "Untitled",
#             "datasetRef": view_to_ds_id[view],
#             "limit":      int(looker_limit) if looker_limit else None,
#             "sortOrder":  sort_order,
#         }

#         if chart_type == "TABLE":
#             viz["columns"] = (
#                 [{"field": d["column"], "type": "DIMENSION", "dataType": d["dataType"]} for d in d_list] +
#                 [{"field": m["column"], "type": "MEASURE", "aggregation": m["aggregation"], "dataType": m["dataType"]} for m in m_list]
#             )
#             viz["config"] = {
#                 "showTotals": True,
#                 "conditionalFormatting": False,
#                 "pagination": {"enabled": True, "pageSize": 500},
#             }
#         elif chart_type in ["BAR", "STACKED_BAR"]:
#             viz["x"]        = [d["column"] for d in d_list[:1]]
#             viz["stack"]    = [d["column"] for d in d_list[1:]]
#             viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"]} for m in m_list]
#         elif chart_type in ["LINE", "AREA"]:
#             viz["x"]        = [{"column": d["column"], "timeGrain": "DAY"} for d in d_list[:1]]
#             viz["stack"]    = []
#             viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"]} for m in m_list]
#             viz["axes"]     = {"x": {"title": "Date"}, "y": {"title": "Value"}}
#         elif chart_type == "PIE":
#             viz["categories"] = [d["column"] for d in d_list[:1]]
#             viz["measures"]   = [{"column": m["column"], "aggregation": m["aggregation"]} for m in m_list[:1]]
#         elif chart_type == "SCATTER":
#             viz["categories"] = [d["column"] for d in d_list]
#             for i, m in enumerate(m_list[:2]):
#                 m["mapping"] = "XAXIS" if i == 0 else "YAXIS"
#             viz["measures"] = [
#                 {"column": m["column"], "aggregation": m["aggregation"], "mapping": m.get("mapping")}
#                 for m in m_list
#             ]
#             viz["config"] = {"showLegend": False}
#         elif chart_type == "KPI":
#             viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"]} for m in m_list[:1]]

#         visuals.append(viz)

#     # ---- Finalise ----
#     # Write deduped calc fields into the schema
#     unified["calculatedFields"] = list(all_calc_fields.values())
#     unified["pages"].append({
#         "id":      f"page_{dashboard_id}",
#         "name":    "Main Dashboard",
#         "visuals": visuals,
#     })
#     return unified

# ----------------

import json
import requests
import traceback
from typing import Dict, Any, Optional, List

from calc_field_translator import parse_dynamic_fields


class LookerClient:
    def __init__(self, client_id: str, client_secret: str, base_url: str):
        self.session = requests.Session()
        self.base_url = base_url.rstrip('/')
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = self._get_token()
        self.explore_cache = {}

    def _get_token(self):
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
        mapping = {
            "SUM_DISTINCT":     "SUM",
            "AVERAGE_DISTINCT": "AVG",
            "COUNT_DISTINCT":   "COUNT_DISTINCT",
            "LIST":             "COUNT",
            "YESNO":            "SUM",
        }
        upper_agg = looker_agg.upper() if looker_agg else "SUM"
        return mapping.get(upper_agg, upper_agg)

    def get_dashboard(self, dashboard_id: str) -> dict:
        url = f"{self.base_url}/api/4.0/dashboards/{dashboard_id}"
        headers = {'Authorization': f'token {self.token}'}
        res = self.session.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        return res.json()

    def get_explore_fields(self, model: str, view: str) -> dict:
        key = f"{model}:{view}"
        if key in self.explore_cache:
            return self.explore_cache[key]
        url = f"{self.base_url}/api/4.0/lookml_models/{model}/explores/{view}"
        headers = {'Authorization': f'token {self.token}'}
        try:
            res = self.session.get(url, headers=headers, timeout=15)
            if res.status_code != 200:
                return {}
            data = res.json().get('fields', {})
            field_map = {}
            for m in data.get('measures', []):
                clean_agg = self._normalize_agg(m.get('type', 'SUM'))
                field_map[m['name']] = {"type": "MEASURE", "agg": clean_agg, "dt": "NUMERIC"}
                field_map[m['name'].split('.')[-1]] = field_map[m['name']]
            for d in data.get('dimensions', []):
                dt = "DATE" if d.get('is_timeframe') or "date" in d['name'].lower() else "STRING"
                field_map[d['name']] = {"type": "DIMENSION", "agg": None, "dt": dt}
                field_map[d['name'].split('.')[-1]] = field_map[d['name']]
            self.explore_cache[key] = field_map
            return field_map
        except Exception:
            return {}


def get_safe_query(element: dict) -> Optional[dict]:
    q = element.get("query")
    if isinstance(q, dict):
        return q
    rm = element.get("result_maker")
    if isinstance(rm, dict):
        rm_q = rm.get("query")
        if isinstance(rm_q, dict):
            return rm_q
    return None


def _extract_calc_fields_from_query(query: dict, column_mapping: dict = None) -> list:
    """
    Parse the query's dynamic_fields and return unified calculatedField dicts.
    """
    raw_df = query.get("dynamic_fields")
    if not raw_df:
        return []

    parsed = parse_dynamic_fields(raw_df, column_mapping or {})
    results = []
    for cf in parsed:
        if cf["category"] == "table_calculation":
            if isinstance(raw_df, str):
                try:
                    raw_list = json.loads(raw_df)
                except Exception:
                    raw_list = []
            else:
                raw_list = raw_df if isinstance(raw_df, list) else []
            original_expr = next(
                (f.get("expression", "") for f in raw_list
                 if f.get("table_calculation") == cf["name"] or f.get("label") == cf["label"]),
                ""
            )
        else:
            original_expr = cf.get("beast_mode", "")

        results.append({
            "name":        cf["name"],
            "label":       cf["label"],
            "expression":  original_expr,
            "beast_mode":  cf["beast_mode"],
            "is_disabled": cf["is_disabled"],
            "category":    cf["category"],
        })
    return results


def _build_calc_field_set(calc_fields: list) -> dict:
    return {cf["name"]: cf for cf in calc_fields}


def transform_looker_to_unified(
    dashboard_id: str,
    client_id: str,
    client_secret: str,
    base_url: str,
    column_mapping: dict = None,
) -> dict:
    column_mapping = column_mapping or {}
    api = LookerClient(client_id, client_secret, base_url)
    looker_data = api.get_dashboard(dashboard_id)

    unified = {
        "schemaVersion": "1.3",
        "source": {
            "tool":          "Looker",
            "dashboardId":   str(looker_data.get("id")),
            "dashboardName": looker_data.get("title"),
            "folder":        (looker_data.get("folder") or {}).get("name", "Unknown"),
            "createdAt":     looker_data.get("created_at"),
        },
        "datasets":         [],
        "calculatedFields": [],
        "pages":            [],
    }

    view_to_ds_id  = {}
    visuals        = []
    all_calc_fields: dict = {}

    for element in looker_data.get("dashboard_elements", []):
        if element.get("type") == "text":
            visuals.append({
                "id":      str(element.get("id")),
                "type":    "TEXT",
                "title":   "Note",
                "content": element.get("body_text", ""),
            })
            continue

        query = get_safe_query(element)
        if not query:
            continue

        looker_limit = query.get("limit")
        looker_sorts = query.get("sorts", [])
        sort_order   = "DESCENDING"
        if looker_sorts and " asc" in looker_sorts[0].lower():
            sort_order = "ASCENDING"

        model, view = query.get("model"), query.get("view")
        field_defs  = api.get_explore_fields(model, view)

        # Extract calc fields from this tile's dynamic_fields
        tile_calc_fields = _extract_calc_fields_from_query(query, column_mapping)
        calc_field_set   = _build_calc_field_set(tile_calc_fields)

        element_id = str(element.get("id", "?"))
        print(f"\n🔍 Element {element_id} | view={view}")
        print(f"   query.fields       : {query.get('fields', [])}")
        print(f"   dynamic_fields raw : {str(query.get('dynamic_fields', ''))[:120]}")
        print(f"   calc_field_set keys: {list(calc_field_set.keys())}")

        # Merge into global map
        for cf in tile_calc_fields:
            all_calc_fields[cf["name"]] = cf

        # Dataset management
        if view not in view_to_ds_id:
            ds_id = f"dataset_{len(view_to_ds_id) + 1}"
            view_to_ds_id[view] = ds_id
            unified["datasets"].append({"id": ds_id, "name": view, "sourceArn": None})

        # ---------------------------------------------------------------
        # Field categorisation
        # KEY FIX: A tile that HAS calc fields should use those calc fields
        # as its measure, even though query.fields only lists base columns.
        # ---------------------------------------------------------------
        m_list, d_list = [], []
        query_field_names = query.get("fields", [])
        matched_calc_names = set()

        # First pass: find fields explicitly named in query.fields that ARE calc fields
        for f_name in query_field_names:
            bare = f_name.split(".")[-1]
            calc_key = bare if bare in calc_field_set else (f_name if f_name in calc_field_set else None)

            if calc_key:
                cf = calc_field_set[calc_key]
                print(f"   ✅ Calc field matched directly: {f_name} → {calc_key}")
                matched_calc_names.add(calc_key)
                if not cf["is_disabled"]:
                    m_list.append({
                        "column":      calc_key,
                        "aggregation": "SUM",
                        "dataType":    "NUMERIC",
                        "is_calc":     True,
                    })
                continue

            info = field_defs.get(f_name) or field_defs.get(bare)
            if info and info["type"] == "MEASURE":
                m_list.append({
                    "column":      bare,
                    "aggregation": info["agg"],
                    "dataType":    "NUMERIC",
                    "is_calc":     False,
                })
            else:
                dt = info["dt"] if info else "STRING"
                d_list.append({"column": bare, "dataType": dt})

        # Second pass: handle unmatched calc fields (not in query.fields)
        # These are the INTENDED metrics — the base columns in query.fields
        # are just inputs to the formula expression.
        unmatched_calcs = [
            cf for name, cf in calc_field_set.items()
            if name not in matched_calc_names and not cf["is_disabled"]
        ]

        if unmatched_calcs:
            vis_type_raw = str(query.get("vis_config", {}).get("type", "")).lower()
            is_kpi_type  = any(x in vis_type_raw for x in ["single_value", "multiple_value"])

            if is_kpi_type:
                # KPI: calc field IS the metric, remove all base columns
                d_list = []
                m_list = []
                for cf in unmatched_calcs:
                    print(f"   ✅ KPI: injecting calc as sole measure: {cf['name']}")
                    m_list.append({
                        "column": cf["name"], "aggregation": "SUM",
                        "dataType": "NUMERIC", "is_calc": True,
                    })
            elif not m_list:
                # No base measures found — calc fields are the measures
                for cf in unmatched_calcs:
                    print(f"   ✅ Injecting unmatched calc as measure: {cf['name']}")
                    m_list.append({
                        "column": cf["name"], "aggregation": "SUM",
                        "dataType": "NUMERIC", "is_calc": True,
                    })
            else:
                # Base measures exist AND we have unmatched calcs.
                # The unmatched calcs ARE the intended display metric (they derive
                # from the base columns). Replace base measures with calc fields.
                # Keep base columns only as dimensions (context).
                base_measure_cols = [m["column"] for m in m_list if not m.get("is_calc")]
                # Move base measure columns to d_list as context dims
                for col in base_measure_cols:
                    d_list.append({"column": col, "dataType": "NUMERIC"})
                # Replace m_list with calc fields only
                m_list = [m for m in m_list if m.get("is_calc")]  # keep already-matched calcs
                for cf in unmatched_calcs:
                    print(f"   ✅ Replacing base measures with calc field: {cf['name']}")
                    m_list.append({
                        "column": cf["name"], "aggregation": "SUM",
                        "dataType": "NUMERIC", "is_calc": True,
                    })
                # For most chart types, remove the base columns from d_list too
                # (they're formula inputs, not dimensions to display)
                d_list = [d for d in d_list if d["column"] not in base_measure_cols]

        # Chart type mapping
        vis_type   = str(query.get("vis_config", {}).get("type", "")).lower()
        chart_type = "TABLE"
        if any(x in vis_type for x in ["grid", "table"]):
            chart_type = "TABLE"
        elif "line" in vis_type:
            chart_type = "LINE"
        elif "area" in vis_type:
            chart_type = "AREA"
        elif "pie" in vis_type:
            chart_type = "PIE"
        elif any(x in vis_type for x in ["bar", "column"]):
            chart_type = "STACKED_BAR" if query.get("vis_config", {}).get("stacking") else "BAR"
        elif "scatter" in vis_type:
            chart_type = "SCATTER"
        elif any(x in vis_type for x in ["single_value", "multiple_value"]):
            chart_type = "KPI"

        # Build visual object
        viz = {
            "id":         str(element.get("id")),
            "type":       chart_type,
            "title":      element.get("title") or "Untitled",
            "datasetRef": view_to_ds_id[view],
            "limit":      int(looker_limit) if looker_limit else None,
            "sortOrder":  sort_order,
        }

        if chart_type == "TABLE":
            viz["columns"] = (
                [{"field": d["column"], "type": "DIMENSION", "dataType": d["dataType"]} for d in d_list] +
                [{"field": m["column"], "type": "MEASURE", "aggregation": m["aggregation"],
                  "dataType": m["dataType"], "is_calc": m.get("is_calc", False)} for m in m_list]
            )
            viz["config"] = {
                "showTotals": True,
                "conditionalFormatting": False,
                "pagination": {"enabled": True, "pageSize": 500},
            }
        elif chart_type in ["BAR", "STACKED_BAR"]:
            viz["x"]        = [d["column"] for d in d_list[:1]]
            viz["stack"]    = [d["column"] for d in d_list[1:]]
            viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"],
                                "is_calc": m.get("is_calc", False)} for m in m_list]
        elif chart_type in ["LINE", "AREA"]:
            viz["x"]        = [{"column": d["column"], "timeGrain": "DAY"} for d in d_list[:1]]
            viz["stack"]    = []
            viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"],
                                "is_calc": m.get("is_calc", False)} for m in m_list]
            viz["axes"]     = {"x": {"title": "Date"}, "y": {"title": "Value"}}
        elif chart_type == "PIE":
            viz["categories"] = [d["column"] for d in d_list[:1]]
            viz["measures"]   = [{"column": m["column"], "aggregation": m["aggregation"],
                                  "is_calc": m.get("is_calc", False)} for m in m_list[:1]]
        elif chart_type == "SCATTER":
            viz["categories"] = [d["column"] for d in d_list]
            for i, m in enumerate(m_list[:2]):
                m["mapping"] = "XAXIS" if i == 0 else "YAXIS"
            viz["measures"] = [
                {"column": m["column"], "aggregation": m["aggregation"],
                 "mapping": m.get("mapping"), "is_calc": m.get("is_calc", False)}
                for m in m_list
            ]
            viz["config"] = {"showLegend": False}
        elif chart_type == "KPI":
            viz["measures"] = [{"column": m["column"], "aggregation": m["aggregation"],
                                "is_calc": m.get("is_calc", False)} for m in m_list[:1]]

        visuals.append(viz)

    unified["calculatedFields"] = list(all_calc_fields.values())
    unified["pages"].append({
        "id":      f"page_{dashboard_id}",
        "name":    "Main Dashboard",
        "visuals": visuals,
    })
    return unified
