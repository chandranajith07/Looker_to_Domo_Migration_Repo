# # domo_adapter.py - FINAL FIXED VERSION
# from typing import Dict, Any
# # from calc_field_translator import qs_to_beast_mode_sql

# class DomoAdapter:
#     def __init__(self, domo_client, dataset_resolver, column_mapping=None):
#         self.client = domo_client
#         self.dataset_resolver = dataset_resolver
#         self.column_mapping = column_mapping or {}
#         self.calculated_fields_map = {}

#     def deploy_dashboard(self, unified_schema: dict, page_id: str):
#         self._process_calculated_fields(unified_schema.get("calculatedFields", []))
        
#         results = []

#         for page in unified_schema.get("pages", []):
#             for visual in page.get("visuals", []):
#                 try:
#                     response = self._deploy_visual(page_id, visual)
#                     results.append({
#                         "visual_id": visual.get("id"),
#                         "type": visual.get("type"),
#                         "status": "SUCCESS",
#                         "card_id": response.get("id")
#                     })
#                 except Exception as e:
#                     results.append({
#                         "visual_id": visual.get("id"),
#                         "type": visual.get("type"),
#                         "status": "FAILED",
#                         "error": str(e),
#                         "card_id": None
#                     })

#         return results

#     def _process_calculated_fields(self, calculated_fields):
#         for cf in calculated_fields:
#             name = cf.get("name")
#             expression = cf.get("expression")
            
#             # beast_mode_sql = qs_to_beast_mode_sql(expression)
            
#             # self.calculated_fields_map[name] = {
#             #     "original_expression": expression,
#             #     "beast_mode_sql": beast_mode_sql
#             # }

#     def _map_column(self, column_name):
#         return self.column_mapping.get(column_name, column_name)

#     def _normalize_aggregation(self, agg):
#         if isinstance(agg, dict):
#             agg = agg.get("SimpleNumericalAggregation", "SUM")
        
#         agg_map = {
#             "AVERAGE": "AVG",
#             "SUM": "SUM",
#             "COUNT": "COUNT",
#             "MIN": "MIN",
#             "MAX": "MAX",
#             "DISTINCT_COUNT": "COUNT_DISTINCT"
#         }
        
#         agg_upper = str(agg).upper()
#         return agg_map.get(agg_upper, agg_upper)

#     def _extract_time_grain(self, x_entry):
#         """Extract time grain from x-axis entry"""
#         if isinstance(x_entry, dict):
#             time_grain = x_entry.get("timeGrain", "DAY")
#             column = x_entry.get("column")
#             return column, time_grain
#         return x_entry, "DAY"

#     def _map_time_grain_to_domo(self, qs_grain):
#         """Map QuickSight time grain to Domo dateTimeElement"""
#         grain_map = {
#             "YEAR": "YEAR",
#             "QUARTER": "QUARTER",
#             "MONTH": "MONTH",
#             "WEEK": "WEEK",
#             "DAY": "DAY",
#             "HOUR": "HOUR",
#             "MINUTE": "MINUTE"
#         }
#         return grain_map.get(qs_grain.upper(), "DAY")

#     def _is_date_column(self, column_name):
#         """Check if a column is a date field"""
#         date_keywords = ["date", "time", "timestamp", "datetime", "day", "month", "year"]
#         return any(keyword in column_name.lower() for keyword in date_keywords)

#     def _deploy_visual(self, page_id: str, visual: dict):
#         visual_type = visual["type"].upper()

#         if visual_type == "TABLE":
#             payload = self._build_table_payload(visual)
#         elif visual_type == "BAR":
#             payload = self._build_bar_payload(visual)
#         elif visual_type == "STACKED_BAR":
#             payload = self._build_stacked_bar_payload(visual)
#         elif visual_type == "KPI":
#             payload = self._build_kpi_payload(visual)
#         elif visual_type == "LINE":
#             payload = self._build_line_payload(visual)
#         elif visual_type == "AREA":
#             payload = self._build_area_payload(visual)
#         elif visual_type == "STACKED_AREA":
#             payload = self._build_stacked_area_payload(visual)
#         elif visual_type == "PIE":
#             payload = self._build_pie_payload(visual)
#         elif visual_type == "DONUT":
#             payload = self._build_donut_payload(visual)
#         elif visual_type in ["SCATTER", "BUBBLE"]:
#             payload = self._build_scatter_payload(visual)
#         elif visual_type == "COMBO":
#             payload = self._build_combo_payload(visual)
#         else:
#             raise NotImplementedError(f"Unsupported visual type: {visual_type}")

#         return self.client.create_card(page_id, payload)


#     def _build_kpi_payload(self, visual):
#         dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
#         m = visual["measures"][0]

#         column_name = self._map_column(m["column"])
#         aggregation = self._normalize_aggregation(m["aggregation"])

#         return {
#             "definition": {
#                 "title": visual["title"],
#                 "subscriptions": {
#                     "main": {
#                         "name": "main",
#                         "columns": [
#                             {
#                                 "column": column_name,
#                                 "aggregation": aggregation,
#                                 "mapping": "VALUE"
#                             }
#                         ],
#                         "filters": [],
#                         "groupBy": [],
#                         "distinct": False
#                     }
#                 },
#                 "charts": {
#                     "main": {
#                         "component": "main",
#                         "chartType": "badge_singlevalue"
#                     }
#                 }
#             },
#             "dataProvider": {
#                 "dataSourceId": dataset_id
#             },
#             "variables": True
#         }

#     def _build_bar_payload(self, visual):
#         dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
#         x = visual["x"][0]
#         m = visual["measures"][0]

#         x_mapped = self._map_column(x)
#         column_name = self._map_column(m["column"])
#         aggregation = self._normalize_aggregation(m["aggregation"])
        
#         # Extract limit and sort from visual if they exist, otherwise use defaults
#         limit_count = visual.get("limit")
#         sort_order = visual.get("sortOrder") # or "ASCENDING"

#         x_title = x_mapped.replace("_", " ").title()
#         y_title = f"{aggregation} of {column_name}".replace("_", " ").title()

#         return {
#             "definition": {
#                 "title": visual.get("title", "rpt_supply_finus_deals"),
#                 "subscriptions": {
#                     "main": {
#                         "name": "main",
#                         "columns": [
#                             {
#                                 "column": x_mapped,
#                                 "mapping": "ITEM"
#                             },
#                             {
#                                 "column": column_name,
#                                 "mapping": "VALUE",
#                                 "aggregation": aggregation
#                             }
#                         ],
#                         "filters": [],
#                         "orderBy": [
#                             {
#                                 "aggregation": aggregation,
#                                 "column": column_name,
#                                 "order": sort_order
#                             }
#                         ],
#                         "groupBy": [
#                             {
#                                 "column": x_mapped
#                             }
#                         ],
#                         "distinct": False,
#                         "limit": limit_count,
#                         "dateGrain": {"column": "date_date"} # Optional: change if you have a specific date col
#                     },
#                     "big_number": {
#                         "name": "big_number",
#                         "columns": [
#                             {
#                                 "aggregation": aggregation,
#                                 "alias": f"{aggregation} of {column_name}",
#                                 "column": column_name,
#                                 "format": {"format": "#A", "type": "abbreviated"}
#                             }
#                         ],
#                         "filters": []
#                     }
#                 },
#                 "formulas": {"dsUpdated": [], "dsDeleted": [], "card": []},
#                 "annotations": {"new": [], "modified": [], "deleted": []},
#                 "conditionalFormats": {"card": [], "datasource": []},
#                 "controls": [],
#                 "segments": {"active": [], "create": [], "update": [], "delete": []},
#                 "charts": {
#                     "main": {
#                         "component": "main",
#                         "chartType": "badge_vert_bar", # Use "badge_vert_stackedbar" if preferred
#                         "overrides": {
#                             "title_x": x_title,
#                             "title_y": y_title
#                         }
#                     }
#                 },
#                 "dynamicTitle": {
#                     "text": [{"text": visual.get("title", "rpt_supply_finus_deals"), "type": "TEXT"}]
#                 },
#                 "chartVersion": "12"
#             },
#             "dataProvider": {
#                 "dataSourceId": dataset_id
#             },
#             "variables": True
#         }

#     def _build_stacked_bar_payload(self, visual):
#         dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        
#         x = visual["x"][0]
#         stack = visual["stack"][0]
#         m = visual["measures"][0]
        
#         x_mapped = self._map_column(x)
#         stack_mapped = self._map_column(stack)
#         column_name = self._map_column(m["column"])
#         aggregation = self._normalize_aggregation(m["aggregation"])

#         x_title = x_mapped.replace("_", " ").title()
#         y_title = f"{aggregation} of {column_name}".replace("_", " ")
        
#         return {
#             "definition": {
#                 "title": visual["title"],
#                 "subscriptions": {
#                     "main": {
#                         "name": "main",
#                         "columns": [
#                             {
#                                 "column": x_mapped,
#                                 "mapping": "ITEM"
#                             },
#                             {
#                                 "column": stack_mapped,
#                                 "mapping": "SERIES"
#                             },
#                             {
#                                 "column": column_name,
#                                 "aggregation": aggregation,
#                                 "mapping": "VALUE"
#                             }
#                         ],
#                         "filters": [],
#                         "groupBy": [
#                             {"column": x_mapped},
#                             {"column": stack_mapped}
#                         ],
#                         "distinct": False
#                     }
#                 },
#                 "charts": {
#                     "main": {
#                         "component": "main",
#                         "chartType": "badge_vert_stackedbar",
#                         "overrides": {
#                             "title_x": x_title,
#                             "title_y": y_title
#                         },
#                         "goal": None
#                     }
#                 }
#             },
#             "dataProvider": {
#                 "dataSourceId": dataset_id
#             },
#             "variables": True
#         }
    
#     def _build_line_payload(self, visual: dict):
#         """
#         ✅ UPDATED: Now supports both single-line and multi-line charts
#         - If visual has 'stack' field with values, creates multi-line chart
#         - Otherwise creates single-line chart
#         """
#         dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])

#         # -------- X AXIS --------
#         x_entry = visual["x"][0]
#         x_col = self._map_column(x_entry["column"])
#         time_grain = x_entry.get("timeGrain", "DAY")

#         domo_grain = self._map_time_grain_to_domo(time_grain)

#         calendar_column_map = {
#             "DAY": "CalendarDay",
#             "WEEK": "CalendarWeek",
#             "MONTH": "CalendarMonth",
#             "QUARTER": "CalendarQuarter",
#             "YEAR": "CalendarYear"
#         }

#         calendar_column = calendar_column_map.get(time_grain, "CalendarDay")

#         # -------- MEASURE --------
#         m = visual["measures"][0]
#         val_col = self._map_column(m["column"])
#         aggregation = self._normalize_aggregation(m.get("aggregation", "SUM"))

#         # -------- CHECK FOR MULTI-LINE (SERIES) --------
#         stack_fields = visual.get("stack", [])
#         has_series = len(stack_fields) > 0
#         series_col = self._map_column(stack_fields[0]) if has_series else None

#         # -------- AXIS TITLES --------
#         x_title = visual.get("axes", {}).get("x", {}).get("title", "Date")
#         y_title = visual.get("axes", {}).get("y", {}).get(
#             "title", f"{aggregation} of {val_col}"
#         )

#         print(
#             f"📈 LINE chart - Date grain: {time_grain} -> "
#             f"Domo: {domo_grain} -> Calendar: {calendar_column}"
#         )
#         if has_series:
#             print(f"   Multi-line mode: SERIES = {series_col}")
#         else:
#             print(f"   Single-line mode")

#         # -------- BIG NUMBER --------
#         big_number_subscription = {
#             "name": "big_number",
#             "columns": [
#                 {
#                     "column": val_col,
#                     "aggregation": aggregation,
#                     "alias": f"{aggregation} of {val_col}",
#                     "format": {
#                         "type": "abbreviated",
#                         "format": "#A"
#                     }
#                 }
#             ],
#             "filters": [],
#             "orderBy": [],
#             "groupBy": [],
#             "fiscal": False,
#             "projection": False,
#             "distinct": False,
#             "limit": 1
#         }

#         # -------- MAIN SUBSCRIPTION --------
#         main_columns = [
#             {
#                 "column": calendar_column,
#                 "calendar": True,
#                 "mapping": "ITEM"
#             },
#             {
#                 "column": val_col,
#                 "aggregation": aggregation,
#                 "mapping": "VALUE"
#             }
#         ]
        
#         main_groupby = [
#             {
#                 "column": calendar_column,
#                 "calendar": True
#             }
#         ]

#         # ✅ ADD SERIES COLUMN FOR MULTI-LINE
#         if has_series:
#             main_columns.append({
#                 "column": series_col,
#                 "mapping": "SERIES"
#             })
#             main_groupby.append({
#                 "column": series_col
#             })

#         main_subscription = {
#             "name": "main",
#             "dataSourceId": dataset_id,
#             "columns": main_columns,
#             "filters": [],
#             "orderBy": [],
#             "groupBy": main_groupby,
#             "dateGrain": {
#                 "column": x_col,              # actual date column
#                 "dateTimeElement": domo_grain
#             },
#             "fiscal": False,
#             "projection": False,
#             "distinct": False
#         }

#         return {
#             "definition": {
#                 "subscriptions": {
#                     "big_number": big_number_subscription,
#                     "main": main_subscription
#                 },
#                 "charts": {
#                     "main": {
#                         "component": "main",
#                         "chartType": "badge_two_trendline",
#                         "overrides": {
#                             "title_x": x_title,
#                             "title_y": y_title
#                         },
#                         "goal": None
#                     }
#                 },
#                 "dynamicTitle": {
#                     "text": [
#                         {"type": "TEXT", "text": visual["title"]}
#                     ]
#                 },
#                 "dynamicDescription": {
#                     "text": [],
#                     "displayOnCardDetails": True
#                 },
#                 "formulas": {"card": [], "dsUpdated": [], "dsDeleted": []},
#                 "annotations": {"new": [], "modified": [], "deleted": []},
#                 "conditionalFormats": {"card": [], "datasource": []},
#                 "controls": [],
#                 "segments": {"active": [], "create": [], "update": [], "delete": []},
#                 "chartVersion": "12",
#                 "inputTable": False,
#                 "title": visual["title"],
#                 "description": "",
#                 "includeEmptyFilters": True
#             },
#             "dataProvider": {
#                 "dataSourceId": dataset_id
#             },
#             "variables": True
#         }


#     def _build_table_payload(self, visual: dict):
#         dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])

#         columns = []
#         group_by = []

#         for col in visual["columns"]:
#             column_name = self._map_column(col["field"])

#             if col["type"] == "DIMENSION":
#                 columns.append({
#                     "column": column_name,
#                     "mapping": "ITEM"
#                 })
#                 group_by.append({
#                     "column": column_name
#                 })

#             elif col["type"] == "MEASURE":
#                 aggregation = self._normalize_aggregation(col["aggregation"])
#                 columns.append({
#                     "column": column_name,
#                     "aggregation": aggregation,
#                     "mapping": "VALUE"
#                 })

#         return {
#             "definition": {
#                 "title": visual["title"],
#                 "subscriptions": {
#                     "main": {
#                         "name": "main",
#                         "columns": columns,
#                         "filters": [],
#                         "groupBy": group_by,
#                         "distinct": False
#                     }
#                 },
#                 "charts": {
#                     "main": {
#                         "component": "main",
#                         "chartType": "badge_table",
#                         "overrides": {}
#                     }
#                 }
#             },
#             "dataProvider": {
#                 "dataSourceId": dataset_id
#             },
#             "variables": True
#         }

#     def _build_area_payload(self, visual: dict):
#         """
#         ✅ UPDATED: Matches Domo UI trace for Area charts.
#         Includes big_number subscription and uses badge_vert_area_overlay.
#         """
#         dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])

#         # 1. Extract X-Axis (Dimension)
#         x_entry = visual["x"][0]
#         # Handle if x_entry is a dict or string
#         x_col_raw = x_entry["column"] if isinstance(x_entry, dict) else x_entry
#         x_col_mapped = self._map_column(x_col_raw)
        
#         # Handle Time Grain
#         time_grain = x_entry.get("timeGrain", "DAY") if isinstance(x_entry, dict) else "DAY"
#         domo_grain = self._map_time_grain_to_domo(time_grain)
        
#         is_date = self._is_date_column(x_col_mapped)
        
#         # Map to Calendar columns if it's a date (matches UI trace)
#         calendar_column = x_col_mapped
#         if is_date:
#             calendar_map = {
#                 "DAY": "CalendarDay",
#                 "WEEK": "CalendarWeek",
#                 "MONTH": "CalendarMonth",
#                 "QUARTER": "CalendarQuarter",
#                 "YEAR": "CalendarYear"
#             }
#             calendar_column = calendar_map.get(time_grain, "CalendarDay")

#         # 2. Extract Measure
#         m = visual["measures"][0]
#         val_col = self._map_column(m["column"])
#         aggregation = self._normalize_aggregation(m.get("aggregation", "SUM"))

#         # 3. Build Main Subscription
#         main_subscription = {
#             "name": "main",
#             "dataSourceId": dataset_id,
#             "columns": [
#                 {
#                     "column": calendar_column,
#                     "calendar": is_date,
#                     "mapping": "ITEM"
#                 },
#                 {
#                     "column": val_col,
#                     "aggregation": aggregation,
#                     "mapping": "VALUE"
#                 }
#             ],
#             "filters": [],
#             "orderBy": [],
#             "groupBy": [
#                 {
#                     "column": calendar_column,
#                     "calendar": is_date
#                 }
#             ],
#             "fiscal": False,
#             "projection": False,
#             "distinct": False
#         }

#         if is_date:
#             main_subscription["dateGrain"] = {
#                 "column": x_col_mapped,
#                 "dateTimeElement": domo_grain
#             }

#         # 4. Build Big Number Subscription (Required for Area Charts)
#         big_number_subscription = {
#             "name": "big_number",
#             "dataSourceId": dataset_id,
#             "columns": [
#                 {
#                     "column": val_col,
#                     "aggregation": aggregation,
#                     "alias": f"{aggregation} of {val_col}",
#                     "format": {
#                         "type": "abbreviated",
#                         "format": "#A"
#                     }
#                 }
#             ],
#             "filters": [],
#             "orderBy": [],
#             "groupBy": [],
#             "fiscal": False,
#             "projection": False,
#             "distinct": False,
#             "limit": 1
#         }

#         # 5. Final Payload
#         return {
#             "definition": {
#                 "subscriptions": {
#                     "main": main_subscription,
#                     "big_number": big_number_subscription
#                 },
#                 "charts": {
#                     "main": {
#                         "component": "main",
#                         "chartType": "badge_vert_area_overlay", # From UI trace
#                         "overrides": {
#                             "title_x": visual.get("axes", {}).get("x", {}).get("title", ""),
#                             "title_y": visual.get("axes", {}).get("y", {}).get("title", "")
#                         },
#                         "goal": None
#                     }
#                 },
#                 "dynamicTitle": {
#                     "text": [{"type": "TEXT", "text": visual["title"]}]
#                 },
#                 "dynamicDescription": {
#                     "text": [],
#                     "displayOnCardDetails": True
#                 },
#                 "formulas": {"card": [], "dsUpdated": [], "dsDeleted": []},
#                 "annotations": {"new": [], "modified": [], "deleted": []},
#                 "conditionalFormats": {"card": [], "datasource": []},
#                 "controls": [],
#                 "segments": {"active": [], "create": [], "update": [], "delete": []},
#                 "chartVersion": "12",
#                 "inputTable": False,
#                 "title": visual["title"],
#                 "description": ""
#             },
#             "dataProvider": {
#                 "dataSourceId": dataset_id
#             },
#             "variables": True
#         }

#     def _build_stacked_area_payload(self, visual: dict):
#         dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])

#         x_entry = visual["x"][0]
#         x_col = self._map_column(x_entry["column"])
#         time_grain = x_entry.get("timeGrain", "DAY")

#         domo_grain = self._map_time_grain_to_domo(time_grain)

#         calendar_column_map = {
#             "DAY": "CalendarDay",
#             "WEEK": "CalendarWeek",
#             "MONTH": "CalendarMonth",
#             "QUARTER": "CalendarQuarter",
#             "YEAR": "CalendarYear"
#         }

#         calendar_column = calendar_column_map.get(time_grain, "CalendarDay")

#         measure = visual["measures"][0]
#         value_col = self._map_column(measure["column"])
#         aggregation = self._normalize_aggregation(measure["aggregation"])

#         stack_col = self._map_column(visual["stack"][0])

#         main_subscription = {
#             "name": "main",
#             "dataSourceId": dataset_id,
#             "columns": [
#                 {
#                     "column": calendar_column,
#                     "calendar": True,
#                     "mapping": "ITEM"
#                 },
#                 {
#                     "column": value_col,
#                     "aggregation": aggregation,
#                     "mapping": "VALUE"
#                 },
#                 {
#                     "column": stack_col,
#                     "mapping": "SERIES"
#                 }
#             ],
#             "filters": [],
#             "orderBy": [],
#             "groupBy": [
#                 {
#                     "column": calendar_column,
#                     "calendar": True
#                 },
#                 {
#                     "column": stack_col
#                 }
#             ],
#             "dateGrain": {
#                 "column": x_col,
#                 "dateTimeElement": domo_grain
#             },
#             "fiscal": False,
#             "projection": False,
#             "distinct": False
#         }

#         payload = {
#             "definition": {
#                 "subscriptions": {
#                     "main": main_subscription
#                 },
#                 "charts": {
#                     "main": {
#                         "component": "main",
#                         "chartType": "badge_stackedtrend",
#                         "overrides": {
#                             "title_x": visual.get("axes", {}).get("x", {}).get("title", ""),
#                             "title_y": visual.get("axes", {}).get("y", {}).get("title", "")
#                         },
#                         "goal": None
#                     }
#                 },
#                 "dynamicTitle": {
#                     "text": [
#                         {
#                             "type": "TEXT",
#                             "text": visual.get(
#                                 "title",
#                                 f"{aggregation} of {value_col} by {x_col} and {stack_col}"
#                             )
#                         }
#                     ]
#                 },
#                 "dynamicDescription": {
#                     "text": [],
#                     "displayOnCardDetails": True
#                 },
#                 "formulas": {"card": [], "dsUpdated": [], "dsDeleted": []},
#                 "annotations": {"new": [], "modified": [], "deleted": []},
#                 "conditionalFormats": {"card": [], "datasource": []},
#                 "controls": [],
#                 "segments": {"active": [], "create": [], "update": [], "delete": []},
#                 "chartVersion": "12",
#                 "inputTable": False,
#                 "title": visual.get("title", ""),
#                 "description": "",
#                 "includeEmptyFilters": True
#             },
#             "dataProvider": {
#                 "dataSourceId": dataset_id
#             },
#             "variables": True
#         }

#         return payload


#     def _build_pie_payload(self, visual: dict):
#         dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        
#         category = visual.get("categories", [None])[0]
#         if not category:
#             category = visual.get("x", [None])[0]
        
#         m = visual["measures"][0]

#         category_mapped = self._map_column(category)
#         column_name = self._map_column(m["column"])
#         aggregation = self._normalize_aggregation(m["aggregation"])

#         return {
#             "definition": {
#                 "title": visual["title"],
#                 "subscriptions": {
#                     "main": {
#                         "name": "main",
#                         "columns": [
#                             {
#                                 "column": category_mapped,
#                                 "mapping": "ITEM"
#                             },
#                             {
#                                 "column": column_name,
#                                 "aggregation": aggregation,
#                                 "mapping": "VALUE"
#                             }
#                         ],
#                         "filters": [],
#                         "groupBy": [
#                             {
#                                 "column": category_mapped
#                             }
#                         ],
#                         "distinct": False
#                     }
#                 },
#                 "charts": {
#                     "main": {
#                         "component": "main",
#                         "chartType": "badge_pie"
#                     }
#                 }
#             },
#             "dataProvider": {
#                 "dataSourceId": dataset_id
#             },
#             "variables": True
#         }

#     def _build_scatter_payload(self, visual: dict):
#         """
#         ✅ FINAL VERSION: Replicates the UI trace exactly.
#         Fixes 400 error for visual 10933 and 10952.
#         """
#         dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        
#         # 1. Map Measures (XAXIS -> VALUE, YAXIS -> SERIES)
#         measures = visual.get("measures", [])
#         x_m = next((m for m in measures if m.get("mapping") == "XAXIS"), None)
#         y_m = next((m for m in measures if m.get("mapping") == "YAXIS"), None)

#         if not x_m or not y_m:
#             if len(measures) >= 2:
#                 x_m, y_m = measures[0], measures[1]
#             else:
#                 raise ValueError("Scatter requires at least 2 measures.")

#         x_col = self._map_column(x_m["column"])
#         x_agg = self._normalize_aggregation(x_m["aggregation"])
        
#         y_col = self._map_column(y_m["column"])
#         y_agg = self._normalize_aggregation(y_m["aggregation"])

#         # 2. Map Dimension (Category -> XTIME)
#         categories = visual.get("categories", [])
#         cat_col = categories[0] if categories else "id"
#         cat_mapped = self._map_column(cat_col)
#         is_date = self._is_date_column(cat_mapped)

#         # 3. Build Subscription Columns (Matching UI Trace Mappings)
#         # Dimension = XTIME, Measure1 = VALUE, Measure2 = SERIES
#         main_columns = [
#             {
#                 "column": cat_mapped,
#                 "calendar": is_date,
#                 "mapping": "XTIME" 
#             },
#             {
#                 "column": x_col,
#                 "aggregation": x_agg,
#                 "mapping": "VALUE"
#             },
#             {
#                 "column": y_col,
#                 "aggregation": y_agg,
#                 "mapping": "SERIES"
#             }
#         ]

#         # 4. Build Main Subscription
#         # In scatter, you must group by the identity (Dimension) 
#         # to ensure Domo knows how to plot the individual points.
#         main_subscription = {
#             "name": "main",
#             "dataSourceId": dataset_id,
#             "columns": main_columns,
#             "filters": [],
#             "orderBy": [],
#             "groupBy": [
#                 {"column": cat_mapped, "calendar": is_date}
#             ],
#             "fiscal": False,
#             "projection": False,
#             "distinct": False
#         }

#         if is_date:
#             main_subscription["dateGrain"] = {
#                 "column": cat_mapped,
#                 "dateTimeElement": "DAY"
#             }

#         # 5. Build Big Number Subscription (Mandatory for these chart types)
#         big_number_subscription = {
#             "name": "big_number",
#             "dataSourceId": dataset_id,
#             "columns": [
#                 {
#                     "column": y_col,
#                     "aggregation": y_agg,
#                     "alias": f"Sum of {y_col}",
#                     "format": {"type": "abbreviated", "format": "#A"}
#                 }
#             ],
#             "filters": []
#         }

#         # 6. Final Payload
#         return {
#             "definition": {
#                 "subscriptions": {
#                     "main": main_subscription,
#                     "big_number": big_number_subscription
#                 },
#                 "charts": {
#                     "main": {
#                         "component": "main",
#                         "chartType": "badge_xybubble", # UI trace used badge_xybubble
#                         "overrides": {
#                             "title_x": visual.get("axes", {}).get("x", {}).get("title", x_col),
#                             "title_y": visual.get("axes", {}).get("y", {}).get("title", y_col)
#                         },
#                         "goal": None
#                     }
#                 },
#                 "dynamicTitle": {
#                     "text": [{"type": "TEXT", "text": visual["title"]}]
#                 },
#                 "dynamicDescription": {"text": [], "displayOnCardDetails": True},
#                 "formulas": {"card": [], "dsUpdated": [], "dsDeleted": []},
#                 "annotations": {"new": [], "modified": [], "deleted": []},
#                 "conditionalFormats": {"card": [], "datasource": []},
#                 "controls": [], 
#                 "segments": {"active": [], "create": [], "update": [], "delete": []},
#                 "chartVersion": "12",
#                 "inputTable": False,
#                 "title": visual["title"],
#                 "description": "",
#                 "includeEmptyFilters": True
#             },
#             "dataProvider": {
#                 "dataSourceId": dataset_id
#             },
#             "variables": True
#         }

#     def _build_combo_payload(self, visual: dict):
#         """✅ FINAL: Working combo chart with big_number subscription"""
#         dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])

#         # Extract x-axis
#         x_entry = visual["x"][0]
#         x_col, time_grain = self._extract_time_grain(x_entry)
#         x_col_mapped = self._map_column(x_col)
        
#         is_date_field = self._is_date_column(x_col_mapped)
#         domo_grain = self._map_time_grain_to_domo(time_grain) if time_grain else "DAY"

#         bar_measures = visual.get("barMeasures", [])
#         line_measures = visual.get("lineMeasures", [])
#         series_list = visual.get("series", [])

#         if not bar_measures or not line_measures:
#             raise ValueError("COMBO requires at least one BAR and one LINE measure")

#         # Resolve columns
#         bar_col = self._map_column(bar_measures[0]["column"])
#         bar_agg = self._normalize_aggregation(bar_measures[0]["aggregation"])

#         line_col = self._map_column(line_measures[0]["column"])
#         line_agg = self._normalize_aggregation(line_measures[0]["aggregation"])

#         series_col = self._map_column(series_list[0]) if series_list else None

#         # Build subscription columns
#         subscription_columns = []
#         group_by = []

#         # X-axis
#         subscription_columns.append({
#             "column": x_col_mapped,
#             "mapping": "ITEM"
#         })
#         group_by.append({
#             "column": x_col_mapped
#         })

#         # Add both measures (bar first, then line)
#         subscription_columns.append({
#             "column": bar_col,
#             "aggregation": bar_agg,
#             "mapping": "VALUE"
#         })
        
#         subscription_columns.append({
#             "column": line_col,
#             "aggregation": line_agg,
#             "mapping": "VALUE"
#         })

#         # Series column (optional)
#         if series_col:
#             subscription_columns.append({
#                 "column": series_col,
#                 "mapping": "SERIES"
#             })
#             group_by.append({
#                 "column": series_col
#             })

#         # Build main subscription
#         main_subscription = {
#             "name": "main",
#             "dataSourceId": dataset_id,
#             "columns": subscription_columns,
#             "filters": [],
#             "orderBy": [],
#             "groupBy": group_by,
#             "fiscal": False,
#             "projection": False,
#             "distinct": False
#         }

#         # Add dateGrain for date fields
#         if is_date_field:
#             main_subscription["dateGrain"] = {
#                 "column": x_col_mapped,
#                 "dateTimeElement": domo_grain
#             }

#         # ✅ Build big_number subscription (REQUIRED for combo charts)
#         big_number_subscription = {
#             "name": "big_number",
#             "dataSourceId": dataset_id,
#             "columns": [
#                 {
#                     "column": bar_col,
#                     "aggregation": bar_agg,
#                     "alias": f"{bar_agg} of {bar_col}",
#                     "format": {
#                         "type": "abbreviated",
#                         "format": "#A"
#                     }
#                 }
#             ],
#             "filters": [],
#             "orderBy": [],
#             "groupBy": [],
#             "fiscal": False,
#             "projection": False,
#             "distinct": False,
#             "limit": 1
#         }

#         payload = {
#             "definition": {
#                 "subscriptions": {
#                     "big_number": big_number_subscription,  # ✅ Add big_number FIRST
#                     "main": main_subscription
#                 },
#                 "formulas": {
#                     "dsUpdated": [],
#                     "dsDeleted": [],
#                     "card": []
#                 },
#                 "conditionalFormats": {
#                     "card": [],
#                     "datasource": []
#                 },
#                 "annotations": {
#                     "new": [],
#                     "modified": [],
#                     "deleted": []
#                 },
#                 "dynamicTitle": {
#                     "text": [{"text": visual.get("title", "Combo Chart"), "type": "TEXT"}]
#                 },
#                 "dynamicDescription": {
#                     "text": [],
#                     "displayOnCardDetails": True
#                 },
#                 "chartVersion": "12",
#                 "charts": {
#                     "main": {
#                         "component": "main",
#                         "chartType": "badge_line_stackedbar",
#                         "overrides": {},
#                         "goal": None
#                     }
#                 },
#                 "allowTableDrill": True,
#                 "segments": {
#                     "active": [],
#                     "definitions": []
#                 },
#                 "controls": [],  # ✅ Add controls array
#                 "inputTable": False
#             },
#             "dataProvider": {
#                 "dataSourceId": dataset_id
#             },
#             "variables": True
#         }
        
#         # DEBUG: Print the payload
#         import json
#         print("\n🔍 COMBO PAYLOAD:")
#         print(json.dumps(payload, indent=2))
#         print("\n")
        
#         return payload

# # ----------------------------------------

# domo_adapter.py — with Beast Mode calculated fields support
# domo_adapter.py — with Beast Mode calculated fields support
# domo_adapter.py — with Beast Mode calculated fields support
# domo_adapter.py — with Beast Mode calculated fields support
# domo_adapter.py — with Beast Mode calculated fields support
import re
import uuid
from typing import Dict, Any
from calc_field_translator import parse_dynamic_fields, qs_to_beast_mode_sql


class DomoAdapter:
    def __init__(self, domo_client, dataset_resolver, column_mapping=None):
        self.client = domo_client
        self.dataset_resolver = dataset_resolver
        self.column_mapping = column_mapping or {}
        self.calculated_fields_map: dict = {}
        self._formula_id_cache: dict = {}   # stable formula ids per calc field name

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def deploy_dashboard(self, unified_schema: dict, page_id: str):
        self._process_calculated_fields(unified_schema.get("calculatedFields", []))

        results = []
        for page in unified_schema.get("pages", []):
            for visual in page.get("visuals", []):
                try:
                    response = self._deploy_visual(page_id, visual)
                    results.append({
                        "visual_id": visual.get("id"),
                        "type":      visual.get("type"),
                        "status":    "SUCCESS",
                        "card_id":   response.get("id"),
                    })
                except Exception as e:
                    results.append({
                        "visual_id": visual.get("id"),
                        "type":      visual.get("type"),
                        "status":    "FAILED",
                        "error":     str(e),
                        "card_id":   None,
                    })
        return results

    # ------------------------------------------------------------------
    # Calculated-field processing
    # ------------------------------------------------------------------

    def _process_calculated_fields(self, calculated_fields: list):
        for cf in calculated_fields:
            name = cf.get("name")
            if not name:
                continue
            beast_mode = cf.get("beast_mode", "")
            if not beast_mode:
                expression = cf.get("expression", "")
                beast_mode = qs_to_beast_mode_sql(expression, self.column_mapping)
            self.calculated_fields_map[name] = {
                "label":       cf.get("label", name),
                "beast_mode":  beast_mode,
                "is_disabled": cf.get("is_disabled", False),
                "category":    cf.get("category", "measure"),
            }

        print(f"\n📐 Loaded {len(self.calculated_fields_map)} calculated fields:")
        for k, v in self.calculated_fields_map.items():
            print(f"   {k} → {v['beast_mode']}")

    def _is_calc_field(self, column_name: str) -> bool:
        return column_name in self.calculated_fields_map

    def _get_beast_mode(self, column_name: str) -> str:
        info = self.calculated_fields_map.get(column_name, {})
        return info.get("beast_mode", "")

    def _get_label(self, column_name: str) -> str:
        info = self.calculated_fields_map.get(column_name, {})
        return info.get("label", column_name)

    # ------------------------------------------------------------------
    # Beast Mode formula builder
    #
    # HOW DOMO BEAST MODE WORKS IN CARD PAYLOADS:
    #
    # 1. definition.formulas.card  — defines the Beast Mode formula:
    #      { "id": <uuid>, "name": <display_name>, "formula": <SQL>,
    #        "columnAlias": <display_name>, "status": "VALID" }
    #
    # 2. subscription column for a Beast Mode measure must use:
    #      { "column": <display_name>,   ← matches formula.name / columnAlias
    #        "mapping": "VALUE" }        ← NO aggregation key for Beast Mode
    #
    # IMPORTANT: Do NOT put an aggregation on the subscription column for
    # Beast Mode — the aggregation is embedded in the formula SQL itself.
    # Domo will 400 if you do COUNT_DISTINCT on a KPI subscription column.
    # ------------------------------------------------------------------

    def _get_source_columns_from_beast_mode(self, beast_mode_sql: str) -> list:
        """Extract all real column names referenced in a Beast Mode expression."""
        return re.findall(r'`([^`]+)`', beast_mode_sql)

    def _get_primary_source_column(self, column_name: str) -> str:
        """
        Get the primary real dataset column to use as the subscription anchor.
        Uses the first column referenced in the beast mode formula.
        """
        beast_mode = self._get_beast_mode(column_name)
        cols = self._get_source_columns_from_beast_mode(beast_mode)
        return cols[0] if cols else column_name

    def _extract_agg_from_beast_mode(self, beast_mode_sql: str) -> str:
        """Extract top-level aggregation function from beast mode SQL."""
        # Check COUNT(DISTINCT first (before generic COUNT match)
        if re.match(r'^\s*COUNT\s*\(\s*DISTINCT', beast_mode_sql, re.IGNORECASE):
            return "COUNT_DISTINCT"
        m = re.match(r'^\s*(SUM|COUNT|AVG|MIN|MAX)\s*[\(`]', beast_mode_sql, re.IGNORECASE)
        if m:
            return m.group(1).upper()
        return "SUM"

    def _build_formula_entry(self, column_name: str) -> dict:
        """
        Build a Domo Beast Mode formula entry for formulas.dsUpdated.
        
        Real Domo API structure (captured from UI):
        - Goes in definition.formulas.dsUpdated (NOT formulas.card)
        - id format: "calculation_<uuid>"
        - Subscription column uses {"formulaId": <id>, "mapping": VALUE}
        - formulas.card stays empty []
        """
        beast_mode = self._get_beast_mode(column_name)
        label      = self._get_label(column_name)
        formula_id = self._get_formula_id(column_name)   # stable cached id
        return {
            "name":                          label,
            "id":                            formula_id,
            "variable":                      False,
            "isCalculation":                 True,
            "isAggregatable":                False,
            "persistedOnDataSource":         True,
            "initialPersistedOnDataSource":  True,
            "dataType":                      "numeric",
            "saved":                         True,
            "query":                         beast_mode,
            "formula":                       beast_mode,
            "status":                        "VALID",
            "bignumber":                     False,
            "nonAggregatedColumns":          [],
            "templateId":                    -1,
            "legacyId":                      formula_id,
            "cacheWindow":                   "non_dynamic",
            "containsAggregation":           True,
            "containsAnalytic":              False,
            "invalidColumns":                [],
            "nonAggregatedExpressions":      [],
            "formulaTemplateDependencies":   [],
            "columnPositions":               [],
            "formulaId":                     formula_id,
            "formulaDependencies":           [],
        }

    def _make_beast_mode_subscription_column(self, column_name: str, mapping: str = "VALUE") -> dict:
        """
        Build a subscription column for a Beast Mode calc field.

        Real Domo API: use {"formulaId": <id>, "mapping": VALUE}
        The formulaId matches the formula entry id in formulas.dsUpdated.
        Do NOT use "column" + "aggregation" — that bypasses beast mode entirely.
        """
        formula_id = self._get_formula_id(column_name)
        return {
            "formulaId": formula_id,
            "mapping":   mapping,
        }

    def _make_big_number_column(self, column_name: str, aggregation: str) -> dict:
        """
        Build the big_number subscription column.
        For Beast Mode: use formulaId reference.
        For regular fields: use column + aggregation.
        """
        if self._is_calc_field(column_name):
            formula_id = self._get_formula_id(column_name)
            return {
                "formulaId": formula_id,
                "format":    {"type": "abbreviated", "format": "#A"},
            }
        return {
            "column":      column_name,
            "aggregation": aggregation,
            "alias":       f"{aggregation} of {column_name}",
            "format":      {"type": "abbreviated", "format": "#A"},
        }

    def _make_orderby_column(self, column_name: str, aggregation: str, order: str) -> dict:
        """
        Build an orderBy entry.
        For Beast Mode: use formulaId reference.
        """
        if self._is_calc_field(column_name):
            formula_id = self._get_formula_id(column_name)
            return {"formulaId": formula_id, "order": order}
        return {"aggregation": aggregation, "column": column_name, "order": order}

    def _get_formula_id(self, column_name: str) -> str:
        """Get or create a stable formula id for a calc field."""
        if column_name not in self._formula_id_cache:
            self._formula_id_cache[column_name] = f"calculation_{uuid.uuid4()}"
        return self._formula_id_cache[column_name]

    def _make_regular_subscription_column(self, column_name: str, aggregation: str, mapping: str = "VALUE") -> dict:
        """Regular (non-Beast Mode) subscription column with aggregation."""
        return {
            "column":      column_name,
            "aggregation": aggregation,
            "mapping":     mapping,
        }

    def _make_value_column(self, column_name: str, aggregation: str, mapping: str = "VALUE") -> dict:
        """Auto-detect whether column needs Beast Mode or regular subscription."""
        if self._is_calc_field(column_name):
            return self._make_beast_mode_subscription_column(column_name, mapping)
        return self._make_regular_subscription_column(column_name, aggregation, mapping)

    def _collect_formulas(self, column_names: list) -> list:
        """Collect all beast mode formula entries for a list of column names."""
        formulas, seen = [], set()
        for col in column_names:
            if col and col not in seen and self._is_calc_field(col):
                formulas.append(self._build_formula_entry(col))
                seen.add(col)
        return formulas

    def _inject_formulas(self, payload: dict, column_names: list) -> dict:
        """
        Insert Beast Mode formulas into payload.
        Real Domo API: formulas go in definition.formulas.dsUpdated (NOT card).
        formulas.card must stay as empty list [].
        """
        formulas = self._collect_formulas(column_names)
        if formulas:
            defn     = payload.setdefault("definition", {})
            f_section = defn.setdefault("formulas", {"dsUpdated": [], "dsDeleted": [], "card": []})
            f_section.setdefault("dsUpdated", []).extend(formulas)
        return payload

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _map_column(self, column_name):
        return self.column_mapping.get(column_name, column_name)

    def _normalize_aggregation(self, agg):
        if isinstance(agg, dict):
            agg = agg.get("SimpleNumericalAggregation", "SUM")
        agg_map = {
            "AVERAGE":        "AVG",
            "SUM":            "SUM",
            "COUNT":          "COUNT",
            "MIN":            "MIN",
            "MAX":            "MAX",
            "DISTINCT_COUNT": "COUNT_DISTINCT",
            "COUNT_DISTINCT": "COUNT_DISTINCT",
        }
        return agg_map.get(str(agg).upper(), str(agg).upper())

    def _safe_aggregation(self, column_name: str, aggregation: str) -> str:
        """
        For Beast Mode columns, aggregation is embedded in the formula SQL.
        Return 'SUM' as a safe default (it won't be used in the subscription).
        For COUNT_DISTINCT on regular columns, convert to a Beast Mode formula.
        """
        if self._is_calc_field(column_name):
            return "SUM"  # won't be used — Beast Mode handles it
        if aggregation == "COUNT_DISTINCT":
            return "COUNT_DISTINCT"  # handled by _convert_count_distinct_to_beast_mode
        return aggregation

    def _convert_count_distinct_to_beast_mode(self, column_name: str) -> bool:
        """
        Domo KPI/PIE subscription columns don't support COUNT_DISTINCT.
        Auto-convert: add a Beast Mode formula COUNT(DISTINCT `col`) and
        register it in calculated_fields_map.
        Returns True if conversion happened.
        """
        if column_name in self.calculated_fields_map:
            return False  # already handled
        # Register a new beast mode calc field for this column
        beast_mode = f"COUNT(DISTINCT `{column_name}`)"
        label = f"count_distinct_{column_name}"
        self.calculated_fields_map[column_name] = {
            "label":       label,
            "beast_mode":  beast_mode,
            "is_disabled": False,
            "category":    "auto_count_distinct",
        }
        print(f"   🔄 Auto-converted COUNT_DISTINCT({column_name}) → Beast Mode: {beast_mode}")
        return True

    def _extract_time_grain(self, x_entry):
        if isinstance(x_entry, dict):
            return x_entry.get("column"), x_entry.get("timeGrain", "DAY")
        return x_entry, "DAY"

    def _map_time_grain_to_domo(self, qs_grain):
        grain_map = {
            "YEAR": "YEAR", "QUARTER": "QUARTER", "MONTH": "MONTH",
            "WEEK": "WEEK", "DAY": "DAY", "HOUR": "HOUR", "MINUTE": "MINUTE",
        }
        return grain_map.get(qs_grain.upper(), "DAY")

    def _is_date_column(self, column_name):
        keywords = ["date", "time", "timestamp", "datetime", "day", "month", "year"]
        return any(kw in column_name.lower() for kw in keywords)

    # ------------------------------------------------------------------
    # Visual dispatch
    # ------------------------------------------------------------------

    def _deploy_visual(self, page_id: str, visual: dict):
        # Reset formula id cache so each card gets fresh formula ids
        self._formula_id_cache = {}
        vt = visual["type"].upper()
        dispatch = {
            "TABLE":        self._build_table_payload,
            "BAR":          self._build_bar_payload,
            "STACKED_BAR":  self._build_stacked_bar_payload,
            "KPI":          self._build_kpi_payload,
            "LINE":         self._build_line_payload,
            "AREA":         self._build_area_payload,
            "STACKED_AREA": self._build_stacked_area_payload,
            "PIE":          self._build_pie_payload,
            "DONUT":        self._build_donut_payload,
            "SCATTER":      self._build_scatter_payload,
            "BUBBLE":       self._build_scatter_payload,
            "COMBO":        self._build_combo_payload,
        }
        builder = dispatch.get(vt)
        if not builder:
            raise NotImplementedError(f"Unsupported visual type: {vt}")
        payload = builder(visual)
        return self.client.create_card(page_id, payload)

    # ------------------------------------------------------------------
    # KPI
    # ------------------------------------------------------------------

    def _build_kpi_payload(self, visual):
        dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        m          = visual["measures"][0]
        column_name = self._map_column(m["column"])
        aggregation = self._normalize_aggregation(m["aggregation"])

        # Auto-convert COUNT_DISTINCT to Beast Mode (Domo KPI doesn't support it natively)
        if aggregation == "COUNT_DISTINCT" and not self._is_calc_field(column_name):
            self._convert_count_distinct_to_beast_mode(column_name)

        sub_column = self._make_value_column(column_name, aggregation, "VALUE")

        payload = {
            "definition": {
                "title": visual["title"],
                "subscriptions": {
                    "main": {
                        "name":    "main",
                        "columns": [sub_column],
                        "filters":  [],
                        "groupBy":  [],
                        "distinct": False,
                    }
                },
                "formulas": {"dsUpdated": [], "dsDeleted": [], "card": []},
                "charts": {"main": {"component": "main", "chartType": "badge_singlevalue"}},
            },
            "dataProvider": {"dataSourceId": dataset_id},
            "variables": True,
        }
        return self._inject_formulas(payload, [column_name])

    # ------------------------------------------------------------------
    # BAR
    # ------------------------------------------------------------------

    def _build_bar_payload(self, visual):
        dataset_id  = self.dataset_resolver.resolve(visual["datasetRef"])
        x           = visual["x"][0]
        m           = visual["measures"][0]
        x_mapped    = self._map_column(x)
        column_name = self._map_column(m["column"])
        aggregation = self._normalize_aggregation(m["aggregation"])
        limit_count = visual.get("limit")
        sort_order  = visual.get("sortOrder")
        x_title     = x_mapped.replace("_", " ").title()
        y_title     = f"{aggregation} of {column_name}".replace("_", " ").title()

        if aggregation == "COUNT_DISTINCT" and not self._is_calc_field(column_name):
            self._convert_count_distinct_to_beast_mode(column_name)

        val_col    = self._make_value_column(column_name, aggregation, "VALUE")
        big_num_col = self._make_big_number_column(column_name, aggregation)
        orderby_col = self._make_orderby_column(column_name, aggregation, sort_order)

        payload = {
            "definition": {
                "title": visual.get("title", ""),
                "subscriptions": {
                    "main": {
                        "name":    "main",
                        "columns": [
                            {"column": x_mapped, "mapping": "ITEM"},
                            val_col,
                        ],
                        "filters":  [],
                        "orderBy":  [orderby_col],
                        "groupBy":  [{"column": x_mapped}],
                        "distinct": False,
                        "limit":    limit_count,
                    },
                    "big_number": {
                        "name":    "big_number",
                        "columns": [big_num_col],
                        "filters": [],
                    },
                },
                "formulas": {"dsUpdated": [], "dsDeleted": [], "card": []},
                "annotations": {"new": [], "modified": [], "deleted": []},
                "conditionalFormats": {"card": [], "datasource": []},
                "controls": [],
                "segments": {"active": [], "create": [], "update": [], "delete": []},
                "charts": {
                    "main": {
                        "component": "main",
                        "chartType": "badge_vert_bar",
                        "overrides": {"title_x": x_title, "title_y": y_title},
                    }
                },
                "dynamicTitle": {"text": [{"text": visual.get("title", ""), "type": "TEXT"}]},
                "chartVersion": "12",
            },
            "dataProvider": {"dataSourceId": dataset_id},
            "variables": True,
        }
        return self._inject_formulas(payload, [x_mapped, column_name])

    # ------------------------------------------------------------------
    # STACKED BAR
    # ------------------------------------------------------------------

    def _build_stacked_bar_payload(self, visual):
        dataset_id   = self.dataset_resolver.resolve(visual["datasetRef"])
        x_mapped     = self._map_column(visual["x"][0])
        stack_mapped = self._map_column(visual["stack"][0])
        m            = visual["measures"][0]
        column_name  = self._map_column(m["column"])
        aggregation  = self._normalize_aggregation(m["aggregation"])

        if aggregation == "COUNT_DISTINCT" and not self._is_calc_field(column_name):
            self._convert_count_distinct_to_beast_mode(column_name)

        val_col = self._make_value_column(column_name, aggregation, "VALUE")

        payload = {
            "definition": {
                "title": visual["title"],
                "subscriptions": {
                    "main": {
                        "name":    "main",
                        "columns": [
                            {"column": x_mapped, "mapping": "ITEM"},
                            {"column": stack_mapped, "mapping": "SERIES"},
                            val_col,
                        ],
                        "filters":  [],
                        "groupBy":  [{"column": x_mapped}, {"column": stack_mapped}],
                        "distinct": False,
                    }
                },
                "formulas": {"dsUpdated": [], "dsDeleted": [], "card": []},
                "charts": {
                    "main": {
                        "component": "main",
                        "chartType": "badge_vert_stackedbar",
                        "overrides": {
                            "title_x": x_mapped.replace("_", " ").title(),
                            "title_y": f"{aggregation} of {column_name}".replace("_", " "),
                        },
                        "goal": None,
                    }
                },
            },
            "dataProvider": {"dataSourceId": dataset_id},
            "variables": True,
        }
        return self._inject_formulas(payload, [x_mapped, stack_mapped, column_name])

    # ------------------------------------------------------------------
    # LINE
    # ------------------------------------------------------------------

    def _build_line_payload(self, visual: dict):
        dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        x_entry    = visual["x"][0]
        x_col      = self._map_column(x_entry["column"])
        time_grain = x_entry.get("timeGrain", "DAY")
        domo_grain = self._map_time_grain_to_domo(time_grain)
        calendar_column_map = {
            "DAY": "CalendarDay", "WEEK": "CalendarWeek", "MONTH": "CalendarMonth",
            "QUARTER": "CalendarQuarter", "YEAR": "CalendarYear",
        }
        calendar_column = calendar_column_map.get(time_grain, "CalendarDay")

        m           = visual["measures"][0]
        val_col     = self._map_column(m["column"])
        aggregation = self._normalize_aggregation(m.get("aggregation", "SUM"))

        if aggregation == "COUNT_DISTINCT" and not self._is_calc_field(val_col):
            self._convert_count_distinct_to_beast_mode(val_col)

        stack_fields = visual.get("stack", [])
        has_series   = len(stack_fields) > 0
        series_col   = self._map_column(stack_fields[0]) if has_series else None

        x_title = visual.get("axes", {}).get("x", {}).get("title", "Date")
        y_title = visual.get("axes", {}).get("y", {}).get("title", f"{aggregation} of {val_col}")

        val_sub_col = self._make_value_column(val_col, aggregation, "VALUE")
        big_num_col = self._make_big_number_column(val_col, aggregation)

        main_columns = [
            {"column": calendar_column, "calendar": True, "mapping": "ITEM"},
            val_sub_col,
        ]
        main_groupby = [{"column": calendar_column, "calendar": True}]

        if has_series:
            main_columns.append({"column": series_col, "mapping": "SERIES"})
            main_groupby.append({"column": series_col})

        main_subscription = {
            "name":         "main",
            "dataSourceId": dataset_id,
            "columns":      main_columns,
            "filters":      [],
            "orderBy":      [],
            "groupBy":      main_groupby,
            "dateGrain":    {"column": x_col, "dateTimeElement": domo_grain},
            "fiscal":       False,
            "projection":   False,
            "distinct":     False,
        }

        big_number_subscription = {
            "name":    "big_number",
            "columns": [big_num_col],
            "filters": [], "orderBy": [], "groupBy": [],
            "fiscal": False, "projection": False, "distinct": False, "limit": 1,
        }

        payload = {
            "definition": {
                "subscriptions":      {"big_number": big_number_subscription, "main": main_subscription},
                "charts":             {
                    "main": {
                        "component": "main",
                        "chartType": "badge_two_trendline",
                        "overrides": {"title_x": x_title, "title_y": y_title},
                        "goal":      None,
                    }
                },
                "dynamicTitle":       {"text": [{"type": "TEXT", "text": visual["title"]}]},
                "dynamicDescription": {"text": [], "displayOnCardDetails": True},
                "formulas":           {"card": [], "dsUpdated": [], "dsDeleted": []},
                "annotations":        {"new": [], "modified": [], "deleted": []},
                "conditionalFormats": {"card": [], "datasource": []},
                "controls":           [],
                "segments":           {"active": [], "create": [], "update": [], "delete": []},
                "chartVersion":       "12",
                "inputTable":         False,
                "title":              visual["title"],
                "description":        "",
                "includeEmptyFilters": True,
            },
            "dataProvider": {"dataSourceId": dataset_id},
            "variables":    True,
        }
        all_cols = [x_col, val_col] + ([series_col] if series_col else [])
        return self._inject_formulas(payload, all_cols)

    # ------------------------------------------------------------------
    # TABLE
    # ------------------------------------------------------------------

    def _build_table_payload(self, visual: dict):
        dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        columns, group_by, all_cols = [], [], []

        for col in visual["columns"]:
            column_name = self._map_column(col["field"])
            is_calc     = col.get("is_calc", False) or self._is_calc_field(column_name)
            all_cols.append(column_name)

            if col["type"] == "DIMENSION":
                columns.append({"column": column_name, "mapping": "ITEM"})
                group_by.append({"column": column_name})
            elif col["type"] == "MEASURE":
                aggregation = self._normalize_aggregation(col["aggregation"])
                if aggregation == "COUNT_DISTINCT" and not is_calc:
                    self._convert_count_distinct_to_beast_mode(column_name)
                    is_calc = True
                if is_calc or self._is_calc_field(column_name):
                    columns.append(self._make_beast_mode_subscription_column(column_name, "VALUE"))
                else:
                    columns.append({"column": column_name, "aggregation": aggregation, "mapping": "VALUE"})

        payload = {
            "definition": {
                "title": visual["title"],
                "subscriptions": {
                    "main": {
                        "name":    "main",
                        "columns": columns,
                        "filters":  [],
                        "groupBy":  group_by,
                        "distinct": False,
                    }
                },
                "formulas": {"dsUpdated": [], "dsDeleted": [], "card": []},
                "charts":   {"main": {"component": "main", "chartType": "badge_table", "overrides": {}}},
            },
            "dataProvider": {"dataSourceId": dataset_id},
            "variables":    True,
        }
        return self._inject_formulas(payload, all_cols)

    # ------------------------------------------------------------------
    # AREA
    # ------------------------------------------------------------------

    def _build_area_payload(self, visual: dict):
        dataset_id   = self.dataset_resolver.resolve(visual["datasetRef"])
        x_entry      = visual["x"][0]
        x_col_raw    = x_entry["column"] if isinstance(x_entry, dict) else x_entry
        x_col_mapped = self._map_column(x_col_raw)
        time_grain   = x_entry.get("timeGrain", "DAY") if isinstance(x_entry, dict) else "DAY"
        domo_grain   = self._map_time_grain_to_domo(time_grain)
        is_date      = self._is_date_column(x_col_mapped)
        calendar_map = {
            "DAY": "CalendarDay", "WEEK": "CalendarWeek", "MONTH": "CalendarMonth",
            "QUARTER": "CalendarQuarter", "YEAR": "CalendarYear",
        }
        calendar_column = calendar_map.get(time_grain, "CalendarDay") if is_date else x_col_mapped

        m           = visual["measures"][0]
        val_col     = self._map_column(m["column"])
        aggregation = self._normalize_aggregation(m.get("aggregation", "SUM"))

        if aggregation == "COUNT_DISTINCT" and not self._is_calc_field(val_col):
            self._convert_count_distinct_to_beast_mode(val_col)

        val_sub_col = self._make_value_column(val_col, aggregation, "VALUE")
        big_num_col = self._make_big_number_column(val_col, aggregation)

        main_subscription = {
            "name": "main", "dataSourceId": dataset_id,
            "columns": [
                {"column": calendar_column, "calendar": is_date, "mapping": "ITEM"},
                val_sub_col,
            ],
            "filters": [], "orderBy": [],
            "groupBy": [{"column": calendar_column, "calendar": is_date}],
            "fiscal": False, "projection": False, "distinct": False,
        }
        if is_date:
            main_subscription["dateGrain"] = {"column": x_col_mapped, "dateTimeElement": domo_grain}

        big_number_subscription = {
            "name": "big_number", "dataSourceId": dataset_id,
            "columns": [big_num_col],
            "filters": [], "orderBy": [], "groupBy": [],
            "fiscal": False, "projection": False, "distinct": False, "limit": 1,
        }

        payload = {
            "definition": {
                "subscriptions": {"main": main_subscription, "big_number": big_number_subscription},
                "charts": {
                    "main": {
                        "component": "main",
                        "chartType": "badge_vert_area_overlay",
                        "overrides": {
                            "title_x": visual.get("axes", {}).get("x", {}).get("title", ""),
                            "title_y": visual.get("axes", {}).get("y", {}).get("title", ""),
                        },
                        "goal": None,
                    }
                },
                "dynamicTitle":       {"text": [{"type": "TEXT", "text": visual["title"]}]},
                "dynamicDescription": {"text": [], "displayOnCardDetails": True},
                "formulas":           {"card": [], "dsUpdated": [], "dsDeleted": []},
                "annotations":        {"new": [], "modified": [], "deleted": []},
                "conditionalFormats": {"card": [], "datasource": []},
                "controls":           [],
                "segments":           {"active": [], "create": [], "update": [], "delete": []},
                "chartVersion":       "12", "inputTable": False,
                "title":              visual["title"], "description": "",
            },
            "dataProvider": {"dataSourceId": dataset_id},
            "variables":    True,
        }
        return self._inject_formulas(payload, [x_col_mapped, val_col])

    # ------------------------------------------------------------------
    # STACKED AREA
    # ------------------------------------------------------------------

    def _build_stacked_area_payload(self, visual: dict):
        dataset_id      = self.dataset_resolver.resolve(visual["datasetRef"])
        x_entry         = visual["x"][0]
        x_col           = self._map_column(x_entry["column"])
        time_grain      = x_entry.get("timeGrain", "DAY")
        domo_grain      = self._map_time_grain_to_domo(time_grain)
        calendar_map    = {
            "DAY": "CalendarDay", "WEEK": "CalendarWeek", "MONTH": "CalendarMonth",
            "QUARTER": "CalendarQuarter", "YEAR": "CalendarYear",
        }
        calendar_column = calendar_map.get(time_grain, "CalendarDay")
        measure         = visual["measures"][0]
        value_col       = self._map_column(measure["column"])
        aggregation     = self._normalize_aggregation(measure["aggregation"])
        stack_col       = self._map_column(visual["stack"][0])

        val_sub_col = self._make_value_column(value_col, aggregation, "VALUE")

        main_subscription = {
            "name": "main", "dataSourceId": dataset_id,
            "columns": [
                {"column": calendar_column, "calendar": True, "mapping": "ITEM"},
                val_sub_col,
                {"column": stack_col, "mapping": "SERIES"},
            ],
            "filters": [], "orderBy": [],
            "groupBy": [{"column": calendar_column, "calendar": True}, {"column": stack_col}],
            "dateGrain": {"column": x_col, "dateTimeElement": domo_grain},
            "fiscal": False, "projection": False, "distinct": False,
        }

        payload = {
            "definition": {
                "subscriptions": {"main": main_subscription},
                "charts": {
                    "main": {
                        "component": "main",
                        "chartType": "badge_stackedtrend",
                        "overrides": {
                            "title_x": visual.get("axes", {}).get("x", {}).get("title", ""),
                            "title_y": visual.get("axes", {}).get("y", {}).get("title", ""),
                        },
                        "goal": None,
                    }
                },
                "dynamicTitle":       {"text": [{"type": "TEXT", "text": visual.get("title", "")}]},
                "dynamicDescription": {"text": [], "displayOnCardDetails": True},
                "formulas":           {"card": [], "dsUpdated": [], "dsDeleted": []},
                "annotations":        {"new": [], "modified": [], "deleted": []},
                "conditionalFormats": {"card": [], "datasource": []},
                "controls":           [],
                "segments":           {"active": [], "create": [], "update": [], "delete": []},
                "chartVersion":       "12", "inputTable": False,
                "title":              visual.get("title", ""), "description": "",
                "includeEmptyFilters": True,
            },
            "dataProvider": {"dataSourceId": dataset_id},
            "variables":    True,
        }
        return self._inject_formulas(payload, [x_col, value_col, stack_col])

    # ------------------------------------------------------------------
    # PIE
    # ------------------------------------------------------------------

    def _build_pie_payload(self, visual: dict):
        dataset_id  = self.dataset_resolver.resolve(visual["datasetRef"])
        category    = (visual.get("categories") or visual.get("x") or [None])[0]
        m           = visual["measures"][0]
        cat_mapped  = self._map_column(category)
        column_name = self._map_column(m["column"])
        aggregation = self._normalize_aggregation(m["aggregation"])
        is_calc     = m.get("is_calc", False) or self._is_calc_field(column_name)

        if aggregation == "COUNT_DISTINCT" and not is_calc:
            self._convert_count_distinct_to_beast_mode(column_name)
            is_calc = True

        print(f"PIE: category={cat_mapped}, measure={column_name}, is_calc={is_calc or self._is_calc_field(column_name)}")

        val_col = self._make_value_column(column_name, aggregation, "VALUE")

        payload = {
            "definition": {
                "title": visual["title"],
                "subscriptions": {
                    "main": {
                        "name":    "main",
                        "columns": [
                            {"column": cat_mapped, "mapping": "ITEM"},
                            val_col,
                        ],
                        "filters":  [],
                        "groupBy":  [{"column": cat_mapped}],
                        "distinct": False,
                    }
                },
                "formulas": {"dsUpdated": [], "dsDeleted": [], "card": []},
                "charts":   {"main": {"component": "main", "chartType": "badge_pie"}},
            },
            "dataProvider": {"dataSourceId": dataset_id},
            "variables":    True,
        }
        return self._inject_formulas(payload, [cat_mapped, column_name])

    # ------------------------------------------------------------------
    # DONUT
    # ------------------------------------------------------------------

    def _build_donut_payload(self, visual: dict):
        payload = self._build_pie_payload(visual)
        payload["definition"]["charts"]["main"]["chartType"] = "badge_donut"
        return payload

    # ------------------------------------------------------------------
    # SCATTER / BUBBLE
    # ------------------------------------------------------------------

    def _build_scatter_payload(self, visual: dict):
        dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        measures   = visual.get("measures", [])
        x_m = next((m for m in measures if m.get("mapping") == "XAXIS"), None)
        y_m = next((m for m in measures if m.get("mapping") == "YAXIS"), None)
        if not x_m or not y_m:
            if len(measures) >= 2:
                x_m, y_m = measures[0], measures[1]
            else:
                raise ValueError("Scatter requires at least 2 measures.")

        x_col  = self._map_column(x_m["column"])
        x_agg  = self._normalize_aggregation(x_m["aggregation"])
        y_col  = self._map_column(y_m["column"])
        y_agg  = self._normalize_aggregation(y_m["aggregation"])
        categories = visual.get("categories", [])
        cat_mapped = self._map_column(categories[0] if categories else "id")
        is_date    = self._is_date_column(cat_mapped)

        main_subscription = {
            "name": "main", "dataSourceId": dataset_id,
            "columns": [
                {"column": cat_mapped, "calendar": is_date, "mapping": "XTIME"},
                self._make_value_column(x_col, x_agg, "VALUE"),
                self._make_value_column(y_col, y_agg, "SERIES"),
            ],
            "filters": [], "orderBy": [],
            "groupBy": [{"column": cat_mapped, "calendar": is_date}],
            "fiscal": False, "projection": False, "distinct": False,
        }
        if is_date:
            main_subscription["dateGrain"] = {"column": cat_mapped, "dateTimeElement": "DAY"}

        big_number_subscription = {
            "name": "big_number", "dataSourceId": dataset_id,
            "columns": [self._make_big_number_column(y_col, y_agg)],
            "filters": [],
        }

        payload = {
            "definition": {
                "subscriptions": {"main": main_subscription, "big_number": big_number_subscription},
                "charts": {
                    "main": {
                        "component": "main",
                        "chartType": "badge_xybubble",
                        "overrides": {
                            "title_x": visual.get("axes", {}).get("x", {}).get("title", x_col),
                            "title_y": visual.get("axes", {}).get("y", {}).get("title", y_col),
                        },
                        "goal": None,
                    }
                },
                "dynamicTitle":       {"text": [{"type": "TEXT", "text": visual["title"]}]},
                "dynamicDescription": {"text": [], "displayOnCardDetails": True},
                "formulas":           {"card": [], "dsUpdated": [], "dsDeleted": []},
                "annotations":        {"new": [], "modified": [], "deleted": []},
                "conditionalFormats": {"card": [], "datasource": []},
                "controls":           [],
                "segments":           {"active": [], "create": [], "update": [], "delete": []},
                "chartVersion":       "12", "inputTable": False,
                "title":              visual["title"], "description": "",
                "includeEmptyFilters": True,
            },
            "dataProvider": {"dataSourceId": dataset_id},
            "variables":    True,
        }
        return self._inject_formulas(payload, [cat_mapped, x_col, y_col])

    # ------------------------------------------------------------------
    # COMBO
    # ------------------------------------------------------------------

    def _build_combo_payload(self, visual: dict):
        dataset_id    = self.dataset_resolver.resolve(visual["datasetRef"])
        x_entry       = visual["x"][0]
        x_col, time_grain = self._extract_time_grain(x_entry)
        x_col_mapped  = self._map_column(x_col)
        is_date_field = self._is_date_column(x_col_mapped)
        domo_grain    = self._map_time_grain_to_domo(time_grain) if time_grain else "DAY"

        bar_measures  = visual.get("barMeasures", [])
        line_measures = visual.get("lineMeasures", [])
        series_list   = visual.get("series", [])

        if not bar_measures or not line_measures:
            raise ValueError("COMBO requires at least one BAR and one LINE measure")

        bar_col  = self._map_column(bar_measures[0]["column"])
        bar_agg  = self._normalize_aggregation(bar_measures[0]["aggregation"])
        line_col = self._map_column(line_measures[0]["column"])
        line_agg = self._normalize_aggregation(line_measures[0]["aggregation"])
        series_col = self._map_column(series_list[0]) if series_list else None

        subscription_columns = [
            {"column": x_col_mapped, "mapping": "ITEM"},
            self._make_value_column(bar_col, bar_agg, "VALUE"),
            self._make_value_column(line_col, line_agg, "VALUE"),
        ]
        group_by = [{"column": x_col_mapped}]
        if series_col:
            subscription_columns.append({"column": series_col, "mapping": "SERIES"})
            group_by.append({"column": series_col})

        main_subscription = {
            "name": "main", "dataSourceId": dataset_id,
            "columns": subscription_columns,
            "filters": [], "orderBy": [], "groupBy": group_by,
            "fiscal": False, "projection": False, "distinct": False,
        }
        if is_date_field:
            main_subscription["dateGrain"] = {"column": x_col_mapped, "dateTimeElement": domo_grain}

        big_number_subscription = {
            "name": "big_number", "dataSourceId": dataset_id,
            "columns": [self._make_big_number_column(bar_col, bar_agg)],
            "filters": [], "orderBy": [], "groupBy": [],
            "fiscal": False, "projection": False, "distinct": False, "limit": 1,
        }

        all_cols = [x_col_mapped, bar_col, line_col] + ([series_col] if series_col else [])

        payload = {
            "definition": {
                "subscriptions":      {"big_number": big_number_subscription, "main": main_subscription},
                "formulas":           {"dsUpdated": [], "dsDeleted": [], "card": []},
                "conditionalFormats": {"card": [], "datasource": []},
                "annotations":        {"new": [], "modified": [], "deleted": []},
                "dynamicTitle":       {"text": [{"text": visual.get("title", "Combo Chart"), "type": "TEXT"}]},
                "dynamicDescription": {"text": [], "displayOnCardDetails": True},
                "chartVersion":       "12",
                "charts":             {
                    "main": {
                        "component": "main",
                        "chartType": "badge_line_stackedbar",
                        "overrides": {}, "goal": None,
                    }
                },
                "allowTableDrill": True,
                "segments":        {"active": [], "definitions": []},
                "controls":        [], "inputTable": False,
            },
            "dataProvider": {"dataSourceId": dataset_id},
            "variables":    True,
        }
        return self._inject_formulas(payload, all_cols)
