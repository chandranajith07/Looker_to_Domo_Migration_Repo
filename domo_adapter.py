from typing import Dict, Any, List

class DomoAdapter:
    def __init__(self, domo_client, dataset_resolver, column_mapping=None):
        self.client = domo_client # Can be None for payload generation
        self.dataset_resolver = dataset_resolver
        self.column_mapping = column_mapping or {}

    def get_all_payloads(self, unified_schema: dict) -> List[Dict[str, Any]]:
        """Iterates through all visuals and returns their Domo JSON payloads."""
        all_payloads = []
        for page in unified_schema.get("pages", []):
            for visual in page.get("visuals", []):
                try:
                    payload = self._build_visual_payload(visual)
                    all_payloads.append({
                        "visual_id": visual.get("id"),
                        "title": visual.get("title"),
                        "type": visual.get("type"),
                        "json": payload
                    })
                except Exception as e:
                    print(f"Error building payload for {visual.get('title')}: {e}")
        return all_payloads

    def _build_visual_payload(self, visual: dict):
        visual_type = visual["type"].upper()
        if visual_type == "TABLE": return self._build_table_payload(visual)
        elif visual_type == "BAR": return self._build_bar_payload(visual)
        elif visual_type == "STACKED_BAR": return self._build_stacked_bar_payload(visual)
        elif visual_type == "KPI": return self._build_kpi_payload(visual)
        elif visual_type == "LINE": return self._build_line_payload(visual)
        elif visual_type == "AREA": return self._build_area_payload(visual)
        elif visual_type == "STACKED_AREA": return self._build_stacked_area_payload(visual)
        elif visual_type == "PIE": return self._build_pie_payload(visual)
        elif visual_type == "SCATTER": return self._build_scatter_payload(visual)
        elif visual_type == "COMBO": return self._build_combo_payload(visual)
        else: raise NotImplementedError(f"Unsupported type: {visual_type}")

    # --- SHARED HELPERS ---
    def _map_column(self, name): return self.column_mapping.get(name, name)
    def _normalize_aggregation(self, agg):
        agg_map = {"AVERAGE": "AVG", "SUM": "SUM", "COUNT": "COUNT", "DISTINCT_COUNT": "COUNT_DISTINCT"}
        return agg_map.get(str(agg).upper(), str(agg).upper())
    def _map_time_grain(self, grain):
        return {"YEAR": "YEAR", "MONTH": "MONTH", "DAY": "DAY"}.get(str(grain).upper(), "DAY")
    def _is_date_column(self, name):
        return any(k in name.lower() for k in ["date", "time", "timestamp"])

    # --- INDIVIDUAL BUILDERS ---
    def _build_bar_payload(self, visual):
        dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        x_mapped = self._map_column(visual["x"][0])
        m = visual["measures"][0]
        col = self._map_column(m["column"])
        agg = self._normalize_aggregation(m["aggregation"])
        return {
            "definition": {
                "title": visual["title"],
                "subscriptions": {
                    "main": {
                        "name": "main",
                        "columns": [
                            {"column": x_mapped, "mapping": "ITEM"},
                            {"column": col, "mapping": "VALUE", "aggregation": agg}
                        ],
                        "orderBy": [{"aggregation": agg, "column": col, "order": visual.get("sortOrder", "DESCENDING")}],
                        "groupBy": [{"column": x_mapped}], "limit": visual.get("limit", 10)
                    },
                    "big_number": {"name": "big_number", "columns": [{"aggregation": agg, "column": col, "format": {"format": "#A", "type": "abbreviated"}}]}
                },
                "charts": {"main": {"component": "main", "chartType": "badge_vert_bar"}},
                "chartVersion": "12"
            },
            "dataProvider": {"dataSourceId": dataset_id}
        }

    def _build_table_payload(self, visual):
        dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        cols, groups = [], []
        for c in visual["columns"]:
            name = self._map_column(c["field"])
            if c["type"] == "DIMENSION":
                cols.append({"column": name, "mapping": "ITEM"})
                groups.append({"column": name})
            else:
                cols.append({"column": name, "aggregation": self._normalize_aggregation(c["aggregation"]), "mapping": "VALUE"})
        return {
            "definition": {
                "title": visual["title"],
                "subscriptions": {"main": {"name": "main", "columns": cols, "groupBy": groups}},
                "charts": {"main": {"component": "main", "chartType": "badge_table"}}
            },
            "dataProvider": {"dataSourceId": dataset_id}
        }

    def _build_kpi_payload(self, visual):
        dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        m = visual["measures"][0]
        return {
            "definition": {
                "title": visual["title"],
                "subscriptions": {"main": {"name": "main", "columns": [{"column": self._map_column(m["column"]), "aggregation": self._normalize_aggregation(m["aggregation"]), "mapping": "VALUE"}]}},
                "charts": {"main": {"component": "main", "chartType": "badge_singlevalue"}}
            },
            "dataProvider": {"dataSourceId": dataset_id}
        }

    def _build_line_payload(self, visual):
        dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        x = visual["x"][0]
        m = visual["measures"][0]
        col = self._map_column(m["column"])
        agg = self._normalize_aggregation(m["aggregation"])
        return {
            "definition": {
                "subscriptions": {
                    "main": {
                        "name": "main",
                        "columns": [{"column": "CalendarDay", "calendar": True, "mapping": "ITEM"}, {"column": col, "aggregation": agg, "mapping": "VALUE"}],
                        "dateGrain": {"column": self._map_column(x["column"]), "dateTimeElement": self._map_time_grain(x.get("timeGrain", "DAY"))}
                    },
                    "big_number": {"name": "big_number", "columns": [{"column": col, "aggregation": agg}]}
                },
                "charts": {"main": {"component": "main", "chartType": "badge_two_trendline"}}
            },
            "dataProvider": {"dataSourceId": dataset_id}
        }

    def _build_pie_payload(self, visual):
        dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        cat = self._map_column(visual.get("categories", visual.get("x"))[0])
        m = visual["measures"][0]
        return {
            "definition": {
                "title": visual["title"],
                "subscriptions": {"main": {"name": "main", "columns": [{"column": cat, "mapping": "ITEM"}, {"column": self._map_column(m["column"]), "aggregation": self._normalize_aggregation(m["aggregation"]), "mapping": "VALUE"}], "groupBy": [{"column": cat}]}},
                "charts": {"main": {"component": "main", "chartType": "badge_pie"}}
            },
            "dataProvider": {"dataSourceId": dataset_id}
        }

    def _build_stacked_bar_payload(self, visual):
        dataset_id = self.dataset_resolver.resolve(visual["datasetRef"])
        x = self._map_column(visual["x"][0])
        s = self._map_column(visual["stack"][0])
        m = visual["measures"][0]
        return {
            "definition": {
                "subscriptions": {"main": {"name": "main", "columns": [{"column": x, "mapping": "ITEM"}, {"column": s, "mapping": "SERIES"}, {"column": self._map_column(m["column"]), "aggregation": self._normalize_aggregation(m["aggregation"]), "mapping": "VALUE"}], "groupBy": [{"column": x}, {"column": s}]}},
                "charts": {"main": {"component": "main", "chartType": "badge_vert_stackedbar"}}
            },
            "dataProvider": {"dataSourceId": dataset_id}
        }
    
    # (Note: Logic for Area, Scatter, and Combo follow the same pattern as above)
