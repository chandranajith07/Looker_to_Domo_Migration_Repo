"""
calc_field_translator.py

Translates Looker dynamic_fields (table calculations & custom measures)
into Domo Beast Mode SQL expressions.

KEY FIX: Looker API returns dynamic fields with _kind_hint / _type_hint
instead of the "category" key used by the Looker UI. This version
infers category from _kind_hint when category is absent.
"""

import re
import json


_FIELD_REF_RE = re.compile(r'\$\{([^}]+)\}')

_AGG_TYPE_MAP = {
    "sum":            "SUM",
    "count":          "COUNT",
    "count_distinct": "COUNT_DISTINCT",
    "average":        "AVG",
    "avg":            "AVG",
    "min":            "MIN",
    "max":            "MAX",
    "median":         "MEDIAN",
    "list":           "GROUP_CONCAT",
    "yesno":          "SUM",
    "number":         "SUM",
    "string":         "MAX",
}


def _strip_view_prefix(field_name: str) -> str:
    if "." in field_name:
        return field_name.split(".")[-1]
    return field_name


def _quote_column(col: str) -> str:
    col = col.strip()
    if not col:
        return col
    if col.startswith("`"):
        return col
    sql_keywords = {"NULL", "TRUE", "FALSE", "AND", "OR", "NOT", "IN", "IS",
                    "LIKE", "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END"}
    if col.upper() in sql_keywords:
        return col
    try:
        float(col)
        return col
    except ValueError:
        pass
    if col.startswith("'") or col.startswith('"'):
        return col
    return f"`{col}`"


def _replace_field_refs(expression: str, column_mapping: dict) -> str:
    def _sub(m):
        raw  = m.group(1)
        bare = _strip_view_prefix(raw)
        mapped = column_mapping.get(bare, bare)
        return _quote_column(mapped)
    return _FIELD_REF_RE.sub(_sub, expression)


def _rewrite_count_distinct(expression: str) -> str:
    return re.compile(r'\bcount_distinct\s*\(', re.IGNORECASE).sub(
        'COUNT(DISTINCT ', expression)


def _rewrite_percent_of_total(expression: str) -> str:
    pct_re = re.compile(r'\bpercent_of_total\s*\(([^)]+)\)', re.IGNORECASE)
    return pct_re.sub(
        lambda m: f"SUM({m.group(1)}) / SUM(SUM({m.group(1)})) OVER ()",
        expression)


def _rewrite_to_string(expression: str) -> str:
    ts_re = re.compile(r'\bto_string\s*\(([^)]+)\)', re.IGNORECASE)
    return ts_re.sub(lambda m: f"CAST({m.group(1)} AS CHAR)", expression)


def _wrap_bare_columns_with_sum(expression: str) -> str:
    has_agg = re.search(
        r'\b(SUM|COUNT|AVG|MIN|MAX|COUNT_DISTINCT|MEDIAN|GROUP_CONCAT)\s*[\(`]',
        expression, re.IGNORECASE)
    if has_agg:
        return expression
    return re.sub(r'`[^`]+`', lambda m: f"SUM({m.group(0)})", expression)


def looker_expression_to_beast_mode(expression: str, column_mapping: dict = None) -> str:
    if not expression or not expression.strip():
        return ""
    column_mapping = column_mapping or {}
    sql = _replace_field_refs(expression, column_mapping)
    sql = _rewrite_count_distinct(sql)
    sql = _rewrite_percent_of_total(sql)
    sql = _rewrite_to_string(sql)
    sql = _wrap_bare_columns_with_sum(sql)
    sql = re.sub(r'\s+', ' ', sql).strip()
    return sql


def looker_measure_to_beast_mode(agg_type: str, based_on_field: str,
                                  column_mapping: dict = None) -> str:
    column_mapping = column_mapping or {}
    bare   = _strip_view_prefix(based_on_field)
    mapped = column_mapping.get(bare, bare)
    quoted = _quote_column(mapped)
    bm_fn  = _AGG_TYPE_MAP.get(agg_type.lower(), "SUM")
    if bm_fn == "COUNT_DISTINCT":
        return f"COUNT(DISTINCT {quoted})"
    return f"{bm_fn}({quoted})"


def _infer_category(f: dict) -> str:
    """
    Looker's REST API omits the 'category' key and uses '_kind_hint' instead.
    Infer the logical category so we can process the field correctly.

    Rules:
      - explicit category present → use it directly
      - has 'table_calculation' key → "table_calculation"
      - _kind_hint == "measure"  OR has 'measure' key → "measure"
      - fallback → "measure" (safe default for numeric dynamic fields)
    """
    explicit = f.get("category", "")
    if explicit:
        return explicit

    if f.get("table_calculation"):
        return "table_calculation"

    kind_hint = f.get("_kind_hint", "")
    if kind_hint == "measure" or f.get("measure"):
        return "measure"

    # Last resort: if it has based_on or type that looks like an agg → measure
    if f.get("based_on") or f.get("type") in _AGG_TYPE_MAP:
        return "measure"

    return "measure"   # safe default


def parse_dynamic_fields(dynamic_fields_raw, column_mapping: dict = None) -> list:
    """
    Parse Looker dynamic_fields (JSON string or list) and return unified
    calculated-field dicts.

    Handles both:
      - UI-originated fields with explicit "category" key
      - API-originated fields with "_kind_hint" / "_type_hint" (no "category")

    Returns list of:
        {
            "name":        str,
            "label":       str,
            "beast_mode":  str,   # Beast Mode SQL
            "is_disabled": bool,
            "category":    str,   # "measure" | "table_calculation"
        }
    """
    column_mapping = column_mapping or {}

    if isinstance(dynamic_fields_raw, str):
        try:
            fields = json.loads(dynamic_fields_raw)
        except (json.JSONDecodeError, TypeError):
            return []
    elif isinstance(dynamic_fields_raw, list):
        fields = dynamic_fields_raw
    else:
        return []

    results = []

    for f in fields:
        category    = _infer_category(f)          # ← KEY FIX
        label       = f.get("label", "")
        is_disabled = f.get("is_disabled", False)

        if category == "table_calculation":
            tc_name    = f.get("table_calculation", label)
            expression = f.get("expression", "")
            beast_mode = looker_expression_to_beast_mode(expression, column_mapping)
            results.append({
                "name":        tc_name,
                "label":       label,
                "beast_mode":  beast_mode,
                "is_disabled": bool(is_disabled),
                "category":    "table_calculation",
            })

        elif category == "measure":
            # API format: uses "measure" key as name; UI format: uses "measure" key too
            measure_name = f.get("measure") or f.get("table_calculation") or label
            agg_type     = f.get("type", "sum")
            based_on     = f.get("based_on", "")
            expression   = f.get("expression") or ""

            if expression.strip():
                beast_mode = looker_expression_to_beast_mode(expression, column_mapping)
            elif based_on:
                beast_mode = looker_measure_to_beast_mode(agg_type, based_on, column_mapping)
            else:
                beast_mode = ""

            results.append({
                "name":        measure_name,
                "label":       label,
                "beast_mode":  beast_mode,
                "is_disabled": bool(is_disabled),
                "category":    "measure",
            })

    return results


# Alias for backward compatibility
def qs_to_beast_mode_sql(expression: str, column_mapping: dict = None) -> str:
    return looker_expression_to_beast_mode(expression, column_mapping)
