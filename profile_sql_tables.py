"""
SQL Server Table Profiler
=========================
Connects to SQL Server, profiles the specified tables, and generates
knowledge YAML files consumed by 0_knowledge_processor.py:

  - schema_columns_<tag>.yaml  — column metadata (dict keyed by column name)
  - column_values_<tag>.yaml   — distinct values per column
  - data_context_<tag>.yaml    — table-level stats, date ranges, row counts

Usage:
    python profile_sql_tables.py \
        --server   "your-server.database.windows.net" \
        --port     1433 \
        --database "YourDB" \
        --username "sa" \
        --password "secret" \
        --schema   "dbo" \
        --tables   "VW_DIRECT_SPEND_ALL,VW_INDIRECT_SPEND_ALL" \
        --output-dir "./knowledge_profiled" \
        [--top-values 20]        # number of distinct values to sample

Prerequisites:
    pip install pyodbc pyyaml python-dotenv
"""

import argparse
import os
import sys
import time
from collections import OrderedDict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyodbc
import yaml
from dotenv import load_dotenv

load_dotenv()

# Mandatory row-level filter applied to every profiling query.
# Keep profiling aligned with the pipeline's business cut-off.
# Set MANDATORY_FILTER = "" to disable.
MANDATORY_FILTER = "INVOICE_DATE >= '2024-04-01'"


def _where(extra: str | None = None, base_filter: str | None = None) -> str:
    """Build a WHERE clause.

    base_filter=None  -> use MANDATORY_FILTER
    base_filter=""    -> skip the mandatory filter (e.g. when querying a
                         temp table that is already filtered).
    base_filter="..." -> use that literal predicate.
    """
    parts = []
    filt = MANDATORY_FILTER if base_filter is None else base_filter
    if filt:
        parts.append(filt)
    if extra:
        parts.append(f"({extra})")
    return ("WHERE " + " AND ".join(parts)) if parts else ""


# ──────────────────────────────────────────────────────────────────────
# YAML setup — preserve key order, clean output
# ──────────────────────────────────────────────────────────────────────

def _yaml_str_representer(dumper, data):
    """Use block style for long strings, plain style otherwise."""
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)

def _yaml_ordered_representer(dumper, data):
    return dumper.represent_mapping("tag:yaml.org,2002:map", data.items())

yaml.add_representer(str, _yaml_str_representer)
yaml.add_representer(OrderedDict, _yaml_ordered_representer)


def _safe_value(val):
    """Convert DB values to YAML-safe Python types."""
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (datetime, date)):
        return str(val)
    if isinstance(val, bytes):
        return val.hex()
    return val


# ──────────────────────────────────────────────────────────────────────
# SQL Server connection
# ──────────────────────────────────────────────────────────────────────

def connect(args) -> pyodbc.Connection:
    """Build a pyodbc connection to SQL Server.

    Aligns session SET options with SSMS/DBeaver defaults so the server
    picks the same cached execution plan. Without SET ARITHABORT ON,
    pyodbc gets a separate (often worse) plan for identical SQL —
    the classic "fast in DBeaver, slow in app" cause.
    """
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={args.server},{args.port};"
        f"DATABASE={args.database};"
        f"UID={args.username};"
        f"PWD={args.password};"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )
    print(f"Connecting to {args.server}:{args.port}/{args.database} ...")
    conn = pyodbc.connect(conn_str, timeout=30)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET ARITHABORT ON")
    cur.execute("SET ANSI_NULLS ON")
    cur.execute("SET ANSI_WARNINGS ON")
    cur.execute("SET CONCAT_NULL_YIELDS_NULL ON")
    cur.execute("SET QUOTED_IDENTIFIER ON")
    cur.close()
    print("Connected.\n")
    return conn


# ──────────────────────────────────────────────────────────────────────
# 1. Schema Columns Profiler
# ──────────────────────────────────────────────────────────────────────

def profile_schema(cur, schema: str, table: str, top_values: int,
                   source_sql: str | None = None,
                   filter_clause: str | None = None) -> dict:
    """
    For each column: name, type, nullable, cardinality, min, max,
    null_count, null_pct, and top N distinct values.

    source_sql    — FROM fragment to query (default: [schema].[table]).
                    Pass a temp table name (e.g. "[#spend]") to bypass the
                    view and profile pre-materialized data.
    filter_clause — override MANDATORY_FILTER; pass "" when the source_sql
                    is already filtered (avoids redundant WHERE evaluation).
    """
    if source_sql is None:
        source_sql = f"[{schema}].[{table}]"
    # INFORMATION_SCHEMA lookup always uses the original view name
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
               NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, (schema, table))
    columns_meta = cur.fetchall()

    # Get total row count
    cur.execute(f"SELECT COUNT(*) FROM {source_sql} {_where(base_filter=filter_clause)}")
    total_rows = cur.fetchone()[0]

    columns = []
    for col_name, data_type, char_len, num_prec, num_scale, is_nullable in columns_meta:
        print(f"    Profiling column: {col_name} ...", end=" ", flush=True)
        start = time.time()

        col_info = OrderedDict()
        col_info["name"] = col_name
        col_info["data_type"] = data_type.upper()

        # Build display type
        if char_len and char_len > 0:
            col_info["display_type"] = f"{data_type.upper()}({char_len})"
        elif num_prec and num_scale is not None and num_scale > 0:
            col_info["display_type"] = f"{data_type.upper()}({num_prec},{num_scale})"
        elif num_prec:
            col_info["display_type"] = f"{data_type.upper()}({num_prec})"
        else:
            col_info["display_type"] = data_type.upper()

        col_info["nullable"] = is_nullable == "YES"

        # Cardinality (distinct count) + null count
        safe_col = f"[{col_name}]"
        try:
            cur.execute(f"""
                SELECT
                    COUNT(DISTINCT {safe_col}) AS distinct_count,
                    SUM(CASE WHEN {safe_col} IS NULL THEN 1 ELSE 0 END) AS null_count
                FROM {source_sql}
                {_where(base_filter=filter_clause)}
            """)
            row = cur.fetchone()
            col_info["cardinality"] = row[0]
            col_info["null_count"] = row[1]
            col_info["null_pct"] = round(row[1] / total_rows * 100, 2) if total_rows > 0 else 0
        except Exception:
            col_info["cardinality"] = None
            col_info["null_count"] = None
            col_info["null_pct"] = None

        # Min / Max for numeric and date columns
        if data_type.lower() in ("int", "bigint", "smallint", "tinyint", "decimal",
                                  "numeric", "float", "real", "money", "smallmoney",
                                  "date", "datetime", "datetime2", "smalldatetime"):
            try:
                cur.execute(f"""
                    SELECT MIN({safe_col}), MAX({safe_col})
                    FROM {source_sql}
                    {_where(base_filter=filter_clause)}
                """)
                min_val, max_val = cur.fetchone()
                col_info["min_value"] = _safe_value(min_val)
                col_info["max_value"] = _safe_value(max_val)
            except Exception:
                pass

        # Top N distinct values with counts
        try:
            cur.execute(f"""
                SELECT TOP {top_values} {safe_col} AS val, COUNT(*) AS cnt
                FROM {source_sql}
                {_where(f"{safe_col} IS NOT NULL", base_filter=filter_clause)}
                GROUP BY {safe_col}
                ORDER BY cnt DESC
            """)
            top_vals = cur.fetchall()
            col_info["top_values"] = [
                {"value": _safe_value(r[0]), "count": r[1]}
                for r in top_vals
            ]
        except Exception:
            col_info["top_values"] = []

        elapsed = time.time() - start
        print(f"({elapsed:.1f}s)")
        columns.append(col_info)

    return {
        "total_rows": total_rows,
        "column_count": len(columns),
        "columns": columns,
    }


# ──────────────────────────────────────────────────────────────────────
# 2. Column Values File (distinct values for categorical/low-cardinality)
# ──────────────────────────────────────────────────────────────────────

def profile_column_values(cur, schema: str, table: str,
                          schema_profile: dict, max_distinct: int = 200,
                          top_values: int = 20,
                          source_sql: str | None = None,
                          filter_clause: str | None = None) -> dict:
    """
    For columns with cardinality <= max_distinct, fetch ALL distinct values.
    For high-cardinality columns, just the top N by frequency.

    See profile_schema for source_sql / filter_clause semantics.
    """
    if source_sql is None:
        source_sql = f"[{schema}].[{table}]"
    columns = {}
    for col in schema_profile["columns"]:
        col_name = col["name"]
        cardinality = col.get("cardinality") or 0
        safe_col = f"[{col_name}]"

        # Skip columns with cardinality = 0 or 1
        if cardinality <= 1:
            continue

        # Low cardinality: fetch all distinct values
        if cardinality <= max_distinct:
            try:
                cur.execute(f"""
                    SELECT DISTINCT {safe_col}
                    FROM {source_sql}
                    {_where(f"{safe_col} IS NOT NULL", base_filter=filter_clause)}
                    ORDER BY {safe_col}
                """)
                vals = [_safe_value(r[0]) for r in cur.fetchall()]
                columns[col_name] = {
                    "cardinality": cardinality,
                    "type": "exhaustive",
                    "values": vals,
                }
            except Exception:
                pass
        else:
            # High cardinality: just reference top values from schema profile
            top_vals = col.get("top_values", [])
            columns[col_name] = {
                "cardinality": cardinality,
                "type": "sampled_top_n",
                "note": f"High cardinality — showing top {len(top_vals)} by frequency",
                "values": [v["value"] for v in top_vals],
            }

    return columns


# ──────────────────────────────────────────────────────────────────────
# 3. Data Context (table-level stats)
# ──────────────────────────────────────────────────────────────────────

def profile_data_context(cur, schema: str, table: str,
                         schema_profile: dict,
                         source_sql: str | None = None,
                         filter_clause: str | None = None) -> dict:
    """
    Build a high-level data context: row counts, date ranges,
    key numeric aggregates, data freshness.

    See profile_schema for source_sql / filter_clause semantics.
    """
    if source_sql is None:
        source_sql = f"[{schema}].[{table}]"
    total_rows = schema_profile["total_rows"]
    context = OrderedDict()
    context["view_name"] = f"{schema}.{table}"
    context["total_rows"] = total_rows
    context["column_count"] = schema_profile["column_count"]

    # Identify date columns and numeric columns
    date_cols = []
    numeric_cols = []
    text_cols = []
    for col in schema_profile["columns"]:
        dt = col["data_type"].lower()
        if dt in ("date", "datetime", "datetime2", "smalldatetime"):
            date_cols.append(col["name"])
        elif dt in ("int", "bigint", "smallint", "tinyint", "decimal",
                     "numeric", "float", "real", "money", "smallmoney"):
            numeric_cols.append(col["name"])
        else:
            text_cols.append(col["name"])

    # Date ranges
    if date_cols:
        date_ranges = OrderedDict()
        for dc in date_cols:
            safe_col = f"[{dc}]"
            try:
                cur.execute(f"""
                    SELECT MIN({safe_col}), MAX({safe_col}),
                           COUNT(DISTINCT {safe_col})
                    FROM {source_sql}
                    {_where(base_filter=filter_clause)}
                """)
                mn, mx, dist = cur.fetchone()
                date_ranges[dc] = OrderedDict([
                    ("min", str(mn) if mn else None),
                    ("max", str(mx) if mx else None),
                    ("distinct_dates", dist),
                ])
            except Exception:
                pass
        context["date_ranges"] = date_ranges

    # Key numeric aggregates (for money/amount columns)
    money_keywords = ("AMOUNT", "SPEND", "COST", "EXPENDITURE", "EXPENSE",
                      "VOUCHER", "TOTAL", "PRICE", "EXCH_RATE")
    money_cols = [c for c in numeric_cols
                  if any(kw in c.upper() for kw in money_keywords)]
    if money_cols:
        numeric_stats = OrderedDict()
        for nc in money_cols:
            safe_col = f"[{nc}]"
            try:
                cur.execute(f"""
                    SELECT
                        MIN({safe_col})                 AS min_val,
                        MAX({safe_col})                 AS max_val,
                        AVG(CAST({safe_col} AS FLOAT))  AS avg_val,
                        SUM(CAST({safe_col} AS FLOAT))  AS sum_val,
                        STDEV({safe_col})               AS stddev_val
                    FROM {source_sql}
                    {_where(base_filter=filter_clause)}
                """)
                row = cur.fetchone()
                numeric_stats[nc] = OrderedDict([
                    ("min", _safe_value(row[0])),
                    ("max", _safe_value(row[1])),
                    ("avg", round(float(row[2]), 2) if row[2] else None),
                    ("sum", round(float(row[3]), 2) if row[3] else None),
                    ("stddev", round(float(row[4]), 2) if row[4] else None),
                ])
            except Exception:
                pass
        context["numeric_stats"] = numeric_stats

    # Column classification
    context["column_groups"] = OrderedDict([
        ("date_columns", date_cols),
        ("numeric_columns", numeric_cols),
        ("text_columns", text_cols),
    ])

    # Data freshness
    if date_cols:
        # Pick the most likely "main" date column
        main_date = None
        for candidate in ("INVOICE_DATE", "PO_DATE", "CREATED_DATE"):
            if candidate in date_cols:
                main_date = candidate
                break
        if not main_date:
            main_date = date_cols[0]

        try:
            cur.execute(f"""
                SELECT MAX([{main_date}]),
                       DATEDIFF(DAY, MAX([{main_date}]), GETDATE())
                FROM {source_sql}
                {_where(base_filter=filter_clause)}
            """)
            latest, days_old = cur.fetchone()
            context["data_freshness"] = OrderedDict([
                ("latest_record_date", str(latest) if latest else None),
                ("days_since_latest", days_old),
                ("reference_column", main_date),
            ])
        except Exception:
            pass

    return context


# ──────────────────────────────────────────────────────────────────────
# YAML writer
# ──────────────────────────────────────────────────────────────────────

def write_yaml(data: dict | list, filepath: Path):
    """Write data to a YAML file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True,
                  sort_keys=False, width=120)
    print(f"  Written: {filepath}")


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Profile SQL Server tables and generate knowledge YAML files"
    )
    parser.add_argument("--server", default=os.getenv("SQL_SERVER_HOST", ""),
                        help="SQL Server hostname")
    parser.add_argument("--port", type=int, default=int(os.getenv("SQL_SERVER_PORT", "1433")),
                        help="SQL Server port (default: 1433)")
    parser.add_argument("--database", default=os.getenv("SQL_SERVER_DATABASE", ""),
                        help="Database name")
    parser.add_argument("--username", default=os.getenv("SQL_SERVER_USERNAME", ""),
                        help="Username")
    parser.add_argument("--password", default=os.getenv("SQL_SERVER_PASSWORD", ""),
                        help="Password")
    parser.add_argument("--schema", default="dbo",
                        help="Schema name (default: dbo)")
    parser.add_argument("--tables", required=True,
                        help="Comma-separated table names to profile")
    parser.add_argument("--output-dir", required=True,
                        help="Output directory for YAML files")
    parser.add_argument("--top-values", type=int, default=20,
                        help="Number of top distinct values to sample per column (default: 20)")
    args = parser.parse_args()

    if not args.server or not args.database:
        print("ERROR: --server and --database are required (or set SQL_SERVER_HOST/SQL_SERVER_DATABASE env vars)")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    conn = connect(args)
    cur = conn.cursor()

    for table in tables:
        print(f"\n{'='*60}")
        print(f"Profiling: [{args.schema}].[{table}]")
        print(f"{'='*60}")

        # Determine tag. "indirect" must be checked first — "direct" is a
        # substring of "indirect", so the naive order would tag VW_INDIRECT_*
        # as "direct" and overwrite the VW_DIRECT_* output.
        short_name = table.lower()
        if "indirect" in short_name:
            tag = "indirect"
        elif "direct" in short_name:
            tag = "direct"
        else:
            tag = short_name.replace("vw_", "").replace(" ", "_")

        # Each tag writes into its own subfolder (direct/, indirect/) so
        # the output layout mirrors knowledge_refined/{direct,indirect}/.
        target_dir = output_dir / tag if tag in ("direct", "indirect") else output_dir

        # Materialize the filtered view into a temp table ONCE, then profile
        # against that. Without this, every per-column query (200+ per table)
        # re-scans the complex view and re-applies the WHERE filter — extremely
        # slow because the predicate doesn't push down through view joins.
        temp_name = "#spend_profile"
        cur.execute(f"IF OBJECT_ID('tempdb..{temp_name}') IS NOT NULL DROP TABLE {temp_name}")
        print(f"\n  Materializing filtered data into {temp_name} (one-time scan)...")
        start = time.time()
        cur.execute(f"SELECT * INTO {temp_name} FROM [{args.schema}].[{table}] {_where()}")
        mat_elapsed = time.time() - start
        print(f"  Materialized in {mat_elapsed:.1f}s")

        src = f"[{temp_name}]"

        # ── 1. Schema Columns ──
        # Output as dict-of-dicts (keyed by column name) to match the shape
        # expected by 0_knowledge_processor._build_column_metadata, which iterates
        # columns.items(). Top-level "table_name" also matches the processor's
        # content-based slot detector.
        print(f"\n  [1/3] Schema & column profiling (top {args.top_values} values per column)...")
        start = time.time()
        schema_profile = profile_schema(
            cur, args.schema, table, args.top_values,
            source_sql=src, filter_clause="",
        )
        elapsed = time.time() - start
        print(f"  Schema profiling done in {elapsed:.1f}s — {schema_profile['column_count']} columns, {schema_profile['total_rows']:,} rows")

        columns_dict = OrderedDict()
        for col in schema_profile["columns"]:
            name = col["name"]
            entry = OrderedDict([
                ("type", col["data_type"]),
                ("display_type", col["display_type"]),
                ("nullable", col["nullable"]),
                ("cardinality", col.get("cardinality")),
                ("null_count", col.get("null_count")),
                ("null_pct", col.get("null_pct")),
            ])
            if "min_value" in col:
                entry["min_value"] = col["min_value"]
            if "max_value" in col:
                entry["max_value"] = col["max_value"]
            entry["top_values"] = col.get("top_values", [])
            columns_dict[name] = entry

        schema_yaml = OrderedDict([
            ("table_name", f"{args.schema.upper()}.{table}"),
            ("total_rows", schema_profile["total_rows"]),
            ("column_count", schema_profile["column_count"]),
            ("columns", columns_dict),
        ])
        write_yaml(dict(schema_yaml), target_dir / f"schema_columns_{tag}.yaml")

        # ── 2. Column Values ──
        print(f"\n  [2/3] Column values profiling...")
        start = time.time()
        col_values = profile_column_values(
            cur, args.schema, table, schema_profile,
            max_distinct=200, top_values=args.top_values,
            source_sql=src, filter_clause="",
        )
        elapsed = time.time() - start
        print(f"  Column values done in {elapsed:.1f}s — {len(col_values)} columns catalogued")

        # Normalize each column entry to use "examples" (what the processor prefers)
        # instead of "values", flatten type/cardinality info into the shape
        # _build_column_values already handles. Top-level key is "column_values".
        col_values_normalized = OrderedDict()
        for col_name, info in col_values.items():
            col_values_normalized[col_name] = OrderedDict([
                ("cardinality", info.get("cardinality")),
                ("examples", info.get("values", [])),
                ("complete", info.get("type") == "exhaustive"),
            ])

        col_values_yaml = OrderedDict([
            ("table_name", f"{args.schema.upper()}.{table}"),
            ("description", f"Distinct column values for {table}"),
            ("column_values", col_values_normalized),
        ])
        write_yaml(dict(col_values_yaml), target_dir / f"column_values_{tag}.yaml")

        # ── 3. Data Context ──
        print(f"\n  [3/3] Data context profiling...")
        start = time.time()
        data_ctx = profile_data_context(
            cur, args.schema, table, schema_profile,
            source_sql=src, filter_clause="",
        )
        elapsed = time.time() - start
        print(f"  Data context done in {elapsed:.1f}s")

        write_yaml(dict(data_ctx), target_dir / f"data_context_{tag}.yaml")

        cur.execute(f"IF OBJECT_ID('tempdb..{temp_name}') IS NOT NULL DROP TABLE {temp_name}")

    cur.close()
    conn.close()

    print(f"\n{'='*60}")
    print(f"All done! Output files in: {output_dir}")
    print(f"{'='*60}")
    print(f"\nGenerated files per table (all compatible with 0_knowledge_processor.py):")
    print(f"  schema_columns_<tag>.yaml  — column metadata (dict-of-dicts, keyed by column name)")
    print(f"  column_values_<tag>.yaml   — distinct values (exhaustive for low cardinality)")
    print(f"  data_context_<tag>.yaml    — table stats, date ranges, numeric aggregates")


if __name__ == "__main__":
    main()
