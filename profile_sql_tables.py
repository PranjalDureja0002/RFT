"""
SQL Server Table Profiler
=========================
Connects to SQL Server, profiles the specified tables, and generates
knowledge YAML files ready for the Talk-to-Data pipeline:

  - schema_columns_<view>.yaml   — column metadata, types, cardinality
  - column_values_<view>.yaml    — distinct values (top N) per column
  - data_context_<view>.yaml     — table-level stats, date ranges, row counts
  - examples_<view>.yaml         — sample rows as example records

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
        [--sample-rows 15]       # number of example rows to fetch

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
    """Build a pyodbc connection to SQL Server."""
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
    print("Connected.\n")
    return conn


# ──────────────────────────────────────────────────────────────────────
# 1. Schema Columns Profiler
# ──────────────────────────────────────────────────────────────────────

def profile_schema(cur, schema: str, table: str, top_values: int) -> dict:
    """
    For each column: name, type, nullable, cardinality, min, max,
    null_count, null_pct, and top N distinct values.
    """
    # Get column metadata from INFORMATION_SCHEMA
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
               NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, (schema, table))
    columns_meta = cur.fetchall()

    # Get total row count
    cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
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
                FROM [{schema}].[{table}]
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
                    FROM [{schema}].[{table}]
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
                FROM [{schema}].[{table}]
                WHERE {safe_col} IS NOT NULL
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
                          top_values: int = 20) -> dict:
    """
    For columns with cardinality <= max_distinct, fetch ALL distinct values.
    For high-cardinality columns, just the top N by frequency.
    """
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
                    FROM [{schema}].[{table}]
                    WHERE {safe_col} IS NOT NULL
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
                         schema_profile: dict) -> dict:
    """
    Build a high-level data context: row counts, date ranges,
    key numeric aggregates, data freshness.
    """
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
                    FROM [{schema}].[{table}]
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
                    FROM [{schema}].[{table}]
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
                FROM [{schema}].[{table}]
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
# 4. Example Rows
# ──��───────────────────────────────────────────────────────────────────

def profile_examples(cur, schema: str, table: str,
                     sample_rows: int) -> list[dict]:
    """Fetch N sample rows as example records."""
    try:
        cur.execute(f"""
            SELECT TOP {sample_rows} *
            FROM [{schema}].[{table}]
            ORDER BY NEWID()
        """)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

        examples = []
        for row in rows:
            record = OrderedDict()
            for col_name, val in zip(columns, row):
                record[col_name] = _safe_value(val)
            examples.append(record)
        return examples
    except Exception as e:
        print(f"    WARNING: Could not fetch example rows: {e}")
        return []


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
    parser.add_argument("--sample-rows", type=int, default=15,
                        help="Number of example rows to fetch (default: 15)")
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

        # Determine short name for file naming
        short_name = table.lower()
        if "direct" in short_name:
            tag = "direct"
        elif "indirect" in short_name:
            tag = "indirect"
        else:
            tag = short_name.replace("vw_", "").replace(" ", "_")

        # ── 1. Schema Columns ──
        print(f"\n  [1/4] Schema & column profiling (top {args.top_values} values per column)...")
        start = time.time()
        schema_profile = profile_schema(cur, args.schema, table, args.top_values)
        elapsed = time.time() - start
        print(f"  Schema profiling done in {elapsed:.1f}s — {schema_profile['column_count']} columns, {schema_profile['total_rows']:,} rows")

        schema_yaml = OrderedDict([
            ("view_name", f"{args.schema.upper()}.{table}"),
            ("total_rows", schema_profile["total_rows"]),
            ("column_count", schema_profile["column_count"]),
            ("columns", schema_profile["columns"]),
        ])
        write_yaml(dict(schema_yaml), output_dir / f"schema_columns_{tag}.yaml")

        # ── 2. Column Values ──
        print(f"\n  [2/4] Column values profiling...")
        start = time.time()
        col_values = profile_column_values(
            cur, args.schema, table, schema_profile,
            max_distinct=200, top_values=args.top_values,
        )
        elapsed = time.time() - start
        print(f"  Column values done in {elapsed:.1f}s — {len(col_values)} columns catalogued")

        col_values_yaml = OrderedDict([
            ("view_name", f"{args.schema.upper()}.{table}"),
            ("description", f"Distinct column values for {table}"),
            ("columns", col_values),
        ])
        write_yaml(dict(col_values_yaml), output_dir / f"column_values_{tag}.yaml")

        # ── 3. Data Context ──
        print(f"\n  [3/4] Data context profiling...")
        start = time.time()
        data_ctx = profile_data_context(cur, args.schema, table, schema_profile)
        elapsed = time.time() - start
        print(f"  Data context done in {elapsed:.1f}s")

        write_yaml(dict(data_ctx), output_dir / f"data_context_{tag}.yaml")

        # ── 4. Example Rows ──
        print(f"\n  [4/4] Fetching {args.sample_rows} example rows...")
        start = time.time()
        examples = profile_examples(cur, args.schema, table, args.sample_rows)
        elapsed = time.time() - start
        print(f"  Examples done in {elapsed:.1f}s — {len(examples)} rows")

        examples_yaml = OrderedDict([
            ("view_name", f"{args.schema.upper()}.{table}"),
            ("description", f"Random sample rows from {table}"),
            ("sample_count", len(examples)),
            ("rows", examples),
        ])
        write_yaml(dict(examples_yaml), output_dir / f"sample_rows_{tag}.yaml")

    cur.close()
    conn.close()

    print(f"\n{'='*60}")
    print(f"All done! Output files in: {output_dir}")
    print(f"{'='*60}")
    print(f"\nGenerated files per table:")
    print(f"  schema_columns_<tag>.yaml  — column metadata, types, cardinality, top values")
    print(f"  column_values_<tag>.yaml   — distinct values (exhaustive for low cardinality)")
    print(f"  data_context_<tag>.yaml    — table stats, date ranges, numeric aggregates")
    print(f"  sample_rows_<tag>.yaml     — {args.sample_rows} random example rows")


if __name__ == "__main__":
    main()
