"""
SQL Server support patch for connector_catalogue.py

Replace the _test_db_connection function in:
  agentcore/src/backend/base/agentcore/api/connector_catalogue.py

Insert the `elif provider == "sqlserver":` block between the PostgreSQL block
and the `else:` block inside _test_db_connection().

Also update the error message in the `else:` block to include "sqlserver".
"""


# ---------------------------------------------------------------------------
# PATCH: Add this elif block inside _test_db_connection(), right before the
#        final `else: raise HTTPException(...)` block.
# ---------------------------------------------------------------------------

# elif provider == "sqlserver":
#     import pyodbc
#
#     conn_str = (
#         f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#         f"SERVER={host},{port};"
#         f"DATABASE={database_name};"
#         f"UID={username};"
#         f"PWD={password};"
#         f"TrustServerCertificate=yes;"
#     )
#     conn = pyodbc.connect(conn_str, timeout=10)
#     cur = conn.cursor()
#     cur.execute("SELECT 1")
#
#     # Fetch table/column metadata
#     schema = schema_name or "dbo"
#     cur.execute("""
#         SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
#         FROM INFORMATION_SCHEMA.COLUMNS
#         WHERE TABLE_SCHEMA = ?
#         ORDER BY TABLE_NAME, ORDINAL_POSITION
#     """, (schema,))
#     columns = cur.fetchall()
#
#     tables = {}
#     for owner, tbl, col, dtype, nullable in columns:
#         qualified_name = f"{owner}.{tbl}"
#         if qualified_name not in tables:
#             tables[qualified_name] = {
#                 "table_name": tbl,
#                 "qualified_name": qualified_name,
#                 "columns": [],
#             }
#         tables[qualified_name]["columns"].append({
#             "name": col,
#             "type": dtype,
#             "nullable": nullable == "YES",
#         })
#
#     # Get row counts
#     for key, tbl_info in tables.items():
#         try:
#             cur.execute(f'SELECT COUNT(*) FROM [{schema}].[{tbl_info["table_name"]}]')
#             tbl_info["row_count"] = cur.fetchone()[0]
#         except Exception:
#             tbl_info["row_count"] = None
#
#     cur.close()
#     conn.close()
#     latency_ms = round((time.time() - start) * 1000, 2)
#
#     return {
#         "success": True,
#         "message": f"Connected successfully. Found {len(tables)} tables/views.",
#         "latency_ms": latency_ms,
#         "tables_metadata": list(tables.values()),
#     }

# ---------------------------------------------------------------------------
# Also update the else block error message:
# OLD: detail=f"Provider '{provider}' is not yet supported. Supported: postgresql",
# NEW: detail=f"Provider '{provider}' is not yet supported. Supported: postgresql, sqlserver",
# ---------------------------------------------------------------------------
