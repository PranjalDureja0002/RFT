"""
SQL Server support patch for database_connector.py

Apply these patches to:
  agentcore/src/backend/base/agentcore/components/tools/database_connector.py

Two methods need SQL Server branches added:
  1. _fetch_schema()  — add elif provider == "sqlserver" before the else
  2. _fetch_foreign_keys() — add elif provider == "sqlserver" before the return []
"""


# ===========================================================================
# PATCH 1: _fetch_schema() — Add this elif block before the final `else:`
# ===========================================================================

# elif provider == "sqlserver":
#     import pyodbc
#
#     conn_str = (
#         f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#         f"SERVER={params['host']},{params['port']};"
#         f"DATABASE={params['database_name']};"
#         f"UID={params['username']};"
#         f"PWD={params['password']};"
#         f"TrustServerCertificate=yes;"
#     )
#     conn = pyodbc.connect(conn_str, timeout=15)
#     cur = conn.cursor()
#
#     schema = params.get("schema_name", "dbo")
#
#     cur.execute("""
#         SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
#         FROM INFORMATION_SCHEMA.COLUMNS
#         WHERE TABLE_SCHEMA = ?
#         ORDER BY TABLE_NAME, ORDINAL_POSITION
#     """, (schema,))
#     rows = cur.fetchall()
#
#     tables = {}
#     for owner, tbl, col, dtype, nullable in rows:
#         qualified_name = f"{owner}.{tbl}"
#         if qualified_name not in tables:
#             tables[qualified_name] = {
#                 "owner": owner,
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
#     cur.close()
#     conn.close()
#
#     # Apply table filter if provided
#     filter_str = self.tables_filter.strip() if self.tables_filter else ""
#     if filter_str:
#         allowed = {t.strip().lower() for t in filter_str.split(",") if t.strip()}
#         tables = {k: v for k, v in tables.items()
#                   if v["table_name"].lower() in allowed
#                   or v.get("qualified_name", "").lower() in allowed}
#
#     return list(tables.values())


# ===========================================================================
# PATCH 2: _fetch_foreign_keys() — Change the guard and add sqlserver branch
#
# OLD:  if provider != "postgresql":
#           return []
#
# NEW:  if provider not in ("postgresql", "sqlserver"):
#           return []
#
# Then add this elif block after the PostgreSQL section:
# ===========================================================================

# elif provider == "sqlserver":
#     import pyodbc
#
#     conn_str = (
#         f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#         f"SERVER={params['host']},{params['port']};"
#         f"DATABASE={params['database_name']};"
#         f"UID={params['username']};"
#         f"PWD={params['password']};"
#         f"TrustServerCertificate=yes;"
#     )
#     conn = pyodbc.connect(conn_str, timeout=15)
#     cur = conn.cursor()
#
#     schema = params.get("schema_name", "dbo")
#
#     cur.execute("""
#         SELECT
#             fk.name              AS constraint_name,
#             tp.name              AS source_table,
#             cp.name              AS source_column,
#             tr.name              AS target_table,
#             cr.name              AS target_column
#         FROM sys.foreign_keys AS fk
#         INNER JOIN sys.foreign_key_columns AS fkc
#             ON fk.object_id = fkc.constraint_object_id
#         INNER JOIN sys.tables AS tp
#             ON fkc.parent_object_id = tp.object_id
#         INNER JOIN sys.columns AS cp
#             ON fkc.parent_object_id = cp.object_id
#            AND fkc.parent_column_id = cp.column_id
#         INNER JOIN sys.tables AS tr
#             ON fkc.referenced_object_id = tr.object_id
#         INNER JOIN sys.columns AS cr
#             ON fkc.referenced_object_id = cr.object_id
#            AND fkc.referenced_column_id = cr.column_id
#         INNER JOIN sys.schemas AS s
#             ON tp.schema_id = s.schema_id
#         WHERE s.name = ?
#     """, (schema,))
#     rows = cur.fetchall()
#
#     cur.close()
#     conn.close()
#
#     # Apply table filter if provided
#     filter_str = self.tables_filter.strip() if self.tables_filter else ""
#     allowed = None
#     if filter_str:
#         allowed = {t.strip().lower() for t in filter_str.split(",") if t.strip()}
#
#     results = []
#     for constraint, src_tbl, src_col, tgt_tbl, tgt_col in rows:
#         if allowed and src_tbl.lower() not in allowed:
#             continue
#         results.append({
#             "source_table": src_tbl,
#             "source_column": src_col,
#             "target_table": tgt_tbl,
#             "target_column": tgt_col,
#         })
#     return results
