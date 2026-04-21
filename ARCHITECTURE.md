# Talk to Data — Dual-Database Architecture (SQL Server + Oracle)

## Why two databases?

The customer's environment splits responsibility across two engines:

| Data domain | Engine | Why |
|---|---|---|
| Spend (large, append-mostly, queried for analytics) | **SQL Server** | Customer's analytics warehouse. Two views: `DBO.VW_DIRECT_SPEND_ALL`, `DBO.VW_INDIRECT_SPEND_ALL`. |
| RBAC / user entitlements (small, mutates frequently) | **Oracle** | Source of truth for access control. Tables `PISLOAD.EIS_*`, `PISVIEW.EIS_*`. |

Cross-database JOINs between Oracle and SQL Server are not feasible (no linked-server / DBLINK is configured). The pipeline therefore executes a **two-step lookup**:

1. **Query Oracle** for the user's entitlements: superuser flag, allowed regions, 4-char commodity prefixes, supplier-classification codes.
2. **Inject SQL Server WHERE clauses** built from those values into the spend query, then execute on SQL Server.

This avoids cross-DB federation while preserving the original RBAC semantics.

## Data flow

```
                 +-------------------+
                 |   User question   |
                 +---------+---------+
                           |
                           v
   +--------------------------------------------+
   |       TalkToDataPipeline (Custom Node)     |
   |                                            |
   |  Inputs:                                   |
   |    db_connection       (SQL Server, Data)  |
   |    rbac_db_connection  (Oracle, Data)      |
   |    user_email          (str)               |
   +-----------+-------------------+------------+
               |                   |
   (1) entitlement fetch       (2) spend query
               |                   |
               v                   v
   +-------------------+   +-------------------+
   |  Oracle (RBAC)    |   |  SQL Server       |
   |                   |   |  (Spend)          |
   |  PISLOAD.EIS_*    |   |  DBO.VW_DIRECT_   |
   |  PISVIEW.EIS_*    |   |  SPEND_ALL        |
   |                   |   |  DBO.VW_INDIRECT_ |
   |  -> regions       |   |  SPEND_ALL        |
   |  -> commodities   |   |                   |
   |  -> supp classes  |   |  Filtered by      |
   |  -> superuser     |   |  injected WHERE   |
   +-------------------+   +-------------------+
```

## RBAC tables (Oracle)

| Table | Purpose |
|---|---|
| `PISLOAD.EIS_SUPERUSER_LIST` | Superusers — full access, no row filters applied. |
| `PISLOAD.EIS_ACCESS_USER_LIST` | `EMAIL` → `APPROVAL_ID`, `ACCESSTYPE` (DIRECT/INDIRECT). |
| `PISLOAD.EIS_USER_COMMODITY_LIST` | `APPROVAL_ID` → `REGION`, `COMMODITY` (4-char prefix), `L0_LEVEL`. |
| `PISVIEW.EIS_USER_SUPPLIER_CLASSIFICATION_LIST` | `APPROVAL_ID` → `SUPP_CLASSIFICATION` (G / O / N / S). |

Constants are defined in [`talk_to_data_pipeline.py`](talk_to_data_pipeline.py) at the top of the file. The canonical standalone reference for the entitlement-fetch logic is [`rbac_oracle.py`](rbac_oracle.py).

## Filter mapping (Oracle entitlements -> SQL Server WHERE)

| Oracle column | SQL Server filter generated |
|---|---|
| `REGION` | `UPPER(LTRIM(RTRIM(REGION))) IN (...)` |
| `COMMODITY` (4-char prefix) | `SUBSTRING(UPPER(ISNULL(COMMODITY,'')),1,4) IN (...)` |
| `SUPP_CLASSIFICATION` | `UPPER(LTRIM(RTRIM(ISNULL(ABC_INDICATOR,'')))) IN ('G' / 'O','N','S','')` |

`SUPP_CLASSIFICATION` mapping:
- `G` or contains `MOTHERSON` → `ABC_INDICATOR = 'G'` (Motherson Group)
- `O`, `N`, `S` or contains `EXTERNAL` → `ABC_INDICATOR IN ('O','N','S','')` (External)

## Component wiring (UI flow)

In the Agent Builder canvas, the `TalkToDataPipeline` node now has **two `Data` inputs**:

1. `db_connection` (**required**) — SQL Server connector for spend tables. Wire from a `DatabaseConnector` node configured with a SQL Server entry from the Connectors Catalogue.
2. `rbac_db_connection` (**required when User Email is set**) — Oracle connector for the RBAC tables. Wire from a second `DatabaseConnector` configured with an Oracle entry.

If `user_email` is set but `rbac_db_connection` is missing, the pipeline returns:
```
Access denied: ... Reason: rbac_oracle_connection_missing
```

## Connector Catalogue

The Connectors page must support both providers. [`database_connector_FIXED.py`](../../TalktoData-UseCase/database_connector_FIXED.py) handles `_fetch_schema()` and `_fetch_foreign_keys()` for **postgresql**, **sqlserver**, and **oracle**.

| Provider | Driver | Schema source | FK source |
|---|---|---|---|
| postgresql | `psycopg2` | `information_schema.columns` | `information_schema.table_constraints` |
| sqlserver | `pyodbc` (ODBC Driver 17) | `INFORMATION_SCHEMA.COLUMNS` | `sys.foreign_keys` |
| oracle | `oracledb` | `ALL_TAB_COLUMNS` | `ALL_CONS_COLUMNS` + `ALL_CONSTRAINTS` (CONSTRAINT_TYPE = 'R') |

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `rbac_oracle_connection_missing` | `rbac_db_connection` input not wired. | Wire an Oracle `DatabaseConnector` to the new handle. |
| `rbac_oracle_connect_failed: ...` | Oracle host/port/service-name/credentials wrong. | Verify Oracle connector in Catalogue. |
| `no_entitlements_for_role` | User exists in `EIS_ACCESS_USER_LIST` but `ACCESSTYPE` doesn't match `DIRECT`/`INDIRECT` for the requested spend type. | Add the user with the correct ACCESSTYPE. |
| `oracledb_import_failed` | `oracledb` Python package missing. | `pip install oracledb`. |
| Query passes through with no filter | User is in `EIS_SUPERUSER_LIST`. | Expected — superusers bypass row filters. |

## Deployment prerequisites

- **SQL Server**: ODBC Driver 17 installed on the worker (Windows: MSI; Docker/AKS Ubuntu: `apt-get install msodbcsql17` from `packages.microsoft.com`).
- **Oracle**: `oracledb` Python package (thin mode — no Oracle Instant Client required).
- Both connectors registered in the Connectors Catalogue with `status = "connected"`.
