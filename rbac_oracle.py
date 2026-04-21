"""
Oracle RBAC Helper
==================
Queries the Oracle access-control tables for user entitlements, then returns
plain Python values that the SQL Server pipeline can use to build WHERE
clause filters on the spend tables.

Why a separate module?
- Spend data lives in SQL Server (DBO.VW_DIRECT_SPEND_ALL, DBO.VW_INDIRECT_SPEND_ALL)
- RBAC tables stay in Oracle (PISLOAD.EIS_*, PISVIEW.EIS_*)
- Cross-database JOINs are not possible across Oracle <-> SQL Server, so we:
    1. Query Oracle for the user's commodity prefixes, regions, supplier classes
    2. Build SQL Server WHERE filters from those values (e.g. WHERE COMMODITY IN (...))
    3. Inject the filters into the spend query that runs on SQL Server

Tables queried (Oracle):
- PISLOAD.EIS_SUPERUSER_LIST                      — superuser whitelist
- PISLOAD.EIS_ACCESS_USER_LIST                    — user -> APPROVAL_ID + ACCESSTYPE
- PISLOAD.EIS_USER_COMMODITY_LIST                 — APPROVAL_ID -> REGION, COMMODITY, L0_LEVEL
- PISVIEW.EIS_USER_SUPPLIER_CLASSIFICATION_LIST   — APPROVAL_ID -> SUPP_CLASSIFICATION
"""

from __future__ import annotations

# Constants — match Oracle table names from the customer environment
ACCESS_USER_TABLE = "PISLOAD.EIS_ACCESS_USER_LIST"
ACCESS_COMMODITY_TABLE = "PISLOAD.EIS_USER_COMMODITY_LIST"
ACCESS_SUPP_CLASS_TABLE = "PISVIEW.EIS_USER_SUPPLIER_CLASSIFICATION_LIST"
ACCESS_SUPERUSER_TABLE = "PISLOAD.EIS_SUPERUSER_LIST"

# Commodity entitlements are stored as 4-character prefixes and must be
# matched against SUBSTRING(table.COMMODITY, 1, 4) on SQL Server side.
COMMODITY_PREFIX_LEN = 4


# ──────────────────────────────────────────────────────────────────────
# Connection helpers
# ──────────────────────────────────────────────────────────────────────

def _connect_oracle(rbac_db_data: dict):
    """Open an oracledb connection from the RBAC DB connection dict.

    rbac_db_data is the same shape produced by the Database Connector
    component when the user picks an Oracle connector from the catalogue.
    Required keys: host, port, database_name (= service name), username, password.
    """
    import oracledb

    host = rbac_db_data.get("host", "")
    port = rbac_db_data.get("port", 1521)
    service_name = rbac_db_data.get("database_name", "")
    username = rbac_db_data.get("username", "")
    password = rbac_db_data.get("password", "")

    if not host or not service_name or not username:
        raise ValueError(
            "RBAC Oracle connection is missing required fields "
            "(host, database_name, username)."
        )

    dsn = oracledb.makedsn(host, int(port), service_name=service_name)
    conn = oracledb.connect(user=username, password=password, dsn=dsn)
    conn.call_timeout = 10000
    return conn


# ──────────────────────────────────────────────────────────────────────
# Public API: superuser check
# ──────────────────────────────────────────────────────────────────────

def is_superuser(rbac_db_data: dict, user_email: str) -> bool:
    """Return True if user_email is in PISLOAD.EIS_SUPERUSER_LIST."""
    if not user_email:
        return False
    try:
        conn = _connect_oracle(rbac_db_data)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT 1 FROM {ACCESS_SUPERUSER_TABLE} "
                f"WHERE UPPER(EMAIL) = UPPER(:email)",
                {"email": user_email},
            )
            row = cur.fetchone()
            cur.close()
            return row is not None
        finally:
            conn.close()
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────
# Public API: full entitlement fetch
# ──────────────────────────────────────────────────────────────────────

def fetch_user_entitlements(
    rbac_db_data: dict, user_email: str, spend_type: str
) -> dict:
    """Fetch full RBAC entitlements for a user + spend type.

    Returns a dict shaped exactly like the existing pipeline expects:
      {
        "has_access": bool,
        "is_superuser": bool,
        "access_types": [str],
        "role_match": bool,
        "expected_role": str,           # "DIRECT" or "INDIRECT"
        "regions": [str],               # uppercase
        "l0_levels": [str],
        "approval_ids": [str],
        "supp_classifications": [str],
        "commodities": [str],           # 4-char prefixes, uppercase
        "user_email": str,
        "spend_type": str,              # "direct" or "indirect"
        "reason": str,                  # explains has_access decision
      }
    """
    if not user_email:
        return {"has_access": False, "reason": "email_required"}

    st = (spend_type or "").strip().upper()
    if st not in ("DIRECT", "INDIRECT"):
        return {"has_access": False, "reason": "spend_type_required"}

    try:
        conn = _connect_oracle(rbac_db_data)
    except Exception as e:
        return {
            "has_access": False,
            "reason": f"rbac_oracle_connect_failed: {e!s}",
            "user_email": user_email,
            "spend_type": st.lower(),
        }

    try:
        cur = conn.cursor()

        # 1) Superuser check — short-circuit
        try:
            cur.execute(
                f"SELECT 1 FROM {ACCESS_SUPERUSER_TABLE} "
                f"WHERE UPPER(EMAIL) = UPPER(:email)",
                {"email": user_email},
            )
            su_row = cur.fetchone()
        except Exception:
            su_row = None

        if su_row is not None:
            cur.close()
            return {
                "has_access": True,
                "is_superuser": True,
                "access_types": [st],
                "role_match": True,
                "expected_role": st,
                "regions": [],
                "l0_levels": [],
                "approval_ids": [],
                "supp_classifications": [],
                "commodities": [],
                "user_email": user_email,
                "spend_type": st.lower(),
                "reason": "superuser",
            }

        # 2) Role-scoped entitlement join
        join_sql = f"""
            SELECT
                m.APPROVAL_ID,
                m.REGION,
                m.L0_LEVEL,
                SUBSTR(l.COMMODITY, 1, {COMMODITY_PREFIX_LEN}) AS COMMODITY,
                sc.SUPP_CLASSIFICATION
            FROM {ACCESS_USER_TABLE} m
            LEFT JOIN {ACCESS_COMMODITY_TABLE} l
                ON m.APPROVAL_ID = l.APPROVAL_ID
               AND UPPER(l.ACCESSTYPE) = :role
            LEFT JOIN {ACCESS_SUPP_CLASS_TABLE} sc
                ON m.APPROVAL_ID = sc.APPROVAL_ID
               AND UPPER(sc.ACCESSTYPE) = :role
            WHERE UPPER(m.EMAIL) = UPPER(:email)
              AND UPPER(m.ACCESSTYPE) = :role
        """
        try:
            cur.execute(join_sql, {"email": user_email, "role": st})
            rows = cur.fetchall() or []
        except Exception as e:
            cur.close()
            return {
                "has_access": False,
                "reason": f"rbac_query_failed: {e!s}",
                "expected_role": st,
                "user_email": user_email,
                "spend_type": st.lower(),
            }
        cur.close()
    finally:
        conn.close()

    if not rows:
        return {
            "has_access": False,
            "reason": "no_entitlements_for_role",
            "expected_role": st,
            "user_email": user_email,
            "spend_type": st.lower(),
        }

    def _u(v):
        return str(v).strip() if v is not None and str(v).strip() else ""

    approval_ids = sorted({_u(r[0]) for r in rows if _u(r[0])})
    regions = sorted({_u(r[1]).upper() for r in rows if _u(r[1])})
    l0_levels = sorted({_u(r[2]) for r in rows if _u(r[2])})
    commodities = sorted({_u(r[3]).upper() for r in rows if _u(r[3])})
    supp_classifications = sorted({_u(r[4]) for r in rows if _u(r[4])})

    return {
        "has_access": True,
        "is_superuser": False,
        "access_types": [st],
        "role_match": True,
        "expected_role": st,
        "regions": regions,
        "l0_levels": l0_levels,
        "approval_ids": approval_ids,
        "supp_classifications": supp_classifications,
        "commodities": commodities,
        "user_email": user_email,
        "spend_type": st.lower(),
        "reason": "entitlements_resolved",
    }


# ──────────────────────────────────────────────────────────────────────
# Public API: SQL Server WHERE clause builders
# (used by the pipeline's _apply_commodity_access_filter, kept here so
# the rules live next to the entitlement fetch logic)
# ──────────────────────────────────────────────────────────────────────

def _esc(v) -> str:
    """Escape a single value for safe inline injection into a SQL Server filter."""
    return str(v).strip().upper().replace("'", "''")


def build_region_filter_sqlserver(regions: list[str]) -> str | None:
    """`UPPER(LTRIM(RTRIM(REGION))) IN ('EU', 'NA', ...)` or None if empty."""
    vals = [_esc(v) for v in regions if str(v).strip()]
    if not vals:
        return None
    return "UPPER(LTRIM(RTRIM(REGION))) IN (" + ", ".join(f"'{v}'" for v in vals) + ")"


def build_commodity_filter_sqlserver(commodities: list[str]) -> str | None:
    """`SUBSTRING(UPPER(ISNULL(COMMODITY,'')),1,4) IN ('XXXX', ...)` or None."""
    vals = [_esc(v) for v in commodities if str(v).strip()]
    if not vals:
        return None
    prefix_list = ", ".join(f"'{v}'" for v in vals)
    return (
        f"SUBSTRING(UPPER(ISNULL(COMMODITY, '')), 1, {COMMODITY_PREFIX_LEN}) "
        f"IN ({prefix_list})"
    )


def build_supplier_class_filter_sqlserver(
    supp_classifications: list[str],
) -> str | None:
    """Map SUPP_CLASSIFICATION codes to ABC_INDICATOR filter on SQL Server.

    Mapping (from existing pipeline logic):
      G              -> Motherson Group         -> ABC_INDICATOR IN ('G')
      O / N / S      -> External Supplier       -> ABC_INDICATOR IN ('O','N','S','')
    """
    classes = [str(v).upper() for v in supp_classifications if str(v).strip()]
    allow_motherson = any("MOTHERSON" in s or s == "G" for s in classes)
    allow_external = any("EXTERNAL" in s or s in ("O", "N", "S") for s in classes)

    abc_allowed = []
    if allow_motherson:
        abc_allowed.append("'G'")
    if allow_external:
        abc_allowed.extend(["'O'", "'N'", "'S'", "''"])

    if not abc_allowed:
        return None
    return (
        "UPPER(LTRIM(RTRIM(ISNULL(ABC_INDICATOR, '')))) IN ("
        + ", ".join(abc_allowed) + ")"
    )
