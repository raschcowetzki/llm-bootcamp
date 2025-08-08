import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from databricks import sql


class DatabricksClient:
    def __init__(
        self,
        host: str,
        http_path: str,
        access_token: str,
        session_parameters: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.host = host
        self.http_path = http_path
        self.access_token = access_token
        self.session_parameters = session_parameters or {}

    @staticmethod
    def from_env() -> Optional["DatabricksClient"]:
        host = os.getenv("DATABRICKS_HOST", "").strip()
        http_path = os.getenv("DATABRICKS_HTTP_PATH", "").strip()
        token = os.getenv("DATABRICKS_TOKEN", "").strip()
        if not (host and http_path and token):
            return None
        return DatabricksClient(host, http_path, token)

    def _connect(self):
        return sql.connect(
            server_hostname=self.host,
            http_path=self.http_path,
            access_token=self.access_token,
            session_configuration=self.session_parameters,
        )

    def test_connection(self) -> Tuple[bool, str]:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")
                _ = cur.fetchone()
            return True, "Connected successfully"
        except Exception as exc:  # noqa: BLE001
            return False, f"Connection failed: {exc}"

    def run_sql(self, sql_text: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql_text, params or {})
            try:
                # Prefer Arrow for performance when available
                tbl = cur.fetchall_arrow()
                return tbl.to_pandas() if tbl is not None else pd.DataFrame()
            except Exception:
                # Not a SELECT or no results
                return pd.DataFrame()

    # ---------- Metadata helpers ----------
    def list_catalogs(self) -> List[str]:
        # Try information_schema first, fallback to SHOW
        queries = [
            "SELECT catalog_name AS name FROM system.information_schema.catalogs ORDER BY name",
            "SHOW CATALOGS",
        ]
        for q in queries:
            try:
                df = self.run_sql(q)
                if not df.empty:
                    col = "name" if "name" in df.columns else df.columns[0]
                    return sorted([str(v) for v in df[col].tolist()])
            except Exception:
                continue
        return []

    def list_schemas(self, catalog: str) -> List[str]:
        queries = [
            f"SELECT schema_name AS name FROM {quote_ident(catalog)}.information_schema.schemata ORDER BY name",
            f"SHOW SCHEMAS IN {quote_ident(catalog)}",
        ]
        for q in queries:
            try:
                df = self.run_sql(q)
                if not df.empty:
                    col = "name" if "name" in df.columns else df.columns[0]
                    return sorted([str(v) for v in df[col].tolist()])
            except Exception:
                continue
        return []

    def list_tables(self, catalog: str, schema: str) -> List[str]:
        queries = [
            (
                f"SELECT table_name AS name FROM {quote_ident(catalog)}.information_schema.tables "
                f"WHERE table_schema = %(schema)s ORDER BY name"
            ),
            f"SHOW TABLES IN {quote_3part(catalog, schema)}",
        ]
        for q in queries:
            try:
                df = self.run_sql(q, {"schema": schema})
                if not df.empty:
                    # SHOW TABLES returns: database, tableName, isTemporary
                    if "name" in df.columns:
                        names = [str(v) for v in df["name"].tolist()]
                    elif "tableName" in df.columns:
                        names = [str(v) for v in df["tableName"].tolist()]
                    else:
                        names = [str(v) for v in df.iloc[:, 0].tolist()]
                    return sorted(names)
            except Exception:
                continue
        return []

    def fetch_columns(self, catalog: str, schema: str) -> pd.DataFrame:
        q = (
            f"SELECT table_name, column_name, data_type, is_nullable, ordinal_position "
            f"FROM {quote_ident(catalog)}.information_schema.columns \n"
            f"WHERE table_schema = %(schema)s"
        )
        return self.run_sql(q, {"schema": schema})

    def fetch_constraints(self, catalog: str, schema: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        tc_q = (
            f"SELECT table_name, constraint_name, constraint_type \n"
            f"FROM {quote_ident(catalog)}.information_schema.table_constraints \n"
            f"WHERE table_schema = %(schema)s"
        )
        kcu_q = (
            f"SELECT constraint_name, table_name, column_name, ordinal_position \n"
            f"FROM {quote_ident(catalog)}.information_schema.key_column_usage \n"
            f"WHERE table_schema = %(schema)s"
        )
        rc_q = (
            f"SELECT constraint_name, unique_constraint_name \n"
            f"FROM {quote_ident(catalog)}.information_schema.referential_constraints \n"
            f"WHERE constraint_schema = %(schema)s"
        )
        return (
            self.run_sql(tc_q, {"schema": schema}),
            self.run_sql(kcu_q, {"schema": schema}),
            self.run_sql(rc_q, {"schema": schema}),
        )


def quote_ident(ident: str) -> str:
    # Quote identifiers using backticks and escape embedded backticks
    escaped = ident.replace("`", "``")
    return f"`{escaped}`"


def quote_3part(catalog: str, schema: str, table: Optional[str] = None) -> str:
    if table is None:
        return f"{quote_ident(catalog)}.{quote_ident(schema)}"
    return f"{quote_ident(catalog)}.{quote_ident(schema)}.{quote_ident(table)}"