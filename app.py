import os
from typing import Dict, List

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from modeler.databricks_client import DatabricksClient
from modeler.erd import (
    build_create_table_sql,
    build_fk_sql,
    build_graphviz_dot,
    build_graphviz_dot_from_model,
    fetch_model_metadata,
    generate_sql_from_model,
    metadata_to_model,
)

# Load .env if present
load_dotenv()

st.set_page_config(page_title="Unity Catalog Data Modeler", layout="wide")


def get_secret_or_env(key: str, default: str = "") -> str:
    # Prefer Streamlit secrets if present, then env
    try:
        return str(st.secrets.get(key, default))
    except Exception:
        return os.getenv(key, default)


@st.cache_resource(show_spinner=False)
def get_client(host: str, http_path: str, token: str) -> DatabricksClient:
    return DatabricksClient(host=host, http_path=http_path, access_token=token)


# Sidebar: Connection
st.sidebar.header("Databricks Connection")
default_host = get_secret_or_env("DATABRICKS_HOST")
default_http_path = get_secret_or_env("DATABRICKS_HTTP_PATH")
default_token = get_secret_or_env("DATABRICKS_TOKEN")

with st.sidebar:
    host = st.text_input("Host", value=default_host, placeholder="adb-xxxxxxxx.azuredatabricks.net")
    http_path = st.text_input("SQL Warehouse HTTP Path", value=default_http_path, placeholder="/sql/1.0/warehouses/xxxx")
    token = st.text_input("Access Token", value=default_token, type="password")
    _connect_btn = st.button("Connect / Refresh", use_container_width=True)

if not (host and http_path and token):
    st.info("Provide Databricks connection info in the sidebar to begin.")
    st.stop()

client = get_client(host, http_path, token)
ok, msg = client.test_connection()
if not ok:
    st.error(msg)
    st.stop()

# Sidebar: Catalog/Schema selectors
st.sidebar.header("Context")
try:
    catalogs = client.list_catalogs()
except Exception as exc:  # noqa: BLE001
    catalogs = []
    st.sidebar.error(f"Failed to list catalogs: {exc}")

catalog = st.sidebar.selectbox("Catalog", catalogs, index=0 if catalogs else None)
schemas: List[str] = []
if catalog:
    try:
        schemas = client.list_schemas(catalog)
    except Exception as exc:  # noqa: BLE001
        st.sidebar.error(f"Failed to list schemas: {exc}")

schema = st.sidebar.selectbox("Schema", schemas, index=0 if schemas else None)

if not (catalog and schema):
    st.info("Select a catalog and schema to continue.")
    st.stop()

st.title("Unity Catalog Data Modeler")

# High-level modes similar to drawdb: Explore (live UC) and Design (client-side)
mode = st.radio("Mode", ["Explore", "Design"], horizontal=True)

if mode == "Explore":
    # Tabs
    erd_tab, create_tab, rel_tab = st.tabs(["ER Diagram", "Create Table", "Add Relationship"]) 

    with erd_tab:
        st.subheader("Entity Relationship Diagram")
        with st.spinner("Loading metadata..."):
            metadata = fetch_model_metadata(client, catalog, schema)
        dot = build_graphviz_dot(metadata, catalog, schema)
        rendered = False
        try:
            st.graphviz_chart(dot, use_container_width=True)
            rendered = True
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Graphviz rendering failed, falling back to interactive view: {exc}")

        if not rendered:
            try:
                from pyvis.network import Network  # Lazy import
                import tempfile
                from streamlit.components.v1 import html

                net = Network(height="650px", width="100%", directed=True)
                net.barnes_hut()

                tables: Dict = metadata.get("tables", {})
                relationships = metadata.get("relationships", [])

                for tname, t in tables.items():
                    title_lines = [f"<b>{schema}.{tname}</b>"]
                    for col in t["columns"]:
                        flags = []
                        if col.get("is_pk"): flags.append("PK")
                        if col.get("is_fk"): flags.append("FK")
                        flag_txt = f" [{' ,'.join(flags)}]" if flags else ""
                        title_lines.append(f"{col['name']}: {col.get('data_type','')}{flag_txt}")
                    net.add_node(tname, label=tname, title="<br/>".join(title_lines), shape="box")

                for child, parent, child_cols, parent_cols, fk in relationships:
                    label = fk
                    if len(child_cols) == len(parent_cols) and len(child_cols) > 0:
                        pairs = ", ".join([f"{c}->{p}" for c, p in zip(child_cols, parent_cols)])
                        label = f"{fk} ({pairs})"
                    net.add_edge(child, parent, title=label, label=label, color="#4b8bbe")

                with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as tmp:
                    net.show(tmp.name)
                    tmp.flush()
                    with open(tmp.name, "r", encoding="utf-8") as fh:
                        html(fh.read(), height=680)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Failed to render fallback view: {exc}")

        with st.expander("Metadata", expanded=False):
            st.json(metadata)

    with create_tab:
        st.subheader("Create Table")
        table_name = st.text_input("Table name")

        st.markdown("Define columns:")
        if "new_table_columns" not in st.session_state:
            st.session_state.new_table_columns = pd.DataFrame(
                [
                    {"name": "id", "data_type": "BIGINT", "nullable": False},
                    {"name": "created_at", "data_type": "TIMESTAMP", "nullable": True},
                ]
            )

        edited_cols = st.data_editor(
            st.session_state.new_table_columns,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "name": st.column_config.TextColumn("Column Name"),
                "data_type": st.column_config.SelectboxColumn(
                    "Data Type",
                    options=[
                        "STRING", "BOOLEAN", "INT", "BIGINT", "DOUBLE", "DECIMAL(38,18)",
                        "DATE", "TIMESTAMP", "BINARY"
                    ],
                    default="STRING",
                ),
                "nullable": st.column_config.CheckboxColumn("Nullable", default=True),
            },
            hide_index=True,
            key="new_table_columns_editor",
        )

        pk_candidates = [r.get("name") for _, r in edited_cols.iterrows() if str(r.get("name")).strip()]
        primary_key_cols = st.multiselect("Primary key columns", options=pk_candidates, default=[c for c in pk_candidates if c == "id"]) 

        if st.button("Create table", type="primary"):
            cols = []
            for _, r in edited_cols.iterrows():
                name = str(r.get("name", "")).strip()
                dtype = str(r.get("data_type", "")).strip()
                if not name or not dtype:
                    continue
                cols.append({"name": name, "data_type": dtype, "nullable": bool(r.get("nullable", True))})
            if not table_name or not cols:
                st.error("Please provide a table name and at least one column with data type.")
            else:
                sql_text = build_create_table_sql(catalog, schema, table_name, cols, primary_key_cols)
                with st.spinner("Creating table..."):
                    try:
                        client.run_sql(sql_text)
                        st.success(f"Table `{catalog}.{schema}.{table_name}` created.")
                    except Exception as exc:  # noqa: BLE001
                        st.error(f"Failed to create table: {exc}")

    with rel_tab:
        st.subheader("Add Foreign Key Relationship")
        try:
            tables = client.list_tables(catalog, schema)
        except Exception as exc:  # noqa: BLE001
            tables = []
            st.error(f"Failed to list tables: {exc}")

        child_table = st.selectbox("Child (referencing) table", tables, index=0 if tables else None, key="child_table")
        parent_table = st.selectbox("Parent (referenced) table", tables, index=1 if len(tables) > 1 else None, key="parent_table")

        if child_table and parent_table:
            # Fetch columns for selected tables
            cols_df = client.fetch_columns(catalog, schema)
            child_cols = cols_df.loc[cols_df["table_name"] == child_table, "column_name"].tolist()
            parent_cols = cols_df.loc[cols_df["table_name"] == parent_table, "column_name"].tolist()

            st.write("Select column mapping(s) in order. The lists must be the same length.")
            num_pairs = st.number_input("Number of column pairs", min_value=1, max_value=16, value=1, step=1)

            child_selected: List[str] = []
            parent_selected: List[str] = []
            cols = st.columns(2)
            for i in range(int(num_pairs)):
                with cols[0]:
                    child_selected.append(
                        st.selectbox(f"Child column {i+1}", options=child_cols, key=f"child_col_{i}")
                    )
                with cols[1]:
                    parent_selected.append(
                        st.selectbox(f"Parent column {i+1}", options=parent_cols, key=f"parent_col_{i}")
                    )

            default_fk_name = f"fk_{child_table}_{parent_table}"
            constraint_name = st.text_input("Constraint name", value=default_fk_name)

            if st.button("Add relationship", type="primary"):
                if len(child_selected) != len(parent_selected) or not child_selected:
                    st.error("Child and parent column lists must have the same non-zero length.")
                else:
                    sql_text = build_fk_sql(
                        catalog=catalog,
                        schema=schema,
                        child_table=child_table,
                        parent_table=parent_table,
                        child_cols=child_selected,
                        parent_cols=parent_selected,
                        constraint_name=constraint_name,
                    )
                    with st.spinner("Adding foreign key constraint..."):
                        try:
                            client.run_sql(sql_text)
                            st.success(f"Added constraint `{constraint_name}` on `{catalog}.{schema}.{child_table}` → `{parent_table}`.")
                        except Exception as exc:  # noqa: BLE001
                            st.error(f"Failed to add relationship: {exc}")
else:
    # Design mode
    st.subheader("Design Mode (drawdb-inspired)")

    if "design_model" not in st.session_state:
        st.session_state.design_model = {"tables": {}, "relationships": []}

    dm = st.session_state.design_model

    cols = st.columns([2, 3])

    with cols[0]:
        st.markdown("### Tables")
        with st.expander("Add / Edit Table", expanded=True):
            table_name = st.text_input("Table name", key="design_table_name")
            if "design_table_columns" not in st.session_state:
                st.session_state.design_table_columns = pd.DataFrame([
                    {"name": "id", "data_type": "BIGINT", "nullable": False, "is_pk": True},
                ])
            edited = st.data_editor(
                st.session_state.design_table_columns,
                num_rows="dynamic",
                use_container_width=True,
                column_config={
                    "name": st.column_config.TextColumn("Column Name"),
                    "data_type": st.column_config.SelectboxColumn(
                        "Data Type",
                        options=[
                            "STRING", "BOOLEAN", "INT", "BIGINT", "DOUBLE", "DECIMAL(38,18)",
                            "DATE", "TIMESTAMP", "BINARY"
                        ],
                        default="STRING",
                    ),
                    "nullable": st.column_config.CheckboxColumn("Nullable", default=True),
                    "is_pk": st.column_config.CheckboxColumn("PK", default=False),
                },
                hide_index=True,
                key="design_table_columns_editor",
            )
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("Add/Update Table", type="primary"):
                    if table_name.strip():
                        dm["tables"][table_name.strip()] = {
                            "columns": [
                                {
                                    "name": str(r.get("name", "")).strip(),
                                    "data_type": str(r.get("data_type", "STRING")).strip(),
                                    "nullable": bool(r.get("nullable", True)),
                                    "is_pk": bool(r.get("is_pk", False)),
                                }
                                for _, r in edited.iterrows()
                                if str(r.get("name", "")).strip()
                            ]
                        }
                        st.success(f"Upserted table `{table_name}` in design model.")
            with c2:
                if st.button("Clear Form"):
                    st.session_state.design_table_columns = pd.DataFrame([
                        {"name": "", "data_type": "STRING", "nullable": True, "is_pk": False},
                    ])
                    st.session_state.design_table_name = ""
            with c3:
                if st.button("Remove Table"):
                    if table_name.strip() in dm["tables"]:
                        del dm["tables"][table_name.strip()]
                        dm["relationships"] = [r for r in dm["relationships"] if r.get("child_table") != table_name and r.get("parent_table") != table_name]
                        st.success(f"Removed table `{table_name}` from design model.")

        if dm["tables"]:
            st.markdown("Existing tables:")
            st.dataframe(pd.DataFrame({"table": list(dm["tables"].keys())}))

        st.markdown("### Relationships")
        if dm["tables"]:
            tnames = sorted(dm["tables"].keys())
            child = st.selectbox("Child table", tnames, key="design_child")
            parent = st.selectbox("Parent table", tnames, key="design_parent")
            child_cols = [c["name"] for c in dm["tables"][child]["columns"]]
            parent_cols = [c["name"] for c in dm["tables"][parent]["columns"]]
            pairs = st.number_input("Number of column pairs", 1, 16, 1)
            cc, pc = st.columns(2)
            selected_child: List[str] = []
            selected_parent: List[str] = []
            for i in range(int(pairs)):
                with cc:
                    selected_child.append(st.selectbox(f"Child column {i+1}", child_cols, key=f"dm_child_{i}"))
                with pc:
                    selected_parent.append(st.selectbox(f"Parent column {i+1}", parent_cols, key=f"dm_parent_{i}"))
            fk_name = st.text_input("Constraint name", value=f"fk_{child}_{parent}")
            if st.button("Add Relationship", type="primary"):
                dm["relationships"].append({
                    "name": fk_name,
                    "child_table": child,
                    "parent_table": parent,
                    "child_cols": selected_child,
                    "parent_cols": selected_parent,
                })
                st.success("Relationship added to design model.")

        if dm["relationships"]:
            st.dataframe(pd.DataFrame(dm["relationships"]))

    with cols[1]:
        st.markdown("### Canvas / Preview")
        dot = build_graphviz_dot_from_model(dm, catalog, schema)
        try:
            st.graphviz_chart(dot, use_container_width=True)
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Graphviz rendering failed: {exc}")

        st.markdown("### Import from Unity Catalog")
        if st.button("Import UC into Design"):
            with st.spinner("Fetching metadata..."):
                m = fetch_model_metadata(client, catalog, schema)
                dm_import = metadata_to_model(m)
                # Merge: UC import wins for existing tables
                st.session_state.design_model = dm_import
                st.success("Imported current UC model into Design mode.")
                dm = st.session_state.design_model

        st.markdown("### SQL Preview")
        stmts = generate_sql_from_model(dm, catalog, schema)
        sql_text = ";\n\n".join(stmts)
        st.code(sql_text or "-- No statements", language="sql")

        if st.button("Apply to Unity Catalog", type="primary"):
            if not stmts:
                st.info("Nothing to apply.")
            else:
                errors = []
                with st.spinner("Applying DDL to UC..."):
                    for s in stmts:
                        try:
                            client.run_sql(s)
                        except Exception as exc:  # noqa: BLE001
                            errors.append(f"{s}\n-- ERROR: {exc}")
                if errors:
                    st.error("Some statements failed. See below.")
                    st.code("\n\n".join(errors), language="sql")
                else:
                    st.success("All statements applied successfully.")

st.caption("Deployed for Databricks Apps. Explore live UC metadata or Design offline like drawDB, then apply changes.")