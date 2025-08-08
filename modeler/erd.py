from typing import Dict, List, Tuple

import pandas as pd

from .databricks_client import DatabricksClient, quote_3part


def fetch_model_metadata(client: DatabricksClient, catalog: str, schema: str) -> Dict:
    columns_df = client.fetch_columns(catalog, schema)
    tc_df, kcu_df, rc_df = client.fetch_constraints(catalog, schema)

    # Normalize column names
    for df in [columns_df, tc_df, kcu_df, rc_df]:
        df.columns = [c.lower() for c in df.columns]

    # Primary keys per table
    pk_constraints = set(
        tc_df.loc[tc_df["constraint_type"].str.upper() == "PRIMARY KEY", "constraint_name"].tolist()
    )
    pk_kcu = kcu_df[kcu_df["constraint_name"].isin(pk_constraints)]
    pk_by_table: Dict[str, List[str]] = {}
    for table_name, group in pk_kcu.groupby("table_name"):
        cols = [str(c) for c in group.sort_values("ordinal_position")["column_name"].tolist()]
        pk_by_table[table_name] = cols

    # Map FK constraint -> (child table, child cols)
    fk_constraints = set(
        tc_df.loc[tc_df["constraint_type"].str.upper() == "FOREIGN KEY", "constraint_name"].tolist()
    )
    child_kcu = kcu_df[kcu_df["constraint_name"].isin(fk_constraints)]

    # rc_df provides mapping to the parent unique/primary constraint
    # Build parent constraint -> (parent table, parent cols)
    unique_constraints = set(rc_df["unique_constraint_name"].dropna().astype(str).tolist())
    unique_tc = tc_df[tc_df["constraint_name"].isin(unique_constraints)][
        ["constraint_name", "table_name"]
    ].drop_duplicates()
    parent_kcu = kcu_df[kcu_df["constraint_name"].isin(unique_constraints)]

    parent_cols_by_constraint: Dict[str, List[str]] = {}
    for constraint_name, group in parent_kcu.groupby("constraint_name"):
        parent_cols_by_constraint[constraint_name] = [
            str(c) for c in group.sort_values("ordinal_position")["column_name"].tolist()
        ]

    parent_table_by_constraint: Dict[str, str] = {
        str(row["constraint_name"]): str(row["table_name"]) for _, row in unique_tc.iterrows()
    }

    # Build relationships list as (child_table, parent_table, child_cols, parent_cols, fk_name)
    relationships: List[Tuple[str, str, List[str], List[str], str]] = []
    for _, rc in rc_df.iterrows():
        fk_name = str(rc["constraint_name"])
        uk_name = str(rc["unique_constraint_name"]) if pd.notna(rc["unique_constraint_name"]) else None
        if fk_name not in fk_constraints or not uk_name:
            continue
        child_group = child_kcu[child_kcu["constraint_name"] == fk_name].sort_values("ordinal_position")
        child_cols = [str(c) for c in child_group["column_name"].tolist()]
        parent_cols = parent_cols_by_constraint.get(uk_name, [])
        parent_table = parent_table_by_constraint.get(uk_name, "")
        if parent_table and child_cols and parent_cols and len(child_cols) == len(parent_cols):
            relationships.append((str(child_group["table_name"].iloc[0]), parent_table, child_cols, parent_cols, fk_name))

    # Compose table structures
    tables: Dict[str, Dict] = {}
    for table_name, group in columns_df.groupby("table_name"):
        cols = []
        pk_cols = set(pk_by_table.get(table_name, []))
        # Determine FK columns quick flag if they appear in any relationship
        fk_cols_set = set(sum([rel[2] for rel in relationships if rel[0] == table_name], []))
        for _, row in group.sort_values("ordinal_position").iterrows():
            col_name = str(row["column_name"])  # type: ignore[index]
            cols.append(
                {
                    "name": col_name,
                    "data_type": str(row.get("data_type", "")),
                    "is_nullable": str(row.get("is_nullable", "YES")).upper() == "YES",
                    "is_pk": col_name in pk_cols,
                    "is_fk": col_name in fk_cols_set,
                }
            )
        tables[str(table_name)] = {"columns": cols, "pk_columns": list(pk_cols)}

    return {"tables": tables, "relationships": relationships}


def metadata_to_model(metadata: Dict) -> Dict:
    # Convert fetched metadata to a simplified client-side model structure
    tables_model: Dict[str, Dict] = {}
    for tname, t in metadata.get("tables", {}).items():
        cols = []
        for c in t.get("columns", []):
            cols.append({
                "name": c.get("name"),
                "data_type": c.get("data_type", "STRING"),
                "nullable": bool(c.get("is_nullable", True)),
                "is_pk": bool(c.get("is_pk", False)),
            })
        tables_model[tname] = {
            "columns": cols,
        }
    relationships_model = []
    for child, parent, child_cols, parent_cols, fk_name in metadata.get("relationships", []):
        relationships_model.append({
            "name": fk_name,
            "child_table": child,
            "parent_table": parent,
            "child_cols": child_cols,
            "parent_cols": parent_cols,
        })
    return {"tables": tables_model, "relationships": relationships_model}


def build_graphviz_dot(metadata: Dict, catalog: str, schema: str) -> str:
    tables = metadata.get("tables", {})
    relationships = metadata.get("relationships", [])

    lines: List[str] = []
    lines.append("digraph ERD {")
    lines.append("  graph [rankdir=LR, bgcolor=white];")
    lines.append("  node [shape=plain, fontname=Helvetica];")
    lines.append("  edge [color=gray50, arrowsize=0.8];")

    # Nodes
    for table_name, t in sorted(tables.items()):
        header = f"{schema}.{table_name}"
        rows = []
        rows.append(f"<TR><TD BGCOLOR=\"#e8e8e8\"><B>{header}</B></TD></TR>")
        for col in t["columns"]:
            flags = []
            if col["is_pk"]:
                flags.append("PK")
            if col["is_fk"]:
                flags.append("FK")
            flag_txt = f" [{' ,'.join(flags)}]" if flags else ""
            dtype = col.get("data_type", "")
            rows.append(
                f"<TR><TD ALIGN=\"LEFT\">{col['name']}: {dtype}{flag_txt}</TD></TR>"
            )
        html_label = (
            "<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">"
            + "".join(rows)
            + "</TABLE>>"
        )
        node_name = f"{schema}_{table_name}".replace("-", "_")
        lines.append(f"  {node_name} [label={html_label}];")

    # Edges (child -> parent)
    for child_table, parent_table, child_cols, parent_cols, fk_name in relationships:
        child_node = f"{schema}_{child_table}".replace("-", "_")
        parent_node = f"{schema}_{parent_table}".replace("-", "_")
        label = fk_name
        if len(child_cols) == len(parent_cols) and len(child_cols) > 0:
            pairs = ", ".join([f"{c}->{p}" for c, p in zip(child_cols, parent_cols)])
            label = f"{fk_name} ({pairs})"
        lines.append(
            f"  {child_node} -> {parent_node} [color=\"#4b8bbe\", label=\"{label}\"];"
        )

    lines.append("}")
    return "\n".join(lines)


def build_graphviz_dot_from_model(model: Dict, catalog: str, schema: str) -> str:
    # Reuse the same renderer but adapt the model shape
    metadata_like = {
        "tables": {
            tname: {
                "columns": [
                    {
                        "name": c.get("name"),
                        "data_type": c.get("data_type", ""),
                        "is_pk": bool(c.get("is_pk", False)),
                        "is_fk": False,  # inferred during relationship loop below
                    }
                    for c in t.get("columns", [])
                ]
            }
            for tname, t in model.get("tables", {}).items()
        },
        "relationships": [
            (r.get("child_table"), r.get("parent_table"), r.get("child_cols", []), r.get("parent_cols", []), r.get("name", "fk"))
            for r in model.get("relationships", [])
        ],
    }
    # Mark FK columns
    for child_table, _parent_table, child_cols, _parent_cols, _ in metadata_like["relationships"]:
        if child_table in metadata_like["tables"]:
            for c in metadata_like["tables"][child_table]["columns"]:
                if c["name"] in set(child_cols):
                    c["is_fk"] = True
    return build_graphviz_dot(metadata_like, catalog, schema)


def build_fk_sql(
    catalog: str,
    schema: str,
    child_table: str,
    parent_table: str,
    child_cols: List[str],
    parent_cols: List[str],
    constraint_name: str,
) -> str:
    child_3p = quote_3part(catalog, schema, child_table)
    parent_3p = quote_3part(catalog, schema, parent_table)
    cols_child = ", ".join([f"`{c}`" for c in child_cols])
    cols_parent = ", ".join([f"`{c}`" for c in parent_cols])
    return (
        f"ALTER TABLE {child_3p} ADD CONSTRAINT `{constraint_name}` "
        f"FOREIGN KEY ({cols_child}) REFERENCES {parent_3p} ({cols_parent})"
    )


def build_create_table_sql(
    catalog: str,
    schema: str,
    table_name: str,
    columns: List[Dict],
    primary_key_cols: List[str],
    if_not_exists: bool = True,
) -> str:
    cols_sql = []
    for col in columns:
        name = col["name"]
        dtype = col["data_type"].strip()
        nullable = col.get("nullable", True)
        null_sql = "" if nullable else " NOT NULL"
        cols_sql.append(f"`{name}` {dtype}{null_sql}")
    pk_sql = (
        f", CONSTRAINT `pk_{table_name}` PRIMARY KEY ({', '.join([f'`{c}`' for c in primary_key_cols])})"
        if primary_key_cols
        else ""
    )
    ine = "IF NOT EXISTS " if if_not_exists else ""
    full_name = quote_3part(catalog, schema, table_name)
    return f"CREATE TABLE {ine}{full_name} (\n  {',\n  '.join(cols_sql)}{pk_sql}\n)"


def generate_sql_from_model(model: Dict, catalog: str, schema: str) -> List[str]:
    stmts: List[str] = []
    for tname, t in model.get("tables", {}).items():
        cols = [
            {"name": c.get("name"), "data_type": c.get("data_type", "STRING"), "nullable": bool(c.get("nullable", True))}
            for c in t.get("columns", [])
            if str(c.get("name", "")).strip() and str(c.get("data_type", "")).strip()
        ]
        pk_cols = [c.get("name") for c in t.get("columns", []) if bool(c.get("is_pk", False)) and str(c.get("name", "")).strip()]
        if cols:
            stmts.append(build_create_table_sql(catalog, schema, tname, cols, pk_cols))
    for idx, r in enumerate(model.get("relationships", [])):
        cname = r.get("name") or f"fk_{r.get('child_table')}_{r.get('parent_table')}_{idx+1}"
        stmts.append(
            build_fk_sql(
                catalog,
                schema,
                str(r.get("child_table")),
                str(r.get("parent_table")),
                [str(c) for c in r.get("child_cols", [])],
                [str(c) for c in r.get("parent_cols", [])],
                cname,
            )
        )
    return stmts