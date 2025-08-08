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