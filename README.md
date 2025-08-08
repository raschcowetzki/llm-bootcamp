# Streamlit Databricks Unity Catalog Data Modeler

A Streamlit app to model Unity Catalog tables in Databricks: create tables, manage relationships (foreign keys), and visualize an ER diagram.

## Features
- Create tables with column definitions and primary keys
- Add foreign key relationships
- Visualize ER diagrams across a catalog/schema

## Prerequisites
- Access to a Databricks SQL Warehouse (serverless or pro)
- A Databricks Personal Access Token (PAT)
- Unity Catalog enabled and permissions to create/alter tables and constraints

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Provide Databricks credentials via environment variables (recommended) or via the app sidebar:
   - `DATABRICKS_HOST` (e.g. adb-1234567890123456.17.azuredatabricks.net)
   - `DATABRICKS_HTTP_PATH` (SQL warehouse HTTP path)
   - `DATABRICKS_TOKEN` (personal access token)

   You can also create a `.env` file in the project root:
   ```env
   DATABRICKS_HOST=...
   DATABRICKS_HTTP_PATH=...
   DATABRICKS_TOKEN=...
   ```

## Run
```bash
streamlit run app.py
```

Open the printed local URL in your browser.

## Notes
- ER diagram is generated from `information_schema` metadata. Ensure your workspace/runtime supports table constraints metadata (PRIMARY KEY/FOREIGN KEY) in Unity Catalog.
- If Graphviz rendering is unavailable on your system, the app will automatically fallback to an interactive PyVis network view.