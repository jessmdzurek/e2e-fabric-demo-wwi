# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "73d813ad-1ac4-4d01-8af3-4ab068dd75f0",
# META       "default_lakehouse_name": "e2e_wwi_lh_bronze",
# META       "default_lakehouse_workspace_id": "4851fb88-89a2-49c6-859e-437aea78897b",
# META       "known_lakehouses": [
# META         {
# META           "id": "73d813ad-1ac4-4d01-8af3-4ab068dd75f0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

bronze_root = "Files/bronze"

# List entries under Files/bronze
entries = mssparkutils.fs.ls(bronze_root)

# Keep only directories; normalize names
bronze_dirs = []
for e in entries:
    # e.name often ends with "/" for directories
    name = e.name.rstrip("/")
    path = e.path.rstrip("/")

    # Skip hidden/system folders just in case
    if name.startswith("_") or name.startswith("."):
        continue

    # Heuristic: treat as directory if name came back with "/" OR path doesn't look like a file
    # In practice, ls(bronze_root) will return folders you created like "brz_*"
    bronze_dirs.append((name, path))

bronze_prefix = "brz_"
bronze_dirs = [(n, p) for (n, p) in bronze_dirs if n.startswith(bronze_prefix)]

print("Discovered Bronze folders:")
for name, path in bronze_dirs:
    print(f" - {name}  ({path})")

if not bronze_dirs:
    raise Exception(f"No Bronze folders found under {bronze_root}. Did the pipeline write files yet?")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.utils import AnalysisException

results = []

for table_name, folder_path in bronze_dirs:
    try:
        df = spark.read.format("parquet").load(folder_path)

        # Idempotent: overwrite table each run (perfect for demos)
        df.write.mode("overwrite").format("delta").saveAsTable(table_name)

        rowcount = df.count()
        results.append((table_name, "SUCCEEDED", rowcount, folder_path))
        print(f"✅ Materialized {table_name}: {rowcount:,} rows")

    except AnalysisException as e:
        results.append((table_name, "FAILED", None, folder_path))
        print(f"❌ FAILED {table_name} (path/schema issue): {folder_path}")
        print(str(e)[:600])

    except Exception as e:
        results.append((table_name, "FAILED", None, folder_path))
        print(f"❌ FAILED {table_name}: {folder_path}")
        print(str(e)[:600])

print("Done.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary_df = spark.createDataFrame(results, ["table_name", "status", "row_count", "source_path"])
display(summary_df.orderBy("table_name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
