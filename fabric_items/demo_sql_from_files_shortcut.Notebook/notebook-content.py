# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c80a8e8b-b47b-491b-bf36-2a59d4aed00c",
# META       "default_lakehouse_name": "e2e_wwi_lh_silver",
# META       "default_lakehouse_workspace_id": "4851fb88-89a2-49c6-859e-437aea78897b",
# META       "known_lakehouses": [
# META         {
# META           "id": "c80a8e8b-b47b-491b-bf36-2a59d4aed00c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# 


# MARKDOWN ********************

# # Demo 1 — Execute staged SQL files in Silver using a Bronze shortcut
# 
# We will show:
# - Bronze and Silver are separate Lakehouses
# - Silver reads Bronze via a shortcut (no data copy)
# - SQL scripts are staged into Silver Lakehouse Files for runtime execution
# - A notebook executes SQL in three patterns: DDL, SELECT+write, and multi-statement

# CELL ********************

# Should see Brozne Lakehouse shortcut table name
spark.sql("SHOW TABLES").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Validate the Bronze → Silver shortcut
# 
# This table is a shortcut in Silver that points to data in Bronze.
# A quick COUNT proves we can read Bronze data without copying it into Silver.

# CELL ********************

spark.sql("SELECT COUNT(*) AS cnt FROM src_bz_wwi_sales_invoicelines").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

mssparkutils.fs.ls("Files/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.ls("Files/sql/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.ls("Files/sql/silver/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.rm("Files/sql", recurse=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.ls("Files/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#mssparkutils.fs.rm("Files/01_invoicelines_ddl.sql", recurse=True)
#mssparkutils.fs.rm("Files/02_invoicelines_select.sql", recurse=True)
mssparkutils.fs.rm("Files/03_invoicelines_multi.sql", recurse=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
