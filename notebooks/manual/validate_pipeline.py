# Databricks notebook source
# MAGIC %md
# MAGIC # Manual dev validation
# MAGIC First install this repository's package with `%pip install "/Workspace/.../repository"`
# MAGIC in a separate cell, then restart Python if prompted. See docs/MANUAL_VALIDATION.md.
# MAGIC No jobs or bundles are required. Run these cells in order. Widgets contain no secrets.

# COMMAND ----------
from equiprag_system2.manual import ManualSession, ManualSettings, widget_defaults

for name, default in widget_defaults().items():
    dbutils.widgets.text(name, default)

# COMMAND ----------
# Fill the widgets before running this cell. Use one run_id throughout this pass.
config = ManualSettings.model_validate({name: dbutils.widgets.get(name) for name in widget_defaults()})
session = ManualSession(spark, config)

# COMMAND ----------
session.run('bronze')

# COMMAND ----------
session.run('silver')

# COMMAND ----------
session.run('interactions')

# COMMAND ----------
session.run('requests')

# COMMAND ----------
session.run('enrich')

# COMMAND ----------
session.run('validate')

# COMMAND ----------
session.run('gold')

# COMMAND ----------
session.run('audit')
