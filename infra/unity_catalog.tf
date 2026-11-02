# Optional. Needs a premium workspace with a Unity Catalog metastore attached.
# Not created on the free account: set enable_unity_catalog = true only after that is in place.

resource "databricks_catalog" "lakehouse" {
  count   = var.enable_unity_catalog ? 1 : 0
  name    = "lakehouse"
  comment = "Lakehouse demo catalog"
}

resource "databricks_schema" "gold" {
  count        = var.enable_unity_catalog ? 1 : 0
  catalog_name = databricks_catalog.lakehouse[0].name
  name         = "gold"
}

# Analysts can read gold, and nothing else. Bronze and silver stay with the engineers.
resource "databricks_grants" "gold_read" {
  count  = var.enable_unity_catalog ? 1 : 0
  schema = databricks_schema.gold[0].id
  grant {
    principal  = var.uc_group_name
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}
