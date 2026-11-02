resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

locals {
  name_prefix = "${var.project}${random_string.suffix.result}"
  tags        = { project = var.project, managed_by = "terraform" }
}

resource "azurerm_resource_group" "lakehouse" {
  name     = "rg-${var.project}"
  location = var.location
  tags     = local.tags
}

# ADLS Gen2 = a storage account with hierarchical namespace turned on.
resource "azurerm_storage_account" "lake" {
  name                     = "${local.name_prefix}lake"
  resource_group_name      = azurerm_resource_group.lakehouse.name
  location                 = azurerm_resource_group.lakehouse.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
  tags                     = local.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "layers" {
  for_each           = toset(["bronze", "silver", "gold"])
  name               = each.key
  storage_account_id = azurerm_storage_account.lake.id
}

resource "azurerm_data_factory" "adf" {
  name                = "adf-${local.name_prefix}"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = azurerm_resource_group.lakehouse.location
  identity {
    type = "SystemAssigned"
  }
  tags = local.tags
}

# Let Data Factory write to the lake with its managed identity, not account keys.
resource "azurerm_role_assignment" "adf_lake_writer" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "lake" {
  name                 = "ls_lake"
  data_factory_id      = azurerm_data_factory.adf.id
  url                  = "https://${azurerm_storage_account.lake.name}.dfs.core.windows.net"
  use_managed_identity = true
}

resource "azurerm_data_factory_linked_service_web" "energinet" {
  name                = "ls_energinet"
  data_factory_id     = azurerm_data_factory.adf.id
  url                 = "https://api.energidataservice.dk/dataset/"
  authentication_type = "Anonymous"
}

resource "azurerm_data_factory_dataset_http" "energinet_source" {
  name                = "ds_energinet_source"
  data_factory_id     = azurerm_data_factory.adf.id
  linked_service_name = azurerm_data_factory_linked_service_web.energinet.name
  relative_url        = var.energinet_relative_url
}

resource "azurerm_data_factory_dataset_json" "bronze_sink" {
  name                = "ds_bronze_energinet_json"
  data_factory_id     = azurerm_data_factory.adf.id
  linked_service_name = azurerm_data_factory_linked_service_data_lake_storage_gen2.lake.name
  azure_blob_storage_location {
    container = azurerm_storage_data_lake_gen2_filesystem.layers["bronze"].name
    path      = "energinet"
    filename  = "prodex5min.json"
  }
}

resource "azurerm_databricks_workspace" "lakehouse" {
  name                        = "dbw-${var.project}"
  resource_group_name         = azurerm_resource_group.lakehouse.name
  location                    = azurerm_resource_group.lakehouse.location
  sku                         = var.databricks_sku
  managed_resource_group_name = "rg-${var.project}-dbw-managed"
  tags                        = local.tags
}

# Data Factory calls Databricks with its managed identity.
resource "azurerm_role_assignment" "adf_databricks" {
  scope                = azurerm_databricks_workspace.lakehouse.id
  role_definition_name = "Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

resource "azurerm_data_factory_linked_service_azure_databricks" "workspace" {
  name                       = "ls_databricks"
  data_factory_id            = azurerm_data_factory.adf.id
  description                = "Databricks workspace that runs the lakehouse job"
  adb_domain                 = "https://${azurerm_databricks_workspace.lakehouse.workspace_url}"
  msi_work_space_resource_id = azurerm_databricks_workspace.lakehouse.id

  new_cluster_config {
    cluster_version       = "15.4.x-scala2.12"
    node_type             = "Standard_DS3_v2"
    min_number_of_workers = 1
    max_number_of_workers = 1
  }
}

# Upload the repo files the notebook needs, under var.workspace_root. The notebook sets its
# REPO_PATH to /Workspace + var.workspace_root, so code and data resolve to the same place.
locals {
  repo_files = toset(concat(
    [for f in fileset(path.module, "../src/**/*.py") : f],
    [for f in fileset(path.module, "../src/**/*.sql") : f],
    [for f in fileset(path.module, "../src/**/*.json") : f],
    [for f in fileset(path.module, "../data/**/*") : f],
  ))
}

resource "databricks_workspace_file" "repo" {
  for_each = local.repo_files
  source   = "${path.module}/${each.value}"
  path     = "${var.workspace_root}/${replace(each.value, "../", "")}"
}

resource "databricks_notebook" "lakehouse" {
  source   = "${path.module}/../databricks/lakehouse_pipeline_notebook.py"
  path     = "${var.workspace_root}/databricks/lakehouse_pipeline_notebook"
  language = "PYTHON"
}

# The job that Databricks runs: the notebook on a small single-node cluster.
data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

data "databricks_node_type" "small" {
  local_disk = true
}

resource "databricks_job" "lakehouse_daily" {
  name = "lakehouse-daily-${var.project}"

  task {
    task_key = "bronze_silver_gold"
    new_cluster {
      num_workers   = 0
      spark_version = data.databricks_spark_version.latest_lts.id
      node_type_id  = data.databricks_node_type.small.id
      spark_conf = {
        "spark.master" = "local[*]"
      }
    }
    notebook_task {
      notebook_path = databricks_notebook.lakehouse.path
    }
  }
}

# Data Factory: copy the source into bronze, then run the Databricks job.
resource "azurerm_data_factory_pipeline" "daily" {
  name            = "pl_energinet_to_lakehouse"
  data_factory_id = azurerm_data_factory.adf.id
  activities_json = jsonencode([
    {
      name    = "copy_energinet_to_bronze"
      type    = "Copy"
      inputs  = [{ referenceName = azurerm_data_factory_dataset_http.energinet_source.name, type = "DatasetReference" }]
      outputs = [{ referenceName = azurerm_data_factory_dataset_json.bronze_sink.name, type = "DatasetReference" }]
      typeProperties = {
        source = { type = "JsonSource", storeSettings = { type = "HttpReadSettings", requestMethod = "GET" } }
        sink   = { type = "JsonSink" }
      }
    },
    {
      name              = "run_databricks_job"
      type              = "DatabricksJob"
      dependsOn         = [{ activity = "copy_energinet_to_bronze", dependencyConditions = ["Succeeded"] }]
      linkedServiceName = { referenceName = azurerm_data_factory_linked_service_azure_databricks.workspace.name, type = "LinkedServiceReference" }
      typeProperties = {
        jobId = databricks_job.lakehouse_daily.id
      }
    }
  ])
}
