output "resource_group" {
  value = azurerm_resource_group.lakehouse.name
}

output "data_factory" {
  value = azurerm_data_factory.adf.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.lakehouse.workspace_url
}

output "lake_containers" {
  value = [for fs in azurerm_storage_data_lake_gen2_filesystem.layers : fs.name]
}
