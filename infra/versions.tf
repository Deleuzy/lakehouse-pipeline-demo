terraform {
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
}

# The Databricks provider points at the workspace created in main.tf, so it needs no token here.
# It authenticates with the same Azure identity as azurerm.
provider "databricks" {
  host = azurerm_databricks_workspace.lakehouse.workspace_url
}
