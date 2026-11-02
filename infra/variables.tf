variable "project" {
  description = "Short name used in resource names. Lowercase letters and digits only."
  type        = string
  default     = "lakehouse"
}

variable "location" {
  description = "Azure region. Keep it close to your data source."
  type        = string
  default     = "westeurope"
}

variable "databricks_sku" {
  description = "standard or premium. Unity Catalog needs premium, which costs more than the free tier."
  type        = string
  default     = "standard"
  validation {
    condition     = contains(["standard", "premium"], var.databricks_sku)
    error_message = "databricks_sku must be standard or premium."
  }
}

variable "enable_unity_catalog" {
  description = "Create Unity Catalog grants. Requires premium SKU and a metastore already attached to the workspace."
  type        = bool
  default     = false
}

variable "uc_group_name" {
  description = "Account group that gets read access to the gold schema (used only when enable_unity_catalog is true)."
  type        = string
  default     = "data-engineers"
}

variable "energinet_relative_url" {
  description = "Relative URL for the copy activity. Default matches the sample file's query."
  type        = string
  default     = "ElectricityProdex5MinRealtime?sort=Minutes5UTC%20ASC&limit=10000&start=2026-11-01T01%3A00&end=2026-11-02T01%3A00"
}

variable "workspace_root" {
  description = "Databricks workspace folder for the lakehouse code. The notebook reads its code from /Workspace + this path (see tests/test_infra_paths.py)."
  type        = string
  default     = "/Shared/lakehouse-pipeline"
}
