"""The Databricks job, the uploaded files and the notebook must agree on one folder."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_notebook_default_matches_terraform_workspace_root():
    variables = (ROOT / "infra" / "variables.tf").read_text(encoding="utf-8")
    default = re.search(r'variable "workspace_root" \{.*?default\s*=\s*"([^"]+)"', variables, re.S).group(1)

    notebook = (ROOT / "databricks" / "lakehouse_pipeline_notebook.py").read_text(encoding="utf-8")
    notebook_default = re.search(r'^DEFAULT_WORKSPACE_ROOT = "([^"]+)"', notebook, re.M).group(1)

    assert notebook_default == default


def test_job_passes_workspace_root_and_points_at_the_uploaded_notebook():
    main = (ROOT / "infra" / "main.tf").read_text(encoding="utf-8")
    assert "notebook_path = databricks_notebook.lakehouse.path" in main
    assert "workspace_root = var.workspace_root" in main
    assert 'path     = "${var.workspace_root}/databricks/lakehouse_pipeline_notebook"' in main
