from typing import Any, Iterable, List, Optional

from google.cloud import bigquery


class BigQueryClient:
    def __init__(self, project_id: str, location: Optional[str] = None) -> None:
        if not project_id:
            raise ValueError("GCP_PROJECT_ID is required to initialize BigQuery client.")
        self.project_id = project_id
        self.location = location
        self.client = bigquery.Client(project=project_id, location=location)

    def run_query(
        self,
        sql: str,
        *,
        parameters: Optional[Iterable[bigquery.ScalarQueryParameter]] = None,
    ) -> List[dict[str, Any]]:
        """Execute a parameterized query and return rows as plain dicts."""
        job_config = bigquery.QueryJobConfig(query_parameters=list(parameters or []))
        job = self.client.query(sql, job_config=job_config, location=self.location)
        return [dict(row) for row in job]

    def table_ref(self, dataset: str, table: str) -> str:
        """Return a fully-qualified table reference wrapped in backticks."""
        return f"`{self.project_id}.{dataset}.{table}`"
