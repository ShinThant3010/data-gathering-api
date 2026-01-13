from pathlib import Path
from typing import Dict, Optional

from google.cloud import bigquery


VALID_WRITE_DISPOSITIONS = {"WRITE_EMPTY", "WRITE_APPEND", "WRITE_TRUNCATE"}


class BigQueryLoader:
    def __init__(self, project_id: str, location: Optional[str] = None) -> None:
        if not project_id:
            raise ValueError("GCP_PROJECT_ID is required to initialize BigQuery client.")
        self.project_id = project_id
        self.location = location
        self.client = bigquery.Client(project=project_id)

    def load_csv(
        self,
        csv_path: Path,
        dataset: str,
        table: str,
        *,
        write_disposition: str = "WRITE_TRUNCATE",
        autodetect: bool = True,
        skip_leading_rows: int = 1,
    ) -> Dict[str, str]:
        if write_disposition not in VALID_WRITE_DISPOSITIONS:
            raise ValueError(f"write_disposition must be one of {sorted(VALID_WRITE_DISPOSITIONS)}")
        if not dataset or not table:
            raise ValueError("dataset and table must both be provided.")

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found at {csv_path}")

        table_id = f"{self.project_id}.{dataset}.{table}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            autodetect=autodetect,
            skip_leading_rows=skip_leading_rows,
            write_disposition=write_disposition,
        )

        with csv_path.open("rb") as csv_file:
            load_job = self.client.load_table_from_file(
                csv_file,
                destination=table_id,
                job_config=job_config,
                location=self.location,
            )

        load_job.result()
        destination_table = self.client.get_table(table_id)

        return {
            "job_id": load_job.job_id,
            "table": table_id,
            "rows_written": str(destination_table.num_rows),
            "bytes_processed": str(load_job.output_bytes),
            "state": load_job.state,
        }
