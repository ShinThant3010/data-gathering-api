## Data Gathering API

FastAPI service for loading CSV/JSONL files from the local `data/` directory into BigQuery. Includes endpoints to check health, list available files, and trigger uploads.

### Prerequisites
- Python 3.10+ and `pip`
- Google Cloud project with BigQuery enabled
- Service account key with `BigQuery Data Editor` (or higher) permissions; set `GOOGLE_APPLICATION_CREDENTIALS` to its JSON path

### Setup
```bash
cd data-gathering-api
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Configuration
Configuration lives in `config.yaml` (repo root):
- `project_id` (required): GCP project id
- `default_dataset` (optional): default dataset for uploads
- `default_table` (optional): default table for uploads
- `data_dir` (optional): path to CSV folder, defaults to `./data`
- `location` (optional): BigQuery location, e.g. `us-central1`
- Table/column names are defined in the same file (snake_case): `exam_result`, `exam_question_result`, `exam_answer_result`, `question`, `answer`, `course` and their associated columns such as `user_id`, `test_id`, `created_at`, `exam_result_id`, `question_result_id`, `lesson_title`, `short_description`, `description`, `link`.

You can override `project_id`, `default_dataset`, `default_table`, `data_dir`, and `location` with env vars (`GCP_PROJECT_ID`, `BIGQUERY_DATASET`, `BIGQUERY_TABLE`, `DATA_DIR`, `LOCATION`) if needed.
`GOOGLE_APPLICATION_CREDENTIALS` is still required in the environment for BigQuery auth.

### Run the API
```bash
uv run uvicorn api:app --reload --port 8080
```

### Endpoints
- `GET /health` – simple readiness check
- `GET /files` – list `.csv` and `.jsonl` files under `DATA_DIR`
- `POST /upload-csv` – load a CSV into BigQuery
- `POST /upload-json` – load a JSONL into BigQuery
- `GET /v1/test-results/students/{studentId}/tests/{testId}` – latest attempts (default 2) with nested question/answer rows (response keys camelCase)
- `GET /v1/test-questions/{testId}` – questions plus answers for a test (response keys camelCase)
- `GET /v1/course-info/{courseId}` – course info (id, lesson_title, created_at, short_description, description, link; response keys camelCase)

Example upload request (uses defaults for dataset/table if provided via `config.yaml`):
```bash
curl -X POST http://localhost:8000/upload-csv \
  -H "Content-Type: application/json" \
  -d '{
    "file_name": "example.csv",
    "dataset": "my_dataset",
    "table": "incoming_data",
    "write_disposition": "WRITE_APPEND",
    "autodetect": true,
    "skip_leading_rows": 1
  }'
```

Place CSV/JSONL files in `data/` before calling the upload endpoints. The loader will create the table if it does not exist (BigQuery `CREATE_IF_NEEDED`) and will respect the provided `write_disposition`.
