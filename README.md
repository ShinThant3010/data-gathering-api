## Data Gathering API

FastAPI service for loading CSV files from the local `data/` directory into BigQuery. Includes endpoints to check health, list available CSV files, and trigger uploads.

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
Environment variables (can be set in your shell or a `.env` you load before starting):
- `GCP_PROJECT_ID` (required): GCP project id
- `BIGQUERY_DATASET` (optional): default dataset for uploads
- `BIGQUERY_TABLE` (optional): default table for uploads
- `GOOGLE_APPLICATION_CREDENTIALS` (required): path to service account JSON
- `DATA_DIR` (optional): path to CSV folder, defaults to `./data`
- `LOCATION` (optional): BigQuery location, e.g. `us-central1`
  - Table/column names are fixed in code (snake_case): `exam_result`, `exam_question_result`, `exam_answer_result`, `question`, `answer`, `course` and their associated columns such as `user_id`, `test_id`, `created_at`, `exam_result_id`, `question_result_id`, `lesson_title`, `short_description`, `description`, `link`.

### Run the API
```bash
uv run uvicorn api:app --reload --port 8080
```

### Endpoints
- `GET /health` – simple readiness check
- `GET /files` – list `.csv` files under `DATA_DIR`
- `POST /upload` – load a CSV into BigQuery
- `GET /v1/test-results/students/{studentId}/tests/{testId}` – latest attempts (default 2) with nested question/answer rows (response keys camelCase)
- `GET /v1/test-questions/{testId}` – questions plus answers for a test (response keys camelCase)
- `GET /v1/course-info/{courseId}` – course info (id, lesson_title, created_at, short_description, description, link; response keys camelCase)

Example upload request (uses defaults for dataset/table if provided via env vars):
```bash
curl -X POST http://localhost:8000/upload \
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

Place CSV files in `data/` before calling `/upload`. The loader will create the table if it does not exist (BigQuery `CREATE_IF_NEEDED`) and will respect the provided `write_disposition`.
