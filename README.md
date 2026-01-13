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
- Table/column overrides for query endpoints (optional):
  - `EXAM_RESULT_TABLE` (default `exam_result`)
  - `EXAM_QUESTION_RESULT_TABLE` (default `exam_question_result`)
  - `EXAM_ANSWER_RESULT_TABLE` (default `exam_answer_result`)
  - `EXAM_RESULT_ORDER_COLUMN` (default `completed_at`)
  - `EXAM_RESULT_STUDENT_COLUMN` (default `student_id`)
  - `EXAM_RESULT_TEST_COLUMN` (default `test_id`)
  - `EXAM_RESULT_ID_COLUMN` (default `id`)
  - `EXAM_QUESTION_RESULT_ID_COLUMN` (default `id`)
  - `EXAM_QUESTION_RESULT_FK_COLUMN` (default `exam_result_id`)
  - `EXAM_ANSWER_RESULT_FK_COLUMN` (default `question_result_id`)
  - `TEST_ID_COLUMN` (default `test_id`)

### Run the API
```bash
uvicorn api:app --reload --port 8000
```

### Endpoints
- `GET /health` – simple readiness check
- `GET /files` – list `.csv` files under `DATA_DIR`
- `POST /upload` – load a CSV into BigQuery
- `GET /students/{student_id}/tests/{test_id}/attempts` – latest attempts (default 2) with nested question/answer rows
- `GET /tests/{test_id}/questions` – questions plus answers for a test

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
