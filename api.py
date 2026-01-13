from functools import lru_cache
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from functions.bigquery_client import BigQueryClient
from functions.bigquery_loader import BigQueryLoader
from functions.config import settings
from functions.repositories.exam_repo import ExamRepository
from functions.repositories.full_question_repo import FullQuestionRepository
from functions.utils.json_naming_converter import convert_keys_snake_to_camel


class UploadRequest(BaseModel):
    file_name: str = Field(..., description="CSV file name located in the data directory")
    dataset: Optional[str] = Field(
        None, description="BigQuery dataset; falls back to BIGQUERY_DATASET env var if omitted"
    )
    table: Optional[str] = Field(
        None, description="BigQuery table; falls back to BIGQUERY_TABLE env var if omitted"
    )
    write_disposition: Literal["WRITE_EMPTY", "WRITE_APPEND", "WRITE_TRUNCATE"] = Field(
        "WRITE_TRUNCATE", description="How to write into the destination table"
    )
    autodetect: bool = Field(
        True, description="Let BigQuery infer schema from the CSV header row"
    )
    skip_leading_rows: int = Field(
        1, ge=0, description="Number of header rows to skip before ingesting data"
    )


app = FastAPI(title="Data Gathering API", version="0.1.0")


def _ensure_dataset() -> str:
    if not settings.default_dataset:
        raise HTTPException(status_code=500, detail="BIGQUERY_DATASET environment variable is required.")
    return settings.default_dataset


@lru_cache
def get_bq_client() -> BigQueryClient:
    if not settings.project_id:
        raise HTTPException(status_code=500, detail="GCP_PROJECT_ID environment variable is required.")
    return BigQueryClient(project_id=settings.project_id, location=settings.location)


@lru_cache
def get_exam_repo() -> ExamRepository:
    dataset = _ensure_dataset()
    return ExamRepository(
        bq=get_bq_client(),
        dataset=dataset,
        exam_result_table=settings.exam_result_table,
        exam_question_result_table=settings.exam_question_result_table,
        exam_answer_result_table=settings.exam_answer_result_table,
        exam_result_order_column=settings.exam_result_order_column,
        exam_result_student_column=settings.exam_result_student_column,
        exam_result_test_column=settings.exam_result_test_column,
        exam_result_id_column=settings.exam_result_id_column,
        question_result_id_column=settings.question_result_id_column,
        question_result_fk_column=settings.question_result_fk_column,
        answer_result_fk_column=settings.answer_result_fk_column,
    )


@lru_cache
def get_full_question_repo() -> FullQuestionRepository:
    dataset = _ensure_dataset()
    return FullQuestionRepository(
        bq=get_bq_client(),
        dataset=dataset,
        question_table=settings.exam_question_result_table,
        answer_table=settings.exam_answer_result_table,
        test_id_column=settings.test_id_column,
        question_result_id_column=settings.question_result_id_column,
        answer_result_fk_column=settings.answer_result_fk_column,
    )


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/files")
def list_files() -> dict:
    data_dir = settings.ensure_data_dir()
    files = sorted(p.name for p in data_dir.glob("*.csv"))
    return {"data_dir": str(data_dir), "files": files}


@app.post("/upload")
def upload_csv(body: UploadRequest) -> dict:
    project_id = settings.project_id
    if not project_id:
        raise HTTPException(status_code=500, detail="GCP_PROJECT_ID environment variable is required.")

    dataset = body.dataset or settings.default_dataset
    table = body.table or settings.default_table
    if not dataset or not table:
        raise HTTPException(
            status_code=400,
            detail="Provide dataset and table in the request or set BIGQUERY_DATASET and BIGQUERY_TABLE.",
        )

    data_dir = settings.ensure_data_dir()
    loader = BigQueryLoader(project_id=project_id, location=settings.location)
    csv_path = data_dir / body.file_name

    try:
        result = loader.load_csv(
            csv_path=csv_path,
            dataset=dataset,
            table=table,
            write_disposition=body.write_disposition,
            autodetect=body.autodetect,
            skip_leading_rows=body.skip_leading_rows,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # pragma: no cover - surfaced to client
        raise HTTPException(status_code=500, detail=f"Upload failed: {exc}")

    return {"message": "upload completed", **result}


@app.get("/v1/test-results/students/{student_id}/tests/{test_id}")
def get_student_attempts(student_id: str, test_id: str, limit: int = 2) -> dict:
    repo = get_exam_repo()
    try:
        attempts = repo.get_latest_attempts(student_id=student_id, test_id=test_id, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"Failed to load attempts: {exc}")

    if not attempts:
        raise HTTPException(status_code=404, detail="No exam attempts found for student/test.")

    payload = {"student_id": student_id, "test_id": test_id, "attempts": attempts}
    return convert_keys_snake_to_camel(payload)


@app.get("/v1/tests/{test_id}/questions")
def get_test_questions(test_id: str) -> dict:
    repo = get_full_question_repo()
    try:
        questions = repo.get_questions_with_answers(test_id=test_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"Failed to load questions: {exc}")

    if not questions:
        raise HTTPException(status_code=404, detail="No questions found for test.")

    payload = {"test_id": test_id, "questions": questions}
    return convert_keys_snake_to_camel(payload)
