from functools import lru_cache
import time
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from functions.bigquery_client import BigQueryClient
from functions.bigquery_loader import BigQueryLoader
from functions.config import settings
from functions.repositories.exam_repo import ExamRepository
from functions.repositories.full_question_repo import FullQuestionRepository
from functions.repositories.course_repo import CourseRepository
from functions.utils.json_naming_converter import convert_keys_snake_to_camel


class UploadRequest(BaseModel):
    file_name: str = Field(..., description="CSV file name located in the data directory")
    dataset: Optional[str] = Field(
        None, description="BigQuery dataset; falls back to config.yaml default_dataset if omitted"
    )
    table: Optional[str] = Field(
        None, description="BigQuery table; falls back to config.yaml default_table if omitted"
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


class UploadJsonRequest(BaseModel):
    file_name: str = Field(..., description="JSONL file name located in the data directory")
    dataset: Optional[str] = Field(
        None, description="BigQuery dataset; falls back to config.yaml default_dataset if omitted"
    )
    table: Optional[str] = Field(
        None, description="BigQuery table; falls back to config.yaml default_table if omitted"
    )
    write_disposition: Literal["WRITE_EMPTY", "WRITE_APPEND", "WRITE_TRUNCATE"] = Field(
        "WRITE_TRUNCATE", description="How to write into the destination table"
    )
    autodetect: bool = Field(
        True, description="Let BigQuery infer schema from the JSONL payload"
    )


app = FastAPI(title="Data Gathering API", version="0.1.0")


@app.middleware("http")
async def add_runtime_header(request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - start
    response.headers["X-Runtime-Seconds"] = f"{elapsed:.2f}"
    return response


def _ensure_dataset() -> str:
    if not settings.default_dataset:
        raise HTTPException(
            status_code=500,
            detail="default_dataset must be set in config.yaml or provided in the request.",
        )
    return settings.default_dataset


@lru_cache
def get_bq_client() -> BigQueryClient:
    if not settings.project_id:
        raise HTTPException(
            status_code=500, detail="project_id must be set in config.yaml."
        )
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
        question_table=settings.question_table,
        answer_table=settings.answer_table,
        test_id_column=settings.test_id_column,
        question_id_column=settings.question_id_column,
        answer_fk_column=settings.answer_fk_column,
    )

@lru_cache
def get_course_repo() -> CourseRepository:
    dataset = _ensure_dataset()
    return CourseRepository(
        bq=get_bq_client(),
        dataset=dataset,
        course_table=settings.course_table,
        course_id_column=settings.course_id_column,
        course_title_column=settings.course_title_column,
        course_created_at_column=settings.course_created_at_column,
        course_short_desc_column=settings.course_short_desc_column,
        course_desc_column=settings.course_desc_column,
        course_link_column=settings.course_link_column,
    )


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/files")
def list_files() -> dict:
    data_dir = settings.ensure_data_dir()
    csv_files = sorted(p.name for p in data_dir.glob("*.csv"))
    jsonl_files = sorted(p.name for p in data_dir.glob("*.jsonl"))
    return {"data_dir": str(data_dir), "csv_files": csv_files, "jsonl_files": jsonl_files}


@app.post("/upload-csv")
def upload_csv(body: UploadRequest) -> dict:
    project_id = settings.project_id
    if not project_id:
        raise HTTPException(status_code=500, detail="project_id must be set in config.yaml.")

    dataset = body.dataset or settings.default_dataset
    table = body.table or settings.default_table
    if not dataset or not table:
        raise HTTPException(
            status_code=400,
            detail="Provide dataset/table in the request or set defaults in config.yaml.",
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
    except Exception as exc: 
        raise HTTPException(status_code=500, detail=f"Upload failed: {exc}")

    return {"message": "upload completed", **result}


@app.post("/upload-json")
def upload_json(body: UploadJsonRequest) -> dict:
    project_id = settings.project_id
    if not project_id:
        raise HTTPException(status_code=500, detail="project_id must be set in config.yaml.")

    dataset = body.dataset or settings.default_dataset
    table = body.table or settings.default_table
    if not dataset or not table:
        raise HTTPException(
            status_code=400,
            detail="Provide dataset/table in the request or set defaults in config.yaml.",
        )

    data_dir = settings.ensure_data_dir()
    loader = BigQueryLoader(project_id=project_id, location=settings.location)
    jsonl_path = data_dir / body.file_name

    try:
        result = loader.load_jsonl(
            jsonl_path=jsonl_path,
            dataset=dataset,
            table=table,
            write_disposition=body.write_disposition,
            autodetect=body.autodetect,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Upload failed: {exc}")

    return {"message": "upload completed", **result}


@app.get("/v1/test-results/{examResultId}/students/{studentId}/tests/{testId}")
def get_student_attempts(examResultId: str, studentId: str, testId: str, limit: int = 2) -> dict:
    repo = get_exam_repo()
    print("student column name: ", settings.exam_result_student_column)
    try:
        attempts = repo.get_latest_attempts(exam_result_id=examResultId, student_id=studentId, test_id=testId, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc: 
        raise HTTPException(status_code=500, detail=f"Failed to load attempts: {exc}")

    if not attempts:
        raise HTTPException(status_code=404, detail="No exam attempts found for result/student/test.")

    payload = {"exam_result_id": examResultId, "student_id": studentId, "test_id": testId, "attempts": attempts}
    return convert_keys_snake_to_camel(payload)


@app.get("/v1/test-questions/{testId}")
def get_test_questions(testId: str) -> dict:
    repo = get_full_question_repo()
    try:
        questions = repo.get_questions_with_answers(test_id=testId)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc: 
        raise HTTPException(status_code=500, detail=f"Failed to load questions: {exc}")

    if not questions:
        raise HTTPException(status_code=404, detail="No questions found for test.")

    payload = {"test_id": testId, "questions": questions}
    return convert_keys_snake_to_camel(payload)


@app.get("/v1/course-info/{courseId}")
def get_course(courseId: str) -> dict:
    repo = get_course_repo()
    try:
        course = repo.get_course(course_id=courseId)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc: 
        raise HTTPException(status_code=500, detail=f"Failed to load course: {exc}")

    if not course:
        raise HTTPException(status_code=404, detail="Course not found.")

    return convert_keys_snake_to_camel({"course": course})
