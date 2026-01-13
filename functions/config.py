import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class Settings:
    project_id: Optional[str] = os.getenv("GCP_PROJECT_ID")
    default_dataset: Optional[str] = os.getenv("BIGQUERY_DATASET")
    default_table: Optional[str] = os.getenv("BIGQUERY_TABLE")
    data_dir: Path = Path(os.getenv("DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
    location: Optional[str] = os.getenv("LOCATION")

    # Fixed table/column names (no env override)
    question_table: str = "question"
    answer_table: str = "answer"

    exam_result_table: str = "exam_result"
    exam_question_result_table: str = "exam_question_result"
    exam_answer_result_table: str = "exam_answer_result"

    question_id_column: str = "id"
    test_id_column: str = "test_id"
    answer_fk_column: str = "question_id"

    exam_result_order_column: str = "created_at"
    exam_result_student_column: str = "user_id"
    exam_result_test_column: str = "test_id"
    exam_result_id_column: str = "id"
    question_result_id_column: str = "id"
    question_result_fk_column: str = "exam_result_id"
    answer_result_fk_column: str = "exam_result_question_id"

    course_table: str = "course"
    course_id_column: str = "id"
    course_title_column: str = "lesson_title"
    course_created_at_column: str = "created_at"
    course_short_desc_column: str = "short_description"
    course_desc_column: str = "description"
    course_link_column: str = "link"

    def ensure_data_dir(self) -> Path:
        path = Path(self.data_dir).expanduser()
        path.mkdir(parents=True, exist_ok=True)
        return path


settings = Settings()
