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

    question_table: str = os.getenv("QUESTION_TABLE", "question")
    answer_table: str = os.getenv("ANSWER_TABLE", "answer")

    exam_result_table: str = os.getenv("EXAM_RESULT_TABLE", "exam_result")
    exam_question_result_table: str = os.getenv("EXAM_QUESTION_RESULT_TABLE", "exam_question_result")
    exam_answer_result_table: str = os.getenv("EXAM_ANSWER_RESULT_TABLE", "exam_answer_result")

    question_id_column: str = os.getenv("QUESTION_ID_COLUMN", "id")
    test_id_column: str = os.getenv("TEST_ID_COLUMN", "testId")
    answer_fk_column: str = os.getenv("QUESTION_ANSWER_ID_COLUMN", "questionId")

    exam_result_order_column: str = os.getenv("EXAM_RESULT_ORDER_COLUMN", "createdAt")
    exam_result_student_column: str = os.getenv("EXAM_RESULT_STUDENT_COLUMN", "userId")
    exam_result_test_column: str = os.getenv("EXAM_RESULT_TEST_COLUMN", "testId")
    exam_result_id_column: str = os.getenv("EXAM_RESULT_ID_COLUMN", "id")
    question_result_id_column: str = os.getenv("EXAM_QUESTION_RESULT_ID_COLUMN", "id")
    question_result_fk_column: str = os.getenv("EXAM_QUESTION_RESULT_FK_COLUMN", "examResultId")
    answer_result_fk_column: str = os.getenv("EXAM_ANSWER_RESULT_FK_COLUMN", "examResultQuestionId")

    def ensure_data_dir(self) -> Path:
        path = Path(self.data_dir).expanduser()
        path.mkdir(parents=True, exist_ok=True)
        return path


settings = Settings()
