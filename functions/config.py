import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml


CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"


def _load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found at {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("config.yaml must contain a mapping at the top level.")
    return data


def _resolve_path(value: str, base_dir: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path


@dataclass
class Settings:
    project_id: Optional[str] = None
    default_dataset: Optional[str] = None
    default_table: Optional[str] = None
    data_dir: Path = Path("data")
    location: Optional[str] = None

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

    @classmethod
    def load(cls, path: Path = CONFIG_PATH) -> "Settings":
        data = _load_config(path)
        base_dir = path.parent

        def _get(key: str, default: Optional[str] = None) -> Optional[str]:
            value = data.get(key)
            return default if value is None else value

        data_dir_value = os.getenv("DATA_DIR") or _get("data_dir")
        if data_dir_value is None:
            data_dir = base_dir / "data"
        else:
            data_dir = _resolve_path(str(data_dir_value), base_dir)

        return cls(
            project_id=os.getenv("GCP_PROJECT_ID") or _get("project_id"),
            default_dataset=os.getenv("BIGQUERY_DATASET") or _get("default_dataset"),
            default_table=os.getenv("BIGQUERY_TABLE") or _get("default_table"),
            data_dir=data_dir,
            location=os.getenv("LOCATION") or _get("location"),
            question_table=_get("question_table", cls.question_table),
            answer_table=_get("answer_table", cls.answer_table),
            exam_result_table=_get("exam_result_table", cls.exam_result_table),
            exam_question_result_table=_get(
                "exam_question_result_table", cls.exam_question_result_table
            ),
            exam_answer_result_table=_get("exam_answer_result_table", cls.exam_answer_result_table),
            question_id_column=_get("question_id_column", cls.question_id_column),
            test_id_column=_get("test_id_column", cls.test_id_column),
            answer_fk_column=_get("answer_fk_column", cls.answer_fk_column),
            exam_result_order_column=_get(
                "exam_result_order_column", cls.exam_result_order_column
            ),
            exam_result_student_column=_get(
                "exam_result_student_column", cls.exam_result_student_column
            ),
            exam_result_test_column=_get("exam_result_test_column", cls.exam_result_test_column),
            exam_result_id_column=_get("exam_result_id_column", cls.exam_result_id_column),
            question_result_id_column=_get(
                "question_result_id_column", cls.question_result_id_column
            ),
            question_result_fk_column=_get(
                "question_result_fk_column", cls.question_result_fk_column
            ),
            answer_result_fk_column=_get(
                "answer_result_fk_column", cls.answer_result_fk_column
            ),
            course_table=_get("course_table", cls.course_table),
            course_id_column=_get("course_id_column", cls.course_id_column),
            course_title_column=_get("course_title_column", cls.course_title_column),
            course_created_at_column=_get(
                "course_created_at_column", cls.course_created_at_column
            ),
            course_short_desc_column=_get(
                "course_short_desc_column", cls.course_short_desc_column
            ),
            course_desc_column=_get("course_desc_column", cls.course_desc_column),
            course_link_column=_get("course_link_column", cls.course_link_column),
        )

    def ensure_data_dir(self) -> Path:
        path = Path(self.data_dir).expanduser()
        path.mkdir(parents=True, exist_ok=True)
        return path


settings = Settings.load()
