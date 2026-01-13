from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from google.cloud import bigquery

from ..bigquery_client import BigQueryClient
from .exam_repo import _ensure_safe_identifier


class CourseRepository:
    def __init__(
        self,
        bq: BigQueryClient,
        *,
        dataset: str,
        course_table: str,
        course_id_column: str,
        course_title_column: str,
        course_created_at_column: str,
        course_short_desc_column: str,
        course_desc_column: str,
        course_link_column: str,
    ) -> None:
        self.bq = bq
        self.dataset = dataset
        self.course_table = course_table
        self.course_id_column = _ensure_safe_identifier(course_id_column, "course id column")
        self.course_title_column = _ensure_safe_identifier(course_title_column, "course title column")
        self.course_created_at_column = _ensure_safe_identifier(course_created_at_column, "course created column")
        self.course_short_desc_column = _ensure_safe_identifier(
            course_short_desc_column, "course short description column"
        )
        self.course_desc_column = _ensure_safe_identifier(course_desc_column, "course description column")
        self.course_link_column = _ensure_safe_identifier(course_link_column, "course link column")

    def get_course(self, course_id: str) -> Optional[Dict[str, Any]]:
        table = self.bq.table_ref(self.dataset, self.course_table)
        sql = f"""
        SELECT
            {self.course_id_column} AS id,
            {self.course_title_column} AS lesson_title,
            {self.course_created_at_column} AS created_at,
            {self.course_short_desc_column} AS short_description,
            {self.course_desc_column} AS description,
            {self.course_link_column} AS link
        FROM {table}
        WHERE {self.course_id_column} = @course_id
        LIMIT 1
        """
        params: Sequence[bigquery.ScalarQueryParameter] = [
            bigquery.ScalarQueryParameter("course_id", "STRING", course_id),
        ]
        rows = self.bq.run_query(sql, parameters=params)
        if not rows:
            return None
        return rows[0]
