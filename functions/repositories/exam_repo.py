from __future__ import annotations
from typing import Any, Dict, Iterable, List, Sequence
from google.cloud import bigquery
from ..bigquery_client import BigQueryClient

def _ensure_safe_identifier(identifier: str, kind: str) -> str:
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if not identifier or any(ch not in allowed for ch in identifier):
        raise ValueError(f"Invalid {kind} '{identifier}'. Only letters, numbers, and underscores are allowed.")
    return identifier


class ExamRepository:
    def __init__(
        self,
        bq: BigQueryClient,
        *,
        dataset: str,
        exam_result_table: str,
        exam_question_result_table: str,
        exam_answer_result_table: str,
        exam_result_order_column: str,
        exam_result_student_column: str,
        exam_result_test_column: str,
        exam_result_id_column: str,
        question_result_id_column: str,
        question_result_fk_column: str,
        answer_result_fk_column: str,
    ) -> None:
        self.bq = bq
        self.dataset = dataset
        self.exam_result_table = exam_result_table
        self.exam_question_result_table = exam_question_result_table
        self.exam_answer_result_table = exam_answer_result_table
        self.exam_result_order_column = _ensure_safe_identifier(exam_result_order_column, "order column")
        self.exam_result_student_column = _ensure_safe_identifier(exam_result_student_column, "student column")
        self.exam_result_test_column = _ensure_safe_identifier(exam_result_test_column, "test column")
        self.exam_result_id_column = _ensure_safe_identifier(exam_result_id_column, "exam result id column")
        self.question_result_id_column = _ensure_safe_identifier(question_result_id_column, "question result id column")
        self.question_result_fk_column = _ensure_safe_identifier(question_result_fk_column, "question result FK column")
        self.answer_result_fk_column = _ensure_safe_identifier(answer_result_fk_column, "answer result FK column")

    def get_latest_attempts(
        self, *, student_id: str, test_id: str, limit: int = 2
    ) -> List[Dict[str, Any]]:
        """Return latest exam attempts with nested question + answer results."""
        safe_limit = max(1, min(limit, 5))  # guardrail to prevent runaway queries
        exam_results = self._fetch_exam_results(student_id=student_id, test_id=test_id, limit=safe_limit)
        if not exam_results:
            return []

        exam_result_ids = [row[self.exam_result_id_column] for row in exam_results if self.exam_result_id_column in row]
        question_results = self._fetch_question_results(exam_result_ids)
        question_result_ids = [
            row[self.question_result_id_column] for row in question_results if self.question_result_id_column in row
        ]
        answer_results = self._fetch_answer_results(question_result_ids) if question_result_ids else []

        answers_by_question_id = self._group_by(
            answer_results, key=self.answer_result_fk_column
        )
        questions_by_exam_id = self._group_by(
            [
                {
                    **row,
                    "_answers": answers_by_question_id.get(row.get(self.question_result_id_column), []),
                }
                for row in question_results
            ],
            key=self.question_result_fk_column,
        )

        attempts: List[Dict[str, Any]] = []
        for result in exam_results:
            exam_id = result.get(self.exam_result_id_column)
            attempts.append(
                {
                    "exam_result": result,
                    "questions": questions_by_exam_id.get(exam_id, []),
                }
            )
        return attempts

    def _fetch_exam_results(self, *, student_id: str, test_id: str, limit: int) -> List[dict[str, Any]]:
        table = self.bq.table_ref(self.dataset, self.exam_result_table)
        sql = f"""
        SELECT *
        FROM {table}
        WHERE {self.exam_result_student_column} = @student_id
          AND {self.exam_result_test_column} = @test_id
        ORDER BY {self.exam_result_order_column} DESC
        LIMIT {limit}
        """
        params: Sequence[bigquery.ScalarQueryParameter] = [
            bigquery.ScalarQueryParameter("student_id", "STRING", student_id),
            bigquery.ScalarQueryParameter("test_id", "STRING", test_id),
        ]
        return self.bq.run_query(sql, parameters=params)

    def _fetch_question_results(self, exam_result_ids: Iterable[Any]) -> List[dict[str, Any]]:
        table = self.bq.table_ref(self.dataset, self.exam_question_result_table)
        sql = f"""
        SELECT *
        FROM {table}
        WHERE {self.question_result_fk_column} IN UNNEST(@exam_result_ids)
        """
        params = [
            bigquery.ArrayQueryParameter("exam_result_ids", "STRING", list(exam_result_ids)),
        ]
        return self.bq.run_query(sql, parameters=params)

    def _fetch_answer_results(self, question_result_ids: Iterable[Any]) -> List[dict[str, Any]]:
        table = self.bq.table_ref(self.dataset, self.exam_answer_result_table)
        sql = f"""
        SELECT *
        FROM {table}
        WHERE {self.answer_result_fk_column} IN UNNEST(@question_result_ids)
        """
        params = [
            bigquery.ArrayQueryParameter("question_result_ids", "STRING", list(question_result_ids)),
        ]
        return self.bq.run_query(sql, parameters=params)

    @staticmethod
    def _group_by(rows: List[dict[str, Any]], *, key: str) -> Dict[Any, List[dict[str, Any]]]:
        grouped: Dict[Any, List[dict[str, Any]]] = {}
        for row in rows:
            group_key = row.get(key)
            if group_key is None:
                continue
            grouped.setdefault(group_key, []).append(row)
        return grouped
