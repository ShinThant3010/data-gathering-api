from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence

from google.cloud import bigquery

from ..bigquery_client import BigQueryClient
from .exam_repo import _ensure_safe_identifier


class FullQuestionRepository:
    def __init__(
        self,
        bq: BigQueryClient,
        *,
        dataset: str,
        question_table: str,
        answer_table: str,
        test_id_column: str,
        question_id_column: str,
        answer_fk_column: str,
    ) -> None:
        self.bq = bq
        self.dataset = dataset
        self.question_table = question_table
        self.answer_table = answer_table
        self.test_id_column = _ensure_safe_identifier(test_id_column, "test id column")
        self.question_id_column = _ensure_safe_identifier(question_id_column, "question result id column")
        self.answer_fk_column = _ensure_safe_identifier(answer_fk_column, "answer result FK column")

    def get_questions_with_answers(self, *, test_id: str) -> List[Dict[str, Any]]:
        questions = self._fetch_questions(test_id)
        if not questions:
            return []

        question_ids = [
            row[self.question_id_column] for row in questions if self.question_id_column in row
        ]
        answers = self._fetch_answers(question_ids) if question_ids else []
        answers_by_question = self._group_by(answers, key=self.answer_fk_column)

        payload: List[Dict[str, Any]] = []
        for question in questions:
            question_id = question.get(self.question_id_column)
            payload.append(
                {
                    "question": question,
                    "answers": answers_by_question.get(question_id, []),
                }
            )
        return payload

    def _fetch_questions(self, test_id: str) -> List[dict[str, Any]]:
        table = self.bq.table_ref(self.dataset, self.question_table)
        print("table:", table, "test_id:", test_id, "column:", self.test_id_column)
        sql = f"""
        SELECT *
        FROM {table}
        WHERE {self.test_id_column} = @test_id
        """
        params: Sequence[bigquery.ScalarQueryParameter] = [
            bigquery.ScalarQueryParameter("test_id", "STRING", test_id),
        ]
        return self.bq.run_query(sql, parameters=params)

    def _fetch_answers(self, question_ids: Iterable[Any]) -> List[dict[str, Any]]:
        table = self.bq.table_ref(self.dataset, self.answer_table)
        sql = f"""
        SELECT *
        FROM {table}
        WHERE {self.answer_fk_column} IN UNNEST(@question_ids)
        """
        params = [
            bigquery.ArrayQueryParameter("question_ids", "STRING", list(question_ids)),
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
