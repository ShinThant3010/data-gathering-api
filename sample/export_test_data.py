#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path
from typing import Iterable

EXAM_RESULT_IDS = [
    "01KCXK1Q6BRCR2C6QESKVZRHQP",
"01KCXK1Q7NM9JBWVVB6SC1155M",
"01KCXK1QCF6H57W5F1P0VMTCHN",
"01KCXK1QD01J4X7X33G4HT5ZWD",
"01KCXK1Q2TMR3KKHYQBGZ9TP1A",
"01KCXK1Q4N3VWVXEEYRXMJ6GP8",
"01KCXK1Q4YWYSVKFEH8NP6AQH9",
"01KCXK1Q6YAVVTAR18XXXP5CEQ",
"01KCXK1QADJ9VR907V1TNDS6YN",
"01KCXK1QBGYPAT3J4QMFWBCDS2",
"01KEY5TSTDE0SS3WK7EETVCQR6",
"01KCXK1QEVP9ENN8C9V15YY8S0",
"01KCXK1QAZQFB2QA99DAMX9DD8",
"01KCXK1Q96V5TD025840DC1RAS",
"01KCXK1Q9WZQ5X7JCECT7XJB23"
]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def _parse_ids(raw_ids: Iterable[str]) -> set[str]:
    ids: set[str] = set()
    for token in raw_ids:
        parts = [p.strip() for p in token.split(",")]
        ids.update(p for p in parts if p)
    return ids


def export_filtered_data(data_dir: Path, output_dir: Path, exam_result_ids: set[str]) -> dict[str, int]:
    exam_result_rows = _read_csv(data_dir / "exam_result.csv")
    exam_question_rows = _read_csv(data_dir / "exam_question_result.csv")
    exam_answer_rows = _read_csv(data_dir / "exam_answer_result.csv")
    question_rows = _read_csv(data_dir / "question.csv")
    answer_rows = _read_csv(data_dir / "answer.csv")

    filtered_exam_results = [r for r in exam_result_rows if r.get("id") in exam_result_ids]
    matched_exam_result_ids = {r["id"] for r in filtered_exam_results if r.get("id")}

    filtered_exam_questions = [
        r for r in exam_question_rows if r.get("exam_result_id") in matched_exam_result_ids
    ]
    matched_exam_question_ids = {r["id"] for r in filtered_exam_questions if r.get("id")}
    matched_question_ids = {r["question_id"] for r in filtered_exam_questions if r.get("question_id")}

    filtered_exam_answers = [
        r for r in exam_answer_rows if r.get("exam_result_question_id") in matched_exam_question_ids
    ]
    answer_ids_from_exam_answers = {r["answer_id"] for r in filtered_exam_answers if r.get("answer_id")}

    filtered_questions = [r for r in question_rows if r.get("id") in matched_question_ids]
    answer_ids_from_questions = {
        r["answer_id"] for r in filtered_questions if r.get("answer_id")
    }

    # Supports both possible schemas:
    # 1) question.answer_id -> answer.id
    # 2) answer.question_id -> question.id
    question_id_to_answer_schema = any("question_id" in r for r in answer_rows)
    answer_id_schema = any("answer_id" in r for r in filtered_questions)

    answer_ids_union = answer_ids_from_exam_answers | answer_ids_from_questions
    if question_id_to_answer_schema:
        filtered_answers = [
            r
            for r in answer_rows
            if r.get("question_id") in matched_question_ids or r.get("id") in answer_ids_union
        ]
    elif answer_id_schema:
        filtered_answers = [r for r in answer_rows if r.get("id") in answer_ids_union]
    else:
        filtered_answers = [r for r in answer_rows if r.get("id") in answer_ids_from_exam_answers]

    _write_csv(output_dir / "exam_result.csv", filtered_exam_results, list(exam_result_rows[0].keys()))
    _write_csv(
        output_dir / "exam_question_result.csv",
        filtered_exam_questions,
        list(exam_question_rows[0].keys()),
    )
    _write_csv(
        output_dir / "exam_answer_result.csv",
        filtered_exam_answers,
        list(exam_answer_rows[0].keys()),
    )
    _write_csv(output_dir / "question.csv", filtered_questions, list(question_rows[0].keys()))
    _write_csv(output_dir / "answer.csv", filtered_answers, list(answer_rows[0].keys()))

    return {
        "exam_result": len(filtered_exam_results),
        "exam_question_result": len(filtered_exam_questions),
        "exam_answer_result": len(filtered_exam_answers),
        "question": len(filtered_questions),
        "answer": len(filtered_answers),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export related exam/test data by exam_result_id(s) from data/*.csv "
            "into separate CSV files under sample/test_data."
        )
    )
    parser.add_argument(
        "--exam-result-ids",
        nargs="+",
        required=False,
        help="One or more exam_result id values. Comma-separated values are also supported.",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Source CSV directory (default: data).",
    )
    parser.add_argument(
        "--output-dir",
        default="sample/test_data",
        help="Output directory for filtered CSVs (default: sample/test_data).",
    )
    args = parser.parse_args()

    if args.exam_result_ids:
        ids = _parse_ids(args.exam_result_ids)
    else:
        ids = _parse_ids(EXAM_RESULT_IDS)
    if not ids:
        raise SystemExit(
            "No valid exam_result_id values were provided. "
            "Set EXAM_RESULT_IDS in the script or pass --exam-result-ids."
        )

    counts = export_filtered_data(
        data_dir=Path(args.data_dir),
        output_dir=Path(args.output_dir),
        exam_result_ids=ids,
    )
    print("Export completed.")
    for name, count in counts.items():
        print(f"{name}: {count}")


if __name__ == "__main__":
    main()
