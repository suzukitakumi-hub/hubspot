#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


JST = dt.timezone(dt.timedelta(hours=9))
TARGET_COURSES = ["CIA", "CISA", "CFE", "IFRS", "USCPA", "MBA"]
REVIEW_HUBSPOT_ONLY_SHEET = "HubSpot未登録送信"
REVIEW_MASTER_UNMATCHED_SHEET = "正本未突合"
REVIEW_SHEET_HEADERS = {
    REVIEW_HUBSPOT_ONLY_SHEET: [
        "送付日",
        "メール件名（HubSpotリンク）",
        "HubSpot内部名",
        "推定講座",
        "送付先区分",
        "送付リスト",
        "INTERNAL HUBSPOT IDS",
        "要確認理由",
    ],
    REVIEW_MASTER_UNMATCHED_SHEET: [
        "配信日",
        "配信時間",
        "メール内部名",
        "件名",
        "推定講座",
        "元タブ",
        "ステータス",
        "要確認理由",
    ],
}
REVIEW_SHEETS = [REVIEW_HUBSPOT_ONLY_SHEET, REVIEW_MASTER_UNMATCHED_SHEET]
COURSE_SHEET_HEADER = [
    "送付日",
    "メール件名（HubSpotリンク）",
    "メール内部名",
    "配信数",
    "開封数（bot除外）",
    "開封数（bot含む）",
    "クリック数",
    "配信停止数",
    "開封率（bot除外）",
    "開封率（bot含む）",
    "クリック率",
    "クリックスルー率",
    "配信停止率",
    "CV数",
    "CV内訳",
    "送付リスト",
    "INTERNAL HUBSPOT IDS",
    "対象月",
    "講座",
]
COURSE_SHEET_DISPLAY_HEADER = [value.replace("（", "\n（", 1) if "（" in value else value for value in COURSE_SHEET_HEADER]
COURSE_SHEET_COLS = len(COURSE_SHEET_HEADER)
COURSE_SHEET_INDEX = {name: idx for idx, name in enumerate(COURSE_SHEET_HEADER)}
COURSE_SHEET_DISPLAY_TO_LOGICAL = {
    display: logical for logical, display in zip(COURSE_SHEET_HEADER, COURSE_SHEET_DISPLAY_HEADER)
}
COURSE_SHEET_DISPLAY_TO_LOGICAL.update({logical: logical for logical in COURSE_SHEET_HEADER})

DEFAULT_SPREADSHEET_ID = "1i64xFz7mo8xzQ-0ceRfyQtM0GnShuEuu4_W95UWtzKE"
DEFAULT_SERVICE_ACCOUNT_JSON = "C:/Users/suzuki.takumi/Desktop/AI/Hubspot/micro-environs-470717-j2-58800aec23bb.json"

STAGING_PREFIX = "__stg__"
DEFAULT_GA4_MAP_MAX_AGE_MINUTES = 180
DEFAULT_VALIDATION_REPORT_MAX_AGE_MINUTES = 30
DEFAULT_PROVISIONAL_DAYS = 12


def now_jst() -> dt.datetime:
    return dt.datetime.now(JST)


def now_jst_iso() -> str:
    return now_jst().isoformat()


def parse_iso_datetime(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, data: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def strip_literal_prefix(value: Any) -> str:
    text = "" if value is None else str(value)
    return text[1:] if text.startswith("'") else text


def staging_tab_title(course: str) -> str:
    return f"{STAGING_PREFIX}{course}"


def review_staging_tab_title(title: str) -> str:
    return f"{STAGING_PREFIX}{title}"


def derive_ga4_map_manifest_path(csv_path: str) -> str:
    path = Path(csv_path)
    suffix = "_email_map.csv"
    if path.name.endswith(suffix):
        return str(path.with_name(path.name[: -len(suffix)] + "_manifest.json"))
    return str(path.with_suffix(path.suffix + ".manifest.json"))


def derive_validation_report_path(month: str, base_dir: str = ".") -> str:
    return str(Path(base_dir) / f"hubspot_course_sheet_validation_{month}.json")


def column_letter(index_1_based: int) -> str:
    if index_1_based <= 0:
        raise ValueError("Column index must be 1-based and positive.")
    out = []
    current = index_1_based
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        out.append(chr(65 + remainder))
    return "".join(reversed(out))


COURSE_SHEET_LAST_COLUMN = column_letter(COURSE_SHEET_COLS)


def normalize_sheet_matrix(values: List[List[Any]], width: int = COURSE_SHEET_COLS) -> List[List[str]]:
    normalized: List[List[str]] = []
    for row in values:
        current = ["" if cell is None else str(cell) for cell in list(row)[:width]]
        if len(current) < width:
            current.extend([""] * (width - len(current)))
        if any(cell != "" for cell in current):
            normalized.append(current)
    return normalized


def normalize_header_name(value: str) -> str:
    return COURSE_SHEET_DISPLAY_TO_LOGICAL.get(str(value), str(value))


def normalize_header_row(row: List[str]) -> List[str]:
    return [normalize_header_name(value) for value in list(row[:COURSE_SHEET_COLS])]


def header_matches_expected(row: List[str]) -> bool:
    return normalize_header_row(row) == COURSE_SHEET_HEADER


def read_worksheet_matrix(worksheet, value_render_option: str | None = None) -> List[List[str]]:
    range_name = f"A1:{COURSE_SHEET_LAST_COLUMN}{max(worksheet.row_count, 1)}"
    kwargs: Dict[str, Any] = {}
    if value_render_option:
        kwargs["value_render_option"] = value_render_option
    values = worksheet.get(range_name, **kwargs)
    return normalize_sheet_matrix(values)


def snapshot_sha256(snapshot: Dict[str, List[List[str]]]) -> str:
    payload = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ensure_worksheet(spreadsheet, title: str, rows: int, cols: int):
    import gspread

    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def set_worksheet_hidden(spreadsheet, worksheet, hidden: bool) -> None:
    spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": worksheet.id, "hidden": hidden},
                        "fields": "hidden",
                    }
                }
            ]
        }
    )


def write_sheet_values(worksheet, values, apply_formatting: bool = True) -> None:
    materialized_values = [list(row) for row in values]
    if materialized_values and normalize_header_row(materialized_values[0]) == COURSE_SHEET_HEADER:
        materialized_values[0] = list(COURSE_SHEET_DISPLAY_HEADER)
    worksheet.clear()
    worksheet.update(materialized_values, value_input_option="USER_ENTERED")
    if not apply_formatting:
        return
    worksheet.freeze(rows=1)
    worksheet.batch_format(
        [
            {"range": "1:1", "format": {"wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE"}},
            {
                "range": f"A2:{COURSE_SHEET_LAST_COLUMN}",
                "format": {"horizontalAlignment": "RIGHT", "verticalAlignment": "MIDDLE"},
            },
            {
                "range": f"{column_letter(COURSE_SHEET_INDEX['送付リスト'] + 1)}2:{column_letter(COURSE_SHEET_INDEX['送付リスト'] + 1)}",
                "format": {"wrapStrategy": "CLIP"},
            },
            {"range": "A:C", "format": {"numberFormat": {"type": "TEXT"}}},
            {"range": "D:H", "format": {"numberFormat": {"type": "NUMBER", "pattern": "0"}}},
            {"range": "I:J", "format": {"numberFormat": {"type": "TEXT"}}},
            {"range": "K:K", "format": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
            {"range": "L:M", "format": {"numberFormat": {"type": "TEXT"}}},
            {"range": "N:N", "format": {"numberFormat": {"type": "NUMBER", "pattern": "0"}}},
            {"range": f"O:{COURSE_SHEET_LAST_COLUMN}", "format": {"numberFormat": {"type": "TEXT"}}},
        ]
    )
    worksheet.spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": worksheet.id,
                            "dimension": "ROWS",
                            "startIndex": 1,
                            "endIndex": worksheet.row_count,
                        },
                        "properties": {"pixelSize": 21},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": worksheet.id,
                            "dimension": "ROWS",
                            "startIndex": 0,
                            "endIndex": 1,
                        },
                        "properties": {"pixelSize": 42},
                        "fields": "pixelSize",
                    }
                }
            ]
        }
    )


def write_simple_sheet_values(worksheet, header: List[str], rows: List[List[Any]], apply_formatting: bool = True) -> None:
    values = [list(header)] + [list(row) for row in rows]
    worksheet.clear()
    worksheet.update(values, value_input_option="USER_ENTERED")
    if not apply_formatting:
        return
    worksheet.freeze(rows=1)
    worksheet.batch_format(
        [
            {"range": "1:1", "format": {"wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE"}},
            {"range": "A2:Z", "format": {"horizontalAlignment": "RIGHT", "verticalAlignment": "MIDDLE"}},
        ]
    )
    worksheet.spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": worksheet.id,
                            "dimension": "ROWS",
                            "startIndex": 1,
                            "endIndex": worksheet.row_count,
                        },
                        "properties": {"pixelSize": 21},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": worksheet.id,
                            "dimension": "ROWS",
                            "startIndex": 0,
                            "endIndex": 1,
                        },
                        "properties": {"pixelSize": 42},
                        "fields": "pixelSize",
                    }
                },
            ]
        }
    )
