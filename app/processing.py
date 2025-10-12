from __future__ import annotations
import csv
import re
from typing import Dict, Tuple, List, Optional
import pandas as pd


def _sanitize_bytes(b: bytes) -> bytes:
    # 制御文字(0x1A)やBOMを除去
    b = b.replace(b"\x1a", b"")
    b = b.replace(b"\xef\xbb\xbf", b"")  # UTF-8 BOM
    return b


def _read_rows(path: str, encoding: str, sep: str) -> List[List[str]]:
    # 1) バイナリで全読込 → 軽くクレンジング → decode
    with open(path, "rb") as f:
        data = _sanitize_bytes(f.read())

    text = data.decode(encoding, errors="ignore")

    # 2) csv.readerで行パース（列数が揃わない行はそのまま。後続でDataFrame側で吸収）
    reader = csv.reader(text.splitlines(), delimiter=sep, quotechar='"')
    rows = [r for r in reader]

    # 3) 全列空白の行は落とす
    rows = [r for r in rows if any((c or "").strip() for c in r)]
    return rows


def _zen2han_space(s: str) -> str:
    return (s or "").replace("　", " ").strip()


def _parse_year_month_any(s: str) -> Optional[pd.Timestamp]:
    """
    先頭数行から拾った文字列から Year-Month を推定。
    対応:
      - 令和X年Y月
      - YYYY/MM, YYYY-MM, YYYY年MM月 などの一般和暦/西暦表記
    成功すれば、その月1日 00:00 の Timestamp を返す。
    """
    if not s:
        return None

    t = re.sub(r"\s+", "", s)

    # 1) 令和
    m = re.search(r"(令和)(\d+)年(\d+)月", t)
    if m:
        era, yy, mm = m.group(1), int(m.group(2)), int(m.group(3))
        if era == "令和":
            year = 2018 + yy  # 令和1=2019
            try:
                return pd.Timestamp(year=year, month=mm, day=1)
            except Exception:
                return None

    # 2) 西暦パターン
    # YYYY年MM月 / YYYY-MM / YYYY/MM / YYYY.M
    m = re.search(r"(\d{4})[年/\-\.](\d{1,2})月?", t)
    if m:
        year, mm = int(m.group(1)), int(m.group(2))
        try:
            return pd.Timestamp(year=year, month=mm, day=1)
        except Exception:
            return None

    return None


def extract_meta_and_dataframe(
    csv_path: str,
    encoding: str = "cp932",
    sep: str = ",",
    preface_rows_to_drop: int = 4,
    header_in_row_after_skip: bool = True,
    meta_cells: dict | None = None,
):
    import io, csv, pandas as pd

    meta_cells = meta_cells or {}

    def _coerce_pos(v):
        if v is None:
            return None
        try:
            if isinstance(v, (list, tuple)) and len(v) >= 2:
                r, c = int(v[0]) - 1, int(v[1]) - 1
            elif isinstance(v, dict):
                r, c = int(v.get("row")) - 1, int(v.get("col")) - 1
            else:
                return None
            return (r, c) if r >= 0 and c >= 0 else None
        except Exception:
            return None

    # --- テキスト読込（フォールバック付き） ---
    text = None
    used_enc = None
    for enc in [encoding, "utf-8-sig", "utf-8", "cp932", "utf-16", "utf-16le", "utf-16be"]:
        try:
            with open(csv_path, "r", encoding=enc, errors="strict") as fp:
                text = fp.read()
            used_enc = enc
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as fp:
            text = fp.read()
        used_enc = "utf-8(replace)"

    lines = text.splitlines()

    # --- メタ抽出 ---
    def _get_meta(key):
        pos = _coerce_pos(meta_cells.get(key))
        if not pos:
            return None
        r, c = pos
        if r < 0 or r >= len(lines):   # ← ここでlinesを使う
            return None
        row_vals = lines[r].split(sep)
        return row_vals[c].strip().strip('"') if c < len(row_vals) else None

    ym_raw = _get_meta("year_month_raw")
    ym = _parse_year_month_any(ym_raw or "")
    meta = {
        "facility_name": _get_meta("facility_name"),
        "year_month_raw": ym_raw,
        "year_month": ym.date() if ym is not None else None,
    }
    print(f"pos: {_coerce_pos(meta_cells.get('facility_name'))}")

    # --- DataFrame化（段階的リトライ） ---
    def _read_with(sep_, header_idx):
        try:
            return pd.read_csv(io.StringIO(text), sep=sep_, header=header_idx, dtype=str, encoding="utf-8")
        except Exception:
            return pd.DataFrame()

    header_idx = preface_rows_to_drop if header_in_row_after_skip else 0
    df = _read_with(sep, header_idx)

    if df.empty and header_idx != 0:
        df = _read_with(sep, 0)

    if df.empty:
        try:
            sniffed = csv.Sniffer().sniff(text[:2048], delimiters=[",", "\t", ";", "|"])
            if sniffed.delimiter != sep:
                df = _read_with(sniffed.delimiter, 0)
        except Exception:
            pass

    if not df.empty:
        df = df.loc[:, ~df.columns.astype(str).str.lower().str.startswith("unnamed")]
        df = df.dropna(how="all")

    return df, meta



def transform_with_mapping(df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
    # 入力ヘッダー側の全角空白・前後空白をまず正規化
    df = df.copy()
    df.columns = [_zen2han_space(str(c)) for c in df.columns]

    # 設定のJP列名側も正規化してから rename マップ化
    rename_map = {}
    for jp, dbcol in (column_mapping or {}).items():
        rename_map[_zen2han_space(jp)] = dbcol

    df = df.rename(columns=rename_map)

    # 余計な全角空白混入をさらにケア（全列文字型で一括置換はしない：型は main 側でキャスト）
    return df
