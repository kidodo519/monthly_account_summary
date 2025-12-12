from __future__ import annotations
import os
import sys
import tempfile
from typing import List, Dict, Any
import numpy as np
import pandas as pd
import yaml
from dotenv import load_dotenv
from multiprocessing import freeze_support

from app.drive_client import DriveClient
from app.db import PostgresClient
from app.processing import extract_meta_and_dataframe, transform_with_mapping


def load_config():
    if getattr(sys, "frozen", False):
        base_path = os.path.dirname(sys.executable)
    else:
        base_path = os.path.dirname(__file__)

    config_candidates = [
        os.path.join(base_path, "config.yaml"),
        os.path.join(base_path, "..", "config.yaml"),
    ]

    for cfg_path in config_candidates:
        abs_path = os.path.abspath(cfg_path)
        if os.path.exists(abs_path):
            with open(abs_path, "r", encoding="utf-8") as fp:
                return yaml.safe_load(fp)

    raise FileNotFoundError(
        f"config.yaml was not found in any of: {', '.join(map(os.path.abspath, config_candidates))}"
    )


def _cast_text(s: pd.Series) -> pd.Series:
    def norm(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        x = str(v).replace("　", " ").strip()
        return x if x else None
    return s.apply(norm)


def _cast_int(s: pd.Series) -> pd.Series:
    def parse(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return 0
        try:
            return int(str(v).replace(",", "").replace("\u2212", "-"))
        except Exception:
            return 0
    return s.map(parse)


def _cast_date(s: pd.Series) -> pd.Series:
    def conv(v):
        if not v or (isinstance(v, float) and pd.isna(v)):
            return None
        dt = pd.to_datetime(str(v).strip(), errors="coerce")
        return None if pd.isna(dt) else dt.date()
    return s.apply(conv)


def _ensure_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df


def _normalize_facility(name: str | None) -> str | None:
    if not name:
        return None
    nm = str(name).replace("　", " ")
    if "夢乃井" in nm:
        return "夢乃井"
    if "加里屋" in nm:
        return "加里屋旅館"
    if ("祥吉" in nm) or ("祥吉" in nm):
        return "祥吉"
    return nm.strip()


#テーブル行数を数えるユーティリティ
def _count_rows(pg: PostgresClient, table: str) -> int:
    schema, name = table.split(".", 1) if "." in table else ("public", table)
    with pg._conn() as conn, conn.cursor() as cur:
        cur.execute(f'SELECT count(*) FROM "{schema}"."{name}"')
        return cur.fetchone()[0]

#DSNを安全に表示（パスワードは出さない）
def _print_dsn(pg: PostgresClient, table: str):
    try:
        with pg._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user, inet_server_addr()::text, inet_server_port()")
            dbname, user, host, port = cur.fetchone()
            print(f"[DB] database={dbname} user={user} host={host} port={port} target_table={table}")
            cur.execute("SELECT current_schema()")
            print(f"[DB] current_schema={cur.fetchone()[0]}")
    except Exception as e:
        print(f"[DB] dsn_print_error={e}")

def run_once():
 # base path / .env 読み込み
    if getattr(sys, "frozen", False):
        base_path = os.path.dirname(sys.executable)
    else:
        base_path = os.path.dirname(__file__)

    env_candidates = [
        os.path.join(base_path, ".env"),
        os.path.join(base_path, "..", ".env"),
    ]

    loaded_env = False
    for env_path in env_candidates:
        abs_path = os.path.abspath(env_path)
        if os.path.exists(abs_path):
            load_dotenv(abs_path, override=True)
            loaded_env = True

    if not loaded_env:
        load_dotenv(override=True)

    cfg = load_config()

    # Drive
    client = DriveClient()
    drive_cfg = cfg["drive"]
    folder_id = drive_cfg["folder_id"]
    archive_folder_id = drive_cfg["archive_folder_id"]

    # DB
    db_url = cfg["database"]["url"]
    if db_url.startswith("postgresql+psycopg2://"):
        db_url = "postgresql://" + db_url.split("postgresql+psycopg2://", 1)[1]
    pg = PostgresClient(db_url)
    table = cfg["ingest"]["target_table"]

    # 接続先DBの確認ログ（パスワードは出さない）
    try:
        with pg._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user, inet_server_addr()::text, inet_server_port()")
            dbname, user, host, port = cur.fetchone()
            cur.execute("SELECT current_schema()")
            current_schema = cur.fetchone()[0]
            print(f"[DB] database={dbname} user={user} host={host} port={port} current_schema={current_schema} target_table={table}")
    except Exception as e:
        print(f"[DB] dsn_print_error={e}")

    schema_types = cfg.get("schema", {}).get("types", {})

    # Driveから対象CSVを列挙
    files = client.list_csv_files(folder_id, page_size=drive_cfg.get("page_size", 100))
    patterns = drive_cfg.get("filename_globs") or []
    if patterns:
        targets = [f for f in files if any(fnmatch.fnmatch(f.name, p) for p in patterns)]
    else:
        targets = files[:]
    if not targets:
        print("[RUN] no target files.")
        return

    # CSV読取設定
    csv_cfg = cfg["csv"]
    encoding = csv_cfg.get("encoding", "cp932")
    sep = csv_cfg.get("sep", ",")
    preface_rows_to_drop = int(csv_cfg.get("preface_rows_to_drop", 4))
    header_in_row_after_skip = bool(csv_cfg.get("header_in_row_after_skip", True))
    meta_cells = csv_cfg.get("meta_cells") or {}
    column_mapping = csv_cfg.get("column_mapping") or {}

    # テーブル列名を一度取得
    try:
        table_cols = pg.get_table_columns(table)
    except Exception as e:
        print(f"[DB] get_table_columns error for {table}: {e}")
        return

    # schema.table の分解（行数カウント用）
    if "." in table:
        t_schema, t_name = table.split(".", 1)
    else:
        t_schema, t_name = "public", table

    for f in targets:
        with tempfile.TemporaryDirectory() as td:
            local_path = os.path.join(td, f.name)
            client.download_file(f.id, local_path, mime_type_hint=f.mimeType)

            # CSV → DF + メタ抽出
            df_raw, meta = extract_meta_and_dataframe(
                csv_path=local_path,
                encoding=encoding,
                sep=sep,
                preface_rows_to_drop=preface_rows_to_drop,
                header_in_row_after_skip=header_in_row_after_skip,
                meta_cells=meta_cells,
            )

            # ログ：原データの概要
            try:
                raw_cols = list(df_raw.columns)
            except Exception:
                raw_cols = []
            print(f"[{f.name}] raw rows={len(df_raw)} cols={raw_cols} meta={meta}")

            # マッピング適用
            df = transform_with_mapping(df_raw.copy(), column_mapping=column_mapping)
            df["facility_name"] = _normalize_facility(meta.get("facility_name"))
            df["year_month"] = meta.get("year_month")

            # 型変換・不足列の補完
            df = _ensure_columns(df, schema_types.keys())
            for col, kind in schema_types.items():
                if col not in df.columns:
                    continue
                if kind == "int":
                    df[col] = _cast_int(df[col])
                elif kind == "date":
                    df[col] = _cast_date(df[col])
                else:
                    df[col] = _cast_text(df[col])

            # ログ：マッピング後の概要
            print(f"[{f.name}] mapped rows={len(df)} cols={list(df.columns)}")

            # テーブルと共通カラムで絞る
            common = [c for c in table_cols if c in df.columns]
            if not common:
                print(f"[{f.name}] no common columns with table {table}. table_cols(sample)={table_cols[:8]} df_cols(sample)={[*df.columns][:8]}")
                # 共通カラム0ならアーカイブせずに次へ
                continue
            df = df[common]

            # 空なら挿入もアーカイブもしない
            if df.empty:
                print(f"[{f.name}] DataFrame is empty after transform. Skip insert & keep file.")
                continue

            # 挿入前後の行数で実増分を確認
            try:
                with pg._conn() as conn, conn.cursor() as cur:
                    cur.execute(f'SELECT COUNT(*) FROM "{t_schema}"."{t_name}"')
                    before = cur.fetchone()[0]

                    if cfg["ingest"].get("method", "insert") == "copy":
                        # copy_dataframe は insert_dataframe のラッパー想定
                        pg.copy_dataframe(df, table)
                    else:
                        pg.insert_dataframe(df, table)

                    cur.execute(f'SELECT COUNT(*) FROM "{t_schema}"."{t_name}"')
                    after = cur.fetchone()[0]
                    delta = after - before
            except Exception as e:
                print(f"[{f.name}] INSERT error: {e}")
                # エラー時は移動しない
                continue

            print(f"[{f.name}] inserted_rows={delta} (before={before} -> after={after})")

            if delta > 0:
                client.move_file(f.id, archive_folder_id)
                print(f"Processed and moved: {f.name} (inserted {delta} rows)")
            else:
                print(f"[{f.name}] actual insert 0 rows → Skip moving to archive.")


if __name__ == "__main__":
    # onefile + multiprocessing 環境でも安全
    freeze_support()
    run_once()
