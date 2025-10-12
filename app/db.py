# app/db.py
from __future__ import annotations
from typing import List, Sequence, Any, Tuple
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from psycopg2 import sql


def _split_table(table: str) -> Tuple[str, str]:
    """
    'schema.table' or 'table' を ('schema', 'table') に分割。
    スキーマ省略時は 'public' を既定とする。
    """
    if "." in table:
        schema, name = table.split(".", 1)
        return schema, name
    return "public", table


def _coerce_nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """
    psycopg2 に渡す前に NaN を None に変換。
    object 以外の列でも None に揃えておくと安定する。
    """
    # pandas 2.x でも OK: where+mask で NaN→None
    return df.where(pd.notnull(df), None)


class PostgresClient:
    def __init__(self, url: str):
        self.url = url

    def _conn(self):
        # 必要なら keepalive 追加:
        # return psycopg2.connect(self.url, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5)
        return psycopg2.connect(self.url)

    def get_table_columns(self, table: str) -> List[str]:
        schema, name = _split_table(table)
        sql_text = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name   = %s
        ORDER BY ordinal_position
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql_text, (schema, name))
            return [r[0] for r in cur.fetchall()]

    def insert_dataframe(self, df: pd.DataFrame, table: str):
        if df.empty:
            return

        df = _coerce_nan_to_none(df)

        cols: List[str] = list(df.columns)
        records: Sequence[Sequence[Any]] = list(df.itertuples(index=False, name=None))

        schema, name = _split_table(table)

        # 安全な識別子クオート（予約語や記号に強い）
        col_ident_list = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
        table_ident = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(name))
        query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(table_ident, col_ident_list)

        try:
            with self._conn() as conn, conn.cursor() as cur:
                extras.execute_values(cur, query.as_string(conn), records, template=None, page_size=1000)
        except psycopg2.Error as e:
            msg = [
                f"psycopg2.Error: {type(e).__name__}",
                f"pgcode={getattr(e, 'pgcode', None)}",
                f"pgerror={getattr(e, 'pgerror', None)}",
                f"diag.message_primary={getattr(getattr(e, 'diag', None), 'message_primary', None)}",
                f"diag.context={getattr(getattr(e, 'diag', None), 'context', None)}",
                f"diag.detail={getattr(getattr(e, 'diag', None), 'detail', None)}",
                f"diag.hint={getattr(getattr(e, 'diag', None), 'hint', None)}",
            ]
            try:
                preview = records[:5]
                msg.append(f"records_preview={preview}")
            except Exception:
                pass
            raise RuntimeError("\n".join(msg)) from e

    def copy_dataframe(self, df: pd.DataFrame, table: str):
        # 必要十分：現在は insert_dataframe のラッパー
        return self.insert_dataframe(df, table)
