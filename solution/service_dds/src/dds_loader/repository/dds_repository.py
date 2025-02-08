import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db


    def insert_statement(self, table_name:str, column_dict:Dict, conflict_column:str, *columns:str) -> None:
        column_list = ''
        wildcard_column_list=''
        for c in columns:
            column_list += f'{c}, '
            wildcard_column_list += f'%({c})s, '
        statement = f'insert into dds.{table_name} ({column_list[:-2]}) values ({wildcard_column_list[:-2]}) on conflict ({conflict_column}) do nothing;'

        with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(statement, column_dict)