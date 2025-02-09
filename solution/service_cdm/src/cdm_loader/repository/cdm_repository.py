import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_category_counters_insert(self,
                                      category_id: str,
                                      category_name: str,
                                      user_id: str
                                     ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters(category_id, category_name, user_id, order_cnt)
                        VALUES (%(category_id)s, %(category_name)s, %(user_id)s, 1)
                            ON CONFLICT (user_id, category_id) DO UPDATE
                           SET order_cnt = user_category_counters.order_cnt + 1;
                        ;
                    """,
                    {
                        'category_id': category_id,
                        'category_name': category_name,
                        'user_id': user_id
                    }
                )

    def user_product_counters_insert(self,
                                     product_id: str,
                                     product_name: str,
                                     user_id: str
                                     ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters(product_id, product_name, user_id, order_cnt)
                        VALUES (%(product_id)s, %(product_name)s, %(user_id)s, 1)
                            ON CONFLICT (user_id, product_id) DO UPDATE
                           SET order_cnt = user_product_counters.order_cnt + 1;
                        ;
                    """,
                    {
                        'product_id': product_id,
                        'product_name': product_name,
                        'user_id': user_id
                    }
                )