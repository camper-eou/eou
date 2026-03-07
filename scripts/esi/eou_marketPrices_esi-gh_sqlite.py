from __future__ import annotations

import queue
import sqlite3
import threading
from pathlib import Path
from typing import Dict, List, Optional

CREATE_ORDERS_SQL = """
CREATE TABLE IF NOT EXISTS orders (
  order_id INTEGER PRIMARY KEY,
  issued TEXT NOT NULL,
  location_id INTEGER NOT NULL,
  type_id INTEGER NOT NULL,
  is_buy INTEGER NOT NULL,
  price REAL NOT NULL,
  volume_remain INTEGER NOT NULL
);
"""


class OrdersWriter:
    """
    Escritor único hacia SQLite.

    Objetivo:
      - evitar contención de escritura entre workers
      - centralizar el dedupe por order_id en una sola conexión SQLite

    Regla de dedupe:
      - ante misma order_id, se conserva la fila con issued mayor
      - si issued empata, SQLite mantiene una de ellas indistintamente
    """

    def __init__(self, db_path: str | Path, queue_max: int = 100000, batch_size: int = 2000):
        self.db_path = Path(db_path)
        self.queue: queue.Queue = queue.Queue(maxsize=queue_max)
        self.batch_size = int(batch_size)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._stop = object()
        self._started = False
        self._exc: Optional[BaseException] = None

    def start(self) -> None:
        self._init_db()
        self._started = True
        self._thread.start()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA busy_timeout=60000;")
            conn.execute(CREATE_ORDERS_SQL)
            conn.commit()
        finally:
            conn.close()

    def push(self, orders: List[Dict]) -> None:
        if not self._started:
            raise RuntimeError("OrdersWriter not started")
        self.queue.put(orders)

    def stop(self) -> None:
        if self._started:
            self.queue.put(self._stop)
            self._thread.join()
        if self._exc:
            raise self._exc

    def _flush(self, conn: sqlite3.Connection, batch: List[tuple]) -> None:
        if not batch:
            return

        insert_sql = """
        INSERT INTO orders(order_id, issued, location_id, type_id, is_buy, price, volume_remain)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(order_id) DO UPDATE SET
          issued=excluded.issued,
          location_id=excluded.location_id,
          type_id=excluded.type_id,
          is_buy=excluded.is_buy,
          price=excluded.price,
          volume_remain=excluded.volume_remain
        WHERE excluded.issued > orders.issued;
        """

        conn.executemany(insert_sql, batch)
        conn.commit()
        batch.clear()

    def _run(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA busy_timeout=60000;")

            batch: List[tuple] = []
            while True:
                item = self.queue.get()
                if item is self._stop:
                    break

                for o in item:
                    batch.append(
                        (
                            int(o["order_id"]),
                            str(o["issued"]),
                            int(o["location_id"]),
                            int(o["type_id"]),
                            1 if bool(o["is_buy_order"]) else 0,
                            float(o["price"]),
                            int(o["volume_remain"]),
                        )
                    )
                    if len(batch) >= self.batch_size:
                        self._flush(conn, batch)

            self._flush(conn, batch)

        except BaseException as e:
            self._exc = e
        finally:
            conn.close()


def connect(db_path: str | Path) -> sqlite3.Connection:
    """
    Devuelve una conexión de lectura/consulta con Row factory activado.
    """
    conn = sqlite3.connect(Path(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=60000;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn
