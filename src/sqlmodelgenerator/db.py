from dataclasses import dataclass
import logging
from sqlalchemy import create_engine
from sqlalchemy.engine import CursorResult, Engine

import os
from pype.exceptions import DbNotConnectedException
from pype.config import get_env


ENGINE: Engine | None = None


@dataclass
class DbConn:
    engine: Engine | None

    def connect(self):
        env = get_env()
        if os.environ.get("USE_PROD") == "y":
            user = env["prod"]["db"]["PGUSER"]
            host = env["prod"]["db"]["DB_HOST"]
            name = env["prod"]["db"]["DB_NAME"]
            pkey = env["prod"]["db"]["pkey"]
            pas_ = os.environ.get(pkey, None)
            if pas_ is None:
                raise Exception(f"Password env var {pkey} not set")
            db_url = f"postgresql://{user}:{pas_}@{host}:5432/{name}"
        else:
            user = env["dev"]["db"]["PGUSER"]
            host = env["dev"]["db"]["DB_HOST"]
            name = env["dev"]["db"]["DB_NAME"]
            db_url = f"postgresql://{user}@{host}:5432/{name}"
        self.engine = create_engine(db_url)

    def connection_ok(self) -> bool:
        return self.engine is not None

    def query(self, statement: str) -> CursorResult:
        if self.engine:
            with self.engine.connect() as conn:
                logging.info(statement)
                return conn.execute(statement)
        raise DbNotConnectedException


def connect() -> DbConn:
    db_conn = DbConn(None)
    db_conn.connect()
    if db_conn:
        return db_conn
    raise DbNotConnectedException

