from typing import Optional
import os

from sqlalchemy import create_engine
import pandas as pd
import pandas.io.sql as psql


class Connection:
    def __init__(self,
                 db_name: str = "lake",
                 host: str = 'localhost',
                 port: int = 5432):
        # database uri parameters
        self.db_name = db_name
        self.host = host
        self.port = port

        # default credentials
        self.username = "admin"
        self.password = "admin"

        # instance of sqlalchemy connection pool
        self.conn = None

    def read_query(self, query: str) -> pd.DataFrame:
        with self.conn.begin() as conn:
            df = psql.read_sql_query(query, conn)

        return df

    def set_credentials(self,
                        username: Optional[str] = None,
                        password: Optional[str] = None):
        """
        Manual credentials setup
        """
        self.username = username
        self.password = password

        return self

    def set_credentials_from_env(self):
        """
        Using credentials from environment variables:

        username <-> POSTGRES_USER
        password <-> POSTGRES_PASSWORD
        """
        self.username = os.environ['POSTGRES_USER']
        self.username = os.environ['POSTGRES_PASSWORD']

        return self

    def open_conn(self):
        """
        Creates connection pool
        """
        self.conn = create_engine(f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}")
        return self

    def close_conn(self):
        """
        Close connection
        """
        self.conn.close()
        return self

    def ping(self):
        """
        Ping database
        """
        with self.conn.begin() as conn:
            conn.execute("select 1")

        return self
