import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


class Connection:
    def __init__(self, db_type='mysql', database=None):
        self.db_type = db_type.lower()
        self.database = database

        if self.db_type == 'mysql':
            self.user = os.getenv("MYSQL_USER")
            self.password = os.getenv("MYSQL_PASSWORD")
            self.host = os.getenv("MYSQL_HOST", "localhost")
            self.port = os.getenv("MYSQL_PORT", "3306")
            self.driver = os.getenv("MYSQL_DRIVER", "mysql+pymysql")
            self.database = database or os.getenv("MYSQL_DATABASE")

        elif self.db_type == 'postgres':
            self.user = os.getenv("POSTGRESQL_USER")
            self.password = os.getenv("POSTGRESQL_PASS")
            self.host = os.getenv("POSTGRESQL_HOST", "localhost")
            self.port = os.getenv("POSTGRESQL_PORT", "5432")
            self.driver = os.getenv("POSTGRESQL_DRIVER", "postgresql")
            self.database = database or os.getenv("POSTGRESQL_DATABASE")

        else:
            raise ValueError("db_type must be either 'mysql' or 'postgres'")

        self.engine = self._create_engine()

    def _create_engine(self) -> Engine:
        try:
            engine = create_engine(
                f"{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
                echo=False
            )

            return engine
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    def execute_query(self, sql: str) -> None:
        logger.info(f"Running query: {sql}")
        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))

        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def get_sql_from_file(self, file_name: str) -> str:
        file_path = os.path.join("sql", file_name)
        try:
            with open(file_path, "r") as f:
                return f.read()
        except FileNotFoundError:
            logger.error(f"SQL file not found: {file_path}")
            raise

    def update_url_scrape_status(self, pkey: int, status: str, timestamp: str) -> None:
        sql = self.get_sql_from_file("update_url_scrape_status.sql")
        formatted_sql = sql.format(
            status=status, timestamp=timestamp, pkey=pkey)
        self.execute_query(formatted_sql)

    def extract_from_sql(self, sql: str) -> pd.DataFrame:
        try:
            return pd.read_sql(sql, self.engine)

        except Exception as e:
            logger.error(e)
            raise e

    def df_to_sql(self, data: pd.DataFrame, table_name: str):
        try:
            n = data.shape[0]
            data.to_sql(table_name, self.engine,
                        if_exists="append", index=False)
            logger.info(
                f"Successfully loaded {n} records to the {table_name}.")

        except Exception as e:
            logger.error(e)
            raise e
