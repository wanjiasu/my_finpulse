import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from .settings import settings

# 创建 SQLAlchemy 引擎
engine = create_engine(settings.POSTGRES_DSN)

def get_conn():
    return psycopg2.connect(settings.POSTGRES_DSN)


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 任务结果表
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS task_results (
                    id SERIAL PRIMARY KEY,
                    celery_task_id TEXT UNIQUE NOT NULL,
                    result TEXT,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )
            # 日线行情表
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_daily (
                    ts_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    pre_close FLOAT,
                    change FLOAT,
                    pct_chg FLOAT,
                    vol FLOAT,
                    amount FLOAT,
                    PRIMARY KEY (ts_code, trade_date)
                );
                CREATE INDEX IF NOT EXISTS idx_stock_daily_date ON stock_daily (trade_date);
                """
            )
            # 复权因子表
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_adj_factor (
                    ts_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    adj_factor FLOAT,
                    PRIMARY KEY (ts_code, trade_date)
                );
                CREATE INDEX IF NOT EXISTS idx_stock_adj_date ON stock_adj_factor (trade_date);
                """
            )
            conn.commit()


def save_result(celery_task_id: str, result: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO task_results (celery_task_id, result)
                VALUES (%s, %s)
                ON CONFLICT (celery_task_id)
                DO UPDATE SET result = EXCLUDED.result;
                """,
                (celery_task_id, result),
            )
            conn.commit()


def fetch_result(celery_task_id: str):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT celery_task_id, result, created_at FROM task_results WHERE celery_task_id=%s",
                (celery_task_id,),
            )
            return cur.fetchone()
