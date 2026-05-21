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
            # 股票基础信息表
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_basic (
                    ts_code TEXT PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    area TEXT,
                    industry TEXT,
                    fullname TEXT,
                    enname TEXT,
                    cnspell TEXT,
                    market TEXT,
                    exchange TEXT,
                    curr TEXT,
                    list_status TEXT,
                    list_date TEXT,
                    delist_date TEXT,
                    is_hs TEXT
                );
                """
            )
            # 同花顺指数表
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_ths_index (
                    ts_code TEXT PRIMARY KEY,
                    name TEXT,
                    count INTEGER,
                    exchange TEXT,
                    list_date TEXT,
                    type TEXT
                );
                """
            )
            # 同花顺板块成分表
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_ths_member (
                    ts_code TEXT NOT NULL,
                    con_code TEXT NOT NULL,
                    con_name TEXT,
                    PRIMARY KEY (ts_code, con_code)
                );
                CREATE INDEX IF NOT EXISTS idx_ths_member_code ON stock_ths_member (con_code);
                """
            )
            # 个股资金流向表
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_moneyflow (
                    ts_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    buy_sm_vol BIGINT,
                    buy_sm_amount FLOAT,
                    sell_sm_vol BIGINT,
                    sell_sm_amount FLOAT,
                    buy_md_vol BIGINT,
                    buy_md_amount FLOAT,
                    sell_md_vol BIGINT,
                    sell_md_amount FLOAT,
                    buy_lg_vol BIGINT,
                    buy_lg_amount FLOAT,
                    sell_lg_vol BIGINT,
                    sell_lg_amount FLOAT,
                    buy_elg_vol BIGINT,
                    buy_elg_amount FLOAT,
                    sell_elg_vol BIGINT,
                    sell_elg_amount FLOAT,
                    net_mf_vol BIGINT,
                    net_mf_amount FLOAT,
                    PRIMARY KEY (ts_code, trade_date)
                );
                CREATE INDEX IF NOT EXISTS idx_stock_moneyflow_date ON stock_moneyflow (trade_date);
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
