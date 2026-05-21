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
                COMMENT ON TABLE task_results IS '任务结果记录表';
                COMMENT ON COLUMN task_results.id IS '自增ID';
                COMMENT ON COLUMN task_results.celery_task_id IS 'Celery任务唯一ID';
                COMMENT ON COLUMN task_results.result IS '任务运行结果内容';
                COMMENT ON COLUMN task_results.created_at IS '创建时间';
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
                COMMENT ON TABLE stock_daily IS '股票日线行情表';
                COMMENT ON COLUMN stock_daily.ts_code IS 'TS股票代码';
                COMMENT ON COLUMN stock_daily.trade_date IS '交易日期';
                COMMENT ON COLUMN stock_daily.open IS '开盘价';
                COMMENT ON COLUMN stock_daily.high IS '最高价';
                COMMENT ON COLUMN stock_daily.low IS '最低价';
                COMMENT ON COLUMN stock_daily.close IS '收盘价';
                COMMENT ON COLUMN stock_daily.pre_close IS '昨收价';
                COMMENT ON COLUMN stock_daily.change IS '涨跌额';
                COMMENT ON COLUMN stock_daily.pct_chg IS '涨跌幅 (未复权)';
                COMMENT ON COLUMN stock_daily.vol IS '成交量 (手)';
                COMMENT ON COLUMN stock_daily.amount IS '成交额 (千元)';
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
                COMMENT ON TABLE stock_adj_factor IS '股票复权因子表';
                COMMENT ON COLUMN stock_adj_factor.ts_code IS 'TS股票代码';
                COMMENT ON COLUMN stock_adj_factor.trade_date IS '交易日期';
                COMMENT ON COLUMN stock_adj_factor.adj_factor IS '复权因子';
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
                COMMENT ON TABLE stock_basic IS '股票基础信息表';
                COMMENT ON COLUMN stock_basic.ts_code IS 'TS代码';
                COMMENT ON COLUMN stock_basic.symbol IS '股票代码';
                COMMENT ON COLUMN stock_basic.name IS '股票名称';
                COMMENT ON COLUMN stock_basic.area IS '地域';
                COMMENT ON COLUMN stock_basic.industry IS '所属行业';
                COMMENT ON COLUMN stock_basic.fullname IS '股票全称';
                COMMENT ON COLUMN stock_basic.enname IS '英文全称';
                COMMENT ON COLUMN stock_basic.cnspell IS '拼音缩写';
                COMMENT ON COLUMN stock_basic.market IS '市场类型';
                COMMENT ON COLUMN stock_basic.exchange IS '交易所代码';
                COMMENT ON COLUMN stock_basic.curr IS '交易货币';
                COMMENT ON COLUMN stock_basic.list_status IS '上市状态 L上市 D退市 P暂停上市';
                COMMENT ON COLUMN stock_basic.list_date IS '上市日期';
                COMMENT ON COLUMN stock_basic.delist_date IS '退市日期';
                COMMENT ON COLUMN stock_basic.is_hs IS '是否沪深通标的 N否 H沪股通 S深股通';
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
                COMMENT ON TABLE stock_ths_index IS '同花顺板块指数表';
                COMMENT ON COLUMN stock_ths_index.ts_code IS '指数代码';
                COMMENT ON COLUMN stock_ths_index.name IS '指数名称';
                COMMENT ON COLUMN stock_ths_index.count IS '成分股数量';
                COMMENT ON COLUMN stock_ths_index.exchange IS '交易所';
                COMMENT ON COLUMN stock_ths_index.list_date IS '发布日期';
                COMMENT ON COLUMN stock_ths_index.type IS '指数类型 (N板块, I指数)';
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
                COMMENT ON TABLE stock_ths_member IS '同花顺板块成分表';
                COMMENT ON COLUMN stock_ths_member.ts_code IS '板块指数代码';
                COMMENT ON COLUMN stock_ths_member.con_code IS '成分股代码';
                COMMENT ON COLUMN stock_ths_member.con_name IS '成分股名称';
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
                COMMENT ON TABLE stock_moneyflow IS '股票个股资金流向表';
                COMMENT ON COLUMN stock_moneyflow.ts_code IS 'TS股票代码';
                COMMENT ON COLUMN stock_moneyflow.trade_date IS '交易日期';
                COMMENT ON COLUMN stock_moneyflow.buy_sm_vol IS '小单买入量 (手)';
                COMMENT ON COLUMN stock_moneyflow.buy_sm_amount IS '小单买入金额 (万元)';
                COMMENT ON COLUMN stock_moneyflow.sell_sm_vol IS '小单卖出量 (手)';
                COMMENT ON COLUMN stock_moneyflow.sell_sm_amount IS '小单卖出金额 (万元)';
                COMMENT ON COLUMN stock_moneyflow.buy_md_vol IS '中单买入量 (手)';
                COMMENT ON COLUMN stock_moneyflow.buy_md_amount IS '中单买入金额 (万元)';
                COMMENT ON COLUMN stock_moneyflow.sell_md_vol IS '中单卖出量 (手)';
                COMMENT ON COLUMN stock_moneyflow.sell_md_amount IS '中单卖出金额 (万元)';
                COMMENT ON COLUMN stock_moneyflow.buy_lg_vol IS '大单买入量 (手)';
                COMMENT ON COLUMN stock_moneyflow.buy_lg_amount IS '大单买入金额 (万元)';
                COMMENT ON COLUMN stock_moneyflow.sell_lg_vol IS '大单卖出量 (手)';
                COMMENT ON COLUMN stock_moneyflow.sell_lg_amount IS '大单卖出金额 (万元)';
                COMMENT ON COLUMN stock_moneyflow.buy_elg_vol IS '特大单买入量 (手)';
                COMMENT ON COLUMN stock_moneyflow.buy_elg_amount IS '特大单买入金额 (万元)';
                COMMENT ON COLUMN stock_moneyflow.sell_elg_vol IS '特大单卖出量 (手)';
                COMMENT ON COLUMN stock_moneyflow.sell_elg_amount IS '特大单卖出金额 (万元)';
                COMMENT ON COLUMN stock_moneyflow.net_mf_vol IS '净流入量 (手)';
                COMMENT ON COLUMN stock_moneyflow.net_mf_amount IS '净流入额 (万元)';
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
