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
                    PRIMARY KEY (ts_code, trade_date)
                );
                ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS open FLOAT;
                ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS high FLOAT;
                ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS low FLOAT;
                ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS close FLOAT;
                ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS pre_close FLOAT;
                ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS change FLOAT;
                ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS pct_chg FLOAT;
                ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS vol FLOAT;
                ALTER TABLE stock_daily ADD COLUMN IF NOT EXISTS amount FLOAT;

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
                    PRIMARY KEY (ts_code, trade_date)
                );
                ALTER TABLE stock_adj_factor ADD COLUMN IF NOT EXISTS adj_factor FLOAT;

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
                    ts_code TEXT PRIMARY KEY
                );
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS symbol TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS name TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS area TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS industry TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS fullname TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS enname TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS cnspell TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS market TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS exchange TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS curr TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS list_status TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS list_date TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS delist_date TEXT;
                ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS is_hs TEXT;

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
                    ts_code TEXT PRIMARY KEY
                );
                ALTER TABLE stock_ths_index ADD COLUMN IF NOT EXISTS name TEXT;
                ALTER TABLE stock_ths_index ADD COLUMN IF NOT EXISTS count INTEGER;
                ALTER TABLE stock_ths_index ADD COLUMN IF NOT EXISTS exchange TEXT;
                ALTER TABLE stock_ths_index ADD COLUMN IF NOT EXISTS list_date TEXT;
                ALTER TABLE stock_ths_index ADD COLUMN IF NOT EXISTS type TEXT;

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
                    PRIMARY KEY (ts_code, con_code)
                );
                ALTER TABLE stock_ths_member ADD COLUMN IF NOT EXISTS con_name TEXT;

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
                    PRIMARY KEY (ts_code, trade_date)
                );
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS buy_sm_vol BIGINT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS buy_sm_amount FLOAT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS sell_sm_vol BIGINT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS sell_sm_amount FLOAT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS buy_md_vol BIGINT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS buy_md_amount FLOAT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS sell_md_vol BIGINT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS sell_md_amount FLOAT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS buy_lg_vol BIGINT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS buy_lg_amount FLOAT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS sell_lg_vol BIGINT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS sell_lg_amount FLOAT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS buy_elg_vol BIGINT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS buy_elg_amount FLOAT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS sell_elg_vol BIGINT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS sell_elg_amount FLOAT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS net_mf_vol BIGINT;
                ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS net_mf_amount FLOAT;

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
            # 指数日线行情表
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS index_daily (
                    ts_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    PRIMARY KEY (ts_code, trade_date)
                );
                ALTER TABLE index_daily ADD COLUMN IF NOT EXISTS close FLOAT;
                ALTER TABLE index_daily ADD COLUMN IF NOT EXISTS open FLOAT;
                ALTER TABLE index_daily ADD COLUMN IF NOT EXISTS high FLOAT;
                ALTER TABLE index_daily ADD COLUMN IF NOT EXISTS low FLOAT;
                ALTER TABLE index_daily ADD COLUMN IF NOT EXISTS pre_close FLOAT;
                ALTER TABLE index_daily ADD COLUMN IF NOT EXISTS change FLOAT;
                ALTER TABLE index_daily ADD COLUMN IF NOT EXISTS pct_chg FLOAT;
                ALTER TABLE index_daily ADD COLUMN IF NOT EXISTS vol FLOAT;
                ALTER TABLE index_daily ADD COLUMN IF NOT EXISTS amount FLOAT;

                CREATE INDEX IF NOT EXISTS idx_index_daily_date ON index_daily (trade_date);
                COMMENT ON TABLE index_daily IS '指数日线行情表';
                COMMENT ON COLUMN index_daily.ts_code IS 'TS指数代码';
                COMMENT ON COLUMN index_daily.trade_date IS '交易日期';
                COMMENT ON COLUMN index_daily.close IS '收盘点位';
                COMMENT ON COLUMN index_daily.open IS '开盘点位';
                COMMENT ON COLUMN index_daily.high IS '最高点位';
                COMMENT ON COLUMN index_daily.low IS '最低点位';
                COMMENT ON COLUMN index_daily.pre_close IS '昨日收盘点';
                COMMENT ON COLUMN index_daily.change IS '涨跌点';
                COMMENT ON COLUMN index_daily.pct_chg IS '涨跌幅';
                COMMENT ON COLUMN index_daily.vol IS '成交量 (手)';
                COMMENT ON COLUMN index_daily.amount IS '成交额 (千元)';
                """
            )
            # 交易日历表
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_calendar (
                    exchange TEXT NOT NULL,
                    cal_date TEXT NOT NULL,
                    PRIMARY KEY (exchange, cal_date)
                );
                ALTER TABLE trade_calendar ADD COLUMN IF NOT EXISTS is_open INTEGER;
                ALTER TABLE trade_calendar ADD COLUMN IF NOT EXISTS pretrade_date TEXT;

                CREATE INDEX IF NOT EXISTS idx_trade_cal_date ON trade_calendar (cal_date);
                COMMENT ON TABLE trade_calendar IS '交易日历表';
                COMMENT ON COLUMN trade_calendar.exchange IS '交易所 SSE上交所 SZSE深交所';
                COMMENT ON COLUMN trade_calendar.cal_date IS '日历日期';
                COMMENT ON COLUMN trade_calendar.is_open IS '是否交易 0休市 1交易';
                COMMENT ON COLUMN trade_calendar.pretrade_date IS '上一个交易日';
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
