from datetime import datetime
from fastapi import FastAPI, HTTPException, Query
from scalar_fastapi import get_scalar_api_reference
from .settings import settings
from .db import init_db, fetch_result
from .data_fetcher import IndexFetcher, SectorFetcher, StockFetcher, MoneyflowFetcher
from .tasks import (
    add, 
    sync_stock_list_task, 
    sync_history_data_task, 
    sync_stock_data_by_day, 
    sync_ths_index_task, 
    sync_ths_member_task,
    sync_moneyflow_by_day_task,
    sync_moneyflow_history_task,
    check_and_fix_daily_data_task
)

app = FastAPI(title=settings.APP_NAME)


@app.get("/docs/scalar", include_in_schema=False)
async def scalar_html():
    return get_scalar_api_reference(
        openapi_url=app.openapi_url,
        title=app.title,
    )


@app.on_event("startup")
def _startup():
    init_db()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/tasks/add")
def create_add_task(x: int, y: int):
    task = add.delay(x, y)
    return {"celery_task_id": task.id, "state": task.state}


@app.get("/tasks/{task_id}")
def get_task_result(task_id: str):
    row = fetch_result(task_id)
    if not row:
        return {"celery_task_id": task_id, "db_result": None, "hint": "任务可能还没跑完，或还没写入数据库"}
    return row


@app.get("/stocks/list")
def get_stocks(
    ts_code: str = "",
    name: str = "",
    market: str = "",
    list_status: str = "L",
    exchange: str = "",
    is_hs: str = ""
):
    """
    直接获取全市场股票基础信息列表 (Tushare API)
    """
    fetcher = StockFetcher()
    df = fetcher.get_stock_list(
        ts_code=ts_code,
        name=name,
        market=market,
        list_status=list_status,
        exchange=exchange,
        is_hs=is_hs
    )
    if df is None:
        raise HTTPException(status_code=500, detail="获取股票列表失败")

    # 将数据保存到数据库 (stock_basic 表)
    fetcher.save_to_db(df, "stock_basic", if_exists="replace")

    # 将 DataFrame 转换为字典列表返回
    return df.to_dict(orient="records")


@app.post("/stocks/sync")
def sync_stocks(
    market: str = Query("", description="市场代码: 主板, 创业板, 科创板, 北交所"), 
    list_status: str = Query("L", description="上市状态: L上市, D退市, P暂停上市")
):
    """
    异步同步全市场股票基础信息列表
    """
    task = sync_stock_list_task.delay(market=market, list_status=list_status)
    return {"celery_task_id": task.id, "state": task.state, "detail": "同步任务已在后台启动"}


@app.post("/stocks/sync_history")
def sync_history(
    start_date: str = Query("20180101", description="开始日期 (YYYYMMDD)"), 
    end_date: str = Query(None, description="结束日期 (YYYYMMDD), 默认为昨天")
):
    """
    异步同步历史行情与复权因子 (按交易日历循环)
    """
    task = sync_history_data_task.delay(start_date=start_date, end_date=end_date)
    return {"celery_task_id": task.id, "state": task.state, "detail": "历史同步任务已启动，请通过 ID 查询进度"}


@app.post("/stocks/sync_daily")
def sync_daily(
    trade_date: str = Query(None, description="交易日期 (YYYYMMDD), 默认为当天")
):
    """
    异步同步指定日期的全市场行情数据
    """
    if not trade_date:
        trade_date = datetime.now().strftime("%Y%m%d")
    task = sync_stock_data_by_day.delay(trade_date=trade_date)
    return {"celery_task_id": task.id, "state": task.state, "detail": f"{trade_date} 同步任务已启动"}


@app.post("/stocks/check_data")
def check_stock_data(
    start_date: str = Query("20180101", description="开始日期 (YYYYMMDD)"), 
    end_date: str = Query(None, description="结束日期 (YYYYMMDD), 默认为今天")
):
    """
    检查本地 stock_daily 数据完整性，并自动修补缺失的交易日数据
    """
    task = check_and_fix_daily_data_task.delay(start_date=start_date, end_date=end_date)
    return {"celery_task_id": task.id, "state": task.state, "detail": "数据完整性检查任务已启动"}


@app.post("/stocks/moneyflow/sync")
def sync_moneyflow(
    trade_date: str = Query(None, description="交易日期 (YYYYMMDD), 默认为当天")
):
    """
    异步同步指定日期的全市场个股资金流向数据
    """
    if not trade_date:
        trade_date = datetime.now().strftime("%Y%m%d")
    task = sync_moneyflow_by_day_task.delay(trade_date=trade_date)
    return {"celery_task_id": task.id, "state": task.state, "detail": f"{trade_date} 资金流向同步任务已启动"}


@app.post("/stocks/moneyflow/sync_history")
def sync_moneyflow_history(
    start_date: str = Query("20180101", description="开始日期 (YYYYMMDD)"), 
    end_date: str = Query(None, description="结束日期 (YYYYMMDD), 默认为昨天")
):
    """
    异步同步历史个股资金流向 (按交易日历循环)
    """
    task = sync_moneyflow_history_task.delay(start_date=start_date, end_date=end_date)
    return {"celery_task_id": task.id, "state": task.state, "detail": "资金流向历史同步任务已启动"}


@app.post("/sectors/ths_index/sync")
def sync_ths_index(
    exchange: str = Query("A", description="交易所: A-中信/同花顺, SSE-上交所, SZSE-深交所"), 
    index_type: str = Query("", description="指数类型: N-板块, I-指数")
):
    """
    异步同步同花顺板块指数
    """
    task = sync_ths_index_task.delay(exchange=exchange, index_type=index_type)
    return {"celery_task_id": task.id, "state": task.state, "detail": "同花顺板块指数同步任务已启动"}


@app.post("/sectors/ths_member/sync")
def sync_ths_member(
    ts_code: str = Query(..., description="板块指数代码 (例如: 885757.TI)")
):
    """
    异步同步同花顺板块成分
    """
    task = sync_ths_member_task.delay(ts_code=ts_code)
    return {"celery_task_id": task.id, "state": task.state, "detail": f"板块 {ts_code} 成分同步任务已启动"}
