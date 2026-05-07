from datetime import datetime
from fastapi import FastAPI, HTTPException
from .settings import settings
from .db import init_db, fetch_result
from .data_fetcher import IndexFetcher, SectorFetcher, StockFetcher
from .tasks import add, sync_stock_list_task, sync_history_data_task, sync_stock_data_by_day, sync_ths_index_task

app = FastAPI(title=settings.APP_NAME)


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


@app.post("/stocks/sync")
def sync_stocks(market: str = "", list_status: str = "L"):
    """
    异步同步股票列表任务
    """
    task = sync_stock_list_task.delay(market=market, list_status=list_status)
    return {"celery_task_id": task.id, "state": task.state, "detail": "同步任务已在后台启动"}


@app.post("/stocks/sync_history")
def sync_history(start_date: str = "20180101", end_date: str = None):
    """
    异步同步历史行情与复权因子 (按天循环)
    """
    task = sync_history_data_task.delay(start_date=start_date, end_date=end_date)
    return {"celery_task_id": task.id, "state": task.state, "detail": "历史同步任务已启动，请通过 ID 查询进度"}


@app.post("/stocks/sync_daily")
def sync_daily(trade_date: str = None):
    """
    异步同步指定日期的全市场数据
    """
    if not trade_date:
        trade_date = datetime.now().strftime("%Y%m%d")
    task = sync_stock_data_by_day.delay(trade_date=trade_date)
    return {"celery_task_id": task.id, "state": task.state, "detail": f"{trade_date} 同步任务已启动"}


@app.post("/sectors/ths_index/sync")
def sync_ths_index(exchange: str = "A", index_type: str = ""):
    """
    异步同步同花顺板块指数
    """
    task = sync_ths_index_task.delay(exchange=exchange, index_type=index_type)
    return {"celery_task_id": task.id, "state": task.state, "detail": "同花顺板块指数同步任务已启动"}
