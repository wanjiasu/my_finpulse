from fastapi import FastAPI, HTTPException
from .settings import settings
from .db import init_db, fetch_result
from .data_fetcher import IndexFetcher, SectorFetcher, StockFetcher
from .tasks import add, sync_stock_list_task

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
