from .celery_app import celery
from .db import save_result
from .data_fetcher import StockFetcher, IndexFetcher, SectorFetcher


@celery.task(name="tasks.add")
def add(x: int, y: int):
    res = x + y
    # 记录到外部 PostgreSQL
    save_result(celery_task_id=add.request.id, result=str(res))
    return res


@celery.task(name="tasks.sync_stock_list")
def sync_stock_list_task(market: str = "", list_status: str = "L"):
    """
    异步同步股票列表任务
    """
    fetcher = StockFetcher()
    df = fetcher.get_stock_list(market=market, list_status=list_status)
    
    if df is not None:
        count = len(df)
        # 这里可以将数据存入数据库，目前仅保存结果摘要
        result_msg = f"成功同步股票列表，共 {count} 只股票。"
        save_result(celery_task_id=sync_stock_list_task.request.id, result=result_msg)
        return {"status": "success", "count": count}
    
    save_result(celery_task_id=sync_stock_list_task.request.id, result="同步股票列表失败。")
    return {"status": "failed"}
