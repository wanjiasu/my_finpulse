import time
from datetime import datetime, timedelta
from .celery_app import celery
from .db import save_result
from .data_fetcher import StockFetcher, IndexFetcher, SectorFetcher


@celery.task(name="tasks.add")
def add(x: int, y: int):
    res = x + y
    # 记录到外部 PostgreSQL
    save_result(celery_task_id=add.request.id, result=str(res))
    return res


@celery.task(
    name="tasks.sync_stock_list",
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3},
    retry_backoff=60
)
def sync_stock_list_task(market: str = "", list_status: str = "L"):
    """
    异步同步股票列表任务
    """
    fetcher = StockFetcher()
    df = fetcher.get_stock_list(market=market, list_status=list_status)
    
    if df is not None:
        count = len(df)
        result_msg = f"成功同步股票列表，共 {count} 只股票。"
        save_result(celery_task_id=sync_stock_list_task.request.id, result=result_msg)
        return {"status": "success", "count": count}
    
    save_result(celery_task_id=sync_stock_list_task.request.id, result="同步股票列表失败。")
    return {"status": "failed"}


@celery.task(
    name="tasks.sync_stock_data_by_day",
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 5},
    retry_backoff=60  # 失败后等待 60s 重试
)
def sync_stock_data_by_day(trade_date: str):
    """
    同步某一天的行情和复权因子
    """
    fetcher = StockFetcher()
    
    print(f"开始同步 {trade_date} 的全市场数据...")
    
    # 1. 获取行情
    daily_df = fetcher.get_stock_daily(trade_date=trade_date)
    daily_success = fetcher.save_to_db(daily_df, "stock_daily")
    
    # 2. 获取复权因子
    adj_df = fetcher.get_adj_factor(trade_date=trade_date)
    adj_success = fetcher.save_to_db(adj_df, "stock_adj_factor")
    
    result_msg = f"日期 {trade_date}: 行情入库{'成功' if daily_success else '失败'}, 复权因子入库{'成功' if adj_success else '失败'}"
    return {"date": trade_date, "msg": result_msg}


@celery.task(name="tasks.sync_history_data")
def sync_history_data_task(start_date: str = "20180101", end_date: str = None):
    """
    按天循环同步历史行情与复权因子
    """
    fetcher = StockFetcher()
    
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
    # 1. 获取交易日历
    cal_df = fetcher.get_trade_cal(start_date=start_date, end_date=end_date)
    if cal_df is None or cal_df.empty:
        save_result(celery_task_id=sync_history_data_task.request.id, result="获取交易日历失败。")
        return {"status": "failed", "reason": "no trade calendar"}
    
    trade_days = cal_df['cal_date'].tolist()
    total_days = len(trade_days)
    
    save_result(celery_task_id=sync_history_data_task.request.id, result=f"历史同步启动: {start_date} -> {end_date}, 共 {total_days} 个交易日。")
    
    success_count = 0
    for i, day in enumerate(trade_days):
        try:
            # 调用单日同步逻辑
            res = sync_stock_data_by_day(day)
            success_count += 1
            
            # 每隔 10 天记录一次进度
            if i % 10 == 0:
                progress_msg = f"进度: {i}/{total_days}, 当前日期: {day}, 已成功同步 {success_count} 天。"
                save_result(celery_task_id=sync_history_data_task.request.id, result=progress_msg)
            
            # 频率控制，避免触发 Tushare 限制
            time.sleep(1) 
            
        except Exception as e:
            print(f"同步日期 {day} 时发生错误: {e}")
            
    final_msg = f"历史同步完成! 共处理 {total_days} 个交易日，成功 {success_count} 天。"
    save_result(celery_task_id=sync_history_data_task.request.id, result=final_msg)
    return {"status": "completed", "total": total_days, "success": success_count}
