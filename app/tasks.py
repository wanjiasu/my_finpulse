import time
from datetime import datetime, timedelta
from .celery_app import celery
from .db import save_result, init_db
from .data_fetcher import StockFetcher, IndexFetcher, SectorFetcher

# 确保数据库表已创建
init_db()


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
        # 将数据保存到数据库 (stock_basic 表)
        success = fetcher.save_to_db(df, "stock_basic", if_exists="replace")
        
        if success:
            result_msg = f"成功同步股票列表并入库，共 {count} 只股票。"
        else:
            result_msg = f"成功同步股票列表，但入库失败。共 {count} 只股票。"
            
        save_result(celery_task_id=sync_stock_list_task.request.id, result=result_msg)
        return {"status": "success" if success else "partial_success", "count": count}
    
    save_result(celery_task_id=sync_stock_list_task.request.id, result="同步股票列表失败。")
    return {"status": "failed"}


@celery.task(
    name="tasks.sync_ths_index",
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3},
    retry_backoff=60
)
def sync_ths_index_task(exchange: str = "A", index_type: str = ""):
    """
    异步同步同花顺板块指数任务
    """
    fetcher = SectorFetcher()
    df = fetcher.get_ths_index(exchange=exchange, index_type=index_type)
    
    if df is not None:
        count = len(df)
        # 将数据保存到数据库，如果已存在则替换
        success = fetcher.save_to_db(df, "stock_ths_index", if_exists="replace")
        
        if success:
            result_msg = f"成功同步同花顺板块指数并入库，共 {count} 条记录。"
        else:
            result_msg = f"成功同步同花顺板块指数，但入库失败。共 {count} 条记录。"
            
        save_result(celery_task_id=sync_ths_index_task.request.id, result=result_msg)
        return {"status": "success" if success else "partial_success", "count": count}
    
    save_result(celery_task_id=sync_ths_index_task.request.id, result="同步同花顺板块指数失败。")
    return {"status": "failed"}


@celery.task(
    name="tasks.sync_ths_member",
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3},
    retry_backoff=60
)
def sync_ths_member_task(ts_code: str):
    """
    异步同步同花顺板块成分任务
    """
    fetcher = SectorFetcher()
    df = fetcher.get_ths_member(ts_code=ts_code)
    
    if df is not None:
        count = len(df)
        # 将数据保存到数据库，采用 append 模式，但由于有复合主键，建议实际中使用 upsert 或先删后插
        # 这里简单起见先 append，数据库主键会保证唯一性（如果冲突会报错，被 Celery 重试捕获）
        # 更好的做法是 if_exists="append" 配合手动处理冲突，或者先删除该板块旧成分
        from .db import engine
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM stock_ths_member WHERE ts_code = '{ts_code}'"))
            conn.commit()
            
        success = fetcher.save_to_db(df[['ts_code', 'con_code', 'con_name']], "stock_ths_member", if_exists="append")
        
        if success:
            result_msg = f"成功同步板块 {ts_code} 成分并入库，共 {count} 条记录。"
        else:
            result_msg = f"成功同步板块 {ts_code} 成分，但入库失败。"
            
        save_result(celery_task_id=sync_ths_member_task.request.id, result=result_msg)
        return {"status": "success" if success else "failed", "ts_code": ts_code, "count": count}
    
    save_result(celery_task_id=sync_ths_member_task.request.id, result=f"同步板块 {ts_code} 成分失败。")
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
    from .db import engine
    from sqlalchemy import text
    
    print(f"开始同步 {trade_date} 的全市场数据...")
    
    # 1. 获取行情
    daily_df = fetcher.get_stock_daily(trade_date=trade_date)
    if daily_df is not None and not daily_df.empty:
        # 先删除已存在的同日期数据，防止主键冲突
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM stock_daily WHERE trade_date = '{trade_date}'"))
            conn.commit()
        daily_success = fetcher.save_to_db(daily_df, "stock_daily", if_exists="append")
    else:
        daily_success = False
    
    # 2. 获取复权因子
    adj_df = fetcher.get_adj_factor(trade_date=trade_date)
    if adj_df is not None and not adj_df.empty:
        # 先删除已存在的同日期数据
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM stock_adj_factor WHERE trade_date = '{trade_date}'"))
            conn.commit()
        adj_success = fetcher.save_to_db(adj_df, "stock_adj_factor", if_exists="append")
    else:
        adj_success = False
    
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
