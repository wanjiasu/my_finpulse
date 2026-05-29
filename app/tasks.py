import time
from datetime import datetime, timedelta
from .celery_app import celery
from .db import save_result, init_db
from .data_fetcher import StockFetcher, IndexFetcher, SectorFetcher, MoneyflowFetcher, FinancialFetcher

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
            result_msg = f"同步股票列表入库失败。"
            save_result(celery_task_id=sync_stock_list_task.request.id, result=result_msg)
            raise RuntimeError(result_msg)
            
        save_result(celery_task_id=sync_stock_list_task.request.id, result=result_msg)
        return {"status": "success", "count": count}
    
    result_msg = "同步股票列表失败: API 返回为空。"
    save_result(celery_task_id=sync_stock_list_task.request.id, result=result_msg)
    raise RuntimeError(result_msg)


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
            result_msg = f"同步同花顺板块指数入库失败。"
            save_result(celery_task_id=sync_ths_index_task.request.id, result=result_msg)
            raise RuntimeError(result_msg)
            
        save_result(celery_task_id=sync_ths_index_task.request.id, result=result_msg)
        return {"status": "success", "count": count}
    
    result_msg = "同步同花顺板块指数失败: API 返回为空。"
    save_result(celery_task_id=sync_ths_index_task.request.id, result=result_msg)
    raise RuntimeError(result_msg)


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
            save_result(celery_task_id=sync_ths_member_task.request.id, result=result_msg)
            return {"status": "success", "ts_code": ts_code, "count": count}
        else:
            result_msg = f"同步板块 {ts_code} 成分入库失败。"
            save_result(celery_task_id=sync_ths_member_task.request.id, result=result_msg)
            raise RuntimeError(result_msg)
    
    result_msg = f"同步板块 {ts_code} 成分失败: API 返回为空。"
    save_result(celery_task_id=sync_ths_member_task.request.id, result=result_msg)
    raise RuntimeError(result_msg)


@celery.task(name="tasks.sync_daily_automatic")
def sync_daily_automatic():
    """
    每日定时执行的任务：同步股票列表和最新一天行情
    """
    today = datetime.now().strftime("%Y%m%d")
    print(f"开始执行每日自动同步任务，日期: {today}")
    
    # 1. 同步股票列表
    sync_stock_list_task.delay()
    
    # 2. 延迟几秒再启动行情同步，避免同时请求 Tushare 触发频率限制
    import time
    time.sleep(5)
    
    # 3. 同步当日行情
    sync_stock_data_by_day.delay(trade_date=today)
    
    return {"status": "triggered", "date": today}


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
        success = fetcher.save_to_db(daily_df, "stock_daily", if_exists="append")
        if not success:
            raise RuntimeError(f"日期 {trade_date}: 日线行情入库失败")
    else:
        # 如果是交易日但没拿数据，抛出异常让 Celery 重试
        raise RuntimeError(f"未能获取到 {trade_date} 的日线行情数据，可能接口限流或数据未更新")
    
    # 2. 获取复权因子
    adj_df = fetcher.get_adj_factor(trade_date=trade_date)
    if adj_df is not None and not adj_df.empty:
        # 先删除已存在的同日期数据
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM stock_adj_factor WHERE trade_date = '{trade_date}'"))
            conn.commit()
        success = fetcher.save_to_db(adj_df, "stock_adj_factor", if_exists="append")
        if not success:
            raise RuntimeError(f"日期 {trade_date}: 复权因子入库失败")
    else:
        # 复权因子缺失也抛出异常
        raise RuntimeError(f"未能获取到 {trade_date} 的复权因子数据")
    
    result_msg = f"日期 {trade_date}: 行情入库成功, 复权因子入库成功"
    return {"date": trade_date, "msg": result_msg}


@celery.task(
    name="tasks.sync_moneyflow_by_day",
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 5},
    retry_backoff=60
)
def sync_moneyflow_by_day_task(trade_date: str):
    """
    同步某一天的股票资金流向
    """
    fetcher = MoneyflowFetcher()
    from .db import engine
    from sqlalchemy import text
    
    print(f"开始同步 {trade_date} 的全市场资金流向数据...")
    
    df = fetcher.get_moneyflow(trade_date=trade_date)
    if df is not None and not df.empty:
        # 先删除已存在的同日期数据
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM stock_moneyflow WHERE trade_date = '{trade_date}'"))
            conn.commit()
        success = fetcher.save_to_db(df, "stock_moneyflow", if_exists="append")
        if success:
            result_msg = f"日期 {trade_date}: 资金流向入库成功，共 {len(df)} 条记录"
            return {"date": trade_date, "status": "success", "msg": result_msg}
        else:
            raise RuntimeError(f"日期 {trade_date}: 资金流向入库失败")
    else:
        # 如果是交易日但没拿数据，抛出异常让 Celery 重试
        raise RuntimeError(f"未能获取到 {trade_date} 的资金流向数据，可能接口限流或数据未更新")


@celery.task(
    name="tasks.sync_moneyflow_hsgt",
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3},
    retry_backoff=60
)
def sync_moneyflow_hsgt_task(trade_date: str = "", start_date: str = "", end_date: str = ""):
    """
    同步沪深港通资金流向任务
    """
    fetcher = MoneyflowFetcher()
    from .db import engine
    from sqlalchemy import text
    
    df = fetcher.get_moneyflow_hsgt(trade_date=trade_date, start_date=start_date, end_date=end_date)
    
    if df is not None and not df.empty:
        count = len(df)
        with engine.begin() as conn:
            # 批量 Upsert：先删除
            dates = df['trade_date'].unique().tolist()
            conn.execute(text("DELETE FROM stock_moneyflow_hsgt WHERE trade_date = ANY(:ds)"), {"ds": dates})
            # 插入
            df.to_sql("stock_moneyflow_hsgt", conn, if_exists="append", index=False)
            
        result_msg = f"成功同步沪深港通资金流向并入库，共 {count} 条记录。"
        save_result(celery_task_id=sync_moneyflow_hsgt_task.request.id, result=result_msg)
        return {"status": "success", "count": count}
    
    # 如果没获取到数据且是单日任务，抛出异常重试
    if trade_date or (start_date and start_date == end_date):
        raise RuntimeError(f"未能获取到沪深港通资金流向数据: {trade_date or start_date}")
    
    return {"status": "no_data", "msg": "API 返回为空"}


@celery.task(name="tasks.sync_moneyflow_history")
def sync_moneyflow_history_task(start_date: str = "20180101", end_date: str = None):
    """
    按天循环同步历史资金流向
    """
    fetcher = StockFetcher() # 使用 StockFetcher 获取交易日历
    
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
    cal_df = fetcher.get_trade_cal(start_date=start_date, end_date=end_date, is_open=1)
    if cal_df is None or cal_df.empty:
        error_msg = f"资金流向历史同步失败: 获取交易日历失败 ({start_date} -> {end_date})。"
        save_result(celery_task_id=sync_moneyflow_history_task.request.id, result=error_msg)
        raise RuntimeError(error_msg)
    
    trade_days = cal_df['cal_date'].tolist()
    total_days = len(trade_days)
    
    save_result(celery_task_id=sync_moneyflow_history_task.request.id, result=f"资金流向历史同步启动: {start_date} -> {end_date}, 共 {total_days} 个交易日。")
    
    success_count = 0
    for i, td in enumerate(trade_days):
        try:
            # 直接调用单日同步函数（同步执行），以便控制频率
            res = sync_moneyflow_by_day_task(td)
            if res.get("status") == "success":
                success_count += 1
            
            if i % 10 == 0:
                progress_msg = f"资金流向进度: {i}/{total_days}, 当前日期: {td}, 已成功同步 {success_count} 天。"
                save_result(celery_task_id=sync_moneyflow_history_task.request.id, result=progress_msg)
            
            time.sleep(1) # 频率控制
        except Exception as e:
            print(f"同步资金流向日期 {td} 时发生错误: {e}")
            
    final_msg = f"资金流向历史同步完成! 共处理 {total_days} 个交易日，成功 {success_count} 天。"
    save_result(celery_task_id=sync_moneyflow_history_task.request.id, result=final_msg)
    return {"status": "completed", "total": total_days, "success": success_count}


@celery.task(
    name="tasks.sync_financial_data",
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3},
    retry_backoff=60
)
def sync_financial_data_task(ts_code: str):
    """
    同步单个股票的财务报表数据（利润表、资产负债表、现金流量表）
    """
    fetcher = FinancialFetcher()
    from .db import engine
    from sqlalchemy import text
    
    # 定义每张表需要的字段，防止 DataFrame 中多余字段导致入库失败
    income_cols = [
        'ts_code', 'ann_date', 'f_ann_date', 'end_date', 'report_type', 'comp_type',
        'basic_eps', 'diluted_eps', 'total_revenue', 'revenue', 'oper_cost',
        'total_cogs', 'operate_profit', 'total_profit', 'income_tax', 'n_income',
        'n_income_attr_p', 'ebit', 'ebitda'
    ]
    
    bs_cols = [
        'ts_code', 'ann_date', 'f_ann_date', 'end_date', 'report_type', 'comp_type',
        'total_share', 'cap_rese', 'undistr_profit', 'surplus_rese', 'money_cap',
        'trad_asset', 'notes_receiv', 'accounts_receiv', 'inventories',
        'total_cur_assets', 'total_assets', 'total_cur_liab', 'total_liab'
    ]
    
    cf_cols = [
        'ts_code', 'ann_date', 'f_ann_date', 'end_date', 'report_type', 'comp_type',
        'net_profit', 'n_cashflow_act', 'n_cashflow_inv_act', 'n_cashflow_fnc_act',
        'n_incr_cash_cash_equ'
    ]

    # 1. 同步利润表
    income_df = fetcher.get_income(ts_code=ts_code)
    if income_df is not None and not income_df.empty:
        # 去重：基于主键去重，保留公告日期最新的记录
        income_df = income_df.sort_values('ann_date', ascending=False).drop_duplicates(
            subset=['ts_code', 'end_date', 'report_type'], keep='first'
        )
        
        # 仅保留数据库中存在的列
        valid_income_df = income_df[[c for c in income_cols if c in income_df.columns]]
        with engine.begin() as conn:
            # 全量清理该股票的利润表数据，确保增量/更新逻辑正确
            conn.execute(text("DELETE FROM fina_income WHERE ts_code = :ts"), {"ts": ts_code})
            # 事务内入库
            valid_income_df.to_sql("fina_income", conn, if_exists="append", index=False)

    # 2. 同步资产负债表
    bs_df = fetcher.get_balancesheet(ts_code=ts_code)
    if bs_df is not None and not bs_df.empty:
        # 去重
        bs_df = bs_df.sort_values('ann_date', ascending=False).drop_duplicates(
            subset=['ts_code', 'end_date', 'report_type'], keep='first'
        )
        
        # 修正可能存在的字段名差异
        if 'undistr_profit' not in bs_df.columns and 'undistr_porfit' in bs_df.columns:
            bs_df = bs_df.rename(columns={'undistr_porfit': 'undistr_profit'})
            
        valid_bs_df = bs_df[[c for c in bs_cols if c in bs_df.columns]]
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM fina_balancesheet WHERE ts_code = :ts"), {"ts": ts_code})
            valid_bs_df.to_sql("fina_balancesheet", conn, if_exists="append", index=False)

    # 3. 同步现金流量表
    cf_df = fetcher.get_cashflow(ts_code=ts_code)
    if cf_df is not None and not cf_df.empty:
        # 去重
        cf_df = cf_df.sort_values('ann_date', ascending=False).drop_duplicates(
            subset=['ts_code', 'end_date', 'report_type'], keep='first'
        )
        
        valid_cf_df = cf_df[[c for c in cf_cols if c in cf_df.columns]]
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM fina_cashflow WHERE ts_code = :ts"), {"ts": ts_code})
            valid_cf_df.to_sql("fina_cashflow", conn, if_exists="append", index=False)

    result_msg = f"股票 {ts_code} 财务报表同步完成。"
    save_result(celery_task_id=sync_financial_data_task.request.id, result=result_msg)
    return {"status": "success", "ts_code": ts_code}


@celery.task(name="tasks.sync_history_data")
def sync_history_data_task(start_date: str = "20180101", end_date: str = None):
    """
    按天循环同步历史行情与复权因子
    """
    fetcher = StockFetcher()
    
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
    # 1. 获取交易日历
    cal_df = fetcher.get_trade_cal(start_date=start_date, end_date=end_date, is_open=1)
    if cal_df is None or cal_df.empty:
        error_msg = f"历史行情同步失败: 获取交易日历失败 ({start_date} -> {end_date})。"
        save_result(celery_task_id=sync_history_data_task.request.id, result=error_msg)
        raise RuntimeError(error_msg)
    
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


@celery.task(
    name="tasks.sync_index_daily",
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3},
    retry_backoff=60
)
def sync_index_daily_task(ts_code: str = "000001.SH", start_date: str = "", end_date: str = "", limit: int = None):
    """
    异步同步指数日线行情任务
    """
    fetcher = IndexFetcher()
    from .db import engine
    from sqlalchemy import text
    
    # 获取指数行情数据
    df = fetcher.get_index_daily(ts_codes=[ts_code], start_date=start_date, end_date=end_date, limit=limit)
    
    if df is not None and not df.empty:
        count = len(df)
        # 按照 ts_code 和 trade_date 进行去重保存
        # 这里为了简单起见，先删除后插入，或者利用 PRIMARY KEY 约束
        with engine.connect() as conn:
            # 简单起见，这里直接调用 save_to_db，如果存在主键冲突会报错并触发重试
            # 如果要更优雅，可以分批处理或使用 UPSERT
            success = fetcher.save_to_db(df, "index_daily", if_exists="append")
            
        if success:
            result_msg = f"成功同步指数日线行情并入库，共 {count} 条记录。"
            save_result(celery_task_id=sync_index_daily_task.request.id, result=result_msg)
            return {"status": "success", "count": count}
        else:
            result_msg = "同步指数日线行情入库失败。"
            save_result(celery_task_id=sync_index_daily_task.request.id, result=result_msg)
            raise RuntimeError(result_msg)
    
    result_msg = "同步指数日线行情失败: API 返回为空。"
    save_result(celery_task_id=sync_index_daily_task.request.id, result=result_msg)
    raise RuntimeError(result_msg)


@celery.task(
    name="tasks.sync_trade_calendar",
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3},
    retry_backoff=60
)
def sync_trade_calendar_task(exchange: str = "SSE"):
    """
    异步增量同步交易日历任务
    """
    fetcher = StockFetcher()
    from .db import engine
    from sqlalchemy import text
    import pandas as pd

    # 1. 获取本地数据库中已有的最大日期
    start_date = "20180101"
    try:
        with engine.connect() as conn:
            query = text("SELECT MAX(cal_date) FROM trade_calendar WHERE exchange = :exchange")
            res = conn.execute(query, {"exchange": exchange}).scalar()
            if res:
                # 如果已有数据，则从最大日期的下一天开始同步
                start_dt = datetime.strptime(res, "%Y%m%d") + timedelta(days=1)
                start_date = start_dt.strftime("%Y%m%d")
    except Exception as e:
        print(f"查询本地交易日历最大日期失败: {e}")

    today = datetime.now().strftime("%Y%m%d")
    
    # 如果起始日期大于今天，说明已经是最新的了
    if start_date > today:
        result_msg = f"交易所 {exchange} 的交易日历已是最新 (截至 {today})。"
        save_result(celery_task_id=sync_trade_calendar_task.request.id, result=result_msg)
        return {"status": "success", "msg": result_msg}

    print(f"开始同步 {exchange} 交易日历: {start_date} -> {today}")

    # 2. 从 Tushare 获取交易日历 (不限 is_open，获取全部日历)
    df = fetcher.get_trade_cal(exchange=exchange, start_date=start_date, end_date=today, is_open=None)
    
    if df is not None and not df.empty:
        count = len(df)
        success = fetcher.save_to_db(df, "trade_calendar", if_exists="append")
        
        if success:
            result_msg = f"成功同步交易所 {exchange} 交易日历并入库，共 {count} 条记录 ({start_date} -> {today})。"
            save_result(celery_task_id=sync_trade_calendar_task.request.id, result=result_msg)
            return {"status": "success", "count": count}
        else:
            result_msg = f"同步交易所 {exchange} 交易日历入库失败。"
            save_result(celery_task_id=sync_trade_calendar_task.request.id, result=result_msg)
            raise RuntimeError(result_msg)
    
    result_msg = f"同步交易所 {exchange} 交易日历失败: API 返回为空。"
    save_result(celery_task_id=sync_trade_calendar_task.request.id, result=result_msg)
    # 如果没有新数据返回，可能确实没有新日期，不一定抛错
    return {"status": "no_data", "msg": result_msg}


@celery.task(
    name="tasks.check_and_fix_daily_data",
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3},
    retry_backoff=60
)
def check_and_fix_daily_data_task(start_date: str = "20180101", end_date: str = None):
    """
    检查数据质量：对比本地 stock_daily 表与 Tushare 交易日历，修补缺失日期的数据
    """
    fetcher = StockFetcher()
    from .db import engine
    import pandas as pd
    from sqlalchemy import text

    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")

    # 1. 获取 Tushare 交易日历中的所有交易日
    cal_df = fetcher.get_trade_cal(start_date=start_date, end_date=end_date, is_open=1)
    if cal_df is None or cal_df.empty:
        error_msg = f"数据检查失败: 无法获取交易日历 ({start_date} -> {end_date})"
        save_result(celery_task_id=check_and_fix_daily_data_task.request.id, result=error_msg)
        raise RuntimeError(error_msg)
    
    tushare_days = set(cal_df['cal_date'].tolist())

    # 2. 获取本地数据库中已有的交易日
    with engine.connect() as conn:
        # 检查行情数据表
        query_daily = text("SELECT DISTINCT trade_date FROM stock_daily WHERE trade_date >= :start AND trade_date <= :end")
        local_daily_df = pd.read_sql(query_daily, conn, params={"start": start_date, "end": end_date})
        local_daily_days = set(local_daily_df['trade_date'].tolist())

        # 检查复权因子表
        query_adj = text("SELECT DISTINCT trade_date FROM stock_adj_factor WHERE trade_date >= :start AND trade_date <= :end")
        local_adj_df = pd.read_sql(query_adj, conn, params={"start": start_date, "end": end_date})
        local_adj_days = set(local_adj_df['trade_date'].tolist())

    # 3. 计算缺失的日期 (行情缺失 或 复权因子缺失 的并集)
    missing_daily = tushare_days - local_daily_days
    missing_adj = tushare_days - local_adj_days
    missing_days = sorted(list(missing_daily | missing_adj))
    total_missing = len(missing_days)

    if total_missing == 0:
        result_msg = f"数据质量检查完成: {start_date} -> {end_date} 期间行情与复权因子数据完整。"
        save_result(celery_task_id=check_and_fix_daily_data_task.request.id, result=result_msg)
        return {"status": "success", "missing_count": 0, "msg": result_msg}

    # 4. 触发缺失日期的同步任务
    result_msg = f"发现 {total_missing} 个缺失交易日，已启动异步修补任务。"
    save_result(celery_task_id=check_and_fix_daily_data_task.request.id, result=result_msg)

    for day in missing_days:
        sync_stock_data_by_day.delay(trade_date=day)

    return {
        "status": "fixing",
        "missing_count": total_missing,
        "missing_days": missing_days,
        "msg": result_msg
    }


@celery.task(
    name="tasks.check_and_fix_moneyflow_data",
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3},
    retry_backoff=60
)
def check_and_fix_moneyflow_data_task(start_date: str = "20180101", end_date: str = None):
    """
    检查数据质量：对比本地 stock_moneyflow 表与 Tushare 交易日历，修补缺失日期的数据
    """
    fetcher = StockFetcher()
    from .db import engine
    import pandas as pd
    from sqlalchemy import text

    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")

    # 1. 获取 Tushare 交易日历中的所有交易日
    cal_df = fetcher.get_trade_cal(start_date=start_date, end_date=end_date, is_open=1)
    if cal_df is None or cal_df.empty:
        error_msg = f"资金流数据检查失败: 无法获取交易日历 ({start_date} -> {end_date})"
        save_result(celery_task_id=check_and_fix_moneyflow_data_task.request.id, result=error_msg)
        raise RuntimeError(error_msg)
    
    tushare_days = set(cal_df['cal_date'].tolist())

    # 2. 获取本地数据库中已有的交易日
    with engine.connect() as conn:
        query = text("SELECT DISTINCT trade_date FROM stock_moneyflow WHERE trade_date >= :start AND trade_date <= :end")
        local_days_df = pd.read_sql(query, conn, params={"start": start_date, "end": end_date})
        local_days = set(local_days_df['trade_date'].tolist())

    # 3. 计算缺失的日期
    missing_days = sorted(list(tushare_days - local_days))
    total_missing = len(missing_days)

    if total_missing == 0:
        result_msg = f"数据质量检查完成: {start_date} -> {end_date} 期间资金流数据完整。"
        save_result(celery_task_id=check_and_fix_moneyflow_data_task.request.id, result=result_msg)
        return {"status": "success", "missing_count": 0, "msg": result_msg}

    # 4. 触发缺失日期的同步任务
    result_msg = f"发现 {total_missing} 个缺失的资金流交易日，已启动异步修补任务。"
    save_result(celery_task_id=check_and_fix_moneyflow_data_task.request.id, result=result_msg)

    for day in missing_days:
        sync_moneyflow_by_day_task.delay(trade_date=day)

    return {
        "status": "fixing",
        "missing_count": total_missing,
        "missing_days": missing_days,
        "msg": result_msg
    }
