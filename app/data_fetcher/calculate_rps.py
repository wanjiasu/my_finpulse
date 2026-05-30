import pandas as pd
from sqlalchemy import text
from ..db import engine
import datetime
from typing import List

class RPSCalculator:
    """
    RPS (Relative Price Strength) 计算器
    """
    def __init__(self):
        self.engine = engine

    def fetch_data(self, target_date: str, lookback_days: int = 400):
        """
        获取计算 RPS 所需的基础数据
        """
        print(f"正在获取 {target_date} 之前约 {lookback_days} 个交易日的行情数据...")
        
        # 1. 获取当前上市股票基础信息
        query_basic = """
            SELECT ts_code, name, market, industry 
            FROM stock_basic 
        """
        df_basic = pd.read_sql(query_basic, self.engine)
        print(f"获取到 {len(df_basic)} 只上市股票基础信息")
        
        # 2. 获取目标日期及其之前的交易日列表
        query_dates = f"""
            SELECT DISTINCT trade_date 
            FROM stock_daily 
            WHERE trade_date <= '{target_date}' 
            ORDER BY trade_date DESC 
            LIMIT {lookback_days}
        """
        df_dates = pd.read_sql(query_dates, self.engine)
        
        if df_dates.empty:
            print(f"警告: 数据库中未找到日期 {target_date} 之前的交易日列表")
            raise ValueError(f"数据库中未找到日期 {target_date} 之前的行情数据")
            
        min_date = df_dates['trade_date'].min()
        max_date = df_dates['trade_date'].max()
        print(f"计算日期范围: {min_date} 至 {max_date}, 共 {len(df_dates)} 个交易日")
        
        # 3. 获取这段时间内的日线行情
        query_daily = f"""
            SELECT ts_code, trade_date, close 
            FROM stock_daily 
            WHERE trade_date >= '{min_date}' AND trade_date <= '{max_date}'
        """
        df_daily = pd.read_sql(query_daily, self.engine)
        print(f"获取到 {len(df_daily)} 条行情记录")
        
        # 确保类型正确
        df_daily['close'] = pd.to_numeric(df_daily['close'])
        
        return df_basic, df_daily

    def calculate_returns(self, df_daily: pd.DataFrame, periods: List[int] = [50, 120, 250]):
        """
        计算各周期的涨跌幅
        """
        print(f"正在计算周期为 {periods} 的涨跌幅...")
        
        # 按股票和日期排序
        df = df_daily.sort_values(['ts_code', 'trade_date']).copy()
        
        # 计算每个周期的涨幅
        for n in periods:
            df[f'return_{n}'] = df.groupby('ts_code')['close'].pct_change(periods=n)
            
        # 检查是否有非空涨幅
        for n in periods:
            non_null_count = df[f'return_{n}'].notnull().sum()
            print(f"周期 {n} 的有效涨跌幅记录数: {non_null_count}")
            
        return df

    def calculate_rankings(self, df: pd.DataFrame, periods: List[int] = [50, 120, 250]):
        """
        计算全市场、板块和行业的 RPS 排名
        """
        print("正在计算多维度 RPS 排名...")
        
        # 我们只关心最新一天（目标日期）的排名
        latest_date = df['trade_date'].max()
        print(f"数据中最新日期为: {latest_date}")
        df_latest = df[df['trade_date'] == latest_date].copy()
        print(f"最新日期 {latest_date} 的记录数: {len(df_latest)}")
        
        if df_latest.empty:
            return df_latest

        # 过滤掉所有涨幅都为 NaN 的行，避免计算无意义的排名
        return_cols = [f'return_{n}' for n in periods]
        df_latest = df_latest.dropna(subset=return_cols, how='all')
        print(f"过滤掉无有效涨幅的记录后，剩余记录数: {len(df_latest)}")

        if df_latest.empty:
            return df_latest

        for n in periods:
            col = f'return_{n}'
            if col not in df_latest.columns:
                continue
            
            # 1. 全市场排名
            df_latest[f'rps_{n}_all'] = df_latest[col].rank(pct=True) * 100
            
            # 2. 按市场类型排名 (如主板、创业板)
            if 'market' in df_latest.columns:
                df_latest[f'rps_{n}_market'] = df_latest.groupby('market')[col].rank(pct=True) * 100
            
            # 3. 按行业排名
            if 'industry' in df_latest.columns:
                df_latest[f'rps_{n}_industry'] = df_latest.groupby('industry')[col].rank(pct=True) * 100
                
        return df_latest

    def save_results(self, df_rps: pd.DataFrame):
        """
        保存计算结果到数据库
        """
        if df_rps.empty:
            print("没有计算结果需要保存")
            return
            
        target_date = df_rps['trade_date'].iloc[0]
        print(f"正在保存 {target_date} 的 RPS 计算结果到数据库...")
        
        # 准备要保存的列
        cols_to_save = [
            'ts_code', 'trade_date',
            'rps_50_all', 'rps_120_all', 'rps_250_all',
            'rps_50_market', 'rps_120_market', 'rps_250_market',
            'rps_50_industry', 'rps_120_industry', 'rps_250_industry'
        ]
        
        # 只保留存在的列
        available_cols = [c for c in cols_to_save if c in df_rps.columns]
        df_save = df_rps[available_cols].copy()
        
        # 保留两位小数
        float_cols = df_save.select_dtypes(include=['float64']).columns
        df_save[float_cols] = df_save[float_cols].round(2)
        
        # 写入数据库前，先删除当天的旧数据（如果存在）
        with self.engine.begin() as conn:
            conn.execute(text(f"DELETE FROM stock_rps_daily WHERE trade_date = '{target_date}'"))
            
        df_save.to_sql('stock_rps_daily', self.engine, if_exists='append', index=False, method='multi', chunksize=2000)
        print(f"成功保存 {len(df_save)} 条记录")

    def run(self, target_date: str):
        """
        运行计算流程
        """
        try:
            # 1. 获取数据
            df_basic, df_daily = self.fetch_data(target_date)
            
            # 2. 合并数据以获得行业和市场信息
            df_merged = pd.merge(df_daily, df_basic, on='ts_code', how='inner')
            
            # 3. 计算涨幅
            df_with_returns = self.calculate_returns(df_merged)
            
            # 4. 计算 RPS 排名
            df_rps = self.calculate_rankings(df_with_returns)
            
            # 5. 存储结果
            self.save_results(df_rps)
            
            return True
        except Exception as e:
            print(f"计算 RPS 时发生错误: {e}")
            import traceback
            traceback.print_exc()
            return False

if __name__ == "__main__":
    import sys
    # 如果作为脚本运行，可以传入日期
    date_str = sys.argv[1] if len(sys.argv) > 1 else datetime.datetime.now().strftime('%Y%m%d')
    calc = RPSCalculator()
    calc.run(date_str)
