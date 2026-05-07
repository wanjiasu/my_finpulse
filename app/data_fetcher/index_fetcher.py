import pandas as pd
try:
    from .base_fetcher import TushareFetcher
except ImportError:
    from base_fetcher import TushareFetcher

class IndexFetcher(TushareFetcher):
    """
    指数数据获取类
    """
    def get_index_daily(self, ts_codes: list = None, start_date: str = "", end_date: str = "", limit: int = 10):
        """
        获取指定指数的日线行情数据。

        参数:
        ----------
        ts_codes : list, 可选
            指数代码列表。默认为 ['000001.SH', '399006.SZ']。
        start_date : str, 可选
            开始日期 (YYYYMMDD)。
        end_date : str, 可选
            结束日期 (YYYYMMDD)。
        limit : int, 可选
            每个指数获取的数据条数上限。默认为 10。

        返回:
        -------
        pd.DataFrame or None
        """
        if ts_codes is None:
            ts_codes = ['000001.SH', '399006.SZ']
        
        print(f"正在获取指数数据: {ts_codes}...")
        
        try:
            df_list = []
            for code in ts_codes:
                df = self.pro.index_daily(
                    ts_code=code, 
                    start_date=start_date, 
                    end_date=end_date, 
                    limit=limit
                )
                if df is not None and not df.empty:
                    df_list.append(df)
            
            if df_list:
                combined_df = pd.concat(df_list, ignore_index=True)
                return self._handle_data(combined_df, "get_index_daily")
            return None
        except Exception as e:
            print(f"获取指数数据时发生错误: {e}")
            return None
