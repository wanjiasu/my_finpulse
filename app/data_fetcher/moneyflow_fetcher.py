import pandas as pd
try:
    from .base_fetcher import TushareFetcher
except ImportError:
    from base_fetcher import TushareFetcher

class MoneyflowFetcher(TushareFetcher):
    """
    股票资金流向获取类
    """
    def get_moneyflow(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = ""):
        """
        获取沪深A股票资金流向数据 (moneyflow)。
        """
        # 构建非空参数字典
        params = {}
        if ts_code: params['ts_code'] = ts_code
        if trade_date: params['trade_date'] = trade_date
        if start_date: params['start_date'] = start_date
        if end_date: params['end_date'] = end_date

        try:
            df = self.call_with_retry(
                self.pro.moneyflow,
                **params
            )
            return self._handle_data(df, f"get_moneyflow({ts_code if ts_code else trade_date})")
        except Exception as e:
            print(f"获取资金流向时发生错误: {e}")
            return None
