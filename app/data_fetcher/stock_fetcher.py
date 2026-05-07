import pandas as pd
try:
    from .base_fetcher import TushareFetcher
except ImportError:
    from base_fetcher import TushareFetcher

class StockFetcher(TushareFetcher):
    """
    股票基础信息、行情与复权因子获取类
    """
    def get_stock_list(self, ts_code: str = "", name: str = "", market: str = "", list_status: str = "L", exchange: str = "", is_hs: str = ""):
        """
        获取全市场股票基础信息列表 (stock_basic)。
        """
        try:
            df = self.call_with_retry(
                self.pro.stock_basic,
                ts_code=ts_code,
                name=name,
                market=market,
                list_status=list_status,
                exchange=exchange,
                is_hs=is_hs
            )
            return self._handle_data(df, "get_stock_list")
        except Exception as e:
            print(f"获取股票列表时发生错误: {e}")
            return None

    def get_trade_cal(self, start_date: str = "", end_date: str = "", exchange: str = "SSE", is_open: int = 1):
        """
        获取交易日历。
        """
        try:
            df = self.call_with_retry(
                self.pro.trade_cal,
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                is_open=is_open
            )
            return self._handle_data(df, "get_trade_cal")
        except Exception as e:
            print(f"获取交易日历时发生错误: {e}")
            return None

    def get_stock_daily(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = ""):
        """
        获取股票日线行情数据 (daily)。
        """
        try:
            df = self.call_with_retry(
                self.pro.daily,
                ts_code=ts_code,
                trade_date=trade_date,
                start_date=start_date,
                end_date=end_date
            )
            return self._handle_data(df, f"get_stock_daily({ts_code if ts_code else '全市场'})")
        except Exception as e:
            print(f"获取股票日线行情时发生错误: {e}")
            return None

    def get_adj_factor(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = ""):
        """
        获取股票复权因子数据 (adj_factor)。
        """
        try:
            df = self.call_with_retry(
                self.pro.adj_factor,
                ts_code=ts_code,
                trade_date=trade_date,
                start_date=start_date,
                end_date=end_date
            )
            return self._handle_data(df, f"get_adj_factor({ts_code if ts_code else '全市场'})")
        except Exception as e:
            print(f"获取复权因子时发生错误: {e}")
            return None
