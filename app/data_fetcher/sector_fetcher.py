import pandas as pd
try:
    from .base_fetcher import TushareFetcher
except ImportError:
    from base_fetcher import TushareFetcher

class SectorFetcher(TushareFetcher):
    """
    板块与涨跌停榜单数据获取类
    """
    def get_limit_list(self, trade_date: str = "", limit_type: str = "涨停池", ts_code: str = "", market: str = "", start_date: str = "", end_date: str = ""):
        """
        获取同花顺每日涨跌停榜单数据 (limit_list_ths)。

        参数:
        ----------
        trade_date : str, 可选
            交易日期 (YYYYMMDD)。
        limit_type : str, 可选
            板单类别：涨停池、连扳池、冲刺涨停、炸板池、跌停池。默认：涨停池。
        """
        params = {}
        if trade_date: params['trade_date'] = trade_date
        if limit_type: params['limit_type'] = limit_type
        if ts_code: params['ts_code'] = ts_code
        if market: params['market'] = market
        if start_date: params['start_date'] = start_date
        if end_date: params['end_date'] = end_date

        try:
            df = self.call_with_retry(
                self.pro.limit_list_ths,
                **params
            )
            return self._handle_data(df, f"get_limit_list({limit_type})")
        except Exception as e:
            print(f"获取涨跌停榜单时发生错误: {e}")
            return None

    def get_limit_cpt_list(self, trade_date: str = "", ts_code: str = "", start_date: str = "", end_date: str = ""):
        """
        获取每天涨停股票最多最强的概念板块 (limit_cpt_list)。
        """
        params = {}
        if trade_date: params['trade_date'] = trade_date
        if ts_code: params['ts_code'] = ts_code
        if start_date: params['start_date'] = start_date
        if end_date: params['end_date'] = end_date

        try:
            df = self.call_with_retry(
                self.pro.limit_cpt_list,
                **params
            )
            return self._handle_data(df, "get_limit_cpt_list")
        except Exception as e:
            print(f"获取最强板块统计时发生错误: {e}")
            return None

    def get_ths_index(self, ts_code: str = "", exchange: str = "A", index_type: str = ""):
        """
        获取同花顺板块指数，包括概念、行业、特色指数 (ths_index)。

        参数:
        ----------
        ts_code : str, 可选
            指数代码。
        exchange : str, 可选
            市场类型：A-a股, HK-港股, US-美股。默认 A。
        index_type : str, 可选
            指数类型：N-概念指数, I-行业指数, R-地域指数, S-同花顺特色指数, ST-同花顺风格指数, TH-同花顺主题指数, BB-同花顺宽基指数。
        """
        params = {}
        if ts_code: params['ts_code'] = ts_code
        if exchange: params['exchange'] = exchange
        if index_type: params['type'] = index_type

        try:
            df = self.call_with_retry(
                self.pro.ths_index,
                **params
            )
            return self._handle_data(df, "get_ths_index")
        except Exception as e:
            print(f"获取同花顺板块指数时发生错误: {e}")
            return None

    def get_ths_member(self, ts_code: str = "", con_code: str = ""):
        """
        获取同花顺概念板块成分列表 (ths_member)。

        参数:
        ----------
        ts_code : str, 可选
            板块指数代码。
        con_code : str, 可选
            股票代码。
        """
        params = {}
        if ts_code: params['ts_code'] = ts_code
        if con_code: params['con_code'] = con_code

        try:
            df = self.call_with_retry(
                self.pro.ths_member,
                **params
            )
            return self._handle_data(df, f"get_ths_member({ts_code if ts_code else con_code})")
        except Exception as e:
            print(f"获取同花顺板块成分时发生错误: {e}")
            return None
