import pandas as pd
try:
    from .base_fetcher import TushareFetcher
except ImportError:
    from base_fetcher import TushareFetcher

class FinancialFetcher(TushareFetcher):
    """
    上市公司财务数据获取类（利润表、资产负债表、现金流量表）
    """
    def get_income(self, ts_code: str, start_date: str = "", end_date: str = "", period: str = "", report_type: str = ""):
        """
        获取利润表数据 (income)
        """
        params = {'ts_code': ts_code}
        if start_date: params['start_date'] = start_date
        if end_date: params['end_date'] = end_date
        if period: params['period'] = period
        if report_type: params['report_type'] = report_type

        try:
            df = self.call_with_retry(self.pro.income, **params)
            return self._handle_data(df, f"get_income({ts_code})")
        except Exception as e:
            print(f"获取利润表发生错误: {e}")
            return None

    def get_balancesheet(self, ts_code: str, start_date: str = "", end_date: str = "", period: str = "", report_type: str = ""):
        """
        获取资产负债表数据 (balancesheet)
        """
        params = {'ts_code': ts_code}
        if start_date: params['start_date'] = start_date
        if end_date: params['end_date'] = end_date
        if period: params['period'] = period
        if report_type: params['report_type'] = report_type

        try:
            df = self.call_with_retry(self.pro.balancesheet, **params)
            return self._handle_data(df, f"get_balancesheet({ts_code})")
        except Exception as e:
            print(f"获取资产负债表发生错误: {e}")
            return None

    def get_cashflow(self, ts_code: str, start_date: str = "", end_date: str = "", period: str = "", report_type: str = ""):
        """
        获取现金流量表数据 (cashflow)
        """
        params = {'ts_code': ts_code}
        if start_date: params['start_date'] = start_date
        if end_date: params['end_date'] = end_date
        if period: params['period'] = period
        if report_type: params['report_type'] = report_type

        try:
            df = self.call_with_retry(self.pro.cashflow, **params)
            return self._handle_data(df, f"get_cashflow({ts_code})")
        except Exception as e:
            print(f"获取现金流量表发生错误: {e}")
            return None
