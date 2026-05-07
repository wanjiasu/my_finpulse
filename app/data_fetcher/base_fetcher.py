import os
import tushare as ts
import pandas as pd
from dotenv import load_dotenv

class TushareFetcher:
    """
    Tushare 数据获取基类
    """
    def __init__(self, token: str = None, http_url: str = "http://101.35.233.113:8020/"):
        load_dotenv()
        self.token = token or os.getenv("TUSHARE_TOKEN")
        if not self.token:
            raise ValueError("错误: 未找到 TUSHARE_TOKEN。请在 .env 文件中设置或在初始化时提供。")
        
        # 初始化 Pro API
        self.pro = ts.pro_api(self.token)
        # 设置自定义 HTTP URL
        self.pro._DataApi__http_url = http_url
        
    def _handle_data(self, df: pd.DataFrame, method_name: str):
        """
        统一处理接口返回的数据
        """
        if df is not None and not df.empty:
            print(f"[{self.__class__.__name__}] {method_name} 成功获取数据，共 {len(df)} 条记录。")
            return df
        else:
            print(f"[{self.__class__.__name__}] {method_name} 未能获取到数据。")
            return None
