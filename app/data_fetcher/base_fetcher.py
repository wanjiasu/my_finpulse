import os
import tushare as ts
import pandas as pd
import time
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
        
    def call_with_retry(self, api_func, max_retries=3, retry_wait=60, **kwargs):
        """
        带重试机制的 API 调用封装
        
        参数:
        ----------
        api_func : callable
            Tushare API 方法，如 self.pro.daily
        max_retries : int
            最大重试次数
        retry_wait : int
            触发限流后的等待时间(秒)
        """
        retries = 0
        while retries <= max_retries:
            try:
                df = api_func(**kwargs)
                return df
            except Exception as e:
                error_msg = str(e)
                # 识别频率限制错误 (Tushare 常见的报错关键字)
                if "抱歉，您每分钟最多访问" in error_msg or "每分钟最多访问" in error_msg or "接口限流" in error_msg:
                    retries += 1
                    if retries <= max_retries:
                        print(f"触发 Tushare 频率限制，等待 {retry_wait} 秒后进行第 {retries} 次重试... (错误: {error_msg})")
                        time.sleep(retry_wait)
                        continue
                
                # 其他不可重试的错误直接抛出或记录
                print(f"API 调用发生非限流错误: {e}")
                raise e
        return None

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
