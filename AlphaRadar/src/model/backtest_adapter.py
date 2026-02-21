import backtrader as bt
import pandas as pd
from typing import Optional

class AlphaRadarPandasData(bt.feeds.PandasData):
    """
    AlphaRadar 专用 Pandas 数据源适配器.
    将 DataNexus 返回的 DataFrame 转换为 Backtrader 可用的 DataFeed.
    
    规则:
    1. 严格类型 (虽然 bt.feeds 主要是类属性配置，但在外部调用时需注意).
    2. 中文注释.
    """
    
    # 定义 DataFrame 列名映射
    # 根据 DataNexus.fetch_bars 返回的列: date, open, high, low, close, volume, symbol
    # 注意: PandasData 默认要求 datetime 为索引 或 指定列名
    
    params = (
        ('datetime', 'date'), # 日期列名
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'volume'),
        ('openinterest', None), # 无持仓量
    )
    
    # 如果数据是 '2023-01-01' 字符串格式，PandasData 会自动尝试解析，
    # 但最好在传入前确保 DataFrame index 是 datetime 或者 'date' 列是 datetime 对象。
