import pandas as pd
import numpy as np
from typing import Optional, Tuple, Dict, Union, List

# 类型别名用于 Strict Type Hinting
PatternInfo = Dict[str, Union[str, float]]

class PatternRecognizer:
    """
    技术分析与形态识别引擎 (Technical Analysis Engine).
    
    负责核心的指标计算与形态匹配.
    遵循规则:
    1. 向量化计算 (Vectorized Operations).
    2. 动态阈值 (ATR Based).
    3. 全中文注释.
    """
    
    def __init__(self) -> None:
        pass

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算基础技术指标.
        
        Args:
            df (pd.DataFrame): 原始 OHLCV 数据.
            
        Returns:
            pd.DataFrame: 包含 ATR, MA, RSI 等指标的数据.
        """
        if df.empty:
            return df
            
        # 确保按日期排序
        df = df.sort_values('date').reset_index(drop=True)
        
        close: pd.Series = df['close']
        high: pd.Series = df['high']
        low: pd.Series = df['low']
        volume: pd.Series = df['volume']
        
        # 1. ATR (平均真实波幅) - 用于动态阈值
        # TR = max(high-low, abs(high-prev_close), abs(low-prev_close))
        prev_close = close.shift(1)
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        
        # 使用 Pandas 向量化操作取最大值
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['atr_14'] = tr.rolling(window=14).mean()
        
        # 2. 均线系统 (Trend)
        df['ma_20'] = close.rolling(window=20).mean()
        df['ma_50'] = close.rolling(window=50).mean()
        df['ma_200'] = close.rolling(window=200).mean()
        
        # 3. 成交量过滤器
        df['vol_ma_20'] = volume.rolling(window=20).mean()
        df['vol_ratio'] = volume / df['vol_ma_20']
        
        return df

    def identify_double_bottom(self, df: pd.DataFrame, lookback: int = 60) -> Tuple[bool, Optional[PatternInfo]]:
        """
        识别双底形态 (Double Bottom).
        
        逻辑:
        1. 在最近 lookback 周期内找到两个显著低点.
        2. 两个低点价差 < 0.5 * ATR.
        3. 颈线高度适中 (> 2 * ATR).
        4. 当前价格有效突破颈线.
        
        Args:
            df (pd.DataFrame): 包含指标的数据.
            lookback (int): 回溯窗口大小.
            
        Returns:
            Tuple[bool, Optional[PatternInfo]]: (是否匹配, 形态详情)
        """
        if len(df) < lookback or 'atr_14' not in df.columns:
            return False, None
            
        # 提取相关数据切片
        subset = df.iloc[-lookback:].reset_index(drop=True)
        current_price: float = float(subset.iloc[-1]['close'])
        atr: float = float(subset.iloc[-1]['atr_14'])
        
        if pd.isna(atr):
            return False, None
            
        # 寻找谷底
        # 简单策略: 将窗口分为左右两部分，分别找最低点
        mid_idx = len(subset) // 2
        left_part = subset.iloc[:mid_idx]
        right_part = subset.iloc[mid_idx:-3] # 排除最近3根K线(防止刚形成的低点被误判为底2)
        
        if left_part.empty or right_part.empty:
            return False, None
            
        # 底 1
        min1_idx = left_part['low'].idxmin()
        min1_val: float = float(left_part.loc[min1_idx, 'low'])
        
        # 底 2
        min2_idx = right_part['low'].idxmin()
        min2_val: float = float(right_part.loc[min2_idx, 'low'])
        
        # 规则 1: 两个底间隔至少 10 天
        # 注意: idxmin 返回的是 index label，因为我们 reset_index 了，所以直接相减即可
        #min2_idx 是相对于 subset 的全局索引吗? idxmin 返回的是 label.
        # left_part 的 label 是 0 ~ mid-1
        # right_part 的 label 是 mid ~ end
        if (min2_idx - min1_idx) < 10:
            return False, None
            
        # 规则 2: 两个低点价差不超过 0.5 * ATR (动态阈值)
        if abs(min1_val - min2_val) > (0.5 * atr):
            return False, None
            
        # 规则 3: 寻找颈线 (两底之间的最高点)
        between_bottoms = subset.iloc[min1_idx:min2_idx]
        if between_bottoms.empty:
            return False, None
            
        neckline_idx = between_bottoms['high'].idxmax()
        neckline_val: float = float(between_bottoms.loc[neckline_idx, 'high'])
        
        # 颈线深度检查 (颈线需高于底部一定幅度)
        avg_bottom = (min1_val + min2_val) / 2
        if (neckline_val - avg_bottom) < (2.0 * atr):
            return False, None
            
        # 规则 4: 突破确认 (当前价格 > 颈线, 且是最近发生的)
        # 检查最近 3 天是否有收盘价突破颈线
        recent_bars = subset.iloc[-3:]
        breakout_confirmed = False
        for row in recent_bars.itertuples():
            if row.close > neckline_val:
                breakout_confirmed = True
                break
                
        if current_price > neckline_val and breakout_confirmed:
             return True, {
                "pattern": "Double Bottom",
                "bottom1": min1_val,
                "bottom2": min2_val,
                "neckline": neckline_val,
                "atr": atr,
                "confidence": 0.85
            }
            
        return False, None

    def identify_wyckoff_spring(self, df: pd.DataFrame) -> Tuple[bool, Optional[PatternInfo]]:
        """
        识别 Wyckoff Spring (弹簧) 形态.
        
        逻辑:
        1. 价格瞬间跌破明显支撑位 (Support).
        2. 收盘迅速拉回支撑位上方.
        3. 下影线较长 (Hammer 结构).
        
        Args:
            df (pd.DataFrame): 数据.
            
        Returns:
            Tuple[bool, Optional[PatternInfo]]: 结果.
        """
        if len(df) < 50:
            return False, None
            
        subset = df.iloc[-50:].reset_index(drop=True)
        atr: float = float(subset.iloc[-1]['atr_14'])
        
        if pd.isna(atr):
            return False, None
            
        # 寻找支撑位 (前 45 天的最低点)
        reference_range = subset.iloc[:-5]
        support_level: float = float(reference_range['low'].min())
        
        # 检查最近 5 根 K 线
        recent = subset.iloc[-5:]
        
        for row in recent.itertuples():
            # 跌破支撑
            if row.low < support_level:
                # 收回支撑上方
                # Wyckoff Spring: 深度刺透但收盘强劲
                if row.close > support_level:
                    # 检查下影线比例
                    bar_range = row.high - row.low
                    # 下影线长度
                    if row.close > row.open:
                        lower_wick = row.open - row.low
                    else:
                        lower_wick = row.close - row.low
                        
                    if bar_range > 0 and (lower_wick / bar_range) > 0.5:
                         return True, {
                            "pattern": "Wyckoff Spring",
                            "support_level": support_level,
                            "dip_low": row.low,
                            "atr": atr,
                            "confidence": 0.8
                        }
        
        return False, None
