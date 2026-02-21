import pandas as pd
import numpy as np
from typing import Optional

class TechnicalFactors:
    """
    基础量化因子库 (Technical Factors).
    
    Role: 生成价量、动量、反转等基础技术因子.
    Reference: src/生成基础量化因子库.md
    
    [Implementation]: 
    Since Ta-Lib is not installed, implemented using Pure Pandas/NumPy.
    """
    
    @staticmethod
    def add_all_factors(df: pd.DataFrame) -> pd.DataFrame:
        """
        一次性计算并添加所有因子.
        """
        if df.empty:
            return df
            
        df = df.copy()
        
        # 预计算 Returns
        df['pct_chg'] = df['close'].pct_change()
        
        # 1. CorrPV (量价相关性)
        df['factor_corr_pv'] = TechnicalFactors.calc_corr_pv(df)
        
        # 2. VWAP_Dev (VWAP 偏离度)
        df['factor_vwap_dev'] = TechnicalFactors.calc_vwap_dev(df)
        
        # 3. RSquared (趋势线性度)
        df['factor_rsquared'] = TechnicalFactors.calc_rsquared(df)
        
        # 4. Sharpe_Momentum (波动调整动量)
        df['factor_sharpe_mom'] = TechnicalFactors.calc_sharpe_momentum(df)
        
        # 5. KDJ_RSI_Coincidence (低位共振)
        df['signal_kdj_rsi'] = TechnicalFactors.calc_kdj_rsi_coincidence(df)
        
        # 6. Boll_Bandwidth (布林带宽度)
        df['factor_boll_width'] = TechnicalFactors.calc_boll_bandwidth(df)
        
        # 7. MACD Golden Cross (MACD 金叉趋势)
        df['signal_macd_cross'] = TechnicalFactors.calc_macd_cross(df)
        
        return df

    @staticmethod
    def calc_corr_pv(df: pd.DataFrame, window: int = 20) -> pd.Series:
        return df['close'].rolling(window=window).corr(df['volume'])

    @staticmethod
    def calc_vwap_dev(df: pd.DataFrame) -> pd.Series:
        if 'amount' in df.columns:
            vwap = df['amount'] / (df['volume'] + 1e-9)
        else:
            vwap = (df['open'] + df['high'] + df['low'] + df['close']) / 4.0
        return (df['close'] - vwap) / vwap

    @staticmethod
    def calc_rsquared(df: pd.DataFrame, window: int = 20) -> pd.Series:
        # R^2 = corr(Price, Time)^2
        idx_series = pd.Series(np.arange(len(df)), index=df.index)
        r = df['close'].rolling(window=window).corr(idx_series)
        return r ** 2

    @staticmethod
    def calc_sharpe_momentum(df: pd.DataFrame, window: int = 20) -> pd.Series:
        pct = df['pct_chg']
        mom_mean = pct.rolling(window=window).mean()
        mom_std = pct.rolling(window=window).std()
        return mom_mean / (mom_std + 1e-9)

    @staticmethod
    def calc_kdj_rsi_coincidence(df: pd.DataFrame) -> pd.Series:
        """
        KDJ(9,3,3) + RSI(6) 低位共振.
        Pandas Implementation.
        """
        close = df['close']
        high = df['high']
        low = df['low']
        
        # --- KDJ ---
        # 1. RSV = (Close - Lowest_9) / (Highest_9 - Lowest_9) * 100
        low_9 = low.rolling(window=9).min()
        high_9 = high.rolling(window=9).max()
        rsv = (close - low_9) / (high_9 - low_9 + 1e-9) * 100
        
        # 2. K = SMA(RSV, 3) (Standard definition is EMA, but generic is SMA of RSV)
        # However, China A-share standard (TongDaXin) uses:
        # K = 2/3 * PrevK + 1/3 * RSV
        # D = 2/3 * PrevD + 1/3 * K
        # This is equivalent to EMA(com=2).
        
        # Using Pandas ewm to simulate TongDaXin KDJ
        k = rsv.ewm(com=2, adjust=False).mean()
        d = k.ewm(com=2, adjust=False).mean()
        # j = 3 * k - 2 * d # J not used in logic
        
        # --- RSI (6) ---
        # RSI = 100 - 100 / (1 + RS)
        # RS = AvgGain / AvgLoss
        delta = close.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # Wilder's Smoothing (alpha = 1/n)
        avg_gain = gain.ewm(alpha=1/6, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/6, adjust=False).mean()
        
        rs = avg_gain / (avg_loss + 1e-9)
        rsi = 100 - (100 / (1 + rs))
        
        # --- Logic ---
        cond_k = k < 20
        cond_d = d < 20
        cond_rsi = rsi < 30
        
        signal = (cond_k & cond_d & cond_rsi).astype(int)
        return pd.Series(signal, index=df.index)

    @staticmethod
    def calc_boll_bandwidth(df: pd.DataFrame, timeperiod: int = 20, nbdev: float = 2.0) -> pd.Series:
        """
        布林带宽度 (Pandas Implementation).
        """
        close = df['close']
        middle = close.rolling(window=timeperiod).mean()
        std = close.rolling(window=timeperiod).std()
        
        upper = middle + nbdev * std
        lower = middle - nbdev * std
        
        return (upper - lower) / (middle + 1e-9)

    @staticmethod
    def calc_macd_cross(df: pd.DataFrame, fastperiod: int = 12, slowperiod: int = 26, signalperiod: int = 9) -> pd.Series:
        """
        MACD Golden Cross (MACD 金叉).
        Pandas Implementation.
        """
        close = df['close']
        ema_fast = close.ewm(span=fastperiod, adjust=False).mean()
        ema_slow = close.ewm(span=slowperiod, adjust=False).mean()
        
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=signalperiod, adjust=False).mean()
        macd = (dif - dea) * 2
        
        # Golden Cross occurs when DIF crosses above DEA (or MACD turns positive) today or yesterday
        # We check if MACD is positive and was negative/zero recently, 
        # OR if it's currently expanding positively and > 0.
        
        cross_up = (macd > 0) & (macd.shift(1) <= 0)
        strong_trend = (macd > 0) & (macd > macd.shift(1))
        
        signal = (cross_up | strong_trend).astype(int)
        return pd.Series(signal, index=df.index)

    @staticmethod
    def calculate_composite_score(df: pd.DataFrame) -> tuple[float, str]:
        """
        计算综合择时得分 (0-100).
        Returns: (Score, DescriptionString)
        """
        if df.empty: return 0.0, ""
        
        last = df.iloc[-1]
        
        # 1. Trend Linearity (R^2)
        s1 = last.get('factor_rsquared', 0) * 100
        
        # 2. PV Correlation
        corr = last.get('factor_corr_pv', 0)
        s2 = (corr + 1) * 50
        
        # 3. Momentum (Sharpe)
        sharpe = last.get('factor_sharpe_mom', 0)
        if np.isnan(sharpe): sharpe = 0
        s3 = (1 / (1 + np.exp(-sharpe))) * 100
        
        final_score = round((s1 + s2 + s3) / 3, 1)
        
        # Detail String for UI
        desc = f"趋势:{int(s1)} 量价:{int(s2)} 动量:{int(s3)}"
        
        return final_score, desc
