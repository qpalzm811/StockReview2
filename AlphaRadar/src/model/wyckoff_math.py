import pandas as pd
import numpy as np

class WyckoffMath:
    """
    Wyckoff Math Engine.
    Implements strict quantitative definitions for Wyckoff Events using Vectorized Pandas/Numpy.
    No AI, No Hallucinations. Pure Math.
    """

    @staticmethod
    def apply(df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all Wyckoff math logic to the DataFrame.
        Adds columns: 'wyckoff_spring', 'wyckoff_upthrust', 'wyckoff_stopping_vol'.
        """
        # [Fix] Allow shorter DF for testing (min 20 for MA20)
        if df.empty or len(df) < 20:
            return df
            
        df = df.copy()
        
        # Ensure base indicators exist
        if 'ma20' not in df.columns:
            df['ma20'] = df['close'].rolling(20).mean()
        if 'ma50' not in df.columns:
            df['ma50'] = df['close'].rolling(50).mean()
        if 'ma200' not in df.columns:
            df['ma200'] = df['close'].rolling(200).mean()
            
        # Volume MA
        df['vol_ma20'] = df['volume'].rolling(20).mean()
        
        # --- 1. Support & Resistance (Dynamic 20-day lookback) ---
        # Shift 1 to avoid lookahead bias (we compare Today vs Previous 20 days)
        df['support_20'] = df['low'].rolling(20).min().shift(1)
        df['resistance_20'] = df['high'].rolling(20).max().shift(1)
        
        # --- 2. Spring (Bullish) ---
        # Logic:
        # A. Low < Support (Undercut)
        # B. Close > Support (Reclaim)
        # C. Volume < Vol_MA20 (Low Supply Test) - Optional strictness, let's allow < 1.2x average
        # D. Trend Context (Optional): Close > MA50? Or just range? Let's keep it pure event first.
        
        # Condition A & B: Undercut and Reclaim
        cond_undercut = df['low'] < df['support_20']
        cond_reclaim = df['close'] > df['support_20']
        
        # Condition C: Volume Test (Not explosive volume, which would break support)
        # Using < 1.0 * Average (Strict Low Volume)
        cond_low_vol = df['volume'] < df['vol_ma20']
        
        df['wyckoff_spring'] = (cond_undercut & cond_reclaim & cond_low_vol).astype(int)
        
        # --- 3. Upthrust (Bearish) ---
        # Logic:
        # A. High > Resistance (Breakout attempt)
        # B. Close < Resistance (Failure)
        # C. Weak Close: Close is in the lower 40% of the bar range
        
        cond_breakout = df['high'] > df['resistance_20']
        cond_fail = df['close'] < df['resistance_20']
        
        # Calculate relative close position (0.0 = Low, 1.0 = High)
        # Avoid division by zero
        range_high_low = df['high'] - df['low']
        close_pos = (df['close'] - df['low']) / range_high_low.replace(0, 1)
        
        cond_weak_close = close_pos < 0.4
        
        df['wyckoff_upthrust'] = (cond_breakout & cond_fail & cond_weak_close).astype(int)
        
        # --- 4. Stopping Volume (Bullish Reversal) ---
        # Logic:
        # A. Down Trend context: Close < MA20
        # B. Explosion Volume: Vol > 2.0 * MA20_Vol
        # C. Recovery Close: Close > Low + 0.6 * Range (Long Lower Shadow or Strong Close)
        
        cond_downtrend = df['close'] < df['ma20']
        cond_huge_vol = df['volume'] > 2.0 * df['vol_ma20']
        cond_recovery = close_pos > 0.6
        
        df['wyckoff_stopping_vol'] = (cond_downtrend & cond_huge_vol & cond_recovery).astype(int)
        
        # --- VSA 1: Churning (Effort vs Result divergence) ---
        # "High Volume, Low Progress" -> Distribution
        # A. Uptrend: Close > MA20
        # B. Ultra High Volume: Vol > 2.0 * Vol_MA20
        # C. Small Real Body: abs(Close - Open) < 0.3 * (High - Low) (Spinning Top/Doji)
        # Note: If High-Low is 0 (flat), division guard needed. Use Range > 0 checks implicitly.
        
        cond_uptrend = df['close'] > df['ma20']
        body_size = (df['close'] - df['open']).abs()
        range_size = (df['high'] - df['low']).replace(0, 1) # Guard div by zero
        cond_small_body = (body_size / range_size) < 0.3
        
        df['vsa_churning'] = (cond_uptrend & cond_huge_vol & cond_small_body).astype(int)
        
        # --- VSA 2: No Demand (Weak Rally) ---
        # "Up Bar on Low Volume" -> Bearish
        # A. Downtrend: Close < MA20
        # B. Up Bar: Close > Previous Close
        # C. Low Volume: Vol < 0.8 * Vol_MA20
        
        cond_up_bar = df['close'] > df['close'].shift(1)
        cond_weak_vol = df['volume'] < 0.8 * df['vol_ma20']
        
        df['vsa_no_demand'] = (cond_downtrend & cond_up_bar & cond_weak_vol).astype(int)
        
        # --- 5. Clean up temp columns ---
        # We might want to keep support/resistance for plotting, but for now just return signals
        return df

    @staticmethod
    def get_phase(df: pd.DataFrame) -> str:
        """
        Determine current Wyckoff Phase based on last bar.
        Returns: 'Accumulation', 'Markup', 'Distribution', 'Markdown' or 'Unknown'
        """
        if df.empty or len(df) < 200:
            return "Unknown"
            
        last = df.iloc[-1]
        close = last['close']
        ma50 = last['ma50']
        ma200 = last['ma200']
        
        # Classic MA Logic
        if close > ma50 > ma200:
            return "Markup (Phase E)"
        elif close < ma50 < ma200:
            return "Markdown (Phase E)"
        elif ma50 > ma200 and close < ma50:
            return "Distribution (Potential)"
        elif ma50 < ma200 and close > ma50:
            return "Accumulation (Potential)"
        else:
            return "Range"
