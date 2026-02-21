import pandas as pd
import numpy as np
from typing import Dict, Any, List

class ComplexPatterns:
    """
    复杂形态识别因子 (Complex Patterns).
    
    Role: 识别收敛三角形、双底、VCP等高级形态.
    Reference: src/复杂形态识别因子.md
    
    [Technology]:
    Using Rolling Windows + NumPy for vectorization (Simulating argrelextrema).
    """

    @staticmethod
    def detect_patterns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Main entry to detect all patterns.
        Adds boolean columns: 'pattern_triangle', 'pattern_double_bottom', 'pattern_vcp'
        """
        if df.empty or len(df) < 60:
            return df
            
        df = df.copy()
        
        # 1. Double Bottom
        df['pattern_double_bottom'] = ComplexPatterns.identify_double_bottom(df)
        
        # 2. Converging Triangle
        df['pattern_triangle'] = ComplexPatterns.identify_triangle(df)
        
        # 3. VCP
        df['pattern_vcp'] = ComplexPatterns.identify_vcp(df)
        
        return df

    @staticmethod
    def identify_double_bottom(df: pd.DataFrame) -> pd.Series:
        """
        Double Bottom Pattern (双底) - Vectorized NumPy Implementation.
        """
        if len(df) < 60:
            return pd.Series(0, index=df.index)
            
        close = df['close'].values
        low = df['low'].values
        high = df['high'].values
        
        # Output Signals
        signals = np.zeros(len(df), dtype=int)
        
        # ATR Proxy (3% of Close)
        atr_proxy = close * 0.03
        
        # --- Vectorized Trough Detection ---
        # Find indices where Low[i] < Low[i-1] and Low[i] < Low[i+1]
        # Pad with infinity to handle edges safely without shift errors
        padded_low = np.pad(low, (1, 1), mode='edge')
        
        # Boolean mask for troughs (center vs neighbors)
        is_trough = (padded_low[1:-1] < padded_low[:-2]) & (padded_low[1:-1] < padded_low[2:])
        
        # Get indices of troughs
        trough_indices = np.where(is_trough)[0]
        
        if len(trough_indices) < 2:
            return pd.Series(signals, index=df.index)
            
        # We only check the Last Bar for Scanner Optimization
        # Logic: Did a breakout happen TODAY?
        
        # 1. Check Breakout Condition (Close > Neckline)
        # We need to find if there exists a valid double bottom structure preceding this breakout.
        
        # Iterate only through RECENT troughs to check if they form a W with an older trough
        # Limit candidate troughs to last 60 bars
        latest_idx = len(df) - 1
        
        # Filter troughs in window [End-60, End-8] (Need space for right leg)
        window_start = max(0, latest_idx - 60)
        window_end = latest_idx - 5 # Right leg must be formed before breakout
        
        valid_troughs = trough_indices[(trough_indices >= window_start) & (trough_indices <= window_end)]
        
        if len(valid_troughs) < 2:
             return pd.Series(signals, index=df.index)
             
        # Check pairs of troughs
        # To avoid O(N^2), we just check the last few combinations (most likely candidates)
        # Double bottom usually spans 20-40 bars.
        
        current_atr = atr_proxy[latest_idx]
        current_close = close[latest_idx]
        
        # Check the last 3 troughs against their predecessors
        # Vectorized check could be complex for arbitrary pairs. 
        # Since we optimize for 'Last Bar' signal, we can iterate just the valid pairs efficiently.
        
        # Let's take the last 5 troughs
        recent_troughs = valid_troughs[-5:]
        found_pattern = False
        
        for i in range(len(recent_troughs)-1, 0, -1): # From newest to oldest
            t2 = recent_troughs[i]
            val2 = low[t2]
            
            for j in range(i-1, -1, -1):
                t1 = recent_troughs[j]
                val1 = low[t1]
                
                # Condition 1: Time Distance (> 8 bars)
                if (t2 - t1) < 8: continue
                
                # Condition 2: Price Diff (< 0.5 ATR)
                if abs(val2 - val1) > (0.5 * current_atr): continue
                
                # Condition 3: Neckline Validation
                # Highest High between t1 and t2
                neck_idx = t1 + np.argmax(high[t1:t2])
                neckline = high[neck_idx]
                
                # Depth Check (> 1.5 ATR)
                avg_bottom = (val1 + val2) / 2
                if (neckline - avg_bottom) < (1.5 * current_atr): continue

                # Condition 4: Breakout Logic
                # Did we JUST break the neckline?
                # Check Last Close > Neckline
                if current_close > neckline:
                     # Check if it's a fresh breakout (e.g. yesterday was below)
                     # or close to it.
                     if close[latest_idx-1] <= neckline or close[latest_idx-2] <= neckline:
                         signals[latest_idx] = 1
                         found_pattern = True
                         break
            if found_pattern: break
            
        return pd.Series(signals, index=df.index)

    @staticmethod
    def identify_triangle(df: pd.DataFrame) -> pd.Series:
        """
        Converging Triangle (收敛三角形) - Vectorized NumPy Implementation.
        """
        if len(df) < 40:
            return pd.Series(0, index=df.index)

        close = df['close'].values
        high = df['high'].values
        low = df['low'].values
        vol = df['volume'].values
        
        # Simple Moving Average for Volume (Fast, no pandas overhead if possible, but pandas rolling is optimized in C)
        # Using the passed DF series is fine.
        vol_ma20 = df['volume'].rolling(20).mean().fillna(0).values
        
        signals = np.zeros(len(df), dtype=int)
        
        # --- Logic for Last Bar Only (Scanner Mode) ---
        idx = len(df) - 1
        
        # 1. Volume Filter (Breakout Volume)
        if vol[idx] < 1.5 * vol_ma20[idx]:
             return pd.Series(signals, index=df.index)
        
        # 2. Pattern Window (last 40 bars)
        window = 40
        start = max(0, idx - window)
        
        w_high = high[start:idx]
        w_low = low[start:idx]
        
        # 3. Find Peaks/Troughs (Vectorized)
        # Pad for edge handling
        p_high = np.pad(w_high, (1,1), mode='edge')
        p_low = np.pad(w_low, (1,1), mode='edge')
        
        is_peak = (p_high[1:-1] > p_high[:-2]) & (p_high[1:-1] > p_high[2:])
        is_trough = (p_low[1:-1] < p_low[:-2]) & (p_low[1:-1] < p_low[2:])
        
        peak_idxs = np.where(is_peak)[0]
        trough_idxs = np.where(is_trough)[0]
        
        if len(peak_idxs) < 2 or len(trough_idxs) < 2:
             return pd.Series(signals, index=df.index)
             
        # 4. Check Slopes (Convergence)
        # Peaks Decreasing?
        # Linear Regression Slope of Peaks
        # y = mx + c
        # Use simple end-point slope for speed, or lstsq for accuracy.
        # End-Point is often sufficient for "visual" triangle.
        
        first_peak_val = w_high[peak_idxs[0]]
        last_peak_val = w_high[peak_idxs[-1]]
        
        first_trough_val = w_low[trough_idxs[0]]
        last_trough_val = w_low[trough_idxs[-1]]
        
        if last_peak_val >= first_peak_val: return pd.Series(signals, index=df.index) # Highs not lower
        if last_trough_val <= first_trough_val: return pd.Series(signals, index=df.index) # Lows not higher
        
        # 5. Resistance Line Calculation
        # Line through Last 2 Peaks (Most relevant resistance)
        # or Max Peak and Last Peak?
        # Standard: Trendline connecting major peaks.
        # Let's use Line connecting First and Last Peak in window.
        
        p1_x, p1_y = peak_idxs[0], first_peak_val
        p2_x, p2_y = peak_idxs[-1], last_peak_val
        
        # Slope
        m = (p2_y - p1_y) / (p2_x - p1_x + 1e-9)
        c = p1_y - m * p1_x
        
        # Current Bar Resistance Projection
        # Current bar index relative to window start is 'len(w_high)' (which is 'window' or smaller)
        curr_rel_idx = len(w_high) 
        resistance = m * curr_rel_idx + c
        
        # 6. Breakout
        if close[idx] > resistance:
             # Additional Check: Close must be higher than the most recent peak to be a "Valid Breakout"
             # helping to filter false breakouts inside the pattern
             if close[idx] > last_peak_val:
                 signals[idx] = 1

        return pd.Series(signals, index=df.index)

    @staticmethod
    def identify_vcp(df: pd.DataFrame) -> pd.Series:
        """
        VCP (Volatility Contraction Pattern).
        Optimized Checks.
        """
        try:
            if len(df) < 60: return pd.Series(0, index=df.index)

            close = df['close'].values
            high = df['high'].values
            low = df['low'].values
            vol = df['volume'].values
            
            # Amplitude
            amp = (high - low) / close
            
            # Use Strides or simple array slicing for speed instead of rolling which creates objects
            # Check last 60 days
            
            # Check conditions only for Last Bar
            idx = len(df) - 1
            
            # Condition 3: Breakout Today
            if vol[idx] <= 0: return pd.Series(0, index=df.index)
            
            # Vol 50 mean
            v50 = np.mean(vol[idx-50:idx])
            if vol[idx] < 2.0 * v50: return pd.Series(0, index=df.index)
            
            pct_chg = (close[idx] - close[idx-1]) / close[idx-1]
            if pct_chg < 0.03: return pd.Series(0, index=df.index)
            
            # Condition 1: Contraction
            # Check max amplitude in 3 chunks
            # Recent 10, Prev 15, Old 20
            # [idx-10:idx], [idx-25:idx-10], [idx-45:idx-25]
            
            amp_1 = np.max(amp[idx-10:idx])
            amp_2 = np.max(amp[idx-25:idx-10])
            amp_3 = np.max(amp[idx-45:idx-25])
            
            if (amp_3 > amp_2) and (amp_2 > amp_1) and (amp_1 < 0.05):
                 # Condition 2: Volume Dry Up
                 # Last 5 days mean < Last 50 days mean
                 v5 = np.mean(vol[idx-5:idx])
                 if v5 < v50:
                     signals = np.zeros(len(df), dtype=int)
                     signals[idx] = 1
                     return pd.Series(signals, index=df.index)
            
            return pd.Series(0, index=df.index)

        except Exception:
            return pd.Series(0, index=df.index)
