import pandas as pd
import logging
from typing import Optional

class DataFilter:
    """
    数据过滤器 (DataFilter).
    负责清洗股票池，剔除垃圾股 (ST, 退市, 流动性差, 无数据).
    """
    
    # 静态规则: 排除包含这些关键词的股票名称
    # [Fix]: Regex Error "nothing to repeat" caused by unescaped '*' in '*ST'
    BLACKLIST_KEYWORDS = ['ST', '\\*ST', '退']
    
    # 静态规则: 合法的 A 股代码前缀 (Regex)
    # 00: 深主, 30: 创业, 60: 沪主, 68: 科创
    # 剔除: 4xxx, 8xxx (北交所), 9xxx (B股)
    VALID_SYMBOL_REGEX = r'^(00|30|60|68)\d{4}$'

    @staticmethod
    def clean_stock_data(df: pd.DataFrame, market: str = 'CN', strict: bool = True) -> pd.DataFrame:
        """
        数据清洗函数 (Clean Stock Data).
        
        Args:
            df: Raw DataFrame
            market: 'CN' or 'US'
            strict: Check columns strictly? (False for Fallback API without price data)
        """
        if df.empty:
            return df
            
        df = df.copy()
        
        if market == 'CN' or market == 'A':
            # 标准化列名
            def to_num(col): return pd.to_numeric(col, errors='coerce').fillna(0)
            
            # Map common columns
            if '最新价' in df.columns: df['close'] = to_num(df['最新价'])
            if '成交额' in df.columns: df['amount'] = to_num(df['成交额'])
            if '名称' in df.columns: df['name'] = df['名称'].astype(str)
            if '代码' in df.columns: df['symbol'] = df['代码'].astype(str)
            
            # Fallback for lightweight API
            if 'name' in df.columns and 'name' not in df: df['name'] = df['name'].astype(str)
            if 'code' in df.columns and 'symbol' not in df: df['symbol'] = df['code'].astype(str)
            
            # 1. Identity Checks (Must Exist)
            if 'name' not in df.columns: df['name'] = ""
            if 'symbol' not in df.columns: return pd.DataFrame()
            
            # 2. Base Filters (Always Apply)
            # [Fix] Drop empty names (Ghost Data Root Cause)
            mask_has_name = (df['name'].notna()) & (df['name'] != "") & (df['name'] != "nan") & (df['name'] != "None")
            mask_no_st = ~df['name'].str.contains('ST|退', case=False, regex=True, na=False)
            
            mask_name = mask_has_name & mask_no_st
            mask_board = df['symbol'].str.match(r'^(00|30|60|68)\d{4}$')
            
            # 3. Price & Amount Filters (Conditional)
            mask_price = pd.Series(True, index=df.index)
            mask_amount = pd.Series(True, index=df.index)
            
            if strict:
                # [Strict Mode]
                if 'close' not in df.columns:
                    logging.warning("DataFilter: Price data missing. Strict Mode: Dropping ALL.")
                    return pd.DataFrame()
                if 'amount' not in df.columns:
                    logging.warning("DataFilter: Amount data missing. Strict Mode: Dropping ALL.")
                    return pd.DataFrame()
                    
                # Strict Price Check
                mask_price = df['close'] >= 2.0
                
                # Strict Market Cap Check (New Rule: >= 40亿)
                mask_mv = pd.Series(True, index=df.index)
                cols_mv = [c for c in df.columns if '市值' in c]
                if cols_mv:
                     mv_col = '总市值' if '总市值' in df.columns else cols_mv[0]
                     df['mv_temp'] = to_num(df[mv_col])
                     # User Request: Filter out < 40 Billion (4,000,000,000)
                     mask_mv = df['mv_temp'] >= 4_000_000_000
                
                # Strict Amount Check (Smart Logic)
                from datetime import datetime
                now = datetime.now()
                is_intraday = (now.hour == 9 and now.minute >= 25) or (now.hour >= 10 and now.hour < 15)
                
                if is_intraday:
                    # Intraday: Relaxed Amount
                    mask_amount = df['amount'] >= 20_000_000
                else:
                    # Post-Market: Strict Amount
                    mask_amount = df['amount'] >= 100_000_000
                
                # Apply Market Cap Filter to Liquidity/Amount mask
                mask_amount = mask_amount & mask_mv
            else:
                # [Lenient Mode]
                if 'close' in df.columns:
                    mask_price = df['close'] >= 2.0
                # Skip Amount check entirely
                logging.info("DataFilter: Non-Strict Mode (Fallback). Skipping missing column checks.")
            
            # Combine ALL
            final_mask = mask_name & mask_board & mask_price & mask_amount
            
            dropped = len(df) - final_mask.sum()
            if dropped > 0:
                logging.info(f"DataFilter: Dropped {dropped} stocks.")
                
            return df[final_mask]
            
        elif market == 'US':
             # ... unchanged ...
             pass
            
        return df

    @staticmethod
    def filter_stock_list(df: pd.DataFrame) -> pd.DataFrame:
        """Old wrapper for compatibility, redirects to clean_stock_data."""
        return DataFilter.clean_stock_data(df, market='CN')
        
    @staticmethod
    def check_quality(df_bars: pd.DataFrame, min_bars: int = 60, min_avg_amount: float = 10_000_000) -> bool:
        """
        动态质量检查: 针对 K 线数据.
        True = 通过 (保留), False = 拒绝 (剔除)
        [Strict Mode]: Missing Columns = REJECT.
        
        Args:
            df_bars: K 线 DataFrame (需包含 'amount', 'volume')
            min_bars: 最小数据条数 (剔除次新股)
            min_avg_amount: 最小日均成交额 (剔除僵尸股, 默认 1000万)
        """
        if df_bars is None or df_bars.empty:
            return False
            
        # [Strict] Missing 'amount' = REJECT
        if 'amount' not in df_bars.columns:
            # logging.debug("DataFilter: 'amount' column missing. Rejecting.")
            return False
            
        # 1. 长度检查 (次新股)
        if len(df_bars) < min_bars:
            return False
            
        # 2. 停牌检查 (最新一天无量)
        # 注意: 某些数据源停牌可能直接不返回当日数据，或者 Volume=0
        if 'volume' in df_bars.columns:
            last_vol = df_bars['volume'].iloc[-1]
            if last_vol <= 0:
                return False
                
        # 3. 流动性检查 (成交额)
        # 计算最后 5 天均值
        recent = df_bars.tail(5)
        
        # [Strict] If any NaN in recent amount, might indicate issues, but mean() handles skipna=True.
        # But if all NaN, mean is NaN.
        avg_amt = recent['amount'].mean()
        
        if pd.isna(avg_amt):
            return False
            
        if avg_amt < min_avg_amount:
            # logging.debug(f"DataFilter: Low liquidity ({avg_amt/10000:.0f}万 < {min_avg_amount/10000:.0f}万)")
            return False
                
        return True
