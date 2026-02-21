import akshare as ak
import yfinance as yf
import pandas as pd
import logging
import time
import os
import requests_cache
from contextlib import contextmanager
from typing import Optional, Dict, Any
from functools import lru_cache

class DataNexus:
    """
    AlphaRadar 统一数据接口 (Unified Data Interface).
    
    路由请求到 AkShare (A股) 或 YFinance (美股/全球)。
    遵循 Strict MVC 架构，仅负责数据获取与标准化，不包含业务逻辑。
    
    [Caching]:
    1. HTTP Cache (requests-cache): File-based, persistent. (For Network IO reduction)
    2. Memory Cache (self._cache): RAM-based. (For Speed)
    """
    
    def __init__(self, db_manager=None) -> None:
        self.logger = logging.getLogger(__name__)
        self.db_manager = db_manager
        # 简单的 TTL 缓存字典: {key: (data, timestamp)}
        self._cache: Dict[str, Any] = {}
        self._cache_ttl = 300 # 默认 5分钟
        
        # [Concurrency] Lock for thread-unsafe libraries (BaoStock)
        import threading
        self._bs_lock = threading.Lock()
        
        # [Cache] Enable Persistent HTTP Cache (requests-cache)
        # This intercepts 'requests' used by akshare
        # Cache file: .gemini/antigravity/brain/.../akshare_cache.sqlite
        cache_dir = os.path.dirname(os.path.abspath(__file__))
        cache_path = os.path.join(cache_dir, "akshare_http_cache")
        
        # [Stability] Cache Disabled to prevent SQLite concurrency crashes
        # requests_cache.install_cache(...)
        # self.logger.info(f"HTTP Disk Cache enabled...")
        self.logger.info("Safe Mode: HTTP Disk Cache Disabled.")
        
        self._bs_batch_mode = False # Flag for persistent BaoStock session
        self._stop_requested = False # [Safety] Flag to abort fetching in lock contentions
    
    def stop(self):
        """Signal to abort ongoing fetch operations."""
        self._stop_requested = True
        
    def reset(self):
        """Reset stop signal."""
        self._stop_requested = False

    def enter_batch_mode(self):
        """Start persistent BaoStock session to avoid re-login spam."""
        try:
            import baostock as bs
            lg = bs.login()
            if lg.error_code == '0':
                self._bs_batch_mode = True
                self.logger.info("DataNexus: Entered Batch Mode (BS Session Persisted).")
            else:
                self.logger.error(f"DataNexus: Failed to enter Batch Mode: {lg.error_msg}")
        except Exception as e:
            self.logger.error(f"DataNexus: Batch Mode Init Error: {e}")

    def exit_batch_mode(self):
        """End persistent BaoStock session."""
        if self._bs_batch_mode:
            try:
                import baostock as bs
                bs.logout()
                self._bs_batch_mode = False
                self.logger.info("DataNexus: Exited Batch Mode.")
            except:
                pass

    @contextmanager
    def _temp_clear_proxy(self):
        """
        临时清除代理设置 (Context Manager).
        用于解决使用了 VPN (Clash/Tun) 等导致 System Proxy 强制开启的问题.
        [Fix]: 必需显式设置为 "" 空字符串，否则 Python 会回退查询 Windows 注册表 (System Proxy).
        """
        # [Optimization] If global bypass is active (Batch Mode), do nothing (safe & fast)
        if getattr(self, '_global_bypass_active', False):
            yield
            return

        proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy']
        stash = {}
        
        # 1. 备份当前配置
        for k in proxy_vars:
            if k in os.environ:
                stash[k] = os.environ[k]
        
        # 2. 备份 NO_PROXY
        old_no_proxy_keys = {}
        for k in ['NO_PROXY', 'no_proxy']:
            if k in os.environ:
                old_no_proxy_keys[k] = os.environ[k]

        try:
            # 3. 覆盖设置: 强制设为空，阻断 Registry Fallback
            for k in proxy_vars:
                os.environ[k] = ""
            
            # 4. 强制直连
            os.environ['NO_PROXY'] = "*"
            os.environ['no_proxy'] = "*"
            
            yield
            
        finally:
            # 5. 还原
            # 还原 NO_PROXY
            for k in ['NO_PROXY', 'no_proxy']:
                if k in old_no_proxy_keys:
                    os.environ[k] = old_no_proxy_keys[k]
                else:
                    # 如果原来没有，现在有了，要删除
                    if k in os.environ:
                        os.environ.pop(k)

            # 还原 Proxy 变量
            for k in proxy_vars:
                if k in stash:
                    os.environ[k] = stash[k]
                else:
                    # 原来没有但被我们设为 ""，需要删除
                    if k in os.environ:
                        os.environ.pop(k)

    def enter_global_proxy_bypass(self):
        """
        [Thread-Safety] 开启全局代理绕过模式.
        必须在启动多线程之前调用 (Main Thread Only).
        """
        if getattr(self, '_global_bypass_active', False):
            return

        self.logger.info("DataNexus: Entering Global Proxy Bypass Mode (Thread-Safe)...")
        self._global_bypass_active = True
        
        # Backup Global State
        self._global_proxy_stash = {}
        proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy']
        
        for k in proxy_vars:
            if k in os.environ:
                self._global_proxy_stash[k] = os.environ[k]
                os.environ[k] = "" # Clear
        
        self._global_no_proxy_stash = {}
        for k in ['NO_PROXY', 'no_proxy']:
            if k in os.environ:
                self._global_no_proxy_stash[k] = os.environ[k]
        
        os.environ['NO_PROXY'] = "*"
        os.environ['no_proxy'] = "*"

    def exit_global_proxy_bypass(self):
        """
        [Thread-Safety] 退出全局代理绕过模式.
        必须在多线程结束之后调用.
        """
        if not getattr(self, '_global_bypass_active', False):
            return
            
        self.logger.info("DataNexus: Exiting Global Proxy Bypass Mode.")
        self._global_bypass_active = False # Disable flag first
        
        # Restore NO_PROXY
        for k in ['NO_PROXY', 'no_proxy']:
            if k in self._global_no_proxy_stash:
                 os.environ[k] = self._global_no_proxy_stash[k]
            elif k in os.environ:
                 os.environ.pop(k)
                 
        # Restore Proxy Vars
        for k, v in self._global_proxy_stash.items():
            os.environ[k] = v
            
        self._global_proxy_stash = {}
        self._global_no_proxy_stash = {}

    def _get_from_cache(self, key: str) -> Any:
        if key in self._cache:
            data, ts = self._cache[key]
            if time.time() - ts < self._cache_ttl:
                return data
            else:
                del self._cache[key]
        return None

    def _set_cache(self, key: str, data: Any) -> None:
        self._cache[key] = (data, time.time())

    @lru_cache(maxsize=1)
    def fetch_stock_list(self, market: str = 'A', force_refresh: bool = False) -> pd.DataFrame:
        """
        获取指定市场的活跃股票列表.
        [Cache]: 使用 lru_cache, 只要参数不变直接返回内存数据 (TTL: 进程生命周期).
        """
        try:
            if market == 'A':
                # [Offline First] Layer 0: Check Local DB (Persistent)
                if self.db_manager and not force_refresh:
                    df_db = self.db_manager.fetch_stock_list()
                    if not df_db.empty:
                         self.logger.info(f"Loaded {len(df_db)} stocks from Local DB (Offline Mode).")
                         df_std = pd.DataFrame()
                         df_std['symbol'] = df_db['symbol'].astype(str)
                         df_std['name'] = df_db['name'].astype(str)
                         df_std['market'] = 'A'
                         df_std['sector'] = df_db.get('sector', 'Unknown')
                         
                         # [Feature] Load Cached Prices
                         df_std['close'] = df_db.get('close', 0.0)
                         df_std['change_pct'] = df_db.get('change_pct', 0.0)
                         df_std['market_cap'] = df_db.get('market_cap', 0.0)
                         
                         return df_std

                self.logger.info("Fetching A-share list from AkShare (Spot Mode for Filtering)...")
                
                # [Fix]: Use SPOT data to get Price/Amount for filtering
                # Add Retry mechanism for stability
                df = pd.DataFrame()
                use_fallback = False
                
                for attempt in range(3):
                    try:
                        with self._temp_clear_proxy():
                            df = ak.stock_zh_a_spot_em()
                        if not df.empty:
                            break
                    except Exception as e:
                        self.logger.warning(f"Spot API attempt {attempt+1}/3 failed: {e}")
                        time.sleep(2) # Wait 2s before retry
                
                if df.empty:
                    self.logger.error("Spot API failed. Switching to lightweight fallback (Name/Code only).")
                    self.logger.warning("Warning: Price/Amount filtering will be SKIPPED.")
                    try:
                        with self._temp_clear_proxy():
                             df = ak.stock_info_a_code_name()
                             use_fallback = True
                    except Exception as e:
                        self.logger.error(f"Fallback API also failed: {e}")
                        return pd.DataFrame()
                
                # --- [Filter] Strict Cleaning at Source (Layer 1) ---
                from model.data_filter import DataFilter
                # Pass RAW df to cleaner
                # [Fix]: If using fallback (Name/Code only, no Price/Amount), disable Strict Mode
                is_strict = not use_fallback
                
                df_clean = DataFilter.clean_stock_data(df, market='A', strict=is_strict)
                
                if df_clean.empty and not df.empty:
                    self.logger.warning("DataNexus: Filter dropped ALL stocks. Check Strict Mode settings.")
                    
                self.logger.info(f"Filtered stock list: {len(df)} -> {len(df_clean)}")
                
                # [Persistence] Save to DB for next startup
                if self.db_manager and not df_clean.empty:
                    try:
                        self.db_manager.upsert_stock_list(df_clean)
                    except Exception as e:
                        self.logger.error(f"Failed to save stock list to DB: {e}")
                
                # Standardize columns AFTER cleaning
                # DataFilter ensures 'symbol' and 'name' columns exist
                df_std = pd.DataFrame()
                if 'symbol' in df_clean.columns:
                    df_std['symbol'] = df_clean['symbol'].astype(str)
                else:
                    return pd.DataFrame()
                    
                if 'name' in df_clean.columns:
                    df_std['name'] = df_clean['name'].astype(str)
                else:
                    df_std['name'] = "Unknown"
                    
                df_std['market'] = 'A'
                df_std['sector'] = 'Unknown' 
                
                # [Feature] Extract additional info for Watchlist Source
                if 'close' in df_clean.columns:
                    df_std['close'] = df_clean['close']
                elif '最新价' in df_clean.columns:
                     df_std['close'] = pd.to_numeric(df_clean['最新价'], errors='coerce')
                     
                if '涨跌幅' in df_clean.columns:
                    df_std['change_pct'] = pd.to_numeric(df_clean['涨跌幅'], errors='coerce')
                
                if '总市值' in df_clean.columns:
                     df_std['market_cap'] = pd.to_numeric(df_clean['总市值'], errors='coerce')
                
                return df_std
            
            elif market == 'US':
                self.logger.warning("US Stock list fetch full scan not fully implemented yet.")
                return pd.DataFrame(columns=['symbol', 'name', 'market', 'sector'])
            
            return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error fetching stock list: {e}")
            return pd.DataFrame()

    def _get_bs_code(self, symbol: str) -> str:
        """Helper to convert symbol to BaoStock format (sh.xxxxxx, sz.xxxxxx)."""
        if symbol.startswith("6"): return f"sh.{symbol}"
        if symbol.startswith("0") or symbol.startswith("3"): return f"sz.{symbol}"
        if symbol.startswith("8") or symbol.startswith("4"): return f"bj.{symbol}"
        return f"sz.{symbol}" # Default

    def _detect_and_fix_volume_units(self, df_bs: pd.DataFrame, df_ak: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        [Adaptive Volume Unit Correction]
        Detect and fix the volume unit difference between BaoStock and AkShare
        using median overlap ratio as per Wyckoff-Reader specification.
        """
        if df_bs.empty or df_ak.empty or 'volume' not in df_bs.columns or 'volume' not in df_ak.columns:
            return df_bs, df_ak
            
        m = df_bs.merge(df_ak, on="date", how="inner", suffixes=("_bs", "_ak"))
        if m.empty:
            df_ak["volume"] = pd.to_numeric(df_ak["volume"], errors='coerce') * 100
            return df_bs, df_ak
            
        m["volume_bs"] = pd.to_numeric(m["volume_bs"], errors='coerce')
        m["volume_ak"] = pd.to_numeric(m["volume_ak"], errors='coerce')
        ratio_med = (m["volume_bs"] / m["volume_ak"]).median()
        
        df_ak["volume"] = pd.to_numeric(df_ak["volume"], errors='coerce')
        if pd.isna(ratio_med) or str(ratio_med) == "inf":
            df_ak["volume"] *= 100
        elif 90 <= ratio_med <= 110:
            df_ak["volume"] *= 100
        elif 900 <= ratio_med <= 1100:
            df_ak["volume"] *= 1000
        elif 9 <= ratio_med <= 11:
            df_ak["volume"] *= 10
            
        return df_bs, df_ak

    def fetch_bars(
        self, 
        symbol: str, 
        period: str = 'daily', 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None,
        use_baostock: bool = True
    ) -> pd.DataFrame:
        """
        获取 OHLCV K线数据 (Hybrid: BaoStock + AkShare).
        Priority: BaoStock (Stable) -> AkShare (Fallback).
        """
        # 简单判断: 6位数字为A股，否则默认为美股
        is_ashare: bool = symbol.isdigit() and len(symbol) == 6
        
        try:
            if is_ashare:
                if not start_date: start_date = "20200101"
                if not end_date:
                    from datetime import datetime
                    end_date = datetime.now().strftime("%Y%m%d")
                
                # Format dates for BaoStock (YYYY-MM-DD)
                bs_start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}" if len(start_date)==8 else start_date
                bs_end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}" if len(end_date)==8 else end_date

                # 1. Try BaoStock (Primary - Reliable & Stable)
                try:
                    if not use_baostock:
                         raise ValueError("Skipped (Turbo Mode)")
                    import baostock as bs
                    bs_code = self._get_bs_code(symbol)
                    
                    must_login = not getattr(self, '_bs_batch_mode', False)
                    
                    if must_login:
                        # With lock just in case
                        with self._bs_lock:
                            bs.login()

                    # fields="date,open,high,low,close,volume,amount"
                    data_list = []
                    with self._bs_lock:
                        # [Safety] Deep Check inside Lock
                        if getattr(self, '_stop_requested', False):
                            return pd.DataFrame()

                        rs = bs.query_history_k_data_plus(
                            bs_code,
                            "date,open,high,low,close,volume,amount",
                            start_date=bs_start, 
                            end_date=bs_end,
                            frequency="d", 
                            adjustflag="2" # [Fix] Use QFQ (Forward Adjust) for correct current price
                        )
                        
                        if rs.error_code == '0':
                            while rs.next():
                                data_list.append(rs.get_row_data())
                    
                    if must_login:
                        with self._bs_lock:
                             bs.logout()
                    
                    if data_list:
                        df_bs = pd.DataFrame(data_list, columns=rs.fields)
                        df_bs['date'] = pd.to_datetime(df_bs['date'])
                        for c in ['open','high','low','close','volume','amount']:
                            df_bs[c] = pd.to_numeric(df_bs[c], errors='coerce')
                            
                        if not df_bs.empty:
                            df_bs['symbol'] = symbol
                            df_bs = df_bs[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
                            
                            # [Hybrid Data Engine / Wyckoff-Reader Skill] 
                            # Stitch recent AkShare data to fill in missing days & apply unit correction
                            from datetime import datetime
                            latest_bs_date = df_bs['date'].max()
                            today = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
                            
                            # Check if latest date is before today, requiring AkShare to fill the gap
                            if latest_bs_date < today and (not end_date or pd.to_datetime(end_date) >= today):
                                try:
                                    ak_start = (latest_bs_date - pd.Timedelta(days=15)).strftime("%Y%m%d")
                                    ak_end = end_date if end_date else today.strftime("%Y%m%d")
                                    
                                    with self._temp_clear_proxy():
                                        df_ak = ak.stock_zh_a_hist(
                                            symbol=symbol, period="daily", start_date=ak_start, 
                                            end_date=ak_end, adjust="qfq"
                                        )
                                        
                                    if not df_ak.empty:
                                        df_ak = df_ak.rename(columns={
                                            "日期": "date", "开盘": "open", "收盘": "close", 
                                            "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount"
                                        })
                                        df_ak['date'] = pd.to_datetime(df_ak['date'])
                                        df_ak['symbol'] = symbol
                                        
                                        # Adaptive Unit Correction
                                        df_bs, df_ak = self._detect_and_fix_volume_units(df_bs, df_ak)
                                        
                                        df_ak_new = df_ak[df_ak['date'] > latest_bs_date]
                                        if not df_ak_new.empty:
                                            df_bs = pd.concat([df_bs, df_ak_new], ignore_index=True)
                                except Exception as e_hybrid:
                                    self.logger.warning(f"Hybrid stitching failed for {symbol}: {e_hybrid}")
                                    
                            return df_bs
                                
                except ValueError as ve:
                    # [Turbo Mode] Silent Skip
                    pass
                except Exception as e_bs:
                    self.logger.warning(f"BaoStock fetch failed for {symbol}: {e_bs}")
                    
                # 2. Fallback to AkShare (Secondary)
                try:
                    with self._temp_clear_proxy():
                        df = ak.stock_zh_a_hist(
                            symbol=symbol, 
                            period="daily", 
                            start_date=start_date, 
                            end_date=end_date, 
                            adjust="qfq"
                        )
                    
                    if not df.empty:
                        # Standardize columns
                        rename_map = {
                            "日期": "date",
                            "开盘": "open",
                            "收盘": "close",
                            "最高": "high",
                            "最低": "low",
                            "成交量": "volume",
                            "成交额": "amount"
                        }
                        df = df.rename(columns=rename_map)
                        
                        # Ensure correct types
                        df['date'] = pd.to_datetime(df['date'])
                        df['symbol'] = symbol
                        
                        # [Unit Correction] AkShare is Lots (100 Shares). Convert to Shares.
                        if 'volume' in df.columns:
                             df['volume'] = df['volume'] * 100
                        
                        return df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
                        
                except Exception as e_ak:
                    pass

                 # Fallback failure
                return pd.DataFrame()
            
            else:
                # US Stocks - Might NEED proxy
                self.logger.info(f"Fetching US/Global history for {symbol}")
                ticker = yf.Ticker(symbol)
                history: pd.DataFrame = ticker.history(
                    period="1y" if not start_date else None, 
                    start=start_date, 
                    end=end_date
                )
                history = history.reset_index()
                
                rename_map = {
                    "Date": "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume"
                }
                history = history.rename(columns=rename_map)
                
                # Standardize types and timezone removal if necessary
                if 'date' in history.columns:
                    history['date'] = pd.to_datetime(history['date']).dt.tz_localize(None)
                
                history['symbol'] = symbol
                cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
                return history[[c for c in cols if c in history.columns]]
                
        except Exception as e:
            # self.logger.error(f"Error fetching bars for {symbol}: {e}") # Reduce logging spam
            return pd.DataFrame()

    def fetch_financial_data(self, symbol: str) -> pd.DataFrame:
        """
        获取个股财务指标数据 (Fetch Financial Data).
        [Cache]: 启用 TTL 缓存 (5分钟).
        """
        cache_key = f"fin_{symbol}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached

        try:
            if symbol.isdigit() and len(symbol) == 6:
                # ... same logic ...
                # 这里为了演示，我们获取"实时"的估值指标
                # ak.stock_zh_a_spot_em() 其实已经包含了部分数据，
                # 但更详细的可能需要 ak.stock_a_indicator_lg(symbol="...") (部分接口可能不稳定)
                # 我们暂时使用 stock_zh_a_spot_em 过滤出该股票的最新数据作为模拟
                
                with self._temp_clear_proxy():
                    df = ak.stock_zh_a_spot_em()
                    
                df['symbol'] = df['代码'].astype(str)
                target = df[df['symbol'] == symbol]
                
                if not target.empty:
                    # 标准化字段
                    mapped = pd.DataFrame()
                    mapped['symbol'] = target['代码'].astype(str)
                    mapped['pe_ttm'] = pd.to_numeric(target.get('市盈率-动态', 0), errors='coerce')
                    mapped['pb'] = pd.to_numeric(target.get('市净率', 0), errors='coerce')
                    mapped['total_mv'] = pd.to_numeric(target.get('总市值', 0), errors='coerce')
                    
                    self._set_cache(cache_key, mapped)
                    return mapped
            
            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error fetching financials for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_stock_news(self, symbol: str, limit: int = 5) -> list:
        """
        获取个股新闻 (Fetch Stock News).
        [Cache]: 启用 TTL 缓存 (5分钟).
        """
        cache_key = f"news_{symbol}_{limit}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached
            
        try:
            self.logger.info(f"Fetching news for {symbol}...")
            
            with self._temp_clear_proxy():
                df = ak.stock_news_em(symbol=symbol)
            
            if df.empty:
                return []
                
            # 标准化
            news_list = []
            df = df.head(limit)
            
            for _, row in df.iterrows():
                item = {
                    'title': row.get('标题', '无标题'),
                    'date': row.get('发布时间', ''),
                    'source': row.get('来源', '未知'),
                    'url': row.get('文章详情链接', '')
                }
                news_list.append(item)
            
            self._set_cache(cache_key, news_list)
            return news_list
            
        except Exception as e:
            self.logger.error(f"Error fetching news for {symbol}: {e}")
            return []

    def fetch_realtime_quotes(self, symbols: list[str]) -> pd.DataFrame:
        """
        获取实时行情 (Realtime Quotes).
        用于哨兵监控.
        [Proxy Fix]: 使用 _temp_clear_proxy.
        [Fallback]: 如果全市场 Spot 失败(常见)，且数量少，则循环获取最新 K 线.
        """
        try:
            # 1. 尝试获取 A 股全市场 Spot (效率较高: 1次请求 vs N次)
            # 必须使用 bypass proxy
            try:
                with self._temp_clear_proxy():
                    df_all = ak.stock_zh_a_spot_em()
                    
                df_all['symbol'] = df_all['代码'].astype(str)
                
                # 2. 过滤
                target_df = df_all[df_all['symbol'].isin(symbols)].copy()
                
                # 3. 标准化
                result = pd.DataFrame()
                if not target_df.empty:
                    result['symbol'] = target_df['symbol']
                    result['name'] = target_df['名称']
                    result['price'] = pd.to_numeric(target_df['最新价'], errors='coerce')
                    result['change_pct'] = pd.to_numeric(target_df['涨跌幅'], errors='coerce')
                    
                return result
            
            except Exception as spot_error:
                self.logger.warning(f"Spot API failed ({spot_error}), switching to iterative fallback for {len(symbols)} stocks.")
                
                # Fallback: Loop fetch for small lists (< 20)
                if len(symbols) > 20:
                    self.logger.error("Too many symbols for fallback loop.")
                    return pd.DataFrame()
                    
                results = []
                import datetime
                # Fetch distinct date range? Just fetch recent
                today = datetime.datetime.now().strftime("%Y%m%d")
                
                with self._temp_clear_proxy():
                    for sym in symbols:
                        try:
                            # Use daily hist - returns latest available
                            # stock_zh_a_hist is very stable
                            df_hist = ak.stock_zh_a_hist(symbol=sym, period="daily", start_date="20240101", adjust="qfq")
                            if not df_hist.empty:
                                last = df_hist.iloc[-1]
                                name_row = ak.stock_info_a_code_name() # Cached inside ideally, or we assume name known?
                                # For speed, just use symbol as name or cached map. 
                                # Actually we can try get name via another call but let's keep it simple.
                                
                                # Calc daily change if not present
                                price = float(last['收盘'])
                                pct = float(last['涨跌幅']) if '涨跌幅' in last else 0.0
                                
                                results.append({
                                    'symbol': sym,
                                    'name': sym, # Name info missing in hist, acceptable for fallback
                                    'price': price,
                                    'change_pct': pct
                                })
                        except Exception as loop_e:
                             self.logger.error(f"Fallback fail for {sym}: {loop_e}")
                             
                return pd.DataFrame(results)

        except Exception as e:
            self.logger.error(f"Error fetching realtime quotes: {e}")
            return pd.DataFrame()
