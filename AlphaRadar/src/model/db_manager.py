import duckdb
import pandas as pd
from typing import Optional

class DBManager:
    """
    DuckDB 数据库管理器 (Database Manager).
    负责数据库连接与 Schema 维护。
    """
    
    def __init__(self, db_path: str = "alpha_radar.db", read_only: bool = False) -> None:
        self.db_path = db_path
        self.read_only = read_only
        if not self.read_only:
            self._init_schema()

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        获取数据库连接 (Returns a new connection).
        
        Returns:
            duckdb.DuckDBPyConnection: DuckDB 连接对象。
        """
        return duckdb.connect(self.db_path, read_only=self.read_only)

    def _init_schema(self) -> None:
        """初始化数据库 Schema (Initializes schema)."""
        con = self.get_connection()
        try:
            # Table: Stock List
            con.execute("""
                CREATE TABLE IF NOT EXISTS stock_list (
                    symbol VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    market VARCHAR, -- 'A', 'US', 'HK'
                    sector VARCHAR,
                    updated_at TIMESTAMP
                )
            """)
            
            # Table: Market Data (Re-create to ensure PK)
            # [Fix]: REMOVED 'DROP TABLE' to persist data!
            # con.execute("DROP TABLE IF EXISTS market_data") 
            con.execute("""
                CREATE TABLE IF NOT EXISTS market_data (
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    amount DOUBLE,
                    PRIMARY KEY (symbol, date)
                )
            """)
            
            # Table: Signals
            # [Migration] Ensure score_desc column exists
            # Use PRAGMA to check columns. Robust against syntax variations.
            try:
                cols = con.execute("PRAGMA table_info('signals')").fetchall()
                # cols is list of tuples: (cid, name, type, notnull, dflt_value, pk)
                col_names = [c[1] for c in cols]
                if 'score_desc' not in col_names:
                    con.execute("ALTER TABLE signals ADD COLUMN score_desc VARCHAR")
            except Exception as e:
                print(f"DB Migration Warning: {e}")
            # [Fix] Persist signals across restarts
            con.execute("CREATE SEQUENCE IF NOT EXISTS seq_signal_id START 1")
            con.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_signal_id'),
                    symbol VARCHAR,
                    signal_date TIMESTAMP,
                    signal_type VARCHAR,
                    confidence DOUBLE,
                    description VARCHAR,
                    score DOUBLE
                )
            """)
            
            # [Feature] Watchlist / Self-Select
            con.execute("""
                CREATE TABLE IF NOT EXISTS watchlist (
                    symbol VARCHAR,
                    group_name VARCHAR DEFAULT 'Default',
                    added_at TIMESTAMP,
                    sort_order INTEGER,
                    PRIMARY KEY (symbol, group_name)
                )
            """)
            
        finally:
            con.close()

    def get_database_status(self) -> dict:
        """
        获取数据库整体状态 (Global Status).
        
        Returns:
            dict: {
                'data_date': str (YYYY-MM-DD) or 'N/A',
                'sync_time': str (YYYY-MM-DD HH:MM:SS) or 'N/A',
                'stock_count': int
            }
        """
        con = self.get_connection()
        status = {
            'data_date': 'N/A',
            'sync_time': 'N/A',
            'stock_count': 0
        }
        try:
            # 1. Latest Data Date (Market Data) - "Strict Data Time"
            res_date = con.execute("SELECT MAX(date) FROM market_data").fetchone()
            if res_date and res_date[0]:
                status['data_date'] = str(res_date[0])
                
            # 2. Last Sync Time (Stock List)
            res_sync = con.execute("SELECT MAX(updated_at) FROM stock_list").fetchone()
            if res_sync and res_sync[0]:
                status['sync_time'] = str(res_sync[0])
                
            # 3. Stock Count
            res_count = con.execute("SELECT COUNT(DISTINCT symbol) FROM stock_list").fetchone()
            if res_count and res_count[0]:
                status['stock_count'] = int(res_count[0])
                
        except Exception:
            pass
        except Exception:
            pass
        finally:
            con.close()
        return status
        
    def get_stock_bars(self, symbol: str) -> pd.DataFrame:
        """获取单只股票历史K线."""
        con = self.get_connection()
        try:
            return con.execute("SELECT * FROM market_data WHERE symbol = ? ORDER BY date", (symbol,)).fetchdf()
        except:
            return pd.DataFrame()
        finally:
            con.close()

    def batch_insert_market_data(self, df: pd.DataFrame) -> None:
        """
        批量插入行情数据 (Batch Insert Market Data).
        使用 Appender 或 INSERT INTO ... SELECT
        """
        if df.empty: return
        con = self.get_connection()
        try:
            # Pandas -> DuckDB
            # Ensure columns match Table Schema
            # Table: symbol, date, open, high, low, close, volume, amount
            # DF must have these columns
            con.register('df_view', df)
            con.execute("""
                INSERT OR REPLACE INTO market_data 
                SELECT symbol, date, open, high, low, close, volume, amount FROM df_view
            """)
            con.unregister('df_view')
        except Exception as e:
            # logging.error(f"Batch Insert Error: {e}")
            pass
        finally:
            con.close()

    def save_daily_scan_results(self, signals: list) -> None:
        """
        保存每日扫描结果 (Save Daily Scan Results).
        Logic: Overwrite signals for the same day (Keep only latest run of the day).
        """
        if not signals: return
        con = self.get_connection()
        try:
            df_signals = pd.DataFrame(signals)
            now = pd.Timestamp.now()
            today_str = now.strftime('%Y-%m-%d')
            
            df_signals['signal_date'] = now
            if 'confidence' not in df_signals.columns: df_signals['confidence'] = 0.8
            if 'score' not in df_signals.columns: df_signals['score'] = 0.0
            if 'score_desc' not in df_signals.columns: df_signals['score_desc'] = ""
            
            # Map columns
            if 'type' in df_signals.columns:
                 df_signals = df_signals.rename(columns={'type': 'signal_type', 'info': 'description'})
            
            con.register('df_sig_view', df_signals)
            
            con.execute("BEGIN TRANSACTION")
            # 1. Clear today's previous results
            con.execute(f"DELETE FROM signals WHERE strftime(signal_date, '%Y-%m-%d') = '{today_str}'")
            
            # 2. Insert new
            con.execute("""
                INSERT INTO signals (symbol, signal_date, signal_type, confidence, description, score, score_desc)
                SELECT symbol, signal_date, signal_type, confidence, description, score, score_desc FROM df_sig_view
            """)
            con.execute("COMMIT")
            
            con.unregister('df_sig_view')
        except Exception as e:
            try: con.execute("ROLLBACK")
            except: pass
            print(f"Save Signal Error: {e}")
        finally:
            con.close()

    # [Compat] Alias for ScannerService
    batch_insert_signals = save_daily_scan_results

    def get_latest_scan_results(self) -> tuple[pd.DataFrame, str]:
        """
        获取最新一次扫描结果 (Get Latest Scan).
        Returns: (DataFrame, timestamp_str)
        """
        con = self.get_connection()
        try:
            # Check latest date
            res = con.execute("SELECT MAX(signal_date) FROM signals").fetchone()
            if not res or not res[0]:
                return pd.DataFrame(), ""
            
            latest_time = res[0]
            # Use subquery and JOIN for Name AND Price
            # [Fix] Use subquery to get LATEST price, avoiding date mismatch (e.g. Scan at 4AM vs Data from yesterday)
            df = con.execute("""
                SELECT s.symbol, l.name, s.signal_type as type, s.description as info, 
                       s.confidence, s.score, s.score_desc, s.signal_date,
                       (SELECT close FROM market_data WHERE symbol = s.symbol ORDER BY date DESC LIMIT 1) as price
                FROM signals s
                LEFT JOIN stock_list l ON s.symbol = l.symbol
                WHERE s.signal_date = (SELECT MAX(signal_date) FROM signals)
            """).fetchdf()
            
            return df, str(latest_time)
        except Exception:
            return pd.DataFrame(), ""
        finally:
            con.close()



    def upsert_stock_list(self, df: pd.DataFrame) -> None:
        """
        更新股票列表 (Upserts stock list).
        [Schema Evolution]: Uses CREATE OR REPLACE to auto-migrate schema.
        """
        if df.empty:
            return

        con = self.get_connection()
        try:
            # [Fix] Deduplicate upstream data
            if 'symbol' in df.columns:
                df.drop_duplicates(subset=['symbol'], inplace=True)
            
            # [Feature] Ensure Cache Columns exist (for Offline Startup)
            # If DF is from Fallback (Code/Name only), fill missing with 0/NaN
            expected_cols = ['close', 'change_pct', 'market_cap']
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = 0.0
            
            # Add Timestamp
            df['updated_at'] = pd.Timestamp.now()
            
            # 使用临时视图
            con.register('df_temp', df)
            
            # [Architecture] High Performance Schema Sync
            # Instead of fixed schema INSERT, we replace the table to match the DataFrame.
            # This allows dynamic addition of 'close', 'change_pct' columns without migration scripts.
            con.execute("CREATE OR REPLACE TABLE stock_list AS SELECT * FROM df_temp")
            
            con.unregister('df_temp')
        finally:
            con.close()

    def fetch_history_batch(self, 
                          symbols: list, 
                          days: int = 365) -> pd.DataFrame:
        """
        批量获取历史数据 (Batch Fetch History).
        优化 IO: 一次查询获取多只股票数据.
        
        Args:
            symbols (list): 股票代码列表.
            days (int): 获取最近 N 天的数据. 默认 365.
            
        Returns:
            pd.DataFrame: 大宽表或长表 (Panel Data).
                Columns: symbol, date, open, high, low, close, volume
        """
        if not symbols:
            return pd.DataFrame()
            
        con = self.get_connection()
        try:
            # 计算起始日期
            start_date = (pd.Timestamp.now() - pd.Timedelta(days=days)).strftime('%Y-%m-%d')
            
            # 使用 IN 查询 (注意: 列表过长需拆分，DuckDB 对 IN 支持较好，但仍建议分批)
            # 这里为了安全性，如果列表过大 (>1000)，最好在 Service 层分批调用此方法
            
            # 格式化 SQL INClause
            # ps: DuckDB 也可以直接用 client 参数化查询
            placeholders = ','.join(['?'] * len(symbols))
            query = f"""
                SELECT symbol, date, open, high, low, close, volume, amount
                FROM market_data
                WHERE symbol IN ({placeholders})
                  AND date >= ?
                ORDER BY symbol, date
            """
            
            params = symbols + [start_date]
            df = con.execute(query, params).fetchdf()
            return df
            
        except Exception as e:
            # 可能是列表太长?
            print(f"Batch fetch error: {e}")
            return pd.DataFrame()
        finally:
            con.close()

    def fetch_stock_list(self) -> pd.DataFrame:
        """
        从本地数据库获取股票列表 (Get local stock list).
        用于扫描器离线扫描.
        """
        con = self.get_connection()
        try:
            return con.execute("""
                SELECT *
                FROM stock_list 
                WHERE name IS NOT NULL AND name != '' AND name != 'nan' AND name != 'None'
                  AND symbol IS NOT NULL AND symbol != ''
            """).fetchdf()
        except Exception:
            return pd.DataFrame()
        finally:
            con.close()

    # --- Watchlist Methods ---
    def add_watchlist_item(self, symbol: str, group_name: str = "Default"):
        con = self.get_connection()
        try:
            now = pd.Timestamp.now()
            # Auto-increment sort_order
            max_sort = con.execute("SELECT MAX(sort_order) FROM watchlist WHERE group_name = ?", [group_name]).fetchone()[0]
            if max_sort is None: max_sort = 0
            new_sort = max_sort + 1
            
            con.execute("""
                INSERT OR REPLACE INTO watchlist (symbol, group_name, added_at, sort_order)
                VALUES (?, ?, ?, ?)
            """, [symbol, group_name, now, new_sort])
        finally:
            con.close()

    def remove_watchlist_item(self, symbol: str, group_name: str):
        con = self.get_connection()
        try:
            con.execute("DELETE FROM watchlist WHERE symbol = ? AND group_name = ?", [symbol, group_name])
        finally:
            con.close()

    def get_watchlist(self, group_name: str = None) -> pd.DataFrame:
        con = self.get_connection()
        try:
            if group_name:
                # Debug Check
                count = con.execute("SELECT COUNT(*) FROM watchlist WHERE group_name = ?", [group_name]).fetchone()[0]
                print(f"[DEBUG] DB check for group '{group_name}': {count} rows")
                
                query = """
                    SELECT w.symbol, w.group_name, w.added_at, 
                           s.name, s.sector 
                    FROM watchlist w
                    LEFT JOIN stock_list s ON w.symbol = s.symbol
                    WHERE w.group_name = ? AND w.symbol != '_META_'
                    ORDER BY w.sort_order ASC
                """
                df = con.execute(query, [group_name]).fetch_df()
                print(f"[DEBUG] DF shape: {df.shape}")
                return df
            else:
                return con.execute("SELECT * FROM watchlist WHERE symbol != '_META_' ORDER BY group_name, sort_order").fetch_df()
        except Exception as e:
            print(f"[ERROR] get_watchlist failed: {e}")
            return pd.DataFrame()
        finally:
            con.close()
            
    def get_watchlist_groups(self) -> list:
        con = self.get_connection()
        try:
            df = con.execute("SELECT DISTINCT group_name FROM watchlist ORDER BY group_name").fetch_df()
            if df.empty: return ["Default"]
            return df['group_name'].tolist()
        except:
            return ["Default"]
        finally:
            con.close()
