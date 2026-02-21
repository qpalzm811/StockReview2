import logging
import pandas as pd
from typing import List
from datetime import datetime, timedelta
from PyQt6.QtCore import QObject, pyqtSignal
import concurrent.futures
import baostock as bs
import math

from model.db_manager import DBManager
from model.data_nexus import DataNexus

def baostock_worker(symbols: List[str]) -> List[pd.DataFrame]:
    """
    Independent Process Worker for BaoStock.
    """
    results = []
    try:
        bs.login()
        for symbol in symbols:
            try:
                # Format code
                code = f"sz.{symbol}" if symbol.startswith(('0','3','8','4')) else f"sh.{symbol}"
                if symbol.startswith(('8','4')): code = f"bj.{symbol}"
                
                # Fetch
                rs = bs.query_history_k_data_plus(
                    code,
                    "date,open,high,low,close,volume,amount",
                    # Fetch from 2020 or later. Ideally receives start_date.
                    # For simplicity in this "Reset" mode, let's fetch sufficient history.
                    start_date="2020-01-01", 
                    end_date=datetime.now().strftime("%Y-%m-%d"),
                    frequency="d", 
                    adjustflag="2"
                )
                
                if rs.error_code != '0': continue
                
                data_list = []
                while rs.next():
                    data_list.append(rs.get_row_data())
                    
                if data_list:
                    df = pd.DataFrame(data_list, columns=rs.fields)
                    df['date'] = pd.to_datetime(df['date'])
                    for c in ['open','high','low','close','volume','amount']:
                        df[c] = pd.to_numeric(df[c], errors='coerce')
                    
                    df['symbol'] = symbol
                    results.append(df)
            except:
                pass
        bs.logout()
    except:
        pass
    return results

class MaintenanceSignals(QObject):
    """维护服务信号."""
    log = pyqtSignal(str)
    progress = pyqtSignal(int, int)
    finished = pyqtSignal()

class DataMaintenanceService(QObject):
    """
    数据维护服务 (Data Maintenance Service).
    负责执行全自动化的数据增量更新 (ETL).
    """
    
    def __init__(self, db_manager: DBManager, data_nexus: DataNexus) -> None:
        super().__init__()
        self.db = db_manager
        self.nexus = data_nexus
        self.logger = logging.getLogger(__name__)
        self.signals = MaintenanceSignals()
        self._is_running = False
        self.executor = None # For external control

    def update_all_data(self) -> None:
        """
        全量更新本地数据 (New Architecture: Parallel BaoStock).
        8-Process / No Proxy / 100x Correction.
        """
        self._is_running = True
        self._safe_emit(self.signals.log, "启动极速更新模式 (8核 - BaoStock专用通道)...")
        
        try:
            # 1. Get List
            con = self.db.get_connection()
            try:
                stocks = con.execute("SELECT symbol FROM stock_list").fetchdf()
            finally:
                con.close()
            
            if stocks.empty:
                self._safe_emit(self.signals.log, "本地股票列表为空，正在初始化...")
                stock_list_df = self.nexus.fetch_stock_list('A')
                self.db.upsert_stock_list(stock_list_df)
                stocks = stock_list_df[['symbol']]
                
            targets = stocks['symbol'].tolist()
            total_targets = len(targets)
            self._safe_emit(self.signals.log, f"检测到 {total_targets} 只股票，正在分发任务...")
            
            # 2. Chunking
            # 8 Processes. Each process handles chunk_size.
            # To show progress smoother, use smaller chunks (e.g. 50 symbols).
            chunk_size = 50
            chunks = [targets[i:i + chunk_size] for i in range(0, total_targets, chunk_size)]
            
            processed_count = 0
            # max_workers = 8 (Physical Cores usually best for heavy pickling/pandas)
            max_workers = 8 
            
            # 使用 ProcessPoolExecutor
            self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)
            
            try:
                futures = {self.executor.submit(baostock_worker, chunk): chunk for chunk in chunks}
                
                for future in concurrent.futures.as_completed(futures):
                    if not self._is_running:
                        self.executor.shutdown(wait=False, cancel_futures=True)
                        break
                        
                    try:
                        results = future.result() # List[DataFrame]
                        if results:
                            # Batch Insert
                            big_df = pd.concat(results, ignore_index=True)
                            self.db.batch_insert_market_data(big_df)
                            processed_count += len(results)
                        else:
                            # Empty chunk result?
                            pass
                            
                    except Exception as e:
                        self.logger.error(f"Worker Error: {e}")
                        
                    # Update Progress
                    # Since chunk is 50, processed_count jumps by ~50.
                    self.signals.progress.emit(processed_count, total_targets)
                    self._safe_emit(self.signals.log, f"进度更新: 已处理 {processed_count}/{total_targets} 只股票...")
                    
            finally:
                if self.executor:
                    self.executor.shutdown(wait=True)
                    self.executor = None

            self._safe_emit(self.signals.log, "数据维护完成.")
            
        except Exception as e:
            self._safe_emit(self.signals.log, f"数据维护发生严重错误: {e}")
        finally:
            self._safe_emit(self.signals.finished)
            self._is_running = False

    def _safe_emit(self, signal, *args):
        """Helper to emit signals safely during shutdown."""
        try:
            signal.emit(*args)
        except RuntimeError:
            pass # C++ object deleted
        except Exception:
            pass

    def _fetch_worker_task(self, symbol: str, use_baostock: bool = True) -> tuple:
        """
        Worker Task for ThreadPool.
        """
        # [Safety] Early Exit if Stopped
        if not self._is_running:
            return symbol, pd.DataFrame()

        try:
            # 1. Check Latency Date
            latest_date = self.db.get_latest_date(symbol)
            start_date_str = "20200101"
            
            if latest_date:
                next_day = latest_date + timedelta(days=1)
                # [Optimization] Update check
                if next_day > datetime.now():
                    return symbol, pd.DataFrame() 
                start_date_str = next_day.strftime("%Y%m%d")
            
            # [Safety] Double Check before expensive IO
            if not self._is_running:
                 return symbol, pd.DataFrame()

            # 2. Fetch (Pass use_baostock control)
            df = self.nexus.fetch_bars(symbol, start_date=start_date_str, use_baostock=use_baostock)
            return symbol, df
            
        except Exception:
            return symbol, pd.DataFrame()

    def _update_single_stock(self, symbol: str) -> bool:
        """
        更新单只股票 (Incremental Update).
        Returns: True if success (or skip), False if error.
        """
        try:
            # 1. 检查本地最新日期
            latest_date = self.db.get_latest_date(symbol)
            start_date_str = None
            
            if latest_date:
                # 增量: 从最新日期的下一天开始
                next_day = latest_date + timedelta(days=1)
                if next_day > datetime.now():
                    # 已经是最新
                    return True
                start_date_str = next_day.strftime("%Y%m%d")
            else:
                # 全量: 默认 20200101
                start_date_str = "20200101"
                
            # 2. 获取数据
            df = self.nexus.fetch_bars(symbol, start_date=start_date_str)
            
            if not df.empty:
                # 3. 批量写入 DB
                self.db.batch_insert_market_data(df)
            
            return True
                
        except Exception as e:
            # 仅记录关键错误，防止刷屏
            # self.signals.log.emit(f"更新 {symbol} 失败: {e}") 
            # 可以在这里做一些简单的错误分类日志
            return False

    # ... (rest of the class)

    def stop(self) -> None:
        """
        [Safety] 强制停止 (Force Stop).
        立即终止所有后台线程.
        """
        self._is_running = False
        self.nexus.stop() # [Safety] Deep Abort
        
        # Force kill the pool if it exists
        if hasattr(self, 'executor') and self.executor:
            self.executor.shutdown(wait=False, cancel_futures=True)
