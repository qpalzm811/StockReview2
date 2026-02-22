import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
from PyQt6.QtCore import QObject, pyqtSignal

from model.data_nexus import DataNexus
from model.db_manager import DBManager
from model.pattern_recognizer import PatternRecognizer
import os
import concurrent.futures

def analyze_stock_worker(symbol: str, df_stock: pd.DataFrame, stock_name: str) -> List[Dict]:
    """
    单只股票分析核心逻辑 (Worker Task - MultiProcessing).
    Pure Data Processing.
    """
    try:
        # --- [Filter] Dynamic Quality Check (Layer 2) ---
        from model.data_filter import DataFilter
        if not DataFilter.check_quality(df_stock, min_bars=60):
                return []
        
        # --- [Factor & Pattern Engine] ---
        from model.technical_factors import TechnicalFactors
        from model.complex_patterns import ComplexPatterns
        from model.wyckoff_math import WyckoffMath
        
        # 1. Calculate Basic Factors
        df_stock = TechnicalFactors.add_all_factors(df_stock)
        
        # 2. Identify Wyckoff Math Patterns
        df_stock = WyckoffMath.apply(df_stock)
        
        # 3. Identify Complex Patterns
        df_stock = ComplexPatterns.detect_patterns(df_stock)
        
        # 4. Check Signals (Last Bar)
        if df_stock.empty: return []
        last_bar = df_stock.iloc[-1]
        last_close = float(last_bar['close'])
        
        found_signals_tuples = []
        
        # --- Classic Patterns ---
        if last_bar.get('pattern_double_bottom', 0) == 1:
            found_signals_tuples.append(('W-Bottom', '双底突破'))
            
        if last_bar.get('pattern_triangle', 0) == 1:
            found_signals_tuples.append(('Triangle', '收敛三角形'))
            
        if last_bar.get('pattern_vcp', 0) == 1:
                found_signals_tuples.append(('VCP', '波动收缩(VCP)'))
                
        # Check Factor Signals
        if last_bar.get('signal_kdj_rsi', 0) == 1:
            found_signals_tuples.append(('Resonance', 'KDJ+RSI共振'))
            
        if last_bar.get('signal_macd_cross', 0) == 1:
            found_signals_tuples.append(('MACD-Cross', 'MACD金叉/多头'))
        
        # [Aggregation Logic]
        # 1. Calculate Score
        score, score_desc = TechnicalFactors.calculate_composite_score(df_stock)
        
        # 2. Stricter Resonance Filtering (User Request: Less noise, higher win rate)
        num_signals = len(found_signals_tuples)
        
        is_valid = False
        if num_signals >= 2:
            # Resonance: Multiple technical signals validating each other
            is_valid = True
        elif num_signals == 1 and score >= 70:
            # Single pattern but very strong background trend/composite score
            is_valid = True
        elif num_signals == 0 and score >= 90:
            # Exceptionally top robust score even without specific pattern
            is_valid = True

        if is_valid:
            
            # Combine Info
            if found_signals_tuples:
                combined_info = " + ".join([desc for _, desc in found_signals_tuples])
                primary_type = "Multi-Signal" if num_signals > 1 else found_signals_tuples[0][0]
                
                # Prepend Resonance tag for UI highlight if multiple
                if num_signals >= 2:
                    primary_type = f"🔥 {primary_type}"
            else:
                combined_info = "极强趋势 (无特定形态)"
                primary_type = "🔥 High Score"
            
            return [{
                "symbol": symbol,
                "name": stock_name,
                "type": primary_type,
                "price": last_close,
                "info": combined_info,
                "score": score,
                "score_desc": score_desc
            }]
            
        return []
        
    except Exception:
        return []

class ScannerSignals(QObject):
    """
    扫描服务信号定义.
    """
    progress = pyqtSignal(int, int) # (当前, 总数)
    signal_found = pyqtSignal(dict) # 信号详情
    finished = pyqtSignal()         # 完成
    error = pyqtSignal(str)         # 错误信息
    log = pyqtSignal(str)           # 日志信息

class ScannerService(QObject):
    """
    智能扫描服务 (Scanner Service).
    
    负责调度全市场扫描任务.
    [Performance]: Batch IO Mode correctly implemented.
    """
    
    def __init__(self, db_manager: DBManager, data_nexus: DataNexus) -> None:
        super().__init__()
        self.db = db_manager
        self.nexus = data_nexus
        self.recognizer = PatternRecognizer()
        self.signals = ScannerSignals()
        self._is_running = False

    def run_scan(self, market: str = 'A') -> None:
        """
        执行扫描 (Execute Scan) [Multithreaded Turbo].
        
        [Performance Optimization]: 
        1. Vectorized Batch IO (DuckDB) -> Fast Read.
        2. Parallel Processing (Thread Pool 32 Threads) -> Fast Calc.
        """
        self._is_running = True
        
        # [Performance] Profiler Start
        profiler = None
        try:
            from pyinstrument import Profiler
            profiler = Profiler()
            profiler.start()
            self.signals.log.emit("性能分析器(pyinstrument) 已启动，将在扫描结束后生成报告...")
        except ImportError:
            self.signals.log.emit("提示: 未安装 pyinstrument，无法生成性能分析报告.")

        self.signals.log.emit(f"开始扫描市场: {market} (Extreme Mode 32 Threads)")
        
        try:
            # 1. 获取股票列表
            stock_list = self.db.fetch_stock_list()
            
            if stock_list.empty:
                self.signals.log.emit("本地数据库为空，请先点击[刷新股票列表]和[一键更新历史数据].")
                self.signals.finished.emit()
                return
                
            total_stocks = len(stock_list)
            self.signals.log.emit(f"获取到 {total_stocks} 只股票，开始批量分析...")
            
            # 2. 批量处理
            # [Optimization] Increased Batch Size for better DuckDB Vectorization (User Request)
            BATCH_SIZE = 500
            all_symbols = stock_list['symbol'].tolist()
            
            # Name Lookup Map for speed
            name_map = dict(zip(stock_list['symbol'], stock_list['name']))
            
            processed_count = 0
            total_market_data_loaded = 0
            
            # [Upgrade] Multi-Processing Executor
            # Bypass GIL -> 100% CPU
            cpu_cores = os.cpu_count() or 16
            self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=cpu_cores)
            self.signals.log.emit(f"核动力引擎(Multi-Processing)已启动，调用核心数: {cpu_cores}")
            
            try:
                # [Memory Optimization] Collect all signals, write ONCE at end.
                all_signals = [] 
                
                for i in range(0, total_stocks, BATCH_SIZE):
                    if not self._is_running:
                        break
                        
                    batch_symbols = all_symbols[i : i + BATCH_SIZE]
                    
                    try:
                        # 3. 批量获取历史数据 (Vectorized IO - Main Thread)
                        batch_df = self.db.fetch_history_batch(batch_symbols, days=365)
                        
                        if batch_df.empty:
                            processed_count += len(batch_symbols)
                            self.signals.progress.emit(processed_count, total_stocks)
                            continue
                            
                        total_market_data_loaded += len(batch_df)
                        
                        # 4. 并行分析 (Parallel Analysis - MP)
                        grouped = batch_df.groupby('symbol')
                        
                        futures = []
                        for symbol, df_stock in grouped:
                            if not self._is_running: break
                            
                            # Submit Task to Process Pool
                            # Must use top-level function
                            f = self.executor.submit(
                                analyze_stock_worker, 
                                symbol, 
                                df_stock.copy(), 
                                name_map.get(symbol, "Unknown")
                            )
                            futures.append(f)
                            
                        # Collect Results (Main Thread)
                        
                        for f in concurrent.futures.as_completed(futures):
                            if not self._is_running:
                                self.executor.shutdown(wait=False, cancel_futures=True)
                                break
                                
                            try:
                                found_signals = f.result()
                                if found_signals:
                                    for sig in found_signals:
                                        # UI Update (Immediate)
                                        self.signals.signal_found.emit(sig)
                                        # Buffer for Memory
                                        all_signals.append(sig)
                            except Exception as e:
                                pass
                        
                        # [Optimization] No frequent writes. Pure Memory.
                        
                    except Exception as e:
                         pass
                    
                    try:
                        processed_count += len(batch_symbols)
                        if self._is_running:
                             self.signals.progress.emit(processed_count, total_stocks)
                    except RuntimeError:
                        break # App closed
            finally:
                if self.executor:
                    self.executor.shutdown(wait=True)
                    self.executor = None
            
            # [Final Write] One IO Transaction
            if all_signals and self._is_running:
                try:
                    self.signals.log.emit(f"正在将 {len(all_signals)} 条信号批量写入数据库...")
                except RuntimeError: pass
                self.db.batch_insert_signals(all_signals)
                
            if total_market_data_loaded == 0:
                try: self.signals.log.emit("警告: 未扫描到任何有效行情数据! 请点击[一键更新历史数据]下载数据.")
                except: pass
            else:
                try: self.signals.log.emit("全市场扫描完成.")
                except: pass
            
        except Exception as e:
            try: self.signals.log.emit(f"扫描服务出错: {e}")
            except: pass
        finally:
            # [Performance] Profiler Stop
            if profiler:
                try:
                    profiler.stop()
                    os.makedirs('logs', exist_ok=True)
                    report_path = os.path.join('logs', 'scan_performance.html')
                    profiler.write_html(report_path)
                    try: self.signals.log.emit(f"性能分析报告已保存至专用目录: {report_path}")
                    except: pass
                except Exception: pass

            self._is_running = False
            try:
                self.signals.finished.emit()
            except RuntimeError:
                pass




    def save_signal(self, data: Dict[str, Any]) -> None:
        """保存信号到数据库."""
        try:
            con = self.db.get_connection()
            # SQL Keywords Uppercase
            con.execute("""
                INSERT INTO signals (symbol, signal_date, signal_type, confidence, description)
                VALUES (?, current_timestamp, ?, ?, ?)
            """, (data['symbol'], data['type'], 0.8, data['info']))
            con.close()
        except Exception as e:
            print(f"DB Error saving signal: {e}")

    def stop(self) -> None:
        """
        [Safety] 停止扫描 (Force Stop).
        立即终止所有后台线程.
        """
        self._is_running = False
        if hasattr(self, 'executor') and self.executor:
             self.executor.shutdown(wait=False, cancel_futures=True)
