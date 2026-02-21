import logging
import time
import pandas as pd
from typing import List, Dict, Any, Optional
from PyQt6.QtCore import QObject, pyqtSignal, QThread
from model.data_nexus import DataNexus

class SentinelSignals(QObject):
    """
    哨兵服务信号定义 (Sentinel Signals).
    """
    alert_triggered = pyqtSignal(str, str) # title, message
    log = pyqtSignal(str)
    stopped = pyqtSignal()

class SentinelService(QObject):
    """
    盘中哨兵服务 (Market Sentinel Service).
    
    功能:
    1. 后台轮询自选股 (每 5 分钟).
    2. 检测异动 (急速拉升, 大单成交).
    3. 发出系统通知信号.
    """

    def __init__(self, data_nexus: DataNexus) -> None:
        super().__init__()
        self.nexus = data_nexus
        self.signals = SentinelSignals()
        self._is_running = False
        self._watchlist: List[str] = []
        self._interval = 300 # 默认 300 秒

    def set_watchlist(self, symbols: List[str]) -> None:
        """设置监控列表."""
        self._watchlist = symbols

    def start_monitoring(self) -> None:
        """
        开始监控循环 (Start Monitoring Loop).
        注意: 此方法会阻塞，必须在 QThread 中运行.
        """
        self._is_running = True
        self.signals.log.emit("哨兵服务已启动，正在后台监控...")
        
        while self._is_running:
            try:
                if not self._watchlist:
                    self.signals.log.emit("监控列表为空，等待添加...")
                    time.sleep(10)
                    continue

                self.signals.log.emit(f"开始轮询 {len(self._watchlist)} 只自选股...")
                self._poll_market()
                
            except Exception as e:
                self.signals.log.emit(f"监控轮询出错: {e}")
            
            # 等待下一次轮询 (分段 sleep 以便快速响应停止信号)
            for _ in range(self._interval):
                if not self._is_running:
                    break
                time.sleep(1)
        
        self.signals.log.emit("哨兵服务已停止.")
        self.signals.stopped.emit()

    def stop(self) -> None:
        """停止服务."""
        self._is_running = False

    def _poll_market(self) -> None:
        """执行市场轮询与异动检测."""
        try:
            # 使用 DataNexus 的新接口，自动处理 Proxy Bypass
            monitor_df = self.nexus.fetch_realtime_quotes(self._watchlist)
            
            if not monitor_df.empty:
                self._detect_rockets(monitor_df)
            else:
                self.signals.log.emit("轮询未获取到数据.")
            
        except Exception as e:
            self.signals.log.emit(f"轮询数据获取失败: {e}")

    def _detect_rockets(self, df: pd.DataFrame) -> None:
        """
        检测火箭发射 (急速拉升).
        逻辑: 涨幅 > 5% (简单演示). 
        """
        # 确保列名存在 (DataNexus 已经标准化为 change_pct)
        if 'change_pct' not in df.columns:
            return
            
        # 向量化筛选
        rockets = df[df['change_pct'] > 5.0]
        
        for row in rockets.itertuples():
            symbol = row.symbol
            # name = row.name # DataNexus standardized to 'name'
            # Note: itertuples uses attributes, ensure correct casing if needed or use dict
            # DataNexus returns 'name', 'price', 'change_pct'
            
            msg = f"{row.name} ({symbol}) 异动: 大涨 {row.change_pct}%! 现价: {row.price}"
            self.signals.alert_triggered.emit("盘中异动提醒", msg)
            self.signals.log.emit(f"检测到异动: {msg}")

class SentinelThread(QThread):
    """哨兵线程包装器"""
    def __init__(self, service: SentinelService):
        super().__init__()
        self.service = service
        
    def run(self):
        self.service.start_monitoring()
    
    def stop(self):
        self.service.stop()
        self.wait()
