from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QPushButton, QTextEdit, QStatusBar,
                             QTabWidget, QTableWidget, QTableWidgetItem, QHeaderView,
                             QSystemTrayIcon, QMenu, QStyle, QCheckBox, QSpinBox,
                             QSplitter, QComboBox)
from PyQt6.QtGui import QIcon, QAction, QColor, QFont
from PyQt6.QtCore import QThreadPool, Qt, QTimer

# 项目模块导入
import sys
import os
import logging
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from model.db_manager import DBManager
from model.data_nexus import DataNexus
from model.factor_engine import FactorEngine
from model.research_agent import ResearchAgent
from controller.worker import Worker
from controller.scanner_service import ScannerService
from controller.sentinel_service import SentinelService, SentinelThread
from controller.backtest_service import BacktestService
from controller.data_maintenance_service import DataMaintenanceService
from model.watchlist_service import WatchlistService
from view.kline_chart import KlineChartWidget
from view.watchlist_tab import WatchlistTab

# 设置日志 (中文)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class NumericTableWidgetItem(QTableWidgetItem):
    """
    Helper for correct numeric sorting in QTableWidget.
    """
    def __lt__(self, other):
        try:
            return float(self.text()) < float(other.text())
        except ValueError:
            return super().__lt__(other)

class MainWindow(QMainWindow):
    """
    AlphaRadar 主窗口.
    """
    def __init__(self):
        super().__init__()
        
        self.setWindowTitle("AlphaRadar - 智能量化终端") # 中文标题
        self.setGeometry(100, 100, 1200, 800)
        
        # 初始化核心组件
        self.db_manager = DBManager()
        self.data_nexus = DataNexus(self.db_manager)
        self.factor_engine = FactorEngine(self.data_nexus)
        self.research_agent = ResearchAgent()
        self.scanner_service = ScannerService(self.db_manager, self.data_nexus)
        self.backtest_service = BacktestService(self.data_nexus)
        self.maintenance_service = DataMaintenanceService(self.db_manager, self.data_nexus)
        self.watchlist_service = WatchlistService(self.db_manager)
        
        # 哨兵服务 (独立线程运行)
        self.sentinel_service = SentinelService(self.data_nexus)
        self.sentinel_thread = None
        
        self.threadpool = QThreadPool()
        
        logging.info(f"多线程池已启动，最大线程数: {self.threadpool.maxThreadCount()}")

        self.init_ui()
        self.init_tray_icon() # 初始化托盘

    def init_ui(self):
        # 加载样式表
        self._load_stylesheet()
        
        # 主容器
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # --- Global Header ---
        header_widget = QWidget()
        header_widget.setStyleSheet("background-color: #2b2b2b; border-bottom: 1px solid #3f3f3f;")
        header_layout = QHBoxLayout(header_widget)
        header_layout.setContentsMargins(15, 10, 15, 10)
        
        # Title/Logo area
        lbl_title = QLabel("AlphaRadar 智能终端")
        lbl_title.setStyleSheet("font-size: 14px; font-weight: bold; color: #E0E0E0;")
        header_layout.addWidget(lbl_title)
        
        header_layout.addStretch()
        
        # Global DB Status Label (All Pages Visible)
        self.lbl_global_status = QLabel("正在初始化数据状态...")
        self.lbl_global_status.setStyleSheet("""
            font-family: 'Consolas', monospace; 
            font-size: 13px; 
            color: #4CAF50; 
            font-weight: bold;
            padding: 4px 8px;
            background-color: #1e1e1e;
            border-radius: 4px;
        """)
        header_layout.addWidget(self.lbl_global_status)
        
        main_layout.addWidget(header_widget)
        # ---------------------
        
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)
        
        # 标签页 1: 实时监控
        self.tab_dashboard = QWidget()
        self.init_dashboard_tab()
        self.tabs.addTab(self.tab_dashboard, "全局监控")
        
        # [Feature] 标签页 2: 我的自选 (Watchlist)
        self.tab_watchlist = WatchlistTab(self.watchlist_service, self.data_nexus)
        self.tabs.addTab(self.tab_watchlist, "我的自选")
        
        # 标签页 3: 智能扫描
        self.tab_scanner = QWidget()
        self.init_scanner_tab()
        self.tabs.addTab(self.tab_scanner, "智能扫描器")
        
        # 标签页 4: 基本面与 AI
        self.tab_fundamental = QWidget()
        self.init_fundamental_tab()
        self.tabs.addTab(self.tab_fundamental, "AI 投研")
        
        # 标签页 5: 策略回测
        self.tab_backtest = QWidget()
        self.init_backtest_tab()
        self.tabs.addTab(self.tab_backtest, "策略回测")
        
        # 状态栏
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("系统就绪")

    def _load_stylesheet(self):
        try:
            # [Skill Applied] Theme Factory: Tech Innovation
            # A bold and modern theme with high-contrast colors.
            # Palette: Electric Blue (#0066ff), Neon Cyan (#00ffff), Dark Gray (#1e1e1e)
            
            style_sheet = """
            QMainWindow {
                background-color: #1e1e1e;
                color: #ffffff;
            }
            QWidget {
                font-family: "Segoe UI", "Microsoft YaHei";
                font-size: 10pt;
                color: #e0e0e0;
            }
            
            /* Panels & Containers */
            QTabWidget::pane {
                border: 1px solid #333333;
                background: #252526;
                border-radius: 4px;
            }
            QTabBar::tab {
                background: #2d2d30;
                color: #cccccc;
                padding: 8px 20px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
                margin-right: 2px;
            }
            QTabBar::tab:selected {
                background: #0066ff; /* Electric Blue */
                color: white;
                font-weight: bold;
            }
            
            /* Tables */
            QTableWidget {
                background-color: #252526;
                gridline-color: #333333;
                border: none;
                selection-background-color: #004c99; /* Darker Blue */
                selection-color: #ffffff;
            }
            QHeaderView::section {
                background-color: #1e1e1e;
                color: #00ffff; /* Neon Cyan Headers */
                padding: 5px;
                border: none;
                border-bottom: 2px solid #0066ff;
                font-weight: bold;
            }
            
            /* Splitter - The "Neon" Touch */
            QSplitter::handle {
                background-color: #333333;
            }
            QSplitter::handle:horizontal {
                width: 2px;
            }
            QSplitter::handle:hover {
                background-color: #00ffff; /* Neon Cyan Hover */
            }
            
            /* Inputs */
            QLineEdit, QSpinBox, QTextEdit, QComboBox {
                background-color: #333333;
                border: 1px solid #444444;
                color: #ffffff;
                padding: 4px;
                border-radius: 2px;
            }
            QLineEdit:focus {
                border: 1px solid #0066ff;
            }
            
            /* Buttons */
            QPushButton {
                background-color: #0066ff;
                color: white;
                border: none;
                padding: 6px 15px;
                border-radius: 3px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #3385ff;
            }
            QPushButton:pressed {
                background-color: #004c99;
            }
            QPushButton:disabled {
                background-color: #444444;
                color: #888888;
            }
            
            /* Scrollbars */
            QScrollBar:vertical {
                border: none;
                background: #1e1e1e;
                width: 10px;
                margin: 0px 0px 0px 0px;
            }
            QScrollBar::handle:vertical {
                background: #444444;
                min-height: 20px;
                border-radius: 5px;
            }
            QScrollBar::handle:vertical:hover {
                background: #0066ff;
            }
            """
            self.setStyleSheet(style_sheet)
            logging.info("Tech Innovation Theme (Skill) loaded successfully.")
        except Exception as e:
             logging.error(f"Failed to load stylesheet: {e}")

    def closeEvent(self, event):
        """
        窗口关闭事件 (Application Shutdown).
        确保所有后台线程被正确停止.
        """
        logging.info("Application shutting down...")
        
        # 1. 停止 ETL 服务
        if hasattr(self, 'maintenance_service'):
            self.maintenance_service.stop()
            
        # 2. 停止 扫描服务
        if hasattr(self, 'scanner_service'):
            self.scanner_service.stop()
            
        # 3. 停止 哨兵服务
        if self.sentinel_thread and self.sentinel_thread.isRunning():
            self.sentinel_thread.stop()
            self.sentinel_thread.wait(2000) # Wait up to 2s
            
        # 4. 等待线程池 (Optional, prevent crash on exit)
        self.threadpool.clear() # Remove pending
        
        logging.info("Background services stopped.")
        event.accept()

    # ... (rest of the class)



    def init_tray_icon(self):
        """初始化系统托盘图标."""
        self.tray_icon = QSystemTrayIcon(self)
        # 简单使用一个通用图标，实际应使用 logo
        self.tray_icon.setIcon(self.style().standardIcon(
            QStyle.StandardPixmap.SP_ComputerIcon
        ))
        
        # 托盘菜单
        tray_menu = QMenu()
        action_show = QAction("显示主界面", self)
        action_quit = QAction("退出", self)
        
        action_show.triggered.connect(self.show)
        action_quit.triggered.connect(QApplication.instance().quit)
        
        tray_menu.addAction(action_show)
        tray_menu.addAction(action_quit)
        
        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.show()
        
        # 连接哨兵报警信号到托盘气泡
        self.sentinel_service.signals.alert_triggered.connect(self.show_tray_notification)

    def show_tray_notification(self, title, message):
        """显示托盘气泡通知."""
        self.tray_icon.showMessage(
            title,
            message,
            QSystemTrayIcon.MessageIcon.Information,
            3000 # 3秒
        )

    def init_dashboard_tab(self):
        layout = QVBoxLayout(self.tab_dashboard)
        
        self.header_label = QLabel("AlphaRadar 量化系统")
        self.header_label.setStyleSheet("font-size: 16px; font-weight: bold; color: #333;")
        layout.addWidget(self.header_label)
        
        # 基础数据控制
        controls_layout = QHBoxLayout()
        self.btn_fetch_ashare = QPushButton("刷新股票列表")
        self.btn_fetch_ashare.clicked.connect(self.on_fetch_ashare_list)
        controls_layout.addWidget(self.btn_fetch_ashare)
        
        # --- Auto Refresh UI ---
        controls_layout.addSpacing(20)
        self.chk_auto_refresh = QCheckBox("自动刷新")
        self.chk_auto_refresh.setStyleSheet("color: #D1D4DC;") 
        self.chk_auto_refresh.toggled.connect(self.on_toggle_auto_refresh)
        controls_layout.addWidget(self.chk_auto_refresh)
        
        controls_layout.addWidget(QLabel("间隔(分):"))
        self.spin_interval = QSpinBox()
        self.spin_interval.setRange(2, 60) # Min 2 mins, Max 60 mins
        self.spin_interval.setValue(5)     # Default 5 mins
        self.spin_interval.setFixedWidth(60)
        self.spin_interval.valueChanged.connect(self.on_interval_changed)
        controls_layout.addWidget(self.spin_interval)
        # -----------------------
        
        # 数据维护按钮 (ETL)
        self.btn_update_data = QPushButton("一键更新历史数据 (ETL)")
        self.btn_update_data.setStyleSheet("background-color: #e0f7fa; color: #006064;")
        self.btn_update_data.clicked.connect(self.on_update_data)
        controls_layout.addWidget(self.btn_update_data)
        
        controls_layout.addStretch()
        layout.addLayout(controls_layout)
        
        # Timer Setup
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self.on_auto_refresh_trigger)
        
        # Initial Update
        self.update_db_info()
        
        # 哨兵控制区
        sentinel_layout = QHBoxLayout()
        sentinel_layout.addWidget(QLabel("哨兵监控:"))
        
        self.btn_start_sentinel = QPushButton("开启监控")
        self.btn_stop_sentinel = QPushButton("停止监控")
        self.btn_stop_sentinel.setEnabled(False)
        
        self.btn_start_sentinel.clicked.connect(self.on_start_sentinel)
        self.btn_stop_sentinel.clicked.connect(self.on_stop_sentinel)
        
        sentinel_layout.addWidget(self.btn_start_sentinel)
        
        # Connect signals ONCE here
        self.sentinel_service.signals.log.connect(self.log)
        
        # Connect Maintenance signals ONCE
        self.maintenance_service.signals.log.connect(self.log)
        self.maintenance_service.signals.finished.connect(lambda: self.btn_update_data.setEnabled(True))
        self.maintenance_service.signals.finished.connect(self.update_db_info) # Refresh info on finish
        
        sentinel_layout.addWidget(self.btn_stop_sentinel)
        sentinel_layout.addStretch()
        
        layout.addLayout(sentinel_layout)
        
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        layout.addWidget(self.log_output)
        
    # --- ETL 操作 ---
    def on_update_data(self):
        self.btn_update_data.setEnabled(False)
        self.log("启动数据维护任务 (增量更新)...")
        
        # 启动
        worker = Worker(self.maintenance_service.update_all_data)
        self.threadpool.start(worker)
        
    def init_scanner_tab(self):
        # Use HBox for Split View (Left: Table, Right: Chart)
        main_layout = QHBoxLayout(self.tab_scanner)
        # [Fix] Splitter (True Resizable)
        splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(splitter)
        
        # --- Left Panel: Controls + Table ---
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0,0,0,0)
        
        # 控制区
        scan_controls = QHBoxLayout()
        self.btn_scan_start = QPushButton("开始全市场扫描 (A股)")
        self.btn_scan_start.clicked.connect(self.on_start_scan)
        self.btn_scan_stop = QPushButton("停止扫描")
        self.btn_scan_stop.clicked.connect(self.on_stop_scan)
        self.btn_scan_stop.setEnabled(False)
        
        scan_controls.addWidget(self.btn_scan_start)
        scan_controls.addWidget(self.btn_scan_stop)
        scan_controls.addStretch()
        left_layout.addLayout(scan_controls)
        
        # 进度显示
        progress_layout = QHBoxLayout()
        self.scan_progress_label = QLabel("等待开始...")
        progress_layout.addWidget(self.scan_progress_label)
        
        progress_layout.addStretch()
        
        # --- Filter ---
        progress_layout.addWidget(QLabel("形态筛选:"))
        self.combo_filter = QComboBox()
        self.combo_filter.addItems([
            "全部 (All)", 
            "🔥 多重共振 (Multi-Signal)", 
            "🔥 极强趋势 (High Score)",
            "双底突破 (W-Bottom)",
            "收敛三角形 (Triangle)",
            "波动收缩 (VCP)",
            "KDJ+RSI共振 (Resonance)",
            "MACD金叉 (MACD-Cross)"
        ])
        self.combo_filter.currentTextChanged.connect(self.on_filter_changed)
        progress_layout.addWidget(self.combo_filter)
        
        progress_layout.addSpacing(10)
        
        # [New] Filter Time Label
        self.lbl_scan_time = QLabel("筛选时间: --")
        self.lbl_scan_time.setStyleSheet("color: #888888; font-size: 11px;")
        progress_layout.addWidget(self.lbl_scan_time)
        
        left_layout.addLayout(progress_layout)
        
        # 结果表格
        self.signal_table = QTableWidget()
        self.signal_table.setAlternatingRowColors(True)
        self.signal_table.setColumnCount(7)
        # [UX] Shorter Headers for compactness
        self.signal_table.setHorizontalHeaderLabels(["代码", "名称", "形态", "价格", "评分", "详情", "评分解析"])
        
        # [Fix] Adaptive Column Widths (True Excel Style)
        header = self.signal_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        header.setStretchLastSection(False) # Allow last column to contain long text without auto-stretching empty space
        
        self.signal_table.setSortingEnabled(True)
        self.signal_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.signal_table.itemClicked.connect(self.on_scanner_table_click)
        left_layout.addWidget(self.signal_table)
        
        # Add to Splitter instead of Layout
        splitter.addWidget(left_panel)
        
        # --- Right Panel: K-Line Chart ---
        self.kline_chart = KlineChartWidget()
        splitter.addWidget(self.kline_chart)
        
        # Set Initial Ratio
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 7)
        
        # [Auto-Load] Load latest scan results
        self.load_scan_history()
        
    def on_filter_changed(self, text):
        """Filter the table rows based on the selected combobox text."""
        filter_keyword = ""
        if "(" in text and ")" in text:
            filter_keyword = text.split("(")[1].split(")")[0]
            
        if filter_keyword == "All":
            # show all
            for row in range(self.signal_table.rowCount()):
                self.signal_table.setRowHidden(row, False)
            return
            
        # Hide rows that don't match the keyword in the Type column
        for row in range(self.signal_table.rowCount()):
            type_item = self.signal_table.item(row, 2)
            if type_item:
                cell_text = type_item.text()
                if filter_keyword in cell_text:
                    self.signal_table.setRowHidden(row, False)
                else:
                    self.signal_table.setRowHidden(row, True)
                    
    def on_scanner_table_click(self, item):
        row = item.row()
        symbol_item = self.signal_table.item(row, 0)
        name_item = self.signal_table.item(row, 1)
        
        if not symbol_item: return
        
        symbol = symbol_item.text()
        name = name_item.text() if name_item else ""
        
        # Fetch Data Async or Sync? 
        # DB fetch is fast enough for Sync usually (local DuckDB)
        # But let's use Worker to be safe/smooth
        worker = Worker(self.db_manager.get_stock_bars, symbol)
        worker.signals.result.connect(lambda df: self.kline_chart.load_data(df, symbol, name))
        self.threadpool.start(worker)
        
    def init_fundamental_tab(self):
        layout = QVBoxLayout(self.tab_fundamental)
        
        # 输入区
        input_layout = QHBoxLayout()
        input_layout.addWidget(QLabel("股票代码:"))
        self.input_symbol = QTextEdit()
        self.input_symbol.setFixedSize(100, 30)
        self.input_symbol.setText("000001") # 默认测试
        input_layout.addWidget(self.input_symbol)
        
        self.btn_analyze = QPushButton("开始 AI 分析")
        self.btn_analyze.clicked.connect(self.on_analyze_stock)
        input_layout.addWidget(self.btn_analyze)
        input_layout.addStretch()
        layout.addLayout(input_layout)
        
        # 结果展示区
        # 分左右两栏: 左边基本面数据，右边 AI 报告
        result_layout = QHBoxLayout()
        
        # 左栏
        self.lbl_fund_metrics = QLabel("基本面指标将显示在这里")
        self.lbl_fund_metrics.setStyleSheet("background-color: #f0f0f0; padding: 10px;")
        self.lbl_fund_metrics.setFixedWidth(300)
        self.lbl_fund_metrics.setAlignment(Qt.AlignmentFlag.AlignTop)
        result_layout.addWidget(self.lbl_fund_metrics)
        
        # 右栏
        self.txt_ai_report = QTextEdit()
        self.txt_ai_report.setPlaceholderText("AI 交易计划书将显示在这里...")
        self.txt_ai_report.setReadOnly(True)
        result_layout.addWidget(self.txt_ai_report)
        
        layout.addLayout(result_layout)

    def log(self, message):
        self.log_output.append(message)
        logging.info(message)
        
    # --- Auto Refresh Logic ---
    def on_toggle_auto_refresh(self, checked):
        if checked:
            minutes = self.spin_interval.value()
            ms = minutes * 60 * 1000
            self.refresh_timer.start(ms)
            self.log(f"自动刷新已开启，间隔: {minutes} 分钟")
            self.spin_interval.setEnabled(False) # Lock interval while running
        else:
            self.refresh_timer.stop()
            self.log("自动刷新已停止")
            self.spin_interval.setEnabled(True)

    def on_interval_changed(self, value):
        # Only effective if not running, UI is locked if running anyway
        pass

    def update_db_info(self):
        """更新数据库信息显示 (Update Global Header)."""
        status = self.db_manager.get_database_status()
        d_date = status.get('data_date', 'N/A')
        sync_time = status.get('sync_time', 'N/A')
        count = status.get('stock_count', 0)
        
        # Format for Global Header
        # Use simple icons or text
        display_text = f"📅 行情日期: {d_date}   ⏱️ 最近更新: {sync_time}   📊 股票: {count}"
        
        self.lbl_global_status.setText(display_text)
        self.lbl_global_status.setToolTip(f"数据源状态:\n最新K线日期: {d_date}\n最后操作时间: {sync_time}\n本地股票总数: {count}")
        
    def on_auto_refresh_trigger(self):
        self.log(">>> 自动刷新触发 <<<")
        self.update_db_info() # Refresh info
        # self.on_fetch_ashare_list() # Maybe don't auto-fetch list every 5 mins? Just refresh UI info.
        # Original code called on_fetch_ashare_list. I will keep it if it was intended.
        # But 'Auto Refresh' in dashboard usually means refreshing status/list.
        # Let's keep original behavior but ADDD `update_db_info`.
        self.on_fetch_ashare_list()
    # --- 仪表盘 操作 ---
    def on_fetch_ashare_list(self):
        self.log("开始异步获取 A 股列表...")
        self.btn_fetch_ashare.setEnabled(False)
        worker = Worker(self.data_nexus.fetch_stock_list, market='A')
        worker.signals.result.connect(self.handle_stock_list_result)
        worker.signals.finished.connect(lambda: self.btn_fetch_ashare.setEnabled(True))
        self.threadpool.start(worker)

    def handle_stock_list_result(self, df):
        if not df.empty:
            self.log(f"成功获取 {len(df)} 只股票 (已自动剔除 ST/退市/非主板).")
            self.log("正在保存至本地 DuckDB...")
            worker = Worker(self.db_manager.upsert_stock_list, df)
            worker.signals.finished.connect(lambda: self.log("数据库更新完成."))
            self.threadpool.start(worker)
        else:
            self.log("获取失败或列表为空.")

    def on_test_history(self):
        symbol = "000001"
        self.log(f"正在获取 {symbol} 历史数据...")
        worker = Worker(self.data_nexus.fetch_bars, symbol=symbol)
        worker.signals.result.connect(self.handle_history_result)
        self.threadpool.start(worker)
        
    def handle_history_result(self, df):
        if not df.empty:
            self.log(f"获取了 {len(df)} 根 K 线.")
            self.log(f"{df.tail().to_string()}")
        else:
            self.log("未获取到数据.")

    # --- 哨兵服务 操作 ---
    def on_start_sentinel(self):
        # 设置监控列表 (测试用)
        # 实际应从数据库或 UI 选择
        test_watchlist = ["000001", "600519", "601318"]
        self.sentinel_service.set_watchlist(test_watchlist)
        
        self.sentinel_thread = SentinelThread(self.sentinel_service)
        # 信号连接已移至 init_dashboard_tab，防止重复连接
        
        self.sentinel_thread.start()
        
        self.btn_start_sentinel.setEnabled(False)
        self.btn_stop_sentinel.setEnabled(True)
        
    def on_stop_sentinel(self):
        if self.sentinel_thread:
            self.sentinel_thread.stop()
            self.btn_start_sentinel.setEnabled(True)
            self.btn_stop_sentinel.setEnabled(False)

    # --- 扫描器 操作 ---
    def on_start_scan(self):
        self.btn_scan_start.setEnabled(False)
        self.btn_scan_stop.setEnabled(True)
        self.scan_progress_label.setText("扫描初始化中...")
        self.signal_table.setRowCount(0) # 清空旧数据
        
        # Reset filter to "All" on new scan
        self.combo_filter.setCurrentIndex(0)
        
        # 连接信号
        self.scanner_service.signals.log.connect(self.log)
        self.scanner_service.signals.progress.connect(self.update_scan_progress)
        self.scanner_service.signals.signal_found.connect(self.add_signal_row)
        self.scanner_service.signals.finished.connect(self.on_scan_finished)
        
        # 启动后台线程
        worker = Worker(self.scanner_service.run_scan, market='A')
        self.threadpool.start(worker)
        
    def on_stop_scan(self):
        self.scanner_service.stop()
        self.log("正在停止扫描...")
        
    def on_scan_finished(self):
        self.btn_scan_start.setEnabled(True)
        self.btn_scan_stop.setEnabled(False)
        self.scan_progress_label.setText("扫描已结束.")
        self.log("扫描流程结束.")
        
        # [Fix] Don't re-save from UI. valid results already saved by Scanner Service.
        # This prevents overwriting rich DB data with partial UI data.
        # self.save_current_scan_results()
        # Update Time Label
        now_str = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        self.lbl_scan_time.setText(f"筛选时间: {now_str}")

    def save_current_scan_results(self):
        self.log("正在保存筛选结果...")
        signals = []
        rows = self.signal_table.rowCount()
        for r in range(rows):
            symbol_item = self.signal_table.item(r, 0)
            if not symbol_item: continue
            symbol = symbol_item.text()
            
            type_item = self.signal_table.item(r, 2)
            sType = type_item.text() if type_item else ""
            
            score_item = self.signal_table.item(r, 4)
            score = float(score_item.text()) if score_item else 0.0
            
            info_item = self.signal_table.item(r, 5)
            info = info_item.text() if info_item else ""
            
            # [Fix] Capture Score Desc
            desc_item = self.signal_table.item(r, 6)
            sDesc = desc_item.text() if desc_item else ""
            
            signals.append({
                'symbol': symbol,
                'signal_type': sType,
                'description': info,
                'score': score,
                'score_desc': sDesc,
                'confidence': 0.8
            })
        
        worker = Worker(self.db_manager.save_daily_scan_results, signals)
        worker.signals.finished.connect(lambda: self.log("筛选结果已保存."))
        self.threadpool.start(worker)

    def load_scan_history(self):
        """Restore last session results."""
        self.log("正在加载历史筛选结果...")
        worker = Worker(self.db_manager.get_latest_scan_results)
        worker.signals.result.connect(self.handle_history_loaded)
        self.threadpool.start(worker)
        
    def handle_history_loaded(self, result):
        df, timestamp = result
        if df.empty: return
        
        self.signal_table.setRowCount(0)
        self.signal_table.setSortingEnabled(False)
        for _, row in df.iterrows():
            # [Fix] Handle price from DB join
            p = row.get('price', 0)
            if pd.isna(p): p = 0
            
            data = {
                'symbol': row['symbol'],
                'name': row.get('name', ''), # Join handles name
                'type': row['type'],
                'price': p, 
                'score': row['score'],
                'info': row['info'],
                'score_desc': row.get('score_desc', '') # [Fix] Map score_desc
            }
            self.add_signal_row(data)
        self.signal_table.setSortingEnabled(True)
        
        try:
            ts_str = str(timestamp).split('.')[0]
            self.lbl_scan_time.setText(f"筛选时间: {ts_str}")
            self.scan_progress_label.setText(f"已加载历史记录 ({len(df)} 条)")
        except:
            pass

    def update_scan_progress(self, current, total):
        self.scan_progress_label.setText(f"扫描进度: {current} / {total}")
        
    def add_signal_row(self, data):
        # [Guard] Prevent Empty/Ghost Rows
        if not data or not data.get('symbol'):
            return
        # Filter invalid price
        try:
            p = float(data.get('price', 0))
            # [Fix] Allow price=0 (for history loading where price might be missing)
            if p < 0 or p != p: # p!=p checks for NaN
                return
        except:
             return
        
        # [Fix] Deduplicate: Check if symbol exists
        symbol = str(data['symbol'])
        row = -1
        for r in range(self.signal_table.rowCount()):
            item = self.signal_table.item(r, 0)
            if item and item.text() == symbol:
                row = r
                break
        
        if row == -1:
            row = self.signal_table.rowCount()
            self.signal_table.insertRow(row)
            self.signal_table.setItem(row, 0, QTableWidgetItem(symbol))
            
        # Update columns (whether new or existing)
        name = str(data.get('name', '')).strip() or "Unknown"
        self.signal_table.setItem(row, 1, QTableWidgetItem(name))
        self.signal_table.setItem(row, 2, QTableWidgetItem(str(data['type'])))
        
        # Numeric Sort for Price
        self.signal_table.setItem(row, 3, NumericTableWidgetItem(f"{data['price']:.2f}"))
        
        # [New] AI Score - Numeric Sort
        score = data.get('score', 0.0)
        item_score = NumericTableWidgetItem(f"{score:.1f}")
        item_score.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        if score > 80:
            item_score = QTableWidgetItem(f"{score:.1f}")
        if score >= 80:
             item_score.setForeground(QColor("#FF4455"))
             item_score.setFont(QFont("Arial", 9, QFont.Weight.Bold))
        
        self.signal_table.setItem(row, 4, item_score)
        self.signal_table.setItem(row, 5, QTableWidgetItem(str(data.get('info', ''))))
        self.signal_table.setItem(row, 6, QTableWidgetItem(str(data.get('score_desc', ''))))
        
        # self.signal_table.setSortingEnabled(sorting)
        
    # --- AI 投研 操作 ---
    def on_analyze_stock(self):
        symbol = self.input_symbol.toPlainText().strip()
        if not symbol:
            return
            
        self.btn_analyze.setEnabled(False)
        self.txt_ai_report.setText("AI 正在思考中... (可能需要几秒钟)")
        
        # 启动后台任务: 串行获取基本面 + AI 生成
        worker = Worker(self._run_analysis_pipeline, symbol)
        worker.signals.result.connect(self.handle_analysis_result)
        worker.signals.finished.connect(lambda: self.btn_analyze.setEnabled(True))
        self.threadpool.start(worker)
        
    def _run_analysis_pipeline(self, symbol):
        # 1. 获取基本面
        metrics = self.factor_engine.get_valuation_metrics(symbol)
        safety = self.factor_engine.assess_safety(metrics)
        
        fund_text = f"PE (TTM): {metrics.get('PE_TTM', 'N/A')}\n" \
                    f"PB: {metrics.get('PB', 'N/A')}\n" \
                    f"总市值: {metrics.get('Total_MV', 0)/100000000:.2f} 亿\n" \
                    f"评级: {safety}"
        
        # 2. 获取新闻情报 (News RAG)
        # 串行获取，可能稍微增加等待时间
        news_list = self.data_nexus.fetch_stock_news(symbol, limit=5)
                    
        # 3. 生成报告
        tech_text = "日线级别均线多头排列，量能温和放大。(系统根据 K 线自动生成)"
        
        report = self.research_agent.generate_report(
            symbol, 
            symbol, 
            tech_text, 
            fund_text,
            news_context=news_list
        )
        
        return fund_text, report
        
    def handle_analysis_result(self, result):
        fund_text, report = result
        self.lbl_fund_metrics.setText(fund_text)
        self.txt_ai_report.setText(report)
        
    # --- 策略回测 操作 ---
    def init_backtest_tab(self):
        layout = QVBoxLayout(self.tab_backtest)
        
        # 控制栏
        controls = QHBoxLayout()
        controls.addWidget(QLabel("回测标的:"))
        self.bt_symbol_input = QTextEdit()
        self.bt_symbol_input.setFixedSize(100, 30)
        self.bt_symbol_input.setText("000001")
        controls.addWidget(self.bt_symbol_input)
        
        self.btn_run_backtest = QPushButton("运行双底策略回测")
        self.btn_run_backtest.clicked.connect(self.on_run_backtest)
        controls.addWidget(self.btn_run_backtest)
        controls.addStretch()
        layout.addLayout(controls)
        
        # 结果输出
        self.bt_output = QTextEdit()
        self.bt_output.setReadOnly(True)
        self.bt_output.setStyleSheet("font-family: Consolas; font-size: 10pt;")
        layout.addWidget(self.bt_output)
        
    def on_run_backtest(self):
        symbol = self.bt_symbol_input.toPlainText().strip()
        if not symbol:
            return
            
        self.btn_run_backtest.setEnabled(False)
        self.bt_output.setText(f"正在回测 {symbol}，可能需要下载历史数据，请稍候...")
        
        # 连接信号
        self.backtest_service.signals.result.connect(self.handle_backtest_result)
        self.backtest_service.signals.log.connect(self.log) # 同时也输出到 Log 栏
        self.backtest_service.signals.finished.connect(lambda: self.btn_run_backtest.setEnabled(True))
        
        # 启动
        worker = Worker(self.backtest_service.run_backtest, 
                        symbol=symbol, 
                        initial_cash=100000.0)
        self.threadpool.start(worker)
        
    def handle_backtest_result(self, report):
        self.bt_output.setText(report)


def main():
    app = QApplication(sys.argv)
    
    # Optional styling
    app.setStyle("Fusion")
    
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
