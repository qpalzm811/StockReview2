from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QSplitter, QTableWidget, QTableWidgetItem,
    QHeaderView, QTabWidget, QLineEdit, QPushButton, QMenu, QMessageBox, QLabel,
    QHeaderView, QTabWidget, QLineEdit, QPushButton, QMenu, QMessageBox, QLabel,
    QAbstractItemView, QInputDialog
)
from view.flow_layout import FlowLayout
from PyQt6.QtCore import Qt, QMimeData, QSize, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QDrag, QAction, QColor, QBrush
import pandas as pd
import logging

class WatchlistTab(QWidget):
    def __init__(self, watchlist_service, data_nexus):
        super().__init__()
        self.service = watchlist_service
        self.data_nexus = data_nexus # 用于获取行情
        self.logger = logging.getLogger(__name__)
        self.init_ui()
        
        # Signals
        self.service.watchlist_updated.connect(self.refresh_current_group)
        self.service.groups_updated.connect(self.refresh_groups)

    def init_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        layout.addWidget(self.splitter)
        
        # --- Left Panel: Watchlist ---
        self.left_panel = QWidget()
        left_layout = QVBoxLayout(self.left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(0)
        
        # 1. Group Filter Area (Flow Layout - Futu Style)
        self.group_container = QWidget()
        self.group_container.setStyleSheet("background: transparent;")
        self.group_layout = FlowLayout(self.group_container, margin=10, spacing=8)
        left_layout.addWidget(self.group_container)
        
        # 2. Watchlist Table
        self.watch_table = WatchlistTable()
        self.watch_table.dropped.connect(self.on_params_dropped)
        self.watch_table.delete_requested.connect(self.delete_selected_stocks)
        left_layout.addWidget(self.watch_table)
        
        self.splitter.addWidget(self.left_panel)
        
        # --- Right Panel: Market Source ---
        self.right_panel = QWidget()
        right_layout = QVBoxLayout(self.right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0) # Tight layout
        
        # Search
        search_layout = QHBoxLayout()
        # [Manual Refresh] Added button per user request
        self.btn_refresh = QPushButton("↻")
        self.btn_refresh.setFixedSize(28, 28)
        self.btn_refresh.setToolTip("刷新 (从服务器获取最新价格)")
        self.btn_refresh.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_refresh.clicked.connect(self.trigger_background_refresh)
        search_layout.addWidget(self.btn_refresh)
        
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("搜索代码/名称/简拼...")
        self.search_input.textChanged.connect(self.filter_source_list)
        search_layout.addWidget(QLabel("🔍"))
        search_layout.addWidget(self.search_input)
        right_layout.addLayout(search_layout)
        
        # Source Table
        self.source_table = SourceTable() # Custom for Drag
        right_layout.addWidget(self.source_table)
        
        self.splitter.addWidget(self.right_panel)
        self.splitter.setStretchFactor(0, 2)
        self.splitter.setStretchFactor(1, 1)

        # Init Data
        self.refresh_groups()
        self.load_source_data()

    def refresh_groups(self):
        # Clear existing items
        while self.group_layout.count():
            item = self.group_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        
        groups = self.service.get_groups()
        self.group_btns = {} 
        
        # Groups Buttons
        for g in groups:
            btn = QPushButton(g)
            btn.setCheckable(True)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            # Futu-style Tag Styling: Minimalist
            btn.setStyleSheet("""
                QPushButton {
                    border: 1px solid #444; 
                    border-radius: 4px; 
                    color: #bbb; 
                    padding: 4px 12px; 
                    background: #2b2b2b;
                    font-size: 9pt;
                }
                QPushButton:hover {
                    border: 1px solid #666;
                    color: #fff;
                    background: #333;
                }
                QPushButton:checked {
                    border: 1px solid #0066ff;
                    background-color: #0066ff33;
                    color: #00ffff;
                    font-weight: bold;
                }
            """)
            btn.clicked.connect(lambda checked, name=g: self.on_group_selected(name))
            self.group_layout.addWidget(btn)
            self.group_btns[g] = btn
            
        # Add (+) Button
        btn_add = QPushButton("+")
        btn_add.setToolTip("新建分组")
        btn_add.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_add.setFixedSize(24, 24)
        btn_add.setStyleSheet("""
            QPushButton {
                 border: 1px dashed #555; border-radius: 4px; color: #666; font-weight: bold; background: transparent;
            }
            QPushButton:hover {
                 border: 1px dashed #0066ff; color: #0066ff;
            }
        """)
        btn_add.clicked.connect(self.add_new_group)
        self.group_layout.addWidget(btn_add)
        
        # Del (-) Button
        btn_del = QPushButton("-")
        btn_del.setToolTip("删除当前分组")
        btn_del.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_del.setFixedSize(24, 24)
        btn_del.setStyleSheet("""
            QPushButton {
                 border: 1px solid transparent; border-radius: 4px; color: #666; font-weight: bold; background: transparent;
            }
            QPushButton:hover {
                 color: #ff4d4d;
            }
        """)
        btn_del.clicked.connect(self.delete_current_group)
        self.group_layout.addWidget(btn_del)

        # Restore Selection
        if hasattr(self, 'current_group') and self.current_group in self.group_btns:
            self.group_btns[self.current_group].setChecked(True)
        elif groups:
            self.on_group_selected(groups[0]) 

    def on_group_selected(self, group_name):
        self.current_group = group_name
        # Update UI exclusive selection
        for name, btn in self.group_btns.items():
            btn.setChecked(name == group_name)
        self.load_watchlist_data(group_name)

    def refresh_current_group(self):
        if hasattr(self, 'current_group'):
            self.load_watchlist_data(self.current_group)

    def load_watchlist_data(self, group_name):
        self.watch_table.setRowCount(0)
        self.logger.info(f"Loading data for group: {group_name}")
        df = self.service.get_stocks(group_name)
        self.logger.info(f"Fetched {len(df)} rows for group {group_name}")
        if df.empty: return
        
        self.watch_table.setRowCount(len(df))
        for row, item in df.iterrows():
            # Symbol
            self.watch_table.setItem(row, 0, QTableWidgetItem(str(item['symbol'])))
            # Name
            self.watch_table.setItem(row, 1, QTableWidgetItem(str(item['name'])))
            # Price (Mock/Latest from DB?) 
            # Ideally fetch real-time. For now, leave empty or "-"
            self.watch_table.setItem(row, 2, QTableWidgetItem("-"))
            self.watch_table.setItem(row, 3, QTableWidgetItem("-"))
            
    def load_source_data(self):
        # Async load this if possible? For now sync is fine for 5000 rows
        try:
            df = self.data_nexus.fetch_stock_list(market='A') # Need to ensure this method exists via DB
            if df.empty: return
            self.full_source_df = df
            self.filter_source_list("")
        except Exception as e:
            self.logger.error(f"Failed to load source: {e}")

    def filter_source_list(self, text):
        if not hasattr(self, 'full_source_df'): return
        
        text = text.upper().strip()
        df = self.full_source_df
        
        if text:
             df = df[
                df['symbol'].str.contains(text) | 
                df['name'].str.contains(text)
            ]
        
        # Optimize: Batch Update
        self.source_table.setUpdatesEnabled(False)
        self.source_table.setSortingEnabled(False)
        self.source_table.setRowCount(0)
        
        # Filter valid
        df = df[df['symbol'].notna() & (df['symbol'] != "")]
        
        self.source_table.setRowCount(len(df))
        
        for row_idx, (_, item) in enumerate(df.iterrows()):
            s_sym = str(item['symbol'])
            s_name = str(item['name'])
            
            # --- Symbol & Name ---
            sym_item = QTableWidgetItem(s_sym)
            sym_item.setData(Qt.ItemDataRole.UserRole, s_sym)
            self.source_table.setItem(row_idx, 0, sym_item)
            self.source_table.setItem(row_idx, 1, QTableWidgetItem(s_name))
            
            # --- Price ---
            price = item.get('close', 0)
            self.source_table.setItem(row_idx, 2, QTableWidgetItem(f"{price:.2f}"))
            
            # --- Change % ---
            pct = item.get('change_pct', 0)
            pct_item = QTableWidgetItem(f"{pct:+.2f}%")
            if pct > 0:
                pct_item.setForeground(QColor("#FF4d4d")) # Red
            elif pct < 0:
                pct_item.setForeground(QColor("#00CC00")) # Green
            self.source_table.setItem(row_idx, 3, pct_item)
            
            # --- Market Cap ---
            mv = item.get('market_cap', 0)
            mv_str = f"{mv/100000000:.2f}亿" if mv > 0 else "-"
            self.source_table.setItem(row_idx, 4, QTableWidgetItem(mv_str))

        self.source_table.setUpdatesEnabled(True)

    def on_params_dropped(self, symbol):
        if hasattr(self, 'current_group') and self.current_group:
            self.service.add_stock(symbol, self.current_group)

    def add_new_group(self):
        name, ok = QInputDialog.getText(self, "新建分组", "请输入分组名称:")
        if ok and name:
            name = name.strip()
            if not name: return
            if name in self.service.get_groups():
                 QMessageBox.warning(self, "错误", "分组名称已存在")
                 return
            self.service.add_stock("_META_", name)
            self.refresh_groups()
            self.on_group_selected(name)

    def delete_current_group(self):
        if not hasattr(self, 'current_group') or not self.current_group: return
        group_name = self.current_group
        
        reply = QMessageBox.question(self, "删除分组", 
                                   f"确定要删除分组 '{group_name}' 包含的所有内容吗?",
                                   QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        
        if reply == QMessageBox.StandardButton.Yes:
            stocks = self.service.get_stocks(group_name)
            self.service.remove_stock("_META_", group_name)
            for _, row in stocks.iterrows():
                self.service.remove_stock(row['symbol'], group_name)
            self.refresh_groups()

    def delete_selected_stocks(self, symbols):
        if not hasattr(self, 'current_group') or not self.current_group: return
        group_name = self.current_group
        
        if not symbols: return
        
        # Direct delete without confirmation
        for s in symbols:
            self.service.remove_stock(s, group_name)
        self.load_watchlist_data(group_name)

    # --- Background Refresh Logic ---
    def trigger_background_refresh(self):
        if not hasattr(self, 'refresh_thread') or not self.refresh_thread.isRunning():
            self.refresh_thread = DataRefreshThread(self.data_nexus)
            self.refresh_thread.finished.connect(self.on_refresh_finished)
            self.refresh_thread.start()
        
    def on_refresh_finished(self):
        self.logger.info("Background data refresh complete. Reloading UI.")
        self.load_source_data()

class DataRefreshThread(QThread):
    finished = pyqtSignal()
    
    def __init__(self, nexus):
        super().__init__()
        self.nexus = nexus
        
    def run(self):
        try:
            self.nexus.fetch_stock_list(market='A', force_refresh=True)
        except Exception:
            pass
        self.finished.emit()

# --- Custom Widgets for Drag & Drop ---

class SourceTable(QTableWidget):
    def __init__(self):
        super().__init__()
        self.setColumnCount(5)
        self.setHorizontalHeaderLabels(["代码", "名称", "现价", "涨幅", "市值"])
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.setDragEnabled(True) # Enable Drag
        self.verticalHeader().setVisible(False)
        self.setAlternatingRowColors(True)

    def startDrag(self, supportedActions):
        item = self.currentItem()
        if not item: return
        
        symbol = self.item(item.row(), 0).data(Qt.ItemDataRole.UserRole)
        
        mime = QMimeData()
        mime.setText(symbol)
        
        drag = QDrag(self)
        drag.setMimeData(mime)
        drag.exec(Qt.DropAction.CopyAction)

class WatchlistTable(QTableWidget):
    from PyQt6.QtCore import pyqtSignal
    dropped = pyqtSignal(str)
    delete_requested = pyqtSignal(list)
    
    def __init__(self):
        super().__init__()
        self.setColumnCount(4)
        self.setHorizontalHeaderLabels(["代码", "名称", "现价", "涨跌幅"])
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.setAcceptDrops(True)
        self.verticalHeader().setVisible(False)
        self.setAlternatingRowColors(True)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_context_menu)

    def show_context_menu(self, pos):
        menu = QMenu()
        del_action = QAction("删除选中", self)
        del_action.triggered.connect(self.emit_delete_request)
        menu.addAction(del_action)
        menu.exec(self.mapToGlobal(pos))
        
    def keyPressEvent(self, event):
        if event.key() == Qt.Key.Key_Delete:
            self.emit_delete_request()
        else:
            super().keyPressEvent(event)
            
    def emit_delete_request(self):
        rows = sorted(set(index.row() for index in self.selectedIndexes()))
        if not rows: return
        
        symbols = []
        for r in rows:
            item = self.item(r, 0)
            if item:
                symbols.append(item.text())
        self.delete_requested.emit(symbols)

    def dragEnterEvent(self, event):
        if event.mimeData().hasText():
            event.acceptProposedAction()
            
    def dragMoveEvent(self, event):
         if event.mimeData().hasText():
            event.acceptProposedAction()

    def dropEvent(self, event):
        symbol = event.mimeData().text()
        if symbol:
            self.dropped.emit(symbol)
            event.acceptProposedAction()
