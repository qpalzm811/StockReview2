import sys
import os
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                             QLabel, QComboBox, QCheckBox, QFrame)
from PyQt6.QtCore import Qt, pyqtSignal

import time 

import matplotlib
# ...
# Use QtAgg for PyQt6
try:
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.backends.backend_qtagg import NavigationToolbar2QT as NavigationToolbar
except ImportError:
    from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar

import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd

class KlineChartWidget(QWidget):
    """
    K-Line Chart Widget using mplfinance.
    Supports: Day/Week/Month, Pan/Zoom.
    """
    
    def __init__(self):
        super().__init__()
        
        # Data storage
        self.raw_df = pd.DataFrame() # Original daily data
        self.current_symbol = ""
        self.current_freq = "D" # D, W, M
        
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0,0,0,0)
        
        # 1. Toolbar Frame
        toolbar_layout = QHBoxLayout()
        toolbar_layout.setContentsMargins(5, 5, 5, 5)
        
        self.lbl_title = QLabel("K线图表")
        self.lbl_title.setStyleSheet("font-weight: bold; font-size: 14px;")
        toolbar_layout.addWidget(self.lbl_title)
        
        toolbar_layout.addStretch()
        
        # Freq Switcher
        self.btn_day = QPushButton("日")
        self.btn_day.setCheckable(True)
        self.btn_day.setChecked(True)
        self.btn_day.clicked.connect(lambda: self.change_freq("D"))
        
        self.btn_week = QPushButton("周")
        self.btn_week.setCheckable(True)
        self.btn_week.clicked.connect(lambda: self.change_freq("W"))
        
        self.btn_month = QPushButton("月")
        self.btn_month.setCheckable(True)
        self.btn_month.clicked.connect(lambda: self.change_freq("M"))
        
        for btn in [self.btn_day, self.btn_week, self.btn_month]:
            btn.setFixedWidth(45) # [Fix] Increase width to show text
            btn.setStyleSheet("""
                QPushButton { background-color: #2b2b2b; color: #aaaaaa; border: 1px solid #3f3f3f; border-radius: 2px; }
                QPushButton:checked { background-color: #0078d7; color: white; border: 1px solid #0078d7; }
                QPushButton:hover { background-color: #383838; }
            """)
            toolbar_layout.addWidget(btn)
            
        layout.addLayout(toolbar_layout)
        
        # 2. Canvas
        # [Style] Futu Dark Theme
        bg_color = '#111318' # Deep Blue-Black
        self.figure = plt.figure(facecolor=bg_color)
        self.canvas = FigureCanvas(self.figure)
        self.canvas.setStyleSheet(f"background-color: {bg_color};")
        layout.addWidget(self.canvas)
        
        # [New] HUD Overlay (Top-Left on Canvas)
        self.lbl_hud = QLabel(self.canvas)
        self.lbl_hud.setStyleSheet("""
            color: #dddddd;
            font-family: 'Consolas', 'Microsoft YaHei';
            font-size: 11px;
            background-color: rgba(17, 19, 24, 0.7);
            border-radius: 4px;
            padding: 4px;
        """)
        self.lbl_hud.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents) # Let clicks through
        self.lbl_hud.move(50, 6) # Top-Left (Offset for y-axis)
        self.lbl_hud.resize(550, 20)
        self.lbl_hud.setText("")
        
        # Hidden Toolbar for Pan/Zoom
        self.toolbar = NavigationToolbar(self.canvas, self)
        self.toolbar.hide()
        # [Fix] Disable standard Pan. We implement custom Drag-Pan with Auto-Y.
        # self.toolbar.pan() 
        
        # Custom Event for Scroll Zoom & Hover
        self.canvas.mpl_connect('scroll_event', self.on_scroll)
        self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)
        # [New] Drag Events
        self.canvas.mpl_connect('button_press_event', self.on_mouse_press)
        self.canvas.mpl_connect('button_release_event', self.on_mouse_release)
        
        # Panning State
        self.is_panning = False
        self.last_mouse_x = 0
        self.last_draw_time = 0 # [Perf] Throttling
        
        # Style
        # Set dark theme params
        plt.style.use('dark_background')
        
        # [Opt] Cache Style (Futu Scheme)
        # Up: Red (#FF4455), Down: Green (#09CF98)
        mc = mpf.make_marketcolors(
            up='#FF4455', down='#09CF98', 
            edge='inherit', wick='inherit', volume='inherit'
        )
        self.mpf_style = mpf.make_mpf_style(
            base_mpf_style='nightclouds', 
            marketcolors=mc,
            facecolor=bg_color,
            edgecolor=bg_color,
            figcolor=bg_color,
            gridcolor='#2A2C35',
            gridstyle='--',
            rc={'axes.labelsize': 8, 'xtick.labelsize': 8, 'ytick.labelsize': 8}
        )
        
        # Refs
        self.ax_main = None # [Fix] Init
        self.ax_vol = None  # [Fix] Init
        self.cursor_v = None
        self.cursor_h = None
        
    def change_freq(self, freq):
        self.current_freq = freq
        # Update UI state
        self.btn_day.setChecked(freq == "D")
        self.btn_week.setChecked(freq == "W")
        self.btn_month.setChecked(freq == "M")
        
        self.update_plot()
        
    def load_data(self, df: pd.DataFrame, symbol: str, name: str = ""):
        if df.empty:
            self.clear_plot()
            return
            
        self.raw_df = df.copy()
        
        # Parse Dates
        if 'date' in self.raw_df.columns:
            self.raw_df['date'] = pd.to_datetime(self.raw_df['date'])
            self.raw_df.set_index('date', inplace=True)
            self.raw_df.sort_index(inplace=True)
            
        self.current_symbol = symbol
        self.lbl_title.setText(f"{symbol} {name}")
        
        self.update_plot()
        
    def update_plot(self):
        if self.raw_df.empty: return
        
        # Resample
        data = self.raw_df
        if self.current_freq == "W":
            data = self.raw_df.resample('W').agg({
                'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
            }).dropna()
        elif self.current_freq == "M":
            data = self.raw_df.resample('ME').agg({
                'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
            }).dropna()
            
        # [Opt] Limit visible history for performance (Max 1000 bars / ~4 years)
        if len(data) > 800:
            data = data.iloc[-800:]
            
        self.current_data = data
        self.figure.clear()
        
        # [Fix] Tight Margins ("严丝合缝")
        # Left/Top/Bottom=0 to fit edges. Right=0.92 to reserve space for Y-Axis Price Labels.
        # [Fix] Ultra Tight Margins ("紧贴右侧")
        # 0.99 leaves minimal gap. Labels might slightly overlap if too long, but fits user request for space.
        self.figure.subplots_adjust(left=0.0, right=0.99, top=1.0, bottom=0.0, hspace=0.0)
        
        # Plot with mplfinance
        # Create axes manually to control layout
        gs = self.figure.add_gridspec(2, 1, height_ratios=[3, 1], hspace=0.0)
        ax1 = self.figure.add_subplot(gs[0])
        ax2 = self.figure.add_subplot(gs[1], sharex=ax1)
        
        self.ax_main = ax1
        self.ax_vol = ax2
        
        try:
            # [Fix] Use 'tight_layout' to maximize space? No, mpf handles it.
            # returnfig=False, volume=True.
            mpf.plot(data, type='candle', ax=ax1, volume=ax2, style=self.mpf_style, warn_too_much_data=2000)
            
            # [UX] Default Zoom: Last 60 bars (Focus on recent trend)
            total_len = len(data)
            zoom_len = 60
            
            start_idx = 0
            end_idx = total_len
            
            if total_len > zoom_len:
                start_idx = total_len - zoom_len
                # Add slight padding on right (+2) for latest candle
                ax1.set_xlim(start_idx, total_len + 1)
                
            # [Fix] Dynamic Scaling (Always run based on visible range)
            visible_data = data.iloc[start_idx:end_idx]
            
            if not visible_data.empty:
                # Price Scale
                y_min = visible_data['low'].min()
                y_max = visible_data['high'].max()
                # Add padding
                pad = (y_max - y_min) * 0.05
                if pad == 0: pad = y_max * 0.01
                ax1.set_ylim(y_min - pad, y_max + pad)
                
                # Volume Scale (Auto-Fit)
                v_max = visible_data['volume'].max()
                if v_max > 0:
                     ax2.set_ylim(0, v_max * 1.2) # Reserve top 20% space
                
            # [UX] Clean Layout: Remove Volume X-Axis Labels (Date)
            # User relies on HUD for precise date-checking
            ax2.xaxis.set_visible(False) 
            ax2.set_xticks([]) 
            ax2.set_xticklabels([])
            
            # [Interactive] Init Crosshair
            self.cursor_v = ax1.axvline(x=0, color='white', linestyle='--', linewidth=0.8, alpha=0)
            self.cursor_h = ax1.axhline(y=0, color='white', linestyle='--', linewidth=0.8, alpha=0)
            
        except Exception as e:
            pass
        
        # Tweak
        ax1.set_ylabel("")
        ax2.set_ylabel("")
        
        self.canvas.draw()
        
        # Reset HUD
        self.lbl_hud.setText(f"{self.current_symbol} {self.current_freq}-Line")
        
    def clear_plot(self):
        self.figure.clear()
        self.canvas.draw()
        self.lbl_title.setText("No Data")
        self.current_data = None
        self.ax_main = None
        self.ax_vol = None

    def on_mouse_press(self, event):
        if event.button == 1:
            self.is_panning = True
            self.last_mouse_x = event.x
            # [Perf] Hide Grid & Crosshair
            if self.ax_main: 
                self.ax_main.grid(False)
                if self.cursor_v: self.cursor_v.set_visible(False)
                if self.cursor_h: self.cursor_h.set_visible(False)
            if self.ax_vol: self.ax_vol.grid(False)
            self.canvas.draw_idle()

    def on_mouse_release(self, event):
        if event.button == 1:
            self.is_panning = False
            # [Fix] Restore Grid with Correct Style (Prevent White Lines)
            grid_style = {'color': '#2A2C35', 'linestyle': '--', 'linewidth': 0.6}
            if self.ax_main: 
                self.ax_main.grid(True, **grid_style)
                # Don't auto-show crosshair, wait for movement
                # But we can set visible=True safely
            if self.ax_vol: self.ax_vol.grid(True, **grid_style)
            self.canvas.draw_idle()

    def _update_visible_limits(self):
        """Auto-scale Y-axis based on visible X-range."""
        if not self.ax_main or self.current_data is None: return
        if self.current_data.empty: return
        
        xlim = self.ax_main.get_xlim()
        start = int(max(0, xlim[0]))
        end = int(min(len(self.current_data), xlim[1]))
        
        if end <= start: return
        
        visible = self.current_data.iloc[start:end]
        if visible.empty: return
        
        # Price
        ymin = visible['low'].min()
        ymax = visible['high'].max()
        if ymin == ymax: pad = 1.0
        else: pad = (ymax - ymin) * 0.05
        self.ax_main.set_ylim(ymin - pad, ymax + pad)
        
        # Volume
        if self.ax_vol:
             vmax = visible['volume'].max()
             if vmax > 0:
                 self.ax_vol.set_ylim(0, vmax * 1.2)

    def on_mouse_move(self, event):
        """Handle mouse hover for Crosshair & Pan."""
        # 1. Handle Panning
        if self.is_panning and event.x is not None and self.ax_main:
             # [Perf] FPS Throttling (Limit to 12 FPS / 80ms)
             # Aggressive tuning for responsiveness on high-load rendering
             now = time.time()
             dt = now - self.last_draw_time
             if dt < 0.08:
                 return
                 
             dx = event.x - self.last_mouse_x
             self.last_mouse_x = event.x
             
             # Calculate scale: bars per pixel
             xlim = self.ax_main.get_xlim()
             span = xlim[1] - xlim[0]
             width = self.canvas.width()
             if width > 0:
                 scale = span / width
                 shift = dx * scale
                 self.ax_main.set_xlim(xlim[0] - shift, xlim[1] - shift)
                 
                 self._update_visible_limits()
                 
                 self.canvas.draw_idle()
                 self.last_draw_time = now
             return

        # 2. Crosshair Logic
        if event.button is not None:
            return

        if not event.inaxes or event.inaxes != self.ax_main:
            return
        if self.current_data is None or self.cursor_v is None:
            return
            
        # 1. Get Index
        try:
            # mpf treats x-axis as range(len(data))
            idx = int(round(event.xdata))
            if idx < 0 or idx >= len(self.current_data):
                return
                
            # 2. Get Row Data
            row = self.current_data.iloc[idx]
            date_str = str(row.name).split()[0] # index is datetime
            
            # 3. Update HUD
            # Format: Date | Open | High | Low | Close | Vol | Change
            # Color logic? Just text for now.
            close = row['close']
            open_p = row['open']
            chg = (close - open_p) / open_p * 100 if open_p > 0 else 0
            
            hud_text = (f"📅 {date_str}  "
                        f"O: {open_p:.2f}  H: {row['high']:.2f}  L: {row['low']:.2f}  C: {close:.2f}  "
                        f"Vol: {row['volume']//100:.0f}手  幅: {chg:+.2f}%")
                        
            self.lbl_hud.setText(hud_text)
            
            # 4. Update Crosshair
            self.cursor_v.set_xdata([event.xdata])
            self.cursor_h.set_ydata([event.ydata])
            self.cursor_v.set_alpha(0.8)
            self.cursor_h.set_alpha(0.8)
            
            self.canvas.draw_idle() # Efficient redraw
            
        except Exception as e:
            pass

    def on_scroll(self, event):
        # Basic Zoom implementation
        ax = event.inaxes
        if not ax: return
        
        # Get current limits
        cur_xlim = ax.get_xlim()
        cur_xrange = (cur_xlim[1] - cur_xlim[0]) * .5
        xdata = event.xdata # get event x location
        
        if event.button == 'up':
            # Zoom in
            scale_factor = 1 / 1.1
        elif event.button == 'down':
            # Zoom out
            scale_factor = 1.1
        else:
            scale_factor = 1
            
        new_width = (cur_xlim[1] - cur_xlim[0]) * scale_factor
        relx = (cur_xlim[1] - xdata)/(cur_xlim[1] - cur_xlim[0])
        
        ax.set_xlim([xdata - new_width * (1-relx), xdata + new_width * (relx)])
        self.canvas.draw()
