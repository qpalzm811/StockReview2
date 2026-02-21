import backtrader as bt
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from PyQt6.QtCore import QObject, pyqtSignal

from model.data_nexus import DataNexus
from model.strategies import DoubleBottomStrategy
from model.backtest_adapter import AlphaRadarPandasData
from model.pattern_recognizer import PatternRecognizer

class BacktestSignals(QObject):
    """回测服务信号."""
    log = pyqtSignal(str)
    result = pyqtSignal(str) # 回测结果文本
    finished = pyqtSignal()
    error = pyqtSignal(str)

class BacktestService(QObject):
    """
    回测服务 (Backtest Service).
    负责:
    1. 准备数据 (DataNexus -> Pandas -> Backtrader Feed).
    2. 生成历史信号 (Historical Signal Generation).
    3. 运行 Cerebro 引擎.
    4. 收集绩效指标.
    """
    
    def __init__(self, data_nexus: DataNexus) -> None:
        super().__init__()
        self.nexus = data_nexus
        self.recognizer = PatternRecognizer()
        self.signals = BacktestSignals()

    def run_backtest(self, 
                     symbol: str, 
                     initial_cash: float = 100000.0,
                     start_date: str = '',
                     end_date: str = '',
                     use_kelly: bool = False) -> None:
        """
        执行回测 (Execute Backtest).
        应在 Worker 线程运行.
        """
        self.signals.log.emit(f"启动回测: {symbol}, 资金: {initial_cash}, 风控: {use_kelly}")
        
        try:
            # 1. 获取数据
            df = self.nexus.fetch_bars(symbol, start_date=start_date, end_date=end_date)
            if df.empty:
                raise ValueError("未获取到历史数据")
                
            self.signals.log.emit(f"数据加载成功: {len(df)} 根 K 线")
            
            # 2. 信号生成 (Historical Pattern Detection)
            trade_signals = self._generate_historical_signals(df)
            self.signals.log.emit(f"历史上共识别出 {len(trade_signals)} 次买入信号")
            
            # 3. 配置 Cerebro
            cerebro = bt.Cerebro()
            
            # 添加策略 (注入信号 + 风控配置)
            cerebro.addstrategy(DoubleBottomStrategy, 
                                signals=trade_signals,
                                use_kelly=use_kelly)
            
            # 添加数据
            data = AlphaRadarPandasData(dataname=df, symbol=symbol)
            cerebro.adddata(data)
            
            # 设置资金与佣金
            cerebro.broker.setcash(initial_cash)
            cerebro.broker.setcommission(commission=0.0003) # 万三佣金
            
            # 添加分析器
            cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
            cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
            cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade')
            
            # 4. 运行
            self.signals.log.emit("Cerebro 引擎正在运行...")
            results = cerebro.run()
            strat = results[0]
            
            # 5. 格式化结果
            final_value = cerebro.broker.getvalue()
            pnl = final_value - initial_cash
            ret_pct = (pnl / initial_cash) * 100
            
            # 提取分析指标
            sharpe_info = strat.analyzers.sharpe.get_analysis()
            sharpe = sharpe_info.get('sharperatio', 0.0)
            
            dd_info = strat.analyzers.drawdown.get_analysis()
            max_dd = dd_info.get('max', {}).get('drawdown', 0.0)
            
            trade_info = strat.analyzers.trade.get_analysis()
            total_trades = trade_info.get('total', {}).get('total', 0)
            won_trades = trade_info.get('won', {}).get('total', 0)
            win_rate = (won_trades / total_trades * 100) if total_trades > 0 else 0.0
            
            report = (
                f"=== 回测报告 ({symbol}) ===\n"
                f"初始资金: {initial_cash:,.2f}\n"
                f"最终净值: {final_value:,.2f}\n"
                f"净利润:   {pnl:+,.2f} ({ret_pct:.2f}%)\n"
                f"----------------------\n"
                f"交易次数: {total_trades}\n"
                f"胜率:     {win_rate:.1f}%\n"
                f"夏普比率: {sharpe:.3f}\n"
                f"最大回撤: {max_dd:.2f}%\n"
            )
            
            self.signals.result.emit(report)
            self.signals.log.emit("回测完成.")
            
        except Exception as e:
            import traceback
            err_msg = f"回测异常: {str(e)}\n{traceback.format_exc()}"
            self.signals.error.emit(err_msg)
        finally:
            self.signals.finished.emit()

    def _generate_historical_signals(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        生成历史信号 (Generate Historical Signals).
        使用向量化方法快速寻找潜在形态，然后验证。
        """
        signals = []
        
        # 计算基础指标
        df = self.recognizer.calculate_indicators(df)
        
        # 寻找局部低点 (Vectorized)
        # Shift 比较: Low[i] < Low[i-1] AND Low[i] < Low[i+1]
        # 注意: 这种简单的局部低点可能非常多，我们可能需要更宽的窗口，例如 argrelextrema(order=5)
        # 这里为了演示简单使用 shift(1) 和 shift(-1)
        lows = df['low']
        is_low = (lows < lows.shift(1)) & (lows < lows.shift(-1))
        local_min_idxs = df.index[is_low].tolist()
        
        if len(local_min_idxs) < 2:
            return []
            
        # 遍历低点对，寻找双底
        # 这是一个 O(N^2) 的操作，但在低点稀疏的情况下是可以接受的
        # 为了优化，只查看距离在 [10, 60] 范围内的低点对
        
        # 转换为 (index_loc, low_val, date) 的列表以便快速操作
        # 注意: local_min_idxs 是 DataFrame 的 Index。如果 reset_index 过，则是整数索引。
        # DataNexus fetch_bars 通常会 reset_index，所以这里 idx 是整数行号
        
        minima_points = []
        for idx in local_min_idxs:
            minima_points.append({
                'idx': idx, 
                'val': df.at[idx, 'low'],
                'atr': df.at[idx, 'atr_14'],
                'date': df.at[idx, 'date']
            })
            
        for i in range(len(minima_points)):
            for j in range(i + 1, len(minima_points)):
                p1 = minima_points[i]
                p2 = minima_points[j]
                
                dist = p2['idx'] - p1['idx']
                
                # 距离过滤
                if dist < 10:
                    continue
                if dist > 80: # 超过一定时间就不算双底了
                    break
                    
                # 能够使用 ATR 进行判断吗？
                # 使用 p2 时的 ATR
                atr = p2['atr']
                if pd.isna(atr):
                    continue
                    
                # 1. 两个底价差 < 0.5 * ATR
                if abs(p1['val'] - p2['val']) > (0.5 * atr):
                    continue
                    
                # 2. 颈线检查
                # idxmax 在切片 range 中
                neckline_slice = df.iloc[p1['idx']:p2['idx']]
                if neckline_slice.empty:
                    continue
                neck_high = neckline_slice['high'].max()
                
                avg_bottom = (p1['val'] + p2['val']) / 2
                
                # 颈线高度检查
                if (neck_high - avg_bottom) < (2.0 * atr):
                    continue
                    
                # 3. 突破检查
                # 在 p2 之后寻找突破颈线的点
                # 搜索范围: p2 之后 10 天内
                search_end = min(p2['idx'] + 10, len(df))
                breakout_slice = df.iloc[p2['idx']:search_end]
                
                breakout_idx = -1
                for k in range(len(breakout_slice)):
                    # 使用 itertuples 优化? 这里数据量小，直接访问即可
                    # 相对索引 k, 绝对索引 p2['idx'] + k
                    abs_idx = p2['idx'] + k
                    close_price = df.at[abs_idx, 'close']
                    
                    if close_price > neck_high:
                        breakout_idx = abs_idx
                        break
                        
                if breakout_idx != -1:
                    # 找到一个信号!
                    breakout_date = df.at[breakout_idx, 'date']
                    
                    # 防止重复信号 (例如连续几天突破)
                    # 简单策略: 如果最近刚发过信号，忽略
                    if signals and (breakout_idx - signals[-1]['idx']) < 5:
                        continue
                        
                    signals.append({
                        'date': breakout_date, # 必须是字符串或 datetime，与 Strategy 匹配
                        'idx': breakout_idx,
                        'price': df.at[breakout_idx, 'close'],
                        'info': f"双底突破 (底1:{p1['val']:.2f}, 底2:{p2['val']:.2f}, 颈线:{neck_high:.2f})"
                    })
                    
        return signals
