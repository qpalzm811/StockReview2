import backtrader as bt
import pandas as pd
import logging
from typing import Dict, Any, List
from model.risk_manager import RiskManager

class DoubleBottomStrategy(bt.Strategy):
    """
    双底回测策略 (Double Bottom Strategy).
    
    逻辑:
    1. 接收外部注入的 "交易信号列表".
    2. 结合 RiskManager 进行科学仓位管理.
    """
    
    params = (
        ('signals', []),       # 外部传入的信号列表
        ('stop_loss', 0.05),   # 静态止损 (Fallback)
        ('take_profit', 0.10), # 静态止盈 (Fallback)
        ('print_log', True),
        ('use_kelly', False),  # 是否启用凯利公式/ATR风控
        ('kelly_win_rate', 0.55), # 预估胜率 (用于凯利计算)
        ('kelly_payoff', 2.0),    # 预估盈亏比
    )

    def __init__(self):
        """初始化策略."""
        self.dataclose = self.datas[0].close
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        self.order = None
        self.buyprice = None
        self.buycomm = None
        
        # ATR 指标 (Backtrader 内置)
        self.atr = bt.indicators.ATR(self.datas[0], period=14)
        
        self.signal_map = {}
        for sig in self.params.signals:
            d = sig.get('date')
            if d:
                self.signal_map[str(d)[:10]] = sig

    def log(self, txt: str, dt=None) -> None:
        """日志输出函数."""
        if self.params.print_log:
            dt = dt or self.datas[0].datetime.date(0)
            logging.info(f'[Backtest] {dt.isoformat()}, {txt}')

    def notify_order(self, order: bt.Order) -> None:
        """订单状态通知."""
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入成效: 价格 {order.executed.price:.2f}, 数量 {order.executed.size}, 费用 {order.executed.comm:.2f}')
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            elif order.issell():
                self.log(f'卖出成效: 价格 {order.executed.price:.2f}, 数量 {order.executed.size}, 费用 {order.executed.comm:.2f}')
            
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('订单被取消/资金不足/拒绝')

        self.order = None

    def notify_trade(self, trade: bt.Trade) -> None:
        """交易结单通知."""
        if not trade.isclosed:
            return
        self.log(f'交易利润: 毛利 {trade.pnl:.2f}, 净利 {trade.pnlcomm:.2f}')

    def next(self) -> None:
        """核心策略逻辑."""
        
        # 1. 检查是否有未完成订单
        if self.order:
            return

        current_date_str = self.datas[0].datetime.date(0).isoformat()
        
        # 2. 检查是否有持仓
        if not self.position:
            # 开仓逻辑
            if current_date_str in self.signal_map:
                sig = self.signal_map[current_date_str]
                self.log(f"触发买入信号: {sig.get('info', '未知')}")
                
                # 计算仓位
                size = 0
                cash = self.broker.getcash()
                cur_price = self.dataclose[0]
                cur_atr = self.atr[0]
                
                if self.params.use_kelly:
                    # 使用 RiskManager 计算
                    # 1. 凯利比例
                    kelly_pct = RiskManager.calculate_kelly_position(
                        win_rate=self.params.kelly_win_rate,
                        payoff_ratio=self.params.kelly_payoff,
                        fraction=0.5 # 半凯利
                    )
                    
                    # 2. ATR 波动率风控 (每笔亏损不超过 2%)
                    vol_adj_size = RiskManager.calculate_volatility_adjusted_size(
                        account_value=self.broker.getvalue(), 
                        entry_price=cur_price, 
                        atr=cur_atr
                    )
                    
                    # 结合两者: 取较小值 (更保守)
                    kelly_size = int((self.broker.getvalue() * kelly_pct) / cur_price)
                    final_size = min(kelly_size, vol_adj_size)
                    
                    # 再次取整 100
                    size = int(final_size / 100) * 100
                    
                    self.log(f"风控计算: Kelly比例 {kelly_pct:.2%}, ATR建议股数 {vol_adj_size}, 最终下单 {size}")
                    
                else:
                    # 原有逻辑: 全仓 (95%)
                    target_value = cash * 0.95
                    size = int(target_value / cur_price / 100) * 100
                
                if size > 0:
                    self.order = self.buy(size=size)
                    # 记录 ATR 用于止损
                    self.entry_atr = cur_atr
        
        else:
            # 平仓逻辑 (止盈止损)
            if self.buyprice:
                pct_change = (self.dataclose[0] - self.buyprice) / self.buyprice
                
                should_sell = False
                reason = ""
                
                if self.params.use_kelly:
                    # 动态 ATR 止损
                    top_price = RiskManager.calculate_dynamic_stop(self.buyprice, self.entry_atr, multiplier=2.0)
                    if self.dataclose[0] < top_price:
                        should_sell = True
                        reason = f"ATR动态止损 (当前 {self.dataclose[0]:.2f} < 止损价 {top_price:.2f})"
                    # 止盈保持默认 10% (或可移动止盈，暂简化)
                    elif pct_change > self.params.take_profit:
                        should_sell = True
                        reason = f"止盈 (涨幅 {pct_change:.2%})"
                else:
                    # 静态比例
                    if pct_change < -self.params.stop_loss:
                        should_sell = True
                        reason = f"静态止损 {pct_change:.2%}"
                    elif pct_change > self.params.take_profit:
                        should_sell = True
                        reason = f"静态止盈 {pct_change:.2%}"
                
                if should_sell:
                    self.log(f'触发卖出: {reason}')
                    self.order = self.sell(size=self.position.size)
