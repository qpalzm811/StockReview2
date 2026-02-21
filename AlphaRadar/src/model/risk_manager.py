import numpy as np
from typing import Optional, Tuple

class RiskManager:
    """
    风控管理器 (Risk Manager).
    负责资金管理计算，包括凯利公式仓位和动态止损。
    遵循严格的数学模型。
    """

    @staticmethod
    def calculate_kelly_position(win_rate: float, 
                                 payoff_ratio: float, 
                                 fraction: float = 0.5) -> float:
        """
        计算凯利公式建议仓位 (Calculate Kelly Position).
        
        Formula: f* = (bp - q) / b = p - q/b
        where:
            f* is the fraction of the current bankroll to wager
            b is the net odds received on the wager (payoff_ratio)
            p is the probability of winning (win_rate)
            q is the probability of losing (1 - p)
            
        Args:
            win_rate (float): 胜率 (0.0 - 1.0). e.g., 0.55
            payoff_ratio (float): 盈亏比 (Avg Win / Avg Loss). e.g., 2.0
            fraction (float): 凯利分数 (Fractional Kelly). 默认 0.5 (半凯利) 以平滑波动.
            
        Returns:
            float: 建议仓位比例 (0.0 - 1.0). 若结果 < 0，返回 0.0 (不交易).
        """
        if payoff_ratio <= 0:
            return 0.0
            
        q = 1.0 - win_rate
        f_star = win_rate - (q / payoff_ratio)
        
        # 应用凯利分数 (Half-Kelly)
        f_star = f_star * fraction
        
        # 边界限制: 不超过 95% 仓位 (留 5% 现金)，不低于 0
        return max(0.0, min(0.95, f_star))

    @staticmethod
    def calculate_dynamic_stop(entry_price: float, 
                               atr: float, 
                               multiplier: float = 2.0) -> float:
        """
        计算 ATR动态止损价 (Calculate ATR Dynamic Stop).
        
        Args:
            entry_price (float): 入场价格.
            atr (float): 当前 ATR 值.
            multiplier (float): ATR 倍数. 默认 2.0.
            
        Returns:
            float: 止损价格.
        """
        return entry_price - (atr * multiplier)

    @staticmethod
    def calculate_volatility_adjusted_size(account_value: float, 
                                           entry_price: float, 
                                           atr: float, 
                                           risk_per_trade: float = 0.02) -> int:
        """
        基于波动率风险的仓位计算 (Volatility Adjusted Sizing).
        使得单笔交易的潜在 ATR 亏损不超过账户总额的 2%.
        
        Formula: Shares = (Account * Risk%) / (ATR * Multiplier)
        这里简化为 Risk Amount / 1ATR risk (or stop distance)
        
        Args:
            account_value (float): 账户总值.
            entry_price (float): 入场价.
            atr (float): ATR.
            risk_per_trade (float): 单笔最大风险比例. 默认 0.02 (2%).
            
        Returns:
            int: 建议股数 (向下取整到 100 整数倍).
        """
        if atr <= 0:
            return 0
            
        risk_amount = account_value * risk_per_trade
        # 假设止损距离为 2 * ATR
        stop_distance = 2.0 * atr
        
        shares = risk_amount / stop_distance
        
        # A股一手 100 股
        return int(shares / 100) * 100
