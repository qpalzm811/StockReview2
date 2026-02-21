import pandas as pd
import numpy as np
from typing import Dict, Any, Optional
import logging
from model.data_nexus import DataNexus

class FactorEngine:
    """
    因子计算引擎 (Factor Engine).
    负责计算基本面因子 (Fundamental Factors).
    """

    def __init__(self, data_nexus: DataNexus) -> None:
        self.nexus = data_nexus
        self.logger = logging.getLogger(__name__)

    def get_valuation_metrics(self, symbol: str) -> Dict[str, float]:
        """
        获取估值指标 (Get Valuation Metrics).
        
        Args:
            symbol (str): 股票代码.
            
        Returns:
            Dict[str, float]: 包含 PE, PB, 市值等.
        """
        try:
            df = self.nexus.fetch_financial_data(symbol)
            if df.empty:
                return {}
            
            # 提取第一行数据 (因为是最新快照)
            # 使用 Vectorized Access (.iloc)
            pe_ttm = float(df.iloc[0]['pe_ttm']) if 'pe_ttm' in df.columns else 0.0
            pb = float(df.iloc[0]['pb']) if 'pb' in df.columns else 0.0
            total_mv = float(df.iloc[0]['total_mv']) if 'total_mv' in df.columns else 0.0
            
            return {
                "PE_TTM": pe_ttm,
                "PB": pb,
                "Total_MV": total_mv
            }
        except Exception as e:
            self.logger.error(f"Error calculation factors for {symbol}: {e}")
            return {}
            
    def assess_safety(self, metrics: Dict[str, float]) -> str:
        """
        简单评估安全边际 (Assess Safety Margin).
        
        Args:
            metrics (Dict): 估值指标.
            
        Returns:
            str: 评估结果 (低估/合理/高估).
        """
        if not metrics:
            return "未知"
            
        pe = metrics.get("PE_TTM", 0)
        # 简单逻辑: PE < 15 低估, 15-30 合理, >30 高估
        # 实际应结合行业分位点
        if pe > 0 and pe < 15:
            return "低估 (Underestimated)"
        elif pe >= 15 and pe < 35:
            return "合理 (Reasonable)"
        elif pe >= 35:
            return "高估 (Overestimated)"
        else:
            return "亏损或异常 (Loss/Abnormal)"
