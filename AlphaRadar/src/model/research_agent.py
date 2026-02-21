import logging
import yaml
import os
from typing import Dict, Any

# 尝试导入 OpenAI，若不存在则 Mock
try:
    from openai import OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

class ResearchAgent:
    """
    AI 研报生成代理 (Research Agent).
    集成 LLM 生成简报.
    """
    
    def __init__(self, config_path: str = "config.yaml") -> None:
        self.logger = logging.getLogger(__name__)
        self.client = None
        self.provider = "mock" # mock, openai, gemini
        self._load_config(config_path)

    def _load_config(self, path: str) -> None:
        """加载配置."""
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                ai_cfg = config.get('ai', {})
                self.provider = ai_cfg.get('provider', 'mock')
                api_key = ai_cfg.get('api_key', '')
                base_url = ai_cfg.get('base_url', 'https://api.openai.com/v1')
                
                if self.provider == 'openai' and HAS_OPENAI and api_key:
                    self.client = OpenAI(api_key=api_key, base_url=base_url)

    def generate_report(self, 
                       symbol: str, 
                       stock_name: str, 
                       tech_info: str, 
                       fund_info: str,
                       news_context: list = None) -> str:
        """
        生成 AI 研报 (Generate AI Report).
        
        Args:
            symbol (str): 代码.
            stock_name (str): 名称.
            tech_info (str): 技术面描述.
            fund_info (str): 基本面描述.
            news_context (list): 新闻列表 (RAG 上下文).
            
        Returns:
            str: 研报文本.
        """
        # 构建新闻文本块
        news_text = "无近期相关新闻。"
        if news_context:
            news_items = []
            for item in news_context:
                news_items.append(f"- [{item['date']}] {item['title']} (来源: {item['source']})")
            news_text = "\n".join(news_items)

        system_prompt = """You are an advanced Financial AI Assistant (FinGPT variant) combining the expertise of Richard D. Wyckoff (Technical Analysis) and a quantitative analyst (Fundamental & Sentiment). Ensure your output is highly structured, objective, and specifically addresses the requested instructions in order."""

        prompt = f"""
[Data Input]
- Stock: {stock_name} ({symbol})
- Fundamental Data: {fund_info}
- Technical Structure: {tech_info}
- Recent News:
{news_text}

[Instruction]
Perform the following financial text analysis and synthesis tasks:

1. **Valuation Analysis**: Asses if the stock is undervalued or overvalued based on the fundamentals.
2. **Sentiment Analysis**: What is the sentiment of this news? Please choose an answer from {{negative/neutral/positive}} and briefly explain why.
3. **Wyckoff Phase Analysis**: Based on the technical structure, identify the current Wyckoff market phase (Accumulation, Markup, Distribution, Markdown) and note any key events (e.g., Springs, Upthrusts).
4. **Trading Plan**: Provide a structured trade setup including:
   - Action: [BUY / SELL / WAIT]
   - Stop Loss: (Specific level)
   - Target: (Specific level)
"""
        
        if self.provider == 'openai' and self.client:
            try:
                response = self.client.chat.completions.create(
                    model="gpt-3.5-turbo", # 可配置
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt}
                    ]
                )
                return response.choices[0].message.content
            except Exception as e:
                self.logger.error(f"OpenAI call failed: {e}")
                return f"AI 分析失败: {str(e)}"
        
        else:
            # Mock Response
            return f"""
            [模拟 AI 研报 - News RAG]
            
            股票: {stock_name} ({symbol})
            -------------------
            1. 估值分析: 
               根据提供的数据 {fund_info}，该股当前处于...
               
            2. 情报分析 (News RAG):
               监测到 {len(news_context) if news_context else 0} 条近期新闻:
               {news_text[:200]}...
               (模拟: 近期无重大负面舆情，管理层释放积极信号...)
               
            3. 技术分析:
               {tech_info}
               
            4. 交易建议:
               建议密切关注。若消息面配合且放量突破，可建仓。
               建议配置仓位: 请参考风险管理模块 (Kelly Criterion)。
            """
