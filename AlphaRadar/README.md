# AlphaRadar - 智能量化终端 🚀

AlphaRadar is a high-performance, local, privacy-first, and AI-enhanced quantitative investment research and monitoring terminal designed specifically for the A-share market.

[English](#english) | [简体中文](#简体中文)

---

<h2 id="简体中文">简体中文</h2>

AlphaRadar 是一款高性能、本地化、注重隐私并融合了 AI 大模型的智能量化投研与监控终端，专为 A 股市场设计。

### 🌟 核心功能

*   **⚡ 极速架构引擎**: 底层采用 **DuckDB** + **Polars** 构建本地高性能列式数据库。界面采用 **PyQt6** 结合异步 QThread 架构，实现极其流畅的非阻塞交互体验。
*   **📊 智能形态扫描器**: 全市场 5000+ 只股票急速扫描，严格过滤低胜率信号。精准捕捉高胜率结构（VCP 波动收缩、W底突破、收敛三角形）以及多指标低位共振（MACD金叉/KDJ底背离）。
*   **🤖 AI 智能投研**: 引入 FinGPT 风格的垂类金融大模型 (LLM)，一键生成个股分析报告、估值穿透与量化交易计划书。
*   **📈 混合数据引擎 (Hybrid-Data)**: 在不耗费过多网络资源的前提下，自动以毫秒级速度将本地历史数据与实时在线接口（AkShare、BaoStock）的最新数据进行无缝拼接修正，并加入了单位极其严格的**自适应量价数据对齐**算法。
*   **🔒 绝对隐私安全**: 你所有的选股池、监控策略和历史记录都完整存储在你个人的本地数据库中。

### 🛠️ 安装与运行 (Windows 环境)

**方法一：一键配置脚本（推荐）**
1. 双击运行项目中自带的 `setup.bat`，脚本将自动创建 Python 虚拟环境并一键安装所有相关核心依赖。
2. 安装完成后，只要双击 `run_alpharadar.bat` 即可快速跨过终端界面，以静默模式 (pythonw) 一键启动！

**方法二：手动配置**
```cmd
# 1. 创建虚拟环境
python -m venv venv

# 2. 激活虚拟环境
venv\Scripts\activate

# 3. 安装所需依赖库
pip install -r requirements.txt
# 或者使用 pyproject.toml： pip install .

# 4. 启动程序
python src\main.py
```

### 📝 社区规范
- **KISS原则**: 严禁无意义的超前防御性设计，系统功能保持轻量化与极简。
- **性能红线**: 一切IO尽可能绕回最内层的 DuckDB / Polars 内存处理，避免纯 Python `for` 循环遍历上百只股票。UI 层要求绝对的 UI-线程剥离与响应。

---

<h2 id="english">English</h2>

AlphaRadar is a high-performance, local, privacy-first, and AI-enhanced quantitative investment research and monitoring terminal designed specifically for the A-share market.

### 🌟 Features

*   **⚡ High-Performance Architecture**: Powered by **DuckDB** and **Polars** for lightning-fast localized data processing. Built on **PyQt6** with async QThread architecture for a buttery smooth UI.
*   **📊 Intelligent Scanner**: Auto-scans 5000+ stocks to detect high-win-rate technical setups (VCP, W-Bottom, Triangle Breakout) and indicator resonance (MACD, KDJ/RSI) based on strict scoring logic.
*   **🤖 AI Research Agent**: Integrates specialized Financial LLMs (FinGPT style) to automatically generate fundamental analysis and actionable trading plans.
*   **📈 Hybrid Data Engine**: Automatically stitches long-term historical data with real-time API quotes seamlessly while utilizing an adaptive volume-unit alignment algorithm.
*   **🔒 Privacy First**: All quote data, custom watchlists, and strategy logs are saved exclusively to your local DuckDB database.

### 🛠️ Installation (Windows)

**Method 1: One-Click Setup (Recommended)**
1. Double-click `setup.bat` to automatically create a virtual environment (`venv`) and install all required framework dependencies.
2. Once installed, simply double-click `run_alpharadar.bat` to quietly launch the app without lingering console windows!

**Method 2: Manual Setup**
```cmd
# Create virtual environment
python -m venv venv
# Activate virtual environment
venv\Scripts\activate
# Install requirements
pip install -r requirements.txt
# Run the application
python src\main.py
```
