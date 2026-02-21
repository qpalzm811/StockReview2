# AlphaRadar

AlphaRadar is a local, privacy-first, AI-enhanced quantitative investment research and monitoring terminal.

## Key Features

*   **Intelligent Scanning**: Traverses the market to identify high-probability patterns (Double Bottom, Triangle, Wyckoff).
*   **Fundamental Quant**: Multi-factor scoring engine (PE, PB, ROE dynamics) and AI-driven financial report interpretation.
*   **Real-time Monitoring**: "Market Sentinel" service for 5-minute intraday anomaly detection.
*   **Privacy First**: All data and strategies are stored locally in DuckDB.

## Architecture

*   **Model**: DuckDB + Polars for high-performance data handling.
*   **View**: PyQt6 for professional desktop GUI.
*   **Controller**: Async QThread architecture for non-blocking operations.

## Setup

1.  Install dependencies: `pip install .`
2.  Run the application: `python src/main.py`
