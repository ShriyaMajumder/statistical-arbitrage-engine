# Distributed Statistical Arbitrage Trading Engine

An automated, market-neutral pairs trading architecture built to ingest asynchronous market data, calculate statistical cointegration, and execute mean-reversion trades with sub-second latency.

## Architecture & Tech Stack
* **Data Ingestion (Producer):** Python script simulating a high-frequency tick stream using historical NSE data.
* **Message Broker:** Apache Kafka (Confluent Local / KRaft) for decoupled, real-time data streaming.
* **State Management:** Redis (In-Memory Datastore) for tracking global portfolio positions and PnL across distributed nodes.
* **Execution Engine (Consumer):** Python pandas environment computing 20-day rolling Z-scores and generating trade signals.
* **Infrastructure:** Docker & Docker Compose for isolated microservices.

## Quantitative Strategy
1. **Cointegration Verification:** Uses Ordinary Least Squares (OLS) regression to determine the hedge ratio between two correlated assets (e.g., HDFC Bank & ICICI Bank). Applies the Augmented Dickey-Fuller (ADF) test to verify spread stationarity (p < 0.05).
2. **Signal Generation:** Calculates a 20-period rolling Z-score of the spread.
    * **Entry:** Goes LONG or SHORT when the spread deviates > 2.0σ from the historical mean.
    * **Exit:** Closes the position when the spread reverts to the mean (Z-score < 0.5).
3. **Risk Management:** Simulates a ₹0.50 transaction cost (slippage/brokerage) per executed trade to accurately reflect net alpha.

## Local Setup & Execution

### 1. Boot the Infrastructure
Ensure Docker Desktop is running, then initialize the Kafka broker and Redis state tracker:
```bash
docker-compose up -d
```

### 2. Setup the Environment
```bash
python -m venv venv
# Windows: .\venv\Scripts\activate
# Mac/Linux: source venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the Distributed Engine
Open two separate terminal windows (ensure the virtual environment is activated in both).

**Terminal 1 (Start the Execution Consumer):**
```bash
python trading_engine.py
```

**Terminal 2 (Start the Market Data Stream):**
```bash
python data_producer.py
```