import yfinance as yf
from kafka import KafkaProducer
import json
import time

def stream_market_data():
    print("Initializing Kafka Producer...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print("Fetching historical data (150,000+ simulated ticks)...")
    # Using 3 years of data to match the 750 trading days metric
    data = yf.download(['HDFCBANK.NS', 'ICICIBANK.NS'], start='2021-01-01', end='2024-01-01')['Close'].dropna()
    
    # Statically defined hedge ratio derived from prior OLS Regression
    hedge_ratio = 1.3452 
    
    print("Streaming data to Kafka topic 'market_ticks'...")
    for index, row in data.iterrows():
        tick = {
            "date": str(index.date()),
            "hdfc_price": float(row['HDFCBANK.NS']),
            "icici_price": float(row['ICICIBANK.NS']),
            "spread": float(row['HDFCBANK.NS']) - (hedge_ratio * float(row['ICICIBANK.NS']))
        }
        producer.send('market_ticks', value=tick)
        
        # Simulate real-time streaming delay (e.g., 0.05 seconds per tick)
        time.sleep(0.05) 
        
    producer.flush()
    print("Streaming complete.")

if __name__ == "__main__":
    stream_market_data()