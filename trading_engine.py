from kafka import KafkaConsumer
import redis
import pandas as pd
import json

# Initialize Connections
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
consumer = KafkaConsumer(
    'market_ticks',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)

# Configuration matching the resume bullets
WINDOW = 20
Z_ENTRY = 2.0
Z_EXIT = 0.5
SLIPPAGE_COST = 0.50

# Initialize state in Redis 
redis_client.set("current_position", 0)
redis_client.set("cumulative_pnl", 0.0)

spread_history = []
last_spread = None

print("Trading Engine running. Listening for market ticks...")

for message in consumer:
    tick = message.value
    current_spread = tick['spread']
    spread_history.append(current_spread)
    
    # Wait until we have enough data for the rolling window
    if len(spread_history) < WINDOW:
        last_spread = current_spread
        continue
        
    # Maintain strict 20-tick rolling window
    if len(spread_history) > WINDOW:
        spread_history.pop(0)
        
    # Calculate Rolling Stats (Pandas)
    s_series = pd.Series(spread_history)
    rolling_mean = s_series.mean()
    rolling_std = s_series.std()
    
    if rolling_std == 0:
        last_spread = current_spread
        continue
        
    z_score = (current_spread - rolling_mean) / rolling_std
    
    # Retrieve global state from Redis
    current_position = int(redis_client.get("current_position"))
    cumulative_pnl = float(redis_client.get("cumulative_pnl"))
    
    # Calculate daily PnL based on previous position
    daily_pnl = current_position * (current_spread - last_spread)
    cumulative_pnl += daily_pnl
    
    # Signal Logic & Execution
    trade_executed = False
    
    if z_score < -Z_ENTRY and current_position != 1:
        print(f"[{tick['date']}] BUY SIGNAL: Z={z_score:.2f}. Going LONG. (Spread: {current_spread:.2f})")
        redis_client.set("current_position", 1)
        trade_executed = True
        
    elif z_score > Z_ENTRY and current_position != -1:
        print(f"[{tick['date']}] SHORT SIGNAL: Z={z_score:.2f}. Going SHORT. (Spread: {current_spread:.2f})")
        redis_client.set("current_position", -1)
        trade_executed = True
        
    elif abs(z_score) < Z_EXIT and current_position != 0:
        print(f"[{tick['date']}] EXIT SIGNAL: Z={z_score:.2f}. Reverted to mean. FLAT.")
        redis_client.set("current_position", 0)
        trade_executed = True

    # Apply slippage/transaction cost if a trade was executed
    if trade_executed:
        cumulative_pnl -= SLIPPAGE_COST
        
    redis_client.set("cumulative_pnl", cumulative_pnl)
    last_spread = current_spread
    
    # Log periodic updates to show alpha generation
    if tick['date'].endswith("-01"): 
        print(f"--> Month End [{tick['date']}]: Cumulative PnL: ₹{cumulative_pnl:.2f}")