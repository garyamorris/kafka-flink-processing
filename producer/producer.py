import json
import time
import random
import os
from datetime import datetime
from collections import defaultdict
from kafka import KafkaProducer
from kafka.errors import KafkaError

HUBS = ["PJM-WEST", "ERCOT-HOUSTON", "NYISO-ZONEJ", "CAISO-NP15"]
ACCOUNTS = ["ACC1", "ACC2", "ACC3"]


def create_producer():
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    max_retries = 30
    retry_delay = 2

    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[kafka_broker],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"Connected to Kafka broker at {kafka_broker}")
            return producer
        except Exception as e:
            print(f"Attempt {i+1}/{max_retries}: Failed to connect to Kafka: {e}")
            time.sleep(retry_delay)

    raise Exception("Failed to connect to Kafka after maximum retries")


def simulate_prices(state):
    # Diurnal pattern + random noise, typical of $/MWh shapes
    hour = datetime.utcnow().hour
    diurnal = 5 * (1 + (1 if 7 <= hour <= 20 else -0.5))  # higher day, lower night
    solar_midday = -3 * (1 if 11 <= hour <= 15 else 0)    # midday renewables can depress prices

    for hub in HUBS:
        price = state[hub]
        drift = random.uniform(-0.3, 0.3)
        shock = random.gauss(0, 0.8)
        baseline = diurnal + solar_midday
        new_price = max(5.0, round(price + drift + shock + baseline * 0.05, 2))
        state[hub] = new_price
    return state


def maybe_emit_trade(cur_prices, trade_id):
    # Sometimes emit a trade around current price
    if random.random() < 0.5:
        hub = random.choice(HUBS)
        account = random.choice(ACCOUNTS)
        side = random.choice(["BUY", "SELL"])
        mw = random.choice([5, 10, 25, 50])
        px = cur_prices[hub] + random.uniform(-1.0, 1.0)
        trade = {
            "trade_id": trade_id,
            "ts": datetime.utcnow().isoformat(),
            "account": account,
            "hub": hub,
            "side": side,
            "mw": mw,
            "price_mwh": round(px, 2),
        }
        return trade
    return None


def main():
    print("Starting trading data producer...")

    # Wait for Kafka to be ready
    time.sleep(10)

    producer = create_producer()
    trade_id = 0

    # Initialize starting prices
    prices = {h: round(random.uniform(15, 75), 2) for h in HUBS}

    try:
        while True:
            prices = simulate_prices(prices)

            now_iso = datetime.utcnow().isoformat()
            # Emit price ticks for all symbols
            for hub in HUBS:
                price_event = {"ts": now_iso, "hub": hub, "price_mwh": prices[hub]}
                fut = producer.send("prices", value=price_event)
                try:
                    fut.get(timeout=5)
                except KafkaError as e:
                    print(f"Failed to send price: {e}")

            # Occasionally emit a trade
            trade_id += 1
            trade_event = maybe_emit_trade(prices, trade_id)
            if trade_event:
                fut = producer.send("trades", value=trade_event)
                try:
                    fut.get(timeout=5)
                    print(f"Trade: {trade_event}")
                except KafkaError as e:
                    print(f"Failed to send trade: {e}")

            # Pace the stream
            time.sleep(0.2)

    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed")


if __name__ == "__main__":
    main()
