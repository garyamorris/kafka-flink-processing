import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

HUBS = ["PJM-WEST", "ERCOT-HOUSTON", "NYISO-ZONEJ", "CAISO-NP15"]


def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()


def create_producer():
    broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    for i in range(30):
        try:
            prod = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"Connected to Kafka at {broker}")
            return prod
        except Exception as e:
            print(f"Kafka connect attempt {i+1} failed: {e}")
            time.sleep(2)
    raise RuntimeError("Could not connect to Kafka")


def simulate_da_components(base):
    # Decompose into energy, congestion, loss components
    energy = base + random.uniform(-2.0, 2.0)
    congestion = random.gauss(0, 1.5)
    losses = random.uniform(-0.5, 0.8)
    lmp = energy + congestion + losses
    return round(lmp, 2), round(energy, 2), round(congestion, 2), round(losses, 2)


def simulate_rt_components(base):
    # Higher volatility for RT
    energy = base + random.uniform(-3.0, 3.0)
    congestion = random.gauss(0, 3.0)
    losses = random.uniform(-1.0, 1.2)
    lmp = energy + congestion + losses
    return round(lmp, 2), round(energy, 2), round(congestion, 2), round(losses, 2)


def diurnal_baseline(hour):
    # Simple higher day, lower night baseline
    base = 25.0
    if 6 <= hour <= 22:
        base += 12.0
        if 12 <= hour <= 17:
            base += 6.0  # afternoon
    else:
        base -= 5.0
    return base


def main():
    print("Starting Day-Ahead and Real-Time price producer...")
    time.sleep(8)
    producer = create_producer()

    try:
        # Loop emitting DA hourly and RT frequent ticks
        next_da_emit = datetime.now(timezone.utc)
        while True:
            now = datetime.now(timezone.utc)
            hour = now.hour
            base = diurnal_baseline(hour)

            # Emit DA once every ~10 seconds per hub (simulated hourly DA)
            if now >= next_da_emit:
                for hub in HUBS:
                    lmp, energy, cong, loss = simulate_da_components(base)
                    da = {
                        "ts": utcnow_iso(),
                        "hub": hub,
                        "lmp_da": lmp,
                        "energy_da": energy,
                        "congestion_da": cong,
                        "loss_da": loss,
                    }
                    producer.send("dayahead_prices", value=da)
                next_da_emit = now + timedelta(seconds=10)

            # Emit RT every 1s per hub (simulated 5-min cadence)
            for hub in HUBS:
                lmp, energy, cong, loss = simulate_rt_components(base)
                rt = {
                    "ts": utcnow_iso(),
                    "hub": hub,
                    "lmp_rt": lmp,
                    "energy_rt": energy,
                    "congestion_rt": cong,
                    "loss_rt": loss,
                }
                producer.send("realtime_prices", value=rt)

            producer.flush()
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()

