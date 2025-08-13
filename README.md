# Energy Trading Streaming (Kafka + Flink + Postgres)

A trading-centric streaming PoC that simulates energy hub price ticks and trades, then calculates per-account PnL and simple price forecasts in real time with Apache Flink. All services run via Docker Compose.

## Architecture

- Kafka: Streams two topics — `prices` ($/MWh) and `trades` (MW fills)
- Flink: Stateful job that persists raw streams, computes SMA forecasts, and emits `positions_pnl`
- Postgres: Stores `prices`, `trades`, `forecasts`, and `positions_pnl`
- Python Producer: Generates diurnal energy price shapes and random trades across hubs/accounts

## Prerequisites

- Docker and Docker Compose
- ~4 GB RAM available

## Quick Start

```bash
# Start everything
docker compose up --build

# Flink UI
# http://localhost:8081
```

The compose file builds and submits the Flink job automatically once Kafka/Postgres/Flink are ready. The Python producer then streams price ticks every ~200ms and occasional trades.

## Data Model

### Kafka topics (JSON)

`prices`
```json
{ "ts": "2024-01-15T10:30:45.123Z", "hub": "PJM-WEST", "price_mwh": 47.12 }
```

`trades`
```json
{
  "trade_id": 123,
  "ts": "2024-01-15T10:30:45.123Z",
  "account": "ACC1",
  "hub": "PJM-WEST",
  "side": "BUY",
  "mw": 25,
  "price_mwh": 46.95
}
```

### Postgres tables

- `prices(ts TEXT, hub TEXT, price_mwh DOUBLE PRECISION)`
- `trades(trade_id BIGINT, ts TEXT, account TEXT, hub TEXT, side TEXT, mw INT, price_mwh DOUBLE PRECISION)`
- `forecasts(ts TEXT, hub TEXT, sma5 DOUBLE PRECISION, sma20 DOUBLE PRECISION, forecast_next DOUBLE PRECISION)`
- `positions_pnl(ts TEXT, account TEXT, hub TEXT, position_mw INT, avg_price_mwh DOUBLE PRECISION, last_price_mwh DOUBLE PRECISION, realized_pnl DOUBLE PRECISION, unrealized_pnl DOUBLE PRECISION, total_pnl DOUBLE PRECISION)`

Note: timestamps are stored as TEXT for simplicity. We can switch to `TIMESTAMPTZ` if needed.

## How It Works

- Producer (`producer/producer.py`):
  - Hubs: PJM-WEST, ERCOT-HOUSTON, NYISO-ZONEJ, CAISO-NP15
  - Simulates diurnal price patterns plus noise; emits `prices` and random `trades`
- Flink job (`KafkaToPostgresJob`):
  - Persists raw `prices`/`trades` to Postgres
  - Forecasts: SMA(5/20) per hub on `price_mwh`, writes to `forecasts`
  - PnL: Maintains per-account position (MW) and avg price ($/MWh) keyed by hub; emits `positions_pnl` on every price or trade

## Inspecting Data

Open the Flink UI at http://localhost:8081 to view job status, throughput, and operators.

Query Postgres (inside the compose stack):
```bash
docker compose exec -u postgres postgres psql -U postgres -d flinkdb
```

Sample SQL:
```sql
-- Latest prices by hub
SELECT *
FROM prices
ORDER BY ts DESC
LIMIT 8;

-- Recent trades
SELECT *
FROM trades
ORDER BY ts DESC
LIMIT 10;

-- Most recent PnL snapshot per account/hub
SELECT DISTINCT ON (account, hub) *
FROM positions_pnl
ORDER BY account, hub, ts DESC;

-- Forecast signals
SELECT *
FROM forecasts
ORDER BY ts DESC, hub
LIMIT 8;
```

Kafka tools:
```bash
# Producer logs
docker compose logs -f producer

# List topics from inside the Kafka container
docker compose exec kafka kafka-topics --list --bootstrap-server kafka:29092
```

## Configuration

- Producer pacing: edit `time.sleep(0.2)` in `producer/producer.py`
- Flink parallelism/task slots: see `docker-compose.yml` under `taskmanager` environment
- JDBC batch sizes: `KafkaToPostgresJob.java` (JdbcExecutionOptions)

## Project Structure

```
docker-compose.yml
producer/
  Dockerfile
  producer.py
  requirements.txt
flink-job/
  Dockerfile
  pom.xml
  src/main/java/com/example/KafkaToPostgresJob.java
postgres/
  init.sql
scripts/
  build.sh
  submit-job.sh
README.md
```

## Troubleshooting

- Flink job not visible: wait for JobManager/TaskManager to register; check `docker compose logs flink-job-submitter`
- No data in Postgres: check `docker compose logs producer` and Flink UI; confirm topics exist
- Kafka CLI inside container: use `kafka:29092` as the bootstrap server

## Next Ideas

- Use `TIMESTAMPTZ` and event time/watermarks
- Add block trades with delivery windows and hourly settlement PnL
- Basis and spread PnL between hubs
- Strategy-driven trades (e.g., SMA crossovers) instead of random

