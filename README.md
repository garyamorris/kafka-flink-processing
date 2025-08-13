# Energy Trading Streaming (Kafka + Flink + Postgres)

This PoC simulates energy hub price streams and trades, then computes forecasts, PnL, and exposure in real time. It now uses TIMESTAMPTZ timestamps, adds Schema Registry, and includes day‑ahead vs real‑time LMP publishers.

## Architecture

- Kafka: Topics `prices`, `trades`, `dayahead_prices`, `realtime_prices`
- Schema Registry: Confluent Schema Registry (ready for Avro/Protobuf)
- Flink jobs: Ingestion, forecasts, PnL/exposure (separate pipelines)
- Postgres: Stores prices, trades, forecasts, PnL, exposure, and DA/RT LMPs
- Python producers: Random‑walk spot prices/trades and DA/RT LMP component publishers

## Quick Start

```bash
docker compose up --build
# Flink UI: http://localhost:8081
# Schema Registry: http://localhost:8082
```

The submitter container waits for the cluster and submits all jobs. Producers start streaming once Kafka is reachable.

## Data Model

### Kafka topics (JSON)

`prices`
```json
{ "ts": "2024-01-15T10:30:45Z", "hub": "PJM-WEST", "price_mwh": 47.12 }
```

`trades`
```json
{ "trade_id": 123, "ts": "2024-01-15T10:30:45Z", "account": "ACC1", "hub": "PJM-WEST", "side": "BUY", "mw": 25, "price_mwh": 46.95 }
```

`dayahead_prices`
```json
{ "ts": "2024-01-15T10:30:45Z", "hub": "PJM-WEST", "lmp_da": 43.1, "energy_da": 40.7, "congestion_da": 1.8, "loss_da": 0.6 }
```

`realtime_prices`
```json
{ "ts": "2024-01-15T10:30:46Z", "hub": "PJM-WEST", "lmp_rt": 45.8, "energy_rt": 42.3, "congestion_rt": 2.9, "loss_rt": 0.6 }
```

### Postgres tables (TIMESTAMPTZ)

- `prices(ts TIMESTAMPTZ, hub TEXT, price_mwh DOUBLE PRECISION)`
- `trades(trade_id BIGINT, ts TIMESTAMPTZ, account TEXT, hub TEXT, side TEXT, mw INT, price_mwh DOUBLE PRECISION)`
- `forecasts(ts TIMESTAMPTZ, hub TEXT, sma5 DOUBLE PRECISION, sma20 DOUBLE PRECISION, forecast_next DOUBLE PRECISION)`
- `positions_pnl(ts TIMESTAMPTZ, account TEXT, hub TEXT, position_mw INT, avg_price_mwh DOUBLE PRECISION, last_price_mwh DOUBLE PRECISION, realized_pnl DOUBLE PRECISION, unrealized_pnl DOUBLE PRECISION, total_pnl DOUBLE PRECISION)`
- `price_exposure(ts TIMESTAMPTZ, account TEXT, hub TEXT, position_mw INT, last_price_mwh DOUBLE PRECISION, pnl01 DOUBLE PRECISION, notional_usd DOUBLE PRECISION)`
- `dayahead_prices(ts TIMESTAMPTZ, hub TEXT, lmp_da DOUBLE PRECISION, energy_da DOUBLE PRECISION, congestion_da DOUBLE PRECISION, loss_da DOUBLE PRECISION)`
- `realtime_prices(ts TIMESTAMPTZ, hub TEXT, lmp_rt DOUBLE PRECISION, energy_rt DOUBLE PRECISION, congestion_rt DOUBLE PRECISION, loss_rt DOUBLE PRECISION)`

All timestamps are `TIMESTAMPTZ` and serialized as ISO‑8601 (UTC) strings.

## How It Works

- Producer (`producer/producer.py`):
  - Hubs: PJM-WEST, ERCOT-HOUSTON, NYISO-ZONEJ, CAISO-NP15
  - Simulates diurnal price patterns plus noise; emits `prices` and random `trades`
- Producer (`producer-da-rt/producer_da_rt.py`):
  - Emits DA LMP components (energy, congestion, loss) every ~10s and RT components every ~1s
  - Topics: `dayahead_prices`, `realtime_prices`
- Flink jobs:
  - `IngestPricesAndTradesJob`: persists raw `prices` and `trades` into Postgres
  - `ForecastsJob`: computes SMA(5/20) per hub and writes to `forecasts`
  - `PnlAndExposureJob`: maintains per-account positions and emits `positions_pnl` and `price_exposure`
  - `IngestDayAheadAndRealTimeJob`: persists `dayahead_prices` and `realtime_prices`

## Inspecting Data

- Flink UI: http://localhost:8081 — see all running jobs
- Schema Registry: http://localhost:8082 (currently unused by producers, ready for Avro/Protobuf)

Postgres shell:
```bash
docker compose exec -u postgres postgres psql -U postgres -d flinkdb
```

Sample SQL:
```sql
-- Latest prices by hub
SELECT * FROM prices ORDER BY ts DESC LIMIT 8;

-- Recent trades
SELECT * FROM trades ORDER BY ts DESC LIMIT 10;

-- Most recent PnL snapshot per account/hub
SELECT DISTINCT ON (account, hub) * FROM positions_pnl ORDER BY account, hub, ts DESC;

-- Exposure (pnl01 and notional)
SELECT DISTINCT ON (account, hub) ts, account, hub, position_mw, last_price_mwh, pnl01, notional_usd
FROM price_exposure ORDER BY account, hub, ts DESC;

-- DA vs RT last values
SELECT * FROM dayahead_prices ORDER BY ts DESC LIMIT 8;
SELECT * FROM realtime_prices ORDER BY ts DESC LIMIT 8;
```

Kafka tools:
```bash
docker compose logs -f producer producer-da-rt
docker compose exec kafka kafka-topics --list --bootstrap-server kafka:29092
```

## Configuration

- Producer pacing: `time.sleep()` in producers
- Flink parallelism/task slots: see `docker-compose.yml` under `taskmanager`
- JDBC batch sizes: see each job’s `JdbcExecutionOptions`

## Project Structure

```
docker-compose.yml
producer/
  Dockerfile
  producer.py
  requirements.txt
producer-da-rt/
  Dockerfile
  producer_da_rt.py
  requirements.txt
flink-job/
  Dockerfile
  pom.xml
  src/main/java/com/example/
    KafkaToPostgresJob.java
    IngestPricesAndTradesJob.java
    IngestDayAheadAndRealTimeJob.java
    ForecastsJob.java
    PnlAndExposureJob.java
postgres/
  init.sql
scripts/
  build.sh
  submit-job.sh
README.md
```

## Notes

- Existing deployments may need migration to TIMESTAMPTZ; the provided init script defines new tables for fresh runs.
- Schema Registry is included but producers currently publish JSON. Next step is to switch to Avro/Protobuf with subject‑version governance.
