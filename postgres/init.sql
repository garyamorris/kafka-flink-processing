-- Create the events table
-- Base demo table (kept for backwards compatibility)
CREATE TABLE IF NOT EXISTS events (
    id INT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

-- Trading-centric tables

-- Price ticks per hub ($/MWh)
CREATE TABLE IF NOT EXISTS prices (
    ts TIMESTAMPTZ NOT NULL,
    hub TEXT NOT NULL,
    price_mwh DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prices_hub_ts ON prices(hub, ts);

CREATE TABLE IF NOT EXISTS trades (
    trade_id BIGINT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    account TEXT NOT NULL,
    hub TEXT NOT NULL,
    side TEXT NOT NULL,   -- BUY or SELL
    mw INT NOT NULL,
    price_mwh DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_account_hub_ts ON trades(account, hub, ts);

CREATE TABLE IF NOT EXISTS positions_pnl (
    ts TIMESTAMPTZ NOT NULL,
    account TEXT NOT NULL,
    hub TEXT NOT NULL,
    position_mw INT NOT NULL,
    avg_price_mwh DOUBLE PRECISION NOT NULL,
    last_price_mwh DOUBLE PRECISION NOT NULL,
    realized_pnl DOUBLE PRECISION NOT NULL,
    unrealized_pnl DOUBLE PRECISION NOT NULL,
    total_pnl DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_positions_pnl_account_hub_ts ON positions_pnl(account, hub, ts);

CREATE TABLE IF NOT EXISTS forecasts (
    ts TIMESTAMPTZ NOT NULL,
    hub TEXT NOT NULL,
    sma5 DOUBLE PRECISION,
    sma20 DOUBLE PRECISION,
    forecast_next DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_forecasts_hub_ts ON forecasts(hub, ts);

-- Price exposure per account/hub snapshot
CREATE TABLE IF NOT EXISTS price_exposure (
    ts TIMESTAMPTZ NOT NULL,
    account TEXT NOT NULL,
    hub TEXT NOT NULL,
    position_mw INT NOT NULL,
    last_price_mwh DOUBLE PRECISION NOT NULL,
    pnl01 DOUBLE PRECISION NOT NULL,           -- $ PnL for $1 move
    notional_usd DOUBLE PRECISION NOT NULL     -- position_mw * last_price_mwh
);
CREATE INDEX IF NOT EXISTS idx_price_exposure_account_hub_ts ON price_exposure(account, hub, ts);

-- Index and grants for legacy table
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
GRANT ALL PRIVILEGES ON TABLE events TO postgres;
GRANT ALL PRIVILEGES ON TABLE prices TO postgres;
GRANT ALL PRIVILEGES ON TABLE trades TO postgres;
GRANT ALL PRIVILEGES ON TABLE positions_pnl TO postgres;
GRANT ALL PRIVILEGES ON TABLE forecasts TO postgres;
GRANT ALL PRIVILEGES ON TABLE price_exposure TO postgres;

-- Day-ahead and Real-time LMPs
CREATE TABLE IF NOT EXISTS dayahead_prices (
    ts TIMESTAMPTZ NOT NULL,
    hub TEXT NOT NULL,
    lmp_da DOUBLE PRECISION NOT NULL,
    energy_da DOUBLE PRECISION NOT NULL,
    congestion_da DOUBLE PRECISION NOT NULL,
    loss_da DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dayahead_hub_ts ON dayahead_prices(hub, ts);

CREATE TABLE IF NOT EXISTS realtime_prices (
    ts TIMESTAMPTZ NOT NULL,
    hub TEXT NOT NULL,
    lmp_rt DOUBLE PRECISION NOT NULL,
    energy_rt DOUBLE PRECISION NOT NULL,
    congestion_rt DOUBLE PRECISION NOT NULL,
    loss_rt DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_realtime_hub_ts ON realtime_prices(hub, ts);
GRANT ALL PRIVILEGES ON TABLE dayahead_prices TO postgres;
GRANT ALL PRIVILEGES ON TABLE realtime_prices TO postgres;
