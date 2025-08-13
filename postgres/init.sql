-- Create the events table
-- Base demo table (kept for backwards compatibility)
CREATE TABLE IF NOT EXISTS events (
    id INT PRIMARY KEY,
    ts TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

-- Trading-centric tables

-- Price ticks per hub ($/MWh)
CREATE TABLE IF NOT EXISTS prices (
    ts TEXT NOT NULL,
    hub TEXT NOT NULL,
    price_mwh DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prices_hub_ts ON prices(hub, ts);

CREATE TABLE IF NOT EXISTS trades (
    trade_id BIGINT PRIMARY KEY,
    ts TEXT NOT NULL,
    account TEXT NOT NULL,
    hub TEXT NOT NULL,
    side TEXT NOT NULL,   -- BUY or SELL
    mw INT NOT NULL,
    price_mwh DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_account_hub_ts ON trades(account, hub, ts);

CREATE TABLE IF NOT EXISTS positions_pnl (
    ts TEXT NOT NULL,
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
    ts TEXT NOT NULL,
    hub TEXT NOT NULL,
    sma5 DOUBLE PRECISION,
    sma20 DOUBLE PRECISION,
    forecast_next DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_forecasts_hub_ts ON forecasts(hub, ts);

-- Index and grants for legacy table
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
GRANT ALL PRIVILEGES ON TABLE events TO postgres;
GRANT ALL PRIVILEGES ON TABLE prices TO postgres;
GRANT ALL PRIVILEGES ON TABLE trades TO postgres;
GRANT ALL PRIVILEGES ON TABLE positions_pnl TO postgres;
GRANT ALL PRIVILEGES ON TABLE forecasts TO postgres;
