package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

public class KafkaToPostgresJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        // Sources: energy prices and trades
        KafkaSource<PriceTick> pricesSource = KafkaSource.<PriceTick>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("prices")
                .setGroupId("flink-prices")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonToPriceDeserializer())
                .build();

        KafkaSource<Trade> tradesSource = KafkaSource.<Trade>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("trades")
                .setGroupId("flink-trades")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonToTradeDeserializer())
                .build();

        DataStream<PriceTick> prices = env.fromSource(
                pricesSource,
                WatermarkStrategy.noWatermarks(),
                "prices-source");

        DataStream<Trade> trades = env.fromSource(
                tradesSource,
                WatermarkStrategy.noWatermarks(),
                "trades-source");

        // Sink raw prices to Postgres
        JdbcExecutionOptions execOpts = JdbcExecutionOptions.builder()
                .withBatchSize(500)
                .withBatchIntervalMs(200)
                .withMaxRetries(3)
                .build();

        JdbcConnectionOptions connOpts = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://postgres:5432/flinkdb")
                .withDriverName("org.postgresql.Driver")
                .withUsername("postgres")
                .withPassword("postgres")
                .build();

        prices.addSink(JdbcSink.sink(
                "INSERT INTO prices (ts, hub, price_mwh) VALUES (?, ?, ?)",
                (JdbcStatementBuilder<PriceTick>) (ps, p) -> {
                    ps.setString(1, p.ts);
                    ps.setString(2, p.hub);
                    ps.setDouble(3, p.priceMwh);
                }, execOpts, connOpts)).name("prices-sink");

        trades.addSink(JdbcSink.sink(
                "INSERT INTO trades (trade_id, ts, account, hub, side, mw, price_mwh) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<Trade>) (ps, t) -> {
                    ps.setLong(1, t.tradeId);
                    ps.setString(2, t.ts);
                    ps.setString(3, t.account);
                    ps.setString(4, t.hub);
                    ps.setString(5, t.side);
                    ps.setInt(6, t.mw);
                    ps.setDouble(7, t.priceMwh);
                }, execOpts, connOpts)).name("trades-sink");

        // Compute forecasts on prices (SMA5/20)
        DataStream<Forecast> forecasts = prices
                .keyBy(p -> p.hub)
                .process(new ForecastFunction());

        forecasts.addSink(JdbcSink.sink(
                "INSERT INTO forecasts (ts, hub, sma5, sma20, forecast_next) VALUES (?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<Forecast>) (ps, f) -> {
                    ps.setString(1, f.ts);
                    ps.setString(2, f.hub);
                    ps.setDouble(3, f.sma5);
                    ps.setDouble(4, f.sma20);
                    ps.setDouble(5, f.forecastNext);
                }, execOpts, connOpts)).name("forecasts-sink");

        // Compute PnL by connecting trades with prices keyed by symbol
        ConnectedStreams<Trade, PriceTick> connected = trades
                .keyBy(t -> t.hub)
                .connect(prices.keyBy(p -> p.hub));

        DataStream<PnlRecord> pnl = connected.process(new PnlCalculator());

        pnl.addSink(JdbcSink.sink(
                "INSERT INTO positions_pnl (ts, account, hub, position_mw, avg_price_mwh, last_price_mwh, realized_pnl, unrealized_pnl, total_pnl) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<PnlRecord>) (ps, r) -> {
                    ps.setString(1, r.ts);
                    ps.setString(2, r.account);
                    ps.setString(3, r.hub);
                    ps.setInt(4, r.positionMw);
                    ps.setDouble(5, r.avgPriceMwh);
                    ps.setDouble(6, r.lastPriceMwh);
                    ps.setDouble(7, r.realizedPnl);
                    ps.setDouble(8, r.unrealizedPnl);
                    ps.setDouble(9, r.totalPnl);
                }, execOpts, connOpts)).name("positions-pnl-sink");

        env.execute("Trading PnL and Forecasts Job");
    }

    // Models
    public static class PriceTick {
        public String ts;
        public String hub;
        public double priceMwh;
    }

    public static class Trade {
        public long tradeId;
        public String ts;
        public String account;
        public String hub;
        public String side; // BUY / SELL
        public int mw;
        public double priceMwh;
    }

    public static class Forecast {
        public String ts;
        public String hub;
        public double sma5;
        public double sma20;
        public double forecastNext;
    }

    public static class PnlRecord {
        public String ts;
        public String account;
        public String hub;
        public int positionMw;
        public double avgPriceMwh;
        public double lastPriceMwh;
        public double realizedPnl;
        public double unrealizedPnl;
        public double totalPnl;
    }

    public static class PositionAccumulator {
        public int positionMw; // signed MW
        public double avgPriceMwh; // $/MWh
        public double realizedPnl;

        public PositionAccumulator() {}
    }

    // Deserializers
    public static class JsonToPriceDeserializer extends AbstractDeserializationSchema<PriceTick> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public PriceTick deserialize(byte[] message) throws IOException {
            JsonNode j = mapper.readTree(message);
            PriceTick p = new PriceTick();
            p.ts = j.get("ts").asText();
            p.hub = j.get("hub").asText();
            p.priceMwh = j.get("price_mwh").asDouble();
            return p;
        }
    }

    public static class JsonToTradeDeserializer extends AbstractDeserializationSchema<Trade> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public Trade deserialize(byte[] message) throws IOException {
            JsonNode j = mapper.readTree(message);
            Trade t = new Trade();
            t.tradeId = j.get("trade_id").asLong();
            t.ts = j.get("ts").asText();
            t.account = j.get("account").asText();
            t.hub = j.get("hub").asText();
            t.side = j.get("side").asText();
            t.mw = j.get("mw").asInt();
            t.priceMwh = j.get("price_mwh").asDouble();
            return t;
        }
    }

    // Forecast SMA5/20 per symbol
    public static class ForecastFunction extends KeyedProcessFunction<String, PriceTick, Forecast> {
        private transient ListState<Double> last5;
        private transient ListState<Double> last20;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            last5 = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "sma5", TypeInformation.of(Double.class)));
            last20 = getRuntimeContext().getListState(new ListStateDescriptor<>(
                    "sma20", TypeInformation.of(Double.class)));
        }

        @Override
        public void processElement(PriceTick value, Context ctx, Collector<Forecast> out) throws Exception {
            addAndTrim(last5, value.price, 5);
            addAndTrim(last20, value.price, 20);

            Forecast f = new Forecast();
            f.ts = value.ts;
            f.hub = value.hub;
            f.sma5 = avgOf(last5);
            f.sma20 = avgOf(last20);
            f.forecastNext = f.sma5; // simple forecast
            out.collect(f);
        }

        private void addAndTrim(ListState<Double> state, double v, int max) throws Exception {
            List<Double> list = new ArrayList<>();
            for (Double d : state.get()) list.add(d);
            list.add(v);
            while (list.size() > max) list.remove(0);
            state.update(list);
        }

        private double avgOf(ListState<Double> state) throws Exception {
            double sum = 0; int n = 0;
            for (Double d : state.get()) { sum += d; n++; }
            return n == 0 ? Double.NaN : sum / n;
        }
    }

    // PnL calculator connecting trades and prices keyed by symbol
    public static class PnlCalculator extends KeyedCoProcessFunction<String, Trade, PriceTick, PnlRecord> {
        private transient ValueState<Double> lastPrice;
        private transient MapState<String, PositionAccumulator> perAccount;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            lastPrice = getRuntimeContext().getState(new ValueStateDescriptor<>(
                    "lastPrice", TypeInformation.of(Double.class)));
            perAccount = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                    "perAccount", TypeInformation.of(String.class), TypeInformation.of(PositionAccumulator.class)));
        }

        @Override
        public void processElement1(Trade trade, Context ctx, Collector<PnlRecord> out) throws Exception {
            PositionAccumulator acc = perAccount.get(trade.account);
            if (acc == null) {
                acc = new PositionAccumulator();
                acc.positionMw = 0;
                acc.avgPriceMwh = 0.0;
                acc.realizedPnl = 0.0;
            }

            int signedQty = trade.side.equalsIgnoreCase("BUY") ? trade.mw : -trade.mw;

            if (acc.positionMw == 0 || Integer.signum(acc.positionMw) == Integer.signum(signedQty)) {
                int newPos = acc.positionMw + signedQty;
                double newAvg;
                if (newPos != 0) {
                    newAvg = (Math.abs(acc.positionMw) * acc.avgPriceMwh + Math.abs(signedQty) * trade.priceMwh) / Math.abs(newPos);
                } else {
                    newAvg = 0.0;
                }
                acc.positionMw = newPos;
                acc.avgPriceMwh = newAvg;
            } else {
                int closingQty = Math.min(Math.abs(acc.positionMw), Math.abs(signedQty));
                double pnlPerUnit = (Integer.signum(acc.positionMw) > 0) ? (trade.priceMwh - acc.avgPriceMwh) : (acc.avgPriceMwh - trade.priceMwh);
                acc.realizedPnl += closingQty * pnlPerUnit;

                int residual = Math.abs(signedQty) - closingQty;
                if (residual == 0) {
                    acc.positionMw = 0;
                    acc.avgPriceMwh = 0.0;
                } else {
                    int newSign = Integer.signum(signedQty);
                    acc.positionMw = newSign * residual;
                    acc.avgPriceMwh = trade.priceMwh;
                }
            }

            perAccount.put(trade.account, acc);

            double lp = lastNonNull(lastPrice.value(), trade.priceMwh);
            emitRecord(out, trade.ts, trade.account, trade.hub, acc, lp);
        }

        @Override
        public void processElement2(PriceTick price, Context ctx, Collector<PnlRecord> out) throws Exception {
            lastPrice.update(price.priceMwh);
            for (String account : perAccount.keys()) {
                PositionAccumulator acc = perAccount.get(account);
                emitRecord(out, price.ts, account, price.hub, acc, price.priceMwh);
            }
        }

        private void emitRecord(Collector<PnlRecord> out, String ts, String account, String hub, PositionAccumulator acc, double lp) {
            double unrealized = acc.positionMw * (lp - acc.avgPriceMwh);
            PnlRecord r = new PnlRecord();
            r.ts = ts;
            r.account = account;
            r.hub = hub;
            r.positionMw = acc.positionMw;
            r.avgPriceMwh = acc.avgPriceMwh;
            r.lastPriceMwh = lp;
            r.realizedPnl = acc.realizedPnl;
            r.unrealizedPnl = unrealized;
            r.totalPnl = acc.realizedPnl + unrealized;
            out.collect(r);
        }

        private double lastNonNull(Double a, double fallback) {
            return a == null ? fallback : a;
        }
    }
}
