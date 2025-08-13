package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class PnlAndExposureJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        KafkaSource<PriceTick> pricesSource = KafkaSource.<PriceTick>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("prices")
                .setGroupId("flink-pnl-prices")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonToPriceDeserializer())
                .build();

        KafkaSource<Trade> tradesSource = KafkaSource.<Trade>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("trades")
                .setGroupId("flink-pnl-trades")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonToTradeDeserializer())
                .build();

        DataStream<PriceTick> prices = env.fromSource(pricesSource, WatermarkStrategy.noWatermarks(), "prices-source");
        DataStream<Trade> trades = env.fromSource(tradesSource, WatermarkStrategy.noWatermarks(), "trades-source");

        ConnectedStreams<Trade, PriceTick> connected = trades.keyBy(t -> t.hub).connect(prices.keyBy(p -> p.hub));
        DataStream<PnlRecord> pnl = connected.process(new PnlCalculator());

        JdbcExecutionOptions execOpts = JdbcExecutionOptions.builder().withBatchSize(500).withBatchIntervalMs(200).withMaxRetries(3).build();
        JdbcConnectionOptions connOpts = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://postgres:5432/flinkdb").withDriverName("org.postgresql.Driver").withUsername("postgres").withPassword("postgres").build();

        pnl.addSink(JdbcSink.sink(
                "INSERT INTO positions_pnl (ts, account, hub, position_mw, avg_price_mwh, last_price_mwh, realized_pnl, unrealized_pnl, total_pnl) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<PnlRecord>) (ps, r) -> {
                    ps.setString(1, r.ts); ps.setString(2, r.account); ps.setString(3, r.hub);
                    ps.setInt(4, r.positionMw); ps.setDouble(5, r.avgPriceMwh); ps.setDouble(6, r.lastPriceMwh);
                    ps.setDouble(7, r.realizedPnl); ps.setDouble(8, r.unrealizedPnl); ps.setDouble(9, r.totalPnl);
                }, execOpts, connOpts)).name("positions-pnl-sink");

        DataStream<ExposureRecord> exposure = pnl.map(r -> {
            ExposureRecord e = new ExposureRecord();
            e.ts = r.ts; e.account = r.account; e.hub = r.hub; e.positionMw = r.positionMw;
            e.lastPriceMwh = r.lastPriceMwh; e.pnl01 = r.positionMw; e.notionalUsd = r.positionMw * r.lastPriceMwh; return e;
        });

        exposure.addSink(JdbcSink.sink(
                "INSERT INTO price_exposure (ts, account, hub, position_mw, last_price_mwh, pnl01, notional_usd) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<ExposureRecord>) (ps, e) -> {
                    ps.setString(1, e.ts); ps.setString(2, e.account); ps.setString(3, e.hub);
                    ps.setInt(4, e.positionMw); ps.setDouble(5, e.lastPriceMwh); ps.setDouble(6, e.pnl01); ps.setDouble(7, e.notionalUsd);
                }, execOpts, connOpts)).name("price-exposure-sink");

        env.execute("PnL and Price Exposure by Hub");
    }

    // Models
    public static class PriceTick { public String ts; public String hub; public double priceMwh; }
    public static class Trade { public long tradeId; public String ts; public String account; public String hub; public String side; public int mw; public double priceMwh; }
    public static class PnlRecord { public String ts; public String account; public String hub; public int positionMw; public double avgPriceMwh; public double lastPriceMwh; public double realizedPnl; public double unrealizedPnl; public double totalPnl; }
    public static class ExposureRecord { public String ts; public String account; public String hub; public int positionMw; public double lastPriceMwh; public double pnl01; public double notionalUsd; }
    public static class PositionAccumulator { public int positionMw; public double avgPriceMwh; public double realizedPnl; }

    // Serdes
    public static class JsonToPriceDeserializer extends AbstractDeserializationSchema<PriceTick> {
        private static final ObjectMapper mapper = new ObjectMapper();
        @Override public PriceTick deserialize(byte[] message) throws IOException {
            JsonNode j = mapper.readTree(message); PriceTick p = new PriceTick();
            p.ts = j.get("ts").asText(); p.hub = j.get("hub").asText(); p.priceMwh = j.get("price_mwh").asDouble(); return p;
        }
    }
    public static class JsonToTradeDeserializer extends AbstractDeserializationSchema<Trade> {
        private static final ObjectMapper mapper = new ObjectMapper();
        @Override public Trade deserialize(byte[] message) throws IOException {
            JsonNode j = mapper.readTree(message); Trade t = new Trade();
            t.tradeId = j.get("trade_id").asLong(); t.ts = j.get("ts").asText(); t.account = j.get("account").asText(); t.hub = j.get("hub").asText();
            t.side = j.get("side").asText(); t.mw = j.get("mw").asInt(); t.priceMwh = j.get("price_mwh").asDouble(); return t;
        }
    }

    // PnL calculator
    public static class PnlCalculator extends KeyedCoProcessFunction<String, Trade, PriceTick, PnlRecord> {
        private transient ValueState<Double> lastPrice; private transient MapState<String, PositionAccumulator> perAccount;
        @Override public void open(org.apache.flink.configuration.Configuration parameters) {
            lastPrice = getRuntimeContext().getState(new ValueStateDescriptor<>("lastPrice", TypeInformation.of(Double.class)));
            perAccount = getRuntimeContext().getMapState(new MapStateDescriptor<>("perAccount", TypeInformation.of(String.class), TypeInformation.of(PositionAccumulator.class)));
        }

        @Override public void processElement1(Trade trade, Context ctx, Collector<PnlRecord> out) throws Exception {
            PositionAccumulator acc = perAccount.get(trade.account);
            if (acc == null) { acc = new PositionAccumulator(); acc.positionMw = 0; acc.avgPriceMwh = 0.0; acc.realizedPnl = 0.0; }
            int signedQty = trade.side.equalsIgnoreCase("BUY") ? trade.mw : -trade.mw;
            if (acc.positionMw == 0 || Integer.signum(acc.positionMw) == Integer.signum(signedQty)) {
                int newPos = acc.positionMw + signedQty; double newAvg = (newPos != 0)
                        ? (Math.abs(acc.positionMw) * acc.avgPriceMwh + Math.abs(signedQty) * trade.priceMwh) / Math.abs(newPos)
                        : 0.0; acc.positionMw = newPos; acc.avgPriceMwh = newAvg;
            } else {
                int closingQty = Math.min(Math.abs(acc.positionMw), Math.abs(signedQty));
                double pnlPerUnit = (Integer.signum(acc.positionMw) > 0) ? (trade.priceMwh - acc.avgPriceMwh) : (acc.avgPriceMwh - trade.priceMwh);
                acc.realizedPnl += closingQty * pnlPerUnit;
                int residual = Math.abs(signedQty) - closingQty;
                if (residual == 0) { acc.positionMw = 0; acc.avgPriceMwh = 0.0; }
                else { int newSign = Integer.signum(signedQty); acc.positionMw = newSign * residual; acc.avgPriceMwh = trade.priceMwh; }
            }
            perAccount.put(trade.account, acc);
            double lp = lastPrice.value() == null ? trade.priceMwh : lastPrice.value();
            emit(out, trade.ts, trade.account, trade.hub, acc, lp);
        }

        @Override public void processElement2(PriceTick price, Context ctx, Collector<PnlRecord> out) throws Exception {
            lastPrice.update(price.priceMwh);
            for (String account : perAccount.keys()) { PositionAccumulator acc = perAccount.get(account); emit(out, price.ts, account, price.hub, acc, price.priceMwh); }
        }

        private void emit(Collector<PnlRecord> out, String ts, String account, String hub, PositionAccumulator acc, double lp) {
            double unrealized = acc.positionMw * (lp - acc.avgPriceMwh);
            PnlRecord r = new PnlRecord(); r.ts = ts; r.account = account; r.hub = hub; r.positionMw = acc.positionMw;
            r.avgPriceMwh = acc.avgPriceMwh; r.lastPriceMwh = lp; r.realizedPnl = acc.realizedPnl; r.unrealizedPnl = unrealized; r.totalPnl = r.realizedPnl + unrealized; out.collect(r);
        }
    }
}

