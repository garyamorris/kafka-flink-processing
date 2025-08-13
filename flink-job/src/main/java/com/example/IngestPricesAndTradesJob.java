package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class IngestPricesAndTradesJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        KafkaSource<PriceTick> pricesSource = KafkaSource.<PriceTick>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("prices")
                .setGroupId("flink-prices-ingest")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonToPriceDeserializer())
                .build();

        KafkaSource<Trade> tradesSource = KafkaSource.<Trade>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("trades")
                .setGroupId("flink-trades-ingest")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonToTradeDeserializer())
                .build();

        DataStream<PriceTick> prices = env.fromSource(pricesSource, WatermarkStrategy.noWatermarks(), "prices-source");
        DataStream<Trade> trades = env.fromSource(tradesSource, WatermarkStrategy.noWatermarks(), "trades-source");

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

        env.execute("Ingest Prices and Trades");
    }

    public static class PriceTick {
        public String ts; public String hub; public double priceMwh;
    }

    public static class Trade {
        public long tradeId; public String ts; public String account; public String hub; public String side; public int mw; public double priceMwh;
    }

    public static class JsonToPriceDeserializer extends AbstractDeserializationSchema<PriceTick> {
        private static final ObjectMapper mapper = new ObjectMapper();
        @Override public PriceTick deserialize(byte[] message) throws IOException {
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
        @Override public Trade deserialize(byte[] message) throws IOException {
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
}

