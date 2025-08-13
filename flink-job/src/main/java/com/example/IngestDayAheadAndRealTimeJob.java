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

public class IngestDayAheadAndRealTimeJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        KafkaSource<DaLmp> daSource = KafkaSource.<DaLmp>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("dayahead_prices")
                .setGroupId("flink-da-ingest")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonToDaDeserializer())
                .build();

        KafkaSource<RtLmp> rtSource = KafkaSource.<RtLmp>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("realtime_prices")
                .setGroupId("flink-rt-ingest")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonToRtDeserializer())
                .build();

        DataStream<DaLmp> da = env.fromSource(daSource, WatermarkStrategy.noWatermarks(), "da-source");
        DataStream<RtLmp> rt = env.fromSource(rtSource, WatermarkStrategy.noWatermarks(), "rt-source");

        JdbcExecutionOptions execOpts = JdbcExecutionOptions.builder()
                .withBatchSize(500).withBatchIntervalMs(200).withMaxRetries(3).build();
        JdbcConnectionOptions connOpts = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://postgres:5432/flinkdb").withDriverName("org.postgresql.Driver")
                .withUsername("postgres").withPassword("postgres").build();

        da.addSink(JdbcSink.sink(
                "INSERT INTO dayahead_prices (ts, hub, lmp_da, energy_da, congestion_da, loss_da) VALUES (?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<DaLmp>) (ps, v) -> {
                    ps.setString(1, v.ts); ps.setString(2, v.hub);
                    ps.setDouble(3, v.lmpDa); ps.setDouble(4, v.energyDa);
                    ps.setDouble(5, v.congestionDa); ps.setDouble(6, v.lossDa);
                }, execOpts, connOpts)).name("dayahead-sink");

        rt.addSink(JdbcSink.sink(
                "INSERT INTO realtime_prices (ts, hub, lmp_rt, energy_rt, congestion_rt, loss_rt) VALUES (?, ?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<RtLmp>) (ps, v) -> {
                    ps.setString(1, v.ts); ps.setString(2, v.hub);
                    ps.setDouble(3, v.lmpRt); ps.setDouble(4, v.energyRt);
                    ps.setDouble(5, v.congestionRt); ps.setDouble(6, v.lossRt);
                }, execOpts, connOpts)).name("realtime-sink");

        env.execute("Ingest Day-Ahead and Real-Time Prices");
    }

    public static class DaLmp { public String ts; public String hub; public double lmpDa, energyDa, congestionDa, lossDa; }
    public static class RtLmp { public String ts; public String hub; public double lmpRt, energyRt, congestionRt, lossRt; }

    public static class JsonToDaDeserializer extends AbstractDeserializationSchema<DaLmp> {
        private static final ObjectMapper mapper = new ObjectMapper();
        @Override public DaLmp deserialize(byte[] message) throws IOException {
            JsonNode j = mapper.readTree(message); DaLmp d = new DaLmp();
            d.ts = j.get("ts").asText(); d.hub = j.get("hub").asText();
            d.lmpDa = j.get("lmp_da").asDouble(); d.energyDa = j.get("energy_da").asDouble();
            d.congestionDa = j.get("congestion_da").asDouble(); d.lossDa = j.get("loss_da").asDouble();
            return d;
        }
    }
    public static class JsonToRtDeserializer extends AbstractDeserializationSchema<RtLmp> {
        private static final ObjectMapper mapper = new ObjectMapper();
        @Override public RtLmp deserialize(byte[] message) throws IOException {
            JsonNode j = mapper.readTree(message); RtLmp r = new RtLmp();
            r.ts = j.get("ts").asText(); r.hub = j.get("hub").asText();
            r.lmpRt = j.get("lmp_rt").asDouble(); r.energyRt = j.get("energy_rt").asDouble();
            r.congestionRt = j.get("congestion_rt").asDouble(); r.lossRt = j.get("loss_rt").asDouble();
            return r;
        }
    }
}

