package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import java.io.IOException;

public class KafkaToPostgresJob {

    public static void main(String[] args) throws Exception {
        // Execution env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000); // once

        // Kafka source -> Row(id:int, ts:string, value:double)
        KafkaSource<Row> kafkaSource = KafkaSource.<Row>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("events")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonToRowDeserializer())
                .build();

        DataStream<Row> stream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // JDBC sink (keeps ts as TEXT)
        JdbcExecutionOptions execOpts = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(3)
                .build();

        JdbcConnectionOptions connOpts = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://postgres:5432/flinkdb")
                .withDriverName("org.postgresql.Driver")
                .withUsername("postgres")
                .withPassword("postgres")
                .build();

        JdbcStatementBuilder<Row> binder = (ps, row) -> {
            ps.setInt(1, (Integer) row.getField(0));
            ps.setString(2, (String) row.getField(1));   // ts as TEXT (per your schema)
            ps.setDouble(3, (Double) row.getField(2));
        };

        SinkFunction<Row> jdbcSink = JdbcSink.<Row>sink(
                "INSERT INTO events (id, ts, value) VALUES (?, ?, ?)",
                binder,
                execOpts,
                connOpts
        );

        // Add sink and set parallelism (adjust to match Kafka partitions)
        stream.addSink(jdbcSink).name("PostgreSQL Sink").setParallelism(3);

        env.execute("Kafka to PostgreSQL Streaming Job");
    }

    // JSON -> Row(id:int, ts:string, value:double)
    public static class JsonToRowDeserializer extends AbstractDeserializationSchema<Row> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Row deserialize(byte[] message) throws IOException {
            JsonNode json = objectMapper.readTree(message);
            Row row = new Row(3);
            row.setField(0, json.get("id").asInt());
            row.setField(1, json.get("ts").asText());
            row.setField(2, json.get("value").asDouble());
            return row;
        }

        @Override
        public boolean isEndOfStream(Row nextElement) {
            return false;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return new RowTypeInfo(Types.INT, Types.STRING, Types.DOUBLE);
        }
    }
}
