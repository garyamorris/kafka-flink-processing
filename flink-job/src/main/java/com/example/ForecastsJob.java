package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ForecastsJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        KafkaSource<PriceTick> pricesSource = KafkaSource.<PriceTick>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("prices")
                .setGroupId("flink-forecasts")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonToPriceDeserializer())
                .build();

        DataStream<PriceTick> prices = env.fromSource(pricesSource, WatermarkStrategy.noWatermarks(), "prices-source");

        DataStream<Forecast> forecasts = prices.keyBy(p -> p.hub).process(new ForecastFunction());

        JdbcExecutionOptions execOpts = JdbcExecutionOptions.builder()
                .withBatchSize(500).withBatchIntervalMs(200).withMaxRetries(3).build();
        JdbcConnectionOptions connOpts = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://postgres:5432/flinkdb").withDriverName("org.postgresql.Driver")
                .withUsername("postgres").withPassword("postgres").build();

        forecasts.addSink(JdbcSink.sink(
                "INSERT INTO forecasts (ts, hub, sma5, sma20, forecast_next) VALUES (?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<Forecast>) (ps, f) -> {
                    ps.setString(1, f.ts);
                    ps.setString(2, f.hub);
                    ps.setDouble(3, f.sma5);
                    ps.setDouble(4, f.sma20);
                    ps.setDouble(5, f.forecastNext);
                }, execOpts, connOpts)).name("forecasts-sink");

        env.execute("Forecasts (SMA5/20) by Hub");
    }

    public static class PriceTick { public String ts; public String hub; public double priceMwh; }
    public static class Forecast { public String ts; public String hub; public double sma5; public double sma20; public double forecastNext; }

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

    public static class ForecastFunction extends KeyedProcessFunction<String, PriceTick, Forecast> {
        private transient ListState<Double> last5; private transient ListState<Double> last20;
        @Override public void open(org.apache.flink.configuration.Configuration parameters) {
            last5 = getRuntimeContext().getListState(new ListStateDescriptor<>("sma5", TypeInformation.of(Double.class)));
            last20 = getRuntimeContext().getListState(new ListStateDescriptor<>("sma20", TypeInformation.of(Double.class)));
        }
        @Override public void processElement(PriceTick v, Context ctx, Collector<Forecast> out) throws Exception {
            addAndTrim(last5, v.priceMwh, 5); addAndTrim(last20, v.priceMwh, 20);
            Forecast f = new Forecast(); f.ts = v.ts; f.hub = v.hub; f.sma5 = avgOf(last5); f.sma20 = avgOf(last20); f.forecastNext = f.sma5; out.collect(f);
        }
        private void addAndTrim(ListState<Double> s, double x, int max) throws Exception { List<Double> l=new ArrayList<>(); for(Double d:s.get()) l.add(d); l.add(x); while(l.size()>max) l.remove(0); s.update(l);}        
        private double avgOf(ListState<Double> s) throws Exception { double sum=0; int n=0; for(Double d:s.get()){sum+=d;n++;} return n==0?Double.NaN:sum/n; }
    }
}

