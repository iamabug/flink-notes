package iamabug;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class Kafka2KafkaAtLeastOnce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "source",
                new SimpleStringSchema(),
                Utils.buildKafkaConsumerProperties()
        );
        DataStream result = env.addSource(consumer);
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "target",
                (element, timestamp) ->
                        new ProducerRecord<>("target", element.getBytes(StandardCharsets.UTF_8)),
                Utils.buildKafkaProducerProperties(),
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
        result.addSink(producer);
        env.execute();
    }
}
