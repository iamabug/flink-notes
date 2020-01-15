package iamabug;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class Kafka2Stdout {
    public static void main(String[] args)  throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(),
                Utils.buildKafkaConsumerProperties());
        env.addSource(consumer).print();
        env.execute();
    }

}
