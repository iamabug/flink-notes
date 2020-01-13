package iamabug;

import java.util.Properties;
import java.util.Random;

public class Utils {

    public static Properties buildKafkaConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", String.valueOf(new Random().nextInt()));
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }
}
