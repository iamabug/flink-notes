# 问题

如何在不考虑 checkpoint 和 exactly-once 的情况下，使用 Flink 实现消费 Kafka 数据，然后再将处理后的数据 sink 到 Kafka 的流程 ？即 **at-least-once 语义的 Kafka2Kafka pipeline**。

# 思路

> 参考链接：https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/kafka.html

 `FlinkKafkaProducer` 有多个构造函数，但有的已经标记为 `deprecated`， `FlinkKafkaProducer(String, KafkaSerializationSchema<IN>, Properties, FlinkKafkaProducer.Semantic)` 这个是没废弃的 ，这个构造函数的第二个参数接收一个 `KafkaSerializationSchema` 对象，这个对象需要实现如下方法：

```java
ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp);
```

参照 [StackOverflow 上的相关问答](https://stackoverflow.com/questions/58644549/how-to-implement-flinkkafkaproducer-serializer-for-kafka-2-2)，对于生产消息为字符串且没有 Key 的简单实现为：

```java
new KafkaSerializationSchema<String> () {
  ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp) {
    return new ProducerRecord<>("target", element.getBytes(StandardCharsets.UTF_8)),
  }
}
```

它是个函数式接口，可以简化成 Lambda 表达式：

```java
(element, timestamp) -> new ProducerRecord<>("target", element.getBytes(StandardCharsets.UTF_8))
```



# 答案

从名为 `source` 的 topic 中消费数据，然后直接写入名为 `target` 的 topic，核心代码如下，完整代码参见 [Kafka2KafkaAtLeastOnce.java](https://github.com/iamabug/flink-notes/blob/master/pipeline/flink-pipeline/src/main/java/iamabug/Kafka2KafkaAtLeastOnce.java)。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Kafka Consumer 配置
Properties prop1 = new Properties();
prop1.setProperty("bootstrap.servers", "localhost:9092");
// 使用随机字符串作为group.id，以便程序重复运行
prop1.setProperty("group.id", String.valueOf(new Random().nextInt()));
prop1.setProperty("auto.offset.reset", "earliest");

// 添加源并打印输出
FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
  "source",
  new SimpleStringSchema(), 
  prop1
);

DataStream result = env.addSource(consumer);

// Kafka Producer 配置
Properties prop2 = new Properties();
prop2.setProperty("bootstrap.servers", "localhost:9092");

FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
	"target",
	(element, timestamp) ->
		new ProducerRecord<>("target", element.getBytes(StandardCharsets.UTF_8)),
	Utils.buildKafkaProducerProperties(),
	FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
);

result.addSink(producer);
// 执行任务
env.execute();
```

# 验证

1. 启动 Kafka，创建两个 topic：`source` 和 `target`，然后在命令行消费 `target`：

   ```bash
   kafka-console-consumer.sh --topic target --bootstrap-server localhost:9092
   ```

2. 在 IDEA 中运行 `Kafka2KafkaAtLeastOnce`  代码

3. 向 `source` 中生产数据：

   ![](https://tva1.sinaimg.cn/large/006tNbRwly1gawwsc0psqj30tf03zgm0.jpg)

4. 观察 `target` 的输出是否和向 `source` 中生产的一样：

   ![](https://tva1.sinaimg.cn/large/006tNbRwly1gawwsv2qgoj30v303w0t4.jpg)