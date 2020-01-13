# 问题

如何使用 Flink 读取 Kafka 中某个 topic 的数据，并将其打印在标准输出？

# 思路

> 参考链接：https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/kafka.html

1. 对于1.0之后的 Kafka，使用通用的 `flink-connector-kafka_2.11`，更早版本的 Kafka 使用专门的 connector。
2. 反序列化方案，即 `DeserializationSchema`，一般使用 `SimpleStringSchema` 即可。
3. 消费起始点，一般来说使用当前消费组的 offset 即可。
4. 暂时不考虑容错和检查点。
5. 因为只消费指定的单个 topic，分区发现也不考虑。
6. 不考虑容错的话，也不考虑 offset 提交。

# 答案

1. 在 `pom.xml`  中添加合适版本的 `flink-connector-kafka`，因为是本地测试，所以 Flink 的核心依赖没有加 `<scope>provided</provied>`。

   ```xml
   <properties>
     <flink.version>1.9.1</flink.version>
   </properties>
   <dependency>
     <groupId>org.apache.flink</groupId>
     <artifactId>flink-java</artifactId>
     <version>${flink.version}</version>
   </dependency>
   <dependency>
     <groupId>org.apache.flink</groupId>
     <artifactId>flink-streaming-java_2.11</artifactId>
     <version>${flink.version}</version>
   </dependency>
   <dependency>
     <groupId>org.apache.flink</groupId>
     <artifactId>flink-clients_2.11</artifactId>
     <version>${flink.version}</version>
   </dependency>
   <dependency>
     <groupId>org.apache.flink</groupId>
     <artifactId>flink-connector-kafka_2.11</artifactId>
     <version>${flink.version}</version>
   </dependency>
   ```

2. 关键代码如下，完整代码参见 [Kafka2Stdout.java](https://github.com/iamabug/flink-notes/blob/master/pipeline/flink-pipeline/src/main/java/iamabug/Kafka2Stdout.java)。

   ```java
   StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
   
   // Kafka 配置
   Properties properties = new Properties();
   properties.setProperty("bootstrap.servers", "localhost:9092");
   // 使用随机字符串作为group.id，以便程序重复运行
   properties.setProperty("group.id", String.valueOf(new Random().nextInt()));
   properties.setProperty("auto.offset.reset", "earliest");
   
   // 添加源并打印输出
   FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties);
   see.addSource(consumer).print();
   
   // 执行任务
   see.execute();
   ```

# 验证

1. 本地安装 Kafka 并启动，创建名为 `test` 的 topic，或者开启 topic 的自动创建，创建命令为：

   ```bash
   $ kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test
   ```

2. 在 IDEA 中运行上面的代码，然后向 `test` 中生产数据，生产命令为：

   ```bash
   $ kafka-console-producer.sh --broker-list localhost:9092 --topic test
   >hello world !
   >new year is coming !
   ```

3. 在 IDEA 中观察输出：

   ![](https://tva1.sinaimg.cn/large/006tNbRwly1gauuxcqvj9j30pc0523zg.jpg)
