package br.com.carloskafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class App {

    static void runProducer() {
        Producer<Long, String> producer = getKafkaProducerOrCreateIfNotExists();
        for (int index = 0; index < 1_000_000; index++) {
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>("messages",
                    "carlos");
            try {
                RecordMetadata metadata = producer.send(record).get();
//                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
//                        + " with offset " + metadata.offset());
            } catch (ExecutionException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
    }

    private static KafkaProducer PRODUCER;

    public synchronized static Producer<Long, String> getKafkaProducerOrCreateIfNotExists() {
        if (PRODUCER == null) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "client1");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
            PRODUCER = new KafkaProducer<>(props);
        }
        return PRODUCER;
    }

    static class Word implements Serializable {
        private static final long serialVersionUID = 1L;
        private String word;
        private Tuple2<Integer, Integer> count;

        Word(String word, Tuple2<Integer, Integer> count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Tuple2<Integer, Integer> getCount() {
            return count;
        }

        public void setCount(Tuple2<Integer, Integer> count) {
            this.count = count;
        }
    }

    public static void main(String[] args) {
        ScheduledExecutorService scheduler = Executors
                .newScheduledThreadPool(1);

        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                JavaStreamingContext streamingContext = gettingJavaStreamingContext();
                JavaInputDStream<ConsumerRecord<String, String>> messagesFromKafka = getDStreamFromKafka(streamingContext);
                processObtainedDStream(streamingContext, messagesFromKafka);
                runApplication(streamingContext);
            }
        }, 0, 1, TimeUnit.MICROSECONDS);

        runProducerSpammerAsync();
    }

    private static void runProducerSpammerAsync() {
        ScheduledExecutorService scheduler = Executors
                .newScheduledThreadPool(1);

        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                runProducer();
            }
        }, 0, 1, TimeUnit.MICROSECONDS);
    }

    private static JavaStreamingContext gettingJavaStreamingContext() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")  // Delete this line when submitting to a cluster
                ;

        return new JavaStreamingContext(
                sparkConf, Durations.seconds(1));
    }

    private static void runApplication(JavaStreamingContext streamingContext) {
        try {
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (Exception e) {
            System.out.println("Error to run app.\nDetails: " + e.getMessage());
        }
    }

    private static JavaDStream<String> processObtainedDStream(JavaStreamingContext streamingContext, JavaInputDStream<ConsumerRecord<String, String>> messages) {
        JavaPairDStream<String, String> results = messages
                .mapToPair(
                        record -> {
                            Tuple2<String, String> tupla = new Tuple2<>(record.key(), record.value());

                            System.out.println("KEY: " + tupla._1() + " VALUE: " + tupla._2());

                            return tupla;
                        }
                );
        JavaDStream<String> lines = results
                .map(
                        tuple2 -> {
                            String tupla = tuple2._2();

                            System.out.println("VALUE: " + tupla);

                             return tupla;
                        }
                );

        JavaPairDStream<String, Integer> wordCounts = lines
                .mapToPair(
                        s -> {
                            Tuple2<String, Integer> tupla = new Tuple2<>(s, 1);
                            System.out.println("KEY: " + tupla._1() + " VALUE: " + tupla._2());
                            return tupla;
                        }
                ).reduceByKey(
                        (i1, i2) -> i1 + i2
                );

        JavaPairDStream<String, Tuple2<Integer, Integer>> joinedWordCount = wordCounts.join(wordCounts).window(Durations.seconds(10), Durations.seconds(1));

        joinedWordCount.foreachRDD(
                javaRdd -> {
                    Map<String, Tuple2<Integer, Integer>> wordCountMap = javaRdd.collectAsMap();
                    for (String key : wordCountMap.keySet()) {
                        List<Word> wordList = Arrays.asList(new Word(key, wordCountMap.get(key)));
                        JavaRDD<Word> rdd = streamingContext.sparkContext().parallelize(wordList);
                        rdd.foreach(word -> {
                                    System.out.println("Word: " + word.getWord() + " Count: " + word.getCount());
                                }
                        );
                    }
                }
        );

        return lines;
    }

    public static JavaInputDStream<ConsumerRecord<String, String>> getDStreamFromKafka(JavaStreamingContext streamingContext) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("messages");

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        return messages;
    }

}