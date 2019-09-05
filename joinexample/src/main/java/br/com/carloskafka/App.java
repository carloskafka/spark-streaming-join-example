package br.com.carloskafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class App {

    public static final Duration MICROBATCH_DURATION = Durations.milliseconds(250);
    private static final Duration WINDOW_DURATION = Durations.seconds(15);
    private static final Duration SLIDING_INTERVAL_IN_WINDOW_DURATION = Durations.milliseconds(500);
    public static final String TOPIC_NAME = "messages";
    public static final String TOPIC_NAME_2 = "messages2";

    static class Objeto implements Serializable {
        private String agencia;
        private String conta;
        private String numerMov;
        private String valorAleatorio;

        public Objeto() {
        }

        public Objeto(String agencia, String conta, String numerMov) {
            this.agencia = agencia;
            this.conta = conta;
            this.numerMov = numerMov;
            this.valorAleatorio = UUID.randomUUID().toString();
        }

        public String getAgencia() {
            return agencia;
        }

        public String getConta() {
            return conta;
        }

        public String getNumerMov() {
            return numerMov;
        }

        public String key() {
            return getAgencia() + getConta() + getNumerMov();
        }

        public String getValorAleatorio() {
            return valorAleatorio;
        }

        @Override
        public String toString() {
            return "Objeto{" +
                    "agencia='" + agencia + '\'' +
                    ", conta='" + conta + '\'' +
                    ", numerMov='" + numerMov + '\'' +
                    ", valorAleatorio='" + valorAleatorio + '\'' +
                    ", key='" + key() + '\'' +
                    '}';
        }
    }

    public static final int AMOUNT_OF_EVENTS = 4;

    static void runProducer2() {
        try {
            Producer<Long, String> producer = getKafkaProducerOrCreateIfNotExists();
            for (int index = 1; index <= AMOUNT_OF_EVENTS; index++) {
                ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC_NAME_2,
                        new ObjectMapper().writeValueAsString(new Objeto("agencia", "conta", "numerMov" + index)));
                try {
                    producer.send(record);
//                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
//                        + " with offset " + metadata.offset());
                } catch (Exception e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                }
            }
        } catch (Exception e) {
            System.out.println("Error to produce events to topic kafka.\nDetails: " + e.getMessage());
        }
    }

    static void runProducer() {
        try {
            Producer<Long, String> producer = getKafkaProducerOrCreateIfNotExists();
            for (int index = 1; index <= AMOUNT_OF_EVENTS; index++) {
                ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC_NAME,
                        new ObjectMapper().writeValueAsString(new Objeto("agencia", "conta", "numerMov" + index)));
                try {
                    producer.send(record);
//                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
//                        + " with offset " + metadata.offset());
                } catch (Exception e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                }
            }
        } catch (Exception e) {
            System.out.println("Error to produce events to topic kafka.\nDetails: " + e.getMessage());
        }
    }

    private static KafkaProducer PRODUCER;

    public synchronized static Producer<Long, String> getKafkaProducerOrCreateIfNotExists() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        PRODUCER = new KafkaProducer<>(props);
        return PRODUCER;
    }

    public static void main(String[] args) {
        //runJoin();
        runTwoProducersSpammerAsync();
    }

    private static void runJoin() {
//        ScheduledExecutorService scheduler = Executors
//                .newScheduledThreadPool(1);
//
//        scheduler.scheduleWithFixedDelay(new Runnable() {
//            public void run() {
        // Start Spark
        JavaStreamingContext streamingContext = gettingJavaStreamingContext();

        // Consume From Two Kafka Topics
        JavaInputDStream<ConsumerRecord<String, String>> messagesFromKafka = getDStreamFromFirstTopicKafka(streamingContext);
        JavaInputDStream<ConsumerRecord<String, String>> messagesFromKafka2 = getDStreamFromSecondTopicKafka(streamingContext);

        // Join
        processObtainedDStream(streamingContext, messagesFromKafka, messagesFromKafka2);

        runApplication(streamingContext);
//            }
//        }, 0, 1, TimeUnit.SECONDS);
    }

    private static void runTwoProducersSpammerAsync() {
//        ScheduledExecutorService scheduler = Executors
//                .newScheduledThreadPool(1);
//
//        scheduler.scheduleWithFixedDelay(new Runnable() {
//            public void run() {
        runProducer();
        runProducer2();
//            }
//        }, 0, 1, TimeUnit.SECONDS);
    }

    private static JavaStreamingContext gettingJavaStreamingContext() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .set("spark.streaming.kafka.consumer.cache.enabled", "false")
                .set("spark.streaming.backpressure.enabled", "true")
                .setMaster("local[*]")  // Delete this line when submitting to a cluster
                ;

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(
                sparkConf, MICROBATCH_DURATION);

        javaStreamingContext.sparkContext().setLogLevel("ERROR");

        return javaStreamingContext;
    }

    private static void runApplication(JavaStreamingContext streamingContext) {
        try {
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (Exception e) {
            System.out.println("Error to run app.\nDetails: " + e.getMessage());
        }
    }

    private static JavaPairDStream<String, Tuple2<Objeto, Objeto>> processObtainedDStream(JavaStreamingContext streamingContext, JavaInputDStream<ConsumerRecord<String, String>> messages, JavaInputDStream<ConsumerRecord<String, String>> messages2) {
        JavaPairDStream<String, Objeto> results = messages
                .mapToPair(
                        record -> {

                            Objeto objeto = new ObjectMapper().readValue(record.value(), Objeto.class);

                            Tuple2<String, Objeto> tupla = new Tuple2<String, Objeto>(objeto.key(), objeto);

                            System.out.println("Tupla received from first topic kafka " + TOPIC_NAME);
                            System.out.println(tupla);

                            return tupla;
                        }
                );

        JavaPairDStream<String, Objeto> results2 = messages2
                .mapToPair(
                        record -> {

                            Objeto objeto = new ObjectMapper().readValue(record.value(), Objeto.class);

                            Tuple2<String, Objeto> tupla = new Tuple2<String, Objeto>(objeto.key(), objeto);

                            System.out.println("Tupla received from second topic kafka " + TOPIC_NAME_2);
                            System.out.println(tupla);

                            return tupla;
                        }
                );

        JavaPairDStream<String, Tuple2<Objeto, Objeto>> joinedWordCount = results.join(results2)
                .window(WINDOW_DURATION, SLIDING_INTERVAL_IN_WINDOW_DURATION);

        joinedWordCount.print();

        return joinedWordCount;
    }

    public static JavaInputDStream<ConsumerRecord<String, String>> getDStreamFromFirstTopicKafka(JavaStreamingContext streamingContext) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", UUID.randomUUID().toString());
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList(TOPIC_NAME);

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        return messages;
    }

    public static JavaInputDStream<ConsumerRecord<String, String>> getDStreamFromSecondTopicKafka(JavaStreamingContext streamingContext) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", UUID.randomUUID().toString());
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList(TOPIC_NAME_2);

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        return messages;
    }

}