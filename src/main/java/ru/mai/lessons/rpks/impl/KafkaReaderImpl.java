package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class KafkaReaderImpl implements KafkaReader {

    private final KafkaConsumer<String, String> consumer;
    private final RuleProcessor ruleProcessor;
    private final DbReader dbReader;
    private final KafkaWriter kafkaWriter;
    private final String inputTopic;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile boolean running = true;

    public KafkaReaderImpl(Config config, RuleProcessor ruleProcessor, DbReader dbReader, KafkaWriter kafkaWriter) {
        this.consumer = new KafkaConsumer<>(buildConsumerProperties(config));
        this.ruleProcessor = ruleProcessor;
        this.dbReader = dbReader;
        this.kafkaWriter = kafkaWriter;
        this.inputTopic = config.hasPath("kafka.consumer.inputTopic")
                ? config.getString("kafka.consumer.inputTopic")
                : "default_input_topic";
        log.info("Kafka Reader created, inputTopic {}", inputTopic);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    private void shutdown() {
        log.info("Shutting down KafkaReader");
        running = false;
        consumer.wakeup();
        executor.shutdown();
    }

    private Properties buildConsumerProperties(Config config) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.consumer.bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka.consumer.group.id"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString("kafka.consumer.auto.offset.reset"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Override
    public void processing() {
        executor.submit(() -> {
            log.info("Subscribed to Kafka topic: {}", inputTopic);
            consumer.subscribe(Collections.singleton(inputTopic));
            try {
                while (running) {
                    consumer.poll(Duration.ofMillis(500))
                            .forEach(record -> processRecord(record, dbReader.readRulesFromDB()));
                }
            } catch (Exception e) {
                log.error("Kafka consumer error", e);
            } finally {
                consumer.close();
                log.info("Kafka consumer closed.");
            }
        });
    }

    private void processRecord(ConsumerRecord<String, String> record, Rule[] rules) {
        Optional.ofNullable(record.value())
                .map(value -> Message.builder().value(value).build())
                .map(message -> ruleProcessor.processing(message, rules))
                .filter(processed -> processed != null && processed.getValue() != null)
                .ifPresentOrElse(
                        processed -> {
                            kafkaWriter.processing(processed);
                            log.info("Message passed filters and sent: {}", processed.getValue());
                        },
                        () -> log.info("Message filtered out: {}", record.value())
                );
    }
}
