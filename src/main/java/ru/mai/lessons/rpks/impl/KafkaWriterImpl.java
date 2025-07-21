package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Properties;

@Slf4j
public class KafkaWriterImpl  implements KafkaWriter {
    private final KafkaProducer<String, String> producer;
    private final String outputTopic;

    public KafkaWriterImpl(Config config) {
        this.producer = new KafkaProducer<>(buildProducerProperties(config));
        this.outputTopic = config.hasPath("kafka.producer.outputTopic")
                ? config.getString("kafka.producer.outputTopic")
                : "default_output_topic";
        log.info("Kafka Writer created, outputTopic {}", outputTopic);
    }

    private Properties buildProducerProperties(Config config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.producer.bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    @Override
    public void processing(Message message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outputTopic, message.getValue());
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send message to Kafka", exception);
            } else {
                log.info("Message sent to Kafka topic {} partition {} offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }
}
