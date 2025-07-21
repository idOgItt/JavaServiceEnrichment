package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.*;

@Slf4j
public class ServiceEnrichment implements Service {

    public void start(Config config) {
        log.info("Starting ServiceEnrichment...");

        MongoDBClientEnricher enricher = new MongoDBClientEnricherImpl(config);
        RuleProcessor ruleProcessor   = new RuleProcessorImpl(enricher);

        DbReader dbReader   = new DbReaderImpl(config);
        KafkaWriter writer  = new KafkaWriterImpl(config);
        KafkaReader reader  = new KafkaReaderImpl(config, ruleProcessor, dbReader, writer);

        reader.processing();
        log.info("ServiceEnrichment started and processing Kafka topic.");
    }

}
