package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Comparator;
import java.util.function.Function;
import java.util.stream.StreamSupport;

@Slf4j
public final class MongoDBClientEnricherImpl implements MongoDBClientEnricher {

    private final MongoClient mongoClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String databaseName;
    private final String collectionName;

    public MongoDBClientEnricherImpl(com.typesafe.config.Config config) {
        this.mongoClient = MongoClients.create(config.getString("mongo.connectionString"));
        this.databaseName = config.getString("mongo.database");
        this.collectionName = config.getString("mongo.collection");
    }

    @Override
    public Message enrich(Rule enrichmentRule, Message inputMessage) {
        MongoCollection<Document> mongoCollection = mongoClient
                .getDatabase(databaseName)
                .getCollection(collectionName);

        Function<Document, String> documentToJson = Document::toJson;

        String enrichmentValue = StreamSupport
                .stream(mongoCollection.find(Filters.eq(enrichmentRule.getFieldNameEnrichment(), enrichmentRule.getFieldValue()))
                        .spliterator(), false)
                .max(Comparator.comparing(document -> document.getObjectId("_id")))
                .map(documentToJson)
                .orElse(enrichmentRule.getFieldValueDefault());

        return applyEnrichmentValue(inputMessage, enrichmentRule.getFieldName(), enrichmentValue);
    }

    private Message applyEnrichmentValue(Message originalMessage, String targetFieldName, String targetFieldValue) {
        try {
            ObjectNode messageJson = (ObjectNode) objectMapper.readTree(originalMessage.getValue());
            messageJson.set(targetFieldName, parseJsonNode(targetFieldValue));
            return Message.builder()
                    .value(objectMapper.writeValueAsString(messageJson))
                    .build();
        } catch (JsonProcessingException e) {
            log.error("Cannot enrich message", e);
            return originalMessage;
        }
    }

    private JsonNode parseJsonNode(String rawValue) {
        try {
            return objectMapper.readTree(rawValue);
        } catch (JsonProcessingException ignore) {
            return TextNode.valueOf(rawValue);
        }
    }
}
