package ru.mai.lessons.rpks;

import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

public interface MongoDBClientEnricher {
    Message enrich (Rule rule, Message message);
}
