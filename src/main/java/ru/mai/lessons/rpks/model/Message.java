package ru.mai.lessons.rpks.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Message {
    private String value; // сообщение из Kafka в формате JSON
}
