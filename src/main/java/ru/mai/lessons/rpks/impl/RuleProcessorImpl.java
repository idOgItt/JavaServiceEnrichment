package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public final class RuleProcessorImpl implements RuleProcessor {

    private final MongoDBClientEnricher mongoEnricher;

    @Override
    public Message processing(Message sourceMessage, Rule[] enrichmentRules) {
        return Arrays.stream(enrichmentRules)
                .collect(Collectors.groupingBy(
                        Rule::getFieldName,
                        Collectors.maxBy(Comparator.comparingLong(Rule::getRuleId))))
                .values().stream()
                .flatMap(Optional::stream)
                .reduce(sourceMessage,
                        (currentMessage, rule) -> mongoEnricher.enrich(rule, currentMessage),
                        (msg1, msg2) -> msg2);
    }
}
