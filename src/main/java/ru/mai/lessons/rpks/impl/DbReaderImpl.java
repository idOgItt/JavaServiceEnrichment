package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import javax.sql.DataSource;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
public class DbReaderImpl implements DbReader, AutoCloseable {
    private final AtomicReference<AtomicReferenceArray<Rule>> rules = new AtomicReference<>(new AtomicReferenceArray<>(0));
    private final DSLContext dslContext;
    private final ScheduledExecutorService scheduler;

    public DbReaderImpl(Config config) {
        DataSource dataSource = initDataSource(config);
        this.dslContext = DSL.using(dataSource, SQLDialect.POSTGRES);

        int updateIntervalSec = config.getInt("application.updateIntervalSec");
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.scheduler.scheduleAtFixedRate(this::updateRules, 0, updateIntervalSec, TimeUnit.SECONDS);
    }

    private DataSource initDataSource(Config config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
        hikariConfig.setUsername(config.getString("db.user"));
        hikariConfig.setPassword(config.getString("db.password"));
        hikariConfig.setDriverClassName(config.getString("db.driver"));

        return new HikariDataSource(hikariConfig);
    }

    private void updateRules() {
        try {
            log.info("Updating rules from DB...");
            Rule[] loadedRules = dslContext
                    .selectFrom("enrichment_rules")
                    .fetchInto(Rule.class)
                    .toArray(new Rule[0]);

            rules.set(new AtomicReferenceArray<>(loadedRules));

            log.info("Loaded {} rules from DB", loadedRules.length);
            Stream.of(loadedRules).forEach(rule ->
                    log.info("Loaded rule: enricherId={}, ruleId={}, fieldName={}, fieldNameEnrichment={}, fieldValue={}, fieldValueDefault={}",
                            rule.getEnricherId(),
                            rule.getRuleId(),
                            rule.getFieldName(),
                            rule.getFieldNameEnrichment(),
                            rule.getFieldValue(),
                            rule.getFieldValueDefault())
            );

        } catch (Exception e) {
            log.error("Failed to update rules from DB", e);
        }
    }

    @Override
    public Rule[] readRulesFromDB() {
        AtomicReferenceArray<Rule> currentRules = rules.get();
        return IntStream.range(0, currentRules.length())
                .mapToObj(currentRules::get)
                .toArray(Rule[]::new);
    }

    @Override
    public void close() {
        log.info("Shutting down scheduler in DbReaderImpl");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
