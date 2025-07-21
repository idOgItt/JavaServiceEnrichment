package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.ConfigReader;
import ru.mai.lessons.rpks.exceptions.ConfigurationLoadException;

@Slf4j
public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig() {
        try {
            Config config = ConfigFactory.load("application.conf");
            log.info("Load configuration successful");
            return config;
        } catch (Exception e) {
            log.error("Load configuration failed", e);
            throw  new ConfigurationLoadException("Failed to load config", e);
        }
    }
}
