package ru.mai.lessons.rpks.exceptions;

public class ConfigurationLoadException extends RuntimeException {
    public ConfigurationLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
