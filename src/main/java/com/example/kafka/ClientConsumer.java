package com.example.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 代替 {@link  java.util.function.Consumer} 允许抛出异常
 *
 * @author xianglin
 */
@FunctionalInterface
public interface ClientConsumer<T> {
    void accept(T adminClient) throws Exception;

    default void tryAccept(T adminClient) {
        try {
            accept(adminClient);
        } catch (Exception exception) {
            Logger logger = LoggerFactory.getLogger(ClientConsumer.class);
            logger.error("", exception);
        }
    }
}