package moe.dare.briareus.common.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;

enum LoggingUncaughtExceptionHandler implements UncaughtExceptionHandler {
    INSTANCE;
    private static final Logger log = LoggerFactory.getLogger(LoggingUncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error("Uncaught exception in thread {}", t, e);
    }
}
