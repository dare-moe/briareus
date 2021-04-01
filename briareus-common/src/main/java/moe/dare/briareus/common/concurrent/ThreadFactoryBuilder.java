package moe.dare.briareus.common.concurrent;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class ThreadFactoryBuilder {
    private final String prefix;
    private Boolean daemon;
    public static ThreadFactoryBuilder withPrefix(String prefix) {
        return new ThreadFactoryBuilder(prefix);
    }

    private ThreadFactoryBuilder(String prefix) {
        this.prefix = requireNonNull(prefix, "Thread name prefix");
    }

    public ThreadFactoryBuilder deamon(boolean daemon) {
        this.daemon = daemon;
        return this;
    }

    public ThreadFactory build() {
        final Boolean factoryDaemon = this.daemon;
        final ThreadFactory backingFactory = Executors.defaultThreadFactory();
        final AtomicInteger num = new AtomicInteger();
        return runnable -> {
            final Thread thread = backingFactory.newThread(runnable);
            thread.setName(prefix + num.getAndIncrement());
            if (factoryDaemon != null) {
                thread.setDaemon(factoryDaemon);
            }
            thread.setUncaughtExceptionHandler(LoggingUncaughtExceptionHandler.INSTANCE);
            return thread;
        };
    }
}
